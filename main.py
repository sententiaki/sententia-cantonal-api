"""
Sententia — Search API  (porta 8002 locale · Render in produzione)
Supporta tutti gli endpoint richiesti da sententia-prototype.html
e sententia-summarize-prototype.html.

Endpoints
─────────
  GET /cerca              Ricerca federale via OpenCaseLaw (risposta JSON)
  GET /cerca_stream       Stessa ricerca via SSE: risultati arrivano in fila man mano che sono pronti
  GET /sintesi            Riassunto AI di una sentenza (alias /sintesi_federal)
  GET /sintesi_federal    Idem
  GET /articolo_fedlex    Testo articolo di legge federale (proxy fedlex-connector.ch)
  GET /html_federale      HTML pulito di una sentenza da bger.li
  GET /health             Health check

Avvio locale:
  pip install fastapi uvicorn httpx beautifulsoup4 openai
  OPENAI_API_KEY=sk-...  uvicorn sententia-search-api:app --port 8002 --reload
"""

import asyncio
import json
import logging
import math
import os
import re
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import AsyncIterator, Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from openai import AsyncOpenAI
from pydantic import BaseModel

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sententia Search API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Constants ─────────────────────────────────────────────────────────────────
OPENCASELAW_BASE    = "https://mcp.opencaselaw.ch/api"
ENTSCHEIDSUCHE_BASE = "https://entscheidsuche.ch/_search.php"

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# Tribunali federali riconosciuti (tutto il resto è cantonale)
_FEDERAL_COURT_KEYWORDS = {
    "bundesgericht", "tribunal fédéral", "tribunale federale",
    "bundesverwaltungsgericht", "tribunal administratif fédéral", "tribunale amministrativo federale",
    "bundesstrafgericht", "tribunal pénal fédéral", "tribunale penale federale",
    "bundespatentgericht", "tribunale federale dei brevetti",
    "bger", "bvger", "bstger", "bpatger",
    "bstg", "bge", "bvge",  # varianti abbreviate usate nei _id ES (CH_BSTG_, CH_BGE_, CH_BVGE_)
}

def _rileva_tipo(court_raw: str) -> str:
    """Restituisce 'federal' se il tribunale è federale, altrimenti 'cantonal'."""
    key = court_raw.lower().strip()
    for kw in _FEDERAL_COURT_KEYWORDS:
        if kw in key:
            return "federal"
    return "cantonal"

def _rileva_tribunal(court_raw: str) -> str:
    """Restituisce 'bger' | 'bvger' | 'bstger' | 'bpatger' | '' per i tribunali federali."""
    key = court_raw.lower().strip()
    if any(kw in key for kw in ("bundesverwaltungsgericht", "tribunal administratif fédéral",
                                  "tribunale amministrativo federale", "bvger", "bvge")):
        return "bvger"
    if any(kw in key for kw in ("bundesstrafgericht", "tribunal pénal fédéral",
                                  "tribunale penale federale", "bstger", "bstg")):
        return "bstger"
    if any(kw in key for kw in ("bundespatentgericht", "bpatger")):
        return "bpatger"
    if any(kw in key for kw in ("bundesgericht", "tribunal fédéral", "tribunale federale",
                                  "federal supreme court", "bger", "bge")):
        return "bger"
    return ""

# Parole chiave per riconoscere il cantone dal nome del tribunale
_CANTON_KEYWORDS: dict[str, list[str]] = {
    "ag": ["aargau", "argovia", "argovie"],
    "ai": ["appenzell innerrhoden", "appenzello interno"],
    "ar": ["appenzell ausserrhoden", "appenzello esterno"],
    "be": ["bern", "berne", "berna"],
    "bl": ["basel-landschaft", "basilea campagna", "bâle-campagne"],
    "bs": ["basel-stadt", "basilea città", "bâle-ville"],
    "fr": ["freiburg", "friburgo", "fribourg"],
    "ge": ["genf", "genève", "geneve", "ginevra", "canton de genève", "kanton genf"],
    "gl": ["glarus", "glarona", "glaris"],
    "gr": ["graubünden", "grigioni", "grisons"],
    "ju": ["jura", "giura"],
    "lu": ["luzern", "lucerna", "lucerne"],
    "ne": ["neuenburg", "neuchâtel", "neuchatel"],
    "nw": ["nidwalden", "nidvaldo", "nidwald"],
    "ow": ["obwalden", "obvaldo", "obwald"],
    "sg": ["st. gallen", "san gallo", "saint-gall"],
    "sh": ["schaffhausen", "sciaffusa", "schaffhouse"],
    "so": ["solothurn", "soletta", "soleure"],
    "sz": ["schwyz", "svitto", "schwytz"],
    "tg": ["thurgau", "turgovia", "thurgovie"],
    "ti": ["ticino", "tessin"],
    "ur": ["uri"],
    "vd": ["vaud", "waadt"],
    "vs": ["wallis", "vallese", "valais"],
    "zg": ["zug", "zugo", "zoug"],
    "zh": ["zürich", "zurich", "zurigo"],
}

def _rileva_cantone(court_raw: str) -> str:
    """Restituisce la sigla del cantone ('ti', 'zh', ...) o '' se non riconosciuto."""
    key = court_raw.lower().strip()
    for canton, keywords in _CANTON_KEYWORDS.items():
        if any(kw in key for kw in keywords):
            return canton
    return ""

# Prefisso numero di dossier → area giuridica
AREA_MAP: dict[str, str] = {
    "6": "penale", "7": "penale",
    "4": "civile", "5": "civile",
    "8": "sociale", "9": "sociale",
    "1": "pubblico", "2": "pubblico", "3": "pubblico",
}

# Nome tribunale grezzo → etichetta display (ordinato dal più specifico al meno specifico)
COURT_DISPLAY: list[tuple[str, str]] = [
    ("bundesverwaltungsgericht",        "BVGer — Trib. amm. federale"),
    ("tribunal administratif fédéral",  "BVGer — Trib. amm. federale"),
    ("verwaltungsgericht des bundes",   "BVGer — Trib. amm. federale"),
    ("bvger",                           "BVGer — Trib. amm. federale"),
    ("bundesstrafgericht",              "BStGer — Trib. penale federale"),
    ("tribunal pénal fédéral",          "BStGer — Trib. penale federale"),
    ("bstger",                          "BStGer — Trib. penale federale"),
    ("bundespatentgericht",             "BPatGer — Trib. fed. brevetti"),
    ("bundesgericht",                   "BGer — Tribunale federale"),
    ("tribunal fédéral",                "BGer — Tribunale federale"),
    ("tribunale federale",              "BGer — Tribunale federale"),
    ("federal supreme court",           "BGer — Tribunale federale"),
    ("bger",                            "BGer — Tribunale federale"),
    ("bge",                             "BGer — Tribunale federale"),
]

# Abbreviazioni legge per la normalizzazione
LAW_ALIASES: dict[str, str] = {
    "OR": "CO", "ZGB": "CC", "STGB": "CP", "ZPO": "CPC", "STPO": "CPP",
    "BGG": "LTF", "KVG": "LAMal", "LAA": "LAINF", "AHVG": "LAVS",
    "IVG": "LAI", "SCHKG": "LEF", "DBG": "LIFD", "MWSTG": "LIVA",
    "UWG": "LCD", "KG": "LCart", "MSCHG": "LPM", "FUSG": "LFus",
    "IPRG": "LDIP", "VWVG": "PA",
    # Costituzione federale
    "COST": "Cost.", "CST": "Cst.",
}

# Parole che NON sono sigle di legge ma possono essere catturate da ARTICLE_RE
_INVALID_CODES: frozenset[str] = frozenset({
    # Indicatori di comma/paragrafo spesso seguiti da un numero
    "ABS", "CPV", "AL", "LETT", "LIT", "ZIFF", "NR", "CH",
    # Riferimenti ad allegati/annessi
    "ALLEGATO", "ALLEGATI", "ANNEX", "ANNEXE", "ANHANG",
    # Plurali latini o abbreviazioni di pagina/seguente usate nei testi
    "SEGG", "SS", "FF", "COMMA", "COMMI", "UNTER",
})

# Regex per l'estrazione degli articoli dal testo
# Pattern principale: "Art. 53 CP" / "art 336 CO" / "Art. 8a CC"
_CODES = (
    "CP|StGB|CO|OR|CPP|StPO|LTF|BGG|BV|Cost\\.|Cst\\.|ZPO|CPC"
    "|LPD|DSG|CC|ZGB|LAMal|KVG|LAINF|LAA|LAVS|AHVG|LAI|IVG"
    "|LEF|SchKG|LIFD|DBG|LIVA|MWSTG|LCD|UWG|LCart|KG|LPM|MSchG"
    "|LFus|FusG|LDIP|IPRG|PA|VwVG"
)
# Cattura: "Art.? N [cpv/abs/al/lett N] CODE"  oppure  "articolo N CODE"
# Il codice legge può essere qualsiasi sigla che inizia con maiuscola (2-8 char)
ARTICLE_RE = re.compile(
    r'(?:[Aa]rt(?:icol[oi])?\.?\s+)(\d+[a-z]{0,8})'
    r'(?:\s+(?:[Cc]pv|[Aa]bs|[Aa]l|[Ll]ett?|[Ll]it|[Zz]iff|ch|[Nn]r)\.?\s*[a-z\d]+)?'
    r'(?:\s+(?:del|della|des?|von|du|de\s+la))?'
    r'\s+([A-Z][A-Za-z]{1,7}\.?)\b',
    re.UNICODE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_ART_CONGIUNTO_RE = re.compile(
    r'Art\.\s+(\d+[a-z]{0,8})\s+(?:e|und|et)\s+(\d+[a-z]{0,8})\s+(' + _CODES + r')\b',
    re.UNICODE,
)

def _espandi_art_congiunti(testo: str) -> str:
    """Art. 137 e 138 DBG → Art. 137 DBG Art. 138 DBG"""
    def _sub(m: re.Match) -> str:
        return f"Art. {m.group(1)} {m.group(3)} Art. {m.group(2)} {m.group(3)}"
    return _ART_CONGIUNTO_RE.sub(_sub, testo)

def estrai_articoli(testo: str, max_art: int = 6) -> list[str]:
    testo = _espandi_art_congiunti(testo)
    seen, result = set(), []
    for num, code in ARTICLE_RE.findall(testo):
        code_key = code.upper().rstrip('.')
        # Scarta falsi positivi (Abs, Allegato, ecc.)
        if code_key in _INVALID_CODES:
            continue
        code_norm = LAW_ALIASES.get(code_key, code)
        label = f"Art. {num} {code_norm}"
        if label not in seen:
            seen.add(label)
            result.append(label)
        if len(result) >= max_art:
            break
    return result


def estrai_articoli_combinati(riass: str, testo: str, min_art: int = 3, max_art: int = 6) -> list[str]:
    """Riassunto AI come fonte primaria; integra dal testo grezzo se risultano < min_art."""
    result = estrai_articoli(riass, max_art) if riass else []
    if len(result) < min_art and testo:
        seen = set(result)
        for art in estrai_articoli(testo, max_art * 3):
            if art not in seen:
                seen.add(art)
                result.append(art)
            if len(result) >= max_art:
                break
    return result

def rileva_area(docket: str) -> str:
    return AREA_MAP.get(docket.strip()[:1], "pubblico")

def normalizza_tribunale(raw: str) -> str:
    key = raw.lower().strip()
    for k, v in COURT_DISPLAY:
        if k in key:
            return v
    # Fallback: pulisce il nome grezzo (rimuove parti dopo virgola/trattino)
    clean = re.split(r'[,\-–]', raw.strip())[0].strip()
    return clean if clean else "BGer — Tribunale federale"

def formatta_data(raw: str) -> str:
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', raw or "")
    return f"{m.group(3)}.{m.group(2)}.{m.group(1)}" if m else (raw or "")

def _normalize_codice(c: str) -> str:
    """Normalizza un numero di ruolo per confronto esatto case-insensitive e separator-agnostico.
    Es. '6B_51/2021', '6b 51/2021', '6B-51/2021' → '6b512021'.
    Gestisce prefissi tribunale (BGE, ATF, BGer, BVGer, BStGer, BPatGer) separati da spazio o _:
    'BGE 105 II 16', 'BGE_134_III_67', 'BVGer F-2684/2026' → normalizzati senza prefisso."""
    c = c.strip()
    # Rimuovi prefisso tribunale seguito da spazio OPPURE underscore
    c = re.sub(r'^(?:bge|atf|bger|bvger|bstger|bpatger|rdaf|rkge)[\s_]+', '', c, flags=re.IGNORECASE)
    return re.sub(r'[\s_\-./]', '', c).lower()

def costruisci_url_bger(codice: str) -> str:
    c = codice.strip()
    # Rimuovi prefisso BGE/ATF/BGer ecc. seguito da spazio OPPURE underscore
    # (es. 'BGE 134 III 67', 'BGE_134_III_67', 'ATF_105_II_16' → tutti corretti)
    c = re.sub(r'^(?:bge|atf|bger|bvger|bstger|bpatger|rdaf|rkge)[\s_]+', '', c, flags=re.IGNORECASE)
    # Sostituisce tutti i separatori (spazi, _, /) con - per il formato URL di bger.li
    c = re.sub(r'[\s_/]', '-', c)
    return f"https://bger.li/{c}"

def sse(data: dict) -> str:
    """Formatta un evento SSE."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# ── Query pre-processor: normalizza articoli prima dell'ottimizzazione AI ──────

# Nomi di legge estesi → abbreviazione (IT / DE / FR)
_LAW_NAME_PATTERNS: list[tuple[re.Pattern, str]] = [
    # Italiano
    (re.compile(r'\bcodice\s+penale\b',                                    re.I), "CP"),
    (re.compile(r'\bcodice\s+civile\b',                                    re.I), "CC"),
    (re.compile(r'\bcodice\s+delle\s+obbligazioni\b',                      re.I), "CO"),
    (re.compile(r'\bcodice\s+di\s+procedura\s+civile\b',                   re.I), "CPC"),
    (re.compile(r'\bcodice\s+di\s+procedura\s+penale\b',                   re.I), "CPP"),
    (re.compile(r'\blegge\s+sul\s+tribunale\s+federale\b',                 re.I), "LTF"),
    (re.compile(r'\bcostituzione\s+federale\b',                            re.I), "Cost."),
    (re.compile(r'\blegge\s+(?:federale\s+)?sulla\s+protezione\s+dei\s+dati\b', re.I), "LPD"),
    (re.compile(r'\blegge\s+sull[ae]\s+esecuzione\b',                      re.I), "LEF"),
    # Tedesco
    (re.compile(r'\bstrafgesetzbuch\b',                                    re.I), "StGB"),
    (re.compile(r'\bzivilgesetzbuch\b',                                    re.I), "ZGB"),
    (re.compile(r'\bobligationenrecht\b',                                   re.I), "OR"),
    (re.compile(r'\bzivilprozessordnung\b',                                re.I), "ZPO"),
    (re.compile(r'\bstrafprozessordnung\b',                                re.I), "StPO"),
    (re.compile(r'\bbundesgerichtsgesetz\b',                               re.I), "BGG"),
    (re.compile(r'\bbundesverfassung\b',                                   re.I), "BV"),
    (re.compile(r'\bdatenschutzgesetz\b',                                  re.I), "DSG"),
    # Francese
    (re.compile(r'\bcode\s+pénal\b',                                       re.I), "CP"),
    (re.compile(r'\bcode\s+civil\b',                                       re.I), "CC"),
    (re.compile(r'\bcode\s+des\s+obligations\b',                           re.I), "CO"),
    (re.compile(r'\bcode\s+de\s+procédure\s+civile\b',                     re.I), "CPC"),
    (re.compile(r'\bcode\s+de\s+procédure\s+pénale\b',                     re.I), "CPP"),
    (re.compile(r'\bloi\s+sur\s+le\s+tribunal\s+fédéral\b',                re.I), "LTF"),
    (re.compile(r'\bconstitution\s+fédérale\b',                            re.I), "Cst."),
]

# Normalizza varianti di "articolo/article/artikel/art" → "Art. NNN"
# Cattura anche "art.111" (senza spazio) e "art 111" (senza punto)
_QUERY_ART_RE = re.compile(
    r'\b(articol[oi]|article|artikel|art)\.?\s*(\d+[a-z]{0,8})',
    re.IGNORECASE | re.UNICODE,
)
# Rimuove preposizioni residue: "Art. 111 del CP" → "Art. 111 CP"
_QUERY_DEL_RE = re.compile(
    r'(Art\.\s+\d+[a-z]{0,8}(?:\s+(?:cpv|abs|al|Abs)\.?\s*\d+)?)\s+'
    r'(?:del|della|der|des|de\s+la|du|von|di)\s+',
    re.IGNORECASE,
)
# Riordina "CODICE Art. NNN" → "Art. NNN CODICE" (es. "CO Art. 97" → "Art. 97 CO")
_QUERY_CODE_BEFORE_ART_RE = re.compile(
    r'\b(CP|CC|CO|CPC|CPP|LTF|BGG|BV|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG)\s+(Art\.\s+\d+[a-z]{0,8})',
    re.IGNORECASE,
)
# Mappa codici → forma canonica (gestisce maiuscole/minuscole)
_CODE_CANON: dict[str, str] = {
    "cp": "CP", "cc": "CC", "co": "CO", "cpc": "CPC", "cpp": "CPP",
    "ltf": "LTF", "bgg": "BGG", "bv": "BV", "bg": "BG",
    "cost.": "Cost.", "cst.": "Cst.",
    "stgb": "StGB", "zgb": "ZGB", "or": "OR", "zpo": "ZPO",
    "stpo": "StPO", "lpd": "LPD", "dsg": "DSG",
    "lef": "LEF", "lp": "LP", "schkg": "SchKG",
    "dbg": "DBG", "lifd": "LIFD",
}

# Normalizza "capoverso N" / "Absatz N" / "alinéa N" → forma standard con punto
_CPV_FULL_RE = re.compile(
    r'\b(capoverso|alinéa|alinea)\s+(\d+)',    re.I | re.UNICODE)
_ABS_FULL_RE = re.compile(
    r'\b(Absatz)\s+(\d+)',                      re.I)
# Aggiunge il punto mancante: "cpv 1" → "cpv. 1",  "Abs 1" → "Abs. 1"
_CPV_NODOT_RE = re.compile(r'\bcpv\s+(\d+)',   re.I)
_ABS_NODOT_RE = re.compile(r'\bAbs\s+(\d+)')
_AL_NODOT_RE  = re.compile(r'\bal\s+(\d+)',    re.I)

# "N CODE" senza prefisso Art. — cattura anche "N cpv/Abs/al N CODE"
_BARE_NUM_CODE_RE = re.compile(
    r'(?<!\w)'
    r'(\d+[a-z]{0,8})'
    r'(\s+(?:cpv\.?|Abs\.?|al\.?)\s*\d+)?'
    r'\s+(CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|LP|SchKG|DBG|LIFD)\b',
    re.UNICODE | re.IGNORECASE,
)
# "CODE N" senza prefisso Art.  (es. "OR 50", "StGB 111", "or 50")
_CODE_BARE_NUM_RE = re.compile(
    r'\b(CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|LP|SchKG|DBG|LIFD)\s+(\d+[a-z]{0,8})\b',
    re.UNICODE | re.IGNORECASE,
)

def pre_processa_query(query: str) -> str:
    """
    Normalizza i riferimenti agli articoli nella query PRIMA dell'ottimizzazione AI.

    Esempi:
      "articolo 111 del codice penale"  →  "Art. 111 CP"
      "art 336 CO" / "art.53 cp"        →  "Art. 336 CO" / "Art. 53 CP"
      "50 OR" / "or 50" / "50 or"       →  "Art. 50 OR"
      "50 cpv 1 OR"                      →  "Art. 50 cpv. 1 OR"
      "capoverso 1 art 50 CO"            →  "Art. 50 cpv. 1 CO"
      "stgb 111"                         →  "Art. 111 StGB"
    """
    result = query
    # 1. Nomi di legge estesi → abbreviazione
    for pat, abbrev in _LAW_NAME_PATTERNS:
        result = pat.sub(abbrev, result)
    # 2. Capoverso: forme complete → abbreviazione standard con punto
    result = _CPV_FULL_RE.sub(lambda m: f"cpv. {m.group(2)}", result)
    result = _ABS_FULL_RE.sub(lambda m: f"Abs. {m.group(2)}", result)
    result = _CPV_NODOT_RE.sub(lambda m: f"cpv. {m.group(1)}", result)
    result = _ABS_NODOT_RE.sub(lambda m: f"Abs. {m.group(1)}", result)
    result = _AL_NODOT_RE.sub( lambda m: f"al. {m.group(1)}",  result)
    # 3. "articolo/article/artikel N" → "Art. N"  (con prefisso esplicito)
    result = _QUERY_ART_RE.sub(lambda m: f"Art. {m.group(2)}", result)
    # 4. Canonicalizza codici ora (stgb→StGB, or→OR) così i passi 5/6 li riconoscono
    def _canon(m: re.Match) -> str:
        return _CODE_CANON.get(m.group(1).lower(), m.group(1).upper())
    result = re.sub(
        r'\b(cp|cc|co|cpc|cpp|ltf|bgg|bv|stgb|zgb|or|zpo|stpo|lpd|dsg|lef|lp|schkg|dbg|lifd|cost\.|cst\.)\b',
        _canon, result, flags=re.IGNORECASE,
    )
    # 5. "CODE N" (senza Art.) → "Art. N CODE"   (es. "OR 50" → "Art. 50 OR")
    def _repl_code_num(m: re.Match) -> str:
        before = result[max(0, m.start()-8):m.start()]
        if re.search(r'Art\.\s+\d', before):
            return m.group(0)  # già parte di "Art. N CODE"
        code = _CODE_CANON.get(m.group(1).lower(), m.group(1))
        return f"Art. {m.group(2)} {code}"
    result = _CODE_BARE_NUM_RE.sub(_repl_code_num, result)
    # 6. "N CODE" (senza Art.) → "Art. N CODE"   (es. "50 OR" → "Art. 50 OR")
    #    Usa finditer per controllare il contesto e non duplicare "Art."
    parts, last = [], 0
    for m in _BARE_NUM_CODE_RE.finditer(result):
        prefix_ctx = result[max(0, m.start()-6):m.start()]
        if re.search(r'[Aa]rt\.\s*$', prefix_ctx):
            continue  # già preceduto da "Art. " — salta
        code = _CODE_CANON.get(m.group(3).lower(), m.group(3))
        parts.append(result[last:m.start()])
        parts.append(f"Art. {m.group(1)}{m.group(2) or ''} {code}")
        last = m.end()
    parts.append(result[last:])
    result = ''.join(parts)
    # 7. Riordina "cpv./Abs. N Art. N CODE" → "Art. N cpv./Abs. N CODE"
    result = re.sub(
        r'\b(cpv\.|Abs\.|al\.)\s*(\d+)\s+(Art\.\s+\d+[a-z]{0,8})',
        lambda m: f"{m.group(3)} {m.group(1)} {m.group(2)}",
        result, flags=re.IGNORECASE,
    )
    # 8. Rimuove preposizioni tra numero articolo e codice
    result = _QUERY_DEL_RE.sub(r'\1 ', result)
    # 9. Riordina "CODICE Art. NNN" → "Art. NNN CODICE"
    result = _QUERY_CODE_BEFORE_ART_RE.sub(lambda m: f"{m.group(2)} {m.group(1)}", result)
    return result.strip()


# Equivalenze tra sigle di legge nelle tre lingue nazionali (IT / FR / DE)
# Per leggi con sigla identica in IT e FR (CO, CP, CC, CPC, CPP, LTF, LPD)
# l'equivalente FR è già coperto dalla sigla IT — si aggiunge solo il DE.
# Per leggi con sigle diverse nelle tre lingue (Cost./Cst./BV, LEF/LP/SchKG)
# si espande a tutte e tre le forme.
_CODE_EQUIVALENTS: dict[str, list[str]] = {
    # IT = FR → aggiunge DE
    "CP":    ["StGB"],          # Codice penale (IT) = Code pénal (FR) = Strafgesetzbuch (DE)
    "CC":    ["ZGB"],           # Codice civile (IT) = Code civil (FR) = Zivilgesetzbuch (DE)
    "CO":    ["OR"],            # Codice obbligazioni (IT) = Code des obligations (FR) = OR (DE)
    "CPC":   ["ZPO"],           # Cod. proc. civile (IT/FR) = Zivilprozessordnung (DE)
    "CPP":   ["StPO"],          # Cod. proc. penale (IT/FR) = Strafprozessordnung (DE)
    "LTF":   ["BGG"],           # Legge TF (IT/FR) = Bundesgerichtsgesetz (DE)
    "LPD":   ["DSG"],           # Legge prot. dati (IT/FR) = Datenschutzgesetz (DE)
    # Costituzione — IT ≠ FR ≠ DE: espande a tutte e tre
    "Cost.": ["Cst.", "BV"],    # IT → FR + DE
    "Cst.":  ["Cost.", "BV"],   # FR → IT + DE
    # Legge esecuzione — IT ≠ FR ≠ DE: espande a tutte e tre
    "LEF":   ["LP", "SchKG"],   # IT → FR + DE
    "LP":    ["LEF", "SchKG"],  # FR → IT + DE
    # Imposta federale diretta — IT/FR = LIFD, DE = DBG
    "LIFD":  ["DBG"],           # IT/FR → DE
    "DBG":   ["LIFD"],          # DE → IT/FR
    # Concorrenza sleale — IT/FR = LCD, DE = UWG
    "LCD":   ["UWG"],
    "UWG":   ["LCD"],
    # DE → IT + FR
    "StGB":  ["CP"],
    "ZGB":   ["CC"],
    "OR":    ["CO"],
    "ZPO":   ["CPC"],
    "StPO":  ["CPP"],
    "BGG":   ["LTF"],
    "BV":    ["Cost.", "Cst."], # DE → IT + FR
    "DSG":   ["LPD"],
    "SchKG": ["LEF", "LP"],     # DE → IT + FR
}

# Trova "Art. NNN [cpv./Abs./al. N] CODE" nella query già normalizzata
_ART_CODE_RE = re.compile(
    r'(Art\.\s+\d+[a-z]{0,8})'
    r'(\s+(?:cpv\.|Abs\.|al\.)\s*\d+)?'
    r'\s+([A-Z][A-Za-z]{0,7}\.?)\b',
    re.UNICODE,
)

# Riconosce una query che è SOLO un riferimento articolo (nessun concetto aggiuntivo)
_PURE_ART_RE = re.compile(
    r'^Art\.\s+\d+[a-z]{0,8}(?:\s+(?:cpv\.|Abs\.|al\.)\s*\d+)?'
    r'(?:\s+[A-Z][A-Za-z]{0,7}\.?)+\s*$',
    re.UNICODE,
)

def _cpv_other_lang(cpv_str: str) -> str:
    """Dato 'cpv. 1' o 'Abs. 1' o 'al. 1', restituisce la forma nell'altra lingua."""
    s = cpv_str.strip()
    m_it = re.match(r'cpv\.\s*(\d+)', s, re.I)
    m_fr = re.match(r'al\.\s*(\d+)',  s, re.I)
    m_de = re.match(r'Abs\.\s*(\d+)', s)
    if m_it:  return f"Abs. {m_it.group(1)}"   # IT → DE
    if m_fr:  return f"Abs. {m_fr.group(1)}"   # FR → DE
    if m_de:  return f"cpv. {m_de.group(1)}"   # DE → IT
    return ""

def espandi_codici_articolo(query: str) -> str:
    """
    Aggiunge una seconda forma dell'articolo nella lingua equivalente,
    come riferimento separato (non due codici sullo stesso articolo).

    Esempi:
      "Art. 111 CP"         →  "Art. 111 CP Art. 111 StGB"
      "Art. 50 cpv. 1 CO"   →  "Art. 50 cpv. 1 CO Art. 50 Abs. 1 OR"
      "Art. 50 Abs. 1 OR"   →  "Art. 50 Abs. 1 OR Art. 50 cpv. 1 CO"
      "Art. 59 LTF"         →  "Art. 59 LTF Art. 59 BGG"
      "violazione Art. 146 CP" → "violazione Art. 146 CP Art. 146 StGB"
    """
    def _expand(m: re.Match) -> str:
        art_part = m.group(1)          # "Art. 50"
        cpv_part = m.group(2) or ""    # " cpv. 1" or ""
        code     = m.group(3)          # "CO"
        extras   = _CODE_EQUIVALENTS.get(code, [])
        if not extras:
            return m.group(0)
        # Costruisce il riferimento equivalente completo (stessa struttura, codice diverso)
        if cpv_part.strip():
            cpv_other = _cpv_other_lang(cpv_part.strip())
            alt_refs = " ".join(
                f"{art_part} {cpv_other} {e}" if cpv_other else f"{art_part}{cpv_part} {e}"
                for e in extras
            )
        else:
            alt_refs = " ".join(f"{art_part} {e}" for e in extras)
        return f"{art_part}{cpv_part} {code} {alt_refs}"

    return _ART_CODE_RE.sub(_expand, query)


# ── Query optimizer (GPT-4o-mini) ─────────────────────────────────────────────

_OPTIMIZER_SYSTEM = """Sei un esperto di ricerca giuridica svizzera.
Trasforma la query dell'utente in termini di ricerca ottimali per un motore full-text
di sentenze svizzere (Elasticsearch multilingue IT/FR/DE).

Regole ASSOLUTE:
- I riferimenti ad articoli di legge (es. "Art. 41 CO Art. 41 OR") sono GIÀ gestiti dal sistema.
  NON toccarli, NON aggiungere nuove forme, lasciali ESATTAMENTE come appaiono.
- NON aggiungere mai sigle o abbreviazioni di leggi (es. LEI, LStrI, CO, CP, CC, OR, StGB, AIG…).
  Usa SOLO linguaggio naturale descrittivo.
- Se la query è un codice sentenza (es. 6B_51/2021, BGE 147 IV 73), restituiscila com'è.

Per query concettuali (senza articoli):
Espandi con i termini tecnico-giuridici precisi che appaiono realmente nelle sentenze svizzere,
nelle TRE lingue nazionali, separando le varianti linguistiche con | (OR).
Aggiungi sinonimi giuridici, termini procedurali e varianti di registro che un giudice userebbe.

Esempi:
"licenziamento abusivo" →
  "licenziamento abusivo disdetta abusiva | missbräuchliche Kündigung Entlassung | licenciement abusif congé"

"impiego dipendenti senza permesso" →
  "lavoratori stranieri senza autorizzazione lavoro clandestino irregolare impiego illegale | Ausländer ohne Bewilligung Schwarzarbeit illegale Beschäftigung | travailleur sans autorisation emploi illégal clandestin"

"taglio pianta spazio comune" →
  "taglio albero pianta area comune condominio proprietà vicini | Baum fällen Gemeinschaftsfläche Stockwerkeigentum Nachbarrecht | abattage arbre espace commun copropriété voisinage"

"doppia imposizione" →
  "doppia imposizione divieto | Doppelbesteuerung Verbot | double imposition interdiction"

Per query miste (concetto + articolo):
Mantieni gli articoli INVARIATI, espandi SOLO la parte concettuale nelle 3 lingue.

Rispondi SOLO con JSON: {"query_ottimizzata": "...", "spiegazione": "..."}"""

async def _ocl_legge_espandi(code: str, http: httpx.AsyncClient) -> list[str]:
    """Chiama OCL per ottenere le sigle equivalenti nelle 3 lingue per un codice sconosciuto.
    Ritorna lista delle sigle diverse dal codice originale (es. ["VZAE"] per "OASA").
    In caso di errore o timeout ritorna lista vuota."""
    try:
        results = await asyncio.gather(
            http.get(f"{OPENCASELAW_BASE}/laws/{code}", params={"language": "de"}, timeout=3.0),
            http.get(f"{OPENCASELAW_BASE}/laws/{code}", params={"language": "fr"}, timeout=3.0),
            http.get(f"{OPENCASELAW_BASE}/laws/{code}", params={"language": "it"}, timeout=3.0),
            return_exceptions=True,
        )
        abbrs = []
        for r in results:
            if isinstance(r, Exception):
                continue
            if r.status_code == 200:
                abbr = r.json().get("abbreviation", "")
                if abbr and abbr != code and abbr not in abbrs:
                    abbrs.append(abbr)
        return abbrs
    except Exception:
        return []


async def ottimizza_query(query: str, ai: AsyncOpenAI, http: httpx.AsyncClient | None = None) -> tuple[str, str]:
    # 1. Normalizza: nomi di legge estesi → sigle, varianti articolo → "Art. NNN CODE"
    query_norm = pre_processa_query(query)
    # 2. Espandi i codici NOTI deterministicamente (CO→OR, CP→StGB, LCD→UWG, ecc.)
    #    PRIMA dell'AI, così l'AI non deve toccarli e non può duplicarli
    query_pre = espandi_codici_articolo(query_norm)

    # 2b. Per i codici SCONOSCIUTI (non in _CODE_EQUIVALENTS), lookup OCL in parallelo
    if http:
        unknown_codes = [
            m.group(3) for m in _ART_CODE_RE.finditer(query_norm)
            if m.group(3) not in _CODE_EQUIVALENTS
        ]
        if unknown_codes:
            lookup_results = await asyncio.gather(
                *[_ocl_legge_espandi(c, http) for c in unknown_codes],
                return_exceptions=True,
            )
            extra_map: dict[str, list[str]] = {}
            for code, res in zip(unknown_codes, lookup_results):
                if isinstance(res, list) and res:
                    extra_map[code] = res
            if extra_map:
                def _expand_extra(m: re.Match) -> str:
                    art, cpv, code = m.group(1), m.group(2) or "", m.group(3)
                    extras = extra_map.get(code, [])
                    if not extras:
                        return m.group(0)
                    alt = " ".join(f"{art} {e}" for e in extras)
                    return f"{art}{cpv} {code} {alt}"
                query_pre = _ART_CODE_RE.sub(_expand_extra, query_pre)

    # 3. Separa la parte concettuale dalla parte articoli.
    #    L'AI vede SOLO i concetti — gli articoli vengono riattaccati dopo.
    #    Questo impedisce all'AI di toccare/duplicare i riferimenti ad articoli.
    art_refs_expanded = _ES_ART_RE.findall(query_pre)
    concetto = _ES_ART_RE.sub("", query_norm).strip()  # parte senza articoli (dalla query originale)

    if not concetto:
        # Solo articoli — niente da tradurre
        return query_pre, "Espansione articolo"

    art_block = " " + " ".join(art_refs_expanded) if art_refs_expanded else ""

    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=250, temperature=0,
            messages=[
                {"role": "system", "content": _OPTIMIZER_SYSTEM},
                {"role": "user",   "content": f'Query: "{concetto}"'},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            d = json.loads(m.group())
            raw_opt = d.get("query_ottimizzata", concetto).strip().strip('"\'')
            # Rimuovi eventuali articoli che l'AI ha riscritto nel corpo della query.
            # Gli articoli corretti sono già in art_block (estratti dalla query originale).
            # Passaggio 1: strip articoli completi (Art. NNN CODE)
            raw_opt = _ES_ART_RE.sub("", raw_opt)
            # Passaggio 2: strip residui "NNN SIGLA" lasciati da articoli malformati
            #   es. "Art. 102 Art. 339 CO" → dopo strip → " 339 CO" → va rimosso
            raw_opt = re.sub(r'\b\d+[a-z]{0,8}\s+[A-Z][A-Za-z]{0,7}\.?\b', "", raw_opt)
            raw_opt = re.sub(r'\s*\.\s*(?=\||$)', " ", raw_opt)   # rimuovi punti isolati rimasti
            raw_opt = raw_opt.strip().rstrip("|").strip()
            final_opt = pre_processa_query(raw_opt) + art_block
            return final_opt, d.get("spiegazione", "")
    except Exception as exc:
        log.warning("Optimizer error: %s", exc)

    # Fallback se AI non disponibile
    return query_pre, "Query diretta"


# ── Summary generator (GPT-4o-mini) ──────────────────────────────────────────

_SUMMARY_SYSTEM = {
    "it": (
        "Sei un esperto legale svizzero. Scrivi un riassunto di questa sentenza in italiano, "
        "massimo 70 parole. Descrivi in modo conciso: il problema giuridico, la decisione del "
        "tribunale e l'esito. Menziona gli articoli applicati nel formato standard "
        "(es. Art. 53 CP, Art. 336 CO). Solo testo fluente, nessun titolo, nessun elenco."
    ),
    "de": (
        "Du bist ein Schweizer Rechtsexperte. Schreibe eine Zusammenfassung dieses Urteils "
        "auf Deutsch, maximal 70 Wörter. Beschreibe knapp: die Rechtsfrage, die Analyse und "
        "das Ergebnis. Nenne die angewandten Artikel im Standardformat (z.B. Art. 53 StGB, "
        "Art. 336 OR). Nur Fließtext, keine Titel, keine Listen."
    ),
    "fr": (
        "Tu es un expert juridique suisse. Rédige un résumé de cet arrêt en français, "
        "maximum 70 mots. Décris brièvement: la question juridique, l'analyse et l'issue. "
        "Mentionne les articles appliqués au format standard (ex. Art. 53 CP, Art. 336 CO). "
        "Texte fluide uniquement, sans titres ni listes."
    ),
}

async def genera_riassunto(testo: str, lang: str, ai: AsyncOpenAI) -> str:
    if not testo:
        return ""
    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=200, temperature=0.2,
            messages=[
                {"role": "system", "content": _SUMMARY_SYSTEM.get(lang, _SUMMARY_SYSTEM["it"])},
                {"role": "user",   "content": testo[:6000]},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("Summary error: %s", exc)
        return ""


# ── Cantonal helpers: Ticino (sentenze.ti.ch) ────────────────────────────────

_TI_OPTIMIZER_SYSTEM = """Sei un esperto di ricerca giuridica del Canton Ticino.
Trasforma la query naturale nella forma ottimale per il motore di sentenze.ti.ch.
Operatori: AND, OR, NOT, virgolette per frasi esatte.
Usa sempre AND per combinare termini.
Tipi: "testo" (concetti), "titolo" (parole chiave precise), "articoli" (solo se c'è art.+numero+codice), "indice".
Rispondi SOLO con JSON: {"query_ottimizzata": "...", "tipo_ricerca": "testo|titolo|articoli|indice", "spiegazione": "..."}"""

async def _ottimizza_query_ti(query: str, ai: AsyncOpenAI) -> tuple[str, str]:
    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=200, temperature=0,
            messages=[
                {"role": "system", "content": _TI_OPTIMIZER_SYSTEM},
                {"role": "user",   "content": f'Query: "{query}"'},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            d = json.loads(m.group())
            return d.get("query_ottimizzata", query), d.get("tipo_ricerca", "testo")
    except Exception as exc:
        log.warning("TI optimizer error: %s", exc)
    return query, "testo"

async def _cerca_sul_portale_ti(
    query: str, tipo: str, limit: int,
    area_filter: Optional[str],
    anno_da: Optional[str], anno_a: Optional[str],
    http: httpx.AsyncClient,
) -> list[dict]:
    """Cerca su sentenze.ti.ch tramite il CGI Omnis Studio."""
    params: dict[str, str] = dict(TI_OMNIS_HIDDEN)
    form_action = TI_PORTAL_CGI
    try:
        form_resp = await http.get(TI_PORTAL_SEARCH, headers=TI_HTTP_HEADERS, timeout=20.0)
        soup_form = BeautifulSoup(form_resp.content.decode("cp1252", errors="replace"), "html.parser")
        form = soup_form.find("form")
        if form:
            action = form.get("action", "")
            if action:
                form_action = action if action.startswith("http") else TI_PORTAL_BASE + action
            for hidden in form.find_all("input", type="hidden"):
                n, v = hidden.get("name",""), hidden.get("value","")
                if n: params[n] = v
            # Checkbox tribunali/area
            if area_filter:
                allowed = set(TI_AREA_CHECKBOXES.get(area_filter, TI_ALL_CHECKBOXES))
            else:
                allowed = TI_ALL_CHECKBOXES
            for cb in form.find_all("input", {"type":"checkbox"}):
                n, v = cb.get("name",""), cb.get("value","")
                if not n or not v: continue
                if n.startswith("bInfoArt_"):
                    if n in allowed: params[n] = v
                else:
                    params[n] = v
    except Exception as exc:
        log.warning("TI form load error: %s — using fallback params", exc)
        for cb_name in (TI_AREA_CHECKBOXES.get(area_filter, []) if area_filter else TI_ALL_CHECKBOXES):
            params[cb_name] = cb_name.replace("bInfoArt_","").rstrip("1")

    params.update({
        "Aufruf": "validate",
        "Template": "results/resultpage_ita.fiw",
        "cSprache": "ITA",
        "cSuchstring": query,
        "cSuchstringZiel": tipo,
        "nSeite": "1",
        "nAnzahlTrefferProSeite": str(max(limit, 10)),
        "cButtonAction": "3. Trova",
    })
    if anno_da: params["cEntscheiddatumVonJahr"] = str(anno_da)
    if anno_a:  params["cEntscheiddatumBisJahr"] = str(anno_a)

    try:
        resp = await http.post(form_action, data=params, headers=TI_HTTP_HEADERS, timeout=30.0)
        html_text = resp.content.decode("cp1252", errors="replace")
        return _parse_results_ti(html_text)[:limit]
    except Exception as exc:
        log.error("TI portal POST failed: %s", exc)
        return []

def _parse_results_ti(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    links = soup.find_all("a", href=lambda h: h and "getMarkupDocument" in h)
    for link_el in links[:10]:
        href = link_el["href"]
        url  = href if href.startswith("http") else TI_PORTAL_BASE + href
        titolo = link_el.get_text(strip=True)
        for ch, rep in [('','‘'),('','’'),('','“'),('','”')]:
            titolo = titolo.replace(ch, rep)
        title_attr = link_el.get("title","")
        incarto_m  = TI_INCARTO_PATTERN.search(title_attr)
        incarto    = incarto_m.group(0) if incarto_m else ""
        parent     = link_el.find_parent("table")
        block_text = parent.get_text(" ", strip=True) if parent else ""
        dates   = TI_DATE_PATTERN.findall(block_text)
        court_m = TI_COURT_PATTERN.search(block_text)
        court   = court_m.group(1) if court_m else ""
        if not incarto:
            im = TI_INCARTO_PATTERN.search(block_text)
            incarto = im.group(0) if im else ""
        anno_str = dates[0][-4:] if dates else ""
        results.append({
            "incarto": incarto,
            "tribunale_abbr": court,
            "tribunale_nome": TI_COURT_NAMES.get(court, court),
            "data": dates[0] if dates else "",
            "anno": int(anno_str) if anno_str.isdigit() else 0,
            "url": url,
            "titolo": titolo,
            "area": _ti_court_to_area(court),
        })
    return results

async def _fetch_full_text_ti(url: str, http: httpx.AsyncClient) -> str:
    """Scarica il testo completo di una sentenza dal portale TI."""
    try:
        resp = await http.get(url, headers=TI_HTTP_HEADERS, timeout=20.0)
        html = resp.content.decode("cp1252", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script","style","nav","header","footer"]):
            tag.decompose()
        body = soup.find("body") or soup
        return body.get_text(" ", strip=True)[:8000]
    except Exception as exc:
        log.warning("TI full text fetch error (%s): %s", url, exc)
        return ""

async def _elabora_risultato_ti(
    hit: dict, rank: int, lang: str,
    ai: Optional[AsyncOpenAI], http: httpx.AsyncClient,
) -> dict:
    testo = await _fetch_full_text_ti(hit["url"], http)
    riass = await genera_riassunto(testo, lang, ai) if (ai and testo) else hit["titolo"]
    art = estrai_articoli_combinati(riass, testo)
    canton_label = f"{TI_COURT_NAMES.get(hit['tribunale_abbr'], hit['tribunale_nome'])} — TI"
    return {
        "rank":      rank,
        "codice":    hit["incarto"] or f"TI-{rank}",
        "tribunale": canton_label,
        "tipo":      "cantonal",
        "area":      hit["area"],
        "data":      hit["data"],
        "anno":      hit["anno"],
        "url":       hit["url"],
        "riassunto": riass,
        "articoli":  art,
    }


# ── OpenCaseLaw helpers ───────────────────────────────────────────────────────

# Mappa cantone → lingua per OCL (usata per pre-filtrare lato OCL)
_CANTON_LANG: dict[str, str] = {
    "ti": "it",
    "ge": "fr", "vd": "fr",
    "zh": "de", "be": "de",
}

async def _ocl_search(
    query: str, limit: int, http: httpx.AsyncClient,
    language: str = "", offset: int = 0,
) -> list[dict]:
    params: dict = {"query": query, "limit": limit}
    if language:
        params["language"] = language
    if offset:
        params["offset"] = offset
    # Timeout ridotto a 8s + un retry automatico in caso di ReadTimeout
    for attempt in range(2):
        try:
            r = await http.get(
                f"{OPENCASELAW_BASE}/decisions",
                params=params,
                timeout=5.0,
            )
            r.raise_for_status()
            d = r.json()
            return d.get("results", d if isinstance(d, list) else [])
        except httpx.ReadTimeout:
            if attempt == 0:
                log.warning("OCL search timeout (attempt 1), retrying: %s", query[:80])
                continue
            log.error("OCL search timeout (attempt 2), giving up: %s", query[:80])
            return []

        except Exception as exc:
            log.error("OCL search error: %s", repr(exc))
            return []
    return []

# ── Entscheidsuche (secondario) ──────────────────────────────────────────────

def _es_normalize(hit: dict) -> dict:
    """Converte un risultato entscheidsuche.ch al formato compatibile con _hit_to_meta."""
    src       = hit.get("_source", {})
    doc_id    = hit.get("_id", "")
    hierarchy = src.get("hierarchy", [])
    # Extract court key from doc_id parts:
    #   CH_BVGE_001_...  → parts[1] = "BVGE"  (federal: use position 1)
    #   GR_VG_001_...    → parts[0] = "GR"    (cantonal: use position 0)
    # hierarchy[0] = "CH" for all federal courts → useless for court identification
    id_parts  = doc_id.split("_")
    if len(id_parts) > 1 and id_parts[0].upper() == "CH":
        court_key = id_parts[1].upper()   # BGER, BVGE, BGE, BSTGER, BPATGER
    else:
        court_key = (id_parts[0] if id_parts else (hierarchy[0] if hierarchy else "")).upper()

    # Mappa gerarchia → nome tribunale leggibile
    _ES_COURT = {
        "BGER": "Tribunale federale", "BGE": "Tribunale federale",
        "BVGER": "Tribunale amministrativo federale", "BVGE": "Tribunale amministrativo federale",
        "BSTGER": "Tribunale penale federale", "BSTG": "Tribunale penale federale",
        "BPATGER": "Tribunale federale dei brevetti",
        "BGE": "Tribunale federale",
        "AG": "Aargau", "BE": "Berna", "BL": "Basilea Campagna",
        "BS": "Basilea Città", "FR": "Friburgo", "GE": "Ginevra",
        "GL": "Glarona", "GR": "Grigioni", "JU": "Giura",
        "LU": "Lucerna", "NE": "Neuchâtel", "NW": "Nidvaldo",
        "OW": "Obvaldo", "SG": "San Gallo", "SH": "Sciaffusa",
        "SO": "Soletta", "SZ": "Svitto", "TG": "Turgovia",
        "TI": "Ticino", "UR": "Uri", "VD": "Vaud",
        "VS": "Vallese", "ZG": "Zugo", "ZH": "Zurigo",
    }
    court_name = _ES_COURT.get(court_key, court_key)

    # ── Docket: estrai dall'_id di entscheidsuche ───────────────────────────
    # Formati _id noti:
    #   CH_BGE_005_BGE-134-III-108_2008   → BGE 134 III 108
    #   CH_BVGER_001_BVGE-2015-48_2015    → BVGE 2015/48
    #   CH_BGER_001_6B-302-2023_2023      → 6B_302/2023
    #   CH_BVGER_007_F-2684-2026_2026     → F-2684/2026  (BVGer lettera-num-anno)
    #   CH_BSTGER_001_SK.2023.1_2023      → SK.2023.1    (BStGer punto-anno-num)
    #   CH_BSTGER_001_SK-2023-1_2023      → SK.2023.1    (stesso, trattini)
    title_it = (src.get("title") or {}).get("it", "")
    title_de = (src.get("title") or {}).get("de", "")
    title    = title_it or title_de or ""

    m_bge    = re.search(r'\b(BGE|ATF)-(\d+)-([IVX]+)-(\d+)',   doc_id, re.I)
    m_bvge   = re.search(r'\b(BVGE)-(\d{4})-(\d+)',             doc_id, re.I)
    m_dkt    = re.search(r'\b([1-9][A-Z]{0,3})-(\d+)-(\d{4})\b', doc_id)        # 6B-302-2023
    m_alpha  = re.search(r'\b([A-Z]{1,3})-(\d{3,6})-(\d{4})\b', doc_id)        # F-2684-2026
    m_bstger = re.search(r'\b([A-Z]{2,4})-(20\d{2})-(\d+)\b',   doc_id)        # SK-2023-1
    m_dot    = re.search(r'\b([A-Z]{2,4})\.(20\d{2})\.(\d+)\b', doc_id)        # SK.2023.1

    if m_bge:
        docket = f"BGE {m_bge.group(2)} {m_bge.group(3)} {m_bge.group(4)}"
    elif m_bvge:
        docket = f"BVGE {m_bvge.group(2)}/{m_bvge.group(3)}"
    elif m_dkt:
        docket = f"{m_dkt.group(1)}_{m_dkt.group(2)}/{m_dkt.group(3)}"
    elif m_dot:
        docket = f"{m_dot.group(1)}.{m_dot.group(2)}.{m_dot.group(3)}"
    elif m_bstger:
        docket = f"{m_bstger.group(1)}.{m_bstger.group(2)}.{m_bstger.group(3)}"
    elif m_alpha:
        docket = f"{m_alpha.group(1)}-{m_alpha.group(2)}/{m_alpha.group(3)}"
    else:
        # Fallback: cerca riferimento nel titolo ES, altrimenti estrai dall'_id
        m_title = re.search(r'BGE\s+(\d+\s+[IVX]+\s+\d+)', title, re.I)
        if m_title:
            docket = "BGE " + m_title.group(1)
        else:
            # Cerca qualsiasi numero di docket nel titolo (es. "SK.2022.45", "6B_302/2023")
            m_title_any = re.search(
                r'\b([A-Z]{1,4}[._-]\d{2,4}[._/]\d+|[1-9][A-Z]{0,3}[._]\d+[/._]\d{4})\b',
                title, re.I,
            )
            if m_title_any:
                docket = m_title_any.group(1)
            else:
                # Ultimo tentativo: 4° segmento dell'_id
                parts = doc_id.split('_')
                raw = parts[3] if len(parts) > 3 else doc_id[:50]
                # Se è solo lettere (camera senza numero), usa l'_id più leggibile
                docket = raw if re.search(r'\d', raw) else doc_id.replace('_', ' ').strip()

    # ── URL: per BGE usa bger.li (HTML pulito garantito);
    #         per gli altri usa content_url dall'indice ES ───────────────────
    attachment = src.get("attachment") or {}
    full_text  = (attachment.get("content") or "").strip()
    abstract   = ((src.get("abstract") or {}).get("it")
               or (src.get("abstract") or {}).get("de")
               or (src.get("abstract") or {}).get("fr") or "")
    if m_bge:
        # BGE → bger.li: URL stabile, HTML leggibile, funziona con /html_federale
        url = costruisci_url_bger(docket)
    else:
        url = attachment.get("content_url", "")
        if not url:
            # Ultimo tentativo: bger.li per tutto il federale (potrebbe non funzionare per BVGer/BStGer)
            url = costruisci_url_bger(docket)

    return {
        "docket_number":  docket,
        "decision_date":  src.get("date", ""),
        "court_name":     court_name,
        "url":            url,
        "relevance_score": min(float(hit.get("_score") or 0) / 100.0, 1.0),
        "citation_count": 0,
        "decision_id":    "",
        "_es_text":       (full_text or abstract)[:50_000],
        "_es_id":         doc_id,
        "_es_score":      float(hit.get("_score") or 0),
    }


_ES_ART_RE = re.compile(
    r'Art\.\s+\d+[a-z]{0,8}(?:\s+(?:cpv\.|Abs\.|al\.)\s*\d+)?\s+[A-Z][A-Za-z]{0,7}\.?\b',
    re.UNICODE,
)

def _score_filter(hits: list[dict], ratio: float = 0.05) -> list[dict]:
    """Scarta risultati il cui score è < ratio * score_massimo. Non ritorna mai lista vuota."""
    if not hits:
        return hits
    max_s = max(h.get("_es_score", 0) for h in hits)
    if max_s <= 0:
        return hits
    filtered = [h for h in hits if h.get("_es_score", 0) >= max_s * ratio]
    return filtered if filtered else hits


def _prepara_query_es(query: str) -> tuple[str, str]:
    """Separa la query in (phrase_block, non_art).

    phrase_block: articoli come phrase queries slop-2 ("Art. 41 CO"~2 | "Art. 41 OR"~2)
    non_art:      resto delle parole (linguaggio naturale)
    """
    art_refs    = _ES_ART_RE.findall(query)
    parts       = _ES_ART_RE.split(query)
    non_art     = " ".join(p.strip() for p in parts if p.strip() and not _ES_ART_RE.fullmatch(p.strip()))
    phrase_block = " | ".join(f'"{ref.strip()}"~2' for ref in art_refs)
    return phrase_block, non_art


async def _entscheidsuche_search(
    query: str, limit: int, http: httpx.AsyncClient, offset: int = 0
) -> list[dict]:
    """Ricerca su entscheidsuche.ch.

    Se la query contiene articoli di legge:
      - fase 1 (strict): bool must=articolo + should=keywords → tutti i risultati contengono l'articolo
      - fase 2 (fallback): solo se fase 1 → 0 risultati, query soft senza filtro articolo
    Se la query non contiene articoli: simple_query_string ordinario.
    """
    phrase_block, non_art = _prepara_query_es(query)

    async def _post(payload: dict) -> list[dict]:
        try:
            r = await http.post(ENTSCHEIDSUCHE_BASE, json=payload, timeout=8.0)
            r.raise_for_status()
            return [_es_normalize(h) for h in r.json().get("hits", {}).get("hits", [])]
        except Exception as exc:
            log.warning("Entscheidsuche search error: %s", repr(exc))
            return []

    base = {"from": offset, "size": limit, "sort": [{"_score": {"order": "desc"}}]}

    if phrase_block:
        # Articolo obbligatorio (must), keywords aumentano score (should)
        bool_q: dict = {"must": [{"simple_query_string": {"query": phrase_block, "default_operator": "or"}}]}
        if non_art:
            bool_q["should"] = [{"simple_query_string": {"query": non_art, "default_operator": "or"}}]
        hits = await _post({**base, "query": {"bool": bool_q}})
        if hits:
            return hits
        # Fallback: nessun risultato con l'articolo, ricerca soft
        log.info("No results with article filter, falling back to soft query")
        soft = f"{non_art} {phrase_block}".strip() if non_art else phrase_block
        return await _post({**base, "query": {"simple_query_string": {"query": soft, "default_operator": "or"}}})

    # Nessun articolo: OR per massimizzare i candidati — il re-rank AI seleziona i rilevanti
    return await _post({**base, "query": {"simple_query_string": {"query": query, "default_operator": "or"}}})


def _merge_hits(ocl: list[dict], es: list[dict]) -> list[dict]:
    """Unisce OCL (primario) + entscheidsuche (secondario), deduplicando per docket."""
    seen: set[str] = set()
    merged: list[dict] = []
    for h in ocl:
        key = (h.get("docket_number") or h.get("file_number") or "").lower().strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        merged.append(h)
    for h in es:
        key = (h.get("docket_number") or "").lower().strip()
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        merged.append(h)
    return merged


def _rerank(hits: list[dict]) -> list[dict]:
    """Re-ordina i risultati OCL per: relevance_score × log(1 + citation_count).
    Le sentenze molto citate (BGE) salgono rispetto a sentenze irrilevanti con
    score simile ma zero citazioni."""
    def _score(h: dict) -> float:
        rel = float(h.get("relevance_score") or 0)
        cit = int(h.get("citation_count") or 0)
        return rel * math.log1p(cit + 1)
    return sorted(hits, key=_score, reverse=True)


async def _ai_rerank(hits: list[dict], query: str, ai: AsyncOpenAI) -> list[dict]:
    """Riordina i risultati per rilevanza rispetto alla query originale dell'utente.
    Una singola chiamata GPT-4o-mini con estratti di tutti i risultati.
    In caso di errore restituisce l'ordine originale invariato."""
    if len(hits) <= 2 or not ai:
        return hits
    items = []
    for i, h in enumerate(hits):
        excerpt = (h.get("_es_text") or "")[:500].replace("\n", " ").strip()
        docket  = h.get("docket_number") or h.get("codice") or f"#{i}"
        court   = h.get("court_name") or h.get("tribunale") or ""
        items.append(f"[{i}] {docket} ({court}): {excerpt}")
    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=60,
            temperature=0,
            messages=[
                {"role": "system", "content":
                    "Sei un esperto giuridico svizzero. Valuta la rilevanza di estratti di sentenze "
                    "rispetto alla query. Rispondi SOLO con gli indici in ordine di rilevanza decrescente, "
                    "separati da virgola. Esempio: '2,0,4,1,3'. Nient'altro."},
                {"role": "user", "content": f'Query: "{query}"\n\n' + "\n\n".join(items)},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        indices = [int(x.strip()) for x in raw.split(",") if x.strip().isdigit()]
        seen, reordered = set(), []
        for idx in indices:
            if 0 <= idx < len(hits) and idx not in seen:
                seen.add(idx)
                reordered.append(hits[idx])
        for i, h in enumerate(hits):
            if i not in seen:
                reordered.append(h)
        return reordered
    except Exception as exc:
        log.warning("AI rerank error: %s", exc)
        return hits


async def _ocl_full_text(decision_id: str, http: httpx.AsyncClient) -> str:
    if not decision_id:
        return ""
    try:
        r = await http.get(
            f"{OPENCASELAW_BASE}/decisions/{decision_id}",
            params={"full_text": "true"},
            timeout=20.0,
        )
        r.raise_for_status()
        d = r.json()
        return (d.get("full_text") or d.get("text") or "").strip()
    except Exception as exc:
        log.warning("OCL full text error (%s): %s", decision_id, exc)
        return ""

def _hit_to_meta(hit: dict, rank: int) -> dict:
    """Estrae i metadati di un risultato OCL."""
    docket    = hit.get("docket_number") or hit.get("file_number") or hit.get("id") or "—"
    data_raw  = hit.get("decision_date") or hit.get("date") or ""
    data_fmt  = formatta_data(data_raw)
    anno_m    = re.search(r'\d{4}', data_raw)
    anno      = int(anno_m.group()) if anno_m else 0
    court_raw = hit.get("court_name") or hit.get("court") or "BGer"
    # Usa l'URL fornito da OCL se disponibile, altrimenti costruisce bger.li
    url_ocl   = hit.get("url") or hit.get("source_url") or hit.get("decision_url") or ""
    url_final = url_ocl if url_ocl.startswith("http") else costruisci_url_bger(docket)
    tipo = _rileva_tipo(court_raw)
    return {
        "rank":           rank,
        "codice":         docket,
        "tribunale":      normalizza_tribunale(court_raw),
        "tipo":           tipo,
        "tribunal_key":   _rileva_tribunal(court_raw),
        "canton_key":     _rileva_cantone(court_raw),
        "camera":         hit.get("division") or hit.get("department") or hit.get("chamber") or "",
        "area":           rileva_area(docket),
        "data":           data_fmt,
        "anno":           anno,
        "url":            url_final,
        "decision_id":    hit.get("decision_id", ""),
        "citation_count": int(hit.get("citation_count") or 0),
    }

async def _elabora_risultato(
    hit: dict, rank: int, lang: str,
    ai: Optional[AsyncOpenAI], http: httpx.AsyncClient,
) -> dict:
    """Scarica il testo completo e genera il riassunto per un singolo risultato."""
    meta  = _hit_to_meta(hit, rank)
    # Se il testo è già incluso (entscheidsuche), non serve un secondo fetch
    es_text = hit.get("_es_text") or ""
    if es_text:
        testo = es_text
    elif meta["decision_id"]:
        testo = await _ocl_full_text(meta["decision_id"], http)
    else:
        # ES hit senza testo precaricato: scarica direttamente da bger.li
        testo = await _fetch_bger_text(meta["url"], http) if meta.get("url") else ""
    riass = await genera_riassunto(testo, lang, ai) if (ai and testo) else ""
    # Articoli: estratti dal riassunto AI (coerente col testo mostrato) → fallback testo grezzo
    art = estrai_articoli_combinati(riass, testo)
    return {**meta, "riassunto": riass, "articoli": art}


# ── /cerca  (JSON bloccante) ─────────────────────────────────────────────────

@app.get("/cerca")
async def cerca(
    query:   str           = Query(..., min_length=1, max_length=400),
    lang:    str           = Query("it"),
    limit:   int           = Query(8, ge=1, le=20),
    anno_da: Optional[str] = Query(None),
    anno_a:  Optional[str] = Query(None),
):
    """Ricerca federale via OpenCaseLaw. Restituisce tutti i risultati in una sola risposta JSON."""
    api_key = os.environ.get("OPENAI_API_KEY", "")
    lang    = lang if lang in ("it", "de", "fr") else "it"
    ai      = AsyncOpenAI(api_key=api_key) if api_key else None

    async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:
        query_opt, spiegazione = await ottimizza_query(query, ai, http) if ai else (query, "")
        log.info("cerca: '%s' → '%s' | lang=%s limit=%d", query, query_opt, lang, limit)

        hits = await _ocl_search(query_opt, limit * 2, http)

        # Filtro anno
        if anno_da or anno_a:
            def _ok(h: dict) -> bool:
                m = re.search(r'\d{4}', h.get("decision_date", "") or "")
                if not m:
                    return True
                y = int(m.group())
                return (not anno_da or y >= int(anno_da)) and (not anno_a or y <= int(anno_a))
            hits = [h for h in hits if _ok(h)]

        hits = _rerank(hits)[:limit]
        if not hits:
            return JSONResponse({
                "risultati": [], "query_originale": query,
                "query_ottimizzata": query_opt, "spiegazione": spiegazione,
                "totale": 0, "errore": "Nessun risultato trovato.",
            })

        # Elabora tutti in parallelo
        tasks = [
            asyncio.create_task(_elabora_risultato(h, i + 1, lang, ai, http))
            for i, h in enumerate(hits)
        ]
        risultati = await asyncio.gather(*tasks)

    return JSONResponse({
        "risultati": list(risultati),
        "query_originale":  query,
        "query_ottimizzata": query_opt,
        "spiegazione":      spiegazione,
        "totale":           len(risultati),
    })


# ── /cerca_stream  (SSE) ──────────────────────────────────────────────────────

@app.get("/cerca_stream")
async def cerca_stream(
    query:    str           = Query(..., min_length=1, max_length=400),
    lang:     str           = Query("it"),
    limit:    int           = Query(8, ge=1, le=20),
    anno_da:  Optional[str] = Query(None),
    anno_a:   Optional[str] = Query(None),
    area:     Optional[str] = Query(None),   # penale | civile | pubblico | sociale
    tipo:     Optional[str] = Query(None),   # federal | cantonal
    tribunal: Optional[str] = Query(None),   # bger | bvger | bstger
    canton:   Optional[str] = Query(None),   # ti | zh | be | ge | vd
    chamber:  Optional[str] = Query(None),   # nome sezione (filtro frontend)
    offset:   int           = Query(0, ge=0),# paginazione OCL
):
    """
    Ricerca via SSE con supporto filtri area e tipo tribunale.

    Formato eventi:
      data: {"type": "status",  "message": "..."}
      data: {"type": "result",  "rank": 1, "codice": "...", ...}
      data: {"type": "done",    "totale": N}
      data: {"type": "error",   "message": "..."}
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    lang    = lang if lang in ("it", "de", "fr") else "it"
    ai      = AsyncOpenAI(api_key=api_key) if api_key else None

    # Normalizza filtri
    area_filter     = area.lower().strip()     if area     else None
    tipo_filter     = tipo.lower().strip()     if tipo     else None
    tribunal_filter = tribunal.lower().strip() if tribunal else None   # bger | bvger | bstger
    canton_filter   = canton.lower().strip()   if canton   else None   # ti | zh | be | ge | vd

    async def generator() -> AsyncIterator[str]:
        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as http:

                yield sse({"type": "status", "message": "Ottimizzazione query..."})
                if ai:
                    query_opt, spiegazione = await ottimizza_query(query, ai, http)
                else:
                    query_opt, spiegazione = query, ""
                yield sse({"type": "status", "message": f"Ricerca: {query_opt}"})

                fetch_limit = 80
                ocl_lang = _CANTON_LANG.get(canton_filter, "") if canton_filter else ""

                # Usa sempre la query trilingue completa per ES — il rank non deve dipendere
                # dalla lingua. Il filtro CH_ è applicato dopo sul pool di 80 risultati.
                es_hits = await _entscheidsuche_search(query_opt, 80, http, offset=0)
                log.info("ES returned %d hits for query: %s", len(es_hits), query_opt[:80])

                # Filtro CH_: unico filtro court (il secondo filtro _rileva_tipo era ridondante
                # per ES ed è stato rimosso — OCL è disabilitato)
                if tipo_filter == "cantonal":
                    hits = [h for h in es_hits if not (h.get("_es_id") or "").startswith("CH_")]
                elif tipo_filter == "all":
                    hits = list(es_hits)
                else:  # federal o None
                    hits = [h for h in es_hits if (h.get("_es_id") or "").startswith("CH_")]

                log.info("After court filter tipo=%s: %d hits remain", tipo_filter, len(hits))

                # Filtro anno
                if anno_da or anno_a:
                    def _ok_anno(h: dict) -> bool:
                        m = re.search(r'\d{4}', h.get("decision_date", "") or "")
                        if not m: return True
                        y = int(m.group())
                        return (not anno_da or y >= int(anno_da)) and (not anno_a or y <= int(anno_a))
                    hits = [h for h in hits if _ok_anno(h)]

                # Filtro area giuridica
                if area_filter:
                    hits = [h for h in hits
                            if rileva_area(h.get("docket_number") or h.get("file_number") or "") == area_filter]

                # Helper comune per court_name
                def _court(h: dict) -> str:
                    return h.get("court_name") or h.get("court") or ""

                # Filtro tribunale specifico (bger / bvger / bstger)
                if tribunal_filter:
                    hits = [h for h in hits if _rileva_tribunal(_court(h)) == tribunal_filter]

                # Filtro cantone specifico
                if canton_filter:
                    hits = [h for h in hits if _rileva_cantone(_court(h)) == canton_filter]

                # Score filter: scarta risultati molto sotto il massimo (applicato dopo tutti i filtri)
                hits = _score_filter(hits)

                # Re-rank: relevance_score × log(1 + citation_count) — BGE citate salgono
                hits = _rerank(hits)[offset:offset + limit]

                # AI re-rank: riordina per rilevanza rispetto alla query originale dell'utente
                hits = await _ai_rerank(hits, query, ai)

                if not hits:
                    if canton_filter:
                        msg = f"Nessuna sentenza trovata per il cantone '{canton_filter.upper()}' su OpenCaseLaw."
                    elif tipo_filter == "federal":
                        msg = "Nessuna sentenza federale trovata per questa ricerca."
                    elif tipo_filter == "cantonal":
                        msg = "Nessuna sentenza cantonale trovata per questa ricerca."
                    elif tribunal_filter:
                        msg = f"Nessuna sentenza trovata per il tribunale '{tribunal_filter.upper()}'."
                    elif area_filter:
                        msg = f"Nessuna sentenza trovata nell'area '{area_filter}'."
                    else:
                        msg = "Nessun risultato trovato."
                    yield sse({"type": "error", "message": msg})
                    return

                yield sse({"type": "status",
                           "message": f"Trovate {len(hits)} sentenze — generazione riassunti..."})

                tasks = [
                    asyncio.create_task(_elabora_risultato(h, i + 1, lang, ai, http))
                    for i, h in enumerate(hits)
                ]
                for task in tasks:
                    risultato = await task
                    yield sse({"type": "result", **risultato})
                yield sse({"type": "done", "totale": len(hits),
                           "query_ottimizzata": query_opt, "spiegazione": spiegazione})

        except Exception as exc:
            log.error("SSE stream error: %s", exc)
            yield sse({"type": "error", "message": str(exc)})

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disabilita il buffering in Nginx/Render
        },
    )


# ── /sintesi_federal  (+ alias /sintesi) ─────────────────────────────────────

_SINTESI_SYSTEM = {
    "it": (
        "Sei un avvocato svizzero con profonda competenza in diritto federale. "
        "Produci analisi giuridiche professionali, strutturate e complete di sentenze del Tribunale federale. "
        "Il tuo linguaggio è tecnico, preciso e adatto a professionisti del diritto."
    ),
    "de": (
        "Du bist ein Schweizer Rechtsanwalt mit fundierter Expertise im Bundesrecht. "
        "Du erstellst professionelle, strukturierte und vollständige rechtliche Analysen von Bundesgerichtsurteilen. "
        "Deine Sprache ist technisch, präzise und für Rechtsfachleute geeignet."
    ),
    "fr": (
        "Vous êtes un avocat suisse avec une profonde expertise en droit fédéral. "
        "Vous produisez des analyses juridiques professionnelles, structurées et complètes des arrêts du Tribunal fédéral. "
        "Votre langage est technique, précis et adapté aux professionnels du droit."
    ),
}

_SINTESI_USER = {
    "it": (
        "Analizza in modo completo e professionale la seguente sentenza del Tribunale federale svizzero. "
        "Struttura la tua analisi esattamente come segue:\n\n"
        "**1. Fattispecie**\n"
        "In 2-3 frasi: parti coinvolte, questione giuridica centrale e iter procedurale.\n\n"
        "**2. Articoli principali applicati**\n"
        "Elenca in modo puntuale tutti gli articoli di legge citati o applicati (indicando codice e numero, es. art. 41 CO, art. 146 CP).\n\n"
        "**3. Considerazioni del Tribunale**\n"
        "Esponi il ragionamento giuridico adottato dalla corte: interpretazione normativa, bilanciamento degli interessi, "
        "giurisprudenza richiamata e argomenti decisivi.\n\n"
        "**4. Dispositivo e implicazioni**\n"
        "In 2 frasi: esito del giudizio (accoglimento/rigetto/rinvio) e principale implicazione pratica.\n\n"
        "Testo della sentenza:\n{testo}"
    ),
    "de": (
        "Analysiere das folgende Urteil des Schweizer Bundesgerichts vollständig und professionell. "
        "Strukturiere deine Analyse genau wie folgt:\n\n"
        "**1. Sachverhalt**\n"
        "In 2-3 Sätzen: beteiligte Parteien, zentrale Rechtsfrage und Verfahrensgang.\n\n"
        "**2. Massgebende Rechtsartikel**\n"
        "Liste alle zitierten oder angewendeten Gesetzesartikel auf (mit Angabe des Gesetzes und Nummer, z.B. Art. 41 OR).\n\n"
        "**3. Erwägungen des Gerichts**\n"
        "Stelle die rechtliche Argumentation des Gerichts dar: Normeninterpretation, Interessenabwägung, "
        "herangezogene Rechtsprechung und entscheidende Argumente.\n\n"
        "**4. Dispositiv und Implikationen**\n"
        "In 2 Sätzen: Urteilsergebnis (Gutheissung/Abweisung/Rückweisung) und wichtigste praktische Implikation.\n\n"
        "Urteilstext:\n{testo}"
    ),
    "fr": (
        "Analysez de manière complète et professionnelle l'arrêt du Tribunal fédéral suisse ci-dessous. "
        "Structurez votre analyse exactement comme suit:\n\n"
        "**1. Faits et procédure**\n"
        "En 2-3 phrases: parties impliquées, question juridique centrale et déroulement de la procédure.\n\n"
        "**2. Articles principaux appliqués**\n"
        "Listez tous les articles de loi cités ou appliqués (avec indication du code et du numéro, ex. art. 41 CO).\n\n"
        "**3. Considérants du Tribunal**\n"
        "Exposez le raisonnement juridique: interprétation normative, pesée des intérêts, "
        "jurisprudence citée et arguments décisifs.\n\n"
        "**4. Dispositif et implications**\n"
        "En 2 phrases: issue du jugement (admission/rejet/renvoi) et principale implication pratique.\n\n"
        "Texte de l'arrêt:\n{testo}"
    ),
}

_VALID_BGER_DOMAINS = {
    "bger.li", "www.bger.li",
    "bger.ch", "www.bger.ch",
    "entscheidsuche.ch", "www.entscheidsuche.ch",
    "relevancy.ch", "www.relevancy.ch",
}

async def _fetch_bger_text(url: str, http: httpx.AsyncClient) -> str:
    """Scarica il testo grezzo di una sentenza da bger.li.
    Ritorna stringa vuota se URL non trovato, redirect loop, o dominio finale non valido."""
    try:
        # Client dedicato con limite redirect: max_redirects è parametro del client, non di .get()
        async with httpx.AsyncClient(
            headers=HTTP_HEADERS,
            follow_redirects=True,
            max_redirects=5,
        ) as bger_client:
            resp = await bger_client.get(url, timeout=25.0)
        if resp.status_code in (404, 410):
            return ""
        resp.raise_for_status()
        # Salta i PDF (binario non parsabile come HTML)
        ct = resp.headers.get("content-type", "")
        if "pdf" in ct or resp.content[:5] == b"%PDF-":
            log.info("Skipping PDF content for %s", url)
            return ""
        # Verifica che il redirect finale porti a un dominio svizzero noto
        final_host = str(resp.url.host).lower()
        if final_host not in _VALID_BGER_DOMAINS:
            log.warning("bger.li redirect to unexpected domain '%s' for %s", final_host, url)
            return ""
    except httpx.TooManyRedirects:
        log.warning("bger.li redirect loop (%s): codice non valido o non trovato", url)
        return ""
    except httpx.HTTPStatusError:
        return ""
    except Exception as exc:
        log.warning("bger.li fetch error (%s): %s", url, repr(exc))
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup.find_all(["nav", "header", "footer", "script", "style",
                              "form", "noscript", "iframe", "aside"]):
        tag.decompose()
    content = (
        soup.find("div", id=re.compile(r"content|main|document|decision", re.I))
        or soup.find("article")
        or soup.find("main")
        or soup.find("body")
    )
    text = (content or soup).get_text(" ", strip=True)
    return text[:8000] if len(text) >= 100 else ""


async def _sintesi_impl(codice: str, lang: str, decision_id: str = "") -> JSONResponse:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return JSONResponse({"errore": "OPENAI_API_KEY non configurata."}, status_code=500)
    lang = lang if lang in ("it", "de", "fr") else "it"

    source = "bger.li"
    async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:
        hit: dict = {}
        full_text = ""

        # ── 1. bger.li: accesso diretto tramite URL costruita dal codice ────────
        bger_url = costruisci_url_bger(codice)
        full_text = await _fetch_bger_text(bger_url, http)
        if full_text:
            hit = {"docket_number": codice, "court_name": "BGer", "decision_date": "", "url": bger_url}
            log.info("bger.li OK for '%s': %s", codice, bger_url)
        else:
            # ── 2. Fallback ES: cerca su entscheidsuche per numero di ruolo ────
            log.info("bger.li miss for '%s', trying entscheidsuche", codice)
            source = "entscheidsuche.ch"
            norm_input = _normalize_codice(codice)
            # Normalizza separatori per ES: "80-2017-7" → "80.2017.7"
            # ES interpreta "-" come negazione in simple_query_string
            codice_es = re.sub(r'(\d+)[-_](\d{4})[-_](\d+)', r'\1.\2.\3', codice.strip())
            es_hits = await _entscheidsuche_search(codice_es, 10, http)
            # Preferisce match esatto sul docket number, altrimenti il primo risultato
            exact = [h for h in es_hits
                     if _normalize_codice(h.get("docket_number") or "") == norm_input]
            best = exact[0] if exact else None
            if best:
                full_text = best.get("_es_text") or ""
                if not full_text and best.get("url"):
                    full_text = await _fetch_bger_text(best["url"], http)
                hit = best
                log.info("ES fallback OK for '%s': docket=%s", codice, best.get("docket_number"))
            else:
                return JSONResponse(
                    {"errore": f"Sentenza '{codice}' non trovata. Verifica il codice e riprova."},
                    status_code=404,
                )

    if len(full_text) < 100:
        return JSONResponse({"errore": "Testo non disponibile."}, status_code=404)

    ai = AsyncOpenAI(api_key=api_key, timeout=90.0)
    try:
        msg = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=1400,
            messages=[
                {"role": "system", "content": _SINTESI_SYSTEM.get(lang, _SINTESI_SYSTEM["it"])},
                {"role": "user",   "content": _SINTESI_USER[lang].format(testo=full_text[:8000])},
            ],
        )
        sintesi = msg.choices[0].message.content
    except Exception as exc:
        return JSONResponse({"errore": f"Errore AI: {exc}"}, status_code=500)

    court_raw = hit.get("court_name") or hit.get("court") or "BGer"
    return JSONResponse({
        "sintesi":       sintesi,
        "data_sentenza": formatta_data(hit.get("decision_date", "")),
        "codice":        hit.get("docket_number", codice),
        "tribunale":     normalizza_tribunale(court_raw),
        "lingua_orig":   hit.get("language", ""),
        "source":        source,
        "statutes":      hit.get("statutes") or [],
        "decision_id":   hit.get("decision_id", decision_id),
        "url":           hit.get("url", ""),
    })

@app.get("/sintesi_federal")
async def sintesi_federal(
    codice:      str           = Query(..., min_length=1, max_length=80, description="Codice sentenza (es. 6B_51/2021)"),
    lang:        str           = Query("it"),
    decision_id: Optional[str] = Query(None),
):
    """Riassunto AI strutturato di una sentenza. Accetta decision_id OCL per accesso diretto."""
    return await _sintesi_impl(codice, lang, decision_id or "")

@app.get("/sintesi")
async def sintesi(
    codice:      str           = Query(..., min_length=1, max_length=80),
    lang:        str           = Query("it"),
    decision_id: Optional[str] = Query(None),
):
    """Alias di /sintesi_federal (compatibilità con sententia-api-3.onrender.com)."""
    return await _sintesi_impl(codice, lang, decision_id or "")


# ── /articolo_ocl  (testo articolo via OCL /laws — sostituisce fedlex-connector) ──

@app.get("/articolo_ocl")
async def articolo_ocl(
    sigla: str = Query(..., description="Abbreviazione legge (es. OR, SVG, LPP)"),
    art:   str = Query(..., description="Numero articolo (es. 41, 16a)"),
    lang:  str = Query("it"),
):
    """
    Testo di un articolo di legge federale svizzera via OpenCaseLaw /laws.
    Accetta qualsiasi sigla nelle tre lingue nazionali (OR/CO/CO, SVG/LCR/LCStr, ecc.)
    senza bisogno di una mappa RS statica.
    """
    lang = lang if lang in ("it", "de", "fr") else "it"
    async with httpx.AsyncClient(timeout=12.0) as client:
        try:
            r = await client.get(
                f"{OPENCASELAW_BASE}/laws/{sigla}",
                params={"article": art, "language": lang},
            )
            r.raise_for_status()
            d = r.json()
        except Exception as exc:
            return JSONResponse({"errore": f"OCL laws error: {exc}"}, status_code=502)

    articles = d.get("articles") or []
    if not articles:
        return JSONResponse({"errore": "Articolo non trovato."}, status_code=404)

    testo = articles[0].get("text") or ""
    if not testo:
        return JSONResponse({"errore": "Testo non disponibile."}, status_code=404)

    return JSONResponse({
        "testo":              testo,
        "sigla":              sigla,
        "art":                art,
        "sr_number":          d.get("sr_number", ""),
        "consolidation_date": d.get("consolidation_date", ""),
        "titolo_legge":       d.get("title", ""),
        "abbreviazione":      d.get("abbreviation", ""),
        "lang":               lang,
    })


# ── /articolo_fedlex  (mantenuto per compatibilità, ora delega a /articolo_ocl) ─

@app.get("/articolo_fedlex")
async def articolo_fedlex(
    rs:   str = Query(..., description="Numero RS (es. 311.0) — ora ignorato, usa /articolo_ocl"),
    art:  str = Query(..., description="Numero articolo (es. 53)"),
    lang: str = Query("it"),
):
    """Alias di compatibilità: usa /articolo_ocl con SR number come sigla."""
    return await articolo_ocl(sigla=rs, art=art, lang=lang)


# ── /testo_decisione  (full text OCL via decision_id) ────────────────────────

@app.get("/testo_decisione")
async def testo_decisione(
    decision_id: str = Query(..., description="OCL decision_id (es. bge_BGE_134_III_108)"),
):
    """Restituisce il testo completo di una sentenza OCL formattato come HTML leggibile."""
    async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:
        testo = await _ocl_full_text(decision_id, http)
    if not testo or len(testo) < 50:
        return JSONResponse({"errore": "Testo non disponibile."}, status_code=404)

    # Converti plain text in HTML strutturato
    html_parts: list[str] = []
    for block in testo.split('\n\n'):
        block = block.strip()
        if not block:
            continue
        lines = block.split('\n')
        # Titoletto: riga singola corta senza punto finale → <h3>
        if len(lines) == 1 and len(block) < 120 and not block.endswith('.'):
            html_parts.append(f"<h3>{block}</h3>")
        else:
            inner = "<br>".join(line.strip() for line in lines if line.strip())
            html_parts.append(f"<p>{inner}</p>")

    return JSONResponse({"html": "\n".join(html_parts), "decision_id": decision_id})


# ── /html_federale ────────────────────────────────────────────────────────────

@app.get("/html_federale")
async def html_federale(
    url: str = Query(..., description="URL sentenza (es. https://bger.li/6B_51-2021)"),
):
    """Scarica e restituisce l'HTML pulito di una sentenza federale da bger.li."""
    async with httpx.AsyncClient(
        timeout=20.0, follow_redirects=True, headers=HTTP_HEADERS
    ) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as exc:
            return JSONResponse({"errore": f"Download fallito: {exc}"}, status_code=502)

    # Rileva PDF: restituisce errore con link diretto invece di binario grezzo
    ct = resp.headers.get("content-type", "")
    if "pdf" in ct or resp.content[:5] == b"%PDF-":
        return JSONResponse({"errore": "pdf", "pdf_url": url}, status_code=200)

    soup = BeautifulSoup(resp.text, "html.parser")
    for tag in soup.find_all(["nav", "header", "footer", "script", "style",
                              "form", "noscript", "iframe", "aside"]):
        tag.decompose()

    content = (
        soup.find("div", id=re.compile(r"content|main|document|decision", re.I))
        or soup.find("article")
        or soup.find("main")
        or soup.find("body")
    )
    html_out = str(content or soup)

    if len(html_out) < 100:
        return JSONResponse({"errore": "Contenuto non trovato."}, status_code=400)

    return JSONResponse({"html": html_out, "url": url})


# ── /feedback ─────────────────────────────────────────────────────────────────

class FeedbackPayload(BaseModel):
    message: str
    email: Optional[str] = None
    query: Optional[str] = None
    result_code: Optional[str] = None

@app.post("/feedback")
async def feedback(payload: FeedbackPayload):
    smtp_pass = os.environ.get("SMTP_PASS", "")
    if not smtp_pass:
        log.warning("SMTP_PASS non impostato — feedback non inviato: %s", payload.message)
        return JSONResponse({"ok": True, "note": "logged only"})

    body_lines = [
        f"Messaggio: {payload.message}",
        f"Email utente: {payload.email or '—'}",
        f"Query: {payload.query or '—'}",
        f"Sentenza: {payload.result_code or '—'}",
    ]
    body = "\n".join(body_lines)

    msg = MIMEMultipart()
    msg["From"]    = "sententiaki@gmail.com"
    msg["To"]      = "sententiaki@gmail.com"
    msg["Subject"] = f"[Sententia feedback] {(payload.query or payload.result_code or 'senza query')[:60]}"
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as s:
            s.ehlo()
            s.starttls()
            s.login("sententiaki@gmail.com", smtp_pass)
            s.sendmail("sententiaki@gmail.com", "sententiaki@gmail.com", msg.as_string())
        log.info("Feedback inviato via email")
    except Exception as exc:
        log.error("Errore invio feedback email: %s", exc)
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)

    return JSONResponse({"ok": True})


# ── /health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status":        "ok",
        "opencaselaw":   OPENCASELAW_BASE,
        "openai_key_set": bool(os.environ.get("OPENAI_API_KEY")),
    }


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, reload=True)
