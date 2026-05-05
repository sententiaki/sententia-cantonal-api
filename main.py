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
from typing import AsyncIterator, Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from openai import AsyncOpenAI

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="Sententia Search API", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ── Constants ─────────────────────────────────────────────────────────────────
OPENCASELAW_BASE = "https://mcp.opencaselaw.ch/api"

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
    "bundesverwaltungsgericht", "tribunal administratif fédéral",
    "bundesstrafgericht", "tribunal pénal fédéral",
    "bundespatentgericht", "bger", "bvger", "bstger", "bpatger",
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
                                  "tribunale amministrativo federale", "bvger")):
        return "bvger"
    if any(kw in key for kw in ("bundesstrafgericht", "tribunal pénal fédéral",
                                  "tribunale penale federale", "bstger")):
        return "bstger"
    if any(kw in key for kw in ("bundespatentgericht", "bpatger")):
        return "bpatger"
    if any(kw in key for kw in ("bundesgericht", "tribunal fédéral", "tribunale federale",
                                  "federal supreme court", "bger", "bge")):
        return "bger"
    return ""

# Parole chiave per riconoscere il cantone dal nome del tribunale
_CANTON_KEYWORDS: dict[str, list[str]] = {
    "ti": ["ticino"],
    "zh": ["zürich", "zurich"],
    "be": ["bern", "berne"],
    "ge": ["genf", "genève", "geneve", "canton de genève", "kanton genf"],
    "vd": ["vaud", "waadt"],
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
}

# Regex per l'estrazione degli articoli dal testo
# Pattern principale: "Art. 53 CP" / "art 336 CO" / "Art. 8a CC"
_CODES = (
    "CP|StGB|CO|OR|CPP|StPO|LTF|BGG|BV|Cost\\.|Cst\\.|ZPO|CPC"
    "|LPD|DSG|CC|ZGB|LAMal|KVG|LAINF|LAA|LAVS|AHVG|LAI|IVG"
    "|LEF|SchKG|LIFD|DBG|LIVA|MWSTG|LCD|UWG|LCart|KG|LPM|MSchG"
    "|LFus|FusG|LDIP|IPRG|PA|VwVG"
)
# Cattura: "Art.? N [cpv/abs/al/lett N] CODE"  oppure  "articolo N CODE"
ARTICLE_RE = re.compile(
    r'(?:[Aa]rt(?:icol[oi])?\.?\s+)(\d+[a-z]?)'
    r'(?:\s+(?:cpv|abs|al|lett?|lit)\.?\s*\d+)?'
    r'(?:\s+(?:del|della|des?|von|du|de\s+la))?'
    r'\s+(' + _CODES + r')\b',
    re.UNICODE,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def estrai_articoli(testo: str, max_art: int = 6) -> list[str]:
    seen, result = set(), []
    for num, code in ARTICLE_RE.findall(testo):
        code_norm = LAW_ALIASES.get(code.upper().rstrip('.'), code)
        label = f"Art. {num} {code_norm}"
        if label not in seen:
            seen.add(label)
            result.append(label)
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
    r'\b(articol[oi]|article|artikel|art)\.?\s*(\d+[a-z]?)',
    re.IGNORECASE | re.UNICODE,
)
# Rimuove preposizioni residue: "Art. 111 del CP" → "Art. 111 CP"
_QUERY_DEL_RE = re.compile(
    r'(Art\.\s+\d+[a-z]?(?:\s+(?:cpv|abs|al|Abs)\.?\s*\d+)?)\s+'
    r'(?:del|della|der|des|de\s+la|du|von|di)\s+',
    re.IGNORECASE,
)
# Riordina "CODICE Art. NNN" → "Art. NNN CODICE" (es. "CO Art. 97" → "Art. 97 CO")
_QUERY_CODE_BEFORE_ART_RE = re.compile(
    r'\b(CP|CC|CO|CPC|CPP|LTF|BGG|BV|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG)\s+(Art\.\s+\d+[a-z]?)',
    re.IGNORECASE,
)
# Mappa codici → forma canonica (gestisce maiuscole/minuscole)
_CODE_CANON: dict[str, str] = {
    "cp": "CP", "cc": "CC", "co": "CO", "cpc": "CPC", "cpp": "CPP",
    "ltf": "LTF", "bgg": "BGG", "bv": "BV", "bg": "BG", "cst.": "Cst.",
    "stgb": "StGB", "zgb": "ZGB", "or": "OR", "zpo": "ZPO",
    "stpo": "StPO", "lpd": "LPD", "dsg": "DSG", "lef": "LEF",
    "schkg": "SchKG", "dbg": "DBG", "lifd": "LIFD",
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
    r'(\d+[a-z]?)'
    r'(\s+(?:cpv\.?|Abs\.?|al\.?)\s*\d+)?'
    r'\s+(CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG)\b',
    re.UNICODE | re.IGNORECASE,
)
# "CODE N" senza prefisso Art.  (es. "OR 50", "StGB 111", "or 50")
_CODE_BARE_NUM_RE = re.compile(
    r'\b(CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG)\s+(\d+[a-z]?)\b',
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
        r'\b(cp|cc|co|cpc|cpp|ltf|bgg|bv|stgb|zgb|or|zpo|stpo|lpd|dsg|lef|schkg|dbg|lifd)\b',
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
        r'\b(cpv\.|Abs\.|al\.)\s*(\d+)\s+(Art\.\s+\d+[a-z]?)',
        lambda m: f"{m.group(3)} {m.group(1)} {m.group(2)}",
        result, flags=re.IGNORECASE,
    )
    # 8. Rimuove preposizioni tra numero articolo e codice
    result = _QUERY_DEL_RE.sub(r'\1 ', result)
    # 9. Riordina "CODICE Art. NNN" → "Art. NNN CODICE"
    result = _QUERY_CODE_BEFORE_ART_RE.sub(lambda m: f"{m.group(2)} {m.group(1)}", result)
    return result.strip()


# Equivalenze tra sigle di legge nelle tre lingue nazionali (IT/FR ↔ DE)
_CODE_EQUIVALENTS: dict[str, list[str]] = {
    "CP":    ["StGB"],          # Codice penale = Strafgesetzbuch
    "CC":    ["ZGB"],           # Codice civile = Zivilgesetzbuch
    "CO":    ["OR"],            # Codice delle obbligazioni = Obligationenrecht
    "CPC":   ["ZPO"],           # Codice proc. civile = Zivilprozessordnung
    "CPP":   ["StPO"],          # Codice proc. penale = Strafprozessordnung
    "LTF":   ["BGG"],           # Legge Trib. federale = Bundesgerichtsgesetz
    "Cost.": ["BV"],             # Costituzione federale IT → DE
    "Cst.":  ["BV"],             # Costituzione federale FR → DE
    "LPD":   ["DSG"],           # Legge protezione dati = Datenschutzgesetz
    "LEF":   ["SchKG"],         # Legge esecuzione = Schuldbetreibungsgesetz
    # Tedesco → Italiano/Francese
    "StGB":  ["CP"],
    "ZGB":   ["CC"],
    "OR":    ["CO"],
    "ZPO":   ["CPC"],
    "StPO":  ["CPP"],
    "BGG":   ["LTF"],
    "BV":    ["Cost."],          # Costituzione federale DE → IT
    "DSG":   ["LPD"],
    "SchKG": ["LEF"],
}

# Trova "Art. NNN [cpv./Abs./al. N] CODE" nella query già normalizzata
_ART_CODE_RE = re.compile(
    r'(Art\.\s+\d+[a-z]?)'
    r'(\s+(?:cpv\.|Abs\.|al\.)\s*\d+)?'
    r'\s+(CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG)\b',
    re.UNICODE,
)

# Riconosce una query che è SOLO un riferimento articolo (nessun concetto aggiuntivo)
_PURE_ART_RE = re.compile(
    r'^Art\.\s+\d+[a-z]?(?:\s+(?:cpv\.|Abs\.|al\.)\s*\d+)?'
    r'(?:\s+(?:CP|CC|CO|CPC|CPP|LTF|BGG|BV|Cost\.|Cst\.|StGB|ZGB|OR|ZPO|StPO|LPD|DSG|LEF|SchKG))+\s*$',
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
di sentenze federali svizzere (OpenCaseLaw).

Regole:
- Se la query contiene riferimenti ad articoli (es. "Art. 111 CP StGB", "Art. 336 CO OR"),
  includili ESATTAMENTE nella query ottimizzata, senza modificarli né rimuovere le sigle.
  Il riferimento articolo deve apparire in primo piano.
- Per query basate solo su articoli, restituisci il riferimento com'è (max 1 concetto aggiuntivo).
- Per query concettuali senza articoli, estrai 1–4 concetti giuridici chiave.
- Se la query è un codice sentenza (es. 6B_51/2021, BGE 147 IV 73), restituiscila com'è.
- Usa la lingua della query (it/de/fr) o termini giuridici standard svizzeri.

Rispondi SOLO con JSON: {"query_ottimizzata": "...", "spiegazione": "..."}"""

async def ottimizza_query(query: str, ai: AsyncOpenAI) -> tuple[str, str]:
    # 1. Normalizza: nomi di legge estesi → sigle, varianti articolo → "Art. NNN"
    query_norm = pre_processa_query(query)
    # 2. Espandi con sigle equivalenti nelle altre lingue ("Art. 111 CP" → "Art. 111 CP StGB")
    query_exp  = espandi_codici_articolo(query_norm)

    # Se è una query puramente articolo, salta l'AI: già ottimale
    if _PURE_ART_RE.match(query_exp.strip()):
        log.info("Pure article query — skip AI optimizer: '%s'", query_exp)
        return query_exp, "Articolo di legge"

    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=150, temperature=0,
            messages=[
                {"role": "system", "content": _OPTIMIZER_SYSTEM},
                {"role": "user",   "content": f'Query: "{query_exp}"'},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            d = json.loads(m.group())
            raw_opt = d.get("query_ottimizzata", query_exp)
            # Re-applica normalizzazione + espansione sull'output AI
            # (l'AI potrebbe aver alterato i riferimenti articolo)
            final_opt = espandi_codici_articolo(pre_processa_query(raw_opt))
            return final_opt, d.get("spiegazione", "")
    except Exception as exc:
        log.warning("Optimizer error: %s", exc)
    return query_exp, "Query diretta"


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
    art   = estrai_articoli(testo) if testo else estrai_articoli(riass)
    if not art and riass:
        art = estrai_articoli(riass)
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
    try:
        r = await http.get(
            f"{OPENCASELAW_BASE}/decisions",
            params=params,
            timeout=15.0,
        )
        r.raise_for_status()
        d = r.json()
        return d.get("results", d if isinstance(d, list) else [])
    except Exception as exc:
        log.error("OCL search error: %s", exc)
        return []

def _rerank(hits: list[dict]) -> list[dict]:
    """Re-ordina i risultati OCL per: relevance_score × log(1 + citation_count).
    Le sentenze molto citate (BGE) salgono rispetto a sentenze irrilevanti con
    score simile ma zero citazioni."""
    def _score(h: dict) -> float:
        rel = float(h.get("relevance_score") or 0)
        cit = int(h.get("citation_count") or 0)
        return rel * math.log1p(cit + 1)
    return sorted(hits, key=_score, reverse=True)

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
        "ocl_statutes":   hit.get("statutes") or [],
    }

async def _elabora_risultato(
    hit: dict, rank: int, lang: str,
    ai: Optional[AsyncOpenAI], http: httpx.AsyncClient,
) -> dict:
    """Scarica il testo completo e genera il riassunto per un singolo risultato."""
    meta   = _hit_to_meta(hit, rank)
    testo  = await _ocl_full_text(meta["decision_id"], http)
    riass  = await genera_riassunto(testo, lang, ai) if (ai and testo) else ""
    # Priorità articoli: campo statutes strutturato di OCL → testo completo → riassunto
    ocl_statutes = meta.pop("ocl_statutes", [])
    if ocl_statutes:
        art = ocl_statutes
    else:
        art = estrai_articoli(testo) if testo else estrai_articoli(riass)
        if not art and riass:
            art = estrai_articoli(riass)
    return {**meta, "riassunto": riass, "articoli": art}


# ── /cerca  (JSON bloccante) ─────────────────────────────────────────────────

@app.get("/cerca")
async def cerca(
    query:   str           = Query(..., min_length=1),
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
        query_opt, spiegazione = await ottimizza_query(query, ai) if ai else (query, "")
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
    query:    str           = Query(..., min_length=1),
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
                    query_opt, spiegazione = await ottimizza_query(query, ai)
                else:
                    query_opt, spiegazione = query, ""
                yield sse({"type": "status", "message": f"Ricerca: {query_opt}"})

                # Se attivi filtri che restringono, prendiamo più risultati per compensare
                needs_extra = bool(tipo_filter or area_filter or tribunal_filter or canton_filter)
                fetch_limit = limit * 4 if needs_extra else limit * 2
                # Per ricerche cantonali passa la lingua al filtro OCL (es. TI→it, ZH→de)
                ocl_lang = _CANTON_LANG.get(canton_filter, "") if canton_filter else ""
                hits = await _ocl_search(query_opt, min(fetch_limit, 40), http,
                                         language=ocl_lang, offset=offset)

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

                # Filtro tipo (federal / cantonal)
                if tipo_filter in ("federal", "cantonal"):
                    hits = [h for h in hits if _rileva_tipo(_court(h)) == tipo_filter]

                # Filtro tribunale specifico (bger / bvger / bstger)
                if tribunal_filter:
                    hits = [h for h in hits if _rileva_tribunal(_court(h)) == tribunal_filter]

                # Filtro cantone specifico
                if canton_filter:
                    hits = [h for h in hits if _rileva_cantone(_court(h)) == canton_filter]

                # Re-rank: relevance_score × log(1 + citation_count) — BGE citate salgono
                hits = _rerank(hits)[:limit]

                if not hits:
                    if canton_filter:
                        msg = f"Nessuna sentenza trovata per il cantone '{canton_filter.upper()}' su OpenCaseLaw."
                    elif tipo_filter == "cantonal":
                        msg = "Nessuna sentenza cantonale trovata per questa ricerca su OpenCaseLaw."
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
    "bger.ch", "www.bger.ch",
    "entscheidsuche.ch", "www.entscheidsuche.ch",
    "relevancy.ch", "www.relevancy.ch",
}

async def _fetch_bger_text(url: str, http: httpx.AsyncClient) -> str:
    """Scarica il testo grezzo di una sentenza da bger.li.
    Ritorna stringa vuota se URL non trovato, redirect loop, o dominio finale non valido."""
    try:
        # Limite redirect esplicito: evita loop infiniti su codici non validi
        resp = await http.get(url, timeout=25.0, follow_redirects=True,
                              max_redirects=5)
        if resp.status_code in (404, 410):
            return ""
        resp.raise_for_status()
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
        log.warning("bger.li fetch error (%s): %s", url, exc)
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

    # ── Validazione formato codice sentenza ──────────────────────────────────
    # Rifiuta immediatamente parole chiave, articoli di legge, ecc.
    # Formati validi: 6B_302/2023 | F-2684/2026 | 143 II 268 | BGE_134_III_67
    _DOCKET_RE = re.compile(
        r"""
        (?:(?:bge|atf|bger|bvger|bstger|bpatger|rdaf|rkge)[\s_]+)?   # prefisso opzionale
        (?:
            [1-9][A-Z]{0,3}_\d+/\d{4}           # 6B_302/2023
          | [A-Z]{1,3}[-_]\d+[/_-]\d{2,4}        # F-2684/2026 / SK.2023.1
          | \d{2,4}[\s_][IVX]{1,5}[\s_]\d+       # 143 II 268 / 134_III_67
        )
        """,
        re.IGNORECASE | re.VERBOSE,
    )
    if not _DOCKET_RE.search(codice.strip()):
        _invalid_msg = {
            "it": (f"'{codice}' non è un codice sentenza. Inserisci un numero di ruolo "
                   f"come 6B_302/2023 o BGE 143 II 268. Per cercare per tema usa Smart Search."),
            "de": (f"'{codice}' ist keine Urteilsnummer. Gib eine Dossiernummer ein wie "
                   f"6B_302/2023 oder BGE 143 II 268. Für die thematische Suche verwende Smart Search."),
            "fr": (f"'{codice}' n'est pas un numéro d'arrêt. Entrez un numéro de dossier "
                   f"comme 6B_302/2023 ou BGE 143 II 268. Pour rechercher par thème, utilisez Smart Search."),
        }
        return JSONResponse({"errore": _invalid_msg[lang]}, status_code=400)

    source = "opencaselaw.ch"
    async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:
        hit: dict = {}
        full_text = ""

        if decision_id:
            # Percorso veloce: abbiamo già decision_id, recupera testo direttamente
            full_text = await _ocl_full_text(decision_id, http)
            # Cerca comunque i metadati (per tribunale, data, statutes)
            hits_meta = await _ocl_search(codice, 1, http)
            hit = hits_meta[0] if hits_meta else {}
        else:
            # ── 1. OCL: ricerca semantica con match esatto sul docket_number ──
            norm_input = _normalize_codice(codice)

            def _find_exact(hits: list[dict]) -> list[dict]:
                return [h for h in hits
                        if _normalize_codice(
                            h.get("docket_number") or h.get("file_number") or ""
                        ) == norm_input]

            # Prova varianti di query per massimizzare la copertura OCL:
            # 1. codice senza prefisso tribunale (es. 'BVGer F-2684/2026' → 'F-2684/2026')
            #    → messo PRIMO perché più preciso per OCL
            # 2. originale
            # 3. separatori → spazio
            bare_codice = re.sub(
                r'^(?:bge|atf|bger|bvger|bstger|bpatger|rdaf|rkge)[\s_]+', '',
                codice, flags=re.IGNORECASE,
            ).strip()
            queries = [bare_codice] if bare_codice != codice else [codice]
            if codice not in queries:
                queries.append(codice)
            alt_sep = re.sub(r'[/_\-]', ' ', codice).strip()
            if alt_sep not in queries:
                queries.append(alt_sep)
            alt_bare = re.sub(r'[\s_\-/]', '', bare_codice)
            if alt_bare not in queries:
                queries.append(alt_bare)

            exact: list[dict] = []
            for q in queries:
                hits_q = await _ocl_search(q, 20, http)
                exact = _find_exact(hits_q)
                if exact:
                    break

            if exact:
                # Match esatto trovato su OCL
                hit = exact[0]
                full_text = await _ocl_full_text(hit.get("decision_id", ""), http)
            else:
                # ── 2. Fallback bger.li: fetch diretto per URL costruita dal codice ──
                # OCL è ricerca semantica e non garantisce di restituire la decisione
                # esatta — bger.li permette accesso diretto via codice.
                bger_url = costruisci_url_bger(codice)
                full_text = await _fetch_bger_text(bger_url, http)
                if full_text:
                    hit = {"docket_number": codice, "court_name": "BGer", "decision_date": ""}
                    source = "bger.li"
                    log.info("bger.li fallback for '%s': %s", codice, bger_url)
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
    })

@app.get("/sintesi_federal")
async def sintesi_federal(
    codice:      str           = Query(..., description="Codice sentenza (es. 6B_51/2021)"),
    lang:        str           = Query("it"),
    decision_id: Optional[str] = Query(None),
):
    """Riassunto AI strutturato di una sentenza. Accetta decision_id OCL per accesso diretto."""
    return await _sintesi_impl(codice, lang, decision_id or "")

@app.get("/sintesi")
async def sintesi(
    codice:      str           = Query(...),
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
