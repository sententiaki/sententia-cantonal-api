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

def costruisci_url_bger(codice: str) -> str:
    c = codice.strip()
    c = re.sub(r'^BGE\s+', '', c, flags=re.IGNORECASE)
    c = re.sub(r'[\s/]', '-', c)
    return f"https://bger.li/{c}"

def sse(data: dict) -> str:
    """Formatta un evento SSE."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


# ── Query optimizer (GPT-4o-mini) ─────────────────────────────────────────────

_OPTIMIZER_SYSTEM = """Sei un esperto di ricerca giuridica svizzera.
Trasforma la query dell'utente in termini di ricerca ottimali per un motore full-text
di sentenze federali svizzere (OpenCaseLaw).

Regole:
- Estrai 1–4 concetti giuridici chiave in forma di parole chiave (non frasi complete)
- Mantieni i riferimenti agli articoli esattamente come scritti (es. "art. 53 CP", "art. 336 CO")
- Se la query è già un codice sentenza (es. 6B_51/2021, BGE 147 IV 73), restituiscilo com'è
- Usa la lingua della query (it/de/fr) o termini giuridici standard

Rispondi SOLO con JSON: {"query_ottimizzata": "...", "spiegazione": "..."}"""

async def ottimizza_query(query: str, ai: AsyncOpenAI) -> tuple[str, str]:
    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=150, temperature=0,
            messages=[
                {"role": "system", "content": _OPTIMIZER_SYSTEM},
                {"role": "user",   "content": f'Query: "{query}"'},
            ],
        )
        raw = resp.choices[0].message.content.strip()
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            d = json.loads(m.group())
            return d.get("query_ottimizzata", query), d.get("spiegazione", "")
    except Exception as exc:
        log.warning("Optimizer error: %s", exc)
    return query, "Query diretta"


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

async def _ocl_search(query: str, limit: int, http: httpx.AsyncClient) -> list[dict]:
    try:
        r = await http.get(
            f"{OPENCASELAW_BASE}/decisions",
            params={"query": query, "limit": limit},
            timeout=15.0,
        )
        r.raise_for_status()
        d = r.json()
        return d.get("results", d if isinstance(d, list) else [])
    except Exception as exc:
        log.error("OCL search error: %s", exc)
        return []

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
        "rank":         rank,
        "codice":       docket,
        "tribunale":    normalizza_tribunale(court_raw),
        "tipo":         tipo,
        "tribunal_key": _rileva_tribunal(court_raw),
        "canton_key":   _rileva_cantone(court_raw),
        "camera":       hit.get("division") or hit.get("department") or hit.get("chamber") or "",
        "area":         rileva_area(docket),
        "data":         data_fmt,
        "anno":         anno,
        "url":          url_final,
        "decision_id":  hit.get("decision_id", ""),
    }

async def _elabora_risultato(
    hit: dict, rank: int, lang: str,
    ai: Optional[AsyncOpenAI], http: httpx.AsyncClient,
) -> dict:
    """Scarica il testo completo e genera il riassunto per un singolo risultato."""
    meta   = _hit_to_meta(hit, rank)
    testo  = await _ocl_full_text(meta["decision_id"], http)
    riass  = await genera_riassunto(testo, lang, ai) if (ai and testo) else ""
    # Estrai articoli dal testo completo prima (più affidabile), poi dal riassunto
    art = estrai_articoli(testo) if testo else estrai_articoli(riass)
    # Se dal testo non è uscito niente, prova anche sul riassunto
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

        hits = hits[:limit]
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
                hits = await _ocl_search(query_opt, min(fetch_limit, 40), http)

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

                hits = hits[:limit]

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

async def _sintesi_impl(codice: str, lang: str) -> JSONResponse:
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return JSONResponse({"errore": "OPENAI_API_KEY non configurata."}, status_code=500)
    lang = lang if lang in ("it", "de", "fr") else "it"

    async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:
        hits = await _ocl_search(codice, 1, http)
        if not hits:
            return JSONResponse({"errore": f"Sentenza non trovata: {codice}"}, status_code=404)
        hit       = hits[0]
        dec_id    = hit.get("decision_id", "")
        full_text = await _ocl_full_text(dec_id, http)

    if len(full_text) < 100:
        return JSONResponse({"errore": "Testo non disponibile."}, status_code=404)

    ai = AsyncOpenAI(api_key=api_key)
    try:
        msg = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=1400,
            messages=[
                {"role": "system", "content": _SINTESI_SYSTEM.get(lang, _SINTESI_SYSTEM["it"])},
                {"role": "user",   "content": _SINTESI_USER[lang].format(testo=full_text[:12000])},
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
        "source":        "opencaselaw.ch",
    })

@app.get("/sintesi_federal")
async def sintesi_federal(
    codice: str = Query(..., description="Codice sentenza (es. 6B_51/2021)"),
    lang:   str = Query("it"),
):
    """Riassunto AI strutturato di una sentenza federale specifica."""
    return await _sintesi_impl(codice, lang)

@app.get("/sintesi")
async def sintesi(
    codice: str = Query(...),
    lang:   str = Query("it"),
):
    """Alias di /sintesi_federal (compatibilità con sententia-api-3.onrender.com)."""
    return await _sintesi_impl(codice, lang)


# ── /articolo_fedlex ─────────────────────────────────────────────────────────

@app.get("/articolo_fedlex")
async def articolo_fedlex(
    rs:   str = Query(..., description="Numero RS (es. 311.0)"),
    art:  str = Query(..., description="Numero articolo (es. 53)"),
    lang: str = Query("it"),
):
    """Testo di un articolo di legge federale via fedlex-connector.ch."""
    lang = lang if lang in ("it", "de", "fr") else "it"
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "get_article",
            "arguments": {"rs_number": rs, "article": art, "language": lang},
        },
    }
    async with httpx.AsyncClient(timeout=12.0) as client:
        try:
            resp = await client.post(
                "https://mcp.fedlex-connector.ch/",
                json=payload,
                headers={"Accept": "application/json, text/event-stream"},
            )
        except Exception as exc:
            return JSONResponse({"errore": f"Fedlex proxy error: {exc}"}, status_code=502)

    m = re.search(r'^data:\s*(.+)$', resp.text, re.MULTILINE)
    if not m:
        return JSONResponse({"errore": "Risposta non valida da fedlex-connector.ch"}, status_code=502)
    try:
        data = json.loads(m.group(1))
    except Exception:
        return JSONResponse({"errore": "JSON non valido"}, status_code=502)

    if data.get("result", {}).get("isError"):
        msg = (data["result"].get("content") or [{}])[0].get("text", "Errore sconosciuto")
        return JSONResponse({"errore": msg}, status_code=404)

    text = (data.get("result", {}).get("content") or [{}])[0].get("text", "")
    return JSONResponse({"testo": text, "rs": rs, "art": art, "lang": lang})


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
