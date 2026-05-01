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

# Prefisso numero di dossier → area giuridica
AREA_MAP: dict[str, str] = {
    "6": "penale", "7": "penale",
    "4": "civile", "5": "civile",
    "8": "sociale", "9": "sociale",
    "1": "pubblico", "2": "pubblico", "3": "pubblico",
}

# Nome tribunale grezzo → etichetta display
COURT_DISPLAY: dict[str, str] = {
    "bundesgericht":           "BGer — Tribunale federale",
    "tribunal fédéral":        "BGer — Tribunale federale",
    "tribunale federale":      "BGer — Tribunale federale",
    "bger":                    "BGer — Tribunale federale",
    "bge":                     "BGer — Tribunale federale",
    "bundesverwaltungsgericht":"BVGer — Trib. amm. federale",
    "tribunal administratif fédéral": "BVGer — Trib. amm. federale",
    "bvger":                   "BVGer — Trib. amm. federale",
    "bundesstrafgericht":      "BStGer — Trib. penale federale",
    "tribunal pénal fédéral":  "BStGer — Trib. penale federale",
    "bstger":                  "BStGer — Trib. penale federale",
}

# Abbreviazioni legge per la normalizzazione
LAW_ALIASES: dict[str, str] = {
    "OR": "CO", "ZGB": "CC", "STGB": "CP", "ZPO": "CPC", "STPO": "CPP",
    "BGG": "LTF", "KVG": "LAMal", "LAA": "LAINF", "AHVG": "LAVS",
    "IVG": "LAI", "SCHKG": "LEF", "DBG": "LIFD", "MWSTG": "LIVA",
    "UWG": "LCD", "KG": "LCart", "MSCHG": "LPM", "FUSG": "LFus",
    "IPRG": "LDIP", "VWVG": "PA",
}

# Regex per l'estrazione degli articoli dal testo
_CODES = (
    "CP|StGB|CO|OR|CPP|StPO|LTF|BGG|BV|Cost\\.|Cst\\.|ZPO|CPC"
    "|LPD|DSG|CC|ZGB|LAMal|KVG|LAINF|LAA|LAVS|AHVG|LAI|IVG"
    "|LEF|SchKG|LIFD|DBG|LIVA|MWSTG|LCD|UWG|LCart|KG|LPM|MSchG"
    "|LFus|FusG|LDIP|IPRG|PA|VwVG|LTF"
)
ARTICLE_RE = re.compile(
    r'[Aa]rt\.?\s+(\d+[a-z]?)(?:\s+(?:cpv|abs|al|lett?|lit)\.?\s*\d+)?'
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
    for k, v in COURT_DISPLAY.items():
        if k in key:
            return v
    return raw.strip() or "BGer — Tribunale federale"

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
        "Sei un esperto legale svizzero. Scrivi un riassunto di questa sentenza federale "
        "in italiano (120–160 parole). Descrivi il problema giuridico centrale, l'analisi "
        "del tribunale e l'esito. Cita gli articoli applicati. Testo fluente, nessun titolo."
    ),
    "de": (
        "Du bist ein Schweizer Rechtsexperte. Schreibe eine Zusammenfassung dieses "
        "Bundesgerichtsurteils auf Deutsch (120–160 Wörter). Beschreibe die Rechtsfrage, "
        "die Analyse und das Ergebnis. Zitiere die angewandten Artikel. Fließender Text."
    ),
    "fr": (
        "Tu es un expert juridique suisse. Rédige un résumé de cet arrêt fédéral en "
        "français (120–160 mots). Décris la question juridique, l'analyse et l'issue. "
        "Cite les articles appliqués. Texte fluide, sans titres."
    ),
}

async def genera_riassunto(testo: str, lang: str, ai: AsyncOpenAI) -> str:
    if not testo:
        return ""
    try:
        resp = await ai.chat.completions.create(
            model="gpt-4o-mini", max_tokens=380, temperature=0.25,
            messages=[
                {"role": "system", "content": _SUMMARY_SYSTEM.get(lang, _SUMMARY_SYSTEM["it"])},
                {"role": "user",   "content": testo[:6000]},
            ],
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        log.error("Summary error: %s", exc)
        return ""


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
    return {
        "rank":        rank,
        "codice":      docket,
        "tribunale":   normalizza_tribunale(court_raw),
        "tipo":        "federal",
        "area":        rileva_area(docket),
        "data":        data_fmt,
        "anno":        anno,
        "url":         costruisci_url_bger(docket),
        "decision_id": hit.get("decision_id", ""),
    }

async def _elabora_risultato(
    hit: dict, rank: int, lang: str,
    ai: Optional[AsyncOpenAI], http: httpx.AsyncClient,
) -> dict:
    """Scarica il testo completo e genera il riassunto per un singolo risultato."""
    meta   = _hit_to_meta(hit, rank)
    testo  = await _ocl_full_text(meta["decision_id"], http)
    riass  = await genera_riassunto(testo, lang, ai) if (ai and testo) else ""
    art    = estrai_articoli(testo or riass)
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
    query:   str           = Query(..., min_length=1),
    lang:    str           = Query("it"),
    limit:   int           = Query(8, ge=1, le=20),
    anno_da: Optional[str] = Query(None),
    anno_a:  Optional[str] = Query(None),
):
    """
    Ricerca federale con SSE.
    I risultati arrivano uno alla volta in ordine di rilevanza, man mano che
    ogni riassunto AI viene generato. Il frontend può renderizzare ogni card
    non appena riceve l'evento corrispondente.

    Formato eventi:
      data: {"type": "status",  "message": "..."}
      data: {"type": "result",  "rank": 1, "codice": "...", ...}
      data: {"type": "done",    "totale": N}
      data: {"type": "error",   "message": "..."}
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    lang    = lang if lang in ("it", "de", "fr") else "it"
    ai      = AsyncOpenAI(api_key=api_key) if api_key else None

    async def generator() -> AsyncIterator[str]:
        try:
            async with httpx.AsyncClient(headers=HTTP_HEADERS, follow_redirects=True) as http:

                # 1. Ottimizza query
                yield sse({"type": "status", "message": "Ottimizzazione query..."})
                if ai:
                    query_opt, spiegazione = await ottimizza_query(query, ai)
                else:
                    query_opt, spiegazione = query, ""
                yield sse({"type": "status", "message": f"Ricerca: {query_opt}"})

                # 2. Cerca su OpenCaseLaw
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
                    yield sse({"type": "error", "message": "Nessun risultato trovato."})
                    return

                yield sse({"type": "status", "message": f"Trovate {len(hits)} sentenze — generazione riassunti..."})

                # 3. Avvia TUTTI i task in parallelo (testo + riassunto) — corrono insieme
                tasks = [
                    asyncio.create_task(_elabora_risultato(h, i + 1, lang, ai, http))
                    for i, h in enumerate(hits)
                ]

                # 4. Emetti in ordine di rank: aspetta il primo, poi il secondo, ecc.
                #    I task successivi continuano a girare mentre aspettiamo quello corrente.
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

_SINTESI_SYSTEM = """Sei un esperto legale svizzero. Produci riassunti strutturati di sentenze
del Tribunale federale nella lingua richiesta.

Usa questo formato (titolo in grassetto, testo a seguire):

**Fattispecie**
[fatti: chi, cosa, iter procedurale]

**Questione giuridica**
[problema centrale e articoli applicati]

**Considerandi**
[ragionamento del Tribunale, precedenti]

**Dispositivo**
[decisione finale e conseguenze]

Sii preciso e professionale. Usa i termini giuridici corretti nella lingua richiesta."""

_SINTESI_USER = {
    "it": "Riassumi in italiano:\n\n{testo}",
    "de": "Fasse auf Deutsch zusammen:\n\n{testo}",
    "fr": "Résume en français:\n\n{testo}",
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
                {"role": "system", "content": _SINTESI_SYSTEM},
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
