"""
Sententia ‚Äì Cantonal API
Backend dedicato alla ricerca delle sentenze cantonali ticinesi su www.sentenze.ti.ch

Funzionamento:
  1. Riceve la query naturale dell'utente
  2. La trasforma con OpenAI nella forma ottimale per il motore di sentenze.ti.ch
  3. Interroga il portale ufficiale
  4. Estrae e restituisce i risultati strutturati

Avvio:
  pip install fastapi uvicorn httpx beautifulsoup4 openai
  OPENAI_API_KEY=sk-... uvicorn sententia-cantonal-api:app --port 8001 --reload
"""

import re
import json
import os
import logging
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from openai import OpenAI

# ‚îÄ‚îÄ‚îÄ Logging ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ‚îÄ‚îÄ‚îÄ App ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
app = FastAPI(
    title="Sententia ‚Äì Cantonal API",
    description="Ricerca sentenze cantonali ticinesi tramite www.sentenze.ti.ch",
    version="1.0.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ‚îÄ‚îÄ‚îÄ Constants ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
PORTAL_BASE   = "https://www.sentenze.ti.ch"
PORTAL_SEARCH = "https://www.sentenze.ti.ch/findinfo/ti/cerca.htm"
PORTAL_CGI    = "https://www.sentenze.ti.ch/cgi-bin/nph-omniscgi"

# Omnis Studio hidden fields required by the CGI (from form inspection)
OMNIS_HIDDEN = {
    "OmnisPlatform":   "WINDOWS",
    "WebServerUrl":    "www.sentenze.ti.ch",
    "WebServerScript": "/cgi-bin/nph-omniscgi",
    "OmnisLibrary":    "JURISWEB",
    "OmnisClass":      "rtFindinfoWebHtmlService",
    "OmnisServer":     "JURISWEB,193.246.182.54:6000",
    "Parametername":   "WWWTI",
    "Schema":          "TI_WEB",
    "Source":          "",
}

# Tribunali cantonali ticinesi (D.1 del manuale)
COURT_NAMES: dict[str, str] = {
    # Civile
    "ICCA":   "Prima camera civile di appello",
    "IICCA":  "Seconda camera civile di appello",
    "IIICCA": "Terza camera civile di appello",
    "CCR":    "Camera civile dei reclami",
    "CCC":    "Camera di cassazione civile",
    "CEF":    "Camera esecuzione e fallimenti",
    "CDP":    "Camera di protezione",
    # Amministrativo
    "TRAM":   "Tribunale cantonale amministrativo",
    "TPT":    "Tribunale della pianificazione del territorio",
    "TCA":    "Tribunale cantonale delle assicurazioni sociali",
    "CDT":    "Camera di diritto tributario",
    "TE":     "Tribunale d'espropriazione",
    # Penale
    "PENAL":  "Tribunale penale cantonale",
    "CARP":   "Corte d'appello e di revisione penale",
    "CCRP":   "Corte di cassazione e di revisione penale",
    "CRPTI":  "Corte dei reclami penali",
    "CRP":    "Camera dei ricorsi penali",
    "GPC":    "Giudice dei provvedimenti coercitivi",
    "GIAR":   "Giudice dell'istruzione e dell'arresto",
    "PRPEN":  "Pretura penale",
}
COURT_PATTERN = re.compile(
    r"\b(ICCA|IICCA|IIICCA|CCR|CCC|CEF|CDP|TRAM|TPT|TCA|CDT|TE|PENAL|CARP|CCRP|CRPTI|CRP|GPC|GIAR|PRPEN)\b"
)

# Formato date dd.mm.yyyy
DATE_PATTERN = re.compile(r"\d{2}\.\d{2}\.\d{4}")

# Numero di incarto (es. 12.2021.234)
INCARTO_PATTERN = re.compile(r"\d+\.\d{4}\.\d+")

# HTTP headers
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-CH,it;q=0.9,en;q=0.5",
}

# ‚îÄ‚îÄ‚îÄ Claude: trasformazione della query ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

SYSTEM_PROMPT = """Sei un esperto di ricerca giuridica del Canton Ticino.
Il tuo compito √® trasformare la query naturale dell'utente nella forma ottimale
per il motore di ricerca del portale www.sentenze.ti.ch.

Regole del motore di ricerca (dal manuale ufficiale del portale):

OPERATORI SUPPORTATI:
- Virgolette per frasi esatte: "stato di ebriet√†"
- AND: ricerca pi√π gruppi di parole: "stato di ebriet√†" AND "incidente mortale"
- OR: alternativa tra gruppi: "stato di ebriet√†" OR "stato di ubriachezza"
- NOT: esclude parole: "stato di ebriet√†" NOT "autoveicolo"

IMPORTANTE: usa SEMPRE AND per combinare i termini. Non usare mai NEAR.

TIPI DI RICERCA:
- "testo": ricerca nel testo completo (catalogate e non catalogate) ‚Äì usa per concetti giuridici, fatti
- "titolo": ricerca nel titolo (solo catalogate) ‚Äì usa per parole chiave semplici e precise
- "articoli": ricerca per articolo di legge ‚Äì usa SOLO se la query contiene "art." + numero + abbreviazione legge
- "indice": ricerca per parole chiave dell'indice ‚Äì usa per termini giuridici standard

FORMATO ARTICOLI: art. 41 co (CO = codice delle obbligazioni, CP = codice penale, CC = codice civile, ecc.)

ABBREVIAZIONI TRIBUNALI:
ICCA, IICCA, CCC, CEF, TRAM, TPT, TCA, CDT, TE, PENAL, CCRP, CRP, GIAR, PRPEN

Rispondi SOLO con JSON valido, senza altri testi."""

USER_PROMPT_TEMPLATE = """Query utente: "{query}"

Rispondi con questo oggetto JSON:
{{
  "query_ottimizzata": "la query trasformata con operatori appropriati",
  "tipo_ricerca": "testo" | "titolo" | "articoli" | "indice",
  "tribunale": "abbreviazione o stringa vuota se non specificato",
  "spiegazione": "una frase che spiega la trasformazione (es. 'Ricerca nel testo con operatore NEAR per termini vicini')"
}}"""


def trasforma_query_con_claude(query_utente: str) -> dict:
    """
    Chiama Claude per trasformare la query naturale nella forma ottimale
    per il motore di sentenze.ti.ch.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        log.warning("OPENAI_API_KEY non impostata ‚Äì uso query diretta")
        return {
            "query_ottimizzata": query_utente,
            "tipo_ricerca": "testo",
            "tribunale": "",
            "spiegazione": "Query inviata direttamente (API key assente)",
        }

    client = OpenAI(api_key=api_key)
    try:
        message = client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=400,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": USER_PROMPT_TEMPLATE.format(query=query_utente)},
            ],
        )
        raw = message.choices[0].message.content.strip()
        log.info("OpenAI response: %s", raw)

        # Estrai JSON
        json_match = re.search(r"\{.*\}", raw, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())

        # Fallback
        return {
            "query_ottimizzata": query_utente,
            "tipo_ricerca": "testo",
            "tribunale": "",
            "spiegazione": "Trasformazione non riuscita ‚Äì query diretta",
        }
    except Exception as exc:
        log.error("Errore OpenAI: %s", exc)
        return {
            "query_ottimizzata": query_utente,
            "tipo_ricerca": "testo",
            "tribunale": "",
            "spiegazione": f"Errore OpenAI: {exc}",
        }


# ‚îÄ‚îÄ‚îÄ Ricerca sul portale ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

# Mappa tipo ricerca ‚Üí valore del parametro cSuchstringZiel
TIPO_ZIEL_MAP: dict[str, str] = {
    "testo":    "testo",
    "titolo":   "titolo",
    "articoli": "articoli",
    "indice":   "indice",
}


async def cerca_sul_portale(
    query: str,
    tipo: str,
    tribunale: str = "",
    anno_da: Optional[str] = None,
    anno_a: Optional[str] = None,
    portata: Optional[str] = None,
    tribunali: Optional[set] = None,
) -> list[dict]:
    """
    Interroga www.sentenze.ti.ch (Omnis Studio CGI) e ritorna una lista di risultati.

    Step 1: scarica la pagina di ricerca per ottenere i campi hidden e le checkbox
            dei tribunali cos√¨ come appaiono nel form reale.
    Step 2: POST al CGI con tutti i parametri corretti.
    """
    async with httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        headers=HTTP_HEADERS,
    ) as client:

        # ‚îÄ‚îÄ Step 1: scarica il form di ricerca ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        params: dict[str, str] = dict(OMNIS_HIDDEN)
        form_action = PORTAL_CGI
        try:
            form_resp = await client.get(PORTAL_SEARCH)
            soup_form = BeautifulSoup(form_resp.text, "html.parser")
            form = soup_form.find("form")
            if form:
                action = form.get("action", "")
                if action:
                    form_action = action if action.startswith("http") else PORTAL_BASE + action

                # Tutti i campi hidden del form
                for hidden in form.find_all("input", type="hidden"):
                    name = hidden.get("name", "")
                    val  = hidden.get("value", "")
                    if name:
                        params[name] = val

                # Mappa portata frontend (P/C/N) ‚Üí valore portale (NUOVO/CONFERMA/SENZA)
                PORTATA_MAP = {"P": "NUOVO", "C": "CONFERMA", "N": "SENZA"}
                portata_filter: set[str] = set()
                if portata:
                    portata_filter = {PORTATA_MAP.get(p.strip(), p.strip()) for p in portata.split(",")}

                # Checkbox: tribunali (bInfoArt_*) e portata (F30_ENTSCHEID_TYP_*)
                for cb in form.find_all("input", {"type": "checkbox"}):
                    name = cb.get("name", "")
                    val  = cb.get("value", "")
                    if not name or not val:
                        continue
                    if name.startswith("bInfoArt_"):
                        # Checkbox tribunali
                        if tribunale:
                            if val == tribunale:
                                params[name] = val
                        elif tribunali:
                            if val in tribunali:
                                params[name] = val
                        else:
                            params[name] = val
                    elif val in ("NUOVO", "CONFERMA", "SENZA"):
                        # Checkbox portata
                        if not portata_filter or val in portata_filter:
                            params[name] = val
                    else:
                        # Altri checkbox: includi sempre
                        params[name] = val

            log.info("Form action: %s | court=%s | portata=%s", form_action, tribunale or "tutti", portata or "tutte")
        except Exception as exc:
            log.warning("Impossibile caricare il form (%s) ‚Äì uso parametri di fallback", exc)
            if tribunale:
                params[f"bInfoArt_{tribunale}1"] = tribunale
            elif tribunali:
                for court in tribunali:
                    params[f"bInfoArt_{court}1"] = court
            else:
                for court in COURT_NAMES:
                    params[f"bInfoArt_{court}1"] = court

        # ‚îÄ‚îÄ Parametri di ricerca ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        params["Aufruf"]                = "validate"
        params["Template"]              = "results/resultpage_ita.fiw"
        params["cSprache"]              = "ITA"
        params["cSuchstring"]           = query
        params["cSuchstringZiel"]       = TIPO_ZIEL_MAP.get(tipo, "testo")
        params["nSeite"]                = "1"
        params["nAnzahlTrefferProSeite"] = "10"
        params["cButtonAction"]         = "3. Trova"

        # ‚îÄ‚îÄ Filtro temporale (anno) ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        if anno_da:
            params["cEntscheiddatumVonJahr"]  = str(anno_da)
            params["cEntscheiddatumVonMonat"] = ""
        if anno_a:
            params["cEntscheiddatumBisJahr"]  = str(anno_a)
            params["cEntscheiddatumBisMonat"] = ""

        log.info("POST %s | query='%s' tipo='%s' periodo=%s-%s",
                 form_action, query, params["cSuchstringZiel"], anno_da or "*", anno_a or "*")

        # ‚îÄ‚îÄ Step 2: invia la ricerca ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
        try:
            response = await client.post(form_action, data=params)
        except Exception as exc:
            log.error("POST fallito: %s", exc)
            return []

        log.info("Risposta portale: status=%s len=%d", response.status_code, len(response.content))
        # Decodifica esplicitamente come Windows-1252 (portale legacy italiano)
        html_text = response.content.decode("cp1252", errors="replace")
        return parse_results(html_text)


def parse_results(html: str) -> list[dict]:
    """
    Estrae i risultati dalla pagina HTML di sentenze.ti.ch.

    Struttura reale del portale: ogni risultato contiene un link
    con href che include "getMarkupDocument" e title="Sentenza numero incarto {XX.YYYY.ZZ}".
    Il testo del link √® il sommario della sentenza.
    Nell'elemento padre si trovano Autorit√†, date e numero incarto.
    """
    soup = BeautifulSoup(html, "html.parser")
    results: list[dict] = []

    # Tutti i link alle sentenze hanno "getMarkupDocument" nell'href
    links = soup.find_all("a", href=lambda h: h and "getMarkupDocument" in h)

    for link_el in links[:10]:
        href = link_el["href"]
        url  = href if href.startswith("http") else PORTAL_BASE + href

        # Testo del link = sommario della sentenza
        titolo = link_el.get_text(strip=True)
        # Fix: CP1252 control chars (0x91-0x94) che l'html parser non converte
        titolo = titolo.replace('\u0091','\u2018').replace('\u0092','\u2019').replace('\u0093','\u201c').replace('\u0094','\u201d')

        # title attribute = "Sentenza numero incarto 52.2015.575"
        title_attr = link_el.get("title", "")
        incarto_m  = INCARTO_PATTERN.search(title_attr)
        incarto    = incarto_m.group(0) if incarto_m else ""

        # Testo dell'elemento padre (tabella del singolo risultato)
        parent = link_el.find_parent("table")
        block_text = parent.get_text(" ", strip=True) if parent else ""

        dates    = DATE_PATTERN.findall(block_text)
        court_m  = COURT_PATTERN.search(block_text)
        court    = court_m.group(1) if court_m else ""

        if not incarto:
            im = INCARTO_PATTERN.search(block_text)
            incarto = im.group(0) if im else ""

        results.append({
            "titolo":             titolo,
            "tribunale":          court,
            "nome_tribunale":     COURT_NAMES.get(court, ""),
            "data_decisione":     dates[0] if len(dates) > 0 else "",
            "data_pubblicazione": dates[1] if len(dates) > 1 else "",
            "numero_incarto":     incarto,
            "url_originale":      url,
        })

    log.info("Risultati estratti: %d", len(results))
    return results


# ‚îÄ‚îÄ‚îÄ Riassunto di una sentenza cantonale ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

SUMMARY_SYSTEM = """Sei un esperto legale svizzero specializzato in diritto cantonale ticinese.
Fornisci riassunti chiari, precisi e professionali delle sentenze."""

SUMMARY_USER = """Riassumi questa sentenza cantonale ticinese in modo chiaro e strutturato.

Usa questa struttura:
**Fatti**: breve descrizione dei fatti rilevanti
**Questione giuridica**: il problema legale centrale affrontato
**Decisione**: il dispositivo e la motivazione principale
**Articoli citati**: i principali articoli di legge applicati (se presenti nel testo)

Testo della sentenza:
{testo}"""


@app.get("/riassumi_cantonale")
async def riassumi_cantonale(
    url: str = Query(..., description="URL completo della sentenza su sentenze.ti.ch"),
):
    """
    Scarica il testo di una sentenza cantonale ticinese e lo riassume con Claude.
    """
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        return JSONResponse(
            {"errore": "OPENAI_API_KEY non configurata sul server."},
            status_code=500,
        )

    # ‚îÄ‚îÄ Scarica la sentenza ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=HTTP_HEADERS) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as exc:
            return JSONResponse(
                {"errore": f"Impossibile scaricare la sentenza: {exc}"},
                status_code=502,
            )

    soup = BeautifulSoup(resp.content.decode("cp1252", errors="replace"), "html.parser")

    # Rimuovi navigazione e footer
    for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
        tag.decompose()

    # Cerca il contenuto principale
    content_el = (
        soup.find("div", class_=re.compile(r"content|document|testo|sentenza|main|article", re.I))
        or soup.find("article")
        or soup.find("main")
        or soup.find("body")
    )
    testo = content_el.get_text("\n", strip=True) if content_el else ""
    testo = testo[:9000]  # Limite token sicuro

    if len(testo) < 50:
        return JSONResponse(
            {"errore": "Impossibile estrarre il testo dalla sentenza. Verifica l'URL."},
            status_code=400,
        )

    # ‚îÄ‚îÄ Riassunto con OpenAI ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
    try:
        ai_client = OpenAI(api_key=api_key)
        message = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1200,
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM},
                {"role": "user", "content": SUMMARY_USER.format(testo=testo)},
            ],
        )
        riassunto = message.choices[0].message.content
    except Exception as exc:
        return JSONResponse({"errore": f"Errore OpenAI: {exc}"}, status_code=500)

    return JSONResponse({"riassunto": riassunto, "url": url})


# ‚îÄ‚îÄ‚îÄ Endpoint principale: ricerca cantonale ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ

@app.get("/ricerca_cantonale")
async def ricerca_cantonale(
    query: str = Query(..., min_length=2, description="Query in linguaggio naturale"),
    tipo_override: Optional[str] = Query(None, description="Forza tipo ricerca: testo|titolo|articoli|indice"),
    tribunale_override: Optional[str] = Query(None, description="Forza tribunale (sigla)"),
    tribunali_override: Optional[str] = Query(None, description="Lista sig–ª–µ tribunali separati da virgola (per filtro area)"),
    anno_da: Optional[str] = Query(None, description="Anno inizio periodo"),
    anno_a: Optional[str] = Query(None, description="Anno fine periodo"),
    portata: Optional[str] = Query(None, description="Portata giuridica: P,C,N (comma-separated)"),
):
    """
    Ricerca sentenze cantonali ticinesi su www.sentenze.ti.ch.

    Pipeline:
      1. Claude trasforma la query naturale ‚Üí query ottimizzata per il portale
         (tipo e tribunale vengono sovrascritti se passati come override)
      2. Ricerca sul portale ufficiale
      3. Parsing e strutturazione dei risultati
      4. Restituzione JSON al frontend
    """
    log.info("Nuova ricerca cantonale: '%s' | overrides: tipo=%s tribunale=%s tribunali=%s periodo=%s-%s portata=%s",
             query, tipo_override, tribunale_override, tribunali_override, anno_da, anno_a, portata)

    # Step 1 ‚Äì Trasformazione query con Claude
    trasf = trasforma_query_con_claude(query)
    query_opt = trasf.get("query_ottimizzata", query)
    tipo = tipo_override if tipo_override else trasf.get("tipo_ricerca", "testo")
    tribunale = tribunale_override if tribunale_override else trasf.get("tribunale", "")
    tribunali = set(tribunali_override.split(",")) if tribunali_override else None
    spiegazione = trasf.get("spiegazione", "")

    log.info("Query ottimizzata: '%s' | tipo: %s | tribunale: %s | tribunali: %s", query_opt, tipo, tribunale, tribunali)

    # Step 2 ‚Äì Ricerca sul portale (con parametri extra opzionali)
    risultati = await cerca_sul_portale(
        query_opt, tipo, tribunale,
        anno_da=anno_da, anno_a=anno_a, portata=portata,
        tribunali=tribunali,
    )

    # Post-filtro per anno (sicurezza: il portale CGI potrebbe non rispettare i parametri data)
    if anno_da or anno_a:
        def _year(d: str) -> int:
            try: return int(d.split(".")[-1])
            except: return 0
        risultati = [
            r for r in risultati
            if not r.get("data_decisione") or (
                (not anno_da or _year(r["data_decisione"]) >= int(anno_da)) and
                (not anno_a  or _year(r["data_decisione"]) <= int(anno_a))
            )
        ]

    return JSONResponse({
        "risultati": risultati,
        "query_originale": query,
        "query_ottimizzata": query_opt,
        "tipo_ricerca": tipo,
        "tribunale": tribunale,
        "spiegazione": spiegazione,
        "filtri": {
            "tipo_override": tipo_override,
            "tribunale_override": tribunale_override,
            "anno_da": anno_da,
            "anno_a": anno_a,
            "portata": portata,
        },
        "totale": len(risultati),
    })


@app.get("/html_cantonale")
async def html_cantonale(
    url: str = Query(..., description="URL completo della sentenza su sentenze.ti.ch"),
):
    """
    Scarica e restituisce l'HTML pulito della sentenza (con grassetti, corsivi, tabelle)
    per la visualizzazione completa con elementi originali nel frontend.
    """
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=HTTP_HEADERS) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as exc:
            return JSONResponse(
                {"errore": f"Impossibile scaricare la sentenza: {exc}"},
                status_code=502,
            )

    soup = BeautifulSoup(resp.content.decode("cp1252", errors="replace"), "html.parser")
    # Rimuovi elementi di navigazione, script, form
    for tag in soup.find_all(["nav", "header", "footer", "script", "style", "form", "noscript"]):
        tag.decompose()
    # Rimuovi anche il banner/logo del portale (prima tabella)
    tables = soup.find_all("table")
    if tables:
        tables[0].decompose()

    content_el = soup.find("body") or soup
    html_content = str(content_el)

    if len(html_content) < 50:
        return JSONResponse({"errore": "Contenuto non trovato nella pagina."}, status_code=400)

    return JSONResponse({"html": html_content, "url": url})


@app.get("/testo_cantonale")
async def testo_cantonale(
    url: str = Query(..., description="URL completo della sentenza su sentenze.ti.ch"),
):
    """
    Scarica e restituisce il testo pulito di una sentenza cantonale,
    per la visualizzazione in anteprima nel frontend.
    """
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True, headers=HTTP_HEADERS) as client:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as exc:
            return JSONResponse(
                {"errore": f"Impossibile scaricare la sentenza: {exc}"},
                status_code=502,
            )

    html_text = resp.content.decode("cp1252", errors="replace")
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
        tag.decompose()

    content_el = (
        soup.find("div", class_=re.compile(r"content|document|testo|sentenza|main|article", re.I))
        or soup.find("article")
        or soup.find("main")
        or soup.find("body")
    )
    testo = content_el.get_text(" ", strip=True) if content_el else ""

    if len(testo) < 30:
        return JSONResponse({"errore": "Testo non trovato nella pagina."}, status_code=400)

    return JSONResponse({"testo": testo, "url": url})


@app.get("/health")
def health():
    return {"status": "ok", "portale": PORTAL_BASE}


# ‚îÄ‚îÄ‚îÄ Entry point ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ‚îÄ
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, reload=True)
