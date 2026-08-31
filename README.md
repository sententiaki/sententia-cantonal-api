# Sententia – Search API

Backend Python (FastAPI) che alimenta la ricerca giuridica svizzera di [sententia.ch](https://sententia.ch). Gira su [Render](https://render.com) e viene interrogato dal frontend HTML via SSE e JSON.

## Struttura della repository

```
sententia-cantonal-deploy/
├── main.py                  # Tutto il backend (unico file Python in produzione)
├── render.yaml              # Configurazione deploy Render
├── requirements.txt         # Dipendenze Python
├── sententia-search.html    # Frontend ricerca (deploy su SiteGround, non Render)
└── sententia-summarize.html # Frontend riassunto (idem)
```

> Le due HTML non vengono servite da Render — vivono su SiteGround (sententia.ch). Sono incluse nella repo per tenere il codice frontend versionato insieme al backend.

---

## Come funziona

### Frontend search (`sententia-search.html`)

Il frontend di ricerca interroga `/cerca_stream` e riceve i risultati via SSE man mano che arrivano.

La pipeline backend esegue in sequenza:

1. **Pre-processing della query** — normalizza articoli di legge (es. "art. 10 bv" → "Art. 10 BV"), espande sigle trilingui (BV = Cost. = Cst.), gestisce abbreviazioni cantonali, rimuove stopword.
2. **Ottimizzazione AI** — chiede a GPT-4o di riformulare la query in forma giuridica ottimale (de/fr/it), estraendo il concetto principale separato dai riferimenti normativi.
3. **Ricerca parallela su due fonti**:
   - **OpenCaseLaw** (`mcp.opencaselaw.ch`) — API MCP per sentenze federali e cantonali.
   - **Entscheidsuche** (`es-proxy.sententia-ki.workers.dev`) — proxy Cloudflare su Elasticsearch con ~1M di decisioni; usa `simple_query_string` con query trilingue.
4. **Merge + reranking** — i risultati delle due fonti vengono uniti, deduplicati e riordinati (prima per corte federale/cantonale, poi per data, poi reranking AI).
5. **Stream SSE** — ogni risultato viene elaborato e inviato al client appena pronto, senza aspettare il completamento di tutti.

### Frontend summarize (`sententia-summarize.html`)

Il frontend di riassunto interroga `/sintesi_federal` passando un codice sentenza (es. `6B_51/2021`).

La pipeline backend:

1. **Recupero testo** — tenta prima da bger.li costruendo l'URL direttamente dal codice; se non trovato, cerca su Entscheidsuche per numero di ruolo.
2. **Riassunto AI** — invia il testo (max 8000 caratteri) a `gpt-4o-mini` con prompt strutturato in it/de/fr, che produce un riassunto con: fatti, questione giuridica, considerazioni, dispositivo.
3. **Risposta JSON** — restituisce la sintesi insieme a metadati (tribunale, data, codice, URL, statutes).

---

## Endpoint API

### Ricerca

| Metodo | Path | Descrizione |
|--------|------|-------------|
| `GET` | `/cerca` | Ricerca JSON sincrona (tutti i risultati in un colpo) |
| `GET` | `/cerca_stream` | Ricerca SSE streaming (risultati in arrivo progressivo) |

**Parametri principali di `/cerca_stream`:**

| Parametro | Tipo | Descrizione |
|-----------|------|-------------|
| `query` | string | Query in testo libero (de/fr/it) |
| `canton` | string | Filtro cantone opzionale (es. `ti`, `zh`, `ge`) |
| `limit` | int | Numero massimo di risultati (default 10) |

### Riassunto

| Metodo | Path | Descrizione |
|--------|------|-------------|
| `GET` | `/sintesi_federal` | Riassunto AI strutturato di una sentenza |
| `GET` | `/sintesi` | Alias di `/sintesi_federal` (compatibilità legacy) |

**Parametri:**

| Parametro | Tipo | Descrizione |
|-----------|------|-------------|
| `codice` | string | Codice sentenza (es. `6B_51/2021`, `BGE 141 III 28`) |
| `lang` | string | Lingua del riassunto: `it`, `de`, `fr` (default `it`) |
| `decision_id` | string | ID OCL opzionale per accesso diretto senza ricerca |

### Articoli e testo decisioni

| Metodo | Path | Descrizione |
|--------|------|-------------|
| `GET` | `/articolo_ocl` | Testo di un articolo di legge federale via OpenCaseLaw |
| `GET` | `/articolo_fedlex` | Testo di un articolo via fedlex-connector.ch |
| `GET` | `/testo_decisione` | Testo completo di una sentenza (da OpenCaseLaw o bger.li) |
| `GET` | `/html_federale` | HTML pulito di una sentenza federale da bger.li |

### Utility

| Metodo | Path | Descrizione |
|--------|------|-------------|
| `POST` | `/feedback` | Feedback utente (invia email via SMTP) |
| `GET` | `/health` | Health check |

---

## Sorgenti dati

| Fonte | URL | Cosa fornisce |
|-------|-----|---------------|
| OpenCaseLaw | `mcp.opencaselaw.ch` | Sentenze federali + cantonali, testo articoli |
| Entscheidsuche | `es-proxy.sententia-ki.workers.dev` | ~1M decisioni su Elasticsearch |
| bger.li | `bger.li` | HTML formattato sentenze Tribunale Federale |
| Fedlex | `fedlex-connector.ch` | Testo leggi federali |

---

## Deploy

Il deploy è gestito da Render tramite `render.yaml`. L'entry point è `main:app`.

```bash
# Avvio locale
pip install -r requirements.txt
OPENAI_API_KEY=sk-... uvicorn main:app --port 8002 --reload
```

**Variabile d'ambiente richiesta:**
- `OPENAI_API_KEY` — usata per ottimizzazione query e generazione riassunti (modello: `gpt-4o-mini` per query, `gpt-4o` per riassunti)

---

## Architettura interna di `main.py`

Il file è organizzato in sezioni logiche:

| Sezione (righe approssimative) | Contenuto |
|-------------------------------|-----------|
| 1–200 | Costanti, rilevamento tribunale/cantone/tipo |
| 200–570 | Pre-processing query (normalizzazione, espansione articoli) |
| 570–710 | Ottimizzazione query AI (`ottimizza_query`) |
| 710–870 | Helpers OpenCaseLaw (`_ocl_search`, `_ocl_full_text`) |
| 870–1080 | Ricerca Entscheidsuche ES + normalizzazione hit |
| 1080–1360 | Endpoint `/cerca` e `/cerca_stream` — pipeline principale |
| 1360–1668 | Endpoint sintesi, articoli, testo, feedback, health |
