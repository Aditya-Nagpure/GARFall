# RagForAll

A fully local, end-to-end RAG (Retrieval-Augmented Generation) pipeline that turns any website into a searchable knowledge base you can chat with.

Point it at a URL, and it will:
1. **Discover** all pages (via sitemap or link extraction)
2. **Crawl** them in parallel with a real headless browser
3. **Filter** out stubs, non-English content, and low-quality pages
4. **Chunk** content into embedding-ready segments
5. **Embed & store** chunks in a local ChromaDB vector database
6. **Answer questions** using retrieved context + a Groq-hosted LLM

---

## Architecture

```
main.py  (CLI)
│
├── scraper/
│   ├── url_discovery.py   — sitemap + link extraction
│   ├── crawler.py         — parallel Crawl4AI browser crawler
│   ├── filters.py         — quality & language filtering
│   └── chunker.py         — heading-aware text chunking → JSONL
│
├── retrieval/
│   ├── ingest.py          — embed chunks → ChromaDB  (CLI)
│   └── query.py           — cosine search + Groq generation  (CLI)
│
└── dashboard.py           — Streamlit UI  (Chat / Ingest / Collections)
```

---

## Tech Stack

| Layer | Library |
|---|---|
| Web crawling | [Crawl4AI](https://github.com/unclecode/crawl4ai) (Playwright/Chromium) |
| Embeddings | [sentence-transformers](https://www.sbert.net/) `all-MiniLM-L6-v2` |
| Vector store | [ChromaDB](https://www.trychroma.com/) (persistent, local) |
| LLM | [Groq](https://groq.com/) `llama-3.3-70b-versatile` |
| Dashboard | [Streamlit](https://streamlit.io/) |
| Language detection | [langdetect](https://pypi.org/project/langdetect/) |

Everything runs locally except the LLM inference call to Groq (free tier available).

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
crawl4ai-setup                   # downloads Playwright browsers
```

### 2. Set environment variables

Create a `.env` file in the project root:

```env
GROQ_API_KEY=gsk_...
```

Get a free key at [console.groq.com](https://console.groq.com).

### 3. Scrape a website

```bash
python main.py https://docs.python.org/3/
```

This discovers all pages, crawls them, filters, chunks, and writes a `.jsonl` file to `output/`.

### 4. Ingest into ChromaDB

```bash
python retrieval/ingest.py output/docs_python_org_*.jsonl
```

### 5. Ask questions (CLI)

```bash
python retrieval/query.py "What are async context managers?"
```

### 6. Or use the dashboard

```bash
streamlit run dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Scraper CLI Reference

```
python main.py <url> [options]

Options:
  --max-concurrent N      Parallel browser sessions (default: 10)
  --memory-threshold N    Pause crawling if RAM % exceeds N (default: 80)
  --no-js                 Disable JavaScript rendering (faster for static sites)
  --output FILE           Custom output path (default: output/<domain>_<timestamp>.jsonl)
  --no-chunk              Save full pages instead of RAG chunks
  --limit N               Max URLs to crawl (0 = no limit)
  --single                Skip URL discovery, crawl only the given URL
```

**Examples:**

```bash
# Crawl an entire docs site
python main.py https://fastapi.tiangolo.com/

# Single page, no JS
python main.py https://example.com --single --no-js

# Limit to 50 pages, custom output
python main.py https://en.wikipedia.org/wiki/Python --limit 50 --output output/python_wiki.jsonl
```

---

## Ingest CLI Reference

```
python retrieval/ingest.py <files> [options]

Options:
  --collection NAME   ChromaDB collection name (default: rag_chunks)
  --reset             Delete the collection before ingesting
```

**Examples:**

```bash
# Ingest one file
python retrieval/ingest.py output/fastapi_tiangolo_com_20260402_162155.jsonl

# Ingest multiple files into a named collection
python retrieval/ingest.py output/*.jsonl --collection docs

# Rebuild collection from scratch
python retrieval/ingest.py output/site.jsonl --collection docs --reset
```

---

## Query CLI Reference

```
python retrieval/query.py "<question>" [options]

Options:
  --top-k N           Chunks to retrieve (default: 5)
  --collection NAME   Collection to query (default: rag_chunks)
  --no-generate       Retrieve only, skip LLM answer
```

**Examples:**

```bash
python retrieval/query.py "What is dependency injection in FastAPI?"
python retrieval/query.py "List all EV models" --collection tatamotors --top-k 10
python retrieval/query.py "Show retrieved chunks" --no-generate
```

---

## Dashboard

The Streamlit dashboard (`dashboard.py`) has three pages:

| Page | What it does |
|---|---|
| **Chat** | Ask questions; streams answers from Groq with source citations |
| **Ingest** | Select `output/*.jsonl` files and embed them into a named collection |
| **Collections** | Browse, inspect, and delete ChromaDB collections |

Run with:
```bash
streamlit run dashboard.py
```

If `GROQ_API_KEY` is not in `.env`, you can paste it directly in the sidebar.

---

## Project Structure

```
RagForAll/
├── main.py                  # Scraper CLI entry point
├── dashboard.py             # Streamlit app
├── requirements.txt
├── .env                     # API keys (not committed)
├── output/                  # Scraped JSONL files
├── chroma_db/               # Persistent ChromaDB vector store
├── scraper/
│   ├── __init__.py
│   ├── url_discovery.py
│   ├── crawler.py
│   ├── filters.py
│   └── chunker.py
└── retrieval/
    ├── ingest.py
    └── query.py
```

---

## How It Works

### Scraping
- **URL discovery** tries `/sitemap.xml` and `/sitemap_index.xml` first, then supplements with link extraction from the root page.
- **Crawling** uses Crawl4AI's `AsyncWebCrawler` with `MemoryAdaptiveDispatcher` to run up to N Chromium sessions in parallel, automatically throttling when RAM exceeds the threshold.
- Pages are converted to Markdown using `DefaultMarkdownGenerator` with `PruningContentFilter` to remove boilerplate.

### Filtering
Pages are dropped if they are shorter than 200 characters, have fewer than 40 words, are non-English (detected via character script analysis + `langdetect`), or have a non-English URL path segment.

### Chunking
Pages are split at heading boundaries (H1–H3), then each section is broken into ~1800-character chunks with 200-character overlap, preserving the full `heading_path` (`Title > Section > Subsection`) as metadata.

### Retrieval
Chunks are embedded with `all-MiniLM-L6-v2` and stored in ChromaDB with cosine similarity. At query time, the top-k nearest chunks are retrieved and injected as context into the LLM prompt.

---

## Extending

- **Swap the LLM**: Change `GROQ_MODEL` in `retrieval/query.py` and `dashboard.py`, or replace the Groq client with any OpenAI-compatible API.
- **Swap the embedding model**: Change `EMBED_MODEL` in `retrieval/ingest.py`. Recreate the collection with `--reset` if you change the model.
- **Add authentication / login flows**: Pass `js_code` or cookies via `CrawlerRunConfig` in `scraper/crawler.py`.
- **Export chunks**: The `output/*.jsonl` files are plain JSON Lines — each line is a chunk dict with `url`, `title`, `heading_path`, `text`, `source_domain`, and `crawled_at`.