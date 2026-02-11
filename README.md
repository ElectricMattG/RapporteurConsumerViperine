# RapporteurApp

A RAG intelligence platform that combines vector search over research indices with LLM-grounded analysis. Query your document corpus through a web UI, get cited responses, and publish findings as PDF reports.

## Architecture

```
Browser (port 5001)
   │
   ▼
frontend-service (Flask)  ──▶  rag-service (FastAPI, port 8000)
                                    │
                                    ├── Embedding service (vector search)
                                    ├── Gemini 2.5 Flash (LLM)
                                    └── JSONL indices (summaries + QA pairs)
```

Kafka handles async ingestion (web scraping, summarisation, indexing) but is **not required** for querying.

## Quick Start

### Prerequisites

- Docker & Docker Compose
- GCP service account key with Vertex AI access (for Gemini)
- Research index files (`.jsonl` with embeddings)

### 1. Start Kafka (optional — only needed for ingestion pipeline)

```bash
docker-compose -f docker-compose.kafka.yml up -d
```

### 2. Start the app

```bash
docker-compose -f docker-compose.services.yml up -d --build
```

This brings up:

| Service | URL | Purpose |
|---------|-----|---------|
| **frontend-service** | http://localhost:5001 | Web UI |
| **rag-service** | http://localhost:8000 | RAG API |

### 3. Open the UI

Navigate to **http://localhost:5001**. Type a query and hit send.

## Configuration

Click the **gear icon** in the UI to open the settings blade. From there you can adjust:

- **Index metadata** — name, version, description, source, coverage, locale
- **Query parameters** — top-K for summaries, QA inclusion, QA top-K
- **Display** — citation debug mode

Settings persist in your browser's localStorage and are included in PDF exports.

## PDF Publishing

Each assistant response has a 📥 button to export as a styled PDF with:

- Auto-generated introduction
- Resolved inline citations linked to source URLs
- Full reference list
- Document metadata from the settings blade

## Project Structure

```
src/
├── clients/           # LLM and embedding API clients
├── components/        # Orchestrator, web searcher, scrapers, chunkers
├── consumers/         # Kafka consumers (scraper, summarizer, indexer)
├── services/
│   ├── rag/           # FastAPI backend — vector search + LLM grounding
│   └── frontend/      # Flask frontend — UI, PDF generation
├── models/            # Data models
├── monitoring/        # Prometheus metrics
├── prompts/           # LLM prompt templates
└── utils/             # Shared utilities
```

## Environment Variables

Key variables (defaults shown):

```bash
GCP_PROJECT_ID=rapporteurclients
GCP_LOCATION=us-central1
GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
EMBEDDING_SERVICE_URL=http://embeddings.192-168-1-3.nip.io
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

## Stopping

```bash
docker-compose -f docker-compose.services.yml down
docker-compose -f docker-compose.kafka.yml down      # if running
```

## License

Private — not for redistribution.
