# RapporteurConsumerViperine

Kafka consumer group for the Rapporteur ingestion pipeline. Scrapes web sources, summarises content, indexes embeddings, and routes to the RAG and frontend services via Kafka topics.

> **Sibling repos**
> - RAG service: `RapportuerRAG`
> - Frontend: `RapportuerFrontEnd`
>
> **Helm / monitoring removed in this repo** — recoverable from git SHA `412ba42`

## Pipeline

```
web-scrape-requests
        │
        ▼
kafka_web_scraper  ──▶  content-extracted
                                │
                                ▼
                        kafka_summarizer  ──▶  summary-completed
                                                        │
                                                        ▼
                                              kafka_summary_indexer  ──▶  reload-index
                                                                      └──▶  frontend-message

Optional (ENABLE_AGGREGATOR=true):
        kafka_index_aggregator  (reads reload-index, writes merged index)

Independent:
        kafka_dead_letter_consumer  (reads dead-letter-queue)
```

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `web-scrape-requests` | external | `kafka_web_scraper` |
| `content-extracted` | `kafka_web_scraper` | `kafka_summarizer` |
| `summary-completed` | `kafka_summarizer` | `kafka_summary_indexer` |
| `reload-index` | `kafka_summary_indexer` | RAG service / `kafka_index_aggregator` |
| `frontend-message` | `kafka_summary_indexer` | frontend service |
| `dead-letter-queue` | all consumers | `kafka_dead_letter_consumer` |

## Quick Start

### 1. Start Kafka

```bash
make kafka_start        # starts kafka + zookeeper + kafka-ui
make kafka_topics       # creates all required topics
# or combined:
make kafka_setup
```

Kafka UI: http://localhost:8090

### 2. Configure environment

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export OPENAI_API_KEY=...
export EMBEDDING_SERVICE_URL=http://embeddings.192-168-1-3.nip.io
# For Gemini provider:
export GCP_PROJECT_ID=rapporteurclients
export GCP_LOCATION=us-central1
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

### 3. Start consumers

```bash
./restart_pipeline.sh
```

Consumers expose Prometheus `/metrics` on ports 8001, 8010, etc.

## Project Structure

```
src/
├── clients/       # LLM and embedding API clients
├── components/    # Web searcher, URL resolver, summarise pipeline
├── consumers/     # Kafka consumers (scraper, summarizer, indexer, aggregator, DLQ)
├── config.py      # pyconfs config
├── config_handler.py  # Base class for all consumers
├── monitoring/    # Prometheus metrics exporters (in-process)
└── utils/         # index_adapter, async_file_io
```

## Make Targets

| Target | Purpose |
|--------|---------|
| `make kafka_start` | Start Kafka + Zookeeper + UI |
| `make kafka_stop` | Stop Kafka |
| `make kafka_topics` | Create all topics |
| `make kafka_setup` | Start + create topics |
| `make kafka_flush` | Delete/recreate topics + clear cache |
| `make install` | Install Python dependencies |
| `make requirements` | Recompile requirements.txt |
| `make sync` | pip-sync all dependencies |
| `make docker_build` | Build consumer Docker image |
| `make docker_tag` | Tag image for ACR |
| `make docker_push` | Push image to ACR |
| `make gcp_gemini` | Set up GCP auth for Gemini |

## License

Private — not for redistribution.
