# RapporteurConsumerViperine

Kafka consumer group for the Rapporteur ingestion pipeline. Scrapes web sources, summarises content, indexes embeddings, and routes to the RAG and frontend services via Kafka topics.

> **Sibling repos**
> - RAG service: `RapportuerRAG`
> - Frontend: `RapportuerFrontEnd`
>
> **Helm / monitoring removed in this repo** — recoverable from git SHA `412ba42`

## Pipeline

```
viperine.web-scrape-requests
        │
        ▼
kafka_web_scraper  ──▶  viperine.content-extracted
                   └──▶  viperine.summarise-document
                                │
                                ▼
                        kafka_summarizer  ──▶  viperine.summary-completed
                                                        │
                                                        ▼
                                              kafka_summary_indexer  ──▶  reload-index
                                                                      └──▶  viperine.frontend-message
                                                                      └──▶  viperine.session-complete

Optional (ENABLE_AGGREGATOR=true):
        kafka_index_aggregator  (reads reload-index, writes merged index)

Independent:
        kafka_dead_letter_consumer  (reads viperine.dead-letter-queue)
```

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| `viperine.web-scrape-requests` | RAG service | `kafka_web_scraper` |
| `viperine.content-extracted` | `kafka_web_scraper` | `kafka_summarizer` |
| `viperine.summarise-document` | `kafka_web_scraper` | `kafka_summarizer` |
| `viperine.summary-completed` | `kafka_summarizer` | `kafka_summary_indexer` |
| `viperine.session-complete` | `kafka_summary_indexer` | RAG service |
| `viperine.frontend-message` | `kafka_summary_indexer` | frontend service |
| `viperine.dead-letter-queue` | all consumers | `kafka_dead_letter_consumer` |
| `reload-index` | `kafka_summary_indexer` | RAG service / `kafka_index_aggregator` |

> **Note:** All `viperine.*` topics are namespaced to isolate this pipeline from other consumers (kaliya/lamia) sharing the same Kafka cluster.

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
| `make kafka_create_topics` | Create all `viperine.*` topics (idempotent) |
| `make kafka_list_topics` | List topics on the broker |
| `make kafka_delete_old_topics` | Delete legacy un-namespaced topics after validation |
| `make kafka_setup` | Start + create topics (local dev) |
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
