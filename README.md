# RapporteurConsumerViperine

Kafka consumer group for the Rapporteur ingestion pipeline. Scrapes web sources, summarises content, indexes embeddings, and routes to the RAG and frontend services via Kafka topics.

> **Sibling repos**
> - RAG service: `RapportuerRAG`
> - Frontend: `RapportuerFrontEnd`

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

### 3. Deploy to k8s

```bash
make k8s_deploy     # build → push → nvidia-safe rollout → status
make k8s_logs       # tail logs from all consumer pods
```

Consumers expose Prometheus `/metrics` on ports 8001, 8010, etc.

> `restart_pipeline.sh` has been deprecated (local-process / pre-k8s workflow) and moved to `archive/deprecated_restart_pipeline.sh`.

## Project Structure

```
src/
├── clients/          # LLM (Gemini/OpenAI) and embedding API clients
├── components/       # Web searcher, URL resolver, summarise pipeline
├── consumers/        # Kafka consumers (scraper, summarizer, indexer, aggregator, DLQ)
├── monitoring/       # Prometheus metrics exporters (in-process)
│   ├── prometheus_metrics.py        # Base counters + lifecycle helpers
│   ├── kafka_metrics.py             # Lag, poll, produce tracking
│   ├── system_metrics.py            # psutil background thread
│   ├── cost.py                      # LLM cost estimation (pricing table)
│   ├── error_categories.py          # Validated ErrorCategory enum
│   └── exporters/metrics_server.py  # /metrics /health /ready HTTP server
├── config.py         # pyconfs config defaults
├── config_handler.py # Base class for all consumers
└── utils/            # index_adapter, async_file_io
```

```
grafana/
└── dashboards/
    └── viperine-pipeline-overview.json  # 11-section, 63-panel Grafana dashboard

monitoring/
├── prometheus/
│   ├── prometheus.yml  # Prometheus scrape config
│   └── alerts.yml      # 14 alert rules (8 critical, 6 warning)
├── runbooks/           # Per-alert remediation guides
└── MONITORING.md       # Full observability reference

helm/
└── rapporteur-consumer-group/  # Helm chart for k3s deployment (release: viperine)
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
| **k8s deployment** | |
| `make k8s_deploy` | Full release pipeline: build → push → nvidia-safe rollout → status |
| `make k8s_rollout` | Nvidia-safe rollout only (pre-built image) |
| `make k8s_build` | Build + tag image for local registry (192.168.1.3:5000) |
| `make k8s_push` | Push image to local registry |
| `make k8s_status` | Show pod and service status for the consumer group |
| `make k8s_logs` | Tail logs from all consumer pods |
| `make k8s_restart` | Rolling restart of all deployments |
| `make k8s_shell` | Open a shell in a consumer pod |
| `make k8s_prereqs` | One-time bootstrap (namespace, hostPath, GCP credentials) |
| `make k8s_uninstall` | Remove the Helm release |
| **monitoring** | |
| `make monitoring_deploy` | Patch Prometheus + Grafana ConfigMaps and hot-reload (idempotent) |
| `make monitoring_status` | Check Prometheus scrape target health |
| `make monitoring_reload` | Hot-reload Prometheus config (safe — never restarts the pod) |
| `make monitoring_open` | Print Prometheus and Grafana URLs |

## Observability

All consumers expose `/metrics`, `/health`, and `/ready` on their assigned ports. Prometheus scrapes them every 10 s; Grafana renders the dashboard.

| Component | URL |
|-----------|-----|
| Prometheus | http://192.168.1.3:30090 |
| Grafana | http://192.168.1.3:30030 |
| Dashboard | http://192.168.1.3:30030/d/viperine-pipeline-overview |

Deploy or refresh the monitoring stack:

```bash
make monitoring_deploy   # idempotent — patches ConfigMaps + hot-reloads Prometheus
make monitoring_status   # verify all 4 scrape targets are UP
```

See [monitoring/MONITORING.md](monitoring/MONITORING.md) for the full metrics catalogue, alert rules, and runbooks.

---

## License

Private — not for redistribution.
