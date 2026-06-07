# Viperine Consumers — Metrics Server & Grafana Dashboard Plan

> **Goal:** Stand up comprehensive Prometheus instrumentation and a Grafana dashboard
> for all Viperine Kafka consumers (including the dead-letter consumer), modeled on the
> proven **Kaliya / Lamia** reference observability stack, so we can isolate issues across
> clients, caches, tokens, calls, index summaries, failures and latency.
>
> Viperine consumers are an **independent consumer group**. Kaliya/Lamia are referenced
> only as a design template — they share no topics, namespaces, or metrics with Viperine.

---

## 1. Scope

Instrument and dashboard the following **5 consumers**:

| Consumer | Module | k8s Deployment | Role / pipeline stage |
|---|---|---|---|
| Web Scraper | `kafka_web_scraper.py` | `viperine-web-scraper` | URL → `content-extracted` |
| Summarizer | `kafka_summarizer.py` | `viperine-summarizer` | `content-extracted` → `summary-completed` |
| Summary Indexer | `kafka_summary_indexer.py` | `viperine-summary-indexer` | `summary-completed` → index / `session-complete` |
| Index Aggregator | `kafka_index_aggregator.py` | *(not deployed in k8s)* | session aggregation → Azure backup |
| Dead-Letter | `kafka_dead_letter_consumer.py` | `viperine-dead-letter-consumer` | `dead-letter-queue` → retry / discard |

---

## 2. Current State (audit findings)

The instrumentation scaffolding **already exists** but is partially wired and contains
deployment bugs. The reference stack (`RapporteurConsumers`) is the template we converge to.

### 2.1 What already exists

- `src/monitoring/` modules (shared lineage with the reference repo):
  - `exporters/metrics_server.py` — `MetricsServer(port, consumer_name, health_check, ready_check)` serving `/metrics`, `/health`, `/ready`.
  - `prometheus_metrics.py` — `PrometheusMetrics(consumer_id, namespace="kafka_consumer")`: messages_consumed, processing_duration histogram, failures, deserialization/connection/timeout errors, retries, dead_letter, state gauge, info.
  - `kafka_metrics.py` — `KafkaMetricsWrapper`: lag, poll_duration, batch_size, rebalances, offset_commit, messages_in_flight, message_age.
  - `system_metrics.py` — `SystemMetricsCollector` (psutil): process/system CPU & memory.
  - `cache_metrics.py` — **STUB, unimplemented** (TODO only).
  - `azure_metrics.py`, `logging_config.py`.
- All 5 consumers already: gate on `TELEMETRY_AVAILABLE` / `self.telemetry_enabled`, instantiate `MetricsServer` + `PrometheusMetrics` + `SystemMetricsCollector` + `KafkaMetricsWrapper`, and define per-consumer custom `kafka_consumer_*` metrics (see §4).

### 2.2 Gaps & bugs to fix (these are why we can't isolate issues today)

| # | Problem | Impact | Fix |
|---|---|---|---|
| **B1** | **Port mismatch — summarizer.** Code (`kafka_summarizer.py`) exposes metrics on **8010**, but the k8s Service `summarizer` and the Prometheus target both point to **8006**. | Summarizer metrics scraped on the wrong port → **no data**. | Reconcile to one canonical port (§5.1). |
| **B2** | **Summary Indexer not scraped.** Code uses **8006**, but there is **no `summary-indexer` Service** and no Prometheus target. (8006 is currently claimed by the `summarizer` Service.) | Indexer = where the duplicate-`doc_id` issue surfaces → **completely blind**. | Add Service + scrape target (§5). |
| **B3** | **No `prometheus.io/*` annotations** on consumer Services; Prometheus uses **static targets** only. | New/renamed consumers silently un-scraped; brittle. | Add annotations + (optionally) `kubernetes_sd` relabel job (§6). |
| **B4** | **`cache_metrics.py` is a stub.** Cache hit/miss/eviction/age metrics are not emitted. | Cache effectiveness (web-scraper, embeddings) invisible. | Implement collector (§4.6, §7 Phase 2). |
| **B5** | **`kafka_metrics.py` double-namespace bug** (`kafka_consumer_kafka_consumer_*`). | Awkward metric names; dashboard queries must double-prefix. | Keep queries matching actual emitted names; optionally fix wrapper. |
| **B6** | **No token/cost metrics** for the summarizer LLM calls beyond `llm_tokens_used_total` (no cost USD, no input/output split, no per-model latency). | Can't track LLM spend or latency regressions. | Add token/cost metrics (§4.2). |

> **Canonical port decision required** — see §5.1. The plan assumes we keep the
> **code ports** as source of truth and fix Services/Prometheus to match, because changing
> code ports requires redeploying images while Service/scrape edits are config-only… but
> the code ports are what the running pods actually expose, so config is the cheaper fix.

---

## 3. Metrics Taxonomy (the categories you asked for)

Every consumer emits a **common core** (from the shared modules) plus **stage-specific**
metrics. Categories requested map as follows:

| Category | Metrics family | Source |
|---|---|---|
| **Clients** | consumer `state` gauge, `up`, connection/timeout errors, rebalances, kafka client lag | `prometheus_metrics`, `kafka_metrics` |
| **Caches** | cache files/size/age, hit/miss/eviction, cleanup duration | `cache_metrics` (to implement) + web-scraper cache gauges |
| **Tokens** | LLM input/output tokens, estimated cost USD (per model/stage) | summarizer custom metrics |
| **Calls** | LLM/API call counts & latency, Azure upload calls, embedding calls | per-consumer custom metrics |
| **Index summaries** | summaries indexed, sessions completed, summaries/session, dedup/duplicates filtered | summary-indexer + aggregator |
| **Failures** | processing failures, deserialization errors, DLQ messages, errors by stage | core + DLQ consumer |
| **Latency** | processing duration, stage latency, poll duration, message age, embedding duration | core + custom histograms |
| **System** | process/system CPU & memory | `system_metrics` |

### 3.1 Naming & labels

- Keep the shared namespace **`kafka_consumer`** for the common core (already emitted, already on the reference *Kafka Consumers Overview* dashboard).
- For Viperine-specific dashboards, scope panels by the **`consumer`** Prometheus job label (set per scrape target) and the **`consumer_id`** metric label.
- Standard label set: `consumer_id`, `topic`, `status`, `stage`, `error_type`, `model`, `reason`, `partition`.
- Histogram buckets follow the reference: `[0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30, 60]` for sub-minute ops; `[1, 5, 10, 30, 60, 120, 300, 600]` for LLM/embedding ops.

---

## 4. Per-Consumer Metric Specifications

Legend: ✅ already emitted · ➕ to add · 🛠 fix/implement

### 4.0 Common core (all 5 consumers)

```
✅ kafka_consumer_messages_consumed_total{consumer_id,topic}
✅ kafka_consumer_message_processing_duration_seconds{consumer_id,topic,status}   (histogram)
✅ kafka_consumer_processing_failures_total{consumer_id,topic,error_type}
✅ kafka_consumer_deserialization_errors_total / _connection_errors_total / _timeout_errors_total
✅ kafka_consumer_retry_attempts_total{consumer_id,topic}
✅ kafka_consumer_dead_letter_messages_total{consumer_id,topic,reason}
✅ kafka_consumer_state{consumer_id}                       (0=stopped,1=running,2=error)
✅ kafka_consumer_info
✅ kafka_consumer_kafka_consumer_lag_messages{consumer_id,topic,partition}
✅ kafka_consumer_kafka_consumer_message_age_seconds (histogram)
✅ kafka_consumer_kafka_consumer_poll_duration_seconds (histogram)
✅ kafka_consumer_kafka_consumer_messages_in_flight
✅ kafka_consumer_kafka_consumer_rebalances_total
✅ kafka_consumer_kafka_consumer_offset_commit_failures_total
✅ kafka_consumer_process_cpu_percent / _process_memory_percent / _process_memory_rss_bytes
✅ kafka_consumer_system_cpu_percent / _system_memory_percent
➕ kafka_consumer_last_successful_poll_timestamp  (already in wrapper — surface as freshness panel)
```

### 4.1 Web Scraper (`port 8001`)

```
✅ kafka_consumer_urls_scraped_total{consumer_id}
✅ kafka_consumer_scraping_duration_seconds{consumer_id,status}   (histogram)
✅ kafka_consumer_cache_files_total{consumer_id}
✅ kafka_consumer_cache_size_bytes{consumer_id}
➕ kafka_consumer_scrape_strategy_total{consumer_id,strategy}      (cloudscraper/playwright/curl_cffi/error)
➕ kafka_consumer_scrape_http_status_total{consumer_id,code}
➕ kafka_consumer_html_bytes_fetched_total{consumer_id,strategy}
➕ kafka_consumer_cf_challenge_detected_total{consumer_id,strategy}
```

### 4.2 Summarizer (`port 8010`)  ← token & cost focus

```
✅ kafka_consumer_documents_summarized_total{consumer_id}
✅ kafka_consumer_summarization_duration_seconds{consumer_id,status}  (histogram)
✅ kafka_consumer_llm_api_calls_total{consumer_id,provider}
✅ kafka_consumer_llm_api_errors_total{consumer_id,provider}
✅ kafka_consumer_llm_tokens_used_total{consumer_id}
✅ kafka_consumer_summary_length_chars{consumer_id}                    (histogram)
➕ kafka_consumer_llm_input_tokens_total{consumer_id,model}
➕ kafka_consumer_llm_output_tokens_total{consumer_id,model}
➕ kafka_consumer_llm_estimated_cost_usd_total{consumer_id,model}
➕ kafka_consumer_llm_latency_seconds{consumer_id,model}              (histogram)
```

### 4.3 Summary Indexer (`port 8006`)  ← index-summary focus + duplicate-doc_id visibility

```
✅ kafka_consumer_summaries_indexed_total{consumer_id}
✅ kafka_consumer_summary_sessions_completed_total{consumer_id}
✅ kafka_consumer_summary_embedding_duration_seconds{consumer_id}     (histogram)
✅ kafka_consumer_summaries_per_session{consumer_id}                   (histogram)
✅ kafka_consumer_summary_duplicates_detected_total{consumer_id,match} (document_id / dedup_key / both)
➕ kafka_consumer_summary_id_scheme_total{consumer_id,scheme}          (req_ / doc_ / uuid_legacy)  ← surfaces the rogue producer
➕ kafka_consumer_index_write_duration_seconds{consumer_id}            (histogram)
➕ kafka_consumer_index_records_total{consumer_id,index}
```

> **Duplicates tracking:** `summary_duplicates_detected_total` is incremented in the
> combined-index aggregation loop whenever an entry is skipped because its `document_id`
> or `dedup_key` already exists. The `match` label (`document_id` / `dedup_key` / `both`)
> distinguishes id-collision duplicates from URL-normalized duplicates — directly
> exposing the duplicate-`doc_id` pattern as it happens.

> **B-link:** `summary_id_scheme_total{scheme="doc_"}` going non-zero is the *direct
> signal* for the deferred duplicate-`doc_id` bug (a foreign producer emitting `doc_<hash>`
> records onto `summary-completed`). This makes the issue observable instead of forensic.

### 4.4 Index Aggregator (`port 8008`, not yet in k8s)

```
✅ kafka_consumer_chunks_aggregated_total / qa_pairs_aggregated_total{consumer_id}
✅ kafka_consumer_chunks_duplicates_filtered_total / qa_duplicates_filtered_total
✅ kafka_consumer_azure_upload_duration_seconds (histogram)
✅ kafka_consumer_session_backup_duration_seconds (histogram)
✅ kafka_consumer_azure_storage_bytes{consumer_id}
✅ kafka_consumer_sessions_aggregated_total{consumer_id}
➕ kafka_consumer_azure_upload_failures_total{consumer_id,error_type}
```

### 4.5 Dead-Letter Consumer (`port 8009`)

```
✅ kafka_consumer_dlq_messages_processed_total{consumer_id}
✅ kafka_consumer_dlq_retry_attempts_total{consumer_id}
✅ kafka_consumer_dlq_messages_reprocessed_total{consumer_id}
✅ kafka_consumer_dlq_messages_discarded_total{consumer_id}
✅ kafka_consumer_dlq_failures_by_stage{consumer_id,stage}            (gauge)
✅ kafka_consumer_dlq_processing_duration_seconds{consumer_id}        (histogram)
➕ kafka_consumer_dlq_messages_by_origin_total{consumer_id,origin_topic}
➕ kafka_consumer_dlq_age_seconds{consumer_id}                        (histogram — time spent in DLQ before reprocess)
```

### 4.6 Cache metrics (implement the stub — applies to web-scraper & embedding caches)

```
🛠 kafka_consumer_cache_files_total{consumer_id,cache}
🛠 kafka_consumer_cache_size_bytes{consumer_id,cache}
🛠 kafka_consumer_cache_age_seconds{consumer_id,cache}                (oldest entry)
🛠 kafka_consumer_cache_hit_total{consumer_id,cache}
🛠 kafka_consumer_cache_miss_total{consumer_id,cache}
🛠 kafka_consumer_cache_eviction_total{consumer_id,cache}
🛠 kafka_consumer_cache_cleanup_duration_seconds{consumer_id,cache}   (histogram)
```

---

## 5. Metrics Server & Port Allocation

Each consumer already starts a `MetricsServer` daemon thread on `0.0.0.0:<port>` exposing
`/metrics`, `/health`, `/ready`. Canonical allocation (**= the code ports today**):

### 5.1 Canonical port map (single source of truth)

| Consumer | Code port (authoritative) | Current Service port | Current scrape target | Action |
|---|---|---|---|---|
| web-scraper | **8001** | 8001 ✅ | 8001 ✅ | none |
| summarizer | **8010** | 8006 ❌ | 8006 ❌ | **Service+scrape → 8010** |
| summary-indexer | **8006** | *(missing)* ❌ | *(missing)* ❌ | **Add Service+scrape → 8006** |
| index-aggregator | **8008** | 8008 ✅ | 8008 ✅ | none (deploy when k8s-hosted) |
| dead-letter | **8009** | 8009 ✅ | 8009 ✅ | verify |

> Alternative (heavier): change code ports to match the existing Service map and rebuild
> the image. Rejected — config-only fix is faster and the running pods already expose the
> code ports.

---

## 6. Kubernetes Service & Prometheus Scrape Design

### 6.1 Services (namespace `consumers`)

Add/fix one `ClusterIP` Service per consumer with **scrape annotations** so discovery is
declarative (fixes **B3**):

```yaml
apiVersion: v1
kind: Service
metadata:
  name: summary-indexer            # NEW (fixes B2)
  namespace: consumers
  labels: { app: summary-indexer, component: consumer, metrics: "true" }
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8006"
    prometheus.io/path: "/metrics"
spec:
  type: ClusterIP
  selector: { app: summary-indexer }
  ports:
    - { name: metrics, port: 8006, targetPort: 8006, protocol: TCP }
```

Apply the same pattern; correct the **summarizer** Service to port **8010** (fixes **B1**).

### 6.2 Prometheus scrape config (two options)

**Option A — explicit static targets** (matches current `helm/prometheus/values.yaml` style):

```yaml
- job_name: 'viperine_summary_indexer'
  metrics_path: /metrics
  static_configs:
    - targets: ['summary-indexer.consumers.svc.cluster.local:8006']
      labels: { consumer: 'summary_indexer', pipeline: 'viperine' }
- job_name: 'viperine_summarizer'
  static_configs:
    - targets: ['summarizer.consumers.svc.cluster.local:8010']
      labels: { consumer: 'summarizer', pipeline: 'viperine' }
# ... web_scraper:8001, index_aggregator:8008, dead_letter:8009
```

**Option B — annotation-based service discovery** (preferred, self-maintaining):

```yaml
- job_name: 'viperine-consumers'
  kubernetes_sd_configs:
    - role: endpoints
      namespaces: { names: ['consumers'] }
  relabel_configs:
    - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_scrape]
      action: keep; regex: "true"
    - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_port,
                      __meta_kubernetes_pod_ip]
      action: replace; regex: (.+);(.+); replacement: $2:$1; target_label: __address__
    - source_labels: [__meta_kubernetes_service_name]
      target_label: consumer
    - replacement: 'viperine'; target_label: pipeline
```

Add a `pipeline="viperine"` label on every target so the dashboard cleanly isolates
Viperine from Kaliya/Lamia/bboa on the shared Prometheus.

---

## 7. Grafana Dashboard Design — "Viperine Pipeline Overview"

File: `helm/grafana/dashboards/viperine-pipeline-overview.json`, auto-provisioned via the
existing `grafana/provisioning/dashboards` provider. Modeled on `lamia-image-pipeline.json`
(rows + stat + timeseries + piechart + gauge). All queries filtered by `pipeline="viperine"`.

### 7.1 Template variables

- `$consumer` — `label_values(kafka_consumer_state{pipeline="viperine"}, consumer)` (multi).
- `$topic` — `label_values(kafka_consumer_messages_consumed_total, topic)`.
- `$interval` — rate window (default `5m`).

### 7.2 Rows & panels

**Row: Pipeline Health (top-line stats)**
| Panel | Type | PromQL |
|---|---|---|
| Consumers Up | stat | `sum(kafka_consumer_state{pipeline="viperine"} == 1)` |
| Messages Consumed (total) | stat | `sum(kafka_consumer_messages_consumed_total{pipeline="viperine"})` |
| Failures (total) | stat | `sum(kafka_consumer_processing_failures_total{pipeline="viperine"})` |
| Dead Letters | stat | `sum(kafka_consumer_dlq_messages_processed_total)` |
| Total Consumer Lag | stat | `sum(kafka_consumer_kafka_consumer_lag_messages{pipeline="viperine"})` |

**Row: Throughput & Latency (per consumer)**
| Panel | Type | PromQL |
|---|---|---|
| Consume Rate /s | timeseries | `sum by (consumer) (rate(kafka_consumer_messages_consumed_total[$interval]))` |
| Processing p50/p95/p99 | timeseries | `histogram_quantile(0.95, sum by (consumer,le)(rate(kafka_consumer_message_processing_duration_seconds_bucket[$interval])))` (×3 quantiles) |
| Messages In Flight | timeseries | `kafka_consumer_kafka_consumer_messages_in_flight` |
| Poll Duration p95 | timeseries | `histogram_quantile(0.95, sum by (consumer,le)(rate(kafka_consumer_kafka_consumer_poll_duration_seconds_bucket[$interval])))` |

**Row: Client / Kafka Health**
| Panel | Type | PromQL |
|---|---|---|
| Consumer State | state-timeline | `kafka_consumer_state` |
| Consumer Lag by partition | timeseries | `kafka_consumer_kafka_consumer_lag_messages` |
| Message Age p95 | timeseries | `histogram_quantile(0.95, sum by (consumer,le)(rate(kafka_consumer_kafka_consumer_message_age_seconds_bucket[$interval])))` |
| Rebalances | timeseries | `rate(kafka_consumer_kafka_consumer_rebalances_total[$interval])` |
| Last Poll Freshness (s) | stat | `time() - kafka_consumer_last_successful_poll_timestamp` |
| Offset Commit Failures | timeseries | `rate(kafka_consumer_kafka_consumer_offset_commit_failures_total[$interval])` |

**Row: Failures & Errors**
| Panel | Type | PromQL |
|---|---|---|
| Processing Failures by type | timeseries | `sum by (error_type)(rate(kafka_consumer_processing_failures_total[$interval]))` |
| Deserialization / Connection / Timeout errors | timeseries | `rate(kafka_consumer_deserialization_errors_total[$interval])` (+conn/timeout) |
| Error Ratio | gauge | `sum(rate(kafka_consumer_processing_failures_total[$interval])) / sum(rate(kafka_consumer_messages_consumed_total[$interval]))` |

**Row: Caches**
| Panel | Type | PromQL |
|---|---|---|
| Cache Size | gauge | `kafka_consumer_cache_size_bytes` |
| Cache Files | gauge | `kafka_consumer_cache_files_total` |
| Cache Hit Rate | gauge | `sum(rate(kafka_consumer_cache_hit_total[$interval])) / (sum(rate(kafka_consumer_cache_hit_total[$interval])) + sum(rate(kafka_consumer_cache_miss_total[$interval])))` |
| Cache Evictions | timeseries | `rate(kafka_consumer_cache_eviction_total[$interval])` |
| Oldest Cache Entry Age | stat | `max(kafka_consumer_cache_age_seconds)` |

**Row: Tokens & LLM Cost (Summarizer)**
| Panel | Type | PromQL |
|---|---|---|
| LLM Calls /s | timeseries | `rate(kafka_consumer_llm_api_calls_total[$interval])` |
| LLM Errors | timeseries | `rate(kafka_consumer_llm_api_errors_total[$interval])` |
| LLM Latency p95 by model | timeseries | `histogram_quantile(0.95, sum by (model,le)(rate(kafka_consumer_llm_latency_seconds_bucket[$interval])))` |
| Input/Output Tokens /s | timeseries | `rate(kafka_consumer_llm_input_tokens_total[$interval])` & `_output_tokens_total` |
| Estimated Cost (USD) | stat | `sum(kafka_consumer_llm_estimated_cost_usd_total) by (model)` |
| Cost Rate (24h) | timeseries | `rate(kafka_consumer_llm_estimated_cost_usd_total[24h])` |

**Row: Index Summaries (Summary Indexer)**
| Panel | Type | PromQL |
|---|---|---|
| Summaries Indexed /s | timeseries | `rate(kafka_consumer_summaries_indexed_total[$interval])` |
| Sessions Completed | stat | `sum(kafka_consumer_summary_sessions_completed_total)` |
| Summaries per Session | timeseries | `histogram_quantile(0.95, sum by (le)(rate(kafka_consumer_summaries_per_session_bucket[$interval])))` |
| Embedding Duration p95 | timeseries | `histogram_quantile(0.95, sum by (le)(rate(kafka_consumer_summary_embedding_duration_seconds_bucket[$interval])))` |
| **ID Scheme split** (doc_ vs req_ vs uuid) | piechart | `sum by (scheme)(kafka_consumer_summary_id_scheme_total)` |
| Duplicates Detected /s | timeseries | `sum by (match)(rate(kafka_consumer_summary_duplicates_detected_total[$interval]))` |

**Row: Dead-Letter Queue**
| Panel | Type | PromQL |
|---|---|---|
| DLQ Processed /s | timeseries | `rate(kafka_consumer_dlq_messages_processed_total[$interval])` |
| Reprocessed vs Discarded | timeseries | `rate(kafka_consumer_dlq_messages_reprocessed_total[$interval])` & `_discarded_total` |
| Failures by Stage | bargauge | `kafka_consumer_dlq_failures_by_stage` |
| DLQ Processing Duration p95 | timeseries | `histogram_quantile(0.95, sum by (le)(rate(kafka_consumer_dlq_processing_duration_seconds_bucket[$interval])))` |
| DLQ by Origin Topic | piechart | `sum by (origin_topic)(kafka_consumer_dlq_messages_by_origin_total)` |

**Row: Aggregator & Azure**
| Panel | Type | PromQL |
|---|---|---|
| Sessions Aggregated /s | timeseries | `rate(kafka_consumer_sessions_aggregated_total[$interval])` |
| Azure Upload Duration p95 | timeseries | `histogram_quantile(0.95, sum by (le)(rate(kafka_consumer_azure_upload_duration_seconds_bucket[$interval])))` |
| Azure Storage Bytes | stat | `kafka_consumer_azure_storage_bytes` |
| Azure Upload Failures | timeseries | `rate(kafka_consumer_azure_upload_failures_total[$interval])` |

**Row: System Resources**
| Panel | Type | PromQL |
|---|---|---|
| Process CPU % | timeseries | `kafka_consumer_process_cpu_percent` |
| Process Memory % | timeseries | `kafka_consumer_process_memory_percent` |
| Process RSS | timeseries | `kafka_consumer_process_memory_rss_bytes` |

---

## 8. Alerting Rules (Prometheus)

Ship as `helm/prometheus/templates/viperine-alerts.yaml`:

| Alert | Expr (sketch) | For | Severity |
|---|---|---|---|
| ConsumerDown | `kafka_consumer_state{pipeline="viperine"} != 1` | 2m | critical |
| ConsumerStalled | `time() - kafka_consumer_last_successful_poll_timestamp > 300` | 5m | critical |
| HighConsumerLag | `kafka_consumer_kafka_consumer_lag_messages > 1000` | 10m | warning |
| HighErrorRatio | `sum(rate(failures[5m]))/sum(rate(consumed[5m])) > 0.05` | 10m | warning |
| DLQGrowing | `rate(kafka_consumer_dlq_messages_processed_total[15m]) > 0` | 15m | warning |
| RogueDocIdProducer | `increase(kafka_consumer_summary_id_scheme_total{scheme="doc_"}[10m]) > 0` | 0m | warning |
| DuplicateDocIdSpike | `increase(kafka_consumer_summary_duplicates_detected_total{match="document_id"}[10m]) > 0` | 0m | warning |
| LLMCostSpike | `rate(kafka_consumer_llm_estimated_cost_usd_total[1h]) > <budget>` | 1h | warning |
| TargetMissing | `up{pipeline="viperine"} == 0` | 2m | critical |

---

## 9. Implementation Phases

**Phase 0 — Reconcile scrape plumbing (config-only, no rebuild)** *(fixes B1, B2, B3)*
1. Fix `summarizer` Service → port **8010**.
2. Add `summary-indexer` Service → port **8006**.
3. Add `prometheus.io/{scrape,port,path}` annotations to all consumer Services.
4. Add Viperine scrape jobs (Option A) or SD job (Option B) + `pipeline="viperine"` label.
5. Verify: `up{pipeline="viperine"} == 1` for all 5; `/metrics` reachable per pod.

**Phase 1 — Dashboard MVP**
6. Author `viperine-pipeline-overview.json` with rows: Health, Throughput/Latency,
   Client/Kafka, Failures, System (all backed by **already-emitted** metrics).
7. Provision via Grafana dashboards provider; confirm panels populate.

**Phase 2 — Fill metric gaps** *(requires code + image rebuild)*
8. Implement `cache_metrics.py` (B4); wire web-scraper + embedding caches.
9. Add summarizer token/cost/latency metrics (B6).
10. Add summary-indexer `summary_id_scheme_total` + dedup metrics (makes duplicate-doc_id observable).
11. Add web-scraper strategy/HTTP/CF metrics; aggregator azure-failure; DLQ origin/age.
12. Extend dashboard rows: Caches, Tokens/LLM, Index Summaries, DLQ, Aggregator.

**Phase 3 — Alerting & hardening**
13. Add Prometheus alert rules (§8); route to existing Alertmanager/notification path.
14. Optional: fix `kafka_metrics.py` double-namespace (B5) — coordinate a metric rename
    migration (update dashboard queries in lockstep).

---

## 10. Validation Checklist

- [ ] `curl http://<pod-ip>:<port>/metrics` returns Prometheus text for each consumer.
- [ ] `/health` → 200 when running, `/ready` → 200 when consumer connected.
- [ ] Prometheus `Targets` page: 5/5 Viperine targets **UP** with `pipeline="viperine"`.
- [ ] Each dashboard row renders non-empty under load (run a small batch through pipeline).
- [ ] Kill one consumer → `ConsumerDown` fires and stat panel reflects it.
- [ ] Inject a DLQ message → DLQ row + `DLQGrowing` react.
- [ ] Force a `doc_`-scheme summary → `RogueDocIdProducer` fires (ties back to deferred bug).

---

## 11. File Manifest (what this plan will create/edit)

| Path | Action |
|---|---|
| `helm/research-indexing-consumers/templates/consumer-services.yaml` | Fix summarizer port; add summary-indexer Service; add `prometheus.io/*` annotations |
| `helm/prometheus/values.yaml` / `templates/configmap.yaml` | Add Viperine scrape jobs + `pipeline` label (or SD job) |
| `helm/prometheus/templates/viperine-alerts.yaml` | **new** — alert rules |
| `helm/grafana/dashboards/viperine-pipeline-overview.json` | **new** — dashboard |
| `src/monitoring/cache_metrics.py` | Implement stub |
| `src/consumers/kafka_summarizer.py` | Add token/cost/LLM-latency metrics |
| `src/consumers/kafka_summary_indexer.py` | Add id-scheme metric; duplicates-detected metric (`summary_duplicates_detected_total`) **done** |
| `src/consumers/kafka_web_scraper.py` | Add strategy/HTTP/CF metrics |
| `src/consumers/kafka_index_aggregator.py` | Add azure-failure metric |
| `src/consumers/kafka_dead_letter_consumer.py` | Add origin/age metrics |

---

### Appendix A — Reference stack we modeled on
- `RapporteurConsumers/src/monitoring/{prometheus_metrics,kafka_metrics,system_metrics,cache_metrics}.py`
- `RapporteurConsumers/src/components/bboa/kaliya_metrics.py`, `src/components/image/lamia_metrics.py` (per-consumer namespaced metric modules)
- `RapporteurConsumers/grafana/dashboards/{kaliya-agenda-analysis,lamia-image-pipeline,kafka-consumers-overview}.json`
- `helm/kaliya-consumer/templates/metrics-service.yaml` (annotation + `metrics` port pattern)
