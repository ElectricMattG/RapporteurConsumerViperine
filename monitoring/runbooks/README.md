# Alert Runbooks — Viperine Pipeline

> **Status:** Provisional — to be validated and expanded as alerts fire in production.
>
> Each alert has a dedicated runbook page linked from the Prometheus alert `runbook_url` annotation.

## Critical Alerts

| Alert | Runbook | Trigger |
|-------|---------|---------|
| ConsumerDown | [consumer_down.md](consumer_down.md) | Consumer state != 1 for 2m |
| ProcessingFailureRateHigh | [processing_failure_rate_high.md](processing_failure_rate_high.md) | Failure rate > 10% for 5m |
| ConsumerLagHigh | [consumer_lag_high.md](consumer_lag_high.md) | Lag > 50 messages for 5m |
| LLMCostBurnRateHigh | [llm_cost_burn_rate_high.md](llm_cost_burn_rate_high.md) | Cost > $5/h |
| LLMErrorRateHigh | [llm_error_rate_high.md](llm_error_rate_high.md) | LLM error rate > 10% for 10m |
| ProduceFailuresSustained | [produce_failures_sustained.md](produce_failures_sustained.md) | Produce errors > 0.1/s for 5m |
| ConsumerRestartLooping | [consumer_restart_looping.md](consumer_restart_looping.md) | > 3 restarts in 1h |
| DiskUsageCritical | [disk_usage_critical.md](disk_usage_critical.md) | Disk > 95% for 2m |

## Warning Alerts

| Alert | Runbook | Trigger |
|-------|---------|---------|
| DLQRateHigh | [dlq_rate_high.md](dlq_rate_high.md) | DLQ > 1 msg/min for 10m |
| EmbeddingLatencyHigh | [embedding_latency_high.md](embedding_latency_high.md) | Embedding p95 > 10s for 5m |
| SessionStuck | [session_stuck.md](session_stuck.md) | Sessions open but no processing for 10m |
| DiskUsageWarning | [disk_usage_warning.md](disk_usage_warning.md) | Disk > 80% for 5m |
| ConsumerStale | [consumer_stale.md](consumer_stale.md) | No message processed in 30m |
| LLMLatencyHigh | [llm_latency_high.md](llm_latency_high.md) | LLM p95 > 60s for 10m |
