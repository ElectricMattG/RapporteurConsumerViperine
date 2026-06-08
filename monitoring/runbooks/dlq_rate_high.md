# DLQRateHigh

> **Severity:** Warning | **Status:** Provisional

**Fires when:** DLQ receiving > 1 message/min sustained for 10 minutes.

## Symptoms

- Messages being permanently dropped from the pipeline
- Usually a symptom of another failing consumer

## Diagnosis

```bash
# Check which stage is producing DLQ messages
curl -s http://<dead-letter-ip>:8009/metrics | grep dlq_messages_processed_total

# Check failures.json in DLQ consumer logs
kubectl logs -n consumers -l consumer=dead_letter --tail=50 | jq '.failed_stage'
```

## Remediation

Usually a symptom of another issue. Fix the upstream error category (see [ProcessingFailureRateHigh](processing_failure_rate_high.md)).

1. If same `failed_stage` dominates — investigate that specific consumer
2. If multiple stages — likely a shared dependency issue (Kafka, network)
3. DLQ messages can be replayed once the root cause is fixed

## Escalation

If DLQ rate > 5 msg/min for 30 minutes.
