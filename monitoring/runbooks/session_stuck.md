# SessionStuck

> **Severity:** Warning | **Status:** Provisional

**Fires when:** Sessions in progress but no messages processed for 10 minutes.

## Symptoms

- Summary indexer has open sessions but isn't making progress
- May indicate a blocking operation or missing input

## Diagnosis

```bash
# Check if input topic has messages
curl -s http://<summary-indexer-ip>:8006/metrics | grep lag_messages

# Check if consumer is processing
curl -s http://<summary-indexer-ip>:8006/metrics | grep last_message_processed

# Check for blocking I/O in logs
kubectl logs -n consumers -l consumer=summary_indexer --tail=50
```

## Remediation

1. If stuck on a single message — check if it's a poison pill (will eventually timeout and DLQ)
2. If no messages on input topic — upstream consumer (summarizer) is down; check summarizer
3. Force-complete: sessions idle > `SESSION_IDLE_TIMEOUT` (120s) are auto-swept
4. If auto-sweep isn't triggering — restart the pod

## Escalation

If sessions stuck > 30 minutes and lag is growing.
