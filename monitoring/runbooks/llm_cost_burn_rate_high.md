# LLMCostBurnRateHigh

> **Severity:** Critical | **Status:** Provisional

**Fires when:** LLM cost increase > $5/h.

## Symptoms

- Runaway cost accumulation
- Possibly a request loop or exceptionally large batch

## Diagnosis

```bash
# Check current session sizes
curl -s http://<summarizer-ip>:8010/metrics | grep llm_estimated_cost

# Check how many messages are being processed
curl -s http://<summarizer-ip>:8010/metrics | grep messages_consumed_total

# Look for large sessions in logs
kubectl logs -n consumers -l consumer=summarizer --tail=200 | jq 'select(.total_requests > 50)'
```

## Remediation

1. If a single session has > 100 documents — this is expected for large research batches; monitor but no action
2. If multiple rapid sessions are firing — check the publisher for a loop condition
3. If cost is from retries — check `llm_api_errors_total` for sustained failure-retry-failure cycles
4. Emergency stop: scale summarizer to 0 replicas: `kubectl scale deploy viperine-summarizer -n consumers --replicas=0`

## Escalation

If cost > $20/h and no known large batch.
