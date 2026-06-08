# EmbeddingLatencyHigh

> **Severity:** Warning | **Status:** Provisional

**Fires when:** Summary embedding p95 > 10s for 5 minutes.

## Symptoms

- Summary indexer slowing down
- Lag may begin building on summary indexer input topic

## Diagnosis

```bash
# Check embedding service pod
kubectl get pods -n consumers -l app=embedding-service

# Check resource usage
kubectl top pod -n consumers -l app=embedding-service

# Check if batch sizes are large
curl -s http://<summary-indexer-ip>:8006/metrics | grep embedding_duration
```

## Remediation

1. Check embedding service replicas — may need scaling
2. Check if network between consumer and embedding service is degraded
3. Temporary: reduce `embedding_service.max_texts_per_request` config
4. If CPU-bound — ensure embedding pod has GPU access or increase CPU limits

## Escalation

If latency > 30s sustained for 15 minutes.
