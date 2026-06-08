# ConsumerRestartLooping

> **Severity:** Critical | **Status:** Provisional

**Fires when:** Consumer startup timestamp changes > 3 times in 1 hour.

## Symptoms

- Consumer repeatedly crashing and restarting
- Lag accumulating on input topic

## Diagnosis

```bash
# Check pod restarts
kubectl get pods -n consumers -l consumer=<name> -o wide

# Check previous crash logs
kubectl logs -n consumers <pod-name> --previous --tail=100

# Check events
kubectl get events -n consumers --sort-by='.lastTimestamp' | grep <pod-name>
```

## Remediation

1. If `OOMKilled` — increase memory limit
2. If Python traceback — code bug; check recent deployments
3. If connection refused (Kafka/Redis/Embedding) — dependency is down; fix upstream first
4. If config error — check ConfigMap or secret values

## Escalation

Immediate — restart loops mean zero processing.
