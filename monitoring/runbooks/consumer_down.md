# ConsumerDown

> **Severity:** Critical | **Status:** Provisional

**Fires when:** `kafka_consumer_state{pipeline="viperine"} != 1` for 2 minutes.

## Symptoms

- Consumer is not processing messages
- Downstream consumers may start lagging

## Diagnosis

```bash
# Check pod status
kubectl get pods -n consumers -l app.kubernetes.io/instance=viperine

# Check logs for crash reason
kubectl logs -n consumers -l consumer=<name> --tail=50

# Check if OOM-killed
kubectl describe pod -n consumers <pod-name> | grep -A5 "Last State"
```

## Remediation

1. If pod is in `CrashLoopBackOff` — check logs for startup errors (missing config, auth failure, Kafka unreachable)
2. If pod is `OOMKilled` — increase memory limit in `helm/rapporteur-consumer-group/values.yaml`
3. If pod is `Pending` — check node resources: `kubectl describe node`
4. If pod terminated cleanly — check if the consumer received SIGTERM (scaling event or deployment)

## Escalation

If consumer doesn't recover within 5 minutes after remediation, page on-call.
