# ProduceFailuresSustained

> **Severity:** Critical | **Status:** Provisional

**Fires when:** Kafka produce error rate > 0.1/s for 5 minutes.

## Symptoms

- Messages being consumed but NOT forwarded to next stage
- Pipeline progression stalls

## Diagnosis

```bash
# Check Kafka broker health
kubectl get pods -n kafka

# Check if topic exists and is writable
kubectl exec -n kafka kafka-0 -- kafka-topics.sh --describe \
  --topic <output-topic> --bootstrap-server localhost:9092

# Check producer errors in logs
kubectl logs -n consumers -l consumer=<name> --tail=100 | grep -i "produce\|delivery"
```

## Remediation

1. If Kafka broker is down — wait for recovery or restart broker pod
2. If topic doesn't exist — create it (should auto-create, check `auto.create.topics.enable`)
3. If message too large — check `message.max.bytes` broker config vs message size
4. If network partition — check inter-pod connectivity in the cluster

## Escalation

If produce failures persist > 10 minutes and broker appears healthy.
