# ConsumerLagHigh

> **Severity:** Critical | **Status:** Provisional

**Fires when:** Consumer lag > 50 messages for 5 minutes.

## Symptoms

- Pipeline falling behind — new requests not being processed in time
- End-to-end latency increases

## Diagnosis

```bash
# Check lag per partition
curl -s http://<consumer-ip>:<port>/metrics | grep lag_messages

# Check if consumer is processing slowly or not processing at all
curl -s http://<consumer-ip>:<port>/metrics | grep last_message_processed

# Check Kafka topic offsets directly
kubectl exec -n kafka kafka-0 -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group <consumer-group-id>
```

## Remediation

1. If consumer is processing but slowly — check if downstream dependency (LLM, embedding service) is slow
2. If consumer is NOT processing — likely stuck on a poison message; check DLQ for recent entries
3. If lag is growing during peak load — consider scaling replicas (Helm `replicas` value)
4. If lag accumulated during downtime — it will self-recover; monitor rate of recovery

## Escalation

Lag > 200 messages and not decreasing.
