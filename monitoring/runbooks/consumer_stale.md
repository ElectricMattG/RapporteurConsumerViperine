# ConsumerStale

> **Severity:** Warning | **Status:** Provisional

**Fires when:** Consumer hasn't processed a message in 30 minutes.

## Symptoms

- Consumer appears running but no work is being done
- May be idle (legitimate) or stuck (problem)

## Diagnosis

```bash
# Check if the input topic has pending messages
curl -s http://<consumer-ip>:<port>/metrics | grep lag_messages

# Check health endpoint
curl -s http://<consumer-ip>:<port>/ready

# Check if the consumer is poll-blocked
kubectl logs -n consumers -l consumer=<name> --tail=20
```

## Remediation

1. If lag > 0 but no processing — consumer may be stuck; restart pod
2. If lag == 0 — no action needed; pipeline is idle (no inbound work)
3. Check `/ready` endpoint — if returning 503, consumer isn't fully initialized
4. Check if Kafka consumer group has been rebalanced away from all partitions

## Escalation

Only if lag > 0 and consumer appears stuck for > 1 hour.
