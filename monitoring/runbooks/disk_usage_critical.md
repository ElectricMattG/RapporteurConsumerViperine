# DiskUsageCritical

> **Severity:** Critical | **Status:** Provisional

**Fires when:** Disk usage > 95% on PVC.

## Symptoms

- Consumer may crash on next write attempt
- Cache accumulation has exceeded capacity

## Diagnosis

```bash
# Check disk usage inside pod
kubectl exec -n consumers <pod-name> -- df -h /app/cache

# Find large directories
kubectl exec -n consumers <pod-name> -- du -sh /app/cache/*
```

## Remediation

1. Delete old session directories: `kubectl exec -n consumers <pod-name> -- find /app/cache -maxdepth 1 -mtime +7 -type d -exec rm -rf {} \;`
2. If cache grows indefinitely — check if cleanup logic is running (index aggregator `cleanup_after_upload`)
3. Increase PVC size if growth is legitimate (edit `persistence.size` in Helm values + resize PV)
4. Emergency: scale consumer to 0, manually clean, scale back up

## Escalation

If disk > 99% — immediate, consumer will crash.
