# DiskUsageWarning

> **Severity:** Warning | **Status:** Provisional

**Fires when:** Disk usage > 80%.

## Symptoms

- PVC approaching capacity
- Not yet critical but requires attention

## Diagnosis

Same as [DiskUsageCritical](disk_usage_critical.md) but less urgent.

```bash
kubectl exec -n consumers <pod-name> -- df -h /app/cache
kubectl exec -n consumers <pod-name> -- du -sh /app/cache/* | sort -rh | head -10
```

## Remediation

1. Schedule cache cleanup — check `archive_before_cleanup` and `cleanup_after_upload` settings
2. If growth is expected (large batch) — monitor but don't act yet
3. Consider increasing PVC if this triggers repeatedly

## Escalation

Becomes critical at 95% — see [DiskUsageCritical](disk_usage_critical.md).
