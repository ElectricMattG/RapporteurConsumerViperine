# ProcessingFailureRateHigh

> **Severity:** Critical | **Status:** Provisional

**Fires when:** Processing failure rate > 10% sustained for 5 minutes.

## Symptoms

- Messages are being consumed but failing to process
- DLQ rate may increase simultaneously

## Diagnosis

```bash
# Check which error categories are spiking
curl -s http://<consumer-ip>:<port>/metrics | grep processing_failures_total

# Check structured logs for the specific error
kubectl logs -n consumers -l consumer=<name> --tail=100 | jq 'select(.level == "ERROR")'
```

## Remediation by Error Category

| Error Category | Action |
|---------------|--------|
| `scraping_403` / `scraping_blocked` | Source site blocking — check if IP reputation degraded; rotate User-Agent or wait |
| `scraping_timeout` | Upstream slow — increase `web_scraper.request_timeout` or check network |
| `json_parse` / `json_decode` | Malformed messages on topic — check publisher |
| `summarization_policy_block` | LLM safety filter — content too sensitive; will DLQ, no action needed |
| `summarization_timeout` | Gemini/OpenAI API slow — check status pages; retries will handle transient |
| `summarization_auth` | API key expired or quota exhausted — rotate credentials |
| `embedding_timeout` | Embedding service overloaded — check `embedding-service-serve-svc` pod |
| `azure_upload` | Azure Blob Storage auth or network — check connection string, storage account status |

## Escalation

If failure rate persists > 10% for 15 minutes and isn't a known upstream issue.
