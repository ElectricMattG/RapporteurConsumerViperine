# LLMErrorRateHigh

> **Severity:** Critical | **Status:** Provisional

**Fires when:** LLM API error rate > 10% sustained for 10 minutes.

## Symptoms

- Summaries not being generated
- Sessions will stall or produce degraded stubs

## Diagnosis

```bash
# Check error types
curl -s http://<summarizer-ip>:8010/metrics | grep llm_api_errors_total

# Check the LLM provider status page
# Gemini: https://status.cloud.google.com/
# OpenAI: https://status.openai.com/
```

## Remediation

1. If `auth` errors — API key expired or quota exhausted; check GCP IAM or OpenAI dashboard
2. If `timeout` errors — provider is under load; the retry mechanism will handle transient issues
3. If `policy_block` — batch of sensitive content; these will degrade to stub summaries automatically
4. If provider outage — wait for recovery; DLQ will capture unrecoverable failures

## Escalation

If error rate > 50% for 30 minutes.
