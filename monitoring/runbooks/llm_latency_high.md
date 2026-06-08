# LLMLatencyHigh

> **Severity:** Warning | **Status:** Provisional

**Fires when:** LLM p95 latency > 60s sustained for 10 minutes.

## Symptoms

- Summarization taking much longer than expected
- Lag may build on summarizer input topic
- Cost may also increase (longer requests ≠ more tokens, but correlates with retries)

## Diagnosis

```bash
# Check latency percentiles
curl -s http://<summarizer-ip>:8010/metrics | grep llm_latency_seconds

# Check if prompts are unusually long
kubectl logs -n consumers -l consumer=summarizer --tail=50 | jq '.input_tokens'

# Check provider status
# Gemini: https://status.cloud.google.com/
# OpenAI: https://status.openai.com/
```

## Remediation

1. If provider degraded — wait for recovery; retries handle transient spikes
2. If consistently slow — consider switching to a faster model (`gemini-2.5-flash`)
3. Check `max_output_tokens` config — very high values increase latency
4. If prompts are very long — check if document chunking is working correctly

## Escalation

If latency > 120s sustained for 30 minutes and provider shows no issues.
