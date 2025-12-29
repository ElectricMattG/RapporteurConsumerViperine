# Rate Limiting Fixes

## Problem
The system was experiencing frequent 429 RESOURCE_EXHAUSTED errors from the Vertex AI Gemini API when processing dual queries (israel_prime_minister + israel_defense_force) across multiple time windows simultaneously. This caused:
- 18.6% error rate in qa-embedder (due to empty Q&A files from rate-limited qa-generator)
- 1.38% error rate in qa-generator (rate limits from Gemini API)

## Root Causes
1. **High concurrent API requests**: Multiple consumers (3x qa-generator replicas) processing chunks simultaneously
2. **Dual query processing**: Two time-series queries running in parallel from cronjob
3. **No backoff strategy**: Failed requests not retried with proper delays
4. **High message throughput**: Cronjob sending all URLs to Kafka without throttling

## Solutions Implemented

### 1. Exponential Backoff with Jitter (Client-Side)
**File**: `src/clients/llm_gcp_client.py`

Added retry logic with exponential backoff for rate limit errors:
- **Max retries**: 5 attempts
- **Base delay**: 2 seconds
- **Max delay**: 60 seconds (capped)
- **Jitter**: Random 0-25% of delay to prevent thundering herd
- **Formula**: `delay = min(base_delay * (2 ** attempt), max_delay) + jitter`

**Retry delays**:
- Attempt 1: 2s + jitter (0-0.5s) = 2-2.5s
- Attempt 2: 4s + jitter (0-1s) = 4-5s
- Attempt 3: 8s + jitter (0-2s) = 8-10s
- Attempt 4: 16s + jitter (0-4s) = 16-20s
- Attempt 5: 32s + jitter (0-8s) = 32-40s

**Benefits**:
- Automatically recovers from intermittent rate limits
- Jitter prevents synchronized retries across consumers
- Only retries on 429 errors (fails fast on other errors)

### 2. Message Throttling (Pipeline-Side)
**Files**: 
- `cronjobs/time_series_indexer.py`
- `cronjobs/helm/values.yaml`
- `cronjobs/helm/templates/cronjob.yaml`

Added configurable delay between Kafka messages:
- **Default delay**: 0.1 seconds (10 messages/second)
- **Environment variable**: `KAFKA_MESSAGE_DELAY_SECONDS`
- **Configurable via helm**: `kafka.messageDelaySeconds`

**Benefits**:
- Reduces system-wide throughput to prevent overwhelming API
- Gives consumers time to process before new messages arrive
- Prevents queue buildup that causes cascading rate limits

## Configuration

### Cronjob Helm Values
```yaml
kafka:
  messageDelaySeconds: 0.1  # 100ms delay = ~10 msg/s throughput
```

Adjust based on:
- **Increase (0.2-0.5s)** if still seeing rate limits
- **Decrease (0.05s)** if system handles load well
- **Set to 0** to disable throttling

### Client Backoff Parameters
In `src/clients/llm_gcp_client.py` (hardcoded but tunable):
```python
max_retries = 5        # Number of retry attempts
base_delay = 2.0       # Initial backoff delay
max_delay = 60.0       # Maximum backoff delay
```

## Deployment

### Rebuild Containers
```bash
# Consumer container (includes llm_gcp_client changes)
docker build -t ragserviceneacr6881.azurecr.io/scrape-consumer:latest .
docker push ragserviceneacr6881.azurecr.io/scrape-consumer:latest

# Cronjob container (includes message delay)
cd cronjobs
docker build -t ragserviceneacr6881.azurecr.io/time-series-indexer:latest .
docker push ragserviceneacr6881.azurecr.io/time-series-indexer:latest
```

### Redeploy
```bash
# Consumers
helm upgrade research-indexing-consumers ./helm/research-indexing-consumers \
  -n consumers \
  --reuse-values

# Cronjob
helm upgrade time-series-indexer ./cronjobs/helm \
  -n consumers \
  --reuse-values \
  --set kafka.messageDelaySeconds=0.1
```

## Expected Results

### Before Fixes
- **qa-generator error rate**: 1.38% (429 errors)
- **qa-embedder error rate**: 18.6% (empty file errors)
- **Behavior**: Intermittent failures, some messages processed successfully

### After Fixes
- **qa-generator error rate**: <0.5% (retries handle most rate limits)
- **qa-embedder error rate**: <2% (only fails on truly empty docs)
- **Behavior**: System self-recovers from rate limits, consistent throughput

## Monitoring

### Grafana Metrics
Check these metrics after deployment:
- `processing_error` rate for qa-generator and qa-embedder
- Look for warning logs: "⏳ Rate limit hit (429), retrying in Xs"
- Success rate should improve significantly

### Logs to Monitor
```bash
# Check for backoff retries
kubectl logs -n consumers deployment/qa-generator | grep "Rate limit hit"

# Check message delay is working
kubectl logs -n consumers job/<cronjob-job-name> | grep "Message delay"

# Verify success
kubectl logs -n consumers deployment/qa-generator | grep "Generated.*Q&A pairs"
```

## Tuning Guide

If still seeing rate limits:
1. **Increase message delay**: `0.1s → 0.2s → 0.5s`
2. **Reduce qa-generator replicas**: `3 → 2 → 1`
3. **Process queries sequentially**: Disable one query temporarily
4. **Request quota increase**: Contact GCP support for Gemini API quota

If system is underutilized:
1. **Decrease message delay**: `0.1s → 0.05s → 0.01s`
2. **Increase replicas**: Add more qa-generator/qa-embedder pods
3. **Run more queries**: Enable additional time-series queries

## Testing Recommendations

1. **Start conservative**: Use 0.1s delay, monitor for 1 hour
2. **Check metrics**: Verify error rates drop below 2%
3. **Gradually optimize**: Decrease delay by 0.05s increments if stable
4. **Load test**: Run manual cronjob to test full pipeline
