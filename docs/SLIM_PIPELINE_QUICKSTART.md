# Slim Pipeline - Quick Start

## What Changed?

### ✅ Updated Files
1. **`restart_pipeline.sh`**
   - Reduced topics from 10 to 5
   - Only starts 3-4 consumers (web_scraper, summarizer, summary_indexer, optional aggregator)
   - Sets `SUMMARIZER_INPUT_TOPIC=content-extracted` automatically
   - Index aggregator gated by `ENABLE_AGGREGATOR` flag

2. **`src/consumers/kafka_summarizer.py`**
   - Now reads `SUMMARIZER_INPUT_TOPIC` environment variable
   - Falls back to config value or `summarise-document` if not set
   - Allows dynamic routing without code changes

### ❌ Removed Components
- Document Chunker
- Citation Extractor
- Chunk Embedder
- Q&A Generator
- Q&A Embedder

## Message Flow

```
web-scrape-requests
       ↓
  Web Scraper
       ↓
content-extracted ← (SUMMARIZER_INPUT_TOPIC=content-extracted)
       ↓
   Summarizer
       ↓
summary-completed
       ↓
 Summary Indexer
       ↓
session-complete
       ↓
Index Aggregator (optional)
       ↓
  Azure Blob Storage
```

## Start the Pipeline

```bash
# Basic (no aggregator)
./restart_pipeline.sh

# With aggregator
ENABLE_AGGREGATOR=true ./restart_pipeline.sh
```

## Verify Running

```bash
# Check running consumers
ps aux | grep "run_kafka" | grep -v grep

# Should see 3 processes:
# - run_kafka_consumer.py (web_scraper)
# - run_kafka_summarizer.py (summarizer)
# - run_kafka_summary_indexer.py (summary_indexer)
# - run_kafka_index_aggregator.py (if ENABLE_AGGREGATOR=true)
```

## Check Logs

```bash
# Tail all logs
tail -f logs/web_scraper.log logs/summarizer.log logs/summary_indexer.log

# Individual logs
tail -f logs/web_scraper.log
tail -f logs/summarizer.log
tail -f logs/summary_indexer.log
tail -f logs/index_aggregator.log  # if enabled
```

## Test the Pipeline

```bash
# Send a test URL
python send_urls.py --session-id test-001 \
  --urls "https://www.bbc.com/news/articles/c1dpp8pj8xeo"

# Watch the flow
# 1. web_scraper.log - URL fetched, content extracted
# 2. summarizer.log - Summary generated
# 3. summary_indexer.log - Summary embedded and indexed
# 4. index_aggregator.log - Session aggregated (if enabled)
```

## Environment Variables

```bash
# Required for Gemini
export PROJECT_ID=project-70236742-1aaa-4686-816
export GOOGLE_CLOUD_PROJECT=project-70236742-1aaa-4686-816

# Pipeline configuration (set automatically by restart_pipeline.sh)
export SUMMARIZER_INPUT_TOPIC=content-extracted  # Auto-set
export ENABLE_AGGREGATOR=false  # Default, set to 'true' to enable
```

## Kafka Topics

Created automatically by `restart_pipeline.sh`:
- `web-scrape-requests` - Input queue
- `content-extracted` - Web scraper output → Summarizer input
- `summarise-document` - (kept for compatibility, not used)
- `summary-completed` - Summarizer output → Indexer input
- `session-complete` - Indexer output → Aggregator input

## Troubleshooting

### Consumers not starting?
```bash
# Check if Kafka is running
docker ps | grep kafka

# Restart Kafka if needed
docker-compose -f docker-compose.kafka.yml up -d

# Check broker health
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Summarizer not consuming from content-extracted?
```bash
# Check environment variable
echo $SUMMARIZER_INPUT_TOPIC  # Should be 'content-extracted'

# Check summarizer log startup
grep "Input topic:" logs/summarizer.log
# Should show: "Input topic: content-extracted"
```

### No messages flowing?
```bash
# Check topic offsets
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic content-extracted

# List consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list

# Check consumer lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group summarizer-consumer \
  --describe
```

## What's Next?

1. ✅ **Test end-to-end** with real URLs
2. ✅ **Monitor logs** for errors
3. ✅ **Verify outputs** in cache directory
4. ⬜ Update docker-compose.yml to not start removed consumers
5. ⬜ Update Helm charts for k8s deployment
6. ⬜ Update CI/CD pipelines

## Rollback Plan

If you need to revert to the full pipeline:

1. Edit `restart_pipeline.sh`:
   - Add back all 10 topics to `TOPICS` array
   - Add back all consumer starts (chunker, embedders, Q&A)
   - Remove `SUMMARIZER_INPUT_TOPIC` override

2. Restart:
   ```bash
   ./restart_pipeline.sh
   ```

3. Or manually set:
   ```bash
   unset SUMMARIZER_INPUT_TOPIC
   # Summarizer will default to 'summarise-document'
   ```
