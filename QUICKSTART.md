# Kafka Web Scraper Consumer - Quick Start

## What Was Built

A complete Kafka consumer service that:

1. **Consumes messages from Kafka** containing URLs to scrape
2. **Resolves redirect URLs** to their final destinations
3. **Extracts links** from the resolved pages (builds a list of URLs)
4. **Scrapes content** from all discovered URLs
5. **Converts HTML to Markdown** using docling
6. **Stores results in cache** organized by session ID

## Message Format

```json
{
    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHC...",
    "title": "longwarjournal.org",
    "context": [
        "The IDF stated the operation targeted an Islamist group...",
        "This operation highlights concerns within the IDF..."
    ],
    "session_id": "unique-session-id"
}
```

## Files Created

### Core Components

1. **`src/components/url_resolver.py`**
   - Resolves redirect URLs through multiple hops
   - Tracks redirect chain
   - Extracts links from web pages
   - Async/await support for concurrent processing

2. **`src/components/kafka_web_scraper.py`**
   - Main Kafka consumer
   - Orchestrates URL resolution → scraping → caching
   - Handles message processing and error recovery
   - Graceful shutdown on SIGINT/SIGTERM

3. **`src/config.py`** (updated)
   - Added Kafka configuration section
   - Added web scraper configuration section
   - Environment variable support

### Scripts

4. **`run_kafka_consumer.py`**
   - CLI tool to start the consumer
   - Command-line options for configuration
   - Status logging and monitoring

5. **`test_producer.py`**
   - Test tool to send messages to Kafka
   - Useful for development and testing
   - Configurable message fields

6. **`example_workflow.py`**
   - Demonstrates the complete workflow without Kafka
   - Useful for testing and understanding the flow
   - Shows each step with detailed output

### Documentation

7. **`docs/KAFKA_WEB_SCRAPER.md`**
   - Comprehensive documentation
   - Architecture overview
   - Configuration guide
   - Troubleshooting tips

8. **`requirements.in`** (updated)
   - Added `confluent-kafka` for Kafka client
   - Added `curl-cffi` for advanced HTTP requests
   - Added other scraping dependencies

## Quick Test (No Kafka Required)

```bash
# Test the workflow without Kafka
python example_workflow.py
```

This will:
- Resolve a sample URL (python.org)
- Extract links from the page
- Scrape the content
- Convert to markdown
- Save to cache

## Running with Kafka

### 1. Start the Consumer

```bash
# Set Kafka connection
export KAFKA_BOOTSTRAP_SERVERS="your-kafka-server:9092"

# Start consumer
python run_kafka_consumer.py
```

### 2. Send Test Messages

```bash
# Send a test message
python test_producer.py \
    --url "https://www.example.com" \
    --session-id "test-123" \
    --title "Example Site" \
    --context "First context line" \
    --context "Second context line"
```

### 3. Check Results

Results are saved in `cache/web_scrapes/<session_id>/scraped_content_*.jsonl`

```bash
# View cached results
cat cache/web_scrapes/test-123/scraped_content_*.jsonl | jq .
```

## Output Format

Each scraped URL creates a record in JSONL format:

```json
{
    "id": 1,
    "source_url": "https://example.com/page",
    "method": "docling_linked",
    "text": "# Page Title\n\nMarkdown content here...",
    "text_length": 5420,
    "metadata": {
        "title": "Page Title",
        "chunk_sample": "First 500 chars...",
        "extracted_links": [...],
        "link_count": 25,
        "extracted_images": [...],
        "image_count": 8
    },
    "extraction_timestamp": "2025-12-02T14:30:22.123456",
    "session_id": "test-123",
    "source_title": "Example Site",
    "source_context": ["First context line", "Second context line"],
    "processing_timestamp": "2025-12-02T14:30:25.789012"
}
```

## Architecture

```
┌─────────────────┐
│  Kafka Topic    │
│  (messages)     │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Kafka Consumer         │
│  (kafka_web_scraper.py) │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  URL Resolver           │
│  (url_resolver.py)      │
│  - Resolve redirects    │
│  - Extract links        │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Document Web Searcher  │
│  (document_websearcher) │
│  - Browser impersonation│
│    (curl_cffi)          │
│  - Fetch HTML           │
│  - Convert to Markdown  │
│    (docling)            │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Cache Storage          │
│  (JSONL files by        │
│   session)              │
└─────────────────────────┘
```

**Key Technology**: All HTTP requests use `curl_cffi` with browser impersonation to bypass anti-bot detection and TLS fingerprinting.

## Configuration

### Environment Variables

```bash
# Required
export KAFKA_BOOTSTRAP_SERVERS="kafka-host:9092"

# Optional - Security
export KAFKA_SASL_USERNAME="username"
export KAFKA_SASL_PASSWORD="password"
```

### Config File (src/config.py)

```python
"kafka": {
    "bootstrap_servers": None,  # From env var
    "group_id": "web-scraper-consumer",
    "input_topic": "web-scrape-requests",
    "auto_offset_reset": "earliest",
    ...
}

"web_scraper": {
    "cache_dir": "./cache/web_scrapes",
    "max_redirects": 10,
    "request_timeout": 15,
    "max_urls_per_message": 50,
    ...
}
```

## Next Steps

### For Development

1. **Test locally**: Run `python example_workflow.py`
2. **Setup Kafka**: Use Docker Compose or connect to existing cluster
3. **Send messages**: Use `test_producer.py` to test end-to-end
4. **Monitor logs**: Check `./logs/logfile.log` for detailed output

### For Production

1. **Configure Kafka**: Set production Kafka servers
2. **Security**: Configure SASL authentication if needed
3. **Scaling**: Run multiple consumer instances with same group ID
4. **Monitoring**: Setup log aggregation and metrics
5. **Cache management**: Setup rotation/cleanup for old cache files

## Troubleshooting

### Consumer won't start
- Check `KAFKA_BOOTSTRAP_SERVERS` is set correctly
- Verify Kafka is accessible: `telnet kafka-host 9092`
- Check logs in `./logs/logfile.log`

### Scraping fails with 403 errors
- Ensure `curl-cffi` is installed: `pip install curl-cffi`
- Check browser impersonation is enabled in logs

### No content in cache
- Check session_id in messages
- Verify cache directory permissions
- Review error logs for conversion failures

## Dependencies Fixed

The initial error with pip-compile was resolved by:
1. Downgrading pip from 25.3 to 24.3.1
2. Successfully compiling requirements with all new dependencies

All dependencies are now properly managed in `requirements.txt`.

## Summary

✅ Complete Kafka consumer service built
✅ URL resolution with redirect tracking
✅ Link extraction for discovering additional URLs
✅ **Advanced browser impersonation using curl_cffi** - bypasses anti-bot detection
✅ HTML to Markdown conversion using docling
✅ Session-based cache storage in JSONL format
✅ Leverages existing DocumentWebSearcher component (no code duplication)
✅ Test tools and comprehensive documentation
✅ Ready for production deployment

**Key Advantage**: The service uses the battle-tested `DocumentWebSearcher` component with `curl_cffi` for all HTTP requests, providing robust browser impersonation that defeats TLS/JA3 fingerprinting and common anti-bot measures.

The service is fully functional and ready to consume messages from Kafka!
