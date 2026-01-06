# Slim Pipeline Message Routing

## Overview
The slim pipeline removes the chunking, embedding, and Q&A components, creating a streamlined flow focused on web scraping, summarization, and indexing summaries.

## Architecture

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  Web Scraper     │────▶│   Summarizer     │────▶│ Summary Indexer  │────▶│ Index Aggregator │
│                  │     │                  │     │                  │     │   (optional)     │
└──────────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘
     consumes:               consumes:               consumes:                consumes:
  web-scrape-requests     content-extracted       summary-completed         session-complete
     produces:               produces:               produces:                produces:
  content-extracted       summary-completed       session-complete          (uploads to Azure)
```

## Topic Flow

### 1. **web-scrape-requests** → Web Scraper
- **Consumer**: `kafka_web_scraper.py`
- **Consumer Group**: `web-scraper-consumer`
- **Message Format**:
  ```json
  {
    "uri": "https://example.com/article",
    "title": "Article Title",
    "context": ["Context line 1", "Context line 2"],
    "session_id": "session-123",
    "request_id": "req-456"
  }
  ```

### 2. **content-extracted** → Summarizer
- **Consumer**: `kafka_summarizer.py`
- **Consumer Group**: `summarizer-consumer`
- **Configuration**: Set `SUMMARIZER_INPUT_TOPIC=content-extracted` environment variable
- **Message Format** (from Web Scraper):
  ```json
  {
    "request_id": "req-456",
    "session_id": "session-123",
    "extracted_content": {
      "url": "https://example.com/article",
      "text": "Full extracted text content...",
      "metadata": {
        "title": "Article Title",
        "author": "Author Name",
        "word_count": 5000,
        "extracted_at": "2026-01-05T10:30:00Z"
      }
    }
  }
  ```

### 3. **summary-completed** → Summary Indexer
- **Consumer**: `kafka_summary_indexer.py`
- **Consumer Group**: `summary-indexer-consumer`
- **Message Format** (from Summarizer):
  ```json
  {
    "request_id": "req-456",
    "session_id": "session-123",
    "document_id": "doc-789",
    "source_url": "https://example.com/article",
    "summary_file": "/cache/web_scrapes/session-123/doc-789_summary.json",
    "current_count": 3,
    "total_requests": 15
  }
  ```

### 4. **session-complete** → Index Aggregator (Optional)
- **Consumer**: `kafka_index_aggregator.py`
- **Consumer Group**: `index-aggregator-consumer`
- **Enabled**: Only when `ENABLE_AGGREGATOR=true`
- **Message Format** (from Summary Indexer):
  ```json
  {
    "session_id": "session-123",
    "summary_index_file": "/cache/web_scrapes/session-123/session_summaries_embedded.jsonl",
    "total_summaries": 15,
    "index_type": "summaries"
  }
  ```

## Consumer Configurations

### Web Scraper (`kafka_web_scraper.py`)
- ✅ **No changes needed**
- Input Topic: `web-scrape-requests`
- Output Topic: `content-extracted`
- Cache Directory: `./cache/web_scrapes/`

### Summarizer (`kafka_summarizer.py`)
- ✅ **Updated to support environment variable override**
- Input Topic: Reads `SUMMARIZER_INPUT_TOPIC` env var (defaults to `summarise-document`)
- Output Topic: `summary-completed`
- Fallback Topic: `content-extracted` (for failed extractions)
- Configuration:
  ```bash
  export SUMMARIZER_INPUT_TOPIC=content-extracted
  ```

### Summary Indexer (`kafka_summary_indexer.py`)
- ✅ **No changes needed**
- Input Topic: `summary-completed`
- Output Topic: `session-complete`
- Embedding Model: `all-MiniLM-L6-v2` (384 dimensions)

### Index Aggregator (`kafka_index_aggregator.py`)
- ✅ **No changes needed** (optional component)
- Input Topic: `session-complete`
- Output: Uploads to Azure Blob Storage
- Enabled: Only when `ENABLE_AGGREGATOR=true` in `restart_pipeline.sh`

## Removed Components (Not Used in Slim Pipeline)

The following consumers are **excluded** from the slim pipeline:

1. ❌ **Document Chunker** (`kafka_document_chunker.py`)
   - Would consume: `content-extracted`
   - Would produce: `citation-extraction`

2. ❌ **Citation Extractor** (`kafka_citation_extractor.py`)
   - Would consume: `citation-extraction`
   - Would produce: `chunk-embedding`

3. ❌ **Chunk Embedder** (`kafka_chunk_embedder.py`)
   - Would consume: `chunk-embedding`
   - Would produce: `qa-generation`

4. ❌ **Q&A Generator** (`kafka_qa_generator.py`)
   - Would consume: `qa-generation`
   - Would produce: `qa-embedding`

5. ❌ **Q&A Embedder** (`kafka_qa_embedder.py`)
   - Would consume: `qa-embedding`
   - Would produce: (final output)

## Topics Created by `restart_pipeline.sh`

```bash
TOPICS=(
    "web-scrape-requests"      # Input for web scraper
    "content-extracted"        # Web scraper → Summarizer
    "summarise-document"       # (kept for compatibility, not used in slim flow)
    "summary-completed"        # Summarizer → Summary Indexer
    "session-complete"         # Summary Indexer → Index Aggregator
)
```

## Environment Variables

### Required for Slim Pipeline
```bash
# Configure summarizer to consume from web scraper output
export SUMMARIZER_INPUT_TOPIC=content-extracted

# Optional: Enable index aggregator (default: false)
export ENABLE_AGGREGATOR=false

# GCP Configuration (for Gemini LLM)
export PROJECT_ID=project-70236742-1aaa-4686-816
export GOOGLE_CLOUD_PROJECT=project-70236742-1aaa-4686-816
```

## Starting the Pipeline

```bash
# Start the slim pipeline
./restart_pipeline.sh

# With index aggregator enabled
ENABLE_AGGREGATOR=true ./restart_pipeline.sh

# Override summarizer input topic (if needed)
SUMMARIZER_INPUT_TOPIC=content-extracted ./restart_pipeline.sh
```

## Verification

After starting, verify consumers are running:
```bash
ps aux | grep "src/consumers/run_kafka" | grep -v grep
```

Expected output:
- `run_kafka_consumer.py` (web_scraper)
- `run_kafka_summarizer.py` (summarizer with SUMMARIZER_INPUT_TOPIC=content-extracted)
- `run_kafka_summary_indexer.py` (summary_indexer)
- `run_kafka_index_aggregator.py` (only if ENABLE_AGGREGATOR=true)

## Testing End-to-End

```bash
# Send a test URL to the pipeline
python send_urls.py --session-id test-session-001 \
  --urls "https://example.com/article"

# Monitor logs
tail -f logs/web_scraper.log
tail -f logs/summarizer.log
tail -f logs/summary_indexer.log
tail -f logs/index_aggregator.log  # if enabled
```

## Benefits of Slim Pipeline

1. **Faster Processing**: Removed 5 intermediate consumers (chunker, embedders, Q&A)
2. **Simpler Architecture**: Direct flow: scrape → summarize → index
3. **Lower Resource Usage**: Fewer consumers, less memory, fewer embeddings
4. **Easier Debugging**: Fewer moving parts, clearer message flow
5. **Cost Savings**: Fewer LLM calls (no Q&A generation), less storage

## Migration Notes

### From Full Pipeline to Slim Pipeline

1. **Stop all consumers**: `pkill -9 -f "src/consumers/run_kafka"`
2. **Update restart script**: Already updated in `restart_pipeline.sh`
3. **Clear old topics**: Script automatically deletes and recreates topics
4. **Update environment**: Export `SUMMARIZER_INPUT_TOPIC=content-extracted`
5. **Start slim pipeline**: `./restart_pipeline.sh`

### Reverting to Full Pipeline

1. Update `TOPICS` array in `restart_pipeline.sh` to include all topics
2. Start all consumers (chunker, embedders, Q&A)
3. Remove `SUMMARIZER_INPUT_TOPIC` override (or set to `summarise-document`)

## Summary of Changes

| Component | Status | Input Topic | Output Topic | Notes |
|-----------|--------|-------------|--------------|-------|
| Web Scraper | ✅ Active | `web-scrape-requests` | `content-extracted` | No changes |
| Summarizer | ✅ Active | `content-extracted` | `summary-completed` | **Updated**: Reads `SUMMARIZER_INPUT_TOPIC` env var |
| Summary Indexer | ✅ Active | `summary-completed` | `session-complete` | No changes |
| Index Aggregator | ⚠️ Optional | `session-complete` | (Azure upload) | Gated by `ENABLE_AGGREGATOR` |
| Document Chunker | ❌ Disabled | - | - | Removed from slim pipeline |
| Citation Extractor | ❌ Disabled | - | - | Removed from slim pipeline |
| Chunk Embedder | ❌ Disabled | - | - | Removed from slim pipeline |
| Q&A Generator | ❌ Disabled | - | - | Removed from slim pipeline |
| Q&A Embedder | ❌ Disabled | - | - | Removed from slim pipeline |
