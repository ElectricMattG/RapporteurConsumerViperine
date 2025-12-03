# Pipeline Architecture

## Overview

The scraping and enrichment pipeline processes web content through multiple stages, with each stage consuming from Kafka topics and publishing results to the next stage.

## Pipeline Flow

```
┌─────────────────┐
│  Input Request  │
│  (web-scrape-   │
│   requests)     │
└────────┬────────┘
         │
         ├──────────────────────────────────────────┐
         ↓                                          ↓
┌─────────────────┐                       ┌─────────────────┐
│  Web Scraper    │                       │  Summarizer     │
│  - Resolves URL │                       │  - Gemini LLM   │
│  - Extracts     │                       │  - Web grounding│
│    content      │                       │  - Summary.md   │
└────────┬────────┘                       └─────────────────┘
         │
         ↓ (content-extracted)
┌─────────────────┐
│    Chunker      │
│  - Chunks doc   │
│  - Saves chunks │
└────────┬────────┘
         │
         ↓ (citation-extraction)
┌─────────────────┐
│   Citation      │
│   Extractor     │
│  - Enriches     │
│    chunks       │
└────────┬────────┘
         │
         ↓ (chunk-embedding)
┌─────────────────┐
│  Chunk Embedder │
│  - Generates    │
│    embeddings   │
└────────┬────────┘
         │
         ↓ (qa-generation)
┌─────────────────┐
│  QA Generator   │
│  - Creates Q&A  │
│    pairs        │
└─────────────────┘
```

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `web-scrape-requests` | External | Web Scraper | Input URLs to scrape |
| `content-extracted` | Web Scraper | Chunker, Summarizer | Extracted content |
| `citation-extraction` | Chunker | Citation Extractor | Chunked documents |
| `chunk-embedding` | Citation Extractor | Chunk Embedder | Enriched chunks |
| `qa-generation` | Chunk Embedder | QA Generator | Embedded chunks |
| `dead-letter-queue` | All consumers | Dead Letter Consumer | Failed messages |

## Stages

### 1. Web Scraper
- **Consumer**: `kafka_web_scraper.py`
- **Input Topic**: `web-scrape-requests`
- **Output Topic**: `content-extracted`
- **Function**: Resolves redirects, extracts web content
- **Output Files**: `cache/web_scrapes/{request_id}/extracted_linked_text.jsonl`

### 2. Summarizer (Parallel Branch)
- **Consumer**: `kafka_summarizer.py`
- **Input Topic**: `content-extracted`
- **Output**: Disk only (no Kafka publish)
- **Function**: Generates summary using Gemini with web grounding
- **Output Files**:
  - `cache/web_scrapes/{request_id}/summary.md`
  - `cache/web_scrapes/{request_id}/summary.json`

### 3. Chunker (Parallel Branch)
- **Consumer**: `kafka_document_chunker.py`
- **Input Topic**: `content-extracted`
- **Output Topic**: `citation-extraction`
- **Function**: Chunks document into manageable pieces
- **Output Files**: `cache/web_scrapes/{request_id}/chunked_document.jsonl`

### 4. Citation Extractor
- **Consumer**: `kafka_citation_extractor.py`
- **Input Topic**: `citation-extraction`
- **Output Topic**: `chunk-embedding`
- **Function**: Enriches chunks with citation metadata
- **Output Files**: `cache/web_scrapes/{request_id}/chunked_document_enriched*.jsonl`

### 5. Chunk Embedder
- **Consumer**: `kafka_chunk_embedder.py`
- **Input Topic**: `chunk-embedding`
- **Output Topic**: `qa-generation`
- **Function**: Generates vector embeddings for chunks
- **Output Files**: `cache/web_scrapes/{request_id}/chunked_document_enriched_embedded.jsonl`

### 6. QA Generator
- **Consumer**: `kafka_qa_generator.py`
- **Input Topic**: `qa-generation`
- **Output**: Disk only (no Kafka publish)
- **Function**: Generates question-answer pairs from chunks
- **Output Files**: `cache/web_scrapes/{request_id}/*qa_pairs*.jsonl`

### 7. Dead Letter Queue Consumer
- **Consumer**: `kafka_dead_letter_consumer.py`
- **Input Topic**: `dead-letter-queue`
- **Function**: Logs all pipeline failures
- **Output Files**:
  - `cache/failures/failures.json` (all failures)
  - `cache/failures/failure_{request_id}_{stage}_{timestamp}.json` (individual)

## Error Handling

All consumers publish failed messages to the `dead-letter-queue` topic with:
- Original message that failed
- Failed stage name
- Error message
- Timestamp
- Request ID and Session ID

The Dead Letter Queue consumer logs failures to:
- `cache/failures/failures.json` - Cumulative log
- Individual failure files for each error

## Running the Pipeline

### Start All Consumers

```bash
# Terminal 1: Web Scraper
python src/consumers/run_kafka_consumer.py > /tmp/web_scraper.log 2>&1 &

# Terminal 2: Summarizer
python src/consumers/run_kafka_summarizer.py > /tmp/summarizer.log 2>&1 &

# Terminal 3: Chunker
python src/consumers/run_kafka_chunker.py > /tmp/chunker.log 2>&1 &

# Terminal 4: Citation Extractor
python src/consumers/run_kafka_citation_extractor.py > /tmp/citation.log 2>&1 &

# Terminal 5: Chunk Embedder
python src/consumers/run_kafka_chunk_embedder.py > /tmp/embedder.log 2>&1 &

# Terminal 6: QA Generator
python src/consumers/run_kafka_qa_generator.py > /tmp/qa.log 2>&1 &

# Terminal 7: Dead Letter Queue
python src/consumers/run_kafka_dead_letter_consumer.py > /tmp/dead_letter.log 2>&1 &
```

### Send Test Request

```python
from confluent_kafka import Producer
import json

config = {"bootstrap.servers": "localhost:9092"}
producer = Producer(config)

message = {
    "uri": "https://example.com/article",
    "title": "Test Article",
    "context": ["Test context"],
    "session_id": "test-session-001"
}

producer.produce("web-scrape-requests", json.dumps(message).encode("utf-8"))
producer.flush()
```

### Monitor Progress

```bash
# Watch logs
tail -f /tmp/web_scraper.log
tail -f /tmp/summarizer.log
tail -f /tmp/chunker.log
tail -f /tmp/citation.log
tail -f /tmp/embedder.log
tail -f /tmp/qa.log
tail -f /tmp/dead_letter.log

# Check output files
find cache/web_scrapes/req_* -name "*.jsonl" -o -name "*.json" -o -name "*.md" | sort

# Check failures
cat cache/failures/failures.json | jq '.[] | {stage: .failed_stage, error: .error, request_id: .request_id}'
```

## Message Schemas

### Web Scrape Request
```json
{
  "uri": "https://example.com/article",
  "title": "Article Title",
  "context": ["Context line 1", "Context line 2"],
  "session_id": "unique-session-id"
}
```

### Content Extracted
```json
{
  "request_id": "req_20251203_123456",
  "session_id": "session-id",
  "extracted_content": {
    "url": "https://example.com/article",
    "text": "Full article text...",
    "metadata": {
      "title": "Article Title",
      "author": "Author Name",
      "published_date": "2025-12-03",
      "publisher": "Publisher Name"
    },
    "extracted_at": "2025-12-03T12:34:56"
  }
}
```

### Citation Extraction Request
```json
{
  "request_id": "req_20251203_123456",
  "session_id": "session-id",
  "chunks_file": "cache/web_scrapes/req_20251203_123456/chunked_document.jsonl",
  "document_id": "doc-req_20251203_123456",
  "source_url": "https://example.com/article",
  "chunk_count": 42
}
```

### Chunk Embedding Request
```json
{
  "request_id": "req_20251203_123456",
  "session_id": "session-id",
  "enriched_chunks_file": "cache/web_scrapes/req_20251203_123456/chunked_document_enriched.jsonl",
  "document_id": "doc-req_20251203_123456",
  "source_url": "https://example.com/article",
  "chunk_count": 42,
  "total_citations": 15
}
```

### QA Generation Request
```json
{
  "request_id": "req_20251203_123456",
  "session_id": "session-id",
  "embedded_chunks_file": "cache/web_scrapes/req_20251203_123456/chunked_document_enriched_embedded.jsonl",
  "document_id": "doc-req_20251203_123456",
  "source_url": "https://example.com/article",
  "chunk_count": 42,
  "embeddings_generated": 42
}
```

### Dead Letter Queue Message
```json
{
  "original_message": {...},
  "failed_stage": "chunker",
  "error": "Error message details",
  "timestamp": "2025-12-03T12:34:56",
  "request_id": "req_20251203_123456",
  "session_id": "session-id"
}
```

## Configuration

Key configuration settings in `config.py`:

```yaml
kafka:
  bootstrap_servers: "localhost:9092"

chunker:
  input_topic: "content-extracted"
  output_topic: "citation-extraction"

citation_extractor:
  input_topic: "citation-extraction"
  output_topic: "chunk-embedding"

chunk_embedder:
  input_topic: "chunk-embedding"
  output_topic: "qa-generation"

qa_generator:
  input_topic: "qa-generation"

dead_letter:
  input_topic: "dead-letter-queue"
  output_directory: "cache/failures"
```

## Troubleshooting

### Pipeline Stuck After Certain Stage

1. Check if consumer is running: `ps aux | grep "run_kafka"`
2. Check consumer logs for errors
3. Check Kafka topic has messages: `kafka-console-consumer --topic <topic-name>`
4. Check dead letter queue for failures: `cat cache/failures/failures.json`

### Messages Not Propagating

- Verify each consumer is subscribed to correct input topic
- Verify each consumer is publishing to correct output topic
- Check Kafka broker is running: `systemctl status kafka`

### High Error Rate

Check `cache/failures/failures.json` for patterns:
```bash
jq '.[] | {stage: .failed_stage, count: 1}' cache/failures/failures.json | \
  jq -s 'group_by(.stage) | map({stage: .[0].stage, count: length})'
```
