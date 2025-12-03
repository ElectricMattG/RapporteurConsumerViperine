# Kafka Web Scraper Consumer

A Kafka consumer service that processes web scraping requests, resolves redirect URLs, extracts content, and stores the results in cache.

## Overview

This service consumes messages from a Kafka topic containing URLs to scrape. For each message, it:

1. **Resolves redirect URLs** - Follows all HTTP redirects to the final destination
2. **Extracts links** - Discovers additional URLs from the resolved page
3. **Scrapes content** - Fetches HTML content from all discovered URLs
4. **Converts to Markdown** - Uses docling to convert HTML to clean markdown
5. **Caches results** - Stores scraped content in JSONL format organized by session

## Message Format

The consumer expects messages in the following JSON format:

```json
{
    "uri": "https://example.com/redirect-url",
    "title": "Source Name",
    "context": [
        "Context line 1 describing the source",
        "Context line 2 with additional info"
    ],
    "session_id": "unique-session-identifier"
}
```

### Fields

- **uri** (required): The URL to scrape (may be a redirect URL)
- **title** (optional): Title or name of the source
- **context** (optional): Array of context strings related to the URL
- **session_id** (required): Unique identifier for grouping related scrapes

## Installation

1. Install dependencies:

```bash
make requirements
pip install -r requirements.txt
```

2. Ensure required packages are installed:
   - `confluent-kafka` - Kafka client
   - `curl-cffi` - Advanced HTTP client with browser impersonation
   - `beautifulsoup4` - HTML parsing
   - `brotli` - Compression support
   - `python-dateutil` - Date parsing

## Configuration

Configuration is managed through `src/config.py`. Key settings:

### Kafka Configuration

Set via environment variables or config file:

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export KAFKA_SASL_USERNAME="your-username"  # Optional
export KAFKA_SASL_PASSWORD="your-password"  # Optional
```

Configuration options:
- `kafka.bootstrap_servers`: Kafka broker addresses
- `kafka.group_id`: Consumer group ID (default: "web-scraper-consumer")
- `kafka.input_topic`: Topic to consume from (default: "web-scrape-requests")
- `kafka.auto_offset_reset`: Offset reset policy (default: "earliest")
- `kafka.security_protocol`: Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)

### Web Scraper Configuration

- `web_scraper.cache_dir`: Cache directory (default: "./cache/web_scrapes")
- `web_scraper.max_redirects`: Maximum redirect hops (default: 10)
- `web_scraper.request_timeout`: Request timeout in seconds (default: 15)
- `web_scraper.max_urls_per_message`: Max URLs to extract per message (default: 50)
- `web_scraper.retry_attempts`: Retry attempts for failed requests (default: 3)

## Usage

### Start the Consumer

Basic usage:

```bash
python run_kafka_consumer.py
```

With custom configuration:

```bash
python run_kafka_consumer.py \
    --bootstrap-servers "kafka:9092" \
    --topic "web-scrape-requests" \
    --group-id "my-consumer-group" \
    --cache-dir "./custom-cache"
```

### Send Test Messages

Use the test producer to send sample messages:

```bash
python test_producer.py \
    --url "https://www.example.com" \
    --session-id "test-session-123" \
    --title "Example Website" \
    --context "Test context line 1" \
    --context "Test context line 2"
```

### Command Line Options

**run_kafka_consumer.py:**
- `-b, --bootstrap-servers`: Kafka bootstrap servers
- `-t, --topic`: Kafka topic to consume from
- `-g, --group-id`: Consumer group ID
- `-c, --cache-dir`: Directory for cached scrapes

**test_producer.py:**
- `-u, --url`: URL to scrape (required)
- `-s, --session-id`: Session ID (default: "test-session")
- `-t, --title`: Source title (default: "Test Source")
- `-c, --context`: Context lines (can be repeated)
- `-b, --bootstrap-servers`: Kafka servers (default: localhost:9092)
- `--topic`: Kafka topic (default: "web-scrape-requests")

## Output Format

Scraped content is saved in JSONL format (one JSON object per line) in the cache directory, organized by session:

```
cache/web_scrapes/
├── session-123/
│   └── scraped_content_20251202_143022.jsonl
└── session-456/
    └── scraped_content_20251202_143045.jsonl
```

### Output Record Format

Each line in the JSONL file contains:

```json
{
    "id": 1,
    "source_url": "https://example.com/page",
    "method": "docling_linked",
    "text": "# Page Title\n\nMarkdown content...",
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
    "session_id": "session-123",
    "source_title": "Source Name",
    "source_context": ["Context line 1", "Context line 2"],
    "processing_timestamp": "2025-12-02T14:30:25.789012"
}
```

## Architecture

### Components

1. **URLResolver** (`src/components/url_resolver.py`)
   - Resolves redirect URLs to final destinations
   - Tracks full redirect chain
   - Extracts links from web pages
   - Async/await support for concurrent processing

2. **DocumentWebSearcher** (`src/components/document_websearcher.py`)
   - **Browser impersonation** using `curl_cffi` to bypass bot detection
   - Fetches web content with advanced TLS fingerprinting
   - Converts HTML to markdown using docling
   - Extracts comprehensive metadata (title, dates, author, etc.)
   - Handles compressed responses (gzip, brotli)

3. **KafkaWebScraperConsumer** (`src/components/kafka_web_scraper.py`)
   - Consumes Kafka messages
   - Orchestrates URL resolution and scraping
   - Leverages DocumentWebSearcher for all HTTP requests
   - Manages caching and error handling

### Processing Flow

```
Kafka Message → URL Resolution → Link Extraction → Content Fetching (curl_cffi) → Markdown Conversion → Cache Storage
```

1. **Message Received**: Consumer polls Kafka topic
2. **URL Resolution**: Follows redirects to final URL using URLResolver
3. **Link Extraction**: Extracts additional URLs from the page
4. **Content Fetching**: Uses DocumentWebSearcher's session with browser impersonation (curl_cffi)
5. **Markdown Conversion**: Converts HTML to markdown using docling
6. **Cache Storage**: Saves results in session-specific JSONL files

**Key Feature**: All HTTP requests use `curl_cffi` for advanced browser impersonation, defeating TLS fingerprinting and anti-bot measures.

## Error Handling

The consumer includes robust error handling:

- **URL Resolution Failures**: Logged and included in result with error details
- **Scraping Failures**: Individual URL failures don't stop batch processing
- **Kafka Connection Issues**: Automatic reconnection with backoff
- **Graceful Shutdown**: SIGINT/SIGTERM handlers for clean shutdown

Failed extractions are logged to `cache/web_scrapes/extract_markdown_failed.json` for debugging and retry.

## Monitoring

The service logs extensively for monitoring:

```
INFO  - KafkaWebScraperConsumer - 🚀 Starting Kafka Web Scraper Consumer
INFO  - KafkaWebScraperConsumer - ✅ Subscribed to topic: web-scrape-requests
INFO  - URLResolver - 🔍 Resolving URL: https://example.com/redirect...
INFO  - URLResolver -    ✅ Resolved to: https://example.com/final
INFO  - KafkaWebScraperConsumer -    📄 Scraping 1/5: https://example.com/final...
INFO  - KafkaWebScraperConsumer -       ✅ Converted to markdown (5420 chars)
INFO  - KafkaWebScraperConsumer -    ✅ Saved 5 items to: cache/web_scrapes/session-123/...
```

## Production Deployment

### Docker

Create a `Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/ src/
COPY run_kafka_consumer.py .

CMD ["python", "run_kafka_consumer.py"]
```

Build and run:

```bash
docker build -t kafka-web-scraper .
docker run -e KAFKA_BOOTSTRAP_SERVERS="kafka:9092" kafka-web-scraper
```

### Kubernetes

See `helm/` directory for Helm charts and deployment manifests.

### Environment Variables

Required:
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka broker addresses

Optional:
- `KAFKA_SASL_USERNAME`: SASL username
- `KAFKA_SASL_PASSWORD`: SASL password

## Troubleshooting

### Consumer not connecting to Kafka

Check bootstrap servers:
```bash
echo $KAFKA_BOOTSTRAP_SERVERS
```

Test connectivity:
```bash
telnet kafka-host 9092
```

### Scraping failures (403 errors)

Ensure `curl-cffi` is installed:
```bash
pip install curl-cffi
```

Check browser impersonation is working in logs.

### Slow processing

- Reduce `max_urls_per_message` in config
- Increase `max_poll_interval_ms` for complex pages
- Check network latency to target sites

### Cache directory full

Configure cache cleanup or rotation:
```bash
# Clean old cache files
find cache/web_scrapes -name "*.jsonl" -mtime +7 -delete
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

```bash
make format  # Format code with black
make lint    # Check code style
```

### Adding New Features

1. Update configuration in `src/config.py`
2. Implement feature in appropriate component
3. Add tests
4. Update this README

## License

See LICENSE file in the repository root.

## Support

For issues or questions, check the logs in `./logs/logfile.log` or open an issue in the repository.
