# Summary Query Service

FastAPI service for querying summary indices with vector similarity and optional LLM grounding.

## Features

- **In-Memory Index**: Loads JSONL index into memory on startup for fast queries
- **Vector Search**: Query using semantic similarity with embeddings
- **LLM Grounding**: Generate comprehensive overviews using retrieved documents
- **Date Filtering**: Filter results by publication date
- **Detailed Responses**: Rich metadata including request ID, retrieved docs, execution time
- **Health Checks**: Built-in health and readiness endpoints

## Installation

```bash
# Install dependencies
pip install fastapi uvicorn pydantic numpy

# Ensure the index file exists
ls cache/summaries/research_summaries_with_embeddings_combined.jsonl
```

## Starting the Service

```bash
# Start the service (default port 8000)
python main.py

# Or with uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run in background with logging
cd src/services/rag
nohup python main.py > ../logs/rag.log 2>&1 &
```

The service will:
1. Load the index file into memory
2. Initialize embedding and LLM clients
3. Start listening on http://localhost:8000

## API Endpoints

### 1. Health Check
```bash
GET /health
```

Returns service health status and uptime.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-01-07T10:30:00.123456",
  "uptime_seconds": 123.45
}
```

### 2. Readiness Check
```bash
GET /ready
```

Checks if service is ready to handle requests (index loaded).

**Response:**
```json
{
  "status": "ready",
  "index_loaded": true,
  "total_summaries": 6,
  "index_path": "cache/summaries/research_summaries_with_embeddings_combined.jsonl",
  "timestamp": "2026-01-07T10:30:00.123456"
}
```

### 3. Search (Vector Similarity Only)
```bash
POST /search
```

Query the index and return matching documents without LLM processing.

**Request Body:**
```json
{
  "query": "Gaza ceasefire negotiations",
  "top_k": 5,
  "start_date": "2025-01-01",
  "end_date": "2026-01-07",
  "post_filter_top_k": null
}
```

**Response:**
```json
{
  "request_id": "uuid-here",
  "session_id": "search_1234567890",
  "query": "Gaza ceasefire negotiations",
  "index_queried": "cache/summaries/research_summaries_with_embeddings_combined.jsonl",
  "top_k": 5,
  "date_filters": {
    "start_date": "2025-01-01",
    "end_date": "2026-01-07"
  },
  "retrieved_docs": [
    {
      "document_id": "req_20260106_165415_002",
      "source_url": "https://www.youtube.com/watch?v=...",
      "publication_date": "2026-01-06",
      "similarity_score": 0.5996,
      "summary_text": "This ILTV Israel News report...",
      "doc_metadata": {...}
    }
  ],
  "total_retrieved": 5,
  "execution_time_ms": 123.45
}
```

### 4. Query with LLM
```bash
POST /query_llm
```

Query the index AND generate LLM response using retrieved documents as grounding.

**Request Body:**
```json
{
  "query": "Gaza ceasefire negotiations",
  "top_k": 3,
  "start_date": null,
  "end_date": null,
  "post_filter_top_k": null,
  "temperature": 0.7,
  "max_tokens": 4000
}
```

**Response:**
```json
{
  "request_id": "uuid-here",
  "session_id": "query_1234567890",
  "query": "Gaza ceasefire negotiations",
  "index_queried": "cache/summaries/research_summaries_with_embeddings_combined.jsonl",
  "top_k": 3,
  "date_filters": {
    "start_date": null,
    "end_date": null
  },
  "retrieved_docs": [...],
  "total_retrieved": 3,
  "execution_time_ms": 1234.56,
  "llm_response": "The provided summaries offer a multi-faceted view...",
  "llm_model": "gemini-2.5-flash",
  "llm_params": {
    "temperature": 0.7,
    "max_tokens": 4000
  },
  "prompt": "Given these summaries, provide a comprehensive overview..."
}
```

## Usage Examples

### Using cURL

```bash
# Health check
curl http://localhost:8000/health

# Readiness check
curl http://localhost:8000/ready

# Search
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Gaza ceasefire",
    "top_k": 5
  }'

# Query with LLM
curl -X POST http://localhost:8000/query_llm \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Gaza ceasefire negotiations",
    "top_k": 3,
    "temperature": 0.7,
    "max_tokens": 4000
  }'
```

### Using Python Test Client

```bash
# Health check
python test_query_service.py --health

# Readiness check
python test_query_service.py --ready

# Search only
python test_query_service.py --search "Gaza ceasefire" --top-k 5

# Query with LLM
python test_query_service.py --query "Gaza ceasefire negotiations" \
  --top-k 3 --temperature 0.7 --max-tokens 4000

# With date filtering
python test_query_service.py --query "Gaza ceasefire" \
  --start-date 2025-01-01 --end-date 2026-01-07

# Show full prompt
python test_query_service.py --query "Gaza ceasefire" --show-prompt
```

### Using Python Requests

```python
import requests

# Search
response = requests.post("http://localhost:8000/search", json={
    "query": "Gaza ceasefire",
    "top_k": 5
})
results = response.json()
print(f"Found {results['total_retrieved']} documents")

# Query with LLM
response = requests.post("http://localhost:8000/query_llm", json={
    "query": "Gaza ceasefire negotiations",
    "top_k": 3,
    "temperature": 0.7
})
results = response.json()
print(results['llm_response'])
```

## Configuration

The service uses these defaults (hardcoded in startup_event):

- **Index Path**: `cache/summaries/research_summaries_with_embeddings_combined.jsonl`
- **Port**: `8000`
- **Host**: `0.0.0.0`
- **Log File**: `src/services/logs/rag.log` (when using nohup)

To customize, modify the `startup_event()` function in `main.py`.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Summary Query Service                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐                                            │
│  │  FastAPI App │                                            │
│  └──────┬───────┘                                            │
│         │                                                     │
│  ┌──────▼──────────────────────────────────────────┐        │
│  │         Service State (In-Memory)               │        │
│  │  • Summaries Index (JSONL loaded at startup)   │        │
│  │  • Embedding Client                             │        │
│  │  • LLM Client (Gemini)                          │        │
│  └──────┬──────────────────────────────────────────┘        │
│         │                                                     │
│  ┌──────▼──────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   /health   │  │    /ready    │  │   /search    │       │
│  └─────────────┘  └──────────────┘  └──────┬───────┘       │
│                                             │                │
│                                    ┌────────▼─────────────┐ │
│                                    │    /query_llm        │ │
│                                    │  1. Vector Search    │ │
│                                    │  2. LLM Grounding    │ │
│                                    └──────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Response Fields

All responses include:
- `request_id`: Unique identifier for the request (UUID)
- `session_id`: Session identifier (timestamp-based)
- `query`: Original search query
- `index_queried`: Path to the index file used
- `top_k`: Number of top results requested
- `date_filters`: Applied date filters
- `retrieved_docs`: List of matching documents with metadata
- `total_retrieved`: Count of retrieved documents
- `execution_time_ms`: Request execution time in milliseconds

Additional fields for `/query_llm`:
- `llm_response`: Generated overview from LLM
- `llm_model`: Name of the LLM model used
- `llm_params`: LLM generation parameters (temperature, max_tokens)
- `prompt`: Full prompt sent to the LLM

## Performance

- **Index Loading**: One-time operation on startup (~1-2 seconds for 1000 summaries)
- **Search Query**: ~100-300ms (embedding generation + similarity calculation)
- **LLM Query**: ~2-5 seconds (includes search + LLM generation)

## Error Handling

The service returns standard HTTP status codes:
- `200 OK`: Successful request
- `404 Not Found`: No matching documents
- `500 Internal Server Error`: Server-side error
- `503 Service Unavailable`: Index not loaded (not ready)

## Logging

The service logs:
- Startup/shutdown events
- Index loading progress
- Request processing (with request_id)
- Errors with stack traces

Logs are written to stdout (or to `rag.log` when using nohup) in the format:
```
2026-01-07 10:30:00,123 | INFO | main | [request_id] Message
```

## Dependencies

- `fastapi`: Web framework
- `uvicorn`: ASGI server
- `pydantic`: Request/response validation
- `numpy`: Vector operations
- Custom clients from `src/`:
  - `EmbeddingServiceClient`: Generate embeddings
  - `GeminiLLMClient`: Generate LLM responses
