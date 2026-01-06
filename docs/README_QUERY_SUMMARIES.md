# Query Summaries & Q&A Pairs - Documentation

## Overview

`query_summaries.py` is a semantic search tool that queries either research summaries or Q&A pairs using vector similarity search and generates AI-powered overviews using Gemini.

## Features

- **Dual Mode Operation**: Query summaries or Q&A pairs from separate indices
- **Vector Similarity Search**: Uses dot product similarity with embeddings
- **Date Filtering**: Filter results by publication date (pre-filter or post-filter)
- **AI Overview Generation**: Synthesizes top results using Gemini 2.5
- **Flexible Top-K Selection**: Choose how many results to retrieve and display

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Syntax

```bash
python3 query_summaries.py "your query" [OPTIONS]
```

### Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `query` | string | **Required** | Natural language query to search |
| `--index` | path | summaries file | Path to JSONL index file |
| `--top-k` | int | 10 | Number of top results from similarity search |
| `--post-filter-top-k` | int | None | Apply date filter after ranking, limit to N results |
| `--start-date` | date | None | Filter start date (YYYY-MM-DD) |
| `--end-date` | date | None | Filter end date (YYYY-MM-DD) |
| `--mode` | choice | summaries | Query mode: `summaries` or `qa` |
| `--show-summaries` | flag | False | Display individual results before overview |

## Query Modes

### 1. Summaries Mode (Default)

Queries research summaries with full document context.

**File Structure Required:**
```json
{
  "document_id": "req_20231101_...",
  "source_url": "https://...",
  "summary_text": "Full summary content...",
  "summary_embedding": [0.123, 0.456, ...],
  "doc_metadata": {
    "publication_date": "2023-11-01",
    "title": "...",
    "authors": ["..."]
  }
}
```

**Example:**
```bash
python3 query_summaries.py \
  "humanitarian situation in Gaza" \
  --top-k 50 \
  --post-filter-top-k 15 \
  --start-date 2023-11-01 \
  --end-date 2023-12-31 \
  --mode summaries
```

### 2. Q&A Pairs Mode

Queries question-answer pairs with extracted Q&A content.

**File Structure Required:**
```json
{
  "content": "QUESTION: ...\n\nANSWER: ...\n\nCONTEXT: ...",
  "chunk_metadata": {
    "chunk_id": "...",
    "source_url": "https://..."
  },
  "document_metadata": {
    "publication_date": "2023-11-01"
  },
  "embedding_enrichment": {
    "embedding_vector": [0.123, 0.456, ...]
  }
}
```

**Example:**
```bash
python3 query_summaries.py \
  "What are the different phases of war" \
  --top-k 50 \
  --post-filter-top-k 10 \
  --start-date 2023-11-01 \
  --end-date 2023-12-31 \
  --mode qa \
  --index /path/to/qa_pairs_with_embeddings.jsonl
```

## Filtering Strategies

### Pre-Filtering (Default)

Filter by date **before** similarity search:

```bash
python3 query_summaries.py "query" \
  --start-date 2023-11-01 \
  --end-date 2023-12-31 \
  --top-k 10
```

**Process:**
1. Filter all entries by date range
2. Calculate similarities on filtered set
3. Return top-k results

### Post-Filtering (Recommended)

Rank by similarity **first**, then filter by date:

```bash
python3 query_summaries.py "query" \
  --top-k 50 \
  --post-filter-top-k 10 \
  --start-date 2023-11-01 \
  --end-date 2023-12-31
```

**Process:**
1. Calculate similarities on all entries
2. Get top 50 most similar results
3. Filter those 50 by date range
4. Return top 10 from filtered set

**Why use post-filtering?**
- Gets most relevant results that happen to be in date range
- Useful when date range has few entries
- Better for chronological queries

## Output Format

### Console Output

```
📚 Loading summaries from: /path/to/index.jsonl
   Loaded 1224 summaries

🔍 Generating embedding for query: 'your query'
   Calculating similarities...
   Found 1224 results, filtering top 50 by date...
   After date filtering: 15 results

📊 Selected 15 summaries for overview generation

================================================================================
📋 SUMMARIES BEING SENT TO GEMINI:
================================================================================
Summary 1 (similarity: 0.7392):
Document ID: req_20231101_...
Publication Date: 2023-11-15
Source: https://...
Content: ...
--------------------------------------------------------------------------------
...

================================================================================
✨ GEMINI RESPONSE:
================================================================================
[AI-generated overview synthesizing all results]
```

### Q&A Mode Output

```
Q&A Pair 1 (similarity: 0.8123):
Document ID: chunk_123
Publication Date: 2023-11-15
Source: https://...
QUESTION: What are the phases of the conflict?
ANSWER: The conflict progressed through multiple phases...
```

## Examples

### Example 1: Recent Humanitarian Updates

```bash
python3 query_summaries.py \
  "humanitarian situation in Gaza" \
  --top-k 50 \
  --post-filter-top-k 15 \
  --start-date 2025-12-15 \
  --end-date 2026-01-04
```

### Example 2: Strategic Analysis

```bash
python3 query_summaries.py \
  "Israeli military strategy and operations" \
  --top-k 20 \
  --start-date 2023-10-01 \
  --end-date 2023-12-31 \
  --show-summaries
```

### Example 3: Q&A Tactical Questions

```bash
python3 query_summaries.py \
  "What are the key tactical components" \
  --mode qa \
  --top-k 50 \
  --post-filter-top-k 10 \
  --start-date 2023-11-01 \
  --end-date 2023-12-31 \
  --index /path/to/qa_pairs.jsonl
```

### Example 4: Diplomatic Coverage

```bash
python3 query_summaries.py \
  "international response and diplomatic efforts" \
  --top-k 30 \
  --start-date 2023-11-01 \
  --end-date 2023-11-30
```

## Technical Details

### Embedding Generation

- **Service**: EmbeddingServiceClient (https://embeddings.rapporteur.ai)
- **Method**: Async HTTP client with connection pooling
- **Segmentation**: Enabled for long texts
- **Timeout**: 360 seconds

### Similarity Calculation

```python
similarity = np.dot(query_embedding, document_embedding)
```

- **Algorithm**: Dot product similarity
- **Normalization**: Assumes pre-normalized embeddings
- **Return**: Float score (higher = more similar)

### AI Overview Generation

- **Model**: Gemini 2.5 Flash
- **Project**: another-project-472212
- **Location**: us-central1
- **Max Tokens**: 4000
- **Temperature**: 0.7
- **Top-P**: 0.9

### Date Extraction

**Summaries Mode:**
```python
pub_date = summary['doc_metadata']['publication_date']  # YYYY-MM-DD
```

**Q&A Mode:**
```python
pub_date = summary['document_metadata']['publication_date']  # YYYY-MM-DD
```

## Troubleshooting

### Issue: No results found

**Check:**
1. Date range contains entries: `jq -r '.doc_metadata.publication_date' index.jsonl | sort | uniq`
2. Correct mode for file type: `--mode summaries` vs `--mode qa`
3. File has required fields: check JSON structure

### Issue: Wrong embedding field

**Summaries:** Must have `summary_embedding` field
**Q&A:** Must have `embedding_enrichment.embedding_vector` field

### Issue: Slow query

**Solutions:**
- Reduce `--top-k` value
- Use pre-filtering with narrow date range
- Check index file size: `wc -l index.jsonl`

### Issue: Out of memory

**Solutions:**
- Process in smaller batches
- Filter by date before loading
- Use post-filtering with smaller top-k

## File Paths

**Default Summaries Index:**
```
/private/var/folders/.../research_summaries_with_embeddings_combined.jsonl
```

**Q&A Pairs Index:**
```
/private/var/folders/.../research_qa_pairs_with_embeddings_combined.jsonl
```

## Performance Tips

1. **Use post-filtering** for better semantic relevance
2. **Start with high top-k** (50-100) when using post-filter
3. **Enable --show-summaries** to debug result quality
4. **Narrow date ranges** for focused queries
5. **Q&A mode** is better for specific factual questions
6. **Summaries mode** is better for contextual analysis

## Integration

### Jupyter Notebook

```python
import subprocess
import json

result = subprocess.run([
    'python3', 'query_summaries.py',
    'your query',
    '--top-k', '20',
    '--mode', 'summaries'
], capture_output=True, text=True)

print(result.stdout)
```

### Shell Script

```bash
#!/bin/bash

QUERY="$1"
START_DATE="$2"
END_DATE="$3"

python3 query_summaries.py "$QUERY" \
  --top-k 50 \
  --post-filter-top-k 15 \
  --start-date "$START_DATE" \
  --end-date "$END_DATE" \
  > "results_$(date +%Y%m%d_%H%M%S).txt"
```

## Advanced Usage

### Custom Index Path

```bash
python3 query_summaries.py "query" \
  --index /custom/path/to/index.jsonl
```

### No Date Filtering

```bash
python3 query_summaries.py "query" --top-k 20
```

### Verbose Output

```bash
python3 query_summaries.py "query" \
  --show-summaries \
  --top-k 10 | tee output.txt
```

## Dependencies

- `numpy`: Vector operations
- `asyncio`: Async client operations
- `python-dotenv`: Environment configuration
- Custom clients:
  - `src.clients.embedding_client.EmbeddingServiceClient`
  - `src.clients.llm_gcp_client.GeminiLLMClient`

## Environment Variables

```bash
# Required for Gemini
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json

# Optional
EMBEDDING_SERVICE_URL=https://embeddings.rapporteur.ai
GEMINI_PROJECT_ID=your-project-id
```

## License

Internal research tool - see main project license.

## Support

For issues or questions, check:
1. This documentation
2. Script help: `python3 query_summaries.py --help`
3. Source code comments in `query_summaries.py`
