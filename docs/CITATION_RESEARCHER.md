# Citation Research Client

## Overview

The `CitationResearchClient` is a specialized OpenAI GPT-5.1 powered tool for researching citations (books, journal articles, reports) by searching the web and generating comprehensive markdown summaries.

## Features

✅ **Citation Extraction** - Automatically extract citations from text using pattern matching
✅ **Web Search Integration** - Search for relevant URLs for each citation
✅ **AI-Powered Summaries** - Generate 500-word summaries using GPT-5.1
✅ **Markdown Output** - Professional markdown formatting for documentation
✅ **Batch Processing** - Process multiple citations in a single request
✅ **Structured Results** - Returns UUID-tracked results with timestamps

## Use Cases

- **Literature Review**: Quickly summarize multiple academic papers
- **Reference Validation**: Research citations to verify sources
- **Documentation**: Generate summaries for bibliography entries
- **Research Assistance**: Understand cited works without full text access

## Installation

```bash
pip install openai  # Requires openai >= 1.58.0
export OPENAI_API_KEY="sk-..."
```

## Quick Start

### Basic Usage

```python
from src.components.__clients.llm_citation_researcher import CitationResearchClient

# Initialize client
client = CitationResearchClient(
    api_key="sk-...",  # Or use OPENAI_API_KEY env var
    model_name="gpt-5.1"
)

# Research citations
citations = [
    "Smith, J. (2020). Climate Change Impact on Agriculture. Environmental Quarterly.",
    "Johnson, M. (2019). Data Science for Beginners. Tech Publishers."
]

result = await client.research_citations(citations)

# Access results
print(result['request_uuid'])  # Unique request ID
print(result['summaries'])     # List of plain text summaries
print(result['markdown'])      # List of markdown summaries
print(result['search']['urls'])  # All URLs found
```

### With Web Search

```python
from src.components.webscrape.document_web_searcher import DocumentWebSearcher

# Initialize with web searcher
searcher = DocumentWebSearcher()
client = CitationResearchClient(web_searcher=searcher)

# Research will now include web search results
result = await client.research_citations(citations)
```

### Extract Citations from Text

```python
# Text with embedded citations
text = """
Important works include Smith (2020) on climate change,
and "Data Science Fundamentals" by Johnson, M. (2019).
"""

# Extract and research
result = await client.research_citations(
    [text],
    extract_embedded=True  # Auto-extract citations
)
```

## Response Structure

```json
{
  "request_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-11-19T10:30:00.000Z",
  "search": {
    "query": "Research request for 2 citations",
    "urls": [
      "https://example.com/article1",
      "https://example.com/article2"
    ]
  },
  "summaries": [
    "Plain text summary of first citation...",
    "Plain text summary of second citation..."
  ],
  "markdown": [
    "# Citation 1\n\n## Overview\n\n...",
    "# Citation 2\n\n## Overview\n\n..."
  ]
}
```

## Configuration

### Via Config File (`app_config.yaml`)

```yaml
openai:
  api_key: sk-...
  organization: org-...  # Optional
  model_name: gpt-5.1
  reasoning_effort: low  # For summaries: none, low, medium, high
  verbosity: high  # For detailed summaries: low, medium, high
  timeout: 60

citation_research:
  summary_word_count: 500  # Target summary length
  max_urls: 5  # Max URLs to find per citation
```

### Via Constructor

```python
client = CitationResearchClient(
    api_key="sk-...",
    model_name="gpt-5.1",
    organization="org-..."
)

# Override defaults programmatically
client.summary_word_count = 750
client.max_urls_per_citation = 10
client.reasoning_effort = 'medium'
```

## Citation Extraction Patterns

The client automatically detects these citation formats:

### Pattern 1: Author (Year)
```
Smith, J. (2020). The Impact of Climate Change. Science Journal.
```

### Pattern 2: Quoted Title
```
"Machine Learning Basics" by Johnson, M.
```

### Pattern 3: Comma-Separated
```
Brown, A., Climate Strategies, 2021, MIT Press
```

### Custom Extraction

```python
# Extract without research
citations = client.extract_citations(text)
print(f"Found {len(citations)} citations")
```

## Summary Generation

### Summary Structure

Each summary includes:

1. **Overview**: Brief description of the work
2. **Key Points**: Main arguments and findings (bullets)
3. **Author Context**: Information about author(s)
4. **Significance**: Impact and contribution
5. **Publication Details**: Where/when published

### Example Output

```markdown
# The Impact of Climate Change on Agriculture

## Overview

This comprehensive study by Dr. Jane Smith (2020) examines the effects
of rising global temperatures on agricultural productivity across multiple
regions and crop types.

## Key Findings

- Global average yields decreased by 15% over the past decade
- Certain regions experienced up to 30% crop losses
- Adaptation strategies can mitigate 40% of negative impacts
- Technology adoption is critical for climate resilience

## Author & Publication

Dr. Smith is a leading climate scientist at the Environmental Research
Institute. Published in Environmental Science Quarterly, a peer-reviewed
journal with impact factor 4.5.

## Significance

This work provides crucial data for policymakers addressing food security
in the context of climate change. Widely cited (500+ citations) as a
foundational study in climate agriculture research.
```

## API Reference

### Class: `CitationResearchClient`

#### `__init__(api_key, model_name, organization, web_searcher)`

Initialize the citation research client.

**Parameters:**
- `api_key` (str, optional): OpenAI API key
- `model_name` (str, optional): Model to use (default: `gpt-5.1`)
- `organization` (str, optional): OpenAI organization ID
- `web_searcher` (object, optional): Web search client instance

#### `extract_citations(text: str) -> List[str]`

Extract citations embedded in text using pattern matching.

**Parameters:**
- `text` (str): Text containing citations

**Returns:**
- `List[str]`: List of extracted citation strings

#### `search_citation(citation: str) -> List[str]`

Search web for URLs related to a citation.

**Parameters:**
- `citation` (str): Citation to search for

**Returns:**
- `List[str]`: URLs found (up to `max_urls_per_citation`)

#### `generate_summary(citation: str, urls: List[str], context: str) -> Dict[str, str]`

Generate markdown summary for a citation.

**Parameters:**
- `citation` (str): Citation string
- `urls` (List[str]): Relevant URLs
- `context` (str, optional): Additional context

**Returns:**
- `Dict[str, str]`: `{'summary': str, 'markdown': str}`

#### `research_citations(citations: List[str], extract_embedded: bool) -> Dict`

Research multiple citations and generate summaries.

**Parameters:**
- `citations` (List[str]): Citation strings or text blocks
- `extract_embedded` (bool): Extract citations from text first

**Returns:**
- `Dict`: Response structure with UUID, summaries, markdown, URLs

#### `set_web_searcher(web_searcher)`

Set or update the web search client.

**Parameters:**
- `web_searcher` (object): Web search client instance

## Model Recommendations

| Model | Best For | Cost | Speed |
|-------|----------|------|-------|
| `gpt-5.1` | Highest quality summaries | $$$ | Medium |
| `gpt-5-mini` | Good balance of quality/cost | $$ | Fast |
| `gpt-5-nano` | High-throughput batch processing | $ | Very Fast |
| `gpt-4o-mini` | Legacy compatibility | $$ | Fast |

## Performance Tips

### 1. Batch Processing

Process multiple citations in one request for efficiency:

```python
# Good: Batch request
result = await client.research_citations(all_citations)

# Avoid: Individual requests
for citation in citations:
    result = await client.research_citations([citation])
```

### 2. Reasoning Effort

Adjust based on citation complexity:

```python
# Simple citations (news articles, basic references)
client.reasoning_effort = 'none'  # Fastest

# Academic papers, complex sources
client.reasoning_effort = 'low'  # More thorough

# Deep research, ambiguous citations
client.reasoning_effort = 'medium'  # Most comprehensive
```

### 3. Verbosity Control

```python
# Brief overviews
client.verbosity = 'low'  # ~200 words

# Standard summaries
client.verbosity = 'medium'  # ~500 words

# Detailed analysis
client.verbosity = 'high'  # ~750+ words
```

## Testing

```bash
python test_citation_researcher.py
```

Tests include:
1. ✅ Citation extraction from text
2. ✅ Summary generation (requires API key)
3. ✅ Embedded citation research
4. ✅ Full workflow with web search

## Error Handling

```python
try:
    result = await client.research_citations(citations)
except ValueError as e:
    # API key not configured
    print(f"Configuration error: {e}")
except Exception as e:
    # Generation failed
    print(f"Research failed: {e}")

# Check individual summaries for errors
for summary in result['summaries']:
    if summary.startswith("Error generating summary"):
        print("Summary generation failed for this citation")
```

## Integration Examples

### With Deduplication Pipeline

```python
# After deduplication, research unique documents
from llm_citation_researcher import CitationResearchClient

client = CitationResearchClient()

# Load unique documents
with open('unique_documents.jsonl') as f:
    docs = [json.loads(line) for line in f]

# Extract source texts as citations
citations = [doc['source_text'] for doc in docs]

# Research all citations
result = await client.research_citations(citations[:10])  # First 10

# Save results
with open('citation_research.json', 'w') as f:
    json.dump(result, f, indent=2)
```

### With Excel Export

```python
import pandas as pd

result = await client.research_citations(citations)

# Create DataFrame
df = pd.DataFrame({
    'citation': citations,
    'summary': result['summaries'],
    'markdown': result['markdown']
})

# Export to Excel
df.to_excel('citation_summaries.xlsx', index=False)
```

## Limitations

- **Web Search**: Requires web searcher client for URL discovery
- **API Costs**: GPT-5.1 API usage charges apply
- **Rate Limits**: Subject to OpenAI rate limits
- **Accuracy**: Summaries based on available web content, may not have full text access
- **Language**: Optimized for English citations

## Support

For issues or questions:
- Check logs with `_logger.debug()` output
- Verify API key is set correctly
- Ensure web searcher is configured if needed
- Review OpenAI API status page

## License

Part of the WebCrawlScrape project.
