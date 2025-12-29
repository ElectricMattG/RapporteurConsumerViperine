# LLM-Based Content Validation for Chunker

## Overview

Implemented Gemini LLM-based content validation in the chunker pipeline to filter out website noise (navigation menus, footer links, UI elements) and extract substantive content from mixed chunks.

## Implementation

### 1. Enhanced MarkdownChunkProcessor

**File**: `src/components/chunkers/markdown_chunk_processor.py`

**Key Changes**:
- Added `llm_client` and `enable_llm_validation` parameters to `__init__`
- Made `process_chunks()` async to support LLM calls
- Added `_validate_content_batch()` method for batch LLM validation (max 20 chunks)
- Added `_apply_validation_results()` to filter and update chunks based on LLM classification

**Validation Flow**:
1. Regex-based noise filtering (fast, catches obvious cases)
2. LLM batch validation (catches nuanced cases)
3. Chunk merging (combines small adjacent chunks)
4. Metadata updates

### 2. Content Classification

**Three Categories**:

- **VALID**: Substantive news/legal content (kept as-is)
- **MIXED**: Contains valid content + UI elements (extracts only substantive portion)
- **INVALID**: Pure navigation/UI/off-topic (filtered out completely)

**Detection Patterns**:
- Navigation lists: `text [number]` repeated 5+ times
- Footer links: "Terms and Conditions", "Privacy Policy"
- Region/country lists with numbered links
- Off-topic: sports, celebrities, contests

### 3. LLM Prompt Design

```
You are a content quality classifier for a research pipeline analyzing news and legal documents.

Classify each chunk as:
- VALID: Substantive news/legal content worth indexing
- MIXED: Contains some valid content but also has UI elements - extract ONLY substantive portion
- INVALID: Navigation, UI elements, or off-topic content

Returns JSON array:
[
  {
    "chunk_id": "chunk_abc123",
    "classification": "valid|mixed|invalid",
    "extracted_content": "...",  // Only for mixed
    "reason": "explanation"
  }
]
```

### 4. Integration with Document Chunker

**File**: `src/consumers/kafka_document_chunker.py`

**Changes**:
- Import `GeminiLLMClient`
- Initialize LLM client if available and enabled
- Pass LLM client to `MarkdownChunkProcessor`
- Made `process_chunks` call async with `await`
- Pass `source_url` for context

### 5. Configuration

**File**: `src/config.py`

Added config option:
```python
"chunker": {
    "enable_llm_validation": True,  # Use Gemini to validate chunk content quality
    ...
}
```

### 6. Batch Processing

**Strategy**:
- Process up to 20 chunks per LLM call
- Group chunks from same document for shared context
- Limit content to 1000 chars per chunk (prevent token overflow)
- Use temperature=0.3 for consistent classification

**Error Handling**:
- On LLM timeout/error: treat all chunks as valid (conservative)
- On malformed JSON: retry once, then default to valid
- On result count mismatch: default to valid

## Cost Analysis

**Per Batch (20 chunks)**:
- Input: ~6000 tokens (chunks + prompt)
- Output: ~1000 tokens (JSON classifications)
- Total: ~7000 tokens

**Using Gemini 2.5 Flash** (cheaper than GPT-4):
- Cost per batch: ~$0.01-0.02
- Cost per chunk: ~$0.0005-0.001

**ROI**: Saves embedding costs for filtered chunks (embedding likely more expensive than validation)

## Benefits

1. **Early Filtering**: Removes noise before embedding (saves costs)
2. **Quality Improvement**: Better content quality in index
3. **Mixed Content Handling**: Extracts valid portions from noisy chunks
4. **Flexible**: Can disable validation via config if needed
5. **Conservative**: On errors, keeps content rather than losing it

## Testing

Test script: `test_chunk_validation.py`

Tests validation with:
- Valid chunks (news articles, legal text)
- Invalid chunks (navigation menus, footer links)
- Mixed chunks (article + footer)

## Metadata Added

Each validated chunk gets:
```python
{
    "chunk_metadata": {
        "validation_status": "valid|mixed|invalid",
        "validation_reason": "explanation from LLM",
        ...
    }
}
```

## Future Enhancements

1. **Caching**: Cache validation for identical content hashes
2. **Domain Rules**: Specific validation criteria for legal vs news content
3. **Confidence Scores**: Track validation quality (currently skipped as LLMs can't represent distributions reliably)
4. **Human Review**: Queue low-confidence cases for manual review
5. **Lightweight Classifier**: Train on LLM-labeled data, use LLM only for edge cases

## Limitations

1. **No Confidence Scores**: Skipped as LLMs cannot reliably represent uncertainty distributions
2. **Max 20 Chunks/Call**: Larger batches risk token limits
3. **Latency**: Adds 2-5s per batch (acceptable with async/parallel)
4. **Conservative on Error**: Prefers false positives over losing content

## Usage

**Default**: Enabled automatically if Gemini client available

**Disable**:
```python
# In config.py
"enable_llm_validation": False
```

**Or in code**:
```python
processor = MarkdownChunkProcessor(
    config=config,
    llm_client=None,  # No LLM client
    enable_llm_validation=False
)
```
