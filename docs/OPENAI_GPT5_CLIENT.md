# OpenAI GPT-5.1 Client Implementation

## Overview

Updated the OpenAI LLM client to support **GPT-5.1**, OpenAI's newest flagship reasoning model, with full compatibility with the existing Gemini client interface.

## Key Changes

### 1. Dual API Support

The client now intelligently routes between two OpenAI APIs based on the model:

- **Responses API** (for GPT-5 family): `gpt-5.1`, `gpt-5`, `gpt-5-mini`, `gpt-5-nano`
- **Chat Completions API** (for GPT-4 and earlier): `gpt-4o`, `gpt-4-turbo`, `gpt-4`, `gpt-3.5-turbo`

### 2. GPT-5 Reasoning Model Features

#### Reasoning Effort (replaces temperature)
- `none` - Fastest, low-latency responses (default for GPT-5.1)
- `low` - Minimal reasoning
- `medium` - Balanced reasoning
- `high` - Maximum reasoning depth

#### Verbosity Control
- `low` - Concise, minimal output
- `medium` - Balanced output (default)
- `high` - Detailed, thorough explanations

### 3. Retry Logic Updates

- For **GPT-5 models**: Escalates reasoning effort (`none` → `low` → `medium`)
- For **non-reasoning models**: Uses traditional approach
- Maintains novelty checking via Jaccard similarity

### 4. Configuration

```yaml
# app_config.yaml
openai:
  api_key: sk-...  # or OPENAI_API_KEY env var
  organization: org-...  # optional, or OPENAI_ORG_ID env var
  model_name: gpt-5.1  # default model
  reasoning_effort: none  # none, low, medium, high
  verbosity: medium  # low, medium, high
  timeout: 60  # seconds
```

## Model Comparison

| Model | Context | Output | Best For |
|-------|---------|--------|----------|
| `gpt-5.1` | 200K | 100K | Complex reasoning, code generation, agentic tasks |
| `gpt-5-mini` | 200K | 100K | Cost-optimized reasoning |
| `gpt-5-nano` | 200K | 100K | High-throughput, simple tasks |
| `gpt-4o` | 128K | 16K | General-purpose, lower cost |
| `gpt-4o-mini` | 128K | 16K | Cost-effective alternative |

## Usage Examples

### Basic Content Generation

```python
from src.components.__clients.llm_openai_client import OpenAILLMClient

client = OpenAILLMClient(api_key="sk-...", model_name="gpt-5.1")

# Fast response with no reasoning
response = await client.generate_content(
    "What is the capital of France?",
    reasoning_effort='none',
    max_tokens=10
)
```

### Query Generation with Reasoning

```python
# Generate semantic queries with medium reasoning
queries = await client.generate_queries(
    root_query="Gaza humanitarian crisis",
    text_content="...",
    num_queries=5
)
# Automatically uses reasoning_effort='none' → 'low' → 'medium' if retries needed
```

### Q&A Assessment

```python
# Assess Q&A clusters with low reasoning for consistency
assessment = await client.assess_qa_clusters(
    clusters_prompt="...",
    reasoning_effort='low'
)
```

## Breaking Changes from GPT-4

### ❌ Not Supported in GPT-5 Models
- `temperature` parameter (raises error)
- `top_p` parameter (raises error)
- `logprobs` parameter (raises error)

### ✅ Use Instead
- `reasoning.effort` - Controls reasoning depth
- `text.verbosity` - Controls output length
- `max_output_tokens` - Replaces `max_tokens`

## Migration Guide

### From Gemini → GPT-5.1
Almost drop-in replacement. Both use reasoning-based approach:

```python
# Both work identically
gemini_client = GeminiLLMClient()
openai_client = OpenAILLMClient(model_name="gpt-5.1")

# Same interface
queries = await gemini_client.generate_queries(root, context, 5)
queries = await openai_client.generate_queries(root, context, 5)
```

### From GPT-4 → GPT-5.1

```python
# Old (GPT-4)
client = OpenAILLMClient(model_name="gpt-4o")
response = await client.generate_content(prompt, temperature=0.7)

# New (GPT-5.1)
client = OpenAILLMClient(model_name="gpt-5.1")
response = await client.generate_content(prompt, reasoning_effort='none')
```

## Installation

```bash
pip install openai  # Requires openai >= 1.58.0 for Responses API
export OPENAI_API_KEY="sk-..."
```

## Testing

```bash
python test_openai_client.py
```

Tests:
1. ✅ Client initialization
2. ✅ Model info retrieval
3. ✅ Simple content generation
4. ✅ Semantic query generation with novelty checking

## Files Modified

- `src/components/__clients/llm_openai_client.py` - Main client implementation
- `test_openai_client.py` - Test suite

## Benefits of GPT-5.1

1. **Faster responses** with `reasoning_effort='none'`
2. **Better code generation** - Post-trained on coding tools (`apply_patch`, `shell`)
3. **Larger context** - 200K tokens vs 128K in GPT-4o
4. **Cost-effective reasoning** - Use `gpt-5-mini` for 80% savings
5. **Steerable output** - Control verbosity and reasoning independently
6. **Compatible interface** - Works with existing Gemini/SimpleLLMClient code

## Next Steps

1. Set `OPENAI_API_KEY` environment variable
2. Choose model: `gpt-5.1` (best), `gpt-5-mini` (cost-effective), `gpt-4o` (fallback)
3. Tune `reasoning_effort` and `verbosity` for your use case
4. Run tests to verify setup

## Reference

- [OpenAI GPT-5.1 Guide](https://platform.openai.com/docs/guides/reasoning)
- [Responses API Documentation](https://platform.openai.com/docs/api-reference/responses)
- [Migration Guide](https://platform.openai.com/docs/guides/reasoning#migration-guidance)
