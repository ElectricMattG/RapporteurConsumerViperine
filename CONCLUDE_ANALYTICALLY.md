# Analytics Conclude — Feature Specification

## Overview

A new `--analytics-conclude` stage in `rapporteur_cli.py` that integrates cluster analysis, Kaliya narratives, EO policy summaries, and sampled media articles into an overarching analytical conclusion about the media's response to EO implementation.

## Status: IMPLEMENTED

- **Module:** `src/components/publisher/analytics_conclude.py`
- **Config:** `AnalyticsConcludeConfig` in `src/models/publisher.py`
- **CLI flag:** `--analytics-conclude`
- **Tests:** `tests/test_analytics_conclude.py` (26 tests)

## Usage

```bash
python src/components/publisher/rapporteur_cli.py docs/.../document.yaml --analytics-conclude
python src/components/publisher/rapporteur_cli.py docs/.../document.yaml --analytics-conclude --dry-run
```

## Architecture

### Grounding Package Assembly (token-budgeted to 850K tokens)

| Part | Source | Tokens (culture_and_media) |
|------|--------|---------------------------|
| Part 1 — Phase 4 Cluster Analysis | `phase4_{category}_{ts}.md` (excluding Appendix) | ~20K |
| Part 2 — Kaliya Narratives | 5 per-cluster narrative markdown files | ~3K |
| Part 3 — EO Policy Summaries | `x_indices_eo` JSONL (8 EOs for culture_and_media) | ~1K |
| Part 4 — Sampled Media Articles | Bundle JSONL, proportionally sampled per cluster | ~490K |
| **Total** | | **~518K tokens** |

### Token Budgeting

1. Fixed grounding (Parts 1-3) is assembled first and token-counted with tiktoken
2. Remaining budget = `target_tokens` (850K) − fixed_tokens − prompt_overhead (4K)
3. Articles are proportionally sampled per cluster — each cluster gets a share relative to its article count
4. For culture_and_media: all 306 cluster assignments (from 293 unique articles) fit within budget

### Article Citation Format

Each article is formatted with:
- Sequential number matching the Phase 4 cluster map (Article 1-293)
- `**Cite as:** (bundle_NNN, summary)` for parenthetical references
- Author, publisher, date prominently displayed for prose citation

### LLM Configuration

- **Provider:** OpenAI (default)
- **Model:** GPT-5.4
- **Reasoning effort:** high
- **Max output tokens:** 32,000
- **Responses API:** with `reasoning` parameter for reasoning tokens

### Output Artefacts

All saved to `output_dir` (default: `/mnt/workspace_2/bboa/documents/analytics/`):

| File | Description |
|------|-------------|
| `analytics_conclude_{category}_{ts}.md` | Final markdown conclusion |
| `analytics_conclude_{category}_{ts}_grounding.md` | Token-budgeted grounding package |
| `analytics_conclude_{category}_{ts}_prompt.md` | Full LLM prompt |
| `analytics_conclude_{category}_{ts}_usage.json` | Token usage, cost, reasoning tokens |

## Configuration (document.yaml)

```yaml
analytics_conclude:
  enabled: true
  category: culture_and_media
  provider: openai
  model: gpt-5.4
  reasoning_effort: high
  target_tokens: 850000
  max_output_tokens: 32000
  clustering_output_dir: /mnt/workspace_2/bboa/documents/clustering
  eo_index_folder: x_indices_eo
  rag_service_url: http://192.168.1.3:8000
  output_dir: /mnt/workspace_2/bboa/documents/analytics
```

## Original Requirements

> Take the results and the analyses of the clusters and integrate them to provide an overarching conclusions about the media's response to EO implementation and media representation. Look back on the grounding data and the analyses and findings and explain the more finer semantic patterns and behaviours of the legacy media in relation to the administrations efforts. We want to reason through and consider the results, motivations of the legacy media.
>
> We want to explore evidence based understanding (citation where necessary): does the media, through shared framing, build a narrative arch longitudinally — are there long-term trends in framing, semantics that point to specific objectives or broader goals of the corporate media complex. What perceptions are being built and intended meaning constructed through long-term trends. Infer if there are any intended behaviours, opinions, attitudes or beliefs that are being constructed through stable reframing patterns over time. Are specific senior members being targeted and why. Use logic to build up to conclusions, guard against confirmation bias, and reason to remove uncertainty and ambiguity to find the most robustly structured conclusions. Draw final conclusions.

- **Citation style:** `(bundle_NNN, summary)` for articles; also cite by author and publication outlet in prose
- **Model:** OpenAI GPT-5.4, reasoning high, verbosity high
- **Save:** reasoning tokens and grounding package
- **Output:** markdown with headers identifying relationships between ideas
