# Citation Resolution

Stage 9 (`CiteStage`) resolves opaque citation tokens into human-readable APA-formatted hyperlinks. It operates in two phases — one for source chapters (token-based) and one for LLM-generated chapters (plain-text EO references).

## Overview

```{mermaid}
flowchart TD
    subgraph Phase1["Phase 1 — Source Chapters"]
        SCAN["Scan for\n(doc_...; req_...) groups"] --> RES["Resolve tokens\nEO index → citation map"]
        RES --> APA["Format APA\n(first vs subsequent mention)"]
        APA --> DEDUP["Deduplicate &\nconsolidate"]
    end

    subgraph Phase2["Phase 2 — Downstream Chapters"]
        DSSCAN["Scan for plain-text\nExecutive Order NNNNN"] --> HALL["Hallucination check\n(title vs ground truth)"]
        HALL --> LINK["Inject hyperlinks"]
    end

    Phase1 --> Phase2
```

## Phase 1 — Token Resolution

### Token Groups

Citation tokens appear in parenthesised groups within chapter text:

```
(doc_298e93575; req_20260304_112859_006)
```

The stage scans for these groups using:

```python
_CITE_GROUP_RE = re.compile(r'(\((?:[^()]+;\s*)*[^()]+\))')
```

### Resolution Order

For each token the stage tries, in order:

1. **EO title index** — built from conversation `eo_results` metadata and cross-referenced with ground-truth EO mentions found in chapter text.
2. **Unified citation map** — the URL-keyed map produced by ParseStage.
3. **Base key fallback** — strip trailing digits to match a parent document.

### APA Formatting

Resolved citations are formatted as Markdown hyperlinks following APA conventions:

| Mention | Format |
|---------|--------|
| First | `[Exec. Order No. 14,148, Initial Rescissions of Harmful… (2025)](url)` |
| Subsequent | `[Exec. Order No. 14,148 (2025)](url)` |

First-mention titles are truncated at `cite.eo_name_max_chars` with an ellipsis.

### Deduplication & Consolidation

After resolution, several cleanup passes run:

1. **Adjacent merge** — `([EO1](url1)); ([EO2](url2))` → `([EO1](url1); [EO2](url2))`.
2. **Trailing section merge** — `([EO](url)), Section X` → `([EO, Section X](url))`.
3. **Intra-group dedup** — remove duplicate references within a single parenthesised group.

Chapters are written back and marked `cite_status: "done"` in the manifest.

## Phase 2 — Downstream Resolution

LLM-generated chapters (conclusion, introduction) do not contain citation tokens — they reference Executive Orders in plain text. Phase 2 handles these.

### Hallucination Detection

The stage scans for patterns like:

```
Executive Order 14148, "Initial Rescissions of Harmful Executive Orders"
```

It extracts the EO number and quoted title, then checks the title against the ground-truth EO title index. Mismatches are logged as hallucination warnings.

### Link Injection

Plain-text `Executive Order NNNNN` references are replaced with Markdown hyperlinks pointing to the known URL for that EO number. A per-line JSON changelog is written to `parsed/{filename}_cite_downstream.json`.

## Inputs & Outputs

| Direction | Artefact |
|-----------|----------|
| In | `build_dir/raw/conversations.json` (citation maps, EO results) |
| In | `build_dir/parsed/session_NNN.json` |
| In | `build_dir/chapters/manifest.json` |
| In | `build_dir/chapters/NN-*.md` |
| Out | Updated chapter files (resolved citations) |
| Out | `build_dir/parsed/eo_title_index.json` |
| Out | `build_dir/parsed/{filename}_cite_downstream.json` |
| Out | Updated `manifest.json` (cite + cite_downstream blocks) |

## Error Handling

- Unresolvable tokens are left in place with a debug log entry.
- Title mismatches in downstream chapters are logged as hallucination warnings but do not halt the pipeline.
- Unresolvable EOs in downstream text are kept as plain text.
