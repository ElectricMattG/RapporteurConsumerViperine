# Chapter Generation & Threat Analysis

Stages 3 and 4 transform parsed sessions into Markdown chapters and optionally enrich them with cached threat-cluster analyses.

## Stage 3 — Chapters

`ChapterStage` renders parsed sessions into publication-ready Markdown files, detects threat clusters, and writes the manifest checkpoint that all downstream stages depend on.

```{mermaid}
flowchart TD
    SES["parsed/session_NNN.json"] --> PLAN["Build chapter plan"]
    PLAN --> TC["Detect threat clusters"]
    TC --> REND["Render chapters"]
    REND --> MD["chapters/NN-slug.md"]
    REND --> IDX["chapters/index.md"]
    REND --> MAN["chapters/manifest.json"]
    TC --> TCJ["parsed/threat_clusters.json"]
```

### Chapter Plan

If `render.chapters` is defined in `document.yaml`, the stage maps sessions to chapters according to that plan. Otherwise each chapter-eligible session becomes its own chapter.

### Threat Cluster Detection

The stage scans responses for cluster headings and their associated data tables:

```python
_TC_HEADING_RE = re.compile(
    r'^\*\*Threat\s+Cluster\s+(\d+)\s*:\s*(.+?)\*\*\s*$',
    re.MULTILINE,
)
```

Table rows below each heading are parsed into structured objects with citation-token cross-references. Detected clusters are written to `parsed/threat_clusters.json` for the Kaliya stage.

### Rendering

For each chapter the stage:

1. Extracts an abstract from the first session (capped at `render.abstract_max_words`).
2. Renders sections with inline citation resolution.
3. Appends a **Top Sources** block (up to 5 abbreviated references per section).
4. Builds chapter-level unified references.
5. Generates a URL-safe slug (truncated to 60 characters).

### Manifest

The manifest is the pipeline's checkpoint file. Its initial schema:

```json
{
  "project": "rapporteur-maduro",
  "generated": "2025-11-14T03:12:44Z",
  "chapters": [
    {
      "index": 1,
      "file": "01-diplomatic-signals.md",
      "title": "Diplomatic Signals and Sanctions Architecture",
      "sessions": [3, 4],
      "word_count": 4280,
      "has_threat_clusters": true
    }
  ]
}
```

Downstream stages extend the manifest with their own metadata blocks.

### Inputs & Outputs

| Direction | Artefact |
|-----------|----------|
| In | `build_dir/raw/conversations.json` |
| In | `build_dir/parsed/session_NNN.json` |
| In | `build_dir/parsed/unified_citations.json` |
| Out | `build_dir/chapters/NN-{slug}.md` |
| Out | `build_dir/chapters/index.md` |
| Out | `build_dir/chapters/manifest.json` |
| Out | `build_dir/parsed/threat_clusters.json` (if clusters detected) |

---

## Stage 4 — Kaliya

`KaliyaStage` matches detected threat clusters to cached Kaliya analyses and injects enrichment sections into chapter files.

```{mermaid}
flowchart TD
    TCJ["threat_clusters.json"] --> IDX["Index Kaliya cache"]
    IDX --> MATCH["Match by URL overlap"]
    MATCH --> IMG["Convert images\n(max 1200px, JPEG 85%)"]
    MATCH --> INJ["Inject analysis sections"]
    INJ --> MD["Updated chapter *.md"]
    IMG --> IMGD["kaliya/images/*.jpeg"]
```

### Cache Matching

The stage indexes every `report.json` in the Kaliya cache directory (configured via `paths.kaliya_cache`) and builds a lookup of normalised URLs per request. For each threat cluster, it finds the cache entry with the highest URL intersection.

### Image Processing

Matched cache entries may include figures. The stage:

1. Loads image files referenced in `figures.json`.
2. Resizes any image wider than **1200 px** using Lanczos resampling.
3. Saves as JPEG at **quality 85** to `build_dir/kaliya/images/`.
4. Replaces `[Figure N]` references in the analysis text with embedded Markdown images.

### Section Injection

For each matched cluster the stage:

1. Strips any previously injected Kaliya section (for idempotency on re-runs).
2. Renders the analysis as a `####`-level section with figures and optional Mermaid timelines.
3. Injects the section immediately after the cluster's data table in the chapter file.

### Inputs & Outputs

| Direction | Artefact |
|-----------|----------|
| In | `build_dir/parsed/threat_clusters.json` |
| In | `build_dir/chapters/manifest.json` |
| In | `build_dir/chapters/NN-*.md` |
| In | Kaliya cache directory (`report.json`, `figures.json`, images) |
| Out | `build_dir/kaliya/images/*.jpeg` |
| Out | Updated chapter Markdown files |
| Out | Updated `manifest.json` (kaliya metadata block) |

### Error Handling

- No threat clusters detected → stage returns immediately.
- Kaliya cache directory missing → error logged, return.
- Individual image conversion failures → skip with warning, continue.
- Missing or malformed `report.json` → skip cache entry with warning.
