# Data Acquisition

Stages 1 and 2 convert raw conversation data into a structured, citation-indexed corpus that downstream stages can consume without re-parsing.

## Stage 1 — Export

`ExportStage` fetches the conversation payload and writes it to the build directory.

```{mermaid}
flowchart LR
    SRC{"Frontend API\nor static file"} --> EX["ExportStage"]
    EX --> TS["conversations_YYYYMMDD_HHMMSS.json"]
    EX --> PTR["conversations.json\n(stable pointer)"]
```

### Algorithm

1. Build a timestamped filename: `raw/conversations_YYYYMMDD_HHMMSS.json`.
2. **Static file mode** — if `export.static_file` is set in `document.yaml`, read that file as JSON.
3. **API mode** — otherwise, GET `{export.frontend_url}/api/conversations` with a 30-second timeout.
4. Log conversation count, total documents, and total response character count.
5. Write the JSON payload to the timestamped file.
6. Create a stable pointer `raw/conversations.json` — first try a symlink, then fall back to a copy if symlinks are not supported.

### Inputs & Outputs

| Direction | Artefact |
|-----------|----------|
| In | Frontend API endpoint **or** static JSON file |
| Out | `build_dir/raw/conversations_YYYYMMDD_HHMMSS.json` |
| Out | `build_dir/raw/conversations.json` (symlink / copy) |

### Error Handling

- Missing static file → `FileNotFoundError`.
- API failure → `requests.raise_for_status()` propagates HTTP errors.
- Symlink failure → silent fallback to file copy.

---

## Stage 2 — Parse

`ParseStage` classifies conversation turns, extracts footnotes, and builds a unified citation map.

```{mermaid}
flowchart TD
    RAW["conversations.json"] --> CLS["Classify turns"]
    CLS --> CH["Chapter turns\n(n_docs > 0 + whitelist)"]
    CLS --> FN["Footnote turns"]
    CH --> CIT["Build citation map"]
    FN --> FNJ["footnotes.json"]
    CIT --> UCI["unified_citations.json"]
    CH --> SES["session_NNN.json"]
```

### Turn Classification

Each conversation turn is labelled **chapter** or **footnote**:

- **Chapter** — the turn has at least one document (`n_docs > 0`) *and* its session index appears in the `sessions.include` whitelist (if one is defined).
- **Footnote** — everything else. The first two sentences of each footnote response are extracted as a compact summary.

### Citation Token Extraction

Citation tokens embedded in responses follow two patterns:

| Pattern | Example | Meaning |
|---------|---------|---------|
| `req_YYYYMMDD_HHMMSS_NNN` | `req_20260304_112859_006` | Request-scoped document reference |
| `doc_HEXID` | `doc_298e93575` | Content-hash document reference |

The regex used:

```python
_TOKEN_RE = re.compile(r'(req_\d{8}_\d{6}_\d{2,4}|doc_[a-f0-9]{4,12})')
```

### Unified Citation Map

The parser scans every chapter response for citation tokens, normalises URLs (strip scheme, fragment, query, trailing slash, lowercase), and builds a merged map:

```json
{
  "www.whitehouse.gov/executive-orders/14148": {
    "url": "https://www.whitehouse.gov/executive-orders/14148/",
    "author": "The White House",
    "date": "2025-01-20",
    "card_type": "web",
    "cited": true,
    "cited_in": ["session_003", "session_007"]
  }
}
```

### Inputs & Outputs

| Direction | Artefact |
|-----------|----------|
| In | `build_dir/raw/conversations.json` |
| Out | `build_dir/parsed/session_NNN.json` (one per chapter turn) |
| Out | `build_dir/parsed/footnotes.json` |
| Out | `build_dir/parsed/unified_citations.json` |

### Error Handling

- Missing `conversations.json` → `FileNotFoundError`.
- Dry-run mode logs intended write counts without touching disk.
