# EO / Events Overlay Feature

**Feature Branch Context:** `x_indices_eo/` overlay — globally toggleable EO and Events indices
**Status:** Planning → Implementation

---

## Overview

Add an independent EO (Executive Order) + Events overlay to the RAG service that can be toggled **on or off concurrently with any active main index**. The overlay is not tied to `active_folder` — it lives in `x_indices_eo/` and is mounted alongside whatever primary index is loaded.

### Data sources (`x_indices_eo/`)

| File | Records | Purpose |
|---|---|---|
| `eo_events_bboa_full_20260310_194954.jsonl` | 314 | Event records linked to EO numbers |
| `research_summaries_with_embeddings_combined.jsonl` | 63 | EO semantic summaries (embedded) |
| `research_qa_pairs_with_embeddings_combined.jsonl` | 2,076 | EO QA pairs (embedded) |

### Event record schema

```json
{
  "event_key": "33d86ae389e52947",
  "window_start": "2025-01-01",
  "window_end": "2025-01-15",
  "descriptor": "Deputy Pentagon Press Secretary … held a press briefing …",
  "eo_identifier": "EO 14347",
  "eo_title": "Restoring the United States Department of War",
  "eo_date": "2025-09-05",
  "source_url": "https://…",
  "exec_official": "Unknown",
  "media_count": 1,
  "risk_counts": {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "EXTREME": 0, "unrated": 1}
}
```

---

## Behaviour Design

### Toggles (independent of `active_folder`)

| Toggle | Default | Effect |
|---|---|---|
| `eo_overlay_enabled` | `false` | Load + query the EO semantic index |
| `events_overlay_enabled` | `false` | Load + query the events JSONL |

Both can be `true` simultaneously. Switching the primary active folder does **not** reset these flags.

### Query cascade when both are enabled

```
User query
    │
    ├─► [Main index] semantic search  (always, when active)
    │
    ├─► [EO overlay]  semantic search → top N EO summaries  (if eo_overlay_enabled)
    │       └─ returns: eo_identifier list from matched summaries
    │
    └─► [Events overlay] lookup by eo_identifier  (if events_overlay_enabled)
            └─ returns: all events matching those EO numbers  (exact key match, non-semantic)
```

Events are fetched **by EO number** derived from the semantic EO results, not by embedding similarity. This is intentional — the link is structural, not conceptual.

### Optional top-N EO results

When `eo_overlay_enabled=true` the caller may set `eo_top_k` (default 10) to control how many EO summaries are returned and surfaced to the LLM grounding. These appear as a separate `eo_results` field in the response alongside the main `retrieved_docs`.

---

## Phase Plan

---

### Phase 1 — State & data loading  ✅ Implemented

**Goal:** Mount the EO/events index files into `ServiceState` without breaking anything.

**Changes — `src/services/rag/main.py`:**

1. Add fields to `ServiceState`:
   ```python
   # EO / Events overlay
   eo_summaries: List[Dict] = []
   eo_index_path: Optional[str] = None
   eo_index_loaded: bool = False
   eo_events: List[Dict] = []          # raw event records
   eo_event_index: Dict[str, List[Dict]] = {}  # keyed by normalised eo_identifier
   eo_events_path: Optional[str] = None
   eo_overlay_enabled: bool = False
   events_overlay_enabled: bool = False
   ```

2. Add `load_eo_events(path)` function:
   - Reads `eo_events_bboa_full_20260310_194954.jsonl`
   - Builds `eo_event_index`: `{ normalised_eo_id → [event, ...] }`
   - Normalise key: strip `"EO "` / `"Executive Order "` prefix, lowercase, strip spaces

3. Add `_find_eo_events_file(folder)` discovery helper (glob `eo_events_bboa_full_*.jsonl`).

4. In `startup_event()`:
   - Resolve `x_indices_eo/` path (hardcoded subfolder name, or env var `EO_INDEX_FOLDER`, default `x_indices_eo`)
   - Load EO summaries via existing `load_summaries_index()` path; store in `state.eo_summaries`
   - Load events via `load_eo_events()`; store in `state.eo_events` + `state.eo_event_index`
   - Log counts; **do not set `eo_overlay_enabled`** — stays `false` until toggled

**Acceptance:** Service starts, `/ready` response includes `eo_overlay` stanza showing record counts and `enabled: false` for both toggles.

---

### Phase 2 — Toggle endpoints  ✅ Implemented

**Goal:** Runtime on/off switches accessible from the frontend or API client.

**New Pydantic models:**

```python
class EoEventsToggleRequest(BaseModel):
    eo_overlay_enabled: Optional[bool] = None     # None = don't change
    events_overlay_enabled: Optional[bool] = None  # None = don't change
    eo_top_k: Optional[int] = Field(None, ge=1, le=50)  # optional default override

class EoEventsToggleResponse(BaseModel):
    eo_overlay_enabled: bool
    events_overlay_enabled: bool
    eo_top_k: int
    eo_summaries_loaded: int
    eo_events_loaded: int
```

**New endpoint:** `POST /toggle-eo-events`

- Accepts partial update (omitted fields leave current state unchanged)
- Returns the current full overlay state after applying the change
- Validates that the indices are actually loaded before enabling; returns `409` if not loaded

**Update `/ready`:** Add `eo_overlay` block:
```json
"eo_overlay": {
  "eo_overlay_enabled": false,
  "events_overlay_enabled": false,
  "eo_top_k": 10,
  "eo_summaries": 63,
  "eo_events": 314,
  "eo_index_path": "…/x_indices_eo/research_summaries_with_embeddings_combined.jsonl",
  "eo_events_path": "…/x_indices_eo/eo_events_bboa_full_20260310_194954.jsonl"
}
```

**Acceptance:** `POST /toggle-eo-events {"eo_overlay_enabled": true}` returns correct state; `/ready` reflects change.

---

### Phase 3 — EO semantic search function  ✅ Implemented

**Goal:** Add `search_eo_index(query, top_k)` reusing the existing embedding + dot-product pipeline.

```python
async def search_eo_index(query: str, top_k: int = 10) -> List[Dict]:
    """Semantic search over EO summaries. Returns list of {summary, similarity, eo_identifiers}."""
```

- Same pattern as `search_index()` / `search_qa_index()`
- Each result enriched with `eo_identifiers`: list of unique `eo_identifier` values from the matched summary's `doc_metadata` (or derived from summary text — to be verified against actual records)
- Returns empty list if `eo_index_loaded = False`

**Acceptance:** Unit-testable in isolation; `pytest` stub passes.

---

### Phase 4 — Events cascade lookup  ✅ Implemented

**Goal:** Given EO identifiers from Phase 3, return matching event records.

```python
def lookup_events_for_eos(eo_identifiers: List[str]) -> List[Dict]:
    """
    Non-semantic lookup. For each eo_identifier, fetch all matching events from
    state.eo_event_index and return deduplicated list ordered by window_start.
    """
```

- Normalise each input identifier the same way keys were normalised at load time
- Deduplication by `event_key`
- Sort by `window_start` ascending
- Returns `[]` if `events_overlay_enabled = False` or index empty

**Acceptance:** Given a known EO identifier, returns correct events from the loaded index.

---

### Phase 5 — Integrate into query endpoints  ✅ Implemented

**Goal:** Attach EO / events overlay results to `/search`, `/query_llm`, `/grounding_preview` responses.

**Response model additions** (new optional fields, backwards-compatible):

```python
class SearchResponse(BaseModel):
    …
    eo_results: Optional[List[Dict]] = None       # EO semantic hits
    event_results: Optional[List[Dict]] = None    # Event records from cascade
```

**Logic in each endpoint** (after main index search):

```python
eo_results = None
event_results = None

if state.eo_overlay_enabled:
    eo_hits = await search_eo_index(request.query, top_k=state.eo_top_k)
    eo_results = eo_hits

    if state.events_overlay_enabled and eo_hits:
        eo_ids = [h['eo_identifier'] for h in eo_hits if h.get('eo_identifier')]
        event_results = lookup_events_for_eos(eo_ids)
```

**LLM grounding (query_llm only):** When `eo_overlay_enabled`, append a compact EO context block to the prompt (similar to existing `enrichment_text` injection):

```
--- Executive Order Context ---
[EO 14183] Prioritizing Military Excellence and Readiness (2025-01-27)
  Summary: …
  Related events (3): [window_start] descriptor | [window_start] descriptor …
```

**Acceptance:** `GET /ready` shows overlays off; `POST /toggle-eo-events {"eo_overlay_enabled":true, "events_overlay_enabled":true}`; next `/query_llm` response contains non-null `eo_results` and `event_results`.

---

### Phase 6 — switch-index reload preserves overlay state  ✅ Implemented

**Verified:** `switch_index` handler contains zero references to `eo_*` fields — EO/events toggles are fully segregated and survive any active-folder switch.

**Goal:** Switching the active main index via `POST /switch-index` must **not** reset EO/events toggles.

- Verify `switch_index()` handler does not touch `state.eo_*` fields (should be true after Phase 1 if fields are segregated)
- Add assertion / log line confirming overlay state is preserved after switch
- Update `/ready` response to clearly show `active_folder` vs `eo_overlay` as independent sections

**Acceptance:** Switch index → overlay toggles unchanged; `/ready` confirms both.

---

### Phase 7 — Frontend integration  ✅ To do

**Goal:** Expose toggle controls and display EO/events results in the UI.

UI additions (tracked separately from backend):
- Toggle switches: "EO Index" / "Events" (independent checkboxes, always visible regardless of active folder)
- `eo_top_k` slider (1–20, default 10), shown when EO toggle is on
- Results panel: "Executive Orders" collapsible card showing EO summaries
- Cascade results: "Related Events" nested under each EO card, sorted by `window_start`
- EO results fed into citation map so `(EO Title, Date)` links appear inline in LLM responses

---

## Implementation Order

```
Phase 1  (state + loading)
    → Phase 2  (toggle endpoints)
        → Phase 3  (EO semantic search)
            → Phase 4  (events cascade)
                → Phase 5  (endpoint integration)
                    → Phase 6  (index-switch preservation)
                        → Phase 7  (frontend)
```

Each phase is independently testable and deployable. Phases 1–2 are safe to ship with
overlays disabled by default — no user-visible change until a toggle is flipped.

---

## File Locations

| File | Changes |
|---|---|
| `src/services/rag/main.py` | Phases 1–6 — all backend changes |
| `x_indices_eo/` | Read-only data source; no changes |
| Frontend (TBD) | Phase 7 — toggle UI + results display |

---

## Open Questions

- [ ] Does `research_summaries_with_embeddings_combined.jsonl` in `x_indices_eo/` have `eo_identifier` in `doc_metadata`? **Resolved:** No — EO numbers are extracted from `summary_text` via `_EO_ID_RE` regex at query time.
- [ ] Should `eo_top_k` be a per-request parameter or a service-level default (settable via toggle endpoint)? Current plan: service-level default, overridable per request.
- [ ] Events date filtering: should `start_date` / `end_date` from the main query apply to `window_start` on events? Likely yes — add in Phase 5.
- [ ] Should the EO overlay also reload on `POST /switch-index`? Current plan: no — `x_indices_eo/` is a fixed global overlay. Reconsider if per-topic EO indices emerge.

---

## EO Filtering / Noise Exclusion  ✅ Complete

Filter specific EO numbers from Phases 5 – 7 output (diagrams, tables, chapter enrichment)
without modifying the underlying cache.  Useful for removing noise EOs from presentation.

### Usage

```bash
# Demo CLI
python -m src.demo.demo_eo_linking --phase5 --phase6 --exclude-eos 14165 14370

# Rapporteur CLI
python -m src.components.publisher.rapporteur_cli --eo-link --exclude-eos 14165 14370
```

### YAML config
```yaml
eo_linking:
  exclude_eos:
    - "14165"
    - "14370"
```

### Implementation
- `filter_precedence_graph(graph, exclude_eos)` — pure utility strips excluded EOs from graph keys and edge lists
- `EODiagramGenerator.generate/generate_focused/generate_for_chapters/generate_unified` — `exclude_eos` param
- `ImplementationTableGenerator.generate_for_chapters/format_table` — `exclude_eos` param
- `ChapterEnricher.__init__` — stores `exclude_eos`, applies during enrichment
- `EOLinkingConfig.exclude_eos` — config field, parsed from YAML
- `eo_linking_stage.py` — reads config, threads through Phases 5-7
