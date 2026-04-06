# Phase 4 Sampling & Grounding Changes

Tracking all modifications to the Phase 4 cluster analysis pipeline.

---

## 1. EO Cache Path Migration
**Files:** `src/demo/demo_media_linkage.py` (line 53), `src/models/publisher.py` (lines 182, 234, 471, 491)
**Change:** `eo_implementation_cache_integrated.jsonl` → `eo_implementation_cache_curated.jsonl`
**Reason:** Curated file (1188 records) removes duplicates and empty-URL entries vs integrated (1688).

## 2. EO Anchor Expansion — culture_and_media
**Files:** `src/components/analysis/media_linkage.py`, `dispatch_kaliya_all.py`
**Change:** Added EO 13963 (DEI purge) and EO 13989 (ethics commitments) to culture_and_media anchors.
**Result:** 10 anchor EOs total: 13950, 13963, 13989, 14148, 14151, 14173, 14183, 14184, 14185, 14347.

## 3. Removed BM25 Topic/Person Filters from Unlinked Sampling
**File:** `src/demo/demo_media_linkage.py` — `run_phase4()`
**Change:** Unlinked article sampling no longer uses BM25 topic or person queries. Sampling is now purely random within a date window.
**Reason:** BM25 topic filters missed articles lacking the `culture_and_media` tag (e.g. detroitnews Warren article). Date-range-only sampling ensures coverage.

## 4. Jan 2025 Hard Floor on Date Window
**File:** `src/demo/demo_media_linkage.py` — `run_phase4()` (~line 1876)
**Change:**
```python
# Before
earliest = min(linked_dates).replace(day=1)
# After
earliest = max(min(linked_dates).replace(day=1), datetime(2025, 1, 1))
```
**Reason:** Constrain sampling to articles from January 2025 onward regardless of linked article date range.

## 5. Flat 150 Date-Window Buffer + BM25 Queries Restored
**File:** `src/demo/demo_media_linkage.py` — `run_phase4()` (~line 1898)
**Change:**
```python
# Before (flat 150 from all unlinked)
sample_size = min(150, len(unlinked_in_window)) if unlinked_in_window else 0
prompt_articles = linked + unlinked_sample

# After (BM25-queried always included + 150 random buffer)
bm25_queried = [a for a in media_articles if source_type starts with "bm25_"]
unlinked = [remaining non-linked, non-bm25 articles]
sample_size = min(150, len(unlinked_in_window))
prompt_articles = linked + bm25_queried + unlinked_sample
```
**Reason:** BM25 queries surface targeted articles; 150 random date-window articles buffer for different patterns the queries may miss. BM25 results always included (not subject to sampling).

---

## Data Snapshot (culture_and_media)
| Pool | Count |
|------|-------|
| Linked articles | 679 |
| Unlinked articles (total) | 1431 |
| Unlinked in date window (≥ Jan 2025) | ~1332 |
| Curated EO grounding events | 519 (of 1188 total) |
| Greylist domains | .gov, .mil, .gov.uk, .gov.au, .gov.ca, .gc.ca, dvidshub.net, wikipedia.org, britannica.com, encyclopedia.com |
| Min confidence | 0.0 (all events pass) |
