# Unit Testing Strategy

> Goal: lock down the index pipeline against regression. We have repeatedly been
> bitten by silent message drops, stuck sessions, duplicate index rows, and
> doc-id format drift. This plan grows the suite from one regression file
> (`tests/test_session_completion.py`) toward **100% line + branch coverage** of
> the index-critical code, organised into modules → classes → test methods with a
> shared `conftest.py`.

---

## 1. Principles

1. **Anchor on the data invariants, not the implementation.** Every test asserts
   a contract: "1-of-N failure still completes the session", "same URL → 1 row",
   "doc_id matches `^req_\d{8}_\d{6}_\d{4}$`". Refactors are free; contracts are not.
2. **Pure functions first, I/O at the boundary.** Logic (`_doc_id`,
   `_failure_taxonomy`, `url_normalize`, dedup math, schema validation) is tested
   directly. Kafka, GCS/Azure blob, the embedding service, and the LLM are mocked.
3. **No network, no real broker, no GPU.** Tests run in <5s on CI with zero
   external services. `tmp_path` for the filesystem; `MagicMock`/`AsyncMock` for
   producers, consumers, and clients.
4. **Branch coverage is mandatory.** `--cov-branch` is already on in
   `pyproject.toml`. Error paths and `else` arms count.
5. **Deterministic.** Freeze time (`freezegun` or injected `when=`), seed any
   randomness, sort before comparing.

---

## 2. Current State

| Item | Status |
| --- | --- |
| Test files | `tests/test_session_completion.py` only (40 tests) |
| `conftest.py` | **none** — fixtures are inline / ad-hoc `sys.path` hacks |
| pytest config | `pyproject.toml [tool.pytest.ini_options]`, `testpaths=["tests"]`, `timeout=30` |
| coverage config | `[tool.coverage.run] branch=true source=["."]`, `fail_under=0` |
| Async support | `anyio` plugin present (tests parametrised asyncio+trio) |
| Plugins available | `pytest-cov 7`, `pytest-mock`, `pytest-anyio`, `pytest-timeout`, `pytest-dotenv` |

Coverage today is concentrated on `_doc_id.py`, `_failure_taxonomy.py`,
`url_normalize.py`, and a slice of the indexer. The large consumers and the
aggregator are effectively untested.

---

## 3. Coverage Targets (priority order)

Priority is weighted by (a) blast radius on the index and (b) past incidents.

### Tier 1 — index-critical (must reach 100%)

| Module | LOC | Why it matters | Key units to cover |
| --- | --- | --- | --- |
| `src/consumers/_doc_id.py` | 51 | doc_id / dedup_key contract | `make_document_id` format, batch-index encoding, dedup_key stability, timestamp independence |
| `src/consumers/_failure_taxonomy.py` | 103 | retry vs drop decision | every category, ordering (embedding before transient), retryable/degrade/max_attempts flags, unknown fallthrough |
| `src/utils/url_normalize.py` | 63 | dedup correctness | tracking-param strip, host lowercase, default-port strip, fragment strip, query sort, empty/garbage input |
| `src/utils/index_adapter.py` | 462 | schema transforms | `transform_qa_index_to_chunks`, `transform_chunks_to_qa_index`, `validate_index_schema` (round-trip + malformed rows) |
| `src/consumers/kafka_summary_indexer.py` | 1066 | the stuck-session bug lived here | dedup, failure markers, idle sweeper, session-state load/save, aggregation dedup |

### Tier 2 — pipeline producers/aggregators

| Module | LOC | Key units |
| --- | --- | --- |
| `src/consumers/kafka_summarizer.py` | 944 | retry loop, always-emit terminal event, degraded stub, DLQ publish, `_publish_summary_completed` payload shape |
| `src/consumers/kafka_index_aggregator.py` | 1796 | dedup against existing ids, `_verify_all_files_ready`, `_check_session_timeouts`, `_normalize_qa_pair`, pre-append backup |
| `src/consumers/kafka_dead_letter_consumer.py` | 474 | `_append_failure`, `process_message` schema handling |
| `src/utils/async_file_io.py` | 235 | locked read/write round-trips for json / jsonl / text / binary, batch append |

### Tier 3 — supporting clients & config (mock-heavy, lower urgency)

`src/config.py`, `src/config_handler.py`, `src/clients/embedding_client.py`,
`src/clients/llm_*`, `src/components/summarise/llm_citation_researcher.py`,
`src/monitoring/*`. Cover the parsing / formatting branches; mock all transport.

> Out of scope for the 100% target: `run_kafka_*.py` entrypoints (thin
> `if __name__` wrappers) and `document_websearcher.py` / `kafka_web_scraper.py`
> network scraping internals — these get smoke/integration coverage instead and
> are added to `[tool.coverage.run] omit`.

---

## 4. Test Layout

Collect by module, group by class, share fixtures via `conftest.py`.

```
tests/
  conftest.py                      # shared fixtures + sys.path setup
  unit/
    __init__.py
    test_doc_id.py                 # TestMakeDocumentId, TestDedupKey
    test_failure_taxonomy.py       # TestCategories, TestRetryFlags, TestOrdering
    test_url_normalize.py          # TestNormalize, TestEdgeCases
    test_index_adapter.py          # TestQaToChunks, TestChunksToQa, TestSchemaValidation, TestRoundTrip
    test_async_file_io.py          # TestJson, TestJsonl, TestText, TestBinary, TestLocking
  consumers/
    __init__.py
    test_summary_indexer.py        # TestSessionState, TestDedup, TestFailureMarkers,
                                    #   TestIdleSweeper, TestAggregation, TestProcessMessage
    test_summarizer.py             # TestRetryLoop, TestTerminalEvent, TestDegradedStub,
                                    #   TestDlqPublish, TestPayloadSchema
    test_index_aggregator.py       # TestDedup, TestReadiness, TestTimeouts, TestNormalizeQa
    test_dead_letter_consumer.py   # TestAppendFailure, TestProcessMessage
  regression/
    test_session_completion.py     # existing 40 tests (moved here, unchanged)
```

Rationale: `unit/` = pure functions (fast, no fixtures), `consumers/` =
class-based with mocked Kafka/clients, `regression/` = the incident guards we
already trust. Keep `test_session_completion.py` intact so we never lose the
proof that the stuck-session bug stays fixed.

---

## 5. `conftest.py` — shared fixtures

Centralise the boilerplate currently duplicated in `test_session_completion.py`
(the `sys.path.insert` block and inline mocks).

```python
# tests/conftest.py
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
for p in (SRC, SRC / "consumers", SRC / "utils"):
    sys.path.insert(0, str(p))


@pytest.fixture
def anyio_backend():
    """Pin anyio tests to asyncio only (avoid trio dependency)."""
    return "asyncio"


@pytest.fixture
def tmp_cache(tmp_path):
    """A throwaway cache/summaries dir tree."""
    d = tmp_path / "cache" / "summaries"
    d.mkdir(parents=True)
    return d


@pytest.fixture
def combined_index(tmp_path):
    """Empty combined-index jsonl file path."""
    f = tmp_path / "research_summaries_combined.jsonl"
    f.touch()
    return f


@pytest.fixture
def mock_producer():
    """Confluent producer stub that records produced messages."""
    from unittest.mock import MagicMock
    prod = MagicMock()
    prod.produced = []
    prod.produce.side_effect = lambda topic, value, **kw: prod.produced.append(
        (topic, value, kw)
    )
    return prod


@pytest.fixture
def mock_embedder():
    """384-dim deterministic embedding client."""
    from unittest.mock import AsyncMock
    emb = AsyncMock()
    emb.embed_text.return_value = [0.0] * 384
    return emb


@pytest.fixture
def make_indexer(tmp_path, mock_producer, mock_embedder, monkeypatch):
    """Factory that builds a KafkaSummaryIndexerConsumer with all I/O mocked."""
    def _factory(**overrides):
        from consumers.kafka_summary_indexer import KafkaSummaryIndexerConsumer
        inst = KafkaSummaryIndexerConsumer.__new__(KafkaSummaryIndexerConsumer)
        inst.cache_dir = tmp_path / "cache" / "summaries"
        inst.cache_dir.mkdir(parents=True, exist_ok=True)
        inst.combined_index_file = tmp_path / "research_summaries_combined.jsonl"
        inst.combined_index_file.touch()
        inst.producer = mock_producer
        inst.embedding_client = mock_embedder
        for k, v in overrides.items():
            setattr(inst, k, v)
        return inst
    return _factory
```

> Build consumer instances with `__new__` + attribute injection so we never run
> `__init__` (which connects to Kafka). This keeps tests hermetic.

---

## 6. Test specifications

### 6.1 `test_doc_id.py`
- `TestMakeDocumentId`
  - format matches `^req_\d{8}_\d{6}_\d{4,}$`
  - `batch_index` is encoded as `1000 + batch_index`
  - injected `when=` produces deterministic timestamp
- `TestDedupKey`
  - 32-char lowercase hex
  - same URL → same key; same URL with different tracking params → same key
  - different URL → different key
  - same URL at two timestamps → same dedup_key, different document_id

### 6.2 `test_failure_taxonomy.py`
- `TestCategories` — one test per category: `model_empty`, `policy_block`,
  `auth`, `not_found`, `bad_input`, `embedding`, `transient`,
  `permanent_internal`, `unknown`.
- `TestOrdering` — **`embedding` is matched before `transient`** when the message
  contains both "embedding" and "connection refused" (the bug we hit).
- `TestRetryFlags` — `retryable`, `max_attempts`, `degrade` values per category.

### 6.3 `test_url_normalize.py`
- `TestNormalize` — strips `utm_*`/`gclid`/`fbclid`, lowercases scheme+host,
  removes default ports 80/443, retains non-default port, strips fragment,
  sorts query params.
- `TestEdgeCases` — empty string, no-scheme URL, trailing slash, duplicate query
  keys, unicode/percent-encoding.

### 6.4 `test_index_adapter.py`
- `TestQaToChunks` / `TestChunksToQa` — field mapping, empty input, missing
  optional fields.
- `TestRoundTrip` — `chunks → qa → chunks` preserves identity on required fields.
- `TestSchemaValidation` — `validate_index_schema` flags missing keys, wrong
  types, and accepts a well-formed file; covers the malformed-line branch.

### 6.5 `test_async_file_io.py` (`@pytest.mark.anyio`)
- `TestJson` / `TestJsonl` / `TestText` / `TestBinary` — write→read round-trip.
- `TestJsonlBatch` — append batch then read back N rows.
- `TestLocking` — two concurrent writers don't interleave (assert valid JSONL).

### 6.6 `test_summary_indexer.py` (`@pytest.mark.anyio`)
- `TestSessionState` — `_load_session_state` defaults include `dedup_keys_processed`,
  `failed`, `last_event_at`; `_save_session_state` round-trips; legacy state
  (missing new keys) is upgraded via `setdefault`.
- `TestDedup`
  - new `dedup_key` present → second message with same key skipped, count still
    increments
  - legacy message (no `dedup_key`) → falls back to `md5(normalize_url(source_url))`
  - neither `dedup_key` nor `source_url` → no dedup, row written (edge case)
- `TestFailureMarkers` — `status='failed'` increments `received_count`, appends to
  `failed`, skips embedding; session completes at N with one failure.
- `TestIdleSweeper`
  - session idle > `SESSION_IDLE_TIMEOUT` is force-aggregated and marked complete
  - fresh session is skipped
  - already-completed session is skipped
  - state with zero `total_requests` is skipped
- `TestAggregation` — `_aggregate_session_into_combined` skips rows whose
  `document_id` **or** `dedup_key` already exist; legacy combined rows get a
  derived dedup_key; new rows are appended exactly once; rerun is idempotent.
- `TestProcessMessage` — happy path embeds + appends + increments; returns the
  expected dict shape.

### 6.7 `test_summarizer.py` (`@pytest.mark.anyio`)
- `TestRetryLoop` — retryable error retried up to `max_attempts`; non-retryable
  fails immediately.
- `TestTerminalEvent` — `_publish_summary_completed` is **always** called
  (success, degraded, failed).
- `TestDegradedStub` — `model_empty` final failure emits a stub summary with
  `status='degraded'`.
- `TestDlqPublish` — hard failure calls `_publish_to_dlq` with the DLQ schema
  (stage, category, attempts, url, error).
- `TestPayloadSchema` — completed payload includes `document_id`, `dedup_key`,
  `status`, `current_count`, `total_requests`.

### 6.8 `test_index_aggregator.py`
- `TestDedup` — `_get_existing_summary_ids` + skip-existing logic.
- `TestReadiness` — `_verify_all_files_ready` true only when all files present.
- `TestTimeouts` — `_check_session_timeouts` triggers aggregation past threshold.
- `TestNormalizeQa` — `_normalize_qa_pair` field defaults / coercions.

### 6.9 `test_dead_letter_consumer.py` (`@pytest.mark.anyio`)
- `TestAppendFailure` — appends a record to the DLQ file with timestamp.
- `TestProcessMessage` — well-formed and malformed messages both handled without
  raising.

---

## 7. Running & enforcing

Add a coverage gate once Tier 1 is green:

```bash
# full run with branch coverage, scoped to the modules we're hardening
pytest tests/ \
  --cov=src/consumers/_doc_id \
  --cov=src/consumers/_failure_taxonomy \
  --cov=src/utils/url_normalize \
  --cov=src/utils/index_adapter \
  --cov=src/consumers/kafka_summary_indexer \
  --cov-branch --cov-report=term-missing --cov-report=html
```

Progression for `fail_under` in `[tool.coverage.report]`:

| Milestone | `fail_under` | Scope |
| --- | --- | --- |
| M1 | 100 | Tier 1 pure functions (`_doc_id`, `_failure_taxonomy`, `url_normalize`) |
| M2 | 100 | + `index_adapter`, indexer dedup/sweeper/aggregation |
| M3 | 95 | + summarizer + aggregator (some defensive branches mocked) |
| M4 | 90 | whole `src/` with entrypoints + scrapers in `omit` |

Wire `pytest --cov` into `make check` / pre-commit so a coverage drop fails CI.

---

## 8. Suggested order of work

1. Create `tests/conftest.py`; move the existing `sys.path` hack out of
   `test_session_completion.py`.
2. Split the pure-function tests already in `test_session_completion.py` into
   `tests/unit/` files; keep the regression file as the end-to-end guard.
3. Fill Tier 1 to 100% (small, fast wins; sets the `fail_under=100` anchor).
4. Add indexer class tests using the `make_indexer` fixture.
5. Add summarizer + aggregator tests.
6. Backfill Tier 3 clients/config to taste; tune `omit` for the genuinely
   untestable entrypoints.
7. Flip `fail_under` up at each milestone and add the gate to `make check`.
