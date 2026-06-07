# Indexer Regression: Stuck Session Aggregation

**Status:** Plan — no code changes yet.
**Date:** 2026-06-04
**Severity:** High — silent data loss; valid embeddings never reach combined index.

---

## 1. Symptom

Session `query_1780577948` produced 18 web-scrape requests. 17 reached the
indexer, were embedded successfully, and were written to the per-session file
[cache/summaries/query_1780577948/query_1780577948_summaries_embedded.jsonl](cache/summaries/query_1780577948/query_1780577948_summaries_embedded.jsonl)
(17 lines, 191 KB). **None of those 17 documents reached the combined index**
[`research_summaries_combined.jsonl`](../RapporteurRAG/x_indices_merged/research_summaries_combined.jsonl).

Index aggregation never fired — the session is permanently stuck at `17/18`.

Expected behaviour (per pipeline contract & previous smoke tests):
- The 17 successful documents **must** be aggregated into the combined index.
- The 1 failing URL **must** be routed to the dead-letter queue.
- Session must finalize within bounded time regardless of upstream failures.

---

## 2. Root Cause

Three independent gaps compound into total session loss:

### 2.1 Summarizer silently drops messages on LLM failure
[src/consumers/kafka_summarizer.py](src/consumers/kafka_summarizer.py)

- Line 406: `_publish_summary_completed()` is called **only on the happy path**.
- Lines 431–435: `except Exception` logs the error and returns. No publish to
  `summary-completed`, no publish to `dead-letter-queue`.
- Concrete failure observed: `Empty response from model (FinishReason.STOP)`
  for URL_010 of session `query_1780577948` at 13:01:00 and 13:01:29.

Result: indexer’s `received_count` for the session never reaches
`total_requests`.

### 2.2 Indexer has no timeout / no incomplete-session sweeper
[src/consumers/kafka_summary_indexer.py](src/consumers/kafka_summary_indexer.py)

- Line 711–720: aggregation only fires when
  `state['received_count'] >= state['total_requests']`.
- Session state carries `started_at` (line 359) but no code reads it.
- Main poll loop (lines ~799–820) does not perform any periodic session-state
  reconciliation — there is no “if a session has been idle for N seconds,
  finalise it with what we have” path.

Result: a single dropped upstream message wedges the entire session forever.

### 2.3 Dead-letter queue is one-way and unused on this path
[src/consumers/kafka_dead_letter_consumer.py](src/consumers/kafka_dead_letter_consumer.py)

- Consumes `dead-letter-queue`, appends to `failures.json`. That’s it.
- No producer in either web-scraper or summarizer publishes to
  `dead-letter-queue` for this failure mode.
- Web scraper’s `_publish_failed_url_for_summary`
  ([kafka_web_scraper.py:1977–2033](src/consumers/kafka_web_scraper.py#L1977-L2033))
  re-publishes to `summarise-document` with `extraction_failed=True`. That is
  fine — but only as long as the summarizer **always** terminates that message
  (success → `summary-completed`, failure → DLQ + completion marker).

### 2.4 Misplaced configuration
[src/config.py](src/config.py) line 233: `session_timeout: 300` lives under
`index_aggregator`, not `summary_indexer`. Nothing reads it.

---

## 3. Contract (Authoritative)

For any session with `total_requests = N`:

1. The indexer **must** observe exactly `N` terminal events (success **or**
   failure) per session. Every upstream stage is responsible for emitting a
   terminal event for every request it receives.
2. Successful events deliver an embeddable summary. Failed events deliver a
   failure marker (no embedding).
3. Aggregation runs when **either**
   - `received_count == total_requests`, **or**
   - the session has been idle (no new event) for `SESSION_IDLE_TIMEOUT`
     seconds, **or**
   - the session has exceeded `SESSION_MAX_AGE` seconds since `started_at`.
4. Whatever successful documents exist at aggregation time **must** be written
   to the combined index. Failures must not block successes.
5. Every failure must produce exactly one DLQ record carrying the original
   `request_id`, `session_id`, stage of failure, and error detail.

---

## 3-bis. Document ID & Dedup-Key Contract

The previous all-`doc_<md5>` scheme dedupes correctly but breaks lineage
with the existing `req_*` corpus already in the combined index. The fix is
to **separate identity from dedup**.

### 3-bis.1 Format

Every record produced by the summarizer carries **both** of these fields:

| Field          | Format                                           | Purpose                          |
|----------------|--------------------------------------------------|----------------------------------|
| `document_id`  | `req_<YYYYMMDD>_<HHMMSS>_100<N>`                 | Human-readable lineage; unique per emission. |
| `dedup_key`    | `md5(normalized_url)` (32 hex chars)             | Deterministic per-URL key. Sole basis for dedup. |

Where:
- `YYYYMMDD_HHMMSS` = UTC timestamp at the moment the summarizer constructs
  the id (second precision).
- `100<N>` = per-batch index starting at `1001`, incremented within the
  same `(session_id, second)` bucket so two records produced in the same
  second still get distinct ids. `<N>` zero-pads to keep monotonic
  alphanumeric ordering (`1001..1999`, then `10001..` if a batch ever
  exceeds 999 in one second).
- `normalized_url` = url with: lowercased scheme+host, default ports
  stripped, fragment removed, trailing slash on path-only urls preserved,
  utm_*/gclid/fbclid query params stripped. (New helper
  `src/utils/url_normalize.py`.)

Example for `https://en.wikipedia.org/wiki/2026_Iran_war` summarised at
2026-06-04 13:01:09 UTC, 7th URL of the batch:
```
document_id = req_20260604_130109_1007
dedup_key   = 7d933c5f60c9e1...     (32 hex chars)
```

### 3-bis.2 Construction site

Construction lives in **one** function in
[src/consumers/_doc_id.py](src/consumers/_doc_id.py) (new), used by the
summarizer at the existing call site (currently
[kafka_summarizer.py:606](src/consumers/kafka_summarizer.py#L606)). No
other consumer constructs ids — they are passive receivers.

```python
def make_document_id(url: str, batch_index: int = 1,
                     when: datetime | None = None) -> tuple[str, str]:
    when = when or datetime.utcnow()
    stamp = when.strftime("%Y%m%d_%H%M%S")
    document_id = f"req_{stamp}_{1000 + batch_index}"
    dedup_key   = hashlib.md5(normalize_url(url).encode()).hexdigest()
    return document_id, dedup_key
```

`document_id` uniqueness is guaranteed by `(timestamp, batch_index)`. The
summarizer is responsible for passing a strictly increasing `batch_index`
for the same second; the per-message counter lives on the summarizer
instance and is keyed by `(session_id, YYYYMMDD_HHMMSS)`.

### 3-bis.3 Indexer dedup change

[kafka_summary_indexer.py](src/consumers/kafka_summary_indexer.py) must
dedup on `dedup_key`, **not** `document_id`:

- The "existing documents" set the indexer loads at aggregation time is
  built by reading every record's `dedup_key` if present, otherwise
  falling back to `md5(record['source_url'])` for legacy records that
  don't carry one.
- New records are written only when their `dedup_key` is not in the set.
- The combined index file gains the `dedup_key` field on new writes;
  existing records are left untouched (a one-shot backfill script is
  optional, listed in §8 Out of Scope).

### 3-bis.4 Migration & legacy IDs

The corpus today contains a mix of:
1. `doc_<md5(url)[:12]>` — recent fix.
2. `req_<YYYYMMDD>_<HHMMSS>_<reqhash>_url_<NNN>` — legacy producer.
3. `<uuid>_url_<NNN>` — older RAG runs.
4. `req_<YYYYMMDD>_<URLHASH6>_<NNNN>` — **new format from this plan.**

Dedup must succeed across all four. Strategy:
- Build the existing-id set as a **set of `dedup_key`s**, computed from
  each record's `dedup_key` field if present, else from
  `md5(normalize_url(record['source_url']))`. All four legacy formats have
  a `source_url` field, so this works uniformly.
- No rewrites of existing `document_id`s. They're frozen for backwards
  compatibility with downstream consumers and citations.

### 3-bis.5 Indexer's own filter

The indexer also computes the dedup_key for incoming messages (in case a
producer forgets to set one) — same one-liner, same normalize_url. This
ensures dedup is correct even if a producer regresses.

---

## 3a. Failure Taxonomy

A *failure* is any condition that prevents emitting a usable summary for a
given `request_id` after exhausting in-process retries (§3b). Failures are
classified by **category** (drives retry/DLQ policy) and **stage** (drives
DLQ routing key). The summarizer + indexer must classify with the same
table.

### 3a.1 Stages
- `web_scraper` — fetching/extracting page content.
- `summarizer` — generating an LLM summary.
- `summary_indexer` — embedding or aggregating.

### 3a.2 Categories

| Category    | Retry? | Definition | Examples |
|-------------|:------:|------------|----------|
| `transient` | ✅ yes | Likely to succeed on retry; not caused by input or policy. | `429 RATE_LIMIT_EXCEEDED`, `503 / 504`, `UNAVAILABLE`, `DEADLINE_EXCEEDED`, raw socket / DNS / TLS reset, `curl (7)` connection refused, embedding service `connection refused`, Kafka `LEADER_NOT_AVAILABLE`. |
| `model_empty` | ✅ yes (limited) | LLM returned no usable content. Often transient (model hiccup, quota). Retry with backoff and, on the **last** retry, with reduced context length / disabled tools. | `FinishReason.STOP` with empty text, `MAX_TOKENS` with empty text, `OTHER` with empty text, `Response candidate has no content`, `No response candidates generated` (without block reason). |
| `policy_block` | ❌ no | LLM/provider refused on safety/policy grounds. Retrying produces the same result. | `FinishReason.SAFETY`, `FinishReason.RECITATION`, `FinishReason.BLOCKLIST`, `FinishReason.PROHIBITED_CONTENT`, `FinishReason.SPII`, `prompt_feedback.block_reason in {SAFETY, OTHER}`. |
| `auth`      | ❌ no | Credentials problem; retry will not help — page operator alarm. | `401`, `403 PERMISSION_DENIED`, expired token, missing project. |
| `not_found` | ❌ no | URL or resource definitively absent. | `404`, `410`, scraper `host_unreachable_after_dns_ok` with definitive 4xx body. |
| `bad_input` | ❌ no | The message itself is malformed or the input cannot be summarised (e.g. truly empty body **and** web grounding disabled). | Missing `request_id`, `url == ''` after extraction failure, content < `MIN_INPUT_CHARS` and grounding off. |
| `permanent_internal` | ❌ no | Bug / contract violation that the operator must fix. | `KeyError` on a required field, schema-validation error, JSON deserialisation error of LLM output after fallback parse. |
| `embedding` | ✅ yes | Embedding service failed for an otherwise-good summary. | HTTP 5xx from embedding service, vector length mismatch, empty vector. |
| `unknown`   | ✅ yes (once) | Uncategorised exception. Retry once; then DLQ as `unknown`. | Any `Exception` not matched above. |

### 3a.3 Mapping (concrete)

The mapper is a small pure function `classify_error(exc, context) -> (category, retryable: bool, terminal: bool)` shared between summarizer and indexer
(new module `src/consumers/_failure_taxonomy.py`). Indicative mapping:

```
gcp.api_core.exceptions.ResourceExhausted        -> transient
gcp.api_core.exceptions.ServiceUnavailable       -> transient
gcp.api_core.exceptions.DeadlineExceeded         -> transient
gcp.api_core.exceptions.PermissionDenied         -> auth
gcp.api_core.exceptions.Unauthenticated          -> auth
gcp.api_core.exceptions.NotFound                 -> not_found
gcp.api_core.exceptions.InvalidArgument          -> bad_input
requests.ConnectionError / Timeout               -> transient
HTTPError 5xx                                    -> transient
HTTPError 401/403                                -> auth
HTTPError 404/410                                -> not_found
HTTPError 4xx (other)                            -> bad_input
"Empty response from model (finish_reason: STOP)" -> model_empty
"Empty response from model (finish_reason: SAFETY|RECITATION|BLOCKLIST|PROHIBITED_CONTENT|SPII)" -> policy_block
"Response blocked: {block_reason}"               -> policy_block
"Response candidate has no content"              -> model_empty
"No response candidates generated" (no block)    -> model_empty
KeyError / TypeError on required field           -> permanent_internal
JSONDecodeError on LLM output after fallback     -> permanent_internal
EmbeddingService.* network / 5xx                 -> embedding (transient subtype)
default                                          -> unknown
```

The classifier returns a stable string `category`. The DLQ record records
both the **original** `error_class` and the **mapped** `category`.

---

## 3b. Retry Policy

Retries happen **inside the consumer process before** producing a terminal
event. Kafka offset is not committed until the terminal event is produced.

### 3b.1 Defaults

```
RETRY_MAX_ATTEMPTS = 3            # total attempts including first
RETRY_BACKOFF_BASE_MS = 500       # exponential base
RETRY_BACKOFF_MAX_MS = 8000
RETRY_JITTER = "full"             # full-jitter exponential backoff
RETRY_TOTAL_BUDGET_S = 45         # hard cap per request, including all attempts
```

Per-category overrides:

| Category    | Max attempts | Backoff base | Notes |
|-------------|:------------:|:------------:|-------|
| `transient` | 3            | 500 ms       | Standard exponential backoff with full jitter. |
| `model_empty` | 2          | 750 ms       | On final attempt, **degrade**: drop optional context (drop tool/grounding extras, halve `context_text`, lower temperature). Still bounded by `RETRY_TOTAL_BUDGET_S`. |
| `embedding` | 3            | 500 ms       | Same as transient; if final attempt also fails, route to DLQ but retain the summary in per-session JSONL (so a future replay can re-embed). |
| `unknown`   | 1 retry      | 500 ms       | Single retry then DLQ. |
| `auth`, `not_found`, `bad_input`, `policy_block`, `permanent_internal` | 0 (no retry) | — | Immediate DLQ + failure marker. |

`Retry-After` header (when present, e.g. on `429`) overrides computed
backoff up to `RETRY_BACKOFF_MAX_MS * RETRY_MAX_ATTEMPTS`.

### 3b.2 Algorithm (per request)

```
attempt = 0
deadline = now() + RETRY_TOTAL_BUDGET_S
while True:
    attempt += 1
    try:
        result = do_work()           # generate_summary / embed / fetch
        emit_success(result)
        return
    except Exception as e:
        category, retryable, terminal = classify_error(e, ctx)
        record_attempt_metric(stage, category, attempt)
        if terminal or not retryable or attempt >= max_attempts(category)
                                       or now() >= deadline:
            emit_failure(category, e, attempts=attempt)   # publish DLQ + completion marker
            return
        sleep_jittered_backoff(attempt, category, retry_after=parse_retry_after(e))
        if category == "model_empty" and attempt == max_attempts(category) - 1:
            degrade_request(ctx)      # drop tools / shorten / lower temp
        continue
```

### 3b.3 Pipeline-level retries (separate from in-process)

In-process retry handles transient blips. Pipeline-level retry (different
model, re-scraping after several minutes) is intentionally out of scope for
this plan and tracked separately. The DLQ is the boundary: anything in the
DLQ has already exhausted its in-process retries.

### 3b.4 Idempotency

- Producers must **only** commit Kafka offset after the terminal event is
  emitted. A crash mid-retry causes the message to be re-delivered and
  retried from attempt 1 — acceptable, because the indexer dedupes on
  `document_id` and the DLQ entry carries `request_id` + `attempts` so
  duplicates are recognisable.
- `request_id` is the idempotency key end-to-end. The summarizer must not
  produce two `summary-completed` events for the same `request_id`; if
  retry happens after a crash and the same content is re-summarised, the
  resulting `document_id` (md5 of url) collides and is deduped at the
  indexer.

---

## 4. Remediation Plan

Phased so we can ship the unblocking fix first, then harden.

### Phase A — Stop the bleeding (must merge together)

**A1. Summarizer always emits a terminal event, with bounded retries.**
File: [src/consumers/kafka_summarizer.py](src/consumers/kafka_summarizer.py)
- Wrap the LLM call in the retry loop from §3b.2, using
  `classify_error()` (§3a.3) to decide retry vs DLQ.
- Every code path either publishes `summary-completed` with
  `status="success"` (happy path) or publishes `summary-completed` with
  `status="failed", category=<taxonomy>` **and** publishes to
  `dead-letter-queue` with the schema in §C2.
- The failure marker on `summary-completed` must include `session_id`,
  `request_id`, `query_id`, `total_requests`, `status="failed"`, `category`,
  `attempts`, so the indexer can advance `received_count` without trying
  to embed.
- On `model_empty` final attempt apply degradation: drop web grounding,
  truncate context to 500 chars, lower temperature. If that also fails,
  emit failure terminal event.
- Commit Kafka offset only after the terminal event is produced (or DLQ’d).

**A2. Indexer handles failure markers.**
File: [src/consumers/kafka_summary_indexer.py](src/consumers/kafka_summary_indexer.py)
- New branch in the main message handler: if `status == "failed"`,
  do **not** call the embedding service. Just increment `received_count`,
  record the failed `request_id` in `state['failed']`, and proceed to the
  completion check.

**A3. Indexer adds an idle-session sweeper.**
- Background timer thread (or in-loop check on each `poll()` returning `None`)
  that walks `self.session_states` and, for any session where
  `now - last_event_at > SESSION_IDLE_TIMEOUT` and not yet completed:
  - logs a `⚠ Finalizing incomplete session` line including
    `received/total/failed`,
  - calls `_aggregate_to_combined_index(session_id)` with the partial set,
  - marks `state['completed']=True`,
  - publishes `session-complete` with `partial=True` and the failure list.
- New state fields: `last_event_at`, `failed: List[request_id]`.
- Defaults: `SESSION_IDLE_TIMEOUT=120s`, `SESSION_MAX_AGE=900s`. Read from
  config; env override `INDEXER_SESSION_IDLE_TIMEOUT`.

**A4. Recover the stuck session in production.**
- One-shot script (kept under `scripts/`) that, given a session id, walks
  `cache/summaries/<sid>/<sid>_summaries_embedded.jsonl` and runs the same
  aggregation path the indexer would. Idempotent (uses existing
  `document_id` dedupe).
- Run once to recover `query_1780577948` (and any other stuck sessions found
  by listing `cache/summaries/*` whose `*_summaries_embedded.jsonl` has rows
  not present in the combined index).

### Phase B — Move the misplaced knob

**B1.** Move `session_timeout` (and add `session_max_age`,
`session_idle_timeout`) from `index_aggregator` to `summary_indexer` in
[src/config.py](src/config.py). Wire them through to the indexer. Keep an
alias so existing values aren’t silently ignored.

### Phase C — Harden the DLQ contract

**C1.** Web scraper failures: when `_publish_failed_url_for_summary` is the
chosen recovery, also emit a heartbeat that the request is still in flight,
so the summarizer’s eventual terminal event is the **only** terminal event
the indexer expects (no double-counting).

**C2.** DLQ payload schema (single source of truth, used by every stage):
```json
{
  "stage": "summarizer",
  "category": "model_empty",
  "session_id": "...",
  "request_id": "...",
  "query_id": "...",
  "url": "...",
  "error_class": "Exception",
  "error_message": "Empty response from model (finish_reason: STOP)",
  "occurred_at": "ISO-8601",
  "attempts": 2,
  "retryable": false,
  "degraded": true,
  "first_attempt_at": "ISO-8601",
  "last_attempt_at": "ISO-8601"
}
```
`stage ∈ {web_scraper, summarizer, summary_indexer}`,
`category` from §3a.2. Document this in [docs/](docs/) and use the same
shape from web scraper / indexer / image extractor.

**C3.** DLQ consumer: add a metric counter per `(stage, error_class)` and
expose via the existing `/metrics` endpoint so we can alert on regressions.

### Phase D — Observability

- Indexer prints a one-liner per minute summarizing in-flight sessions:
  `sid=… received/total=17/18 idle=42s failed=1`.
- Aggregation logs include the new partial-session line with the failed
  `request_id`s when `partial=True`.
- Add Prometheus gauges:
  - `summary_indexer_sessions_open`
  - `summary_indexer_session_idle_seconds` (per session id, label-bounded)
  - `summary_indexer_partial_aggregations_total`

---

## 5. Tests (regression guards — write **before** the code changes land)

Location: [tests/](tests/) with new file
[`tests/test_session_completion.py`](tests/test_session_completion.py).
All tests use the existing in-process Kafka stub / fakes already used by
other consumer tests — no real broker.

### 5.1 Failure taxonomy + retry

New module under test: `src/consumers/_failure_taxonomy.py`.

- **`test_classify_transient_errors`** — `ResourceExhausted`, `ServiceUnavailable`, `DeadlineExceeded`, raw `ConnectionError`, HTTP 503 → `(transient, retryable=True)`.
- **`test_classify_finish_reason_stop_empty_text`** — the regression: `Exception("Empty response from model (finish_reason: STOP)")` → `(model_empty, retryable=True)`.
- **`test_classify_finish_reason_safety`** — `FinishReason.SAFETY|RECITATION|BLOCKLIST|PROHIBITED_CONTENT|SPII` → `(policy_block, retryable=False)`.
- **`test_classify_auth`** — 401/403/`PermissionDenied`/`Unauthenticated` → `(auth, retryable=False)`.
- **`test_classify_not_found`** — 404/410/`NotFound` → `(not_found, retryable=False)`.
- **`test_classify_bad_input`** — missing `request_id`, empty url with no grounding → `(bad_input, retryable=False)`.
- **`test_classify_unknown_default`** — arbitrary `RuntimeError` → `(unknown, retryable=True)`.

### 5.2 Summarizer behaviour with retries

- **`test_summarizer_retries_transient_then_succeeds`**
  LLM raises `ServiceUnavailable` twice, succeeds on third call. Assert: 3
  calls, exactly 1 `summary-completed` with `status="success"`, 0 DLQ
  messages, no offset committed before the success produce.
- **`test_summarizer_dlq_after_max_attempts_transient`**
  LLM raises `ServiceUnavailable` on every attempt. Assert: `RETRY_MAX_ATTEMPTS`
  calls, 1 `summary-completed` with `status="failed", category="transient", attempts=3`,
  1 DLQ record matching schema with `attempts=3, retryable=False` (post-exhaustion).
- **`test_summarizer_no_retry_on_policy_block`**
  LLM raises `Exception("Response blocked: SAFETY")`. Assert: 1 call only,
  1 failure terminal event with `category="policy_block"`, 1 DLQ entry,
  `attempts=1`.
- **`test_summarizer_no_retry_on_auth`**
  `PermissionDenied` raised. 1 call, immediate DLQ + failure marker, `category="auth"`.
- **`test_summarizer_model_empty_degrades_on_final_attempt`** — the regression case.
  LLM raises `"Empty response from model (finish_reason: STOP)"` on every call.
  Assert: 2 attempts (per `model_empty` policy), final attempt called with
  degraded params (no web grounding, context truncated, lower temperature),
  then 1 failure terminal event + 1 DLQ entry with `category="model_empty", degraded=true`.
- **`test_summarizer_model_empty_recovers_after_degrade`**
  LLM raises empty on attempt 1, succeeds on degraded attempt 2. Assert:
  1 success terminal event, 0 DLQ.
- **`test_summarizer_retry_total_budget_enforced`**
  Each attempt sleeps 30s (mocked). `RETRY_TOTAL_BUDGET_S=45`. Assert: only
  2 attempts before budget exhaustion, then DLQ.
- **`test_summarizer_respects_retry_after_header`**
  `ResourceExhausted` with `Retry-After: 2`. Assert backoff sleeps ≥2s on
  the failed attempt.
- **`test_summarizer_offset_commit_only_after_terminal`**
  Producer mock fails to flush on first attempt; consumer mock asserts
  `commit()` was not called until the terminal event was successfully
  produced.
- **`test_summarizer_does_not_publish_dlq_on_success`**
  Negative control: success path produces exactly one `summary-completed`,
  zero DLQ messages.

### 5.3 Indexer — failure markers

- **`test_indexer_advances_count_on_failure_marker_without_embedding`**
  Feed 2 success + 1 failure message for a 3-request session; assert the
  embedding client was called exactly twice, `state['failed']` contains the
  failed `request_id`, and aggregation fired exactly once with 2 documents
  written to the combined file.

- **`test_indexer_aggregates_partial_when_session_completes_with_failures`**
  3-request session, all 3 events received, 1 is a failure. Combined index
  receives 2 new entries. `session-complete` event is published with
  `partial=True`.

### 5.4 Indexer — idle / max-age sweeper

- **`test_idle_sweeper_finalizes_session_after_timeout`**
  Inject 17 successes, never deliver the 18th. Advance the clock past
  `SESSION_IDLE_TIMEOUT`. Assert sweeper aggregates 17 entries to the
  combined file and emits `session-complete` with `partial=True` and 1
  missing `request_id`.

- **`test_max_age_sweeper_finalizes_session`**
  Slow drip of events (each refreshing `last_event_at`) but session crosses
  `SESSION_MAX_AGE`. Assert sweeper still finalizes.

- **`test_sweeper_does_not_double_aggregate`**
  Sweeper finalizes session, then a late 18th message arrives. Assert no
  second aggregation, no duplicate writes (idempotency via `document_id`
  dedupe is exercised end-to-end).

### 5.5 Aggregation correctness under partial state

- **`test_aggregation_writes_only_new_doc_ids`**
  Pre-populate combined file with one of the session’s records. Run
  aggregation; assert `new=N-1`, `duplicates=1`, total file lines increase
  by exactly `N-1`.

- **`test_aggregation_locks_real_path_not_symlink`**
  Create `combined.jsonl` with sibling symlink `filtered.jsonl`. Spawn two
  threads calling `_aggregate_to_combined_index` concurrently. Assert no
  interleaved/torn lines and final line count matches expected (this also
  guards the symlink-resolved lock fix from regressing).

### 5.5a Document ID & dedup key

New module under test: `src/consumers/_doc_id.py`.

- **`test_make_document_id_format`**
  `make_document_id("https://en.wikipedia.org/wiki/2026_Iran_war", batch_index=7, when=2026-06-04 13:01:09)` returns `("req_20260604_130109_1007", "7d933c5f60c9e1...")` and the `dedup_key` is the full 32-char md5 of the normalized URL.
- **`test_make_document_id_url_normalization`**
  These three URLs all yield the **same `dedup_key`**:
  - `HTTPS://EN.Wikipedia.org/wiki/2026_Iran_war?utm_source=foo`
  - `https://en.wikipedia.org:443/wiki/2026_Iran_war#section`
  - `https://en.wikipedia.org/wiki/2026_Iran_war`
- **`test_make_document_id_uniqueness_within_second`**
  10 calls in the same second with `batch_index=1..10` yield 10 distinct
  `document_id`s (`req_..._1001`..`req_..._1010`).
- **`test_make_document_id_default_batch_and_time`**
  Omitting `batch_index` yields `_1001`. Omitting `when` uses `datetime.utcnow()`.
- **`test_make_document_id_stability`**
  Calling 100 times for the same `(url, batch_index, when)` yields identical output.
- **`test_indexer_dedup_uses_dedup_key_not_document_id`**
  Pre-populate combined index with a record whose `document_id =
  "req_20260101_120000_1001"` and `dedup_key = md5(URL)`. Submit a new
  message for the same URL with `document_id = "req_20260604_130109_1007"`.
  Assert: aggregation reports `duplicates=1`, no new line written.
- **`test_indexer_dedup_against_legacy_records_without_dedup_key`**
  Pre-populate combined index with three legacy records (no `dedup_key`
  field): one `doc_<md5>`, one `req_<DATE>_<HH>_url_<NNN>`, one
  `<uuid>_url_<NNN>` — all sharing one URL. Submit a new message for
  that same URL. Assert duplicate, no new line.
- **`test_indexer_computes_dedup_key_when_missing_from_message`**
  Producer omits `dedup_key`. Indexer recomputes it from `source_url` and
  dedup still works.
- **`test_combined_index_new_writes_carry_dedup_key_field`**
  After aggregation of a fresh session, every newly-appended JSONL line
  contains a `dedup_key` field (32 hex chars). Existing legacy lines are
  untouched.
- **`test_summarizer_emits_both_document_id_and_dedup_key`**
  Successful summarization message published to `summary-completed`
  contains both fields; `document_id` matches `^req_\d{8}_\d{6}_1\d{3,}$`.

### 5.6 End-to-end pipeline (in-process)

- **`test_pipeline_partial_failure_e2e`**
  Stand up the three consumers against in-process Kafka fakes. Submit one
  session of 5 URLs, force one URL’s LLM call to raise. Assert:
  - 4 documents land in combined index,
  - 1 DLQ record exists with stage=`summarizer`,
  - `session-complete` arrives with `partial=True`, `failed=[request_id_x]`,
  - no orphan rows in the per-session embedded jsonl that were not aggregated.

### 5.7 Configuration

- **`test_session_timeout_resolved_from_summary_indexer_section`**
  Loads config with `summary_indexer.session_idle_timeout=7`, asserts the
  indexer object exposes `7` and not the old `index_aggregator` value.
- **`test_session_timeout_env_override`**
  `INDEXER_SESSION_IDLE_TIMEOUT=11` overrides config.

---

## 6. Acceptance Criteria

The regression is fixed when **all** of the following hold:

1. With one URL forced to fail mid-session, the combined index gains
   `N-1` rows for that session within `SESSION_IDLE_TIMEOUT + 5s`.
2. The DLQ contains exactly one record matching the failure schema for the
   failing `request_id`.
3. `session-complete` is published with `partial=True` and the failed
   `request_id` listed.
4. Re-delivery of any message in the session (replay / late arrival) does
   **not** produce duplicate rows in the combined index.
5. New `document_id`s match `^req_\d{8}_\d{6}_1\d{3,}$` and every new
   record carries a 32-hex `dedup_key`.
6. The same URL submitted in two different sessions / batches produces
   **two distinct `document_id`s but one `dedup_key`**, and the indexer
   writes only one row.
7. All tests in §5 pass in CI.
8. The recovery script (Phase A4) successfully aggregates the existing
   stuck session(s) found under `cache/summaries/`.

---

## 7. Rollout

1. Land tests in §5 first (will fail — that’s the point).
2. Land Phase A (A1–A3); tests in §5.1–§5.3 should now pass.
3. Run Phase A4 recovery script against current cluster; verify combined
   index grows by the expected number of orphaned rows.
4. Land Phase B (config move).
5. Land Phase C (DLQ schema + metrics) and Phase D (observability) together;
   §5.5 e2e test should now pass.
6. Bake for one full pipeline run; check Prometheus
   `summary_indexer_partial_aggregations_total` is sensible.

---

## 8. Out of Scope (tracked separately)

- Retrying failed Gemini calls with backoff before DLQ’ing (separate
  resilience ticket).
- Rebuilding the combined index from per-session files as a periodic
  reconciliation job (nice-to-have; the recovery script in A4 covers the
  one-shot need).
- Removing legacy `req_*` document IDs from the combined index (cleanup,
  not a regression).
