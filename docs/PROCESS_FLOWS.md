# Viperine Consumer Group — Process Flows

**Helm chart version:** 0.1.0
**Prepared:** June 2026
**Scope:** End-to-end traces for every major pipeline operation. See [ARCHITECTURE.md](ARCHITECTURE.md) for component descriptions and module reference.

This document answers the question: *when the RAG service enqueues a research request, what actually happens?* Each section traces one flow from the Kafka message that triggers it to the final state change it produces, naming every function, module, and state mutation along the way. Sequence diagrams give the bird's-eye view; the step-by-step lists below each diagram pinpoint the exact code responsible for each action.

Readers from a product background should focus on the narrative openers and the diagrams. Readers from an operations background should focus on the step lists, error paths, and timeout values — those are the points where things go wrong in production.

---

## Table of Contents

1. [URL Extraction — WebScraper](#1-url-extraction--webscraper)
2. [LLM Summarisation — Summarizer](#2-llm-summarisation--summarizer)
3. [Embedding and Indexing — SummaryIndexer](#3-embedding-and-indexing--summaryindexer)
4. [Session Completion and Notification](#4-session-completion-and-notification)
5. [Dead Letter Queue — Failure Audit](#5-dead-letter-queue--failure-audit)
6. [Index Aggregation — IndexAggregator](#6-index-aggregation--indexaggregator)
7. [Hot Folder Switch — Live Index Migration](#7-hot-folder-switch--live-index-migration)

---

## 1. URL Extraction — WebScraper

This is the entry point of the pipeline. The RAG service has identified a set of URLs that are relevant to a research query. It publishes one Kafka message per URL to `viperine.web-scrape-requests`. The WebScraper's job is to retrieve the content at that URL and pass it to the Summarizer. The analyst never sees this step directly, but its success or failure determines what material the LLM has to work with.

The most common failure modes here are Cloudflare bot-detection (mitigated by the strategy cascade) and URLs that resolve to paywalled or login-gated content (returned as degraded extraction with `extraction_failed=True`). In both cases the scraper does not drop the document — it always publishes a downstream message so the session counter in the Indexer stays consistent.

**Trigger:** `viperine.web-scrape-requests` message consumed by `KafkaWebScraperConsumer`.
**Outcome:** A message is published to `viperine.summarise-document` with either extracted content or `extraction_failed=True`.

### Sequence

```mermaid
sequenceDiagram
    participant RAG as RAG Service
    participant WS as KafkaWebScraperConsumer
    participant UR as URLResolver
    participant YT as YouTubeTranscriptApi
    participant CS as cloudscraper
    participant PW as Playwright (stealth)
    participant CF as curl_cffi (chrome110)
    participant K as Kafka

    RAG->>K: viperine.web-scrape-requests\n{uri, title, context[], session_id, request_id, total_requests}
    K->>WS: poll() → message

    WS->>WS: json.loads(message.value)
    WS->>UR: _resolve_redirects(uri)
    Note over UR: Follow redirect chain manually<br/>Special-case google.com/url?q=
    UR-->>WS: resolved_url

    alt YouTube URL
        WS->>YT: _extract_youtube_transcript(resolved_url)
        YT-->>WS: transcript text
        WS->>WS: _is_extraction_valid(text)
        WS->>K: viperine.summarise-document {extracted_content}
    else Cloudflare-listed domain
        WS->>CS: _extract_with_cloudscraper(resolved_url)
        CS-->>WS: html / text
        WS->>WS: _is_cloudflare_bot_page(text, title)
        alt bot-challenge page detected
            WS->>PW: _extract_with_playwright(resolved_url)\nstealth_async + async_playwright
            PW-->>WS: html / text
        end
        WS->>WS: _is_extraction_valid() + _check_content_quality()
        WS->>K: viperine.summarise-document
    else Standard URL
        WS->>CF: session.get(resolved_url, impersonate=chrome110)
        Note over CF: PDF? → PyPDF2 PdfReader extraction
        CF-->>WS: html / pdf bytes
        WS->>WS: _is_extraction_valid() + _check_content_quality()
        WS->>K: viperine.summarise-document
    end

    alt All strategies fail or low quality
        WS->>K: viperine.summarise-document\n{extraction_failed: true}
        WS->>K: viperine.dead-letter-queue\n{failed_stage: web_scraper, error: ...}
    end

    WS->>WS: _save_extraction()\ncache/{request_id}/extracted_linked_text.jsonl
```

![flow url extraction sequence](diagrams/process_flows_flow_url_extraction_sequence.png)

### Step-by-step

1. **Poll** — The main loop calls `confluent_kafka.Consumer.poll(timeout=1.0)`. Messages with `KafkaError` are logged and skipped. A `None` poll result (no message) triggers a lag check via `kafka_metrics.record_lag()`.

2. **Deserialise** — `json.loads(message.value().decode('utf-8'))`. If this fails, `metrics.increment_deserialization_errors()` is called with `ErrorCategory.KAFKA_DESERIALIZE` and the message is routed to the DLQ. No retry — a malformed message will always be malformed.

3. **`_resolve_redirects(uri)`** — Uses `URLResolver.resolve()` which follows each hop manually with a 10-second timeout per hop. Google redirect URLs (`google.com/url?q=...`) have the target extracted directly from the `q=` query parameter to avoid hitting the Google confirmation page. Returns the final resolved URL and the redirect chain.

4. **YouTube check** — If `youtube.com/watch?v=` or `youtu.be/` patterns are found in the resolved URL, `_extract_youtube_transcript()` is called. This uses `YouTubeTranscriptApi.fetch_transcript()` with preferred subtitle languages `['en', 'en-US', 'en-GB']` (config: `downloader.subtitle_languages`). YouTube transcripts are typically very high quality for research and bypass the web-scraping entirely.

5. **Cloudflare heuristic** — A domain list of known Cloudflare-heavy sites is checked against the resolved hostname. For those domains, cloudscraper is tried first because it spoofs the TLS handshake to pass Cloudflare's JS challenge check, usually succeeding where a plain `requests.get()` would get a 403.

6. **`_extract_with_cloudscraper(url)`** — Creates a `cloudscraper.create_scraper()` session and performs a GET. The HTML response is parsed by BeautifulSoup; `_extract_full_text_content(soup)` strips navigation and boilerplate. If `_is_cloudflare_bot_page()` detects that cloudscraper was also challenged, Playwright is tried next.

7. **`_extract_with_playwright(url)`** — Launches a headless Chromium browser via `async_playwright()`. `stealth_async(page)` patches the browser fingerprint (WebGL, `navigator.webdriver`, etc.) to reduce bot-detection. The page's `textContent` is extracted after the JS renders. This is the slowest strategy (3–10 seconds per page) but succeeds on sites that check for a real browser.

8. **`curl_cffi` extraction** — For non-Cloudflare URLs: `curl_cffi.requests.Session(impersonate="chrome110").get(url, timeout=30)`. For PDF responses (detected by `Content-Type: application/pdf` or `.pdf` URL suffix), `_extract_pdf_text()` uses `PyPDF2.PdfReader` to extract the text layer. For HTML, BeautifulSoup text extraction is used.

9. **Markdown conversion (opt-in)** — If `web_scraper.extract_markdown=True` and Docling is installed, `DocumentConverter().convert(html)` renders structured Markdown from the raw HTML. This preserves heading hierarchy and table structure, improving LLM summarisation quality. Disabled by default.

10. **Validity gates** — Before publishing, the extracted text is checked:
    - `_is_extraction_valid(text)` — rejects if fewer than `min_words_for_valid_extraction` words
    - `_is_cloudflare_bot_page(text, title)` — rejects if the response looks like a bot challenge
    - `_check_content_quality(text, title)` — rejects if the text is suspiciously short or pure navigation

11. **Publish to `viperine.summarise-document`** — `_publish_to_kafka()` builds the output message and calls `producer.produce()` followed by `producer.flush()`. The message includes `extraction_failed=False` on the success path.

12. **Degraded path** — If all strategies are exhausted or quality is too low, `_publish_failed_url_for_summary()` publishes the message with `extraction_failed=True`. The Summarizer will use Gemini web-grounding on that message. The original failure is also written to `viperine.dead-letter-queue`.

13. **Cache write** — `_save_extraction()` writes the extracted content to `cache/{request_id}/extracted_linked_text.jsonl` for debugging and replay.

### Error paths

| Condition | Behaviour | DLQ? |
|---|---|---|
| `JSONDecodeError` on incoming message | Increment `deserialization_errors`, skip message | Yes |
| URL redirect loop / DNS failure | `URLResolver` raises; `_publish_failed_url_for_summary(extraction_failed=True)` | Yes |
| All strategies return bot-challenge pages | Publish with `extraction_failed=True` | Yes |
| PDF has no text layer | Empty `_extract_pdf_text()` result → `extraction_failed=True` | Yes |
| Kafka producer `produce()` error | `kafka_metrics.record_publish(..., result='error')`, exception logged | No (message lost) |

---

## 2. LLM Summarisation — Summarizer

Once the WebScraper has extracted the raw content, the Summarizer turns it into a structured research asset. This is the most latency-intensive step: a single Gemini call for a long article can take 15–60 seconds, and complex documents that trigger Gemini web-grounding can take longer. The retry-and-degrade framework ensures that even a difficult document eventually produces a terminal event so the session can close.

**Trigger:** `viperine.summarise-document` message consumed by `KafkaSummarizerConsumer`.
**Outcome:** A summary JSONL is written to `cache/{request_id}/summary.jsonl` and a message is published to `viperine.summary-completed`.

### Sequence

```mermaid
sequenceDiagram
    participant WS as KafkaWebScraperConsumer
    participant SU as KafkaSummarizerConsumer
    participant CR as CitationResearchClient
    participant CC as CachedLLMClient
    participant GC as GeminiLLMClient
    participant VA as Vertex AI
    participant RD as Redis
    participant K as Kafka

    WS->>K: viperine.summarise-document
    K->>SU: poll() → message

    SU->>SU: Snapshot _inner_client.get_usage() → _usage_before
    SU->>CR: generate_summary(citation, urls, context, use_web_grounding)
    CR->>CC: generate_content(prompt)
    CC->>CC: sha256(model + "|" + prompt) → cache_key
    CC->>RD: GET cache_key

    alt Cache hit
        RD-->>CC: cached response string
        CC-->>CR: response
    else Cache miss
        CC->>GC: generate_content(prompt, enable_search=use_web_grounding)
        GC->>VA: genai.Client.models.generate_content()
        Note over VA: LLM inference (5s – 120s)
        VA-->>GC: response + usage_metadata
        GC->>GC: _total_usage['input_tokens'] += prompt_tokens
        GC->>GC: _total_usage['output_tokens'] += candidates_tokens
        GC-->>CC: response text
        CC->>RD: SET cache_key response EX 432000
        CC-->>CR: response text
    end

    CR->>CR: json.loads(response) — with repair fallback
    CR-->>SU: {metadata, markdown_summary}

    SU->>SU: validate_summary_content(markdown_summary)
    alt Error-as-content detected
        SU->>SU: classify_error → model_empty / raise ValueError
    end

    SU->>GC: _inner_client.get_usage() → _usage_after
    SU->>SU: input_tokens = _usage_after - _usage_before
    SU->>SU: estimate_cost(model_name, input_tokens, output_tokens)
    SU->>SU: Emit token + cost + latency metrics

    SU->>SU: Write cache/{request_id}/summary.jsonl
    SU->>K: viperine.summary-completed\n{session_id, request_id, document_id, dedup_key,\nsource_url, summary_file, current_count, total_requests, status}
```

![flow llm summarisation sequence](diagrams/process_flows_flow_llm_summarisation_sequence.png)

### Step-by-step

1. **Token snapshot** — Before any LLM activity, `_inner_client.get_usage()` is called on the `GeminiLLMClient` instance (reached via `self.llm_client.client` — the `CachedLLMClient`'s `_inner` property). This gives the cumulative token baseline for this process.

2. **`CitationResearchClient.generate_summary()`** — Builds a structured prompt: `f"{author}. {title}. {url}"` is used as the citation; `context_text` (first 2000 characters of the extracted body) is appended when `extraction_failed=False`. When `extraction_failed=True`, `use_web_grounding=True` is passed, instructing Gemini to retrieve the page content itself via Google Search grounding.

3. **`CachedLLMClient.generate_content(prompt)`** — Computes `cache_key = sha256(model_name + "|" + prompt)[:32]`. Checks Redis. A cache hit means the identical prompt has been seen within the last 5 days — this is most useful for retry scenarios where the summarizer was restarted mid-session.

4. **`GeminiLLMClient.generate_content()`** — Sends the async request to Vertex AI (`genai.Client(vertexai=True, project=..., location=...)`). Response `usage_metadata.prompt_token_count` and `candidates_token_count` are added to `_total_usage` on return. Exponential backoff retry (up to `llm.max_retries=3`) on transient errors.

5. **JSON repair fallback** — `CitationResearchClient` expects the LLM to return valid JSON. If `json.loads()` raises `JSONDecodeError`, a lenient repair pass is attempted before re-raising. This handles the common case where the model adds a markdown code fence around the JSON.

6. **`validate_summary_content(markdown_summary)`** — Scans the summary text against `_MODEL_ERROR_OUTPUT_PAT` in `_failure_taxonomy.py`. Patterns include `# Error`, `I am unable to generate`, `I cannot access`, etc. If matched, a `ValueError` is raised, which `classify_error()` maps to the `model_empty` category triggering a retry (max 2 attempts) and ultimately a degraded stub summary.

7. **Token delta computation** — After the call: `input_tokens = _usage_after.get('input_tokens', 0) - _usage_before.get('input_tokens', 0)`. If the delta is 0 (e.g. cache hit or older LLM client without usage metadata), the fallback is `summary_result.get('usage', {})` for any usage dict the result might carry.

8. **Cost estimation** — `estimate_cost(llm_model, input_tokens, output_tokens)` where `llm_model = getattr(self.llm_client, 'model_name', None)`. This resolves to `"gemini-2.5-pro"` which matches the pricing table. The returned float is added to `llm_estimated_cost_usd_total`.

9. **Retry loop** — `classify_error(exc)` returns a `FailureClassification`. If `retryable=True` and `_attempt < max_attempts`, the loop continues with `asyncio.sleep(2 ** _attempt)` backoff. On exhaustion with `degrade=True`, a stub summary (`{"markdown_summary": "", "metadata": {...}}`) is written and published with `status=degraded`. On exhaustion without degrade, `status=failed` is published.

10. **Always-emit invariant** — `_publish_summary_completed()` is called unconditionally in the `finally` block, regardless of outcome. This ensures the Indexer session counter always closes — no session can hang indefinitely because the Summarizer dropped its terminal event.

11. **`viperine.summary-completed` message** — Contains `session_id`, `request_id`, `document_id`, `dedup_key`, `source_url`, `summary_file` (local path to the written JSONL), `current_count` (how many summaries have been published for this session so far), `total_requests`, and `status` (`success`/`degraded`/`failed`).

### Error paths

| Exception | `classify_error()` category | Retries | Final action |
|---|---|---|---|
| `asyncio.TimeoutError` / HTTP 429 / 503 | `transient` | 3× with backoff | DLQ + `status=failed` |
| LLM returns empty STOP response | `model_empty` | 2× | Emit stub (`status=degraded`) |
| SAFETY / RECITATION / PROHIBITED_CONTENT | `policy_block` | None | DLQ + `status=failed` |
| 403 / `permission denied` | `auth` | None | DLQ + `status=failed` |
| 404 / no such file | `not_found` | None | DLQ + `status=failed` |
| Empty text / bad JSON input | `bad_input` | None | DLQ + `status=failed` |
| Embedding service error | `embedding` | 3× | DLQ + `status=failed` |
| `AssertionError` / `AttributeError` | `permanent_internal` | None | DLQ + `status=failed` |
| Any other `Exception` | `unknown` | 2× | DLQ + `status=failed` |

---

## 3. Embedding and Indexing — SummaryIndexer

With a validated summary in hand, the Indexer's job is to make it searchable. It calls the embedding service to convert the summary text into a 384-dimensional vector, writes the enriched record to the shared research index, and tracks session state to know when all documents in a research session have been processed.

From the analyst's perspective, this is what produces the "new documents available" notification in the browser. Every time the Indexer appends a record to the combined index it signals the RAG service to reload its in-memory index, and notifies the frontend via SSE.

**Trigger:** `viperine.summary-completed` message consumed by `KafkaSummaryIndexerConsumer`.
**Outcome:** A new record is appended to `ACTIVE_INDEX_FOLDER/research_summaries_filtered.jsonl` and downstream notifications are published.

### Sequence

```mermaid
sequenceDiagram
    participant SU as KafkaSummarizerConsumer
    participant SI as KafkaSummaryIndexerConsumer
    participant EM as EmbeddingServiceClient
    participant FS as Filesystem (shared PVC)
    participant K as Kafka

    SU->>K: viperine.summary-completed
    K->>SI: poll() → message

    SI->>SI: Load / create session state\ncache/summaries/{session_id}/{session_id}_state.json
    SI->>SI: Check document_id in document_ids_processed
    SI->>SI: Check dedup_key in dedup_keys_processed

    alt Duplicate detected
        SI->>SI: duplicates_detected.inc(match=document_id|dedup_key)
        SI-->>K: (message ack, no further action)
    end

    SI->>SI: Read summary text from summary_file

    SI->>EM: embed([summary_text])
    Note over EM: httpx.AsyncClient pool<br/>max 20 connections<br/>lazy loop re-init
    EM-->>SI: [embedding_vector] 384-dim

    SI->>SI: Build index entry\n{document_id, dedup_key, source_url,\nsummary_text, summary_embedding,\ndoc_metadata, embedding_metadata, indexed_at}

    SI->>FS: _append_to_index(session_id, entry)\ncache/summaries/{session_id}/{session_id}_summaries_embedded.jsonl

    SI->>SI: Update session state\ndocument_ids_processed.add(document_id)\ndedup_keys_processed.add(dedup_key)\nreceived_count++

    SI->>FS: Acquire fcntl.LOCK_EX on .lock file
    SI->>FS: _aggregate_session_into_combined()\nAppend to ACTIVE_INDEX_FOLDER/research_summaries_filtered.jsonl
    SI->>FS: Release lock

    SI->>K: reload-index\n{action, new_summaries, total_summaries, index_file}
    SI->>K: viperine.frontend-message\n{type, session_id, new_summaries_count, total_summaries_count}

    alt Session complete (received_count >= total_requests)
        SI->>K: viperine.session-complete\n{session_id, summary_index_file, total_summaries, index_type}
        SI->>SI: Mark session complete in state file
    end
```

![flow embedding indexing sequence](diagrams/process_flows_flow_embedding_indexing_sequence.png)

### Step-by-step

1. **Session state load** — `cache/summaries/{session_id}/{session_id}_state.json` is read (or created on first message). The state file persists across pod restarts, ensuring already-processed documents are not double-indexed even after a restart during a long session.

2. **Deduplication check** — `document_id` and `dedup_key` are both checked against the sets stored in the state file. Either match causes the message to be silently acknowledged without further processing. The `duplicates_detected` counter is incremented with `match=document_id` or `match=dedup_key` to make dedup activity visible in Grafana.

3. **Summary file read** — The `summary_file` field in the incoming message is a local path (`cache/web_scrapes/{request_id}/summary.jsonl`). The file is read and the `markdown_summary` field is extracted. If the file is missing (e.g. pod was rescheduled between Summarizer write and Indexer read), the error is classified and published to the DLQ.

4. **`EmbeddingServiceClient.embed([summary_text])`** — Sends an async POST to `https://embeddings.192-168-1-3.nip.io/embeddings`. The client detects if the current `asyncio.run()` created a new event loop (by comparing `id(asyncio.get_event_loop())` with `self._client_loop`) and creates a fresh `httpx.AsyncClient` if so. Retries up to `max_retries=3` with exponential backoff on connection errors.

5. **Index entry construction** — The record written to JSONL:
   ```json
   {
     "document_id": "req_20260610_090000_1001",
     "dedup_key": "6f3e4a2b...",
     "source_url": "https://...",
     "summary_text": "# Headline\n...",
     "summary_embedding": [0.123, ...],
     "doc_metadata": {"title": "...", "author": "...", "word_count": 5000},
     "embedding_metadata": {"embedding_id": "sum_req_...", "embedding_dim": 384, ...},
     "indexed_at": "2026-06-10T09:00:11Z"
   }
   ```

6. **Session JSONL append** — `_append_to_index()` opens `cache/summaries/{session_id}/{session_id}_summaries_embedded.jsonl` in append mode and writes the JSON line. No lock is needed here — only one Indexer pod processes a given session (session_id is routed deterministically by Kafka partition assignment).

7. **Combined index write (locked)** — `_aggregate_session_into_combined()` opens the master index file and `fcntl.flock(LOCK_EX)`. Reads existing entries, builds a dedup set of known `dedup_key` values, appends only new entries, writes back. Releases the lock in the `finally` block. The lock file path is `{index_file}.lock` — it is a sibling of the real index file, resolved through any symlinks before locking.

8. **`reload-index` publish** — Instructs the RAG service to reload its in-memory index from disk. The message includes the updated entry count so the RAG service can log a meaningful "loaded N documents" message.

9. **`viperine.frontend-message` publish** — Delivers a live notification to all connected browser tabs via the RapporteurFrontEnd SSE stream. Includes the count of new summaries and the updated total.

10. **Session completion check** — After incrementing `received_count`, if `received_count >= total_requests`, the session is marked complete and `viperine.session-complete` is published. The idle sweeper (background thread, `SESSION_IDLE_TIMEOUT=120s`) handles sessions that never receive all their messages (e.g. the Summarizer hard-failed on some).

### Error paths

| Condition | Behaviour |
|---|---|
| `summary_file` not found on disk | Classify as `not_found`, publish to DLQ, session count still incremented |
| Embedding service returns 5xx / timeout | Classify as `embedding`, retry 3× with backoff, publish to DLQ on exhaustion |
| `fcntl.flock()` blocks > 30s | Lock timeout, classify as `INDEX_FILE_LOCK`, publish to DLQ |
| JSON decode error in state file | Rebuild empty state (safe because dedup keys are also in the JSONL) |
| `status=failed` incoming message | Increment `received_count` (for session accounting), skip embedding, no index write |

---

## 4. Session Completion and Notification

When a research session reaches its final document (or the idle sweeper fires), the Indexer publishes to `viperine.session-complete`. This triggers the IndexAggregator if enabled. More importantly for immediate user feedback, the `reload-index` and `viperine.frontend-message` signals are also published on every indexed document, not just at session end.

**Session completion conditions:**
1. `received_count >= total_requests` — all expected documents accounted for
2. `SESSION_IDLE_TIMEOUT` (default 120 s, configurable via `SESSION_IDLE_TIMEOUT` env var) elapsed since `last_event_at` — graceful handling of Summarizer failures that consumed a DLQ slot instead of publishing a `summary-completed` event

The idle sweeper runs as a `threading.Thread(daemon=True)` inside `KafkaSummaryIndexerConsumer.start()`:

```mermaid
flowchart TD
    SWEEP["Idle sweeper fires every 30s"]
    ITER["Iterate session_states"]
    CHECK{"last_event_at >\nSESSION_IDLE_TIMEOUT ago?"}
    COMPLETE["Mark session complete\n_publish_session_complete()\n_aggregate_to_combined_index()"]
    SKIP["Skip — session still active"]

    SWEEP --> ITER --> CHECK
    CHECK -- yes --> COMPLETE
    CHECK -- no --> SKIP
```

![flow session idle sweeper](diagrams/process_flows_flow_session_idle_sweeper.png)

**`viperine.session-complete` message schema:**
```json
{
    "session_id": "sess_abc123",
    "summary_index_file": "cache/summaries/sess_abc123/sess_abc123_summaries_embedded.jsonl",
    "total_summaries": 14,
    "index_type": "summaries",
    "partial": true,
    "failed_request_ids": ["req_20260610_090000_1003"]
}
```

`partial=true` when `total_summaries < total_requests` (some documents failed). `failed_request_ids` lists the request IDs that did not produce a successful summary, so operators can identify which sources failed and why from the DLQ log.

---

## 5. Dead Letter Queue — Failure Audit

The DLQ consumer is a write-only audit sink. It does not retry, re-route, or alert — it only persists. The philosophy is that retry logic belongs in the publishing consumer (the WebScraper, Summarizer, or Indexer all have their own retry loops before publishing to the DLQ). By the time a message reaches the DLQ, the publishing consumer has already exhausted all recovery options.

**Trigger:** Any consumer publishes to `viperine.dead-letter-queue`.
**Outcome:** Record appended to `cache/failures/failures.json`; `dlq_failures_by_stage` gauge updated.

### Sequence

```mermaid
sequenceDiagram
    participant ANY as Any Consumer
    participant K as Kafka
    participant DL as KafkaDeadLetterConsumer
    participant FS as Filesystem

    ANY->>K: viperine.dead-letter-queue\n{original_message, failed_stage, error, timestamp,\nrequest_id, session_id, origin_topic}
    K->>DL: poll() → message

    DL->>DL: json.loads(message.value)
    DL->>DL: dlq_messages_processed.inc(failed_stage)
    DL->>DL: dlq_messages_by_origin.inc(origin_topic)
    DL->>DL: Compute message age (now - failure timestamp)
    DL->>DL: dlq_age_seconds.observe(age)
    DL->>DL: dlq_failures_by_stage.labels(stage).inc()

    DL->>FS: Read cache/failures/failures.json
    DL->>FS: failures.append(record)
    DL->>FS: Write cache/failures/failures.json

    DL->>FS: Write cache/failures/{failed_stage}/{request_id}.json
```

![flow dlq sequence](diagrams/process_flows_flow_dlq_sequence.png)

### DLQ record format

```json
{
    "original_message": {
        "uri": "https://example.com/article",
        "session_id": "sess_abc123",
        "request_id": "req_20260610_090000_1001"
    },
    "failed_stage": "summarizer",
    "error": "HTTP 429 Too Many Requests",
    "timestamp": "2026-06-10T09:00:00Z",
    "request_id": "req_20260610_090000_1001",
    "session_id": "sess_abc123",
    "origin_topic": "viperine.summarise-document"
}
```

### Replaying a failed message

The DLQ consumer does not support automatic replay. To manually re-process a failed document:

1. Read the `original_message` from `cache/failures/failures.json`
2. Re-publish it to the `origin_topic` using a Kafka producer or the Makefile's `kafka_produce` helper
3. The corresponding consumer will pick it up on the next poll cycle

---

## 6. Index Aggregation — IndexAggregator

The IndexAggregator handles the optional path where per-session chunk and Q&A pair indices are merged into a cloud-backed combined index in Azure Blob Storage. This is disabled by default because the primary research index (`research_summaries_filtered.jsonl`) is already maintained by the SummaryIndexer. The aggregator is relevant only when the upstream Q&A generation pipeline is active.

**Trigger:** `viperine.session-complete` message consumed by `KafkaIndexAggregatorConsumer` (when `ENABLE_AGGREGATOR=true`).
**Outcome:** `research_chunks_with_embeddings_combined.jsonl` and `research_qa_pairs_with_embeddings_combined.jsonl` updated in Azure Blob Storage.

### Three-flag accumulation model

The aggregator requires three independent completion signals before triggering aggregation for a session:

```mermaid
flowchart LR
    SC["session_summaries_complete[session_id] = True\n(from viperine.session-complete)"]
    CC["session_chunks_complete[session_id] = True\n(from qa-generation topic)"]
    QC["session_qa_complete[session_id] = True\n(from qa-embedding topic)"]
    AND{All three True?}
    AGG["_aggregate_session(session_id)"]

    SC --> AND
    CC --> AND
    QC --> AND
    AND -- yes --> AGG
    AND -- no --> WAIT["Wait for remaining signals"]
```

![flow aggregation three flags](diagrams/process_flows_flow_aggregation_three_flags.png)

### Azure write sequence

```mermaid
sequenceDiagram
    participant IA as KafkaIndexAggregatorConsumer
    participant AZ as Azure Blob Storage

    IA->>AZ: Download indices/research_chunks_with_embeddings_combined.jsonl
    AZ-->>IA: existing combined JSONL (or 404 → empty)

    IA->>IA: Merge session chunks with existing\nDedup by document_id + chunk_id
    IA->>IA: transform_qa_index_to_chunks() if QA schema

    IA->>AZ: Upload indices/research_chunks_with_embeddings_combined.jsonl
    IA->>AZ: Upload indices/research_qa_pairs_with_embeddings_combined.jsonl
    IA->>AZ: Archive session files to research-archive/ container
    IA->>IA: Clean local cache if cleanup_after_upload=True
```

![flow azure write sequence](diagrams/process_flows_flow_azure_write_sequence.png)

### Schema transformation

Q&A pairs use a flat schema that must be normalised to the enriched-chunks format before upload. `transform_qa_index_to_chunks()` in `src/utils/index_adapter.py` performs this conversion:

| Q&A Index field | Maps to Enriched Chunks field |
|---|---|
| `chunk_content` | `content` |
| `question` + `answer` | Prepended to `content` when `include_qa_in_content=True` |
| `combined_embedding` | `embedding_enrichment.combined_embedding` |
| `chunk_id` | `chunk_metadata.chunk_id` |
| `document_id` | `document_metadata.document_id` |

---

## 7. Hot Folder Switch — Live Index Migration

The `KafkaSummaryIndexerConsumer` supports live migration of the combined index to a different directory without a pod restart. This is used when the active `x_indices_*` folder needs to be rotated (e.g. a fresh index generation run has completed and the new folder is ready to serve).

**Trigger:** A message is published to the `index-folder-switch` Kafka topic.
**Outcome:** `self.combined_index_file` is atomically re-routed to the new directory path.

```mermaid
sequenceDiagram
    participant OP as Operator
    participant K as Kafka
    participant SI_BG as SummaryIndexer\nfolder-switch thread
    participant SI_MAIN as SummaryIndexer\nmain poll thread

    OP->>K: index-folder-switch\n{"folder": "x_indices_eo"}
    K->>SI_BG: background thread poll

    SI_BG->>SI_BG: Validate folder name starts with "x_indices_"
    SI_BG->>SI_BG: Resolve new combined_index_file path
    SI_BG->>SI_BG: self.combined_index_file = new_path  # atomic assignment

    Note over SI_MAIN: Next _aggregate_session_into_combined() call<br/>reads self.combined_index_file → new path
```

![flow hot folder switch](diagrams/process_flows_flow_hot_folder_switch.png)

The folder-switch thread and the main poll thread share `self.combined_index_file`. Python's GIL makes the `self.combined_index_file = new_path` assignment atomic, so there is no window where the main thread reads a partially-updated path.

The `fcntl.LOCK_EX` call in `_aggregate_session_into_combined()` always resolves symlinks to the real file before acquiring the lock, so the lock file correctly sibling-targets the new index file after a switch.
