"""Media Linking — link media articles to EOs and EO events.

Phase 1 components:
- ``MediaExtractor``: inverse of GovMilExtractor — keeps non-.gov/.mil media
- ``MediaLinkageCache``: JSONL append-only cache for media-to-EO linkage records

Phase 2-3-4 components:
- ``_MEDIA_LINK_PROMPT`` / ``_MEDIA_REFLECT_PROMPT``: media-specific prompts
- ``MediaLinkReflectDriver``: two-call LLM synthesis (LINK then REFLECT)
- ``MediaLinkageExtractor``: parse REFLECT JSON into flat media linkage records
- ``_format_eo_cache_as_grounding()``: format EO impl cache for prompt injection
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from src.components.analysis.eo_linking import (_RAG_PRICING, ArticleRecord,
                                                _clean_json_markdown,
                                                _normalise_url)
from src.config_handler import Base

# ---------------------------------------------------------------------------
# TokenCounter — provider-native token counting (no estimation)
# ---------------------------------------------------------------------------

class TokenCounter(Base):
    """Count tokens using the provider's own tokenizer — no char/4 estimation.

    - OpenAI: ``tiktoken.encoding_for_model(model)``
    - Gemini: ``genai.Client().models.count_tokens()``
    """

    def __init__(self, provider: str, model: str) -> None:
        super().__init__()
        self.provider = provider
        self.model = model
        if provider == "openai":
            import tiktoken
            try:
                self._enc = tiktoken.encoding_for_model(model)
            except KeyError:
                # Unknown model — fall back to o200k_base (GPT-4/GPT-4o tokenizer)
                self._logger.warning(
                    "Model %s not recognized by tiktoken, using o200k_base encoding",
                    model,
                )
                self._enc = tiktoken.get_encoding("o200k_base")
        elif provider == "gemini":
            from google import genai
            self._client = genai.Client()

    def count(self, text: str) -> int:
        """Return exact token count for *text*."""
        if not text:
            return 0
        if self.provider == "openai":
            return len(self._enc.encode(text))
        elif self.provider == "gemini":
            resp = self._client.models.count_tokens(
                model=self.model, contents=text,
            )
            return resp.total_tokens
        return 0


# ---------------------------------------------------------------------------
# MediaExtractor — inverse of GovMilExtractor
# ---------------------------------------------------------------------------

class MediaExtractor(Base):
    """Filter a cross-index article catalog to NON-government media articles.

    Inverse of ``GovMilExtractor`` — *excludes* ``.gov``, ``.mil``,
    ``dvids.net`` and keeps everything else.  Same dedup logic: when
    multiple index copies of the same URL exist, the copy with the
    longest ``summary_text`` wins.
    """

    def __init__(
        self,
        domain_exclude: list[str] | None = None,
    ) -> None:
        super().__init__()
        self.domain_exclude = domain_exclude or [
            ".gov", ".mil", "dvids.net", "dvidshub.net",
            ".gov.il", "dirco.gov.za",
        ]

    # -- public API ----------------------------------------------------------

    def extract(self, catalog: list[ArticleRecord]) -> list[ArticleRecord]:
        """Return deduplicated media articles (non-gov/mil) from *catalog*."""
        # 1. Filter — keep everything NOT matching excluded domains
        included: list[ArticleRecord] = []
        for article in catalog:
            hostname = article.domain
            if not hostname:
                continue
            if self._is_excluded(hostname):
                continue
            included.append(article)

        # 2. Deduplicate by normalised URL — longest summary wins
        seen: dict[str, ArticleRecord] = {}
        for article in included:
            key = _normalise_url(article.source_url)
            if not key:
                continue
            existing = seen.get(key)
            if existing is None or len(article.summary_text) > len(existing.summary_text):
                seen[key] = article

        result = list(seen.values())

        # 3. Log breakdown
        domain_counts: dict[str, int] = defaultdict(int)
        index_counts: dict[str, int] = defaultdict(int)
        for a in result:
            domain_counts[a.domain] += 1
            index_counts[a.index_folder] += 1

        self._logger.info(
            "Extracted %d unique media articles from %d total across %d indices",
            len(result), len(catalog), len(index_counts),
        )
        for domain, count in sorted(domain_counts.items(), key=lambda x: -x[1])[:20]:
            self._logger.info("  %s: %d", domain, count)

        return result

    # -- private helpers -----------------------------------------------------

    def _is_excluded(self, hostname: str) -> bool:
        """HC-8: strict suffix/exact matching on parsed hostname."""
        for pattern in self.domain_exclude:
            if pattern.startswith("."):
                # Suffix match: hostname ends with .gov, .mil, etc.
                if hostname.endswith(pattern) or hostname == pattern.lstrip("."):
                    return True
            else:
                # Exact match: hostname IS dvids.net or sub.dvids.net
                if hostname == pattern or hostname.endswith("." + pattern):
                    return True
        return False


# ---------------------------------------------------------------------------
# MediaLinkageCache — JSONL append-only cache for media-to-EO linkage
# ---------------------------------------------------------------------------

def _media_record_id(url: str, eo_number: str) -> str:
    """Deterministic record ID: sha256(url:eo_number) truncated to 16 hex."""
    key = f"{url}:{eo_number}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


class MediaLinkageCache(Base):
    """Append-only JSONL cache for media-to-EO linkage records.

    Mirrors ``EOImplementationCache`` — upsert dedup via ``record_id``.

    Each record pairs a media article with linked EOs, relevant events,
    and a linkage rationale.
    """

    def __init__(self, cache_path: str | Path) -> None:
        super().__init__()
        self.cache_path = Path(cache_path)
        self._records: dict[str, dict[str, Any]] = {}  # record_id → record
        if self.cache_path.exists():
            self._load()

    # -- public API ----------------------------------------------------------

    def append(self, records: list[dict[str, Any]]) -> int:
        """Upsert records into the cache. Returns count of new records added."""
        added = 0
        for rec in records:
            rid = rec.get("record_id", "")
            if not rid:
                continue
            if rid not in self._records:
                added += 1
            self._records[rid] = rec
        self._flush()
        return added

    def load(self) -> list[dict[str, Any]]:
        """Return all cached records."""
        return list(self._records.values())

    def by_eo(self, eo_number: str) -> list[dict[str, Any]]:
        """Filter records whose linked_eos list contains *eo_number*."""
        return [
            r for r in self._records.values()
            if any(
                eo.get("eo_number") == eo_number
                for eo in r.get("linked_eos", [])
            )
        ]

    def by_domain(self, domain: str) -> list[dict[str, Any]]:
        """Filter records by article_domain."""
        return [
            r for r in self._records.values()
            if r.get("article_domain") == domain
        ]

    def summary(self) -> dict[str, Any]:
        """Return summary statistics."""
        all_recs = list(self._records.values())
        eo_set: set[str] = set()
        for r in all_recs:
            for eo in r.get("linked_eos", []):
                if isinstance(eo, dict):
                    eo_num = eo.get("eo_number")
                    if eo_num:
                        eo_set.add(eo_num)
                elif isinstance(eo, str):
                    # Legacy format: plain string
                    eo_set.add(eo)
        domains = {r.get("article_domain", "") for r in all_recs}
        domains.discard("")
        return {
            "total_records": len(all_recs),
            "unique_eos": len(eo_set),
            "unique_domains": len(domains),
            "eo_numbers": sorted(eo_set),
        }

    # -- private helpers -----------------------------------------------------

    def _load(self) -> None:
        """Load existing cache from JSONL."""
        count = 0
        with open(self.cache_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    rid = rec.get("record_id", "")
                    if rid:
                        self._records[rid] = rec
                        count += 1
                except json.JSONDecodeError:
                    continue
        self._logger.info("Loaded %d records from cache %s", count, self.cache_path.name)

    def _flush(self) -> None:
        """Rewrite the full cache (upsert semantics — dedup on write)."""
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as fh:
            for rec in self._records.values():
                fh.write(json.dumps(rec, default=str) + "\n")
        self._logger.info(
            "Wrote %d records to cache %s", len(self._records), self.cache_path.name,
        )

# ---------------------------------------------------------------------------
# Phase 3 — MEDIA LINK-REFLECT Prompts
# ---------------------------------------------------------------------------

_MEDIA_LINK_PROMPT = """\
You are a linkage analyst. Your ONLY task is to connect each media \
article to the relevant Executive Orders (EOs) and verified EO \
implementation events from the grounding provided below.

Do NOT perform any media analysis. Do NOT assess spin, factualness, \
convergence/divergence, agenda, or bias. Only produce linkages.

**SOURCE TRACEABILITY — CRITICAL REQUIREMENT**
Every media article listed below has a "URL:" line. That URL is the \
canonical source identifier. You MUST copy its exact URL verbatim \
into the output. Do NOT omit, shorten, or invent URLs.

For each media article:
1. Identify which EO(s) it covers (use 5-digit EO numbers and official \
   titles from the framework).
2. Identify which verified EO implementation events it relates to.
3. Write a brief linkage rationale explaining why the article maps to \
   those EOs/events.

**CRITICAL: Respond with a single JSON object. Do NOT use markdown. \
Output raw JSON only.**

Required schema:

    {{
      "article_url": "<exact_article_url>",
      "article_title": "<title>",
      "article_date": "<YYYY-MM-DD or empty string>",
      "article_domain": "<domain>",
      "publisher": "<publisher name or empty string>",
      "author": "<author name or empty string>",
      "linked_eos": [
        {{"eo_number": "<5-digit EO number>", "eo_name": "<official EO title>"}}
      ],
      "relevant_events": [
        {{
          "eo_number": "<5-digit>",
          "event_date": "<YYYY-MM-DD>",
          "event_source_url": "<.gov/.mil URL if available>",
          "description": "<brief event description>"
        }}
      ],
      "linkage_rationale": "<why this article maps to the identified EOs/events>",
      "confidence": <0.0-1.0>
    }}


Rules:
- Each article MUST be a separate object in the array with its own "article_url" field.
- Copy article_date, publisher, author from the source metadata where available.
- "linked_eos": array of objects with "eo_number" (5-digit string) and "eo_name" \
  (official title from the EO framework). Empty array if no EO link found.
- "relevant_events": array of matching events from the EO IMPLEMENTATION EVENTS \
  section. Empty array if none identified.
- Do NOT add spin, factualness, convergence, divergence, agenda, or any \
  media-analysis fields.

--- EO LEGAL FRAMEWORK ---

{eo_grounding}

--- EO IMPLEMENTATION EVENTS (verified .gov/.mil linkages) ---

{eo_linkage_grounding}

--- MEDIA ARTICLES ---

{batch_conversations}
"""

_MEDIA_REFLECT_PROMPT = """\
You are a linkage validator. Your ONLY task is to review the initial \
media-to-EO linkages for completeness and accuracy. You must NOT \
perform any media analysis — no spin, factualness, convergence, \
divergence, agenda, or bias assessment.

For each linkage, verify:
- Are the correct EO(s) identified? Check against the EO framework.
- Are the relevant implementation events accurate? Cross-check dates, \
  descriptions, and source URLs against the EO IMPLEMENTATION EVENTS.
- Is the linkage rationale sound?
- Are there any MISSING EO or event linkages the initial pass overlooked?
- Is the metadata (title, date, domain, publisher, author) correct?
- Are EO events contextualised by date, and event, and person/dept?
- Are you taking into account temporal precedence:
* does the media occur before or after the EO event?
* does the media occur before or after the EO signing date?
* is it related to an EO as a precusors or consequence?
* is it unrelated to an EO event but sets context?

If you find errors, correct them. If you find missing linkages, add them. \
Output the complete validated set.

**SOURCE TRACEABILITY — MANDATORY**
Every article has a "URL:" line. You MUST copy the exact URL from the \
matching source article. Do NOT omit, shorten, or fabricate URLs.

**CRITICAL: Respond with a single JSON object. Do NOT use markdown. \
Output raw JSON only.**

Required schema:

    {{
      "article_url": "<exact_article_url>",
      "article_title": "<article title>",
      "article_date": "<YYYY-MM-DD>",
      "article_domain": "<domain>",
      "publisher": "<publisher name>",
      "author": "<author name or empty string>",
      "linked_eos": [
        {{"eo_number": "<5-digit EO number>", "eo_name": "<official EO title>"}}
      ],
      "relevant_events": [
        {{
          "eo_number": "<5-digit>",
          "event_date": "<YYYY-MM-DD>",
          "event_source_url": "<.gov/.mil URL if available>",
          "description": "<brief event description>"
        }}
      ],
      "linkage_rationale": "<why this article maps to the identified EOs/events>",
      "confidence": <0.0-1.0>
    }}

Rules:
- Each article MUST be a separate object in the array with its own "article_url" field.
- Every entry MUST have ALL keys shown above.
- "linked_eos": array of objects with "eo_number" and "eo_name". Never empty if a link exists.
- "relevant_events": array of verified events. Empty array if none.
- Do NOT add spin, factualness, convergence, divergence, agenda, or any \
  media-analysis fields.
- "confidence": float between 0.0 and 1.0.

--- EO LEGAL FRAMEWORK ---

{eo_grounding}

--- EO IMPLEMENTATION EVENTS (verified .gov/.mil linkages) ---

{eo_linkage_grounding}

--- INITIAL LINKAGE ANALYSIS ---

{link_output}

--- MEDIA ARTICLES ---

{batch_conversations}
"""


# ---------------------------------------------------------------------------
# Phase 3 — MediaLinkReflectDriver
# ---------------------------------------------------------------------------

class MediaLinkReflectDriver(Base):
    """Two-call LLM synthesis per batch: LINK then REFLECT for media articles.

    Mirrors ``LinkReflectDriver`` but uses ``_MEDIA_LINK_PROMPT`` /
    ``_MEDIA_REFLECT_PROMPT`` and injects EO linkage grounding from the
    prior EO linking run.
    """

    def __init__(
        self,
        model_name: str = "gpt-5.4",
        location: str = "global",
        output_dir: str | Path = ".",
        timeout: int = 900,
        provider: str = "openai",
        reasoning_effort: str = "high",
        token_counter: TokenCounter | None = None,
    ) -> None:
        super().__init__()
        self.model_name = model_name
        self.location = location
        self.output_dir = Path(output_dir)
        self.timeout = timeout
        self.provider = provider
        self.reasoning_effort = reasoning_effort
        self._client = None  # lazy init
        self._tc = token_counter
        self.total_usage: dict[str, int] = {
            "input": 0, "output": 0, "thinking": 0,
        }
        self.estimated_usage: dict[str, int] = {
            "input": 0, "output": 0, "thinking": 0,
        }

    def _get_client(self):
        """Lazy-init the LLM client (OpenAI or Gemini)."""
        if self._client is None:
            if self.provider == "openai":
                import os

                from openai import OpenAI
                self._logger.info(
                    "[CLIENT INIT] Initializing OpenAI client (no caching wrapper)"
                )
                self._client = OpenAI(
                    api_key=os.environ["OPENAI_API_KEY"],
                    timeout=self.timeout,
                )
            else:
                from src._clients.llm_gcp_client import GeminiLLMClient
                from src.clients.llm_cache_client import CachedLLMClient
                from src.config import app_conf
                self._logger.info(
                    "[CLIENT INIT] Initializing Gemini client with Redis caching wrapper"
                )
                _inner = GeminiLLMClient(
                    model_name=self.model_name,
                    location=self.location,
                )
                redis_host = app_conf.get("redis.host") or ""
                redis_port = int(app_conf.get("redis.port") or 6379)
                ttl = int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000)
                self._logger.info(
                    "[CLIENT INIT] Redis config: host=%s, port=%d, ttl=%d seconds",
                    redis_host, redis_port, ttl,
                )
                self._client = CachedLLMClient(
                    _inner,
                    redis_host=redis_host,
                    redis_port=redis_port,
                    redis_password=app_conf.get("redis.password") or None,
                    ttl_seconds=ttl,
                )
        return self._client

    # -- public API ----------------------------------------------------------

    def process_batch(
        self,
        batch_index: int,
        batch_conversations: str,
        eo_grounding: str = "",
        eo_linkage_grounding: str = "",
    ) -> dict[str, Any]:
        """Run LINK + REFLECT for a single media batch.

        Saves artefacts to batch_{b}/link.json, reflect.json, status.json.
        Skips if status.json already shows "complete" (checkpointing).
        """

        batch_dir = self.output_dir / f"batch_{batch_index}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        status_path = batch_dir / "status.json"

        # -- Checkpoint: skip completed batches -----------------------------
        if status_path.exists():
            try:
                existing = json.loads(status_path.read_text(encoding="utf-8"))
                if existing.get("status") == "complete":
                    self._logger.info(
                        "Batch %d already complete — skipping.", batch_index,
                    )
                    return existing
            except (json.JSONDecodeError, KeyError):
                pass  # corrupt status file — re-run

        result: dict[str, Any] = {
            "batch": batch_index,
            "status": "error",
            "link_tokens": {},
            "reflect_tokens": {},
            "error": None,
        }

        link_input_tokens = 0
        reflect_input_tokens = 0
        pricing = _RAG_PRICING.get(
            self.model_name, {"input": 0, "output": 0},
        )

        try:
            # -- Call 1: LINK -----------------------------------------------
            self._logger.info("Batch %d — MEDIA LINK call...", batch_index)
            link_prompt = _MEDIA_LINK_PROMPT.format(
                eo_grounding=eo_grounding,
                eo_linkage_grounding=eo_linkage_grounding,
                batch_conversations=batch_conversations,
            )

            # Pre-call token count & cost estimate
            if self._tc:
                link_input_tokens = self._tc.count(link_prompt)
                link_est_cost = (link_input_tokens / 1_000_000) * pricing["input"]
                self._logger.info(
                    "Batch %d — LINK pre-call: %d tokens, est $%.4f input",
                    batch_index, link_input_tokens, link_est_cost,
                )
            else:
                self._logger.info(
                    "Batch %d — LINK prompt: %d chars",
                    batch_index, len(link_prompt),
                )

            link_text, link_usage, link_reasoning = self._generate(
                link_prompt, response_mime_type="application/json",
            )
            result["link_tokens"] = link_usage
            if link_reasoning:
                result["link_reasoning"] = link_reasoning

            link_path = batch_dir / "link.json"
            link_path.write_text(link_text, encoding="utf-8")

            # Save reasoning content alongside the link result
            if link_reasoning:
                reasoning_path = batch_dir / "link_reasoning.md"
                reasoning_path.write_text(
                    "\n\n---\n\n".join(link_reasoning), encoding="utf-8",
                )

            # Post-call: log actual and delta if pre-call was measured
            if self._tc:
                actual_in = link_usage.get("input", 0)
                delta = actual_in - link_input_tokens
                self._logger.info(
                    "Batch %d — LINK done: %d chars, pre %d / actual %d tokens (delta %+d), %d output tokens",
                    batch_index, len(link_text),
                    link_input_tokens, actual_in, delta,
                    link_usage.get("output", 0),
                )
            else:
                self._logger.info(
                    "Batch %d — LINK done: %d chars, %d input / %d output tokens",
                    batch_index, len(link_text),
                    link_usage.get("input", 0), link_usage.get("output", 0),
                )

            # -- Call 2: REFLECT --------------------------------------------
            self._logger.info("Batch %d — MEDIA REFLECT call...", batch_index)
            reflect_prompt = _MEDIA_REFLECT_PROMPT.format(
                eo_grounding=eo_grounding,
                eo_linkage_grounding=eo_linkage_grounding,
                link_output=link_text,
                batch_conversations=batch_conversations,
            )

            # Pre-call token count & cost estimate
            if self._tc:
                reflect_input_tokens = self._tc.count(reflect_prompt)
                reflect_est_cost = (reflect_input_tokens / 1_000_000) * pricing["input"]
                self._logger.info(
                    "Batch %d — REFLECT pre-call: %d tokens, est $%.4f input",
                    batch_index, reflect_input_tokens, reflect_est_cost,
                )
            else:
                self._logger.info(
                    "Batch %d — REFLECT prompt: %d chars",
                    batch_index, len(reflect_prompt),
                )

            reflect_text, reflect_usage, reflect_reasoning = self._generate(
                reflect_prompt, response_mime_type="application/json",
            )
            result["reflect_tokens"] = reflect_usage
            if reflect_reasoning:
                result["reflect_reasoning"] = reflect_reasoning

            reflect_path = batch_dir / "reflect.json"
            reflect_path.write_text(reflect_text, encoding="utf-8")

            # Save reasoning content alongside the reflect result
            if reflect_reasoning:
                reasoning_path = batch_dir / "reflect_reasoning.md"
                reasoning_path.write_text(
                    "\n\n---\n\n".join(reflect_reasoning), encoding="utf-8",
                )

            # Post-call: log actual and delta if pre-call was measured
            if self._tc:
                actual_in = reflect_usage.get("input", 0)
                delta = actual_in - reflect_input_tokens
                self._logger.info(
                    "Batch %d — REFLECT done: %d chars, pre %d / actual %d tokens (delta %+d), %d output tokens",
                    batch_index, len(reflect_text),
                    reflect_input_tokens, actual_in, delta,
                    reflect_usage.get("output", 0),
                )
            else:
                self._logger.info(
                    "Batch %d — REFLECT done: %d chars, %d input / %d output tokens",
                    batch_index, len(reflect_text),
                    reflect_usage.get("input", 0), reflect_usage.get("output", 0),
                )

            result["status"] = "complete"
            result["link_chars"] = len(link_text)
            result["reflect_chars"] = len(reflect_text)

        except Exception as exc:
            self._logger.error("Batch %d — LLM error: %s", batch_index, exc)
            result["error"] = str(exc)
            # Track estimated tokens even when calls fail
            if link_input_tokens > 0:
                self.estimated_usage["input"] += link_input_tokens
            if reflect_input_tokens > 0:
                self.estimated_usage["input"] += reflect_input_tokens

        # -- Write status checkpoint ----------------------------------------
        status_path.write_text(
            json.dumps(result, indent=2, default=str), encoding="utf-8",
        )
        return result

    # -- private helpers -----------------------------------------------------

    def _generate(
        self,
        prompt: str,
        response_mime_type: str | None = None,
    ) -> tuple[str, dict[str, int], list[str]]:
        """Call LLM (OpenAI or Gemini) and return (text, usage_dict, reasoning_content)."""
        import time
        usage: dict[str, int] = {"input": 0, "output": 0, "thinking": 0}
        reasoning_content: list[str] = []

        if self.provider == "openai":
            client = self._get_client()
            kwargs: dict[str, Any] = {
                "model": self.model_name,
                "input": prompt,
                "reasoning": {"effort": self.reasoning_effort},
                "max_output_tokens": 96_000,
            }
            if response_mime_type == "application/json":
                kwargs["text"] = {"format": {"type": "json_object"}}

            # Pre-call logging
            prompt_len = len(prompt)
            self._logger.info(
                "[API CALL START] provider=%s, model=%s, prompt_size=%d chars, "
                "max_output_tokens=%d, response_type=%s, reasoning_effort=%s",
                self.provider, self.model_name, prompt_len,
                kwargs["max_output_tokens"], response_mime_type or "text",
                self.reasoning_effort,
            )
            start_time = time.time()

            # Actual API call
            self._logger.info("[API CALL] Calling client.responses.create()...")
            response = client.responses.create(**kwargs)
            elapsed = time.time() - start_time

            self._logger.info(
                "[API CALL END] Received response in %.2f seconds",
                elapsed,
            )
            elapsed = time.time() - start_time

            self._logger.info(
                "[API CALL END] Received response in %.2f seconds",
                elapsed,
            )

            # Parse response
            self._logger.info("[RESPONSE PARSE] Extracting output_text...")
            text = (
                response.output_text if hasattr(response, "output_text") else ""
            )
            self._logger.info(
                "[RESPONSE PARSE] Extracted %d chars of text",
                len(text),
            )

            # Parse usage
            self._logger.info("[RESPONSE PARSE] Extracting usage metadata...")
            resp_usage = getattr(response, "usage", None)
            if resp_usage:
                usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
                usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = (
                        getattr(details, "reasoning_tokens", 0) or 0
                    )
                self._logger.info(
                    "[RESPONSE PARSE] Usage: input=%d, output=%d, thinking=%d",
                    usage["input"], usage["output"], usage["thinking"],
                )

            # Extract reasoning content from response output items
            for item in getattr(response, "output", []):
                if getattr(item, "type", None) == "reasoning":
                    for summary in getattr(item, "summary", []):
                        text_part = getattr(summary, "text", "")
                        if text_part:
                            reasoning_content.append(text_part)
            if reasoning_content:
                self._logger.info(
                    "[RESPONSE PARSE] Extracted %d reasoning blocks",
                    len(reasoning_content),
                )
        else:
            from google.genai import types as genai_types
            client = self._get_client()
            config_params: dict[str, Any] = {
                "temperature": 0.2,
                "top_p": client.top_p,
                "max_output_tokens": 96_000,
            }
            if response_mime_type:
                config_params["response_mime_type"] = response_mime_type

            # Check if wrapped in cache client
            is_cached = "CachedLLMClient" in str(type(client))

            # Pre-call logging
            prompt_len = len(prompt)
            self._logger.info(
                "[API CALL START] provider=%s, model=%s, prompt_size=%d chars, "
                "max_output_tokens=%d, response_type=%s, cached_client=%s",
                self.provider, self.model_name, prompt_len,
                config_params["max_output_tokens"], response_mime_type or "text",
                is_cached,
            )
            start_time = time.time()

            config = genai_types.GenerateContentConfig(**config_params)

            # Actual API call
            self._logger.info("[API CALL] Calling client.client.models.generate_content()...")
            response = client.client.models.generate_content(
                model=client.model_name,
                contents=prompt,
                config=config,
            )
            elapsed = time.time() - start_time

            self._logger.info(
                "[API CALL END] Received response in %.2f seconds",
                elapsed,
            )

            # Parse response
            self._logger.info("[RESPONSE PARSE] Extracting text...")
            text = response.text if response.text else ""
            self._logger.info(
                "[RESPONSE PARSE] Extracted %d chars of text",
                len(text),
            )

            # Parse usage
            self._logger.info("[RESPONSE PARSE] Extracting usage metadata...")
            um = getattr(response, "usage_metadata", None)
            if um:
                usage["input"] = getattr(um, "prompt_token_count", 0) or 0
                usage["output"] = (
                    getattr(um, "candidates_token_count", 0) or 0
                )
                usage["thinking"] = (
                    getattr(um, "thoughts_token_count", 0) or 0
                )
                self._logger.info(
                    "[RESPONSE PARSE] Usage: input=%d, output=%d, thinking=%d",
                    usage["input"], usage["output"], usage["thinking"],
                )

        # Accumulate totals
        self._logger.info("[COMPLETE] Accumulating usage totals...")
        for key in ("input", "output", "thinking"):
            self.total_usage[key] += usage[key]

        return text, usage, reasoning_content

    def format_conversations_for_prompt(
        self,
        conv_path: Path,
    ) -> str:
        """Load a batch conversation.json and format for prompt injection."""
        if not conv_path.exists():
            return ""
        conversations = json.loads(conv_path.read_text(encoding="utf-8"))
        parts: list[str] = []
        for entry in conversations:
            meta = entry.get("metadata", {})
            response = entry.get("assistant_response", "")
            if len(response) > 2000:
                response = response[:2000] + " [...]"

            article_url = meta.get("article_url", "")
            block = (
                f"### {meta.get('article_title', 'Untitled')}\n"
                f"URL: {article_url}\n"
                f"Domain: {meta.get('domain', '')}\n"
                f"Publisher: {meta.get('publisher', '')}\n"
                f"Date: {meta.get('date', '')}\n\n"
                f"{response}\n"
            )

            parts.append(block)
        return "\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Phase 4 — MediaLinkageExtractor
# ---------------------------------------------------------------------------

# Regex for 5-digit EO numbers
_EO_NUM_RE = re.compile(r"(\d{5})")


class MediaLinkageExtractor(Base):
    """Parse REFLECT JSON output into flat media linkage records.

    Handles the ``media_linkages`` array schema (each article is a
    separate object with ``article_url``).  Falls back to legacy
    dict-keyed-by-URL format and common wrapper keys.
    """

    def extract(
        self,
        reflect_text: str,
        batch_id: str,
        article_lookup: dict[str, dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract media linkage records from REFLECT output.

        Returns a list of flat dicts (one per article, with linked_eos list).
        """
        # -- Parse JSON from REFLECT output ---------------------------------
        json_blocks = re.findall(r"```json\s*([\s\S]*?)```", reflect_text)
        if not json_blocks:
            cleaned = _clean_json_markdown(reflect_text)
            json_start = cleaned.find("{")
            if json_start < 0:
                self._logger.warning(
                    "No JSON found in REFLECT output for %s", batch_id,
                )
                return []
            # Find matching close brace
            depth = 0
            end = len(cleaned)
            for i in range(json_start, len(cleaned)):
                if cleaned[i] == "{":
                    depth += 1
                elif cleaned[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            json_blocks = [cleaned[json_start:end]]

        records: list[dict[str, Any]] = []

        for block in json_blocks:
            block = block.strip()
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                continue

            if not isinstance(data, dict):
                continue

            # Find the media linkages — try known wrapper keys
            linkages_list: list[dict[str, Any]] = []
            for key in (
                "media_linkages",
                "media_links",
                "linkages",
                "articles",
            ):
                val = data.get(key)
                if isinstance(val, list):
                    linkages_list = val
                    break
                # Legacy dict-keyed-by-URL format
                if isinstance(val, dict):
                    for url, entry in val.items():
                        if isinstance(entry, dict):
                            entry.setdefault("article_url", url)
                            linkages_list.append(entry)
                    break

            # Fallback: top-level URL-like keys or dicts with linked_eos
            if not linkages_list:
                for k, v in data.items():
                    if isinstance(v, dict) and (
                        k.startswith("http") or "linked_eos" in v
                    ):
                        v.setdefault("article_url", k)
                        linkages_list.append(v)

            if not linkages_list:
                top_keys = list(data.keys())[:5]
                self._logger.warning(
                    "No media linkages found in JSON block for %s — "
                    "unexpected schema: %s",
                    batch_id, top_keys,
                )
                continue

            for entry in linkages_list:
                if not isinstance(entry, dict):
                    continue
                article_url = entry.get("article_url", "")
                record = self._build_record(
                    article_url, entry, batch_id, article_lookup,
                )
                if record:
                    records.append(record)

        self._logger.info(
            "Extracted %d media linkage records from %s", len(records), batch_id,
        )
        return records

    # -- helpers -------------------------------------------------------------

    def _build_record(
        self,
        article_url: str,
        entry: dict[str, Any],
        batch_id: str,
        article_lookup: dict[str, dict[str, str]] | None,
    ) -> dict[str, Any] | None:
        """Build a flat media linkage record from a REFLECT entry."""
        # Extract linked EOs — normalise to list of {eo_number, eo_name}
        raw_eos = entry.get("linked_eos", [])
        linked_eos: list[dict[str, str]] = []
        seen_nums: set[str] = set()
        for eo in raw_eos:
            if isinstance(eo, dict):
                # New format: {"eo_number": "...", "eo_name": "..."}
                for m in _EO_NUM_RE.finditer(str(eo.get("eo_number", ""))):
                    num = m.group(1)
                    if num not in seen_nums:
                        seen_nums.add(num)
                        linked_eos.append({
                            "eo_number": num,
                            "eo_name": eo.get("eo_name", ""),
                        })
            else:
                # Legacy format: plain string
                for m in _EO_NUM_RE.finditer(str(eo)):
                    num = m.group(1)
                    if num not in seen_nums:
                        seen_nums.add(num)
                        linked_eos.append({"eo_number": num, "eo_name": ""})

        if not linked_eos:
            return None

        # Hydrate missing fields from article_lookup
        title = entry.get("article_title", "")
        date = entry.get("article_date", "")
        domain = entry.get("article_domain", "")
        publisher = entry.get("publisher", "")
        author = entry.get("author", "")

        if article_lookup:
            meta = article_lookup.get(article_url, {})
            if not meta and title:
                meta = article_lookup.get(title, {})
            if meta:
                if not title:
                    title = meta.get("title", "")
                if not date:
                    date = meta.get("date", "")
                if not domain:
                    domain = meta.get("domain", "")
                if not publisher:
                    publisher = meta.get("publisher", "")

        if not domain and article_url:
            domain = self._extract_domain(article_url)

        # Build record ID: sha256(url:first_eo)
        first_eo = linked_eos[0]["eo_number"] if linked_eos else ""
        record_id = _media_record_id(article_url or title, first_eo)

        return {
            "record_id": record_id,
            "article_url": article_url,
            "article_title": title,
            "article_date": date,
            "article_domain": domain,
            "publisher": publisher,
            "author": author,
            "linked_eos": linked_eos,
            "relevant_events": entry.get("relevant_events", []),
            "linkage_rationale": entry.get("linkage_rationale", ""),
            "confidence": entry.get("confidence", 0.0),
            "batch_id": batch_id,
        }

    @staticmethod
    def _extract_domain(url: str) -> str:
        if not url:
            return ""
        try:
            return urlparse(url).hostname or ""
        except Exception:
            return ""


# ---------------------------------------------------------------------------
# EO Cache Grounding Formatter
# ---------------------------------------------------------------------------

def _format_eo_cache_as_grounding(eo_cache_path: str | Path) -> str:
    """Format EO implementation cache as grounding for media LINK prompt.

    Groups by EO number, lists verified .gov/.mil communications with
    action descriptions, dates, officials — the factual baseline the
    LLM uses to assess media articles.
    """
    path = Path(eo_cache_path)
    if not path.exists():
        return ""

    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not records:
        return ""

    by_eo: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        eo_num = r.get("eo_number", "")
        if eo_num:
            by_eo[eo_num].append(r)

    parts: list[str] = [
        "## EO Implementation Events (verified .gov/.mil linkages)\n",
    ]
    for eo_num in sorted(by_eo):
        recs = by_eo[eo_num]
        title = recs[0].get("eo_title", "")
        parts.append(f"\n### EO {eo_num}: {title}\n")
        for r in recs:
            comm = r.get("communication")
            if not comm or not isinstance(comm, dict):
                continue
            parts.append(
                f"- [{comm.get('date', '')}] "
                f"{comm.get('action_description', '')}\n"
                f"  Official: {comm.get('official', '')}\n"
                f"  Source: {comm.get('url', '')}\n"
            )
    return "".join(parts)
