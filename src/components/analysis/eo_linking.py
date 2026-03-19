"""EO Event Linking — Phase 1, 2, 3 & 4 components.

Provides:
- ``GovMilExtractor``: filters cross-index article catalog to .gov/.mil domains
- ``EORegistry``: loads EO book chapters, events, and supercession graph
- ``TokenBudgetBatcher``: groups articles into batches by index folder
- ``EOChatDriver``: queries RAG /query-llm per article with EO enrichment
- ``ConversationBuilder``: assembles conversation JSON per batch
- ``LinkReflectDriver``: two-call LINK-REFLECT LLM synthesis per batch
- ``LinkageExtractor``: parses REFLECT JSON into structured linkage records
- ``EOImplementationCache``: append-only JSONL cache with upsert dedup
- ``EODiagramGenerator``: Mermaid LR tree diagrams for EO precedence chains
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from src.config_handler import Base

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Primary EO number from chapter H1 headings
EO_HEADING_RE = re.compile(
    r"Exec\.?\s+Order\s+No\.?\s*(\d{5})\s*\((\d{4})\)", re.IGNORECASE
)

# Supercession / revocation detection in chapter body text.
# Limit the gap between verb and EO ref to 120 chars to avoid
# matching long sentences where a different EO is the subject.
SUPERCESSION_RE = re.compile(
    r"(?:rescind|revok|supersed|replac)\w*\s+.{0,120}?"
    r"(?:Exec(?:utive)?\.?\s+Order\s+(?:No\.?\s+)?)([\d,]+)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Article record (lightweight dict wrapper)
# ---------------------------------------------------------------------------

class ArticleRecord:
    """Thin wrapper over a JSONL summary record with index provenance."""

    __slots__ = ("document_id", "source_url", "summary_text",
                 "doc_metadata", "index_folder", "raw")

    def __init__(self, raw: dict[str, Any], index_folder: str) -> None:
        self.raw = raw
        self.document_id: str = raw.get("document_id", "")
        self.source_url: str = raw.get("source_url", "")
        self.summary_text: str = raw.get("summary_text", "")
        self.doc_metadata: dict[str, Any] = raw.get("doc_metadata") or {}
        self.index_folder: str = index_folder

    @property
    def title(self) -> str:
        return self.doc_metadata.get("title", "")

    @property
    def domain(self) -> str:
        """Extract hostname from source_url (HC-8 safe)."""
        try:
            return urlparse(self.source_url).hostname or ""
        except Exception:
            return ""


# ---------------------------------------------------------------------------
# URL normalisation
# ---------------------------------------------------------------------------

def _normalise_url(url: str) -> str:
    """Stable key from a URL — strips scheme, fragment, query, trailing slash."""
    if not url:
        return ""
    normalised = re.sub(r"^https?://", "", url.strip().lower())
    normalised = normalised.split("#")[0]
    normalised = normalised.split("?")[0]
    return normalised.rstrip("/")


# ---------------------------------------------------------------------------
# GovMilExtractor
# ---------------------------------------------------------------------------

class GovMilExtractor(Base):
    """Filter a cross-index article catalog to .gov/.mil domains.

    Uses ``urllib.parse`` for hostname extraction (HC-8) to avoid false
    partial matches from substring-based domain checks.
    """

    def __init__(
        self,
        domain_include: list[str] | None = None,
        domain_exclude: list[str] | None = None,
    ) -> None:
        super().__init__()
        self.domain_include = domain_include or [".gov", ".mil", ".gov.il", "dirco.gov.za"]
        self.domain_exclude = domain_exclude or []

    # -- public API ----------------------------------------------------------

    def extract(self, catalog: list[ArticleRecord]) -> list[ArticleRecord]:
        """Return deduplicated .gov/.mil articles from the catalog."""
        # 1. Filter by domain
        included: list[ArticleRecord] = []
        for article in catalog:
            hostname = article.domain
            if not hostname:
                continue
            if self._is_excluded(hostname):
                continue
            if self._is_included(hostname):
                included.append(article)

        # 2. Deduplicate by normalised URL — keep the copy with the longest
        #    summary (proxy for "most enriched").
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
            "Extracted %d unique .gov/.mil articles from %d total across %d indices",
            len(result), len(catalog), len(index_counts),
        )
        for domain, count in sorted(domain_counts.items(), key=lambda x: -x[1]):
            self._logger.info("  %s: %d", domain, count)

        return result

    # -- private helpers -----------------------------------------------------

    def _is_included(self, hostname: str) -> bool:
        """HC-8: strict suffix/exact matching on parsed hostname."""
        for pattern in self.domain_include:
            if pattern.startswith("."):
                # Suffix match: hostname must end with this suffix
                if hostname.endswith(pattern) or hostname == pattern.lstrip("."):
                    return True
            else:
                # Exact match
                if hostname == pattern or hostname.endswith("." + pattern):
                    return True
        return False

    def _is_excluded(self, hostname: str) -> bool:
        for pattern in self.domain_exclude:
            if pattern.startswith("."):
                if hostname.endswith(pattern) or hostname == pattern.lstrip("."):
                    return True
            else:
                if hostname == pattern or hostname.endswith("." + pattern):
                    return True
        return False


# ---------------------------------------------------------------------------
# Index loader — load JSONL summaries from index folders
# ---------------------------------------------------------------------------

class IndexLoader(Base):
    """Load research summaries from multiple index folders into a flat catalog."""

    def __init__(self) -> None:
        super().__init__()

    def load(
        self,
        index_folders: list[str],
        base_dir: str | Path = ".",
    ) -> list[ArticleRecord]:
        """Load research summaries from multiple index folders into a flat catalog."""
        base = Path(base_dir)
        catalog: list[ArticleRecord] = []

        for folder_name in index_folders:
            folder = base / folder_name
            if not folder.is_dir():
                self._logger.warning("Index folder not found: %s", folder)
                continue

            # Find summary JSONL files
            summary_files = sorted(folder.glob("research_summaries*.jsonl"))
            if not summary_files:
                self._logger.warning("No summary JSONL files in %s", folder)
                continue

            for summary_file in summary_files:
                count = 0
                with open(summary_file, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        catalog.append(ArticleRecord(record, folder_name))
                        count += 1
                self._logger.info("Loaded %d records from %s", count, summary_file.name)

        self._logger.info("Total catalog: %d records from %d index folders", len(catalog), len(index_folders))
        return catalog


def load_index_catalog(
    index_folders: list[str],
    base_dir: str | Path = ".",
) -> list[ArticleRecord]:
    """Convenience wrapper — instantiates IndexLoader and loads catalog."""
    return IndexLoader().load(index_folders, base_dir)


# ---------------------------------------------------------------------------
# EORegistry
# ---------------------------------------------------------------------------

class EOEntry:
    """A single EO in the registry."""

    __slots__ = ("eo_number", "title", "date", "chapter_file",
                 "chapter_index", "events", "summaries")

    def __init__(
        self,
        eo_number: str,
        title: str = "",
        date: str = "",
        chapter_file: str = "",
        chapter_index: int = -1,
    ) -> None:
        self.eo_number = eo_number
        self.title = title
        self.date = date
        self.chapter_file = chapter_file
        self.chapter_index = chapter_index
        self.events: list[dict[str, Any]] = []
        self.summaries: list[dict[str, Any]] = []


class EORegistry(Base):
    """Registry of EOs parsed from the book chapters + events JSONL.

    Provides:
    - ``entries``: mapping of EO number → ``EOEntry``
    - ``supercessions``: mapping of EO number → list of EO numbers it rescinds
    - ``chapter_texts``: ordered list of (filename, content) for grounding
    """

    def __init__(self) -> None:
        super().__init__()
        self.entries: dict[str, EOEntry] = {}
        self.supercessions: dict[str, list[str]] = defaultdict(list)
        self.chapter_texts: list[tuple[str, str]] = []

    @classmethod
    def load(
        cls,
        eo_book_dir: str | Path,
        eo_index_folder: str | Path,
    ) -> EORegistry:
        """Build the registry from chapter files and event/summary JSONLs."""
        registry = cls()
        book_dir = Path(eo_book_dir)
        index_dir = Path(eo_index_folder)

        # 1. Load chapter files (including intro + conclusion)
        registry._load_chapters(book_dir)

        # 2. Load EO events
        registry._load_events(index_dir)

        # 3. Load EO summaries
        registry._load_summaries(index_dir)

        # Stats
        eos_with_chapters = sum(1 for e in registry.entries.values() if e.chapter_file)
        eos_with_events = sum(1 for e in registry.entries.values() if e.events)
        total_supercessions = sum(len(v) for v in registry.supercessions.values())
        registry._logger.info(
            "EO registry: %d EOs with chapters, %d EOs with events, %d supercession links",
            eos_with_chapters, eos_with_events, total_supercessions,
        )
        return registry

    @property
    def full_grounding_text(self) -> str:
        """Concatenate all chapter texts for LLM grounding."""
        return "\n\n---\n\n".join(
            f"## {filename}\n\n{content}" for filename, content in self.chapter_texts
        )

    # -- private loaders -----------------------------------------------------

    def _load_chapters(self, book_dir: Path) -> None:
        """Parse chapter .md files for EO headings and supercessions."""
        if not book_dir.is_dir():
            self._logger.warning("EO book directory not found: %s", book_dir)
            return

        md_files = sorted(book_dir.glob("*.md"))
        for idx, md_file in enumerate(md_files):
            # Skip non-chapter files like index.md, manifest.json
            if md_file.name in ("index.md", "manifest.json"):
                continue

            content = md_file.read_text(encoding="utf-8")
            self.chapter_texts.append((md_file.name, content))

            # Parse EO heading
            match = EO_HEADING_RE.search(content)
            if match:
                eo_number = match.group(1)
                eo_year = match.group(2)
                # Extract title from the first H1 line
                h1_match = re.search(r"^#\s+(.+)$", content, re.MULTILINE)
                title = h1_match.group(1).strip() if h1_match else ""

                # Only use this title if the H1 actually contains the EO number
                # (avoids introduction/conclusion chapters bleeding their H1 into
                # an EO entry that is merely mentioned in the body text).
                h1_has_eo = bool(
                    h1_match and re.search(rf"\b{eo_number}\b", h1_match.group(1))
                )

                entry = self.entries.get(eo_number)
                if entry is None:
                    entry = EOEntry(eo_number, title=title, date=eo_year,
                                    chapter_file=md_file.name, chapter_index=idx)
                    self.entries[eo_number] = entry
                else:
                    # Prefer the chapter whose H1 names the EO (the dedicated chapter)
                    if h1_has_eo:
                        entry.title = title
                        entry.chapter_file = md_file.name
                        entry.chapter_index = idx
                    elif not entry.chapter_file:
                        entry.chapter_file = md_file.name
                        entry.chapter_index = idx

                # Extract supercessions from body
                for sup_match in SUPERCESSION_RE.finditer(content):
                    raw_nums = sup_match.group(1).replace(",", "")
                    for num in re.findall(r"\d{5}", raw_nums):
                        if num != eo_number:
                            self.supercessions[eo_number].append(num)

                # Deduplicate supercession list
                if eo_number in self.supercessions:
                    self.supercessions[eo_number] = list(
                        dict.fromkeys(self.supercessions[eo_number])
                    )

        self._logger.info("Loaded %d chapter files from %s", len(self.chapter_texts), book_dir)

    def _load_events(self, index_dir: Path) -> None:
        """Load EO events from eo_events_*.jsonl and group by eo_identifier."""
        if not index_dir.is_dir():
            self._logger.warning("EO index folder not found: %s", index_dir)
            return

        event_files = sorted(index_dir.glob("eo_events_*.jsonl"))
        total = 0
        for ef in event_files:
            with open(ef, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    eo_id = record.get("eo_identifier", "")
                    # Normalise: extract 5-digit EO number
                    nums = re.findall(r"\d{5}", eo_id)
                    if not nums:
                        continue
                    eo_number = nums[0]
                    entry = self.entries.get(eo_number)
                    if entry is None:
                        entry = EOEntry(eo_number,
                                        title=record.get("eo_title", ""),
                                        date=record.get("eo_date", ""))
                        self.entries[eo_number] = entry
                    entry.events.append(record)
                    total += 1

        self._logger.info("Loaded %d EO events", total)

    def _load_summaries(self, index_dir: Path) -> None:
        """Load EO research summaries and match to registry entries."""
        if not index_dir.is_dir():
            return

        summary_files = sorted(index_dir.glob("research_summaries*.jsonl"))
        total = 0
        for sf in summary_files:
            with open(sf, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    # Try to extract EO number from the summary text or URL
                    text = record.get("summary_text", "")
                    nums = re.findall(r"(?:EO|Executive Order|Exec\.?\s*Order\s*No\.?)\s*(\d{5})",
                                      text, re.IGNORECASE)
                    for num in nums:
                        entry = self.entries.get(num)
                        if entry is not None:
                            entry.summaries.append(record)
                            total += 1
                            break  # one summary → one primary EO match

        self._logger.info("Matched %d EO summaries to registry entries", total)


# ---------------------------------------------------------------------------
# Phase 2 — TokenBudgetBatcher
# ---------------------------------------------------------------------------

class TokenBudgetBatcher(Base):
    """Group articles into batches of ≤ batch_size, sub-grouped by index_folder.

    Batching is purely mechanical (token budget) — no embedding similarity,
    no EO affinity scoring.  The LLM's reasoning determines all EO linkages.
    Within each batch, articles from the same index_folder are contiguous
    to minimise ``/switch-index`` calls.
    """

    def __init__(self, batch_size: int = 25) -> None:
        super().__init__()
        self.batch_size = batch_size

    def batch(self, articles: list[ArticleRecord]) -> list[list[ArticleRecord]]:
        """Return a list of batches, each ≤ batch_size articles."""
        # Sub-group by index_folder for efficient /switch-index
        by_index: dict[str, list[ArticleRecord]] = defaultdict(list)
        for article in articles:
            by_index[article.index_folder].append(article)

        # Flatten in index-folder order, then chunk
        ordered: list[ArticleRecord] = []
        for folder in sorted(by_index):
            ordered.extend(by_index[folder])

        batches: list[list[ArticleRecord]] = []
        for i in range(0, len(ordered), self.batch_size):
            batches.append(ordered[i : i + self.batch_size])

        self._logger.info(
            "Batched %d articles into %d batches (max %d per batch)",
            len(articles), len(batches), self.batch_size,
        )
        return batches


# ---------------------------------------------------------------------------
# Phase 2 — EOChatDriver
# ---------------------------------------------------------------------------

_EO_QUERY_PROMPT = (
    "Describe this official government communication. "
    "Identify the executive order(s) it implements or relates to. "
    "What action, decision, or event does it communicate? "
    "Who is the responsible official?"
)


class EOChatDriver(Base):
    """Query each article via RAG /query-llm with EO enrichments.

    Reuses the multi-index aware query pattern: switches index when the
    article's index_folder differs from the currently active one, then
    posts to /query-llm with enrichment flags for events and
    executive_orders.

    Retries with exponential backoff on HTTP 429 (Too Many Requests).
    """

    _MAX_RETRIES = 5
    _BACKOFF_BASE = 2.0  # seconds; doubles each retry

    def __init__(
        self,
        rag_service_url: str,
        top_k: int = 3,
        qa_top_k: int = 3,
        timeout: int = 600,
    ) -> None:
        super().__init__()
        self.rag_url = rag_service_url.rstrip("/")
        self.top_k = top_k
        self.qa_top_k = qa_top_k
        self.timeout = timeout
        self._active_folder: str | None = None

    # -- public API ----------------------------------------------------------

    def query_batch(
        self, batch: list[ArticleRecord]
    ) -> list[dict[str, Any]]:
        """Query every article in a batch, returning chat result dicts."""
        results: list[dict[str, Any]] = []
        for article in batch:
            result = self.query_article(article)
            results.append(result)
        return results

    def query_article(self, article: ArticleRecord) -> dict[str, Any]:
        """Call /query-llm for a single article, switching index if needed."""
        # Switch index if necessary
        if article.index_folder != self._active_folder:
            self._switch_index(article.index_folder)

        query_text = f"{article.title}\n\n{article.summary_text[:2000]}"

        payload = {
            "query": query_text,
            "mode": "ask",
            "top_k": self.top_k,
            "temperature": 0.5,
            "max_tokens": 16000,
            "use_qa": True,
            "qa_top_k": self.qa_top_k,
            "enrich": True,
            "enrich_types": ["events", "executive_orders"],
            "get_linked": False,
        }

        data = self._post_with_retry(
            f"{self.rag_url}/query_llm",
            payload,
            label=article.source_url,
        )

        return {
            "article_url": article.source_url,
            "article_title": article.title,
            "document_id": article.document_id,
            "publisher": article.doc_metadata.get("author", ""),
            "domain": article.domain,
            "index_folder": article.index_folder,
            "llm_response": data.get("llm_response", ""),
            "retrieved_docs": data.get("retrieved_docs", []),
            "retrieved_qa": data.get("retrieved_qa", []),
            "enrichment_results": data.get("enrichment_results", []),
            "eo_results": data.get("eo_results") or [],
            "event_results": data.get("event_results") or [],
            "citation_map": data.get("citation_map", {}),
            "execution_time_ms": data.get("execution_time_ms", 0),
            "llm_model": data.get("llm_model", ""),
        }

    # -- private helpers -----------------------------------------------------

    def _post_with_retry(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        label: str = "",
        req_timeout: int | None = None,
    ) -> dict[str, Any]:
        """POST with exponential backoff on 429 responses."""
        timeout = req_timeout or self.timeout
        for attempt in range(self._MAX_RETRIES + 1):
            try:
                resp = requests.post(url, json=payload, timeout=timeout)
                if resp.status_code == 429:
                    if attempt < self._MAX_RETRIES:
                        wait = self._BACKOFF_BASE * (2 ** attempt)
                        self._logger.warning(
                            "429 Too Many Requests for %s — retrying in %.1fs (attempt %d/%d)",
                            label or url, wait, attempt + 1, self._MAX_RETRIES,
                        )
                        time.sleep(wait)
                        continue
                    self._logger.error(
                        "429 Too Many Requests for %s — exhausted %d retries",
                        label or url, self._MAX_RETRIES,
                    )
                    return {}
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as exc:
                self._logger.error("Request failed for %s: %s", label or url, exc)
                return {}
        return {}

    def _switch_index(self, folder: str) -> None:
        """POST /switch-index to change the active RAG index."""
        result = self._post_with_retry(
            f"{self.rag_url}/switch-index",
            {"folder": folder},
            label=f"switch-index/{folder}",
            req_timeout=60,
        )
        if result is not None:
            self._active_folder = folder
            self._logger.info("Switched RAG index → %s", folder)


# ---------------------------------------------------------------------------
# Phase 2 — ConversationBuilder
# ---------------------------------------------------------------------------

class ConversationBuilder(Base):
    """Assemble per-batch conversation JSON (ExportStage-compatible schema)."""

    def __init__(self, output_dir: str | Path) -> None:
        super().__init__()
        self.output_dir = Path(output_dir)

    def build_and_save(
        self,
        batch_index: int,
        chat_results: list[dict[str, Any]],
    ) -> Path:
        """Build conversation list and write to batch_{b}/conversation.json."""
        conversation: list[dict[str, Any]] = []
        for result in chat_results:
            entry = {
                "user_query": _EO_QUERY_PROMPT,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "assistant_response": result.get("llm_response", ""),
                "retrieved_docs": result.get("retrieved_docs", []),
                "retrieved_qa": result.get("retrieved_qa", []),
                "eo_results": result.get("eo_results", []),
                "event_results": result.get("event_results", []),
                "enrichment_results": result.get("enrichment_results", []),
                "metadata": {
                    "article_url": result.get("article_url", ""),
                    "article_title": result.get("article_title", ""),
                    "document_id": result.get("document_id", ""),
                    "publisher": result.get("publisher", ""),
                    "domain": result.get("domain", ""),
                    "index_folder": result.get("index_folder", ""),
                    "llm_model": result.get("llm_model", ""),
                    "execution_time_ms": result.get("execution_time_ms", 0),
                    "citation_map": result.get("citation_map", {}),
                },
            }
            conversation.append(entry)

        batch_dir = self.output_dir / f"batch_{batch_index}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        out_path = batch_dir / "conversation.json"
        out_path.write_text(
            json.dumps(conversation, indent=2, default=str),
            encoding="utf-8",
        )
        self._logger.info(
            "Wrote %d conversation entries → %s", len(conversation), out_path,
        )
        return out_path


# ---------------------------------------------------------------------------
# Phase 2 — Cost tracking
# ---------------------------------------------------------------------------

# Pricing per 1M tokens (input/output) — extend as needed
_RAG_PRICING: dict[str, dict[str, float]] = {
    "gemini-3.1-pro-preview":  {"input": 1.25, "output": 10.00},
    "gemini-2.5-pro":          {"input": 1.25, "output": 10.00},
    "gemini-2.5-flash":        {"input": 0.30, "output":  2.50},
}


class CostTracker(Base):
    """Accumulate RAG call costs across batches."""

    def __init__(self) -> None:
        super().__init__()
        self.total_calls: int = 0
        self.total_time_ms: float = 0.0
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0

    def record(self, chat_results: list[dict[str, Any]]) -> None:
        """Record costs from a batch of chat results."""
        for r in chat_results:
            self.total_calls += 1
            self.total_time_ms += r.get("execution_time_ms", 0)
            # Token counts from llm_params if available
            params = r.get("llm_params", {})
            self.total_input_tokens += params.get("input_tokens", 0)
            self.total_output_tokens += params.get("output_tokens", 0)

    def summary(self, model: str = "") -> dict[str, Any]:
        """Return cost summary dict."""
        pricing = _RAG_PRICING.get(model, {"input": 0, "output": 0})
        input_cost = (self.total_input_tokens / 1_000_000) * pricing["input"]
        output_cost = (self.total_output_tokens / 1_000_000) * pricing["output"]
        return {
            "total_calls": self.total_calls,
            "total_time_s": round(self.total_time_ms / 1000, 2),
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "estimated_cost_usd": round(input_cost + output_cost, 4),
            "model": model,
        }


# ---------------------------------------------------------------------------
# Phase 3 — LINK-REFLECT Prompt Tuple
# ---------------------------------------------------------------------------

_LINK_PROMPT = """\
Given the EO legal framework defined in the chapters provided and the \
official communication from the government, create linkages between the \
official government comms and the EO framework. Each EO chapter can be \
linked to a communication around an action and/or decision taken. We want \
to logically group these against specific EOs. Actions and decisions can be \
composites speaking to general protective values behind the EOs as well — \
we want to make those linkages clear and unambiguous. We want to refer to \
timeline representations linking EOs and the comms.

For each communication, extract:
- The EO it implements (number, title, subsection if identifiable)
- The action, decision, or event taken
- The responsible official or department
- The date of the communication
- Whether this implements, extends, or modifies the EO directive

For each EO referenced, identify:
- EOs it rescinds or supersedes (legal precedence)
- EOs it is derived from or read together with
- The EO that is the common root (e.g., the rescinding EO as pivotal anchor)

Output as structured JSON with markdown headings per EO for RegExp anchoring.

--- EO LEGAL FRAMEWORK ---

{eo_grounding}

--- ACTUAL EXECUTIVE ORDER TEXT (Federal Register) ---

{eo_article_grounding}

--- OFFICIAL GOVERNMENT COMMUNICATIONS ---

{batch_conversations}
"""

_REFLECT_PROMPT = """\
Review the linkages between government communications and executive orders.
Validate each linkage: is the EO assignment correct? Are there missing \
linkages — communications that relate to EOs not yet identified? Are the \
supercession and revocation chains complete?

For each EO, verify the implementation timeline is chronologically consistent.
Identify gaps in the implementation record — EOs with no corresponding \
official communications. Identify cross-EO actions that implement multiple \
orders simultaneously.

Use the actual executive order text provided below to verify whether each \
EO truly rescinds, supersedes, derives from, or is read together with \
other EOs. The Federal Register text is authoritative — if the chapter \
analysis conflicts with the actual order text, prefer the order text.

Refine the structured JSON output to include corrected linkages, expanded \
precedence chains, and confidence scores per linkage.

--- EO LEGAL FRAMEWORK (Analytical Chapters) ---

{eo_grounding}

--- ACTUAL EXECUTIVE ORDER TEXT (Federal Register) ---

{eo_article_grounding}

--- LINK ANALYSIS OUTPUT ---

{link_output}

--- ORIGINAL GOVERNMENT COMMUNICATIONS ---

{batch_conversations}
"""


def _clean_json_markdown(text: str) -> str:
    """Strip markdown code fences from LLM JSON output (HC-10)."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[len("```json"):]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    return text.strip()


class LinkReflectDriver(Base):
    """Two-call LLM synthesis per batch: LINK then REFLECT.

    Uses ``gemini-3.1-pro-preview`` with ``reasoning_effort: "high"``
    and ``location: "global"`` via the ``GeminiLLMClient``.

    HC-9: 15-minute request timeout for 100k+ token input with high reasoning.
    """

    def __init__(
        self,
        model_name: str = "gemini-3.1-pro-preview",
        location: str = "global",
        output_dir: str | Path = ".",
        timeout: int = 900,
    ) -> None:
        super().__init__()
        self.model_name = model_name
        self.location = location
        self.output_dir = Path(output_dir)
        self.timeout = timeout
        self._client = None  # lazy init
        self.total_usage: dict[str, int] = {
            "input": 0, "output": 0, "thinking": 0,
        }

    def _get_client(self):
        """Lazy-init the Gemini client."""
        if self._client is None:
            from src.clients.llm_gcp_client import GeminiLLMClient
            self._client = GeminiLLMClient(
                model_name=self.model_name,
                location=self.location,
            )
        return self._client

    # -- public API ----------------------------------------------------------

    def process_batch(
        self,
        batch_index: int,
        eo_grounding: str,
        batch_conversations: str,
        eo_article_grounding: str = "",
    ) -> dict[str, Any]:
        """Run LINK + REFLECT for a single batch. Returns result dict.

        Saves artefacts to batch_{b}/link.md, reflect.md, status.json.
        Skips if status.json already shows "complete" (checkpointing).
        """
        batch_dir = self.output_dir / f"batch_{batch_index}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        status_path = batch_dir / "status.json"

        # -- Checkpoint: skip completed batches (HC-15) ---------------------
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

        try:
            # -- Call 1: LINK -----------------------------------------------
            self._logger.info("Batch %d — LINK call...", batch_index)
            link_prompt = _LINK_PROMPT.format(
                eo_grounding=eo_grounding,
                eo_article_grounding=eo_article_grounding or "(No reconstituted EO text available)",
                batch_conversations=batch_conversations,
            )
            link_text, link_usage = self._generate(link_prompt)
            result["link_tokens"] = link_usage

            link_path = batch_dir / "link.md"
            link_path.write_text(link_text, encoding="utf-8")
            self._logger.info(
                "Batch %d — LINK done: %d chars, %d input / %d output tokens",
                batch_index, len(link_text),
                link_usage.get("input", 0), link_usage.get("output", 0),
            )

            # -- Call 2: REFLECT --------------------------------------------
            self._logger.info("Batch %d — REFLECT call...", batch_index)
            reflect_prompt = _REFLECT_PROMPT.format(
                eo_grounding=eo_grounding,
                eo_article_grounding=eo_article_grounding or "(No reconstituted EO text available)",
                link_output=link_text,
                batch_conversations=batch_conversations,
            )
            reflect_text, reflect_usage = self._generate(reflect_prompt)
            result["reflect_tokens"] = reflect_usage

            reflect_path = batch_dir / "reflect.md"
            reflect_path.write_text(reflect_text, encoding="utf-8")
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

        # -- Write status checkpoint ----------------------------------------
        status_path.write_text(
            json.dumps(result, indent=2, default=str), encoding="utf-8",
        )
        return result

    # -- private helpers -----------------------------------------------------

    _MAX_RETRIES = 5
    _BACKOFF_BASE = 10.0  # seconds; doubles each retry (10, 20, 40, 80, 160)

    def _generate(self, prompt: str) -> tuple[str, dict[str, int]]:
        """Call Gemini and return (text, usage_dict).

        Retries with exponential backoff on 429 RESOURCE_EXHAUSTED.
        """
        from google.genai import types as genai_types
        from google.genai.errors import ClientError

        client = self._get_client()
        config = genai_types.GenerateContentConfig(
            temperature=0.2,
            top_p=client.top_p,
            max_output_tokens=32_000,
        )

        for attempt in range(self._MAX_RETRIES + 1):
            try:
                response = client.client.models.generate_content(
                    model=client.model_name,
                    contents=prompt,
                    config=config,
                )
                break  # success
            except ClientError as exc:
                if exc.code == 429 and attempt < self._MAX_RETRIES:
                    wait = self._BACKOFF_BASE * (2 ** attempt)
                    self._logger.warning(
                        "Gemini 429 RESOURCE_EXHAUSTED — retrying in %.0fs (attempt %d/%d)",
                        wait, attempt + 1, self._MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue
                raise  # non-429 or exhausted retries

        text = response.text if response.text else ""
        usage: dict[str, int] = {"input": 0, "output": 0, "thinking": 0}
        um = getattr(response, "usage_metadata", None)
        if um:
            usage["input"] = getattr(um, "prompt_token_count", 0) or 0
            usage["output"] = getattr(um, "candidates_token_count", 0) or 0
            usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

        # Accumulate totals
        for key in ("input", "output", "thinking"):
            self.total_usage[key] += usage[key]

        return text, usage

    def format_conversations_for_prompt(
        self, conv_path: Path,
    ) -> str:
        """Load a batch conversation.json and format for prompt injection."""
        if not conv_path.exists():
            return ""
        conversations = json.loads(conv_path.read_text(encoding="utf-8"))
        parts: list[str] = []
        for entry in conversations:
            meta = entry.get("metadata", {})
            parts.append(
                f"### {meta.get('article_title', 'Untitled')}\n"
                f"URL: {meta.get('article_url', '')}\n"
                f"Domain: {meta.get('domain', '')}\n"
                f"Publisher: {meta.get('publisher', '')}\n\n"
                f"{entry.get('assistant_response', '')}\n"
            )
        return "\n---\n\n".join(parts)


# ---------------------------------------------------------------------------
# Phase 4 — LinkageExtractor
# ---------------------------------------------------------------------------

# Regex to extract 5-digit EO numbers from varied key formats
_EO_KEY_RE = re.compile(r"(\d{5})")

# Regex to extract EO numbers from text like "Exec. Order No. 14148"
_EO_REF_RE = re.compile(
    r"(?:Exec\.?\s+Order\s+(?:No\.?\s*)?)(\d{5})", re.IGNORECASE,
)


class LinkageExtractor(Base):
    """Parse REFLECT JSON output into flat linkage records.

    Handles the variable JSON schemas produced by Gemini across batches:
    - Keys may be ``## Exec. Order No. XXXXX (YYYY)`` or ``Exec. Order No. XXXXX (YYYY)``
    - Communication fields vary (url vs no url, different action_type keys)
    - EO numbers in precedence lists may be bare or ``Exec. Order No. XXXXX``
    """

    def extract(
        self,
        reflect_text: str,
        batch_id: str,
    ) -> list[dict[str, Any]]:
        """Extract linkage records from a REFLECT output.

        Returns a list of flat dicts (one per EO-communication pair),
        plus EO-level records for precedence even when comms are empty.
        """
        cleaned = _clean_json_markdown(reflect_text)

        # The REFLECT output may contain prose before the JSON block.
        # Find the first '{' that starts the JSON object.
        json_start = cleaned.find("{")
        if json_start < 0:
            self._logger.warning("No JSON found in REFLECT output for %s", batch_id)
            return []
        cleaned = cleaned[json_start:]

        # Find the matching closing brace
        depth = 0
        json_end = -1
        for i, ch in enumerate(cleaned):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    json_end = i + 1
                    break
        if json_end > 0:
            cleaned = cleaned[:json_end]

        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            self._logger.error(
                "Failed to parse REFLECT JSON for %s: %s", batch_id, exc,
            )
            return []

        records: list[dict[str, Any]] = []

        for eo_key, eo_data in data.items():
            if not isinstance(eo_data, dict):
                continue

            # Extract EO number from key
            eo_match = _EO_KEY_RE.search(eo_key)
            if not eo_match:
                continue
            eo_number = eo_match.group(1)

            eo_title = eo_data.get("eo_title") or eo_data.get("title", "")
            eo_date = (
                eo_data.get("chronological_placement")
                or eo_data.get("eo_date", "")
            )

            # Parse precedence — extract EO numbers from varied formats
            precedence = {
                "rescinds": self._extract_eo_nums(
                    eo_data.get("rescinds_supersedes")
                    or eo_data.get("rescinds_or_supersedes", [])
                ),
                "derived_from": self._extract_eo_nums(
                    eo_data.get("derived_from_read_together")
                    or eo_data.get("derived_from_or_read_together_with", [])
                ),
                "read_with": self._extract_eo_nums(
                    eo_data.get("read_with", [])
                ),
            }

            comms = eo_data.get("communications", [])
            if not comms:
                # EO-only record (no comms but has precedence info)
                records.append({
                    "eo_number": eo_number,
                    "eo_title": eo_title,
                    "eo_date": eo_date,
                    "communication": None,
                    "precedence": precedence,
                    "confidence": 0.0,
                    "batch_id": batch_id,
                    "record_id": hashlib.sha256(
                        f"{eo_number}:no_comm:{batch_id}".encode()
                    ).hexdigest()[:16],
                })
                continue

            for comm in comms:
                if not isinstance(comm, dict):
                    continue

                comm_url = comm.get("url", "")
                comm_title = comm.get("communication_title", "")
                action_type = (
                    comm.get("implementation_status")
                    or comm.get("implements_extends_or_modifies", "")
                )
                # Normalise action_type to lowercase simple form
                action_lower = action_type.lower().split("(")[0].strip() if action_type else ""
                if "implement" in action_lower:
                    action_type_norm = "implements"
                elif "extend" in action_lower:
                    action_type_norm = "extends"
                elif "modif" in action_lower or "challeng" in action_lower:
                    action_type_norm = "modifies"
                elif "announce" in action_lower:
                    action_type_norm = "announces"
                else:
                    action_type_norm = action_lower or "unknown"

                # Build deterministic record ID for dedup (HC-11)
                dedup_key = f"{comm_url or comm_title}:{eo_number}"
                record_id = hashlib.sha256(dedup_key.encode()).hexdigest()[:16]

                records.append({
                    "eo_number": eo_number,
                    "eo_title": eo_title,
                    "eo_date": eo_date,
                    "communication": {
                        "url": comm_url,
                        "title": comm_title,
                        "date": comm.get("date", ""),
                        "domain": self._extract_domain(comm_url),
                        "official": (
                            comm.get("responsible_official_or_department")
                            or comm.get("responsible_official", "")
                        ),
                        "action_type": action_type_norm,
                        "action_description": (
                            comm.get("action_decision_or_event")
                            or comm.get("action_decision_event", "")
                        ),
                        "subsection": comm.get("subsection") or comm.get("eo_implemented", ""),
                    },
                    "precedence": precedence,
                    "confidence": comm.get("confidence_score", 0.0),
                    "batch_id": batch_id,
                    "record_id": record_id,
                    "cross_eo": comm.get("cross_eo_linkages", []),
                })

        self._logger.info(
            "Extracted %d linkage records from %s (%d EOs)",
            len(records), batch_id,
            len({r["eo_number"] for r in records}),
        )
        return records

    # -- helpers -------------------------------------------------------------

    def _extract_eo_nums(self, items: list[str] | None) -> list[str]:
        """Extract 5-digit EO numbers from a list of varied format strings."""
        if not items or not isinstance(items, list):
            return []
        result: list[str] = []
        for item in items:
            if not isinstance(item, str):
                continue
            for m in _EO_REF_RE.finditer(item):
                num = m.group(1)
                if num not in result:
                    result.append(num)
            # Also try bare 5-digit numbers
            if not _EO_REF_RE.search(item):
                for m in _EO_KEY_RE.finditer(item):
                    num = m.group(1)
                    if num not in result:
                        result.append(num)
        return result

    @staticmethod
    def _extract_domain(url: str) -> str:
        if not url:
            return ""
        try:
            return urlparse(url).hostname or ""
        except Exception:
            return ""


# ---------------------------------------------------------------------------
# Phase 4 — EOImplementationCache
# ---------------------------------------------------------------------------

class EOImplementationCache(Base):
    """Append-only JSONL cache with upsert dedup via record_id (HC-11).

    File: ``eo_implementation_cache.jsonl``
    One JSON object per line. Each record has a deterministic ``record_id``
    (sha256 of url+eo_number) to prevent duplicates on pipeline replays.
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
        """Filter records by EO number."""
        return [r for r in self._records.values() if r.get("eo_number") == eo_number]

    def by_domain(self, domain: str) -> list[dict[str, Any]]:
        """Filter records by source domain."""
        return [
            r for r in self._records.values()
            if r.get("communication", {}) and r["communication"].get("domain") == domain
        ]

    def precedence_graph(self) -> dict[str, dict[str, list[str]]]:
        """Build merged precedence graph across all records.

        Returns ``{eo_number: {rescinds: [...], derived_from: [...], read_with: [...]}}``
        """
        graph: dict[str, dict[str, list[str]]] = defaultdict(
            lambda: {"rescinds": [], "derived_from": [], "read_with": []}
        )
        for rec in self._records.values():
            eo = rec.get("eo_number", "")
            prec = rec.get("precedence", {})
            if not eo or not prec:
                continue
            for key in ("rescinds", "derived_from", "read_with"):
                for num in prec.get(key, []):
                    if num not in graph[eo][key]:
                        graph[eo][key].append(num)
        return dict(graph)

    def validate_precedence(self) -> list[str]:
        """Check for circular rescission conflicts. Returns list of warnings."""
        graph = self.precedence_graph()
        conflicts: list[str] = []
        for eo_a, prec_a in graph.items():
            for eo_b in prec_a.get("rescinds", []):
                prec_b = graph.get(eo_b, {})
                if eo_a in prec_b.get("rescinds", []):
                    msg = f"Circular rescission: EO {eo_a} ↔ EO {eo_b}"
                    if msg not in conflicts:
                        conflicts.append(msg)
        return conflicts

    def summary(self) -> dict[str, Any]:
        """Return summary statistics."""
        all_recs = list(self._records.values())
        eo_set = {r["eo_number"] for r in all_recs}
        comms_recs = [r for r in all_recs if r.get("communication")]
        domains = {r["communication"]["domain"] for r in comms_recs if r["communication"].get("domain")}
        return {
            "total_records": len(all_recs),
            "unique_eos": len(eo_set),
            "records_with_comms": len(comms_recs),
            "records_without_comms": len(all_recs) - len(comms_recs),
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
# EO Name Lookup — cached mapping of EO numbers to official titles
# ---------------------------------------------------------------------------

# Hardcoded fallbacks for EOs that predate our data sources (Federal Register
# official titles).  These will seed the cache if no upstream source provides
# a title for these numbers.
_EO_NAME_FALLBACKS: dict[str, str] = {
    "11246": "Equal Employment Opportunity",
    "12898": "Federal Actions to Address Environmental Justice in Minority Populations and Low-Income Populations",
    "13533": "Providing an Order of Succession Within the Department of Defense",
    "13583": "Establishing a Coordinated Government-Wide Initiative to Promote Diversity and Inclusion in the Federal Workforce",
    "13637": "Administration of Reformed Export Controls",
    "13950": "Combating Race and Sex Stereotyping",
    "13960": "Promoting the Use of Trustworthy Artificial Intelligence in the Federal Government",
    "13985": "Advancing Racial Equity and Support for Underserved Communities Through the Federal Government",
    "14004": "Enabling All Qualified Americans to Serve Their Country in Uniform",
    "14192": "Unleashing Prosperity Through Deregulation",
}


class EONameLookup(Base):
    """Persistent EO number → official name mapping.

    Merges names from multiple upstream sources (implementation cache,
    EO index JSONL, chapter body-text references, and hardcoded fallbacks)
    and caches the result as a JSON file so future runs start with a warm
    lookup.
    """

    def __init__(self, cache_path: str | Path) -> None:
        super().__init__()
        self._path = Path(cache_path)
        self._names: dict[str, str] = {}

    # -- public API ---------------------------------------------------------

    @property
    def names(self) -> dict[str, str]:
        return dict(self._names)

    def get(self, eo_number: str) -> str:
        """Return official name or empty string."""
        return self._names.get(eo_number, "")

    def load(self) -> int:
        """Load from the JSON cache file.  Returns count of loaded names."""
        if self._path.exists():
            with open(self._path, "r", encoding="utf-8") as fh:
                self._names = json.load(fh)
            self._logger.info("Loaded %d EO names from %s", len(self._names), self._path.name)
        return len(self._names)

    def save(self) -> None:
        """Persist the current lookup to the JSON cache file."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        ordered = dict(sorted(self._names.items()))
        with open(self._path, "w", encoding="utf-8") as fh:
            json.dump(ordered, fh, indent=2, ensure_ascii=False)
        self._logger.info("Saved %d EO names to %s", len(self._names), self._path.name)

    def populate(
        self,
        *,
        impl_cache_path: str | Path | None = None,
        eo_index_dir: str | Path | None = None,
        chapters_dir: str | Path | None = None,
    ) -> int:
        """Merge names from upstream sources.  Returns count of *new* names added.

        Priority order (first writer wins per EO number):
          1. Existing cache entries (already in ``self._names``)
          2. Implementation cache JSONL (``eo_title`` field)
          3. EO index JSONL (``eo_title`` / ``title`` field)
          4. Quoted references in chapter body text
          5. Hardcoded fallbacks (``_EO_NAME_FALLBACKS``)
        """
        before = len(self._names)

        # Source 1: Implementation cache JSONL
        if impl_cache_path:
            p = Path(impl_cache_path)
            if p.exists():
                with open(p, "r", encoding="utf-8") as fh:
                    for line in fh:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        num = rec.get("eo_number", "")
                        title = rec.get("eo_title", "")
                        if num and title:
                            self._names.setdefault(num, title)

        # Source 2: EO index JSONL files
        if eo_index_dir:
            idx = Path(eo_index_dir)
            if idx.is_dir():
                for jf in sorted(idx.glob("*.jsonl")):
                    with open(jf, "r", encoding="utf-8") as fh:
                        for line in fh:
                            try:
                                rec = json.loads(line)
                            except json.JSONDecodeError:
                                continue
                            eo_id = rec.get("eo_identifier", "")
                            title = rec.get("eo_title", "") or rec.get("title", "")
                            if eo_id and title:
                                m = re.search(r"\d{5}", eo_id)
                                if m:
                                    self._names.setdefault(m.group(), title)

        # Source 3: Quoted references in chapter body text
        if chapters_dir:
            cdir = Path(chapters_dir)
            if cdir.is_dir():
                ref_re = re.compile(r'EO\s+(\d{5})\s*\("([^"]+)"\)')
                for md in sorted(cdir.glob("*.md")):
                    for m in ref_re.finditer(md.read_text(encoding="utf-8")):
                        self._names.setdefault(m.group(1), m.group(2))

        # Source 4: Hardcoded fallbacks
        for num, title in _EO_NAME_FALLBACKS.items():
            self._names.setdefault(num, title)

        added = len(self._names) - before
        if added:
            self._logger.info("Populated %d new EO names (total: %d)", added, len(self._names))
        return added


# ---------------------------------------------------------------------------
# Phase 5 — EODiagramGenerator
# ---------------------------------------------------------------------------

# Characters that break Mermaid node labels (HC-12)
_MERMAID_UNSAFE_RE = re.compile(r'["\[\](){}|<>#&]')

# Analytical title prefixes to strip for clean EO names
_TITLE_PREFIX_RE = re.compile(
    r"^(?:Analysis of |An Analysis of )?"
    r"(?:Exec\.?\s+Order\s+No\.?\s*\d{5}\s*\(\d{4}\))"
    r"[\s:]*(?:and the |and |on |—\s*)?",
    re.IGNORECASE,
)
# Trailing analytical framing: ": An Analysis of Exec. Order No. XXXXX (YYYY)"
_TITLE_SUFFIX_RE = re.compile(
    r":?\s*(?:An )?Analysis of Exec\.?\s+Order\s+No\.?\s*\d{5}\s*\(\d{4}\)\s*$",
    re.IGNORECASE,
)
# Mid-sentence EO reference: "Through Exec. Order No. XXXXX (YYYY)"
_TITLE_MID_EO_RE = re.compile(
    r"\s*(?:Through|of|in|in Section \d+ of)\s+Exec\.?\s+Order\s+No\.?\s*\d{5}\s*\(\d{4}\)\s*$",
    re.IGNORECASE,
)


def _clean_eo_title(title: str) -> str:
    """Strip analytical framing to get the human-readable EO name."""
    if not title:
        return ""
    # 1. Try stripping prefix (most common: "Analysis of Exec. Order No. ... and the ...")
    cleaned = _TITLE_PREFIX_RE.sub("", title).strip()
    # 2. If prefix consumed everything, use original
    if len(cleaned) < 5:
        cleaned = title
    # 3. Strip suffix pattern ("...: An Analysis of Exec. Order No. XXXXX (YYYY)")
    cleaned = _TITLE_SUFFIX_RE.sub("", cleaned).strip()
    # 4. Strip mid-sentence EO references ("... Through Exec. Order No. XXXXX (YYYY)")
    cleaned = _TITLE_MID_EO_RE.sub("", cleaned).strip()
    # 5. Capitalize first letter if lowercase after stripping
    if cleaned and cleaned[0].islower():
        cleaned = cleaned[0].upper() + cleaned[1:]
    # 6. Remove stray leading punctuation
    cleaned = cleaned.lstrip(":— ").strip()
    return cleaned


def _mermaid_label(text: str, max_len: int = 50) -> str:
    """Sanitise a string for use in a Mermaid node label."""
    text = _MERMAID_UNSAFE_RE.sub("", text)
    if len(text) > max_len:
        text = text[: max_len - 1] + "…"
    return text.strip()


def _node_id(prefix: str, eo_number: str) -> str:
    """Deterministic Mermaid node ID."""
    return f"{prefix}_{eo_number}"


class EODiagramGenerator(Base):
    """Generate Mermaid LR tree diagrams showing EO precedence chains.

    - **Root node** (blue): the chapter's primary EO.
    - **Right side (→)**: derived-from / read-with EOs (green).
    - **Left side (←)**: rescinded/superseded EOs pointing *into* root (red).
    """

    # -- colour palette for Mermaid styles --
    STYLE_ROOT = "fill:#4c6ef5,stroke:#364fc7,color:#fff"
    STYLE_RESCINDED = "fill:#ffffff,stroke:#333333,color:#000"
    STYLE_DERIVED = "fill:#51cf66,stroke:#2b8a3e,color:#fff"
    STYLE_READ = "fill:#fcc419,stroke:#e67700,color:#000"

    def _eo_label(
        self,
        eo_number: str,
        registry: EORegistry,
        name_lookup: EONameLookup | None = None,
    ) -> str:
        """Build a clean two-line Mermaid node label: ``EO XXXXX<br/>Name``.

        Resolution order:
          1. ``name_lookup`` (official Federal Register titles)
          2. Registry entry title (chapter H1 heading, cleaned)
        """
        name = ""
        # Prefer the name lookup (official titles, cached)
        if name_lookup:
            raw = name_lookup.get(eo_number)
            if raw:
                name = _mermaid_label(raw)
        # Fallback to registry chapter title (cleaned)
        if not name:
            entry = registry.entries.get(eo_number)
            if entry and entry.title:
                name = _mermaid_label(_clean_eo_title(entry.title))
        return f"EO {eo_number}<br/>{name}" if name else f"EO {eo_number}"

    def generate(
        self,
        eo_number: str,
        precedence_graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
        name_lookup: EONameLookup | None = None,
    ) -> str:
        """Return a Mermaid markdown string for *eo_number*'s precedence."""
        root_id = _node_id("ROOT", eo_number)
        lines: list[str] = ["graph LR"]
        styles: list[str] = []

        # Root node
        root_label = self._eo_label(eo_number, registry, name_lookup)
        lines.append(f'  {root_id}["{root_label}"]')
        styles.append(f"  style {root_id} {self.STYLE_ROOT}")

        prec = precedence_graph.get(eo_number, {})

        # -- Disconnected: rescinded/superseded EOs (no edge) -------------
        for resc_eo in prec.get("rescinds", []):
            nid = _node_id("RESC", resc_eo)
            label = self._eo_label(resc_eo, registry, name_lookup)
            lines.append(f'  {nid}["{label}"]')
            styles.append(f"  style {nid} {self.STYLE_RESCINDED}")

        # -- Derived-from EOs (direction based on chronological order) -----
        root_num = int(eo_number) if eo_number.isdigit() else 0
        for der_eo in prec.get("derived_from", []):
            nid = _node_id("DER", der_eo)
            label = self._eo_label(der_eo, registry, name_lookup)
            der_num = int(der_eo) if der_eo.isdigit() else 0
            if der_num < root_num:
                # Older EO → root derives from it: arrow INTO root
                lines.append(f'  {nid}["{label}"] --> {root_id}')
            else:
                # Newer EO → it derives from root: arrow OUT of root
                lines.append(f'  {root_id} --> {nid}["{label}"]')
            styles.append(f"  style {nid} {self.STYLE_DERIVED}")

        # -- Right side: read-with EOs (different colour) -------------------
        for rw_eo in prec.get("read_with", []):
            nid = _node_id("RW", rw_eo)
            label = self._eo_label(rw_eo, registry, name_lookup)
            lines.append(f'  {root_id} --> {nid}["{label}"]')
            styles.append(f"  style {nid} {self.STYLE_READ}")

        # Legend
        lines.append("")
        lines.append("  subgraph Legend")
        lines.append('    direction LR')
        lines.append('    L1["Subject EO"]')
        lines.append('    L2["Rescinded / Superseded"]')
        lines.append('    L3["Derives from / Derived by"]')
        lines.append('    L4["Read with Subject"]')
        lines.append("  end")
        styles.append(f"  style L1 {self.STYLE_ROOT}")
        styles.append(f"  style L2 {self.STYLE_RESCINDED}")
        styles.append(f"  style L3 {self.STYLE_DERIVED}")
        styles.append(f"  style L4 {self.STYLE_READ}")

        # Combine
        lines.append("")
        lines.extend(styles)
        return "\n".join(lines)

    # -- Chapter-level batch generation (5.2) --------------------------------

    def generate_for_chapters(
        self,
        registry: EORegistry,
        precedence_graph: dict[str, dict[str, list[str]]],
        output_dir: str | Path,
        name_lookup: EONameLookup | None = None,
    ) -> list[dict[str, Any]]:
        """Generate one diagram per chapter that has a primary EO.

        Saves each diagram to ``output_dir/diagrams/{chapter_num}_eo_{eo_number}.md``
        and returns a manifest of generated files.
        """
        diagrams_dir = Path(output_dir) / "diagrams"
        diagrams_dir.mkdir(parents=True, exist_ok=True)

        # Merge registry supercessions into the precedence graph so chapters
        # whose EOs aren't yet in the cache still get rescission edges.
        merged = self._merge_supercessions(precedence_graph, registry)

        manifest: list[dict[str, Any]] = []
        eo_heading_re = re.compile(r"(\d{5})")

        for filename, content in registry.chapter_texts:
            # Extract chapter number prefix (e.g. "07" from "07-explain_...")
            chapter_num = filename.split("-")[0] if "-" in filename else filename.split(".")[0]

            # Identify primary EO from the first H1 heading in content
            eo_match = None
            for line in content.split("\n"):
                if line.startswith("# "):
                    eo_match = eo_heading_re.search(line)
                    break
            if not eo_match:
                continue
            eo_number = eo_match.group(1)
            if eo_number not in registry.entries:
                continue

            diagram = self.generate(eo_number, merged, registry, name_lookup)
            node_count = self._count_nodes(diagram)

            # Validate (5.3)
            warnings = self._validate(eo_number, merged, registry)

            out_path = diagrams_dir / f"{chapter_num}_eo_{eo_number}.md"
            header = (
                f"# EO {eo_number} — Precedence Diagram\n\n"
                f"Chapter: {filename}\n\n"
            )
            out_path.write_text(
                header + "```mermaid\n" + diagram + "\n```\n",
                encoding="utf-8",
            )

            manifest.append({
                "chapter": filename,
                "chapter_num": chapter_num,
                "eo_number": eo_number,
                "path": str(out_path),
                "nodes": node_count,
                "warnings": warnings,
            })

        self._logger.info(
            "Generated %d diagrams for %d chapters",
            len(manifest),
            len(registry.chapter_texts),
        )
        return manifest

    # -- helpers -------------------------------------------------------------

    @staticmethod
    def _merge_supercessions(
        graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
    ) -> dict[str, dict[str, list[str]]]:
        """Layer registry supercessions under the cache-based graph."""
        merged: dict[str, dict[str, list[str]]] = {}

        # Deep copy existing graph
        for eo, prec in graph.items():
            merged[eo] = {
                "rescinds": list(prec.get("rescinds", [])),
                "derived_from": list(prec.get("derived_from", [])),
                "read_with": list(prec.get("read_with", [])),
            }

        # Collect all EOs that appear in derived_from / read_with across
        # the *entire* graph — these have a known non-rescission relationship
        # and the regex-based registry supercessions should not override them.
        non_rescind: set[tuple[str, str]] = set()
        for eo, prec in merged.items():
            for d in prec.get("derived_from", []):
                non_rescind.add((eo, d))
            for r in prec.get("read_with", []):
                non_rescind.add((eo, r))

        # Add registry supercessions (these come from the EO book chapters)
        for eo, rescinded_list in registry.supercessions.items():
            if eo not in merged:
                merged[eo] = {"rescinds": [], "derived_from": [], "read_with": []}
            for resc_eo in rescinded_list:
                # Skip if the cache already classifies this link as
                # derived-from or read-with (either direction).
                if (eo, resc_eo) in non_rescind or (resc_eo, eo) in non_rescind:
                    continue
                if resc_eo not in merged[eo]["rescinds"]:
                    merged[eo]["rescinds"].append(resc_eo)

        return merged

    @staticmethod
    def _count_nodes(diagram: str) -> int:
        """Count unique node IDs in a Mermaid diagram."""
        # Nodes are defined by ID[" patterns
        return len(re.findall(r'\w+\["', diagram))

    @staticmethod
    def _validate(
        eo_number: str,
        graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
    ) -> list[str]:
        """Validate a diagram's EO references. Returns warnings."""
        warnings: list[str] = []
        prec = graph.get(eo_number, {})

        all_refs = (
            prec.get("rescinds", [])
            + prec.get("derived_from", [])
            + prec.get("read_with", [])
        )

        for ref_eo in all_refs:
            if ref_eo not in registry.entries:
                warnings.append(
                    f"EO {ref_eo} referenced by EO {eo_number} "
                    f"not found in registry"
                )

        # Circular rescission check
        for resc_eo in prec.get("rescinds", []):
            resc_prec = graph.get(resc_eo, {})
            if eo_number in resc_prec.get("rescinds", []):
                warnings.append(
                    f"Circular rescission: EO {eo_number} <-> EO {resc_eo}"
                )

        return warnings

    # -- Unified (chapter-independent) diagram (5.6) -------------------------

    def generate_unified(
        self,
        precedence_graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
        output_dir: str | Path,
        name_lookup: EONameLookup | None = None,
    ) -> dict[str, Any]:
        """Generate a single Mermaid diagram containing ALL EOs and edges.

        Unlike per-chapter diagrams, this shows the full precedence network
        independent of any chapter mapping.  Returns a manifest dict.
        """
        merged = self._merge_supercessions(precedence_graph, registry)

        lines: list[str] = ["graph LR"]
        styles: list[str] = []
        emitted_nodes: set[str] = set()
        emitted_edges: set[str] = set()

        def _ensure_node(eo: str, style: str) -> str:
            nid = f"EO_{eo}"
            if nid not in emitted_nodes:
                label = self._eo_label(eo, registry, name_lookup)
                lines.append(f'  {nid}["{label}"]')
                emitted_nodes.add(nid)
                styles.append(f"  style {nid} {style}")
            return nid

        # Collect all EOs that are only targets of rescission (never a root)
        root_eos = set(merged.keys())
        rescinded_only: set[str] = set()
        for prec in merged.values():
            for r in prec.get("rescinds", []):
                if r not in root_eos:
                    rescinded_only.add(r)

        # 1. Emit root EO nodes
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            _ensure_node(eo, self.STYLE_ROOT)

        # 2. Emit rescinded nodes (disconnected, white)
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            for resc_eo in merged[eo].get("rescinds", []):
                _ensure_node(resc_eo, self.STYLE_RESCINDED)

        # 3. Derived-from edges (chronological direction)
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            eo_num = int(eo) if eo.isdigit() else 0
            for der_eo in merged[eo].get("derived_from", []):
                der_num = int(der_eo) if der_eo.isdigit() else 0
                nid_der = _ensure_node(der_eo, self.STYLE_DERIVED)
                nid_root = f"EO_{eo}"
                if der_num < eo_num:
                    edge = f"  {nid_der} --> {nid_root}"
                else:
                    edge = f"  {nid_root} --> {nid_der}"
                if edge not in emitted_edges:
                    lines.append(edge)
                    emitted_edges.add(edge)

        # 4. Read-with edges
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            for rw_eo in merged[eo].get("read_with", []):
                nid_rw = _ensure_node(rw_eo, self.STYLE_READ)
                nid_root = f"EO_{eo}"
                edge_a = f"  {nid_root} --- {nid_rw}"
                edge_b = f"  {nid_rw} --- {nid_root}"
                if edge_a not in emitted_edges and edge_b not in emitted_edges:
                    lines.append(edge_a)
                    emitted_edges.add(edge_a)

        # Legend
        lines.append("")
        lines.append("  subgraph Legend")
        lines.append("    direction LR")
        lines.append('    L1["Active EO"]')
        lines.append('    L2["Rescinded / Superseded"]')
        lines.append('    L3["Derives from / Derived by"]')
        lines.append('    L4["Read with"]')
        lines.append("  end")
        styles.append(f"  style L1 {self.STYLE_ROOT}")
        styles.append(f"  style L2 {self.STYLE_RESCINDED}")
        styles.append(f"  style L3 {self.STYLE_DERIVED}")
        styles.append(f"  style L4 {self.STYLE_READ}")

        lines.append("")
        lines.extend(styles)
        diagram = "\n".join(lines)

        # Write out
        diagrams_dir = Path(output_dir) / "diagrams"
        diagrams_dir.mkdir(parents=True, exist_ok=True)
        out_path = diagrams_dir / "unified_precedence.md"

        header = (
            "# Unified EO Precedence Diagram\n\n"
            f"All {len(emitted_nodes)} EOs from the precedence graph.\n\n"
        )
        out_path.write_text(
            header + "```mermaid\n" + diagram + "\n```\n",
            encoding="utf-8",
        )

        node_count = self._count_nodes(diagram)
        self._logger.info(
            "Generated unified diagram: %d nodes, %d edges",
            node_count, len(emitted_edges),
        )

        return {
            "path": str(out_path),
            "nodes": node_count,
            "edges": len(emitted_edges),
        }
