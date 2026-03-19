"""EO Event Linking — Phase 1 & 2 components.

Provides:
- ``GovMilExtractor``: filters cross-index article catalog to .gov/.mil domains
- ``EORegistry``: loads EO book chapters, events, and supercession graph
- ``TokenBudgetBatcher``: groups articles into batches by index folder
- ``EOChatDriver``: queries RAG /query-llm per article with EO enrichment
- ``ConversationBuilder``: assembles conversation JSON per batch
"""

from __future__ import annotations

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

# Supercession / revocation detection in chapter body text
SUPERCESSION_RE = re.compile(
    r"(?:rescind|revok|supersed|replac)\w*\s+.*?"
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

                entry = self.entries.get(eo_number)
                if entry is None:
                    entry = EOEntry(eo_number, title=title, date=eo_year,
                                    chapter_file=md_file.name, chapter_index=idx)
                    self.entries[eo_number] = entry
                else:
                    entry.chapter_file = md_file.name
                    entry.chapter_index = idx
                    if not entry.title:
                        entry.title = title

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
    """

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

        try:
            resp = requests.post(
                f"{self.rag_url}/query_llm",
                json=payload,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as exc:
            self._logger.error(
                "RAG query failed for %s: %s", article.source_url, exc
            )
            data = {}

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

    def _switch_index(self, folder: str) -> None:
        """POST /switch-index to change the active RAG index."""
        try:
            resp = requests.post(
                f"{self.rag_url}/switch-index",
                json={"folder": folder},
                timeout=60,
            )
            if resp.status_code == 200:
                self._active_folder = folder
                self._logger.info("Switched RAG index → %s", folder)
            else:
                self._logger.warning(
                    "Failed to switch index to %s: HTTP %d",
                    folder, resp.status_code,
                )
        except requests.exceptions.RequestException as exc:
            self._logger.warning("Index switch failed for %s: %s", folder, exc)


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

Refine the structured JSON output to include corrected linkages, expanded \
precedence chains, and confidence scores per linkage.

--- EO LEGAL FRAMEWORK ---

{eo_grounding}

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

    def _generate(self, prompt: str) -> tuple[str, dict[str, int]]:
        """Call Gemini and return (text, usage_dict)."""
        from google.genai import types as genai_types

        client = self._get_client()
        config = genai_types.GenerateContentConfig(
            temperature=0.2,
            top_p=client.top_p,
            max_output_tokens=32_000,
        )
        response = client.client.models.generate_content(
            model=client.model_name,
            contents=prompt,
            config=config,
        )
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
