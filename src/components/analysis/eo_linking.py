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
    """Build conversation history from local article metadata — no API calls.

    The URL carries the event linkage.  Phase 3 (LINK-REFLECT) does all
    the LLM work using the conversation history + EO grounding.
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

    # -- public API ----------------------------------------------------------

    def query_batch(
        self, batch: list[ArticleRecord]
    ) -> list[dict[str, Any]]:
        """Build metadata dicts for every article in a batch."""
        return [self.query_article(article) for article in batch]

    def query_article(self, article: ArticleRecord) -> dict[str, Any]:
        """Extract metadata from the local ArticleRecord — no API call.

        The metadata (URL, title, publisher, date, summary) is sufficient
        for the conversation history consumed by Phase 3.
        """
        publisher = (article.doc_metadata.get("publisher", "")
                     or article.doc_metadata.get("author", ""))
        date = (article.doc_metadata.get("publication_date", "")
                or article.doc_metadata.get("date", ""))
        title = article.title or "Untitled"
        summary = article.summary_text or ""

        return {
            "article_url": article.source_url,
            "article_title": title,
            "document_id": article.document_id,
            "publisher": publisher,
            "date": date,
            "domain": article.domain,
            "index_folder": article.index_folder,
            "article_content": summary,
        }


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
                "assistant_response": result.get("article_content", ""),
                "metadata": {
                    "article_url": result.get("article_url", ""),
                    "article_title": result.get("article_title", ""),
                    "document_id": result.get("document_id", ""),
                    "publisher": result.get("publisher", ""),
                    "date": result.get("date", ""),
                    "domain": result.get("domain", ""),
                    "index_folder": result.get("index_folder", ""),
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

**SOURCE TRACEABILITY — CRITICAL REQUIREMENT**
Every communication listed below has a "URL:" line. That URL is the \
canonical source identifier. When you create a linkage for a communication, \
you MUST copy its exact URL verbatim into the "url" field of your output. \
Do NOT omit, paraphrase, shorten, or invent URLs. The URL is the primary \
key that links your analysis back to the original article — without it the \
record is incomplete. If a single source article relates to multiple EOs, \
repeat its URL in each EO's communication entry.

For each communication, extract:
- **url**: the exact URL from the source communication (MANDATORY)
- The EO it implements (number, title, subsection if identifiable)
- The action, decision, or event taken
- The responsible official or department
- The date of the communication (use the Date: from the source)
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

**SOURCE TRACEABILITY — MANDATORY**
The ORIGINAL GOVERNMENT COMMUNICATIONS section below contains the \
canonical source articles. Every article has a "URL:" line. That URL is \
the primary key that links your analysis back to the original source. \
For EVERY communication object you output you MUST:
1. Copy the exact URL from the matching source article into the "url" field.
2. Copy the article heading (the ### title) into "communication_title".
3. Use the "Date:" value from the source article for the "date" field.
Do NOT omit, shorten, or fabricate URLs. If a source article maps to \
multiple EOs, repeat its URL in each EO entry. A communication without \
a URL is an INCOMPLETE record.

**CRITICAL: You MUST respond with a single JSON object matching the exact \
schema below. Do NOT use any other key names, wrappers, or structures. \
Do NOT wrap the JSON in markdown code fences. Output raw JSON only.**

Required schema:
{{
  "executive_orders": {{
    "<Exec. Order No. NNNNN>": {{
      "title": "<EO title string>",
      "rescinds_or_supersedes": ["Exec. Order No. NNNNN", ...],
      "derived_from_or_read_together_with": ["Exec. Order No. NNNNN", ...],
      "communications": [
        {{
          "url": "<EXACT source URL from the article — MANDATORY>",
          "communication_title": "<### heading of the source article>",
          "date": "<YYYY-MM-DD from the source article Date: field>",
          "action_decision_or_event": "<description of action taken>",
          "responsible_official_or_department": "<official or dept name>",
          "implementation_status": "<implements|extends|modifies|announces>",
          "confidence_score": <0.0-1.0>
        }}
      ]
    }}
  }}
}}

Rules:
- Top-level key MUST be "executive_orders" containing a dict keyed by EO identifier.
- EO keys MUST use format "Exec. Order No. NNNNN" (5-digit number).
- Every EO entry MUST have "title", "rescinds_or_supersedes", \
"derived_from_or_read_together_with", and "communications" keys.
- "rescinds_or_supersedes" and "derived_from_or_read_together_with" are \
arrays of strings. Use empty arrays [] if none apply.
- "communications" is an array of objects. Use empty array [] if no comms.
- Each communication object MUST have ALL seven keys shown above.
- "url": MUST be the exact URL from the source article. Never empty, \
never fabricated. This is the traceable link back to the original source.
- "communication_title": MUST match the ### heading of the source article.
- "date": MUST come from the Date: field of the source article.
- "confidence_score": float between 0.0 and 1.0.
- "implementation_status": one of "implements", "extends", "modifies", "announces".
- Do NOT invent wrapper keys like "validated_linkages", "link_analysis", \
"executive_order_linkages", "executive_order_implementations", etc.
- Do NOT use a top-level array. Always use the dict-of-dicts structure above.
- Do NOT nest a "metadata" sub-object inside EO entries.

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

    Uses ``gemini-3.1-pro-preview`` with high reasoning via the
    ``GeminiLLMClient``.  Gemini's 1M+ token context window handles
    large chapter grounding + batch conversations without truncation.
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
        """Lazy-init the Gemini client, wrapped with Redis cache."""
        if self._client is None:
            from src._clients.llm_gcp_client import GeminiLLMClient
            from src.clients.llm_cache_client import CachedLLMClient
            from src.config import app_conf
            _inner = GeminiLLMClient(
                model_name=self.model_name,
                location=self.location,
            )
            self._client = CachedLLMClient(
                _inner,
                redis_host=app_conf.get("redis.host") or "",
                redis_port=int(app_conf.get("redis.port") or 6379),
                redis_password=app_conf.get("redis.password") or None,
                ttl_seconds=int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000),
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
        # Merge article grounding into eo_grounding if provided
        if eo_article_grounding:
            eo_grounding = eo_grounding + "\n\n" + eo_article_grounding
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
            reflect_text, reflect_usage = self._generate(
                reflect_prompt, response_mime_type="application/json",
            )
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

    def _generate(
        self,
        prompt: str,
        response_mime_type: str | None = None,
    ) -> tuple[str, dict[str, int]]:
        """Call Gemini and return (text, usage_dict)."""
        from google.genai import types as genai_types

        client = self._get_client()
        config_params: dict[str, Any] = {
            "temperature": 0.2,
            "top_p": client.top_p,
            "max_output_tokens": 32_000,
        }
        if response_mime_type:
            config_params["response_mime_type"] = response_mime_type
        config = genai_types.GenerateContentConfig(**config_params)
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
        """Load a batch conversation.json and format for prompt injection.

        Includes metadata, the canonical URL, and the assistant response
        summary so the LLM can trace every linkage back to a specific
        source article.
        """
        if not conv_path.exists():
            return ""
        conversations = json.loads(conv_path.read_text(encoding="utf-8"))
        parts: list[str] = []
        for entry in conversations:
            meta = entry.get("metadata", {})
            response = entry.get("assistant_response", "")
            # Truncate long responses to keep within token budget
            if len(response) > 2000:
                response = response[:2000] + " [...]"
            parts.append(
                f"### {meta.get('article_title', 'Untitled')}\n"
                f"URL: {meta.get('article_url', '')}\n"
                f"Domain: {meta.get('domain', '')}\n"
                f"Publisher: {meta.get('publisher', '')}\n"
                f"Date: {meta.get('date', '')}\n\n"
                f"{response}\n"
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

# Words to skip when doing content-based keyword matching
_STOP_WORDS = frozenset({
    "about", "after", "between", "could", "department", "during", "every",
    "following", "including", "order", "other", "president", "secretary",
    "should", "since", "state", "their", "these", "those", "through",
    "trump", "under", "united", "which", "while", "would",
})


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
        article_lookup: dict[str, dict[str, str]] | None = None,
    ) -> list[dict[str, Any]]:
        """Extract linkage records from a REFLECT output.

        Returns a list of flat dicts (one per EO-communication pair),
        plus EO-level records for precedence even when comms are empty.

        Handles three JSON schemas produced by Gemini:
          A) dict_keys: ``{"Exec. Order No. NNNNN": {comms, precedence}}``
          B) exec_orders: ``{"executive_orders": {"EO_NNNNN": {metadata, comms}}}``
          C) list: ``[{"eo_number": "...", comms, legal_precedence}]``

        Args:
            reflect_text: The full text output from the REFLECT prompt.
            batch_id: Identifier for the batch (provenance).
            article_lookup: Optional mapping of URL/Title -> metadata dict.
        """
        # Try all ```json blocks first, fall back to bare JSON
        json_blocks = re.findall(r'```json\s*([\s\S]*?)```', reflect_text)
        if not json_blocks:
            cleaned = _clean_json_markdown(reflect_text)
            # Find the first JSON structure (object or array)
            json_start = cleaned.find("{")
            list_start = cleaned.find("[")
            if json_start < 0 and list_start < 0:
                self._logger.warning("No JSON found in REFLECT output for %s", batch_id)
                return []
            # Pick whichever delimiter comes first
            if list_start >= 0 and (json_start < 0 or list_start < json_start):
                start = list_start
                open_ch, close_ch = "[", "]"
            else:
                start = json_start
                open_ch, close_ch = "{", "}"
            # Find the matching close
            depth = 0
            end = len(cleaned)
            for i in range(start, len(cleaned)):
                if cleaned[i] == open_ch:
                    depth += 1
                elif cleaned[i] == close_ch:
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break
            json_blocks = [cleaned[start:end]]

        records: list[dict[str, Any]] = []

        for block in json_blocks:
            block = block.strip()
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                continue

            # Normalise to a flat dict of {eo_key: eo_data_dict}
            eo_items: dict[str, dict] = {}

            if isinstance(data, list):
                # Top-level list: [{"eo_number": "...", ...}]
                eo_items = self._normalise_eo_list(data)

            elif isinstance(data, dict):
                # Look for a known wrapper key that holds the EO data
                _EO_WRAPPER_KEYS = (
                    "executive_orders",
                    "validated_linkages",
                    "executive_order_linkages",
                    "executive_order_implementations",
                    "link_analysis",
                )
                inner = None
                for wk in _EO_WRAPPER_KEYS:
                    if wk in data:
                        inner = data[wk]
                        break

                if inner is not None:
                    if isinstance(inner, list):
                        eo_items = self._normalise_eo_list(inner)
                    elif isinstance(inner, dict):
                        # Dict keyed by EO identifiers — may need metadata flattening
                        for k, v in inner.items():
                            if not isinstance(v, dict):
                                continue
                            eo_items[k] = self._flatten_eo_entry(v)
                else:
                    # Format A: top-level keys ARE the EO identifiers
                    for k, v in data.items():
                        if not isinstance(v, dict):
                            continue
                        # Heuristic: skip metadata-only keys that don't look like EO entries
                        if not (v.get("communications") or _EO_KEY_RE.search(k)):
                            continue
                        eo_items[k] = self._flatten_eo_entry(v)

            for eo_key, eo_data in eo_items.items():
                self._extract_eo_records(eo_key, eo_data, batch_id,
                                         article_lookup, records)

            if not eo_items:
                # Log the unexpected schema for debugging
                top_keys = list(data.keys()) if isinstance(data, dict) else f"list[{len(data)}]"
                self._logger.warning(
                    "No EO items extracted from JSON block in %s — "
                    "unexpected schema: %s",
                    batch_id, top_keys,
                )

        self._logger.info(
            "Extracted %d linkage records from %s (%d EOs)",
            len(records), batch_id,
            len({r["eo_number"] for r in records}),
        )
        return records

    # -- normalisation helpers ------------------------------------------------

    def _normalise_eo_list(self, items: list) -> dict[str, dict]:
        """Convert a list of EO dicts into a keyed dict, flattening each."""
        result: dict[str, dict] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            # Try several key names for the EO identifier
            eo_id = (
                item.get("eo_number")
                or item.get("eo_identifier")
                or item.get("executive_order")
                or item.get("eo", "")
            )
            if not eo_id:
                continue
            result[str(eo_id)] = self._flatten_eo_entry(item)
        return result

    @staticmethod
    def _flatten_eo_entry(entry: dict) -> dict:
        """Flatten metadata / legal_precedence sub-dicts and normalise comm fields."""
        flat = dict(entry)

        # Flatten metadata (Format B: {"metadata": {"title": ..., "date_signed": ...}})
        meta = flat.pop("metadata", None) or flat.pop("eo_metadata", None)
        if isinstance(meta, dict):
            flat.setdefault("title", meta.get("title", ""))
            flat.setdefault("eo_title", meta.get("title", ""))
            flat.setdefault("eo_date", meta.get("date_signed", ""))
            flat.setdefault("rescinds_or_supersedes",
                            meta.get("rescinds_or_supersedes", []))
            flat.setdefault("derived_from_or_read_together_with",
                            meta.get("derived_from_or_read_together_with", []))

        # Flatten legal_precedence (Format C)
        lp = flat.pop("legal_precedence", None)
        if isinstance(lp, dict):
            flat.setdefault("rescinds_or_supersedes",
                            lp.get("rescinds_or_supersedes",
                                   lp.get("rescinds_supersedes", [])))
            flat.setdefault("derived_from_or_read_together_with",
                            lp.get("derived_from_or_read_together_with",
                                   lp.get("derived_from_read_together", [])))

        # Flatten precedence_chain / read_together_with (batch_14/31 style)
        if "precedence_chain" in flat and "rescinds_or_supersedes" not in flat:
            flat["rescinds_or_supersedes"] = flat.pop("precedence_chain", [])
        if "read_together_with" in flat and "derived_from_or_read_together_with" not in flat:
            flat["derived_from_or_read_together_with"] = flat.pop("read_together_with", [])

        # Normalise communication field names
        comms = flat.get("communications", [])
        normalised_comms = []
        for c in comms:
            if not isinstance(c, dict):
                continue
            nc = dict(c)
            if "source_url" in nc and "url" not in nc:
                nc["url"] = nc.pop("source_url")
            if "action_decision_event" in nc and "action_decision_or_event" not in nc:
                nc["action_decision_or_event"] = nc.pop("action_decision_event")
            if "responsible_official_department" in nc and "responsible_official_or_department" not in nc:
                nc["responsible_official_or_department"] = nc.pop("responsible_official_department")
            normalised_comms.append(nc)
        flat["communications"] = normalised_comms

        return flat

    def _extract_eo_records(
        self,
        eo_key: str,
        eo_data: dict,
        batch_id: str,
        article_lookup: dict[str, dict[str, str]] | None,
        records: list[dict[str, Any]],
    ) -> None:
        """Extract records for a single EO entry into *records*."""
        eo_match = _EO_KEY_RE.search(eo_key)
        if not eo_match:
            return
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
            return

        for comm in comms:
            if not isinstance(comm, dict):
                continue

            comm_url = comm.get("url", "")
            comm_title = comm.get("communication_title", "")
            comm_date = comm.get("date", "")

            # Hydrate missing metadata from article lookup (if available)
            if article_lookup:
                meta = {}
                # 1. Exact match on URL
                if comm_url and comm_url in article_lookup:
                    meta = article_lookup[comm_url]
                # 2. Exact match on title
                elif comm_title and comm_title in article_lookup:
                    meta = article_lookup[comm_title]
                # 3. Normalized title match
                elif comm_title:
                    norm = comm_title.strip().lower()
                    if norm in article_lookup:
                        meta = article_lookup[norm]
                # 4. Fuzzy substring match: action description against
                #    article titles in the lookup
                if not meta:
                    action_desc = (
                        comm.get("action_decision_or_event")
                        or comm.get("action_decision_event", "")
                    )
                    if action_desc:
                        desc_lower = action_desc.lower()
                        best_score = 0
                        for lk_key, lk_meta in article_lookup.items():
                            lk_title = lk_meta.get("title", "")
                            if not lk_title or len(lk_title) < 10:
                                continue
                            t_lower = lk_title.lower()
                            # Check if significant portion of title appears in action
                            words = [w for w in t_lower.split() if len(w) > 3]
                            if not words:
                                continue
                            hits = sum(1 for w in words if w in desc_lower)
                            score = hits / len(words)
                            if score > best_score and score >= 0.5:
                                best_score = score
                                meta = lk_meta

                # 5. Content-based match: search comm title + action desc
                #    keywords in the assistant_response text stored in the
                #    lookup entries.
                if not meta:
                    search_parts = [
                        p for p in (
                            comm_title,
                            comm.get("action_decision_or_event")
                            or comm.get("action_decision_event", ""),
                        ) if p
                    ]
                    search_text = " ".join(search_parts)
                    if search_text:
                        # Strip punctuation from words for cleaner matching
                        raw = re.sub(r"[^\w\s]", " ", search_text.lower())
                        words = [
                            w for w in raw.split()
                            if len(w) > 4 and w not in _STOP_WORDS
                        ]
                        if words:
                            seen_urls: set[str] = set()
                            best_score = 0.0
                            for lk_meta in article_lookup.values():
                                lk_url = lk_meta.get("url", "")
                                if not lk_url or lk_url in seen_urls:
                                    continue
                                seen_urls.add(lk_url)
                                resp = lk_meta.get("response_text", "")
                                if not resp:
                                    continue
                                resp_lower = resp.lower()
                                hits = sum(
                                    1 for w in words if w in resp_lower
                                )
                                score = hits / len(words)
                                if score > best_score and score >= 0.35:
                                    best_score = score
                                    meta = lk_meta

                # 6. Date-based fallback: match comm date against
                #    conversation dates; if only one article shares the
                #    date, assign its URL.  If several share the date,
                #    pick the one with the best content overlap (even
                #    below the normal threshold).
                if not meta and comm_date and article_lookup:
                    date_matches: list[dict[str, str]] = []
                    seen_urls2: set[str] = set()
                    for lk_meta in article_lookup.values():
                        lk_url = lk_meta.get("url", "")
                        if not lk_url or lk_url in seen_urls2:
                            continue
                        seen_urls2.add(lk_url)
                        lk_date = lk_meta.get("date", "")
                        if lk_date and comm_date.startswith(lk_date[:7]):
                            date_matches.append(lk_meta)
                    if len(date_matches) == 1:
                        meta = date_matches[0]
                    elif len(date_matches) > 1:
                        # Multiple same-month: pick best content overlap
                        raw_fb = re.sub(
                            r"[^\w\s]", " ",
                            (comm_title + " " + (
                                comm.get("action_decision_or_event")
                                or comm.get("action_decision_event", "")
                            )).lower(),
                        )
                        fb_words = [
                            w for w in raw_fb.split()
                            if len(w) > 4 and w not in _STOP_WORDS
                        ]
                        if fb_words:
                            best_fb = 0.0
                            for dm in date_matches:
                                resp = dm.get("response_text", "").lower()
                                hits = sum(1 for w in fb_words if w in resp)
                                sc = hits / len(fb_words)
                                if sc > best_fb:
                                    best_fb = sc
                                    meta = dm

                # 7. Best-available fallback: every communication was
                #    derived from one of the batch's conversation items.
                #    Pick the item whose response_text has the highest
                #    keyword overlap — no minimum threshold.
                if not meta and article_lookup:
                    raw_final = re.sub(
                        r"[^\w\s]", " ",
                        " ".join(filter(None, [
                            comm_title,
                            comm.get("action_decision_or_event")
                            or comm.get("action_decision_event", ""),
                            comm.get("responsible_official_or_department")
                            or comm.get("responsible_official", ""),
                        ])).lower(),
                    )
                    final_words = [
                        w for w in raw_final.split()
                        if len(w) > 2 and w not in _STOP_WORDS
                    ]
                    if final_words:
                        seen_urls3: set[str] = set()
                        best_final = 0
                        for lk_meta in article_lookup.values():
                            lk_url = lk_meta.get("url", "")
                            if not lk_url or lk_url in seen_urls3:
                                continue
                            seen_urls3.add(lk_url)
                            resp = lk_meta.get("response_text", "")
                            if not resp:
                                continue
                            hits = sum(
                                1 for w in final_words
                                if w in resp.lower()
                            )
                            if hits > best_final:
                                best_final = hits
                                meta = lk_meta

                if meta:
                    if not comm_url:
                        comm_url = meta.get("url", "")
                    if not comm_title:
                        comm_title = meta.get("title", "")

                    # Date enhancement: prefer metadata date if more precise than LLM date
                    meta_date = meta.get("date", "")
                    if not comm_date or (meta_date and len(meta_date) > len(comm_date)):
                        comm_date = meta_date

                    # Domain is handled below via _extract_domain(comm_url)

                    # Fallback for official/department using publisher if available
                    if not comm.get("responsible_official_or_department") and meta.get("publisher"):
                        comm["responsible_official_or_department"] = meta["publisher"]

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
                    "date": comm_date,
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
    "12866": "Regulatory Planning and Review",
    "12898": "Federal Actions to Address Environmental Justice in Minority Populations and Low-Income Populations",
    "13533": "Providing an Order of Succession Within the Department of Defense",
    "13583": "Establishing a Coordinated Government-Wide Initiative to Promote Diversity and Inclusion in the Federal Workforce",
    "13637": "Administration of Reformed Export Controls",
    "13859": "Maintaining American Leadership in Artificial Intelligence",
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
# EO filtering — exclude / noise labelling
# ---------------------------------------------------------------------------


def filter_precedence_graph(
    graph: dict[str, dict[str, list[str]]],
    exclude_eos: set[str],
) -> dict[str, dict[str, list[str]]]:
    """Return a copy of *graph* with all references to *exclude_eos* removed.

    - EOs in *exclude_eos* are dropped as top-level keys.
    - References to excluded EOs are stripped from ``rescinds``,
      ``derived_from``, and ``read_with`` lists.
    """
    if not exclude_eos:
        return graph
    filtered: dict[str, dict[str, list[str]]] = {}
    for eo, prec in graph.items():
        if eo in exclude_eos:
            continue
        filtered[eo] = {
            k: [v for v in vals if v not in exclude_eos]
            for k, vals in prec.items()
        }
    return filtered


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


_MERMAID_MIN_LABEL_WIDTH = 30  # pad labels so blocks render at uniform width


def _mermaid_label(text: str, max_len: int = 50) -> str:
    """Sanitise a string for use in a Mermaid node label."""
    text = _MERMAID_UNSAFE_RE.sub("", text)
    if len(text) > max_len:
        text = text[: max_len - 1] + "…"
    text = text.strip()
    # Pad short labels so Mermaid renders blocks at a uniform width
    if len(text) < _MERMAID_MIN_LABEL_WIDTH:
        pad = _MERMAID_MIN_LABEL_WIDTH - len(text)
        left = pad // 2
        right = pad - left
        text = "\u00A0" * left + text + "\u00A0" * right
    return text


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
    # Blue for the subject EO (the chapter topic)
    STYLE_ROOT = "fill:#e1f5fe,stroke:#01579b,stroke-width:2px,color:#000"
    # Neutral/White for all other nodes (rescinded, derived, read-with)
    STYLE_RESCINDED = "fill:#ffffff,stroke:#333333,color:#000"
    STYLE_DERIVED = "fill:#ffffff,stroke:#333333,color:#000"
    STYLE_READ = "fill:#ffffff,stroke:#333333,color:#000"

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
        if not name:
            name = "Executive Order"
        return f"EO {eo_number}<br/>{name}"

    @staticmethod
    def _emit_chrono_chain(
        nodes: list[tuple[int, str]],
        lines: list[str],
    ) -> None:
        """Append invisible ``~~~`` links to enforce chronological LR order.

        *nodes* is a list of ``(eo_number_int, mermaid_node_id)`` tuples.
        After sorting by EO number (== signing date), consecutive pairs are
        linked with invisible edges so Mermaid places earlier EOs to the
        left and later EOs to the right — no vertical alignment of nodes
        with different dates.
        """
        if len(nodes) < 2:
            return
        sorted_nodes = sorted(nodes, key=lambda x: x[0])
        for i in range(len(sorted_nodes) - 1):
            lines.append(
                f"  {sorted_nodes[i][1]} ~~~ {sorted_nodes[i + 1][1]}"
            )

    def generate(
        self,
        eo_number: str,
        precedence_graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
        name_lookup: EONameLookup | None = None,
        exclude_eos: set[str] | None = None,
    ) -> str:
        """Return a Mermaid markdown string for *eo_number*'s precedence.

        Rescinded EOs are excluded.  Nodes are ordered left-to-right by
        date (EO number) via invisible links so that earlier EOs always
        appear to the left of later ones.
        """
        if exclude_eos:
            precedence_graph = filter_precedence_graph(precedence_graph, exclude_eos)
        root_id = _node_id("ROOT", eo_number)
        root_num = int(eo_number) if eo_number.isdigit() else 0
        lines: list[str] = ["graph LR"]
        styles: list[str] = []
        # Track (eo_number_int, node_id) for chronological ordering
        all_nodes: list[tuple[int, str]] = []

        # Root node
        root_label = self._eo_label(eo_number, registry, name_lookup)
        lines.append(f'  {root_id}["{root_label}"]')
        styles.append(f"  style {root_id} {self.STYLE_ROOT}")
        all_nodes.append((root_num, root_id))

        prec = precedence_graph.get(eo_number, {})

        # -- Derived-from EOs (arrow direction = chronological) ------------
        for der_eo in prec.get("derived_from", []):
            nid = _node_id("DER", der_eo)
            label = self._eo_label(der_eo, registry, name_lookup)
            der_num = int(der_eo) if der_eo.isdigit() else 0
            if der_num < root_num:
                lines.append(f'  {nid}["{label}"] --> {root_id}')
            else:
                lines.append(f'  {root_id} --> {nid}["{label}"]')
            styles.append(f"  style {nid} {self.STYLE_DERIVED}")
            all_nodes.append((der_num, nid))

        # -- Read-with EOs (undirected, chronological placement) -----------
        for rw_eo in prec.get("read_with", []):
            nid = _node_id("RW", rw_eo)
            label = self._eo_label(rw_eo, registry, name_lookup)
            rw_num = int(rw_eo) if rw_eo.isdigit() else 0
            if rw_num < root_num:
                lines.append(f'  {nid}["{label}"] --- {root_id}')
            else:
                lines.append(f'  {root_id} --- {nid}["{label}"]')
            styles.append(f"  style {nid} {self.STYLE_READ}")
            all_nodes.append((rw_num, nid))

        # -- Invisible chronological chain (left = oldest) -----------------
        self._emit_chrono_chain(all_nodes, lines)

        # Combine
        lines.append("")
        lines.extend(styles)
        return "\n".join(lines)

    # -- Focused dependency diagram ------------------------------------------

    def generate_focused(
        self,
        root_eo: str,
        precedence_graph: dict[str, dict[str, list[str]]],
        registry: EORegistry,
        name_lookup: EONameLookup | None = None,
        exclude_eos: set[str] | None = None,
    ) -> str:
        """Generate a diagram showing *root_eo* and all forward-connected nodes.

        Walks derived_from and read_with edges transitively to find all EOs
        reachable from *root_eo*.
        """
        merged = self._merge_supercessions(precedence_graph, registry)
        if exclude_eos:
            merged = filter_precedence_graph(merged, exclude_eos)

        # BFS to find all forward-connected nodes
        visited: set[str] = set()
        queue: list[str] = [root_eo]
        while queue:
            eo = queue.pop(0)
            if eo in visited:
                continue
            visited.add(eo)
            prec = merged.get(eo, {})
            for der in prec.get("derived_from", []):
                if der not in visited:
                    queue.append(der)
            for rw in prec.get("read_with", []):
                if rw not in visited:
                    queue.append(rw)

        # Also check reverse: EOs that list root_eo in their derived_from
        for eo, prec in merged.items():
            if root_eo in prec.get("derived_from", []) and eo not in visited:
                visited.add(eo)

        lines: list[str] = ["graph LR"]
        styles: list[str] = []
        emitted_edges: set[str] = set()

        # Emit all nodes (sorted chronologically)
        all_nodes: list[tuple[int, str]] = []
        for eo in sorted(visited, key=lambda e: int(e) if e.isdigit() else 0):
            nid = f"EO_{eo}"
            label = self._eo_label(eo, registry, name_lookup)
            lines.append(f'  {nid}["{label}"]')
            style = self.STYLE_ROOT if eo == root_eo else self.STYLE_DERIVED
            styles.append(f"  style {nid} {style}")
            all_nodes.append((int(eo) if eo.isdigit() else 0, nid))

        # Emit edges
        for eo in sorted(visited, key=lambda e: int(e) if e.isdigit() else 0):
            eo_num = int(eo) if eo.isdigit() else 0
            prec = merged.get(eo, {})
            for der in prec.get("derived_from", []):
                if der not in visited:
                    continue
                der_num = int(der) if der.isdigit() else 0
                nid_a, nid_b = f"EO_{eo}", f"EO_{der}"
                if der_num < eo_num:
                    edge = f"  {nid_b} --> {nid_a}"
                else:
                    edge = f"  {nid_a} --> {nid_b}"
                if edge not in emitted_edges:
                    lines.append(edge)
                    emitted_edges.add(edge)
            for rw in prec.get("read_with", []):
                if rw not in visited:
                    continue
                nid_a, nid_b = f"EO_{eo}", f"EO_{rw}"
                edge_a = f"  {nid_a} --- {nid_b}"
                edge_b = f"  {nid_b} --- {nid_a}"
                if edge_a not in emitted_edges and edge_b not in emitted_edges:
                    lines.append(edge_a)
                    emitted_edges.add(edge_a)

        # Invisible chronological chain (left = oldest)
        self._emit_chrono_chain(all_nodes, lines)

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
        exclude_eos: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate one diagram per chapter that has a primary EO.

        Saves each diagram to ``output_dir/diagrams/{chapter_num}_eo_{eo_number}.md``
        and returns a manifest of generated files.
        """
        diagrams_dir = Path(output_dir) / "diagrams"
        diagrams_dir.mkdir(parents=True, exist_ok=True)

        # Remove stale per-chapter diagram files from previous runs
        # (chapter numbering can change, leaving orphans).
        for old in diagrams_dir.glob("[0-9]*_eo_*.md"):
            old.unlink()

        # Merge registry supercessions into the precedence graph so chapters
        # whose EOs aren't yet in the cache still get rescission edges.
        merged = self._merge_supercessions(precedence_graph, registry)
        if exclude_eos:
            merged = filter_precedence_graph(merged, exclude_eos)

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
        exclude_eos: set[str] | None = None,
    ) -> dict[str, Any]:
        """Generate a single Mermaid diagram containing ALL EOs and edges.

        Unlike per-chapter diagrams, this shows the full precedence network
        independent of any chapter mapping.  Returns a manifest dict.
        """
        merged = self._merge_supercessions(precedence_graph, registry)
        if exclude_eos:
            merged = filter_precedence_graph(merged, exclude_eos)

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

        # 1. Emit root EO nodes
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            _ensure_node(eo, self.STYLE_ROOT)

        # 2. Derived-from edges (chronological direction)
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

        # 3. Read-with edges
        for eo in sorted(merged.keys(), key=lambda e: int(e) if e.isdigit() else 0):
            for rw_eo in merged[eo].get("read_with", []):
                nid_rw = _ensure_node(rw_eo, self.STYLE_READ)
                nid_root = f"EO_{eo}"
                edge_a = f"  {nid_root} --- {nid_rw}"
                edge_b = f"  {nid_rw} --- {nid_root}"
                if edge_a not in emitted_edges and edge_b not in emitted_edges:
                    lines.append(edge_a)
                    emitted_edges.add(edge_a)

        # 4. Invisible chronological chain (left = oldest)
        chrono_nodes = [
            (int(nid.removeprefix("EO_")) if nid.removeprefix("EO_").isdigit() else 0, nid)
            for nid in emitted_nodes
        ]
        self._emit_chrono_chain(chrono_nodes, lines)

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


# ---------------------------------------------------------------------------
# Phase 6 — ImplementationTableGenerator
# ---------------------------------------------------------------------------

_DATE_PARSE_FMTS = [
    "%B %d, %Y",     # January 27, 2025
    "%B %Y",         # January 2025
    "%Y-%m-%d",      # 2025-01-27
    "%m/%d/%Y",      # 01/27/2025
    "%d %B %Y",      # 27 January 2025
    "%Y",            # 2025
]


def _parse_date_loose(raw: str) -> datetime | None:
    """Best-effort date parse from communication date strings."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in _DATE_PARSE_FMTS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _normalise_date(raw: str) -> str:
    """Normalise a date string to YYYY-MM-DD format.  Falls back to raw."""
    dt = _parse_date_loose(raw)
    if dt is None:
        return raw or "—"
    # Year-only input → return just the year
    if raw.strip().isdigit() and len(raw.strip()) == 4:
        return raw.strip()
    # Month+Year only (no day) → YYYY-MM
    if dt.day == 1 and raw.strip().replace(",", "").split()[-1].isdigit():
        # Check if original string had a day component
        tokens = raw.strip().replace(",", "").split()
        has_day = any(t.isdigit() and len(t) <= 2 for t in tokens[:-1])
        if not has_day:
            return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


def _eo_apa_cite(
    eo_number: str,
    year: str = "",
    registry: EORegistry | None = None,
    name_lookup: EONameLookup | None = None,
) -> str:
    """Format EO citation in APA inline style, linking to local chapter if available."""
    text = f"Exec. Order No. {eo_number} ({year})" if year else f"Exec. Order No. {eo_number}"

    # If registry is available, check for chapter link
    if registry:
        entry = registry.entries.get(eo_number)
        if entry and entry.chapter_file:
            return f"[{text}]({entry.chapter_file})"

    return text


def _eo_name(eo_number: str, name_lookup: EONameLookup | None = None,
             registry: EORegistry | None = None) -> str:
    """Return the official EO name via name_lookup or registry."""
    if name_lookup:
        name = name_lookup.get(eo_number)
        if name:
            return _clean_eo_title(name)
    if registry:
        entry = registry.entries.get(eo_number)
        if entry and entry.title:
            return _clean_eo_title(entry.title)
    return ""


def _truncate(text: str, max_len: int = 120) -> str:
    """Truncate text with ellipsis if needed."""
    if not text or len(text) <= max_len:
        return text or ""
    return text[:max_len - 1] + "…"


class ImplementationTableGenerator(Base):
    """Generate time-ordered Markdown implementation tables from cached linkage records.

    Produces per-chapter tables (one table per EO) and a cross-EO actions table
    that groups communications implementing multiple EOs simultaneously.
    """

    def __init__(self) -> None:
        super().__init__()

    # -- single-EO table -----------------------------------------------------

    def generate(
        self,
        eo_number: str,
        cache: EOImplementationCache,
        name_lookup: EONameLookup | None = None,
        registry: EORegistry | None = None,
    ) -> str:
        """Return a Markdown table for one EO's implementation communications.

        Rows sorted chronologically earliest→latest. Returns empty string if no comms exist.
        """
        records = cache.by_eo(eo_number)
        rows = [r for r in records if r.get("communication")]
        if not rows:
            return ""
        return self.format_table(rows, registry=registry, name_lookup=name_lookup)

    # -- per-chapter tables --------------------------------------------------

    def generate_for_chapters(
        self,
        registry: EORegistry,
        cache: EOImplementationCache,
        output_dir: str | Path,
        name_lookup: EONameLookup | None = None,
        exclude_eos: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Generate one implementation table per chapter.

        For each chapter's primary EO, also includes communications linked to
        any derived-from or read-with EOs from the precedence graph.

        Returns a manifest list of ``{chapter_num, eo_number, path, rows}``.
        """
        prec = cache.precedence_graph()
        if exclude_eos:
            prec = filter_precedence_graph(prec, exclude_eos)
        tables_dir = Path(output_dir) / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)

        manifest: list[dict[str, Any]] = []
        total_rows = 0

        for eo_number, entry in sorted(
            registry.entries.items(),
            key=lambda kv: kv[1].chapter_index,
        ):
            if not entry.chapter_file:
                continue

            # Determine set of EOs to include (primary + related)
            related_eos = {eo_number}
            eo_prec = prec.get(eo_number, {})
            for der in eo_prec.get("derived_from", []):
                related_eos.add(der)
            for rw in eo_prec.get("read_with", []):
                related_eos.add(rw)

            # Remove excluded EOs from related set
            if exclude_eos:
                related_eos -= exclude_eos

            # Collect rows for ALL related EOs
            records: list[dict[str, Any]] = []
            for num in related_eos:
                records.extend(cache.by_eo(num))

            # Format combined table — primary_eo sorts chapter's EO to top
            content = self.format_table(
                records,
                registry=registry,
                name_lookup=name_lookup,
                title=f"Implementation Timeline (Includes related EOs)",
                primary_eo=eo_number,
            )

            # Filter out empty tables (if it only returns empty string, not headers)
            if not content.strip():
                continue

            num_rows = len([r for r in records if r.get("communication")])
            total_rows += num_rows

            chapter_num = entry.chapter_file.split("-")[0] if "-" in entry.chapter_file else entry.chapter_file.split(".")[0]
            out_path = tables_dir / f"{chapter_num}_implementation.md"
            out_path.write_text(content, encoding="utf-8")

            manifest.append({
                "chapter_num": chapter_num,
                "eo_number": eo_number,
                "included_eos": sorted(list(related_eos)),
                "path": str(out_path),
                "rows": num_rows,
            })

        self._logger.info(
            "Generated %d implementation tables (%d total rows)",
            len(manifest), total_rows,
        )
        return manifest

    # -- cross-EO table ------------------------------------------------------

    def generate_cross_eo(
        self,
        cache: EOImplementationCache,
        registry: EORegistry,
        output_dir: str | Path,
        name_lookup: EONameLookup | None = None,
    ) -> str:
        """Generate a table for communications that link to multiple EOs."""
        rows = [r for r in cache.load() if r.get("communication") and len(r.get("cross_eo", [])) > 0]
        # Keep only records that have a communication block
        rows = [r for r in rows if r.get("communication")]
        rows.sort(key=lambda r: _parse_date_loose(r["communication"].get("date", "")) or datetime.max)

        content = self.format_table(rows, registry=registry, name_lookup=name_lookup, title="Cross-EO Implementation Actions")

        tables_dir = Path(output_dir) / "tables"
        tables_dir.mkdir(parents=True, exist_ok=True)
        out_path = tables_dir / "cross_eo_actions.md"
        out_path.write_text(content, encoding="utf-8")

        return str(out_path)

    # -- generic formatter ---------------------------------------------------

    def format_table(
        self,
        records: list[dict[str, Any]],
        registry: EORegistry | None = None,
        name_lookup: EONameLookup | None = None,
        title: str = "",
        primary_eo: str = "",
        exclude_eos: set[str] | None = None,
    ) -> str:
        """Format a list of LinkageRecords into a Markdown table.

        Columns: Date | EO Linkage | EO Name | Official / Department |
                 Action / Decision | Source |
                 Media (Spin) | Media (Factualness) | Media (Convergence) | Media (Agenda)

        When *primary_eo* is set, rows for that EO appear first (marked
        with a blue indicator) followed by linked EO rows.  Within each
        group rows are sorted earliest → latest.
        """
        # Keep only records that have a communication block
        rows = [r for r in records if r.get("communication")]
        # Exclude filtered EOs
        if exclude_eos:
            rows = [r for r in rows if r.get("eo_number") not in exclude_eos]
        if not rows:
            return ""

        # Sort: primary-EO rows first, then others; within each group → earliest→latest
        def _sort_key(r: dict) -> tuple:
            is_primary = 0 if (primary_eo and r.get("eo_number") == primary_eo) else 1
            dt = _parse_date_loose(r["communication"].get("date", "")) or datetime.max
            return (is_primary, dt)

        rows.sort(key=_sort_key)

        header = (
            "| Date | EO Linkage | EO Name | Official / Department | "
            "Action / Decision | Source | "
            "Media (Spin) | Media (Factualness) | Media (Convergence) | Media (Agenda) |\n"
            "|------|-----------|---------|----------------------|"
            "-------------------|--------|"
            "-------------|-------------------|---------------------|----------------|\n"
        )
        body_lines: list[str] = []
        # Dedup by record_id
        seen_ids: set[str] = set()

        for rec in rows:
            rid = rec.get("record_id", "")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            comm = rec["communication"]
            date_str = _normalise_date(comm.get("date", ""))
            eo_year = rec.get("eo_date", "")[:4] if rec.get("eo_date") else ""

            linkage = _eo_apa_cite(rec["eo_number"], eo_year, registry, name_lookup)
            eo_name = _eo_name(rec["eo_number"], name_lookup, registry) or rec.get("eo_title", "") or "—"

            official = comm.get("official", "—") or "—"
            action = comm.get("action_description", "") or "—"
            url = comm.get("url", "")
            domain = comm.get("domain", "")
            source = f"[{domain}]({url})" if url and domain else domain or "—"

            # Visual marker: primary EO rows get a blue indicator
            if primary_eo and rec.get("eo_number") == primary_eo:
                marker = "🔵 "
            else:
                marker = ""

            body_lines.append(
                f"| {marker}{date_str} | {linkage} | {eo_name} | {official} | "
                f"{action} | {source} | "
                f"— | — | — | — |"
            )

        return header + "\n".join(body_lines) + "\n"

    # -- helper --------------------------------------------------------------

    def _build_table_from_records(self, records: list[dict[str, Any]],
                                   registry: EORegistry | None = None,
                                   name_lookup: EONameLookup | None = None) -> str:
        """Build a markdown table from a list of pre-sorted records."""
        return self.format_table(records, registry=registry, name_lookup=name_lookup)

# ---------------------------------------------------------------------------
# Phase 7 — Chapter Integration
# ---------------------------------------------------------------------------

class ChapterEnricher(Base):
    """Inject implementation sections into EO book chapters."""

    SECTION_HEADER = "## Implementation Record"

    def __init__(
        self,
        registry: EORegistry,
        cache: EOImplementationCache,
        diagram_gen: EODiagramGenerator,
        table_gen: ImplementationTableGenerator,
        name_lookup: EONameLookup | None = None,
        exclude_eos: set[str] | None = None,
    ) -> None:
        super().__init__()
        self.registry = registry
        self.cache = cache
        self.diagram_gen = diagram_gen
        self.table_gen = table_gen
        self.name_lookup = name_lookup
        self.exclude_eos = exclude_eos or set()

    def enrich_chapters(self, chapters_dir: str | Path) -> None:
        """Process all markdown chapters and inject implementation records.

        Iterates through the *already loaded* chapters in the registry to find
        valid target files, then updates them in place.
        """
        base_dir = Path(chapters_dir)
        if not base_dir.is_dir():
            self._logger.error("Chapters directory not found: %s", base_dir)
            return

        # Pre-calculate precedence graph (dict form) for diagrams
        # The cache returns dict already: {eo: {rescinds: [], ...}}
        precedence_graph = self.cache.precedence_graph()

        updated_count = 0

        # Iterate over known EO entries that have a chapter file
        for eo_number, entry in self.registry.entries.items():
            if not entry.chapter_file:
                continue

            chapter_path = base_dir / entry.chapter_file
            if not chapter_path.exists():
                self._logger.warning("Chapter file missing: %s", chapter_path)
                continue

            self._logger.info("Enriching chapter for EO %s: %s",
                            eo_number, entry.chapter_file)

            # 1. Generate Diagram (Mermaid)
            # Use static helper from diagram generator
            merged_graph = self.diagram_gen._merge_supercessions(precedence_graph, self.registry)
            if self.exclude_eos:
                merged_graph = filter_precedence_graph(merged_graph, self.exclude_eos)

            diagram_body = self.diagram_gen.generate(
                eo_number, merged_graph, self.registry,
                name_lookup=self.name_lookup,
            )

            # Wrap in mermaid block
            diagram_md = f"```mermaid\n{diagram_body}\n```"

            # 2. Generate Table — include related EOs with primary shading
            related_eos = {eo_number}
            eo_prec = precedence_graph.get(eo_number, {})
            for der in eo_prec.get("derived_from", []):
                related_eos.add(der)
            for rw in eo_prec.get("read_with", []):
                related_eos.add(rw)

            # Remove excluded EOs from related set
            if self.exclude_eos:
                related_eos -= self.exclude_eos

            records: list[dict[str, Any]] = []
            for num in related_eos:
                records.extend(self.cache.by_eo(num))

            table_md = self.table_gen.format_table(
                records,
                registry=self.registry,
                name_lookup=self.name_lookup,
                primary_eo=eo_number,
            )
            if not table_md:
                 table_md = "_No official communications linked to this EO yet._"

            # 3. Construct Section
            implementation_section = (
                f"\n\n{self.SECTION_HEADER}\n\n"
                f"### EO Precedence\n\n"
                f"{diagram_md}\n\n"
                f"### Official Communications Timeline\n\n"
                f"{table_md}\n"
            )

            # 4. Update File
            try:
                original_content = chapter_path.read_text(encoding="utf-8")

                if self.SECTION_HEADER in original_content:
                    # Replace existing section - assume it is at the end
                    parts = original_content.split(self.SECTION_HEADER)
                    new_content = parts[0].rstrip() + implementation_section
                else:
                    # Append to end
                    new_content = original_content.rstrip() + implementation_section

                chapter_path.write_text(new_content, encoding="utf-8")
                updated_count += 1
            except Exception as e:
                self._logger.error("Failed to update chapter %s: %s", chapter_path.name, e)

        self._logger.info("Enriched %d chapters with implementation records", updated_count)
