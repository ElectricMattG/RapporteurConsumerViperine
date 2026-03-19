"""EO Event Linking — Phase 1 components.

Provides:
- ``GovMilExtractor``: filters cross-index article catalog to .gov/.mil domains
- ``EORegistry``: loads EO book chapters, events, and supercession graph
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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
