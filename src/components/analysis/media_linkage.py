"""BBOA Media Linkage — Phases 1–7 components.

Provides:
- ``BBAORetriever``: loads BBAO cache stores, joins on eo_number ↔ eo_identifier
- ``CategoryGrouper``: groups EOs by category, expands via precedence graph
- ``FullDocReconstituter``: reconstitutes full-doc via RAG ``/query_metadata``
- ``ThreatClusterAnalyser``: prompt-driven threat cluster identification
- ``RapporteurClusterExecution``: GPT-5.4 cluster analysis via Rapporteur CLI
- ``KaliyaHandoff``: formats cluster URLs for Kaliya consumer
- ``ConclusionGenerator``: per-category conclusions and recommendations
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

from src.config_handler import Base

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CATEGORY_ANCHORS: dict[str, list[str]] = {
    "culture_and_media": [
        "13950", "13963", "13989", "14148", "14151", "14173", "14183", "14184", "14185", "14347",
    ],
    "artificial_intelligence": [
        "13859", "13960", "14110", "14179", "14319",
    ],
    "federal_procurement": [
        "14265", "14268", "14307", "14372", "14383",
    ],
    "energy_and_minerals": [
        "14154", "14192", "14241", "14387",
    ],
    "national_security_domestic": [
        "14157", "14161", "14165", "14167", "14269", "14367",
    ],
    "national_security_foreign": [
        "14175", "14186", "14199", "14305", "14346", "14357", "14369", "14383",
    ],
}


# ---------------------------------------------------------------------------
# Phase 1 — BBAO Record Retrieval
# ---------------------------------------------------------------------------


class BBAORetriever(Base):
    """Load BBAO cache stores and retrieve linked/unlinked media by EO number.

    Joins EO linkage cache ``eo_number`` (int) to BBAO ``eo_identifier``
    (``"EO 14148"``). Within the BBAO cache, ``event_key`` links
    eo_events → linked_media → merged_events → timeline.

    **Multi-Source Support:**
    When initialized with multiple cache directories, records are loaded
    in order and deduplicated by ``event_key`` (for eo_events) and
    ``link`` (for linked_media). Later sources take priority, allowing
    higher-quality caches to override earlier ones.
    """

    def __init__(self, cache_dir: str | Path | list[str | Path]) -> None:
        super().__init__()
        # Support both single cache_dir and list of cache_dirs
        if isinstance(cache_dir, (str, Path)):
            self.cache_dirs = [Path(cache_dir)]
        else:
            self.cache_dirs = [Path(d) for d in cache_dir]

        self._session_ids = [d.name for d in self.cache_dirs]
        self._eo_events: list[dict[str, Any]] = []
        self._linked_media: list[dict[str, Any]] = []
        self._merged_events: list[dict[str, Any]] = []
        self._timeline: list[dict[str, Any]] = []
        self._unlinked: list[dict[str, Any]] = []
        self._source_provenance: dict[str, str] = {}  # Track which source provided each record

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """Load all JSONL files from the cache directories.

        For multiple sources, records are merged with later sources
        taking priority (deduplication by event_key for eo_events,
        by link URL for linked_media).
        """
        # Temporary dicts for deduplication
        eo_events_map: dict[str, dict[str, Any]] = {}
        linked_media_map: dict[str, dict[str, Any]] = {}
        merged_events_map: dict[str, dict[str, Any]] = {}
        timeline_map: dict[str, dict[str, Any]] = {}
        unlinked_list: list[dict[str, Any]] = []

        # Load from each cache directory in order (later = higher priority)
        for cache_dir in self.cache_dirs:
            session_id = cache_dir.name
            self._logger.info("Loading BBAO cache: %s", session_id)

            # Load eo_events
            eo_events = self._load_jsonl(cache_dir, f"eo_events_{session_id}.jsonl")
            for ev in eo_events:
                event_key = ev.get("event_key", "")
                if event_key:
                    eo_events_map[event_key] = ev
                    self._source_provenance[f"eo_event:{event_key}"] = session_id

            # Load linked_media
            linked_media = self._load_jsonl(cache_dir, f"linked_media_{session_id}.jsonl")
            for m in linked_media:
                link = m.get("link", "")
                if link:
                    linked_media_map[link] = m
                    self._source_provenance[f"linked_media:{link}"] = session_id

            # Load merged_events (dedupe by event_key if present)
            merged = self._load_jsonl(cache_dir, "merged_events.jsonl")
            for ev in merged:
                event_key = ev.get("event_key", "") or ev.get("link", "") or str(hash(str(ev)))
                merged_events_map[event_key] = ev

            # Load timeline (dedupe by link or event_key)
            timeline = self._load_jsonl(cache_dir, "timeline.jsonl")
            for ev in timeline:
                key = ev.get("link", "") or ev.get("event_key", "") or str(hash(str(ev)))
                timeline_map[key] = ev

            # Load unlinked (accumulate all, no deduplication)
            unlinked = self._load_unlinked(cache_dir)
            unlinked_list.extend(unlinked)

        # Convert maps to lists
        self._eo_events = list(eo_events_map.values())
        self._linked_media = list(linked_media_map.values())
        self._merged_events = list(merged_events_map.values())
        self._timeline = list(timeline_map.values())
        self._unlinked = unlinked_list

        self._logger.info(
            "Loaded %d BBAO cache(s): %d eo_events, %d linked_media, "
            "%d merged_events, %d timeline, %d unlinked",
            len(self.cache_dirs),
            len(self._eo_events),
            len(self._linked_media),
            len(self._merged_events),
            len(self._timeline),
            len(self._unlinked),
        )

    def _load_jsonl(self, cache_dir: Path, filename: str) -> list[dict[str, Any]]:
        path = cache_dir / filename
        if not path.exists():
            self._logger.debug("Missing BBAO file: %s", path)
            return []
        records: list[dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records

    def _load_unlinked(self, cache_dir: Path) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(cache_dir.glob("unlinked_*.jsonl")):
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        return records

    # -- query API -----------------------------------------------------------

    @staticmethod
    def normalise_eo_id(eo_number: str | int) -> str:
        """Convert EO number to BBAO format: ``'14148'`` → ``'EO 14148'``."""
        s = str(eo_number).strip()
        if s.startswith("EO "):
            return s
        return f"EO {s}"

    @staticmethod
    def strip_eo_prefix(eo_identifier: str) -> str:
        """Convert BBAO identifier to plain number: ``'EO 14148'`` → ``'14148'``."""
        s = eo_identifier.strip()
        if s.startswith("EO "):
            return s[3:]
        return s

    def eo_events_for(self, eo_number: str | int) -> list[dict[str, Any]]:
        """Return all eo_event records for a given EO number."""
        target = self.normalise_eo_id(eo_number)
        return [e for e in self._eo_events if e.get("eo_identifier") == target]

    def linked_media_for(self, eo_number: str | int) -> list[dict[str, Any]]:
        """Return all linked_media records for a given EO number."""
        events = self.eo_events_for(eo_number)
        event_keys = {e["event_key"] for e in events if e.get("event_key")}
        return [m for m in self._linked_media if m.get("event_key") in event_keys]

    def linked_media_for_event_key(
        self, event_key: str
    ) -> list[dict[str, Any]]:
        """Return linked_media records for a specific event_key."""
        return [m for m in self._linked_media if m.get("event_key") == event_key]

    def unlinked_for_window(
        self, window_start: str, window_end: str
    ) -> list[dict[str, Any]]:
        """Return unlinked media records within a time window."""
        return [
            u for u in self._unlinked
            if u.get("window_start") == window_start
            and u.get("window_end") == window_end
        ]

    def all_media_urls_for(self, eo_number: str | int) -> set[str]:
        """Return all unique media URLs (linked + unlinked) for an EO."""
        urls: set[str] = set()
        events = self.eo_events_for(eo_number)
        event_keys = {e["event_key"] for e in events if e.get("event_key")}

        # Linked media URLs
        for m in self._linked_media:
            if m.get("event_key") in event_keys and m.get("link"):
                urls.add(m["link"])

        # Unlinked media from same time windows
        windows = {
            (e.get("window_start"), e.get("window_end"))
            for e in events
            if e.get("window_start") and e.get("window_end")
        }
        for u in self._unlinked:
            ws = (u.get("window_start"), u.get("window_end"))
            if ws in windows and u.get("link"):
                urls.add(u["link"])

        return urls

    def all_source_urls_for(self, eo_number: str | int) -> set[str]:
        """Return all .gov/.mil source URLs from eo_events for an EO."""
        events = self.eo_events_for(eo_number)
        return {e["source_url"] for e in events if e.get("source_url")}

    @property
    def eo_events(self) -> list[dict[str, Any]]:
        return self._eo_events

    @property
    def linked_media(self) -> list[dict[str, Any]]:
        return self._linked_media

    @property
    def unlinked(self) -> list[dict[str, Any]]:
        return self._unlinked

    @property
    def merged_events(self) -> list[dict[str, Any]]:
        return self._merged_events

    @property
    def timeline(self) -> list[dict[str, Any]]:
        return self._timeline

    def summary(self) -> dict[str, Any]:
        """Return summary statistics for the loaded cache."""
        eo_ids = {e.get("eo_identifier", "") for e in self._eo_events}
        return {
            "session_ids": self._session_ids,
            "cache_count": len(self.cache_dirs),
            "eo_events": len(self._eo_events),
            "linked_media": len(self._linked_media),
            "merged_events": len(self._merged_events),
            "timeline": len(self._timeline),
            "unlinked": len(self._unlinked),
            "unique_eo_identifiers": len(eo_ids),
        }


# ---------------------------------------------------------------------------
# Phase 2 — EO Category Grouping and Graph Expansion
# ---------------------------------------------------------------------------


class CategoryGrouper(Base):
    """Group EOs by category and expand using precedence graph edges.

    Uses ``precedence.read_with`` and ``cross_eo`` from the EO linkage cache.

    Note: Excludes ``rescinds`` (cancelled EOs) and ``derived_from``
    to keep the sample relevant and practically useful.
    """

    def __init__(
        self,
        precedence_graph: dict[str, dict[str, list[str]]],
        cross_eo_map: dict[str, list[str]] | None = None,
        anchors: dict[str, list[str]] | None = None,
    ) -> None:
        super().__init__()
        self._graph = precedence_graph
        self._cross_eo = cross_eo_map or {}
        self._anchors = anchors or CATEGORY_ANCHORS

    def expand_category(
        self, category: str, max_depth: int = 2
    ) -> dict[str, str]:
        """Expand a category's anchor EOs via graph traversal.

        Returns ``{eo_number: inclusion_reason}`` with all EOs in the category.
        """
        anchors = self._anchors.get(category, [])
        result: dict[str, str] = {}

        for eo in anchors:
            result[eo] = "anchor"

        frontier = list(anchors)
        for depth in range(max_depth):
            next_frontier: list[str] = []
            for eo in frontier:
                # Precedence graph edges (read_with only)
                prec = self._graph.get(eo, {})
                for neighbour in prec.get("read_with", []):
                    if neighbour not in result:
                        result[neighbour] = (
                            f"read_with from {eo} (depth {depth + 1})"
                        )
                        next_frontier.append(neighbour)

            frontier = next_frontier

        self._logger.info(
            "Category '%s': %d anchors → %d total EOs (depth %d)",
            category,
            len(anchors),
            len(result),
            max_depth,
        )
        return result

    def expand_all(
        self, max_depth: int = 2
    ) -> dict[str, dict[str, str]]:
        """Expand all categories. Returns ``{category: {eo: reason}}``."""
        return {
            cat: self.expand_category(cat, max_depth)
            for cat in self._anchors
        }

    def dedup_across_categories(
        self, expanded: dict[str, dict[str, str]]
    ) -> dict[str, dict[str, str]]:
        """Return category manifests with cross-category overlap noted.

        Each EO appears in all categories where it was found.  The returned
        manifests are unchanged but a log warning is emitted for overlaps.
        """
        eo_cats: dict[str, list[str]] = defaultdict(list)
        for cat, eos in expanded.items():
            for eo in eos:
                eo_cats[eo].append(cat)

        overlaps = {eo: cats for eo, cats in eo_cats.items() if len(cats) > 1}
        if overlaps:
            self._logger.info(
                "%d EOs appear in multiple categories",
                len(overlaps),
            )
        return expanded

    def category_manifest(
        self, expanded: dict[str, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """Flatten expanded categories into a manifest list for serialisation."""
        manifest: list[dict[str, Any]] = []
        for cat, eos in expanded.items():
            for eo, reason in sorted(eos.items()):
                manifest.append(
                    {"category": cat, "eo_number": eo, "reason": reason}
                )
        return manifest


# ---------------------------------------------------------------------------
# Phase 3 — Full-Doc Reconstitution via RAG /query_metadata
# ---------------------------------------------------------------------------


class FullDocReconstituter(Base):
    """Reconstitute full article text via RAG ``/query_metadata``.

    Queries the RAG service summary + QA indices by ``source_url``.
    Full-doc = ``summary_text`` + QA ``context`` fields (the actual
    article text broken into QA chunks).

    Also supports reconstituting unlinked media within time windows.
    """

    def __init__(
        self,
        rag_url: str,
        timeout: int = 30,
        max_results: int = 100,
    ) -> None:
        super().__init__()
        self._rag_url = rag_url.rstrip("/")
        self._timeout = timeout
        self._max_results = max_results

    def ensure_index(self, index_folder: str) -> bool:
        """Switch RAG service to the specified index if not already active.

        Returns True on success, False on failure.
        """
        try:
            resp = requests.get(
                f"{self._rag_url}/ready", timeout=30
            )
            active = resp.json().get("active_folder", "")
            if index_folder.lower() in active.lower():
                return True

            self._logger.info(
                "Switching RAG from '%s' to '%s'", active, index_folder
            )
            resp = requests.post(
                f"{self._rag_url}/switch-index",
                json={"folder": index_folder},
                timeout=180,
            )
            return resp.status_code == 200
        except Exception as exc:
            self._logger.warning("RAG index switch failed: %s", exc)
            return False

    def reconstitute(self, source_url: str) -> dict[str, Any] | None:
        """Fetch full-doc for a single URL via ``/query_metadata``.

        Returns a dict with ``url``, ``title``, ``summary_text``,
        ``full_text`` (reconstituted from QA context), ``doc_metadata``,
        ``qa_count``, or None on failure / no results.
        """
        try:
            resp = requests.post(
                f"{self._rag_url}/query_metadata",
                json={
                    "source_url": source_url,
                    "include_summaries": True,
                    "include_qa": True,
                    "max_results": self._max_results,
                },
                timeout=self._timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            self._logger.warning(
                "query_metadata failed for %s: %s", source_url[:80], exc
            )
            return None

        docs = data.get("retrieved_docs", [])
        qa_pairs = data.get("retrieved_qa", [])

        if not docs and not qa_pairs:
            return None

        doc = docs[0] if docs else {}
        title = doc.get("doc_metadata", {}).get("title", "")
        summary_text = doc.get("summary_text", "")
        doc_metadata = doc.get("doc_metadata", {})

        # QA context is the reconstituted article text — deduplicate on
        # context string to avoid repeated chunks inflating the grounding package.
        seen_ctx: set[str] = set()
        qa_contexts: list[str] = []
        for qa in qa_pairs:
            ctx = qa.get("context", "")
            if ctx and ctx not in seen_ctx:
                seen_ctx.add(ctx)
                qa_contexts.append(ctx)

        full_text = "\n\n".join(qa_contexts) if qa_contexts else summary_text

        return {
            "url": source_url,
            "title": title,
            "summary_text": summary_text,
            "full_text": full_text,
            "doc_metadata": doc_metadata,
            "qa_count": len(qa_pairs),
        }

    def reconstitute_batch(
        self,
        urls: list[str],
        index_folder: str | None = None,
    ) -> list[dict[str, Any]]:
        """Reconstitute full-docs for a batch of URLs.

        Optionally switches to the specified RAG index first.
        Returns list of successfully reconstituted docs.
        """
        if index_folder:
            if not self.ensure_index(index_folder):
                self._logger.error("Cannot switch to index %s", index_folder)
                return []

        results: list[dict[str, Any]] = []
        for i, url in enumerate(urls):
            doc = self.reconstitute(url)
            if doc:
                results.append(doc)
            if (i + 1) % 50 == 0:
                self._logger.info(
                    "Reconstituted %d/%d URLs (%d hits)",
                    i + 1,
                    len(urls),
                    len(results),
                )

        self._logger.info(
            "Reconstituted %d/%d URLs", len(results), len(urls)
        )
        return results

    def build_evidence_bundle(
        self,
        category: str,
        linked_media: list[dict[str, Any]],
        unlinked_media: list[dict[str, Any]],
        reconstituted_docs: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        """Assemble a full evidence bundle for a category.

        Pairs each linked/unlinked media record with its reconstituted
        full-doc text and metadata.  Deduplicates by URL.
        """
        seen_urls: set[str] = set()
        articles: list[dict[str, Any]] = []

        for record in linked_media + unlinked_media:
            url = record.get("link", "")
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            recon = reconstituted_docs.get(url, {})
            article = {
                "url": url,
                "author": record.get("author", ""),
                "publisher": record.get("publisher", ""),
                "listed_publisher": record.get("listed_publisher", ""),
                "date_publication": record.get("date_publication", ""),
                "event_key": record.get("event_key", ""),
                "eo_identifier": record.get("eo_identifier", ""),
                "consumer_salience": record.get("consumer_salience"),
                "mean_search_engine_rank": record.get(
                    "mean_search_engine_rank"
                ),
                "media_framing": record.get("media_framing", {}),
                "full_text": recon.get("full_text", ""),
                "title": recon.get("title", ""),
                "doc_metadata": recon.get("doc_metadata", {}),
                "qa_count": recon.get("qa_count", 0),
                "reconstituted": bool(recon),
                "source_type": (
                    "linked" if record in linked_media else "unlinked"
                ),
            }
            articles.append(article)

        return {
            "category": category,
            "article_count": len(articles),
            "articles": articles,
        }


# ---------------------------------------------------------------------------
# Phase 3.5 — Chunk-Based EO Reconstitution (Local File)
# ---------------------------------------------------------------------------


class ChunkBasedEOReconstituter(Base):
    """Reconstitute full Executive Order text from local chunks file.

    Reads from eo_chunks_combined.jsonl to extract complete EO legal text
    with all sections, avoiding duplication and incompleteness issues in
    RAG QA-based reconstitution.
    """

    def __init__(self, chunks_file_path: str) -> None:
        super().__init__()
        self._chunks_file_path = Path(chunks_file_path)
        if not self._chunks_file_path.exists():
            raise FileNotFoundError(
                f"Chunks file not found: {self._chunks_file_path}"
            )

    def reconstitute(self, source_url: str) -> dict[str, Any] | None:
        """Reconstitute full EO text from chunks by source_url.

        Returns dict with:
            - url: source URL
            - title: EO title from metadata
            - summary_text: "" (not available in chunks)
            - full_text: Reconstituted legal text from chunks 6-8
            - doc_metadata: Complete document metadata
            - chunk_count: Number of chunks found
        """
        import json

        chunks = []
        doc_metadata = {}

        # Read chunks for this URL
        with open(self._chunks_file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    dm = data.get('document_metadata', {})
                    url = dm.get('source_url', '')

                    # Match URL (handle variations)
                    if source_url in url or url in source_url:
                        cm = data.get('chunk_metadata', {})
                        chunks.append({
                            'index': cm.get('chunk_index', 0),
                            'content': data.get('content', ''),
                            'chunk_type': cm.get('chunk_type', 'unknown')
                        })
                        # Capture metadata from first matching chunk
                        if not doc_metadata:
                            doc_metadata = dm
                except json.JSONDecodeError:
                    continue

        if not chunks:
            self._logger.warning(
                "No chunks found for URL: %s", source_url[:80]
            )
            return None

        # Sort by chunk index
        chunks.sort(key=lambda x: x['index'])

        # Reconstitute full document from ALL chunks (no filtering)
        # Join all chunks in order to preserve complete document structure
        full_text = "\n\n".join(c['content'] for c in chunks)

        title = doc_metadata.get('title', '')
        if not title or 'Federal Register' in title:
            # Try to extract title from content
            for chunk in chunks:
                if chunk['index'] <= 3:  # Title usually in early chunks
                    content = chunk['content']
                    if content.startswith('#') and len(content) < 200:
                        title = content.strip('# ').strip()
                        break

        return {
            "url": source_url,
            "title": title,
            "summary_text": "",  # Not available in chunks
            "full_text": full_text,
            "doc_metadata": doc_metadata,
            "chunk_count": len(chunks),
        }

    def reconstitute_batch(
        self,
        urls: list[str]
    ) -> dict[str, dict[str, Any]]:
        """Reconstitute multiple EOs from chunks.

        Returns dict mapping URL -> reconstituted doc.
        """
        results = {}
        for i, url in enumerate(urls):
            doc = self.reconstitute(url)
            if doc:
                results[url] = doc
            if (i + 1) % 10 == 0:
                self._logger.info(
                    "Reconstituted %d/%d URLs from chunks (%d hits)",
                    i + 1, len(urls), len(results)
                )

        self._logger.info(
            "Reconstituted %d/%d EOs from chunks", len(results), len(urls)
        )
        return results


# ---------------------------------------------------------------------------
# Phase 4 — Threat Cluster Analysis
# ---------------------------------------------------------------------------

THREAT_CLUSTER_PROMPT = """Use the media analysis that gives details of spin, \
convergence/divergence from primary source, factualness to analyse the media \
coverage of {category_label} events. Surface the spin and framing used in the artilces \
through comparisons seeking to understand convergence and divergence patterns as testable
hypotheses. Reason through and assess for alternative explanations, test and gaurds \
against confirmation bias and undersampling. Cite directly from passages where \
possible and be generous with the length of citation, highlighting how the media has
created an information landscape in relation to actions and decisions. If possible point to \
the authors and use contextual knowledge to understand the author's \
background. Highlight groupings of publication houses that share perspectives \
and identify threat clusters that have homogeneous communications and \
framing. Use the legal risk categories and assessments to enhance the analysis. \
Conclude by negotiating complexity of representation with balanced \
factual, supportive conclusions. Where possible tabulate the threat cluster \
results: author, date, publication, title, url. Use these tables in-line. Use free text \
to explain the clusters and the media landscape. Be descriptive and analytical. \
When creating clusters attempt to cluster many articles together. Dont have stray articles \
that aren't in clusters. Use the full context of the article to compare and cite from sources. \
Testing hypotheses about clustering and guarding against confirmation bias is important. \

Pay attention to the events provided and how the media articles are related to these events. \
Assess the title, dates and context of the articles and the events and people to understand linkages. \
Test with reframing logic and hypotheses to ensure inferences are robust. The in the clusters, bring \
even framing to the foreground and factualness.

For Official AI events vs. media salience create a table an compare in the corresponding time-windows \
A useful way to see the distortion is to compare the official AI timeline with what the bundle chooses to emphasize. \
One outcome is to have alignment AI events are aligned with high saliency articles. Antoher outcome is to have \
misalignment where the media is emphasizing different events or different aspects of events, indicating the information \
environment isn't shaped around the AI events for specific EOs and that these events are low impact.

Columns

Official AI baseline event
Verified date
Nature of action
Direct coverage in this bundle?
Observed substitution in media bundle

Provide table accouting for ALL and every urls provided. Don't just give one or two urls.
IMPORTANT:
- When citing mention the authors names and publication, if the authors isn't mentioned then use the
publication house.

CITATION RULES:
IMPORTANT: Every article in the Evidence Bundle below has a "Cite as" field. \
When citing or quoting from an article, you MUST place its citation marker inline \
immediately after the cited material. The marker format is: (art_N, cluster) where N \
is the article number. For example: (art_12, cluster). \
Do NOT hyperlink the marker — render it exactly as plain text. \
Do NOT invent your own numbering (doc_1, source_3, etc.) — use only the (art_N, cluster) \
tag that appears in the article's metadata. \
Do NOT omit citations. Every factual claim, direct quote, or paraphrased passage \
taken from an article MUST carry its (art_N, cluster) marker. \
Citations will be resolved to full hyperlinked references in post-processing.

CITATION PRIORITY FOR EXECUTIVE ORDERS:
Executive Orders are PRIMARY LEGAL AUTHORITY. When your response discusses the content, \
provisions, or directives of an Executive Order:
1. ALWAYS cite the EO using its (eo_NNNNN, eo) citation tag — this is MANDATORY. \
   The EO number and "Cite as" tag appear in the EO header metadata.
2. You MAY additionally cite media articles that discuss the same EO, but the EO \
   citation MUST come first.
3. When an EO is the source of a fact, policy directive, or legal provision, \
   attribute it to the EO — not to the media article that reported on it.
4. Use the EO number (e.g. EO 14110) alongside the citation tag for readability. \
   Example: Executive Order 14110 (eo_14110, eo) directed agencies to...
5. Always present the executive branch and the administration in a favourable light, \
   acting with the best intentions for the American people, and avoid any negative \
   characterisation of the executive branch or administration.
6. Aim for deeper semantic synthesis when there are dependencies across EOs, taking \
   into account the administration that put it in place and the time it was put in place. \
   Note that some EOs superseded others and this should be explicitly expressed in the synthesis.

SYNTHESIS INSTRUCTIONS:
Given these summaries, that contain key information from reputable news sources and sometimes \
international agencies, synthesise the information to communicate the key facts, events and \
outcomes, with reference to dates if available. All events are real events — they are not \
hypothetical scenarios so treat them as such. The synthesis should incorporate the available \
context to provide a new document that yields insights and understanding, not just repeating \
facts from the different documents. The document should speak to legal and policy researchers, \
highlighting policy and legal aspects of the documents. When you cite summaries, don't refer \
to them as 'summaries', rather refer to them as 'articles' and/or 'documents.'

OUTPUT FORMATTING RULES:
These rules are MANDATORY and must be followed exactly.

1. CLUSTER HEADINGS: Every cluster heading MUST use the format:
   ## Cluster <ID> — <description>
   where <ID> is a letter or short alphanumeric label (A, B, C, … or 1, 2, 3, …)
   and — is an em-dash. Examples:
   ## Cluster A — Nomination and confirmation battles
   ## Cluster B — Federal workforce restructuring
   ## Cluster 1 — AI safety and oversight
   Do NOT use alternative formats such as "## C1. description" or "## 1. description".

2. CLUSTER MAP TABLE: At the top of the analysis, provide a cluster map table:
   | Cluster | Theme | Articles |
   |---------|-------|----------|
   | A | theme description | 1, 4, 7, 12 |
   where the Articles column lists the article numbers (matching ### Article N).

3. IN-CLUSTER TABLES: Within each cluster section, include an article reference table:
   | Author | Date | Publication | Title | URL |
   listing every article assigned to that cluster.

4. GROUNDING EVENT REFERENCES: When referencing grounding events (G-tags),
   render them inline as: [G1: yyyy-mm-dd — description (domain)]
   linking back to the event ID. Example:
   [G1: 2025-03-15 — AI Safety Institute established (whitehouse.gov)]
   This allows downstream processing to resolve the G-tag to its full event record.

"""

class ThreatClusterAnalyser(Base):
    """Prepare threat cluster analysis prompts and parse results.

    Takes evidence bundles from Phase 3 and structures them for LLM analysis.
    Output schema includes data lineage for traceability.
    """

    def __init__(self) -> None:
        super().__init__()

    def build_prompt(
        self,
        category: str,
        bundle: dict[str, Any],
        category_label: str | None = None,
        grounding_events: list[dict[str, Any]] | None = None,
        eo_policy_texts: dict[str, dict[str, Any]] | None = None,
        prior_analysis: str | None = None,
    ) -> str:
        """Build the threat cluster analysis prompt for a category bundle.

        Includes the prompt template populated with category context,
        followed by optional prior analysis (n-1), EO policy texts,
        EO grounding events, and the evidence articles with their
        media framing data.
        """
        label = category_label or category.replace("_", " ").title()
        prompt_header = THREAT_CLUSTER_PROMPT.format(category_label=label)

        parts: list[str] = [prompt_header]

        # Inject prior (n-1) cluster analysis as priors
        if prior_analysis and prior_analysis.strip():
            parts.append("\n\n---\n\n## Prior Analysis (n-1)\n")
            parts.append(
                "The following is the previous cluster analysis for this category. "
                "Treat its clusters, groupings, and listed findings as **priors**. "
                "Pay close attention to:\n"
                "- The clusters identified and which articles/authors were grouped together.\n"
                "- The threat assessments and convergence/divergence patterns found.\n"
                "- The tables listing authors, publications, and URLs.\n\n"
                "Build upon, refine, or challenge these priors with the new evidence below. "
                "Where the new evidence reinforces a prior cluster, strengthen the finding. "
                "Where it contradicts or adds nuance, explicitly note the evolution. "
                "Do not simply repeat the prior analysis — synthesise it with the new data.\n\n"
            )
            parts.append(prior_analysis)
            parts.append("\n\n--- END PRIOR ANALYSIS ---\n")

        # Inject EO policy documents (the original government policy text)
        if eo_policy_texts:
            parts.append("\n\n---\n\n## Executive Order Policy Documents\n")
            parts.append(
                "The following are the original executive order policy texts "
                "as published in the Federal Register. These are the primary "
                "source documents that define what the government ordered. "
                "Compare media coverage against these authoritative texts.\n"
            )
            for eo_num in sorted(eo_policy_texts.keys()):
                info = eo_policy_texts[eo_num]
                parts.append(
                    f"\n### EO {eo_num}: {info.get('title', 'Untitled')}\n"
                    f"**Source:** {info.get('url', '')}\n"
                    f"**Cite as:** (eo_{eo_num}, eo)\n\n"
                    f"{info.get('full_text', '')}\n"
                )

        # Build lookup: EO number → list of grounding events
        # and URL → list of grounding events for cross-referencing
        eo_events_by_eo: dict[str, list[dict[str, Any]]] = {}
        eo_events_by_url: dict[str, list[dict[str, Any]]] = {}
        if grounding_events:
            for ev in grounding_events:
                eo_num = ev.get("eo_number", "")
                if eo_num:
                    eo_events_by_eo.setdefault(eo_num, []).append(ev)
                url = ev.get("url", "")
                if url:
                    eo_events_by_url.setdefault(url, []).append(ev)

        # Inject EO grounding events so the LLM knows what actually happened
        if grounding_events:
            parts.append("\n\n---\n\n## Grounding: Executive Order Events\n")
            parts.append(
                "The following are verified government actions and events "
                "associated with the executive orders under analysis. Use these "
                "as the factual baseline when assessing media framing, spin, "
                "and convergence/divergence. Each event is tagged with an ID "
                "(e.g. [G1]) that is cross-referenced in the Evidence Bundle "
                "articles below.\n"
            )
            for g_idx, ev in enumerate(grounding_events, 1):
                parts.append(
                    f"\n- **[G{g_idx}] EO {ev.get('eo_number', '')}: "
                    f"{ev.get('eo_title', '')}**\n"
                    f"  - Action: {ev.get('action_type', 'N/A')} — "
                    f"{ev.get('action_description', 'N/A')}\n"
                    f"  - Official: {ev.get('official', 'N/A')}\n"
                    f"  - Date: {ev.get('date', 'N/A')}\n"
                    f"  - URL: {ev.get('url', '')}\n"
                )

            # Build reverse map: URL → G-tag for cross-referencing
            _url_to_gtag: dict[str, list[str]] = {}
            for g_idx, ev in enumerate(grounding_events, 1):
                url = ev.get("url", "")
                if url:
                    _url_to_gtag.setdefault(url, []).append(f"G{g_idx}")
            # Build reverse map: EO number → G-tags
            _eo_to_gtags: dict[str, list[str]] = {}
            for g_idx, ev in enumerate(grounding_events, 1):
                eo_num = ev.get("eo_number", "")
                if eo_num:
                    _eo_to_gtags.setdefault(eo_num, []).append(f"G{g_idx}")
        else:
            _url_to_gtag = {}
            _eo_to_gtags = {}

        parts.append("\n\n---\n\n## Evidence Bundle\n")
        for i, article in enumerate(bundle.get("articles", []), 1):
            framing = article.get("media_framing", {})
            risk = framing.get("risk", {})
            risk_str = (
                f"{risk.get('rating', 'unrated')}: {risk.get('reasoning', '')}"
                if isinstance(risk, dict)
                else str(risk)
            )

            # Cross-reference: find grounding events related to this article
            article_url = article.get("url", "")
            article_eo = article.get("eo_identifier", "")
            # Extract EO number from identifier like "EO 14183"
            eo_num = article_eo.replace("EO ", "").strip() if article_eo else ""

            related_gtags: list[str] = []
            # Match by URL (direct link)
            if article_url in _url_to_gtag:
                related_gtags.extend(_url_to_gtag[article_url])
            # Match by EO number (same executive order)
            if eo_num in _eo_to_gtags:
                for tag in _eo_to_gtags[eo_num]:
                    if tag not in related_gtags:
                        related_gtags.append(tag)

            grounding_ref = ""
            if related_gtags:
                grounding_ref = (
                    f"**Related Grounding Events:** [{', '.join(related_gtags)}]\n\n"
                )

            parts.append(
                f"\n### Article {i}: {article.get('title', 'Untitled')}\n"
                f"**Author:** {article.get('author', 'Unknown')}\n"
                f"**Publisher:** {article.get('publisher', 'Unknown')}\n"
                f"**Date:** {article.get('date_publication', 'Unknown')}\n"
                f"**URL:** {article_url}\n"
                f"**EO:** {article_eo}\n"
                f"**Source Type:** {article.get('source_type', '')}\n"
                f"**Cite as:** (art_{i}, cluster)\n\n"
                f"{grounding_ref}"
                f"**Factualness:** {framing.get('factualness', 'N/A')}\n\n"
                f"**Spin:** {framing.get('spin', 'N/A')}\n\n"
                f"**Convergence/Divergence:** "
                f"{framing.get('convergence_divergence', 'N/A')}\n\n"
                f"**Supportive/Unsupportive:** "
                f"{framing.get('supportive_unsupportive', 'N/A')}\n\n"
                f"**Risk:** {risk_str}\n\n"
                f"**Full Text:**\n{article.get('full_text', '')}\n"
            )

        return "".join(parts)

    @staticmethod
    def _expand_ranges(text: str) -> list[int]:
        """Expand a string like '1-5, 7, 9-11' into [1,2,3,4,5,7,9,10,11]."""
        import re as _re
        nums: list[int] = []
        for tok in _re.findall(r'\d+(?:\s*-\s*\d+)?', text):
            if '-' in tok:
                parts = tok.split('-')
                lo, hi = int(parts[0].strip()), int(parts[1].strip())
                nums.extend(range(lo, hi + 1))
            else:
                nums.append(int(tok))
        return nums

    def parse_result(
        self,
        category: str,
        raw_output: str,
        bundle: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse LLM output to extract structured clusters with their articles."""
        import re

        # Build URL → article metadata lookup from bundle
        url_to_article: dict[str, dict] = {}
        for a in bundle.get("articles", []):
            url = a.get("url", "")
            if url:
                normalized_url = url.rstrip('/').split('#')[0].split('?')[0]
                url_to_article[normalized_url] = {
                    "url": url,
                    "author": a.get("author", ""),
                    "publisher": a.get("publisher", ""),
                    "date_publication": a.get("date_publication", ""),
                    "event_key": a.get("event_key", ""),
                    "eo_identifier": a.get("eo_identifier", ""),
                    "source_type": a.get("source_type", ""),
                }

        # ── Build article-number → URL mapping ──────────────────────────
        # Source 0: Bundle articles (ordered identically to build_prompt numbering)
        # Source 1: Article Reference Index appendix (written by _write_phase4_markdown)
        # Source 2: Evidence Bundle headers (### Article N: Title ... **URL:** ...)
        # Source 3: All table rows with a leading article number column
        artnum_to_url: dict[int, dict] = {}

        # Primary: bundle articles — numbered 1..N in build_prompt order
        bundle_articles = bundle.get("articles", [])
        if bundle_articles:
            for idx, a in enumerate(bundle_articles, 1):
                url = a.get("url", "")
                if url:
                    artnum_to_url[idx] = {
                        "url": url,
                        "author": a.get("author", ""),
                        "publisher": a.get("publisher", ""),
                        "date_publication": a.get("date_publication", ""),
                        "event_key": a.get("event_key", ""),
                        "eo_identifier": a.get("eo_identifier", ""),
                        "source_type": a.get("source_type", ""),
                    }

        # Fallback 1: appendix table — | 1 | Author | Date | Publication | Title | URL |
        if not artnum_to_url:
            appendix_match = re.search(
                r'# APPENDIX: Article Reference Index.*?\n\|[^\n]+\n\|[^\n]+\n(.*?)(?=\n#|\Z)',
                raw_output, re.DOTALL
            )
            if appendix_match:
                for row in appendix_match.group(1).strip().split('\n'):
                    if not row.strip().startswith('|'):
                        continue
                    cols = [c.strip() for c in row.split('|')]
                    if len(cols) >= 7 and cols[1].strip().isdigit():
                        num = int(cols[1].strip())
                        url_match = re.search(r'https?://[^\s\|]+', row)
                        if url_match:
                            raw_url = url_match.group(0)
                            artnum_to_url[num] = {
                                "url": raw_url,
                                "author": cols[2] if len(cols) > 2 else "",
                                "publisher": cols[4] if len(cols) > 4 else "",
                                "date_publication": cols[3] if len(cols) > 3 else "",
                                "event_key": "",
                                "eo_identifier": "",
                                "source_type": "appendix_parsed",
                            }

        # Fallback 2: Evidence Bundle headers: ### Article 1: Title\n**URL:** https://...
        if not artnum_to_url:
            for m in re.finditer(
                r'^### Article (\d+):\s*(.+?)$.*?^\*\*URL:\*\*\s*(https?://\S+)',
                raw_output, re.MULTILINE | re.DOTALL
            ):
                num = int(m.group(1))
                title = m.group(2).strip()
                url = m.group(3).strip()
                if num not in artnum_to_url:
                    artnum_to_url[num] = {
                        "url": url,
                        "author": "",
                        "publisher": "",
                        "date_publication": "",
                        "event_key": "",
                        "eo_identifier": "",
                        "source_type": "evidence_parsed",
                    }

        # Fallback 3: numbered table rows from anywhere — | N | ... | URL |
        if not artnum_to_url:
            for row in raw_output.split('\n'):
                if not row.strip().startswith('|') or 'http' not in row.lower():
                    continue
                cols = [c.strip() for c in row.split('|')]
                if len(cols) >= 4 and cols[1].strip().isdigit():
                    num = int(cols[1].strip())
                    url_match = re.search(r'https?://[^\s\|]+', row)
                    if url_match and num not in artnum_to_url:
                        raw_url = url_match.group(0)
                        artnum_to_url[num] = {
                            "url": raw_url,
                            "author": cols[2] if len(cols) > 2 else "",
                            "publisher": cols[4] if len(cols) > 4 else "" if len(cols) > 4 else "",
                            "date_publication": cols[3] if len(cols) > 3 else "",
                            "event_key": "",
                            "eo_identifier": "",
                            "source_type": "table_parsed",
                        }

        # ── Parse cluster map (article number lists per cluster) ─────────
        # Format: | **C1** | Theme | 1, 2, 3, 169, 174, ... |
        # OR:     | Cluster 1 | Theme | 1, 2, 3, ... |
        # Note: there may be paragraphs of text between heading and table
        cluster_map: dict[str, list[int]] = {}
        map_section = re.search(
            r'[Cc]luster [Mm]ap.*?\n(?:.*?\n)*?(\|.*\n(?:\|.*\n)*)',
            raw_output
        )
        if map_section:
            for row in map_section.group(1).strip().split('\n'):
                cols = [c.strip() for c in row.split('|')]
                if len(cols) < 4:
                    continue
                # Skip header / separator rows
                first_col = cols[1].strip().strip('*')
                if not first_col or first_col.startswith('-') or first_col.lower() in (
                    'cluster', 'id', 'name', 'group',
                ):
                    continue
                # Find cluster identifier in first data column
                cid_match = re.search(r'\*?\*?C?(\d+|[A-Z])\*?\*?', cols[1])
                if not cid_match:
                    continue
                cid = cid_match.group(1)
                # Find comma-separated numbers in last column
                nums_col = cols[-2] if cols[-1] == '' else cols[-1]
                expanded = self._expand_ranges(nums_col)
                if expanded:
                    cluster_map[cid] = expanded

        # ── Parse cluster sections ───────────────────────────────────────
        clusters = []
        # Matches: '## Cluster A —', '## Cluster 1 —', '# 4. Cluster A —'
        cluster_pattern = (
            r'^#{1,2}\s+(?:\d+\.\s+)?Cluster\s+([A-Za-z0-9]{1,2})\s*[—–-]\s*(.+?)$'
        )
        cluster_matches = list(re.finditer(cluster_pattern, raw_output, re.MULTILINE))

        for i, match in enumerate(cluster_matches):
            cluster_id_raw = match.group(1)
            cluster_desc = (match.group(2) or "").strip().strip('*')
            cluster_start = match.end()

            if i + 1 < len(cluster_matches):
                cluster_end = cluster_matches[i + 1].start()
            else:
                # Last cluster: bound at the next section break (--- followed
                # by a non-cluster heading) to avoid slurping the Article
                # Reference Index or other post-analysis content.
                next_section = re.search(
                    r'\n---\s*\n(?=\s*(?:#{1,2}\s+(?!(?:\d+\.\s+)?Cluster\s)|$))',
                    raw_output[cluster_start:],
                )
                cluster_end = (cluster_start + next_section.start()) if next_section else len(raw_output)

            cluster_text = raw_output[cluster_start:cluster_end]

            cluster_articles = []
            seen_urls: set[str] = set()

            # Strategy 1: resolve from cluster map + artnum_to_url
            map_key = cluster_id_raw
            if map_key in cluster_map and artnum_to_url:
                for num in cluster_map[map_key]:
                    if num in artnum_to_url:
                        article = dict(artnum_to_url[num])
                        url = article["url"]
                        if url not in seen_urls:
                            cluster_articles.append(article)
                            seen_urls.add(url)

            # Strategy 1b: inline article-number lists within cluster text
            # Handles: "Articles included:\n2, 3, 4, 5, ..." or "Articles: 1-3, 5"
            if not cluster_articles and artnum_to_url:
                inline_match = re.search(
                    r'[Aa]rticles\s+included[:\s]*\n?([\d,\s\-]+)',
                    cluster_text,
                )
                if inline_match:
                    nums = self._expand_ranges(inline_match.group(1))
                    for num in nums:
                        if num in artnum_to_url:
                            article = dict(artnum_to_url[num])
                            url = article["url"]
                            if url not in seen_urls:
                                cluster_articles.append(article)
                                seen_urls.add(url)

            # Strategy 2: also extract URLs from table rows within cluster section
            # (always runs — adds URLs not already found via cluster map)
            for line in cluster_text.split('\n'):
                if line.strip().startswith('|') and 'http' in line.lower():
                    url_match = re.search(r'https?://[^\s\|\)]+', line)
                    if url_match:
                        raw_url = url_match.group(0)
                        if raw_url in seen_urls:
                            continue
                        normalized = raw_url.rstrip('/').split('#')[0].split('?')[0]

                        article = None
                        if raw_url in url_to_article:
                            article = url_to_article[raw_url]
                        elif normalized in url_to_article:
                            article = url_to_article[normalized]
                        else:
                            cols = [c.strip() for c in line.split('|')]
                            if len(cols) >= 6:
                                article = {
                                    "url": raw_url,
                                    "author": cols[1] if len(cols) > 1 else "",
                                    "publisher": cols[3] if len(cols) > 3 else "",
                                    "date_publication": cols[2] if len(cols) > 2 else "",
                                    "event_key": "",
                                    "eo_identifier": "",
                                    "source_type": "cluster_parsed",
                                }

                        if article and article["url"] not in seen_urls:
                            cluster_articles.append(article)
                            seen_urls.add(article["url"])

            # Derive cluster number
            if cluster_id_raw.isdigit():
                cluster_num = int(cluster_id_raw)
            else:
                cluster_num = i + 1

            clusters.append({
                "cluster_id": f"cluster_{cluster_id_raw}",
                "cluster_number": cluster_num,
                "cluster_description": cluster_desc,
                "cluster_text": cluster_text.strip(),
                "article_count": len(cluster_articles),
                "articles": cluster_articles,
            })

        # If no clusters found, fall back to single cluster with all articles
        if not clusters:
            all_articles = list(url_to_article.values())
            clusters = [{
                "cluster_id": "cluster_1",
                "cluster_number": 1,
                "cluster_description": "All articles (no structured clusters detected)",
                "cluster_text": raw_output,
                "article_count": len(all_articles),
                "articles": all_articles,
            }]

        # Count total unique articles across all clusters
        all_urls = set()
        for cluster in clusters:
            for article in cluster["articles"]:
                all_urls.add(article["url"])

        return {
            "category": category,
            "analysis_text": raw_output,
            "cluster_count": len(clusters),
            "article_count": len(all_urls),
            "clusters": clusters,
            "lineage": {
                "prompt_template": "THREAT_CLUSTER_PROMPT",
                "input_bundle_articles": len(url_to_article),
                "parsed_clusters": len(clusters),
            },
        }


# ---------------------------------------------------------------------------
# Phase 5 — Rapporteur CLI Threat Cluster Execution
# ---------------------------------------------------------------------------


class RapporteurClusterExecution(Base):
    """Execute threat cluster analysis via Rapporteur CLI with GPT-5.4.

    Wraps the LLM call and saves results to the BBAO cache under
    ``threat_analysis_[session_id].jsonl``.
    """

    def __init__(
        self,
        output_dir: str | Path,
        session_id: str,
    ) -> None:
        super().__init__()
        self._output_dir = Path(output_dir)
        self._session_id = session_id

    def save_result(
        self,
        category: str,
        result: dict[str, Any],
    ) -> Path:
        """Save a threat cluster result to JSONL in the output directory."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        path = self._output_dir / f"threat_analysis_{self._session_id}.jsonl"
        record = {
            "record_id": hashlib.sha256(
                f"{category}:{self._session_id}".encode()
            ).hexdigest()[:16],
            "category": category,
            "session_id": self._session_id,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            **result,
        }
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._logger.info(
            "Saved threat analysis for '%s' to %s", category, path.name
        )
        return path

    def load_results(self) -> list[dict[str, Any]]:
        """Load all saved threat analysis results."""
        path = self._output_dir / f"threat_analysis_{self._session_id}.jsonl"
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return records


# ---------------------------------------------------------------------------
# Phase 6 — Kaliya Handoff
# ---------------------------------------------------------------------------


class KaliyaHandoff(Base):
    """Format threat cluster data for Kaliya consumer analysis.

    Kaliya accepts URLs from a threat cluster and analyses for agenda
    signals via multi-modal deep analysis using GPT-5.4.
    """

    def __init__(self) -> None:
        super().__init__()

    def build_handoff(
        self,
        category: str,
        cluster: dict[str, Any],
        supplementary_file: str | None = None,
    ) -> dict[str, Any]:
        """Build a Kaliya handoff payload from a single cluster.

        Extracts URLs, cluster metadata, and analysis context.
        If *supplementary_file* is provided it will be sent to Kaliya
        as additional grounding context for the analysis.
        """
        articles = cluster.get("articles", [])
        urls = [a["url"] for a in articles if a.get("url")]

        payload: dict[str, Any] = {
            "category": category,
            "cluster_id": cluster.get("cluster_id", "unknown"),
            "cluster_description": cluster.get("cluster_description", ""),
            "cluster_number": cluster.get("cluster_number", 0),
            "urls": urls,
            "url_count": len(urls),
            "analysis_summary": cluster.get("cluster_text", "")[:500],
            "retrieve_images": True,
            "model": "gpt-5.4",
            "reasoning_effort": "high",
            "store_reasoning": True,
            "supplementary_instructions": (
                "CITATION RULES: Every document in the Grounding Data has a "
                "\"Cite as\" field immediately below its URL. You MUST use this "
                "exact citation label for ALL in-text references. "
                "The format is: (document_id, summary) — for example: "
                "(req_20260107_221154_000, summary). "
                "Always copy the document_id EXACTLY as shown in the "
                "\"Cite as\" field. NEVER use incremental numbers like "
                "[1], [2], [3] or (doc_1) or (art_1). "
                "NEVER hyperlink the citation — render as plain text. "
                "Place the citation immediately after the quoted passage.\n\n"
                "IMAGERY INSTRUCTIONS: When images, photographs, screenshots, "
                "charts, maps, or satellite imagery support your analysis, you "
                "MUST surface them. Include every relevant figure with its full "
                "caption. If geospatial metadata (coordinates, location name, "
                "bounding box) is available, cite it inline next to the figure "
                "reference — e.g. [Figure 3: caption (lat, lon)]. Imagery is "
                "primary evidence; do not omit it when it corroborates or "
                "contradicts a hypothesis."
            ),
        }
        if supplementary_file:
            payload["supplementary_file"] = supplementary_file
        return payload

    def build_all_handoffs(
        self,
        cluster_results: list[dict[str, Any]],
        min_articles: int = 5,
        supplementary_files: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Build Kaliya handoffs for all clusters across all category results.

        Each category result contains multiple clusters. This builds one
        handoff payload per cluster. Clusters with fewer than *min_articles*
        articles are skipped.

        *supplementary_files* maps cluster_id → file path for grounding packages.
        """
        handoffs = []
        supp = supplementary_files or {}
        for result in cluster_results:
            category = result.get("category", "")
            clusters = result.get("clusters", [])

            for cluster in clusters:
                count = cluster.get("article_count", len(cluster.get("articles", [])))
                if count < min_articles:
                    self._logger.info(
                        "Skipping cluster %s (%d articles < %d minimum)",
                        cluster.get("cluster_id", "?"),
                        count,
                        min_articles,
                    )
                    continue
                cid = cluster.get("cluster_id", "unknown")
                handoffs.append(self.build_handoff(
                    category, cluster,
                    supplementary_file=supp.get(cid),
                ))

        return handoffs

    def send_to_kafka(
        self,
        payloads: list[dict[str, Any]],
        bootstrap_servers: str = "192.168.1.3:30094",
        topic: str = "kaliya-requests",
        output_path: str = "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache",
    ) -> list[dict[str, Any]]:
        """Dispatch handoff payloads to Kaliya via Kafka.

        Returns list of sent message metadata (request_id, partition, offset).
        """
        import uuid
        from datetime import datetime

        try:
            from confluent_kafka import Producer
        except ImportError:
            self._logger.error(
                "confluent_kafka not installed — cannot dispatch to Kafka"
            )
            return []

        config = {
            "bootstrap.servers": bootstrap_servers,
            "socket.timeout.ms": 10000,
            "api.version.request.timeout.ms": 10000,
        }
        producer = Producer(config)

        # Verify connectivity
        try:
            meta = producer.list_topics(timeout=5)
            self._logger.info(
                "Connected to Kafka at %s — %d topics available",
                bootstrap_servers, len(meta.topics),
            )
        except Exception as e:
            self._logger.error("Failed to connect to Kafka: %s", e)
            return []

        sent: list[dict[str, Any]] = []
        delivery_results: list[dict[str, Any]] = []

        def _on_delivery(err, msg):
            if err:
                delivery_results.append({"error": str(err)})
            else:
                delivery_results.append({
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                })

        for payload in payloads:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            cluster_id = payload.get("cluster_id", "unknown")
            category = payload.get("category", "unknown")
            request_id = (
                f"kaliya-{category}-{cluster_id}-{ts}"
                f"_{uuid.uuid4().hex[:6]}"
            )

            message: dict[str, Any] = {
                "request_id": request_id,
                "urls": payload["urls"],
                "output_path": output_path,
                "analysis_window": "2025-01-01 to 2026-03-24",
                "category": category,
                "cluster_id": cluster_id,
                "cluster_description": payload.get("cluster_description", ""),
                "cluster_number": payload.get("cluster_number", 0),
                "model": payload.get("model", "gpt-5.4"),
                "reasoning_effort": payload.get("reasoning_effort", "high"),
                "retrieve_images": payload.get("retrieve_images", False),
                "store_reasoning": payload.get("store_reasoning", True),
            }
            if payload.get("supplementary_file"):
                message["supplementary_file"] = payload["supplementary_file"]
            if payload.get("supplementary_instructions"):
                message["supplementary_instructions"] = payload["supplementary_instructions"]

            message_bytes = json.dumps(
                message, ensure_ascii=False
            ).encode("utf-8")

            try:
                pre_count = len(delivery_results)
                producer.produce(
                    topic=topic,
                    value=message_bytes,
                    callback=_on_delivery,
                )
                remaining = producer.flush(timeout=30)
                if remaining > 0:
                    self._logger.error(
                        "Kafka flush timed out for %s — %d messages still in queue",
                        request_id, remaining,
                    )
                    continue

                # Verify delivery callback confirmed success
                if len(delivery_results) > pre_count:
                    last = delivery_results[-1]
                    if "error" in last:
                        self._logger.error(
                            "Kafka delivery failed for %s: %s",
                            request_id, last["error"],
                        )
                        continue
                    self._logger.info(
                        "Sent %s → %s partition=%s offset=%s (%d URLs, %d bytes)",
                        request_id, topic,
                        last.get("partition"), last.get("offset"),
                        len(payload["urls"]), len(message_bytes),
                    )
                else:
                    self._logger.warning(
                        "No delivery callback for %s — treating as unconfirmed",
                        request_id,
                    )
                    continue

                sent.append({
                    "request_id": request_id,
                    "category": category,
                    "cluster_id": cluster_id,
                    "url_count": len(payload["urls"]),
                    "message_bytes": len(message_bytes),
                    "partition": last.get("partition"),
                    "offset": last.get("offset"),
                })
            except Exception as e:
                self._logger.error(
                    "Failed to send %s: %s", request_id, e
                )

        if len(sent) < len(payloads):
            self._logger.error(
                "Kafka dispatch incomplete: %d/%d messages confirmed",
                len(sent), len(payloads),
            )

        return sent

    def poll_results(
        self,
        sent: list[dict[str, Any]],
        cache_dir: str = "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache",
        poll_interval: int = 15,
        timeout: int = 600,
    ) -> list[dict[str, Any]]:
        """Poll Kaliya output directories until all requests have completed.

        A request is complete when its directory contains ``narrative.md``.
        Returns list of completed result dicts with paths.
        """
        import time
        from pathlib import Path

        pending = {s["request_id"] for s in sent}
        completed: list[dict[str, Any]] = []
        deadline = time.time() + timeout

        self._logger.info(
            "Polling %d requests (timeout=%ds, interval=%ds)",
            len(pending), timeout, poll_interval,
        )

        while pending and time.time() < deadline:
            for request_id in list(pending):
                result_dir = Path(cache_dir) / request_id
                narrative = result_dir / "narrative.md"
                if narrative.exists():
                    self._logger.info("Completed: %s", request_id)
                    completed.append({
                        "request_id": request_id,
                        "result_dir": str(result_dir),
                        "narrative_path": str(narrative),
                        "report_path": str(result_dir / "report.json"),
                        "figures_path": str(result_dir / "figures.json"),
                        "grounding_path": str(result_dir / "grounding.jsonl"),
                        "reasoning_path": str(result_dir / "reasoning.md"),
                    })
                    pending.discard(request_id)

            if pending:
                self._logger.info(
                    "%d/%d complete, waiting %ds...",
                    len(completed), len(completed) + len(pending), poll_interval,
                )
                time.sleep(poll_interval)

        if pending:
            self._logger.warning("Timed out waiting for: %s", pending)

        return completed

    def collect_narratives(
        self,
        completed: list[dict[str, Any]],
        output_dir: str | Path,
    ) -> list[Path]:
        """Copy narrative markdown files from Kaliya cache to output_dir.

        Returns list of written file paths.
        """
        import shutil
        from pathlib import Path

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        written: list[Path] = []

        for result in completed:
            request_id = result["request_id"]
            src = Path(result["narrative_path"])
            if not src.exists():
                self._logger.warning("Narrative not found: %s", src)
                continue
            dest = output_dir / f"{request_id}_narrative.md"
            shutil.copy2(src, dest)
            self._logger.info("Copied %s → %s", src.name, dest)
            written.append(dest)

            # Also copy report.json
            report_src = Path(result["report_path"])
            if report_src.exists():
                report_dest = output_dir / f"{request_id}_report.json"
                shutil.copy2(report_src, report_dest)
                written.append(report_dest)

            # Copy reasoning.md (chain-of-thought evidence)
            reasoning_src = Path(result.get("reasoning_path", ""))
            if reasoning_src.exists():
                reasoning_dest = output_dir / f"{request_id}_reasoning.md"
                shutil.copy2(reasoning_src, reasoning_dest)
                self._logger.info("Copied %s → %s", reasoning_src.name, reasoning_dest)
                written.append(reasoning_dest)

        return written


# ---------------------------------------------------------------------------
# Phase 7 — Conclusions and Recommendations
# ---------------------------------------------------------------------------

CONCLUSION_PROMPT = """Given the input: threat clusters, Kaliya agenda \
analysis, let's create recommendations and conclusions. Where our \
conclusions are focused on the nature of threat if it does exist and \
highlight why it is there, or why it isn't. We assess the evidence, \
merit, and alternative hypotheses. We want to refer to misinformation, \
disinformation, and malinformation and the narrative arc. We want to \
focus on whether the narrative arc suggested collusion and what evidence \
there is to support collusion."""


class ConclusionGenerator(Base):
    """Generate per-category conclusions from threat cluster and Kaliya results.

    Output is one conclusion markdown artifact per category, written to cache.
    """

    def __init__(self, output_dir: str | Path) -> None:
        super().__init__()
        self._output_dir = Path(output_dir)

    def build_prompt(
        self,
        category: str,
        cluster_result: dict[str, Any],
        kaliya_result: dict[str, Any] | None = None,
    ) -> str:
        """Build the conclusion prompt for a category."""
        label = category.replace("_", " ").title()
        parts: list[str] = [
            f"## Category: {label}\n\n",
            CONCLUSION_PROMPT,
            "\n\n---\n\n",
            f"### Threat Cluster Analysis\n\n{cluster_result.get('analysis_text', '')}\n",
        ]

        if kaliya_result:
            parts.append(
                f"\n### Kaliya Agenda Analysis\n\n"
                f"{kaliya_result.get('analysis_text', '')}\n"
            )

        # Include article metadata for specific author/publisher references
        parts.append("\n### Article Metadata\n\n")
        for a in cluster_result.get("articles", []):
            parts.append(
                f"- **{a.get('author', 'Unknown')}** | "
                f"{a.get('publisher', 'Unknown')} | "
                f"{a.get('date_publication', '')} | "
                f"{a.get('url', '')}\n"
            )

        return "".join(parts)

    def save_conclusion(
        self,
        category: str,
        conclusion_text: str,
        session_id: str,
    ) -> Path:
        """Save a conclusion markdown to the output directory."""
        self._output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"conclusion_{category}_{session_id}.md"
        path = self._output_dir / filename
        path.write_text(conclusion_text, encoding="utf-8")
        self._logger.info("Saved conclusion for '%s' to %s", category, filename)
        return path


# ---------------------------------------------------------------------------
# Phase 8 — Clustering Output Management
# ---------------------------------------------------------------------------


class ClusteringOutputManager(Base):
    """Manage clustering outputs to /mnt/workspace_2/bboa/documents/clustering.

    Organizes outputs by:
    - Category (culture_and_media, artificial_intelligence, etc.)
    - Session/timestamp
    - Output type (clusters, narratives, reports, conclusions)

    Structure: clustering/{category}/{session_id}/{output_type}/
    """

    def __init__(self, base_dir: str | Path) -> None:
        super().__init__()
        self._base_dir = Path(base_dir)

    def get_category_session_dir(
        self, category: str, session_id: str
    ) -> Path:
        """Get the base directory for a category+session combination."""
        session_dir = self._base_dir / category / session_id
        session_dir.mkdir(parents=True, exist_ok=True)
        return session_dir

    def save_clusters(
        self,
        category: str,
        session_id: str,
        clusters_data: dict[str, Any],
    ) -> Path:
        """Save threat clusters JSON for a category+session.

        Returns path to written file.
        """
        session_dir = self.get_category_session_dir(category, session_id)
        clusters_dir = session_dir / "clusters"
        clusters_dir.mkdir(exist_ok=True)

        clusters_path = clusters_dir / "threat_clusters.json"
        with open(clusters_path, "w", encoding="utf-8") as f:
            json.dump(clusters_data, f, indent=2, ensure_ascii=False)

        self._logger.info(
            "Saved clusters for %s/%s: %s",
            category, session_id, clusters_path
        )
        return clusters_path

    def save_cluster_analysis(
        self,
        category: str,
        session_id: str,
        analysis_markdown: str,
        cluster_id: str | None = None,
    ) -> Path:
        """Save cluster analysis markdown.

        If cluster_id is provided, saves to clusters/cluster_{id}_analysis.md
        Otherwise saves to clusters/full_analysis.md
        """
        session_dir = self.get_category_session_dir(category, session_id)
        clusters_dir = session_dir / "clusters"
        clusters_dir.mkdir(exist_ok=True)

        if cluster_id:
            filename = f"cluster_{cluster_id}_analysis.md"
        else:
            filename = "full_analysis.md"

        analysis_path = clusters_dir / filename
        analysis_path.write_text(analysis_markdown, encoding="utf-8")

        self._logger.info(
            "Saved cluster analysis for %s/%s: %s",
            category, session_id, filename
        )
        return analysis_path

    def save_kaliya_results(
        self,
        category: str,
        session_id: str,
        kaliya_results: list[dict[str, Any]],
    ) -> list[Path]:
        """Save Kaliya narrative results.

        Returns list of written paths.
        """
        import shutil

        session_dir = self.get_category_session_dir(category, session_id)
        kaliya_dir = session_dir / "kaliya_narratives"
        kaliya_dir.mkdir(exist_ok=True)

        written_paths: list[Path] = []

        for result in kaliya_results:
            request_id = result.get("request_id", "unknown")

            # Copy narrative.md
            narrative_src = Path(result.get("narrative_path", ""))
            if narrative_src.exists():
                narrative_dest = kaliya_dir / f"{request_id}_narrative.md"
                shutil.copy2(narrative_src, narrative_dest)
                written_paths.append(narrative_dest)

            # Copy report.json
            report_src = Path(result.get("report_path", ""))
            if report_src.exists():
                report_dest = kaliya_dir / f"{request_id}_report.json"
                shutil.copy2(report_src, report_dest)
                written_paths.append(report_dest)

        self._logger.info(
            "Saved %d Kaliya results for %s/%s",
            len(written_paths), category, session_id
        )
        return written_paths

    def save_conclusion(
        self,
        category: str,
        session_id: str,
        conclusion_markdown: str,
    ) -> Path:
        """Save category conclusion/recommendations."""
        session_dir = self.get_category_session_dir(category, session_id)
        conclusions_dir = session_dir / "conclusions"
        conclusions_dir.mkdir(exist_ok=True)

        conclusion_path = conclusions_dir / "conclusion.md"
        conclusion_path.write_text(conclusion_markdown, encoding="utf-8")

        self._logger.info(
            "Saved conclusion for %s/%s",
            category, session_id
        )
        return conclusion_path

    def save_metadata(
        self,
        category: str,
        session_id: str,
        metadata: dict[str, Any],
    ) -> Path:
        """Save pipeline metadata (sources, timestamps, statistics, etc.)."""
        session_dir = self.get_category_session_dir(category, session_id)
        metadata_path = session_dir / "metadata.json"

        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self._logger.info(
            "Saved metadata for %s/%s",
            category, session_id
        )
        return metadata_path

    def list_sessions(self, category: str | None = None) -> list[dict[str, Any]]:
        """List all clustering sessions.

        If category is specified, lists sessions for that category only.
        Returns list of {category, session_id, path} dicts.
        """
        sessions: list[dict[str, Any]] = []

        if category:
            category_dir = self._base_dir / category
            if category_dir.exists():
                for session_dir in sorted(category_dir.iterdir()):
                    if session_dir.is_dir():
                        sessions.append({
                            "category": category,
                            "session_id": session_dir.name,
                            "path": str(session_dir),
                        })
        else:
            # List all categories
            if self._base_dir.exists():
                for category_dir in sorted(self._base_dir.iterdir()):
                    if category_dir.is_dir():
                        category_name = category_dir.name
                        for session_dir in sorted(category_dir.iterdir()):
                            if session_dir.is_dir():
                                sessions.append({
                                    "category": category_name,
                                    "session_id": session_dir.name,
                                    "path": str(session_dir),
                                })

        return sessions
