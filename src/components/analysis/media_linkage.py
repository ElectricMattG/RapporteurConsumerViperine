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
        "14148", "14173", "13950", "14151", "14183", "14185", "14184", "14347",
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
    """

    def __init__(self, cache_dir: str | Path) -> None:
        super().__init__()
        self.cache_dir = Path(cache_dir)
        self._session_id = self.cache_dir.name
        self._eo_events: list[dict[str, Any]] = []
        self._linked_media: list[dict[str, Any]] = []
        self._merged_events: list[dict[str, Any]] = []
        self._timeline: list[dict[str, Any]] = []
        self._unlinked: list[dict[str, Any]] = []

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """Load all JSONL files from the cache directory."""
        self._eo_events = self._load_jsonl(
            f"eo_events_{self._session_id}.jsonl"
        )
        self._linked_media = self._load_jsonl(
            f"linked_media_{self._session_id}.jsonl"
        )
        self._merged_events = self._load_jsonl("merged_events.jsonl")
        self._timeline = self._load_jsonl("timeline.jsonl")
        self._unlinked = self._load_unlinked()
        self._logger.info(
            "Loaded BBAO cache %s: %d eo_events, %d linked_media, "
            "%d merged_events, %d timeline, %d unlinked",
            self._session_id,
            len(self._eo_events),
            len(self._linked_media),
            len(self._merged_events),
            len(self._timeline),
            len(self._unlinked),
        )

    def _load_jsonl(self, filename: str) -> list[dict[str, Any]]:
        path = self.cache_dir / filename
        if not path.exists():
            self._logger.warning("Missing BBAO file: %s", path)
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

    def _load_unlinked(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for path in sorted(self.cache_dir.glob("unlinked_*.jsonl")):
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
            "session_id": self._session_id,
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
                f"{self._rag_url}/ready", timeout=5
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
                timeout=30,
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

        # QA context is the reconstituted article text
        qa_contexts: list[str] = []
        for qa in qa_pairs:
            ctx = qa.get("context", "")
            if ctx:
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
IMPORTANT: When citing sources, use the "Cite as" field provided for each summary. The format is: \
(document_id, summary) — for example: (req_20260107_221154_000, summary). Always include the exact \
document_id and the word "summary" separated by a comma. Do not use the actual source meta-data for \
in-line citations. Do not hyperlink the in-line citation; just render it according to the format \
provided above. Never add hyperlinks to the in-line citations under any circumstances. \
IMPORTANT: Never render with doc_1 or doc_3, never use incremental numbering for the document ID. \
Always use the actual document_id from the grounding data for the in-line citation.

CITATION PRIORITY FOR EXECUTIVE ORDERS:
Executive Orders are PRIMARY LEGAL AUTHORITY. When your response discusses the content, \
provisions, or directives of an Executive Order:
1. ALWAYS cite the EO using its (document_id, eo) citation tag — this is MANDATORY. \
   The document_id and "Cite as" tag appear in the EO header metadata.
2. You MAY additionally cite media articles that discuss the same EO, but the EO \
   citation MUST come first.
3. When an EO is the source of a fact, policy directive, or legal provision, \
   attribute it to the EO — not to the media article that reported on it.
4. Use the EO number (e.g. EO 14110) alongside the citation tag for readability. \
   Example: Executive Order 14110 (doc_abc123, eo) directed agencies to...
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
                    f"**Source:** {info.get('url', '')}\n\n"
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
                f"**Source Type:** {article.get('source_type', '')}\n\n"
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

    def parse_result(
        self,
        category: str,
        raw_output: str,
        bundle: dict[str, Any],
    ) -> dict[str, Any]:
        """Parse LLM output to extract structured clusters with their articles."""
        import re

        # Build URL → article metadata lookup from bundle
        url_to_article = {}
        for a in bundle.get("articles", []):
            url = a.get("url", "")
            if url:
                # Normalize URL for matching (strip trailing slashes, fragments)
                normalized_url = url.rstrip('/').split('#')[0].split('?')[0]
                url_to_article[normalized_url] = {
                    "url": url,  # Keep original URL from bundle
                    "author": a.get("author", ""),
                    "publisher": a.get("publisher", ""),
                    "date_publication": a.get("date_publication", ""),
                    "event_key": a.get("event_key", ""),
                    "eo_identifier": a.get("eo_identifier", ""),
                    "source_type": a.get("source_type", ""),
                }

        # Parse clusters from the markdown output
        clusters = []

        # Find all cluster headers: # Cluster N — Description  or  # Cluster A — Description
        cluster_pattern = r'^# Cluster ([A-Za-z0-9]+) — (.+?)$'
        cluster_matches = list(re.finditer(cluster_pattern, raw_output, re.MULTILINE))

        for i, match in enumerate(cluster_matches):
            cluster_id_raw = match.group(1)
            cluster_desc = match.group(2).strip()
            cluster_start = match.end()

            # Find the end of this cluster (start of next cluster or end of text)
            if i + 1 < len(cluster_matches):
                cluster_end = cluster_matches[i + 1].start()
            else:
                cluster_end = len(raw_output)

            cluster_text = raw_output[cluster_start:cluster_end]

            # Parse markdown table to extract URLs
            # Find table section (starts with | Author | Date... and has URL column)
            table_rows = []
            for line in cluster_text.split('\n'):
                # Match markdown table rows (|...|...|URL|)
                if line.strip().startswith('|') and 'http' in line.lower():
                    table_rows.append(line)

            # Extract URLs from table rows
            cluster_articles = []
            seen_urls = set()

            for row in table_rows:
                # Extract URL from last column (URL is rightmost)
                url_match = re.search(r'https?://[^\s\|\)]+', row)
                if url_match:
                    raw_url = url_match.group(0)
                    # Normalize for lookup
                    normalized = raw_url.rstrip('/').split('#')[0].split('?')[0]

                    # Try exact match first, then normalized match
                    article = None
                    if raw_url in url_to_article:
                        article = url_to_article[raw_url]
                    elif normalized in url_to_article:
                        article = url_to_article[normalized]
                    else:
                        # URL not in bundle - create minimal metadata from table row
                        # Parse table columns: | Author | Date | Publication | Title | URL |
                        cols = [c.strip() for c in row.split('|')]
                        if len(cols) >= 6:  # Has at least Author, Date, Publication, Title, URL
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

            # Derive cluster number: numeric if digit, ordinal position if letter
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
    ) -> dict[str, Any]:
        """Build a Kaliya handoff payload from a single cluster.

        Extracts URLs, cluster metadata, and analysis context.
        Does not retrieve related images (per plan constraints).
        """
        articles = cluster.get("articles", [])
        urls = [a["url"] for a in articles if a.get("url")]

        return {
            "category": category,
            "cluster_id": cluster.get("cluster_id", "unknown"),
            "cluster_description": cluster.get("cluster_description", ""),
            "cluster_number": cluster.get("cluster_number", 0),
            "urls": urls,
            "url_count": len(urls),
            "analysis_summary": cluster.get("cluster_text", "")[:500],
            "retrieve_images": False,
            "model": "gpt-5.4",
            "reasoning_effort": "high",
        }

    def build_all_handoffs(
        self,
        cluster_results: list[dict[str, Any]],
        min_articles: int = 10,
    ) -> list[dict[str, Any]]:
        """Build Kaliya handoffs for all clusters across all category results.

        Each category result contains multiple clusters. This builds one
        handoff payload per cluster. Clusters with fewer than *min_articles*
        articles are skipped.
        """
        handoffs = []
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
                handoffs.append(self.build_handoff(category, cluster))

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

            message = {
                "request_id": request_id,
                "urls": payload["urls"],
                "output_path": f"{output_path}/{request_id}",
                "analysis_window": "2025-01-01 to 2026-03-24",
                "category": category,
                "cluster_id": cluster_id,
                "cluster_description": payload.get("cluster_description", ""),
                "cluster_number": payload.get("cluster_number", 0),
                "model": payload.get("model", "gpt-5.4"),
                "reasoning_effort": payload.get("reasoning_effort", "high"),
                "retrieve_images": payload.get("retrieve_images", False),
            }

            message_bytes = json.dumps(
                message, ensure_ascii=False
            ).encode("utf-8")

            try:
                producer.produce(
                    topic=topic,
                    value=message_bytes,
                    callback=_on_delivery,
                )
                producer.flush(timeout=10)

                sent.append({
                    "request_id": request_id,
                    "category": category,
                    "cluster_id": cluster_id,
                    "url_count": len(payload["urls"]),
                    "message_bytes": len(message_bytes),
                })
                self._logger.info(
                    "Sent %s → %s (%d URLs, %d bytes)",
                    request_id, topic,
                    len(payload["urls"]), len(message_bytes),
                )
            except Exception as e:
                self._logger.error(
                    "Failed to send %s: %s", request_id, e
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
