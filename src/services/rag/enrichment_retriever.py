"""
EnrichmentRetriever — Phase 2

Given a set of primary result URLs (from the summary or QA index), retrieves
matching enrichment records from the event-linked and unlinked BBOA indices.

Lookup strategy (REQ 2 / REQ 3):
    1. Try event-linked map first (richer — contains event, EO, media, risk).
    2. Fall back to unlinked map if no linked record found.
    3. If neither map has the URL, skip gracefully (REQ 4).

Filtering by enrichment type (REQ 5):
    Accepted values: "events", "executive_orders", "media", "risk", "all"

    Actual schema (confirmed from data):
      media_framing keys: factualness, spin, convergence_divergence,
                          supportive_unsupportive, linked_to_dow, risk
      media_framing.risk: null  OR  {"rating": "HIGH|MEDIUM|LOW", "reasoning": "..."}
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Optional

# Ensure the project root is on sys.path so src.* imports work whether this
# module is loaded from the rag service, from tests, or from the project root.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config_handler import Base  # noqa: E402

# Valid enrichment type tokens
VALID_ENRICH_TYPES = frozenset({"events", "executive_orders", "media", "risk", "all"})


def _normalise_url(url: str) -> str:
    """Inline normaliser — mirrors main.py:normalise_url without a circular import."""
    import re
    if not url:
        return ""
    normalised = re.sub(r"^https?://", "", url.strip().lower())
    normalised = normalised.split("#")[0]
    normalised = normalised.split("?")[0]
    return normalised.rstrip("/")


class EnrichmentRetriever(Base):
    """
    Look up BBOA enrichment records for a set of primary article URLs and
    return a filtered, deduplicated list ready for LLM grounding or frontend
    display.

    Inherits centralised logging (``self._logger``) and config
    (``self._app_config``) from ``Base``.
    """

    def __init__(
        self,
        linked_map: Dict[str, List[Dict]],
        unlinked_map: Dict[str, List[Dict]],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._linked = linked_map
        self._unlinked = unlinked_map
        # Secondary index: event_key → all linked records sharing that event.
        # Built once at construction so get_linked expansion is O(1) per key.
        self._event_index: Dict[str, List[Dict]] = {}
        for recs in linked_map.values():
            for rec in recs:
                ek = rec.get("event_key")
                if ek:
                    self._event_index.setdefault(ek, []).append(rec)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def lookup(
        self,
        urls: List[str],
        enrich_types: Optional[List[str]] = None,
        get_linked: bool = False,
    ) -> List[Dict]:
        """
        Return enrichment records for *urls*, filtered by *enrich_types*.

        By default only unlinked records are returned.  Pass
        ``get_linked=True`` to also include event-linked records (tried first;
        unlinked is the fallback for URLs not found in the linked map).

        Args:
            urls: Article URLs from primary retrieval results.
            enrich_types: Subset filter.  Defaults to ["all"] (no filtering).
            get_linked: When True, linked records are included and take
                priority over unlinked for the same URL.  Defaults to False.

        Returns:
            Flat list of enrichment record dicts, deduped and filtered.
        """
        if enrich_types is None:
            enrich_types = ["all"]

        raw = self._collect(urls, get_linked=get_linked)
        deduped = self._dedup(raw)
        return self.filter_by_type(deduped, enrich_types)

    def filter_by_type(
        self,
        records: List[Dict],
        enrich_types: List[str],
    ) -> List[Dict]:
        """
        Filter *records* to those matching any of the requested *enrich_types*.

        Type semantics:
            "all"              — return all records unchanged
            "events"           — record has a non-empty event_descriptor
            "executive_orders" — record has a non-empty eo_identifier
            "media"            — record has a non-null media_framing block
            "risk"             — record has media_framing.risk that is non-null
        """
        types = set(enrich_types)

        if "all" in types:
            return list(records)

        out = []
        for rec in records:
            if self._matches_any_type(rec, types):
                out.append(rec)
        return out

    def format_for_grounding(self, records: List[Dict]) -> str:
        """
        Produce a structured text block suitable for appending to an LLM prompt.

        Each record is rendered as a labelled section.  Records with event data
        are distinguished from unlinked records.  The block is clearly delimited
        so the LLM can separate it from primary grounding.
        """
        if not records:
            return ""

        lines: List[str] = [
            "### Enrichment Context (BBOA Media Analysis)",
            "",
            "The following additional context has been retrieved from the media "
            "analysis pipeline and may be used to supplement the primary sources above.",
            "",
        ]

        for i, rec in enumerate(records, 1):
            is_linked = bool(rec.get("event_descriptor"))
            label = "Event-Linked Article" if is_linked else "Unlinked Media Article"
            lines.append(f"#### {i}. {label}")

            # Source
            link = rec.get("link", "N/A")
            publisher = rec.get("publisher", "Unknown")
            author = rec.get("author", "Unknown")
            date = rec.get("date_publication", "N/A")
            salience = rec.get("consumer_salience")
            time_window = rec.get("time_window", "N/A")

            lines.append(f"- **Source:** {link}")
            lines.append(f"- **Publisher:** {publisher} | **Author:** {author} | **Date:** {date}")
            lines.append(f"- **Time window:** {time_window}")
            if salience is not None:
                lines.append(f"- **Salience score:** {salience:.4f}")

            # Event data (linked only)
            event_desc = rec.get("event_descriptor")
            eo = rec.get("eo_identifier")
            if event_desc:
                lines.append(f"- **Event:** {event_desc}")
            if eo:
                lines.append(f"- **Executive Order:** {eo}")

            # Media framing
            mf = rec.get("media_framing")
            if mf and isinstance(mf, dict):
                factualness = mf.get("factualness")
                spin = mf.get("spin")
                convergence = mf.get("convergence_divergence")
                supportive = mf.get("supportive_unsupportive")
                risk_block = mf.get("risk")

                if factualness:
                    lines.append(f"- **Factualness:** {str(factualness)[:300]}")
                if spin:
                    lines.append(f"- **Spin / Framing:** {str(spin)[:300]}")
                if convergence:
                    lines.append(f"- **Convergence/Divergence:** {str(convergence)[:300]}")
                if supportive:
                    lines.append(f"- **Stance:** {str(supportive)[:300]}")
                if risk_block and isinstance(risk_block, dict):
                    rating = risk_block.get("rating", "UNKNOWN")
                    reasoning = risk_block.get("reasoning", "")
                    lines.append(f"- **Risk Rating:** {rating}")
                    if reasoning:
                        lines.append(f"- **Risk Reasoning:** {str(reasoning)[:400]}")

            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _collect(self, urls: List[str], get_linked: bool = False) -> List[Dict]:
        """
        For each URL attempt unlinked lookup (and optionally linked-first).

        When *get_linked* is False (default) the linked map is skipped
        entirely — only unlinked records are returned.

        When *get_linked* is True the linked map is tried first.  On a hit,
        the matched record's ``event_key`` is used to expand the result to
        **all** linked articles for that event, not just the URL-matched ones.
        The unlinked map is used as a fallback only when no linked record is
        found for a URL.

        Returns a flat (potentially duplicate) list of all matched records.
        """
        collected: List[Dict] = []
        for url in urls:
            key = _normalise_url(url)
            if not key:
                continue

            if get_linked:
                linked_hits = self._linked.get(key)
                if linked_hits:
                    # Expand to all linked records for each matched event_key.
                    event_keys = {rec.get("event_key") for rec in linked_hits if rec.get("event_key")}
                    expanded: List[Dict] = []
                    for ek in event_keys:
                        event_recs = self._event_index.get(ek, [])
                        expanded.extend(event_recs)
                        self._logger.debug(
                            f"Enrichment linked expand: event_key={ek} → {len(event_recs)} record(s)"
                        )
                    if expanded:
                        collected.extend(expanded)
                        continue
                    # No event_key on matched records — fall through to direct hits.
                    collected.extend(linked_hits)
                    continue

            unlinked_hits = self._unlinked.get(key)
            if unlinked_hits:
                self._logger.debug(f"Enrichment unlinked: {key} ({len(unlinked_hits)} record(s))")
                collected.extend(unlinked_hits)
            else:
                self._logger.debug(f"Enrichment no hit: {key}")

        return collected

    def _dedup(self, records: List[Dict]) -> List[Dict]:
        """
        Deduplicate by (normalised link, event_key, time_window).

        The same article may appear multiple times if two primary docs share
        the same enrichment URL.  Using all three fields as the key ensures:
          - identical records are collapsed (REQ 4 dedup)
          - same article in different time windows is preserved
          - same article linked to different events is preserved
        We keep the first occurrence of each unique triplet.
        """
        seen: set = set()
        out: List[Dict] = []
        for rec in records:
            link_key = _normalise_url(rec.get("link", ""))
            event_key = rec.get("event_key") or ""
            time_window = rec.get("time_window") or ""
            dedup_key = (link_key, event_key, time_window)
            if dedup_key not in seen:
                seen.add(dedup_key)
                out.append(rec)
        return out

    @staticmethod
    def _matches_any_type(rec: Dict, types: set) -> bool:
        """Return True if *rec* satisfies at least one of the requested types."""
        if "events" in types and rec.get("event_descriptor"):
            return True

        if "executive_orders" in types and rec.get("eo_identifier"):
            return True

        mf = rec.get("media_framing")

        if "media" in types and mf:
            return True

        if "risk" in types and isinstance(mf, dict) and mf.get("risk"):
            return True

        return False
