#!/usr/bin/env python3
"""
demo_enrichment_retriever.py — standalone demo for EnrichmentRetriever.

Loads the linked + unlinked BBOA enrichment indices from the project root,
picks a handful of real URLs from the indices, then exercises each
enrich_type filter and prints the formatted grounding block.

Run from the project root:
    python demo_enrichment_retriever.py [--type all|events|executive_orders|media|risk]
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List

# ---------------------------------------------------------------------------
# Path bootstrap — must happen before any src.* import
# File lives at src/demo/demo_enrichment_retriever.py, so climb three levels.
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

RAG_DIR = PROJECT_ROOT / "src" / "services" / "rag"

from src.services.rag.enrichment_retriever import \
    EnrichmentRetriever  # noqa: E402

# ---------------------------------------------------------------------------
# Minimal index loader (mirrors main.py:load_enrichment_index)
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
_log = logging.getLogger(__name__)


def _normalise(url: str) -> str:
    if not url:
        return ""
    n = re.sub(r"^https?://", "", url.strip().lower())
    n = n.split("#")[0].split("?")[0]
    return n.rstrip("/")


def load_index(path: Path) -> Dict[str, List[Dict]]:
    lookup: Dict[str, List[Dict]] = {}
    malformed = 0
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            key = _normalise(rec.get("link", ""))
            if key:
                lookup.setdefault(key, []).append(rec)
    if malformed:
        _log.warning("%s: skipped %d malformed lines", path.name, malformed)
    _log.info("Loaded %-55s  %4d unique URLs", path.name, len(lookup))
    return lookup


def find_index(prefix: str) -> Path | None:
    """Return the most-recent JSONL matching *prefix*_*.jsonl in project root."""
    candidates = sorted(PROJECT_ROOT.glob(f"{prefix}_*.jsonl"), reverse=True)
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Demo EnrichmentRetriever")
    parser.add_argument(
        "--type",
        dest="enrich_type",
        default="all",
        choices=["all", "events", "executive_orders", "media", "risk"],
        help="Enrichment type filter to demonstrate (default: all)",
    )
    parser.add_argument(
        "--get-linked", action="store_true", default=True,
        help="Include event-linked records (default: True)",
    )
    parser.add_argument(
        "--sample", type=int, default=5,
        help="Number of sample URLs to query (default: 5)",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Discover and load indices
    # ------------------------------------------------------------------
    linked_path = find_index("linked_media_bboa_full")
    unlinked_path = find_index("unlinked_media_bboa_full")

    if not linked_path and not unlinked_path:
        sys.exit(
            "No enrichment index files found in project root.\n"
            "Expected files matching linked_media_bboa_full_*.jsonl "
            "and/or unlinked_media_bboa_full_*.jsonl"
        )

    linked_map: Dict[str, List[Dict]] = load_index(linked_path) if linked_path else {}
    unlinked_map: Dict[str, List[Dict]] = load_index(unlinked_path) if unlinked_path else {}

    # ------------------------------------------------------------------
    # 2. Build the retriever (uses default config → self._logger available)
    # ------------------------------------------------------------------
    retriever = EnrichmentRetriever(linked_map=linked_map, unlinked_map=unlinked_map)

    # ------------------------------------------------------------------
    # 3. Pick sample URLs — prefer from linked (richer), pad from unlinked
    # ------------------------------------------------------------------
    sample_urls: List[str] = []

    # Grab the raw 'link' value from the first record of each normalised key
    for recs in linked_map.values():
        if len(sample_urls) >= args.sample:
            break
        raw_link = recs[0].get("link", "")
        if raw_link:
            sample_urls.append(raw_link)

    if len(sample_urls) < args.sample:
        for recs in unlinked_map.values():
            if len(sample_urls) >= args.sample:
                break
            raw_link = recs[0].get("link", "")
            if raw_link and raw_link not in sample_urls:
                sample_urls.append(raw_link)

    _log.info("Querying with %d sample URL(s): enrich_type=%r  get_linked=%s",
              len(sample_urls), args.enrich_type, args.get_linked)
    for url in sample_urls:
        _log.info("  %s", url)

    # ------------------------------------------------------------------
    # 4. Retrieve and display
    # ------------------------------------------------------------------
    results = retriever.lookup(sample_urls, enrich_types=[args.enrich_type], get_linked=args.get_linked)

    print()
    print("=" * 72)
    print(f"  EnrichmentRetriever demo  |  type={args.enrich_type!r}  |  get_linked={args.get_linked}  |  hits={len(results)}")
    print("=" * 72)

    if not results:
        print("\n(No records matched the requested filter.)\n")
        return

    grounding_text = retriever.format_for_grounding(results)
    print(grounding_text)
    print()
    print(f"--- {len(results)} record(s) returned ---")


if __name__ == "__main__":
    main()
