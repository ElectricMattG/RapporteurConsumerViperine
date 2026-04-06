#!/usr/bin/env python3
"""
demo_media_linkage.py — standalone demo for BBOA Media Linkage (Phases 1–7).

URLs are the common data linking every phase:
  Phase 1: .gov/.mil source URLs + media URLs retrieved from BBAO cache
  Phase 2: EO graph expansion surfaces more EOs → more URLs via Phase 1
  Phase 3: URLs sent to RAG /query_metadata → full-doc reconstituted
  Phase 4: URLs embedded in threat cluster prompt as evidence
  Phase 5: URLs persisted in threat_analysis JSONL with data lineage
  Phase 6: URLs handed off to Kaliya for agenda analysis
  Phase 7: URLs cited in per-category conclusions

Run from the project root:
    python -m src.demo.demo_media_linkage                     # all phases, dry run
    python -m src.demo.demo_media_linkage --phase 1           # Phase 1 only
    python -m src.demo.demo_media_linkage --phase 3 --live    # Phase 1-3 (needs RAG)
    python -m src.demo.demo_media_linkage --category culture_and_media
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import tempfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.components.analysis.article_retriever import \
    ArticleRetriever  # noqa: E402
from src.components.analysis.media_linkage import (  # noqa: E402
    CATEGORY_ANCHORS, BBAORetriever, CategoryGrouper, ConclusionGenerator,
    FullDocReconstituter, KaliyaHandoff, RapporteurClusterExecution,
    ThreatClusterAnalyser)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Multi-source BBOA cache (priority: last = highest quality)
BBAO_CACHE_SOURCES = [
    "/mnt/workspace_2/bboa/cache/bboa_full_20260310_194954",
    "/mnt/workspace_2/bboa/cache/bboa_trump_20260311_220355",
    "/mnt/workspace_2/bboa/cache/bboa_combined_20260326_153800",
    "/mnt/workspace_2/bboa/cache/bboa_20260327_141107",  # Latest, highest priority
]
EO_CACHE_PATH = (
    "/mnt/workspace_2/bboa/workflows/eo_linking/eo_implementation_cache_curated.jsonl"
)
MEDIA_LINKING_CACHE_PATH = (
    "/mnt/workspace_2/bboa/workflows/media_linking/media_linkage_cache.jsonl"
)
RAG_URL = "http://localhost:8000"
INDEX_FOLDERS = ["x_indices_merged"]
EO_INDEX_FOLDER = "x_indices_eo"
DEMO_SESSION_ID = "demo_media_linkage"

# LLM pricing (per 1M tokens)
# Gemini: source: ai.google.dev/gemini-api/docs/pricing
# OpenAI: source: platform.openai.com/docs/pricing
LLM_PRICING = {
    # OpenAI GPT-5 family
    "gpt-5.4":                      {"input": 2.50, "output": 10.00},
    "gpt-5.1":                      {"input": 2.50, "output": 10.00},
    "gpt-5":                        {"input": 2.00, "output":  8.00},
    "gpt-5-mini":                   {"input": 0.40, "output":  1.60},
    # Gemini family (tiered pricing: ≤200K and >200K input tokens)
    "gemini-3.1-pro-preview":       {"input": 4.00, "output": 18.00, "input_short": 2.00, "output_short": 12.00},
    "gemini-3.1-flash-lite-preview":{"input": 0.25, "output":  1.50},
    "gemini-3-flash-preview":       {"input": 0.50, "output":  3.00},
    "gemini-2.5-pro":               {"input": 2.50, "output": 15.00, "input_short": 1.25, "output_short": 10.00},
    "gemini-2.5-flash":             {"input": 0.30, "output":  2.50},
}

# Output directory for Phase 4 analysis results
WORKING_DIR = Path("/mnt/workspace_2/bboa/documents/clustering")
CACHE_DIR = PROJECT_ROOT / "cache"
KALIYA_CACHE_DIR = Path("/mnt/workspace_2/bboa/cache/kaliya_agenda_cache")

# Configure logging: suppress component loggers, only show demo output
logging.basicConfig(
    level=logging.WARNING,  # Suppress INFO from Base components
    format="%(message)s",
)
log = logging.getLogger("demo_media_linkage")
log.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hr(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"{title}")
    print(f"{'=' * 70}")


def _load_eo_cache() -> list[dict]:
    """Load the EO implementation cache JSONL."""
    records = []
    with open(EO_CACHE_PATH, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _load_media_linking_cache() -> list[dict]:
    """Load the workflow media linking cache JSONL."""
    records = []
    with open(MEDIA_LINKING_CACHE_PATH, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _extract_urls_for_eo(
    eo_cache: list[dict],
    eo_number: str,
    min_confidence: float = 0.0,
) -> tuple[set[str], dict[str, int]]:
    """Extract primary URLs for a given EO from workflow cache.

    Args:
        eo_cache: List of workflow cache records
        eo_number: EO number to filter by
        min_confidence: Minimum confidence score (0.0-1.0)

    Returns:
        (urls, stats) where stats = {"total": int, "filtered": int, "passed": int}
    """
    urls: set[str] = set()
    stats = {"total": 0, "filtered": 0, "passed": 0}

    for record in eo_cache:
        if str(record.get("eo_number", "")) == str(eo_number):
            stats["total"] += 1
            confidence = record.get("confidence", 0.0)

            if confidence < min_confidence:
                stats["filtered"] += 1
                continue

            comm = record.get("communication") or {}
            if comm.get("url"):
                urls.add(comm["url"])
                stats["passed"] += 1

    return urls, stats


def _get_eo_title(eo_cache: list[dict], eo_number: str) -> str:
    """Get the title of an EO from the workflow cache."""
    for record in eo_cache:
        if str(record.get("eo_number", "")) == str(eo_number):
            return record.get("eo_title", "")
    return "Unknown"


def _load_eo_url_map() -> dict[str, str]:
    """Build EO number → Federal Register source_url map from the EO index."""
    import re
    eo_path = Path.cwd() / EO_INDEX_FOLDER / "research_summaries_with_embeddings_combined.jsonl"
    eo_map: dict[str, str] = {}
    if not eo_path.exists():
        return eo_map
    with open(eo_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            url = rec.get("source_url", "")
            st = rec.get("summary_text", "")
            for m in re.findall(r'(?:EO|Executive Order)\s+(\d{5})', st):
                if m not in eo_map:
                    eo_map[m] = url
    return eo_map


def _load_eo_policy_summaries(
    eo_numbers: list[str],
) -> list[dict[str, str]]:
    """Load EO policy summaries from the local EO index JSONL.

    Reads ``x_indices_eo/research_summaries_with_embeddings_combined.jsonl``
    and extracts the summary text for each requested EO number.

    Returns a list of dicts with keys: eo_number, title, url, summary_text.
    """
    import re as _re

    eo_path = Path.cwd() / EO_INDEX_FOLDER / "research_summaries_with_embeddings_combined.jsonl"
    if not eo_path.exists():
        return []

    target = set(eo_numbers)
    found: dict[str, dict[str, str]] = {}

    with open(eo_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            url = rec.get("source_url", "")
            summary = rec.get("summary_text", "")
            doc_meta = rec.get("doc_metadata", {})
            title = doc_meta.get("title", "")
            # Match EO numbers in the summary or title
            for m in _re.findall(r'(?:EO|Executive Order)\s+(\d{5})', summary + " " + title):
                if m in target and m not in found:
                    found[m] = {
                        "eo_number": m,
                        "title": title,
                        "url": url,
                        "summary_text": summary,
                    }

    # Return in the order requested
    return [found[eo] for eo in eo_numbers if eo in found]


def _extract_eo_grounding(
    eo_cache: list[dict],
    eo_numbers: list[str],
    min_confidence: float = 0.0,
) -> list[dict]:
    """Extract EO event grounding data paired with URLs from workflow cache.

    Returns a list of dicts, one per qualifying record, with the EO context
    that should be injected into the LLM prompt as grounding data.
    """
    events: list[dict] = []
    seen: set[tuple[str, str]] = set()  # dedupe by (eo_number, url)

    for record in eo_cache:
        eo_num = str(record.get("eo_number", ""))
        if eo_num not in eo_numbers:
            continue
        if record.get("confidence", 0.0) < min_confidence:
            continue
        comm = record.get("communication") or {}
        url = comm.get("url", "")
        if not url:
            continue
        key = (eo_num, url)
        if key in seen:
            continue
        seen.add(key)

        events.append({
            "eo_number": eo_num,
            "eo_title": record.get("eo_title", ""),
            "url": url,
            "action_type": comm.get("action_type", ""),
            "action_description": comm.get("action_description", "").strip(),
            "official": comm.get("official", ""),
            "date": comm.get("date", ""),
            "confidence": record.get("confidence", 0.0),
        })

    return events


def _extract_workflow_articles(
    media_cache: list[dict],
    eo_numbers: list[str],
    min_confidence: float = 0.0,
) -> list[dict]:
    """Extract media articles from workflow cache for given EOs.

    Returns articles with linkage_rationale and relevant_events enrichment.
    Each article is normalized to match BBOA linked_media schema plus extras.
    """
    articles: list[dict] = []
    seen_urls: set[str] = set()

    for record in media_cache:
        # Check confidence threshold
        if record.get("confidence", 0.0) < min_confidence:
            continue

        # Check if any linked_eos match our target EOs
        linked_eos = record.get("linked_eos", [])
        matching_eos = [
            eo for eo in linked_eos
            if str(eo.get("eo_number", "")) in eo_numbers
        ]
        if not matching_eos:
            continue

        url = record.get("article_url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        # Normalize to BBOA schema with enrichment
        article = {
            "link": url,
            "title": record.get("article_title", ""),
            "author": record.get("author", ""),
            "publisher": record.get("publisher", ""),
            "listed_publisher": record.get("publisher", ""),
            "date_publication": record.get("article_date", ""),
            "source_type": "workflow_linked",
            "eo_identifier": matching_eos[0].get("eo_number", ""),  # primary EO
            # Workflow-specific enrichment
            "linkage_rationale": record.get("linkage_rationale", ""),
            "relevant_events": record.get("relevant_events", []),
            "linked_eos": matching_eos,  # All matching EOs
            "workflow_confidence": record.get("confidence", 0.0),
            "media_framing": {},  # May be merged with BBOA later
        }
        articles.append(article)

    return articles


def _build_precedence_graph(
    eo_cache: list[dict],
) -> tuple[dict[str, dict[str, list[str]]], dict[str, list[str]]]:
    """Build precedence graph and cross_eo map from EO cache records.

    Returns (precedence_graph, cross_eo_map) where:
    - precedence_graph[eo_number] = {rescinds: [...], derived_from: [...], read_with: [...]}
    - cross_eo_map[eo_number] = [related_eo_numbers]
    """
    graph: dict[str, dict[str, list[str]]] = {}
    cross_eo: dict[str, list[str]] = {}

    for record in eo_cache:
        eo = str(record.get("eo_number", ""))
        prec = record.get("precedence", {})

        if eo not in graph:
            graph[eo] = {"rescinds": [], "derived_from": [], "read_with": []}

        for edge_type in ("rescinds", "derived_from", "read_with"):
            for neighbour in prec.get(edge_type, []):
                n = str(neighbour)
                if n not in graph[eo][edge_type]:
                    graph[eo][edge_type].append(n)

        xeo = record.get("cross_eo", [])
        if xeo:
            existing = cross_eo.get(eo, [])
            for x in xeo:
                xs = str(x)
                if xs not in existing:
                    existing.append(xs)
            cross_eo[eo] = existing

    return graph, cross_eo


# ---------------------------------------------------------------------------
# Phase 1: URL-Based BBAO Retrieval (Workflow Cache → BBAO)
# ---------------------------------------------------------------------------

def run_phase1(
    min_confidence: float = 0.0,
    include_unlinked: bool = False,
) -> tuple[BBAORetriever, list[dict], list[dict]]:
    _hr("Phase 1: URL-Based Retrieval (Workflow → BBAO)")

    # Step 1: Load workflow caches
    eo_cache = _load_eo_cache()
    media_cache = _load_media_linking_cache()
    print(f"\n[Workflow Caches Loaded]")
    print(f"  → {len(eo_cache)} EO implementation records")
    print(f"  → {len(media_cache)} media linking records")

    # Step 2: Extract primary URLs for test EO with confidence filtering
    test_eo = "14183"
    primary_urls, stats = _extract_urls_for_eo(eo_cache, test_eo, min_confidence)

    print(f"\n[Workflow Cache] EO {test_eo}:")
    print(f"  → {stats['total']} total communication records")
    print(f"  → {stats['filtered']} filtered (confidence < {min_confidence:.2f})")
    print(f"  → {stats['passed']} passed confidence filter")
    print(f"  → {len(primary_urls)} unique primary URLs")
    for i, url in enumerate(sorted(primary_urls)[:3], 1):
        print(f"     {i}. {url}")
    if len(primary_urls) > 3:
        print(f"     ... and {len(primary_urls) - 3} more")

    # Step 3: Load BBAO cache (multi-source with deduplication)
    retriever = BBAORetriever(BBAO_CACHE_SOURCES)
    retriever.load()

    print(f"\n[BBAO Cache] Loaded from {len(BBAO_CACHE_SOURCES)} sources:")
    summary = retriever.summary()
    print(f"  → Sessions: {', '.join(summary['session_ids'])}")
    print(f"  → {summary['eo_events']} eo_events, {summary['linked_media']} linked_media (deduplicated)")
    print(f"  → {summary['unlinked']} unlinked articles, {summary['unique_eo_identifiers']} unique EOs")

    # Step 4: URL-based retrieval — find events matching ANY of the primary URLs
    matched_events: list[dict] = []
    matched_event_keys: set[str] = set()

    for url in primary_urls:
        for ev in retriever.eo_events:
            if ev.get("source_url") == url:
                matched_events.append(ev)
                if ev.get("event_key"):
                    matched_event_keys.add(ev["event_key"])

    print(f"\n[URL Matching] Tracing {len(primary_urls)} URLs through BBAO:")
    print(f"  → Matched {len(matched_events)} events (via source_url)")
    print(f"  → {len(matched_event_keys)} unique event_keys")

    # Display EO grounding events from workflow cache
    eo_grounding = _extract_eo_grounding(eo_cache, [test_eo], min_confidence)
    print(f"\n[EO Grounding Events] {len(eo_grounding)} events from workflow cache:")
    for ev in eo_grounding:
        action = ev.get("action_type", "N/A")
        desc = ev.get("action_description", "")
        # Truncate long descriptions for display
        if len(desc) > 120:
            desc = desc[:117] + "..."
        official = ev.get("official", "N/A")
        date = ev.get("date", "N/A")
        url = ev.get("url", "")
        conf = ev.get("confidence", 0.0)
        print(f"  [{action}] {desc}")
        print(f"    Official: {official} | Date: {date} | Conf: {conf:.2f}")
        print(f"    URL: {url}")

    # Step 5: Get linked media for matched events
    linked_media: list[dict] = []
    for m in retriever.linked_media:
        if m.get("event_key") in matched_event_keys:
            linked_media.append(m)

    linked_urls = {m["link"] for m in linked_media if m.get("link")}

    print(f"  → {len(linked_media)} linked media articles")
    print(f"  → {len(linked_urls)} unique linked media URLs")

    # Step 6: Get unlinked media in time windows (optional)
    unlinked_urls: set[str] = set()
    if include_unlinked:
        windows: set[tuple[str, str]] = set()
        for ev in matched_events:
            if ev.get("window_start") and ev.get("window_end"):
                windows.add((ev["window_start"], ev["window_end"]))

        unlinked_media: list[dict] = []
        for w_start, w_end in windows:
            unlinked = retriever.unlinked_for_window(w_start, w_end)
            unlinked_media.extend(unlinked)

        unlinked_urls = {m["link"] for m in unlinked_media if m.get("link")}

        print(f"  → {len(windows)} time windows")
        print(f"  → {len(unlinked_media)} unlinked media articles in windows")
        print(f"  → {len(unlinked_urls)} unique unlinked URLs")
    else:
        print(f"  → Unlinked media: DISABLED (use --include-unlinked to enable)")

    media_urls = linked_urls | unlinked_urls
    print(f"  → {len(media_urls)} total unique media URLs")

    # Summary
    all_urls = primary_urls | media_urls
    print(f"\n[Phase 1 Complete]")
    print(f"  ✓ {len(primary_urls)} primary URLs (workflow cache, confidence ≥ {min_confidence:.2f})")
    print(f"  ✓ {len(linked_urls)} linked media URLs (BBAO)")
    if include_unlinked:
        print(f"  ✓ {len(unlinked_urls)} unlinked media URLs (BBAO time windows)")
    print(f"  ✓ {len(media_urls)} total media URLs")
    print(f"  ✓ {len(all_urls)} total unique URLs for Phase 2")

    return retriever, eo_cache, media_cache


# ---------------------------------------------------------------------------
# Phase 2: Category Grouping and Graph Expansion
# ---------------------------------------------------------------------------

def run_phase2(
    retriever: BBAORetriever,
    eo_cache: list[dict],
    media_cache: list[dict],
    min_confidence: float = 0.0,
) -> tuple[CategoryGrouper, dict[str, dict[str, str]]]:
    _hr("Phase 2: Category Grouping and Graph Expansion")

    graph, cross_eo = _build_precedence_graph(eo_cache)

    grouper = CategoryGrouper(graph, cross_eo)
    expanded = grouper.expand_all(max_depth=2)
    expanded = grouper.dedup_across_categories(expanded)
    manifest = grouper.category_manifest(expanded)

    # Report per-category with URL counts
    print("\n[Category Expansion] read_with edges only (depth 2):")
    for cat, eos in expanded.items():
        anchors = [e for e, r in eos.items() if r == "anchor"]
        discovered = [(e, r) for e, r in eos.items() if r != "anchor"]

        # Cross-reference against BBAO
        bbao_eos = {
            BBAORetriever.strip_eo_prefix(e.get("eo_identifier", ""))
            for e in retriever.eo_events
        }
        with_bbao = [e for e in eos if e in bbao_eos]

        # Count URLs accessible via this category's EOs (workflow cache)
        cat_urls: set[str] = set()
        for eo in eos:
            eo_urls, _ = _extract_urls_for_eo(eo_cache, eo, min_confidence)
            cat_urls |= eo_urls

        print(
            f"\n  {cat}:"
        )
        print(f"    {len(anchors)} anchors → {len(discovered)} discovered = {len(eos)} total EOs")
        print(f"    {len(with_bbao)} EOs have BBAO events, {len(cat_urls)} workflow URLs")

        # Print anchor EOs with titles
        if anchors:
            print(f"    Anchor EOs:")
            for eo_num in sorted(anchors):
                title = _get_eo_title(eo_cache, eo_num)
                print(f"      • {eo_num}: {title}")

        # Print discovered EOs with reasons and titles
        if discovered:
            print(f"    Discovered EOs:")
            for eo, reason in sorted(discovered):
                title = _get_eo_title(eo_cache, eo)
                print(f"      • {eo}: {title} (via {reason})")

    print(f"\n[Phase 2 Complete]")
    print(f"  ✓ {len(manifest)} manifest entries")
    print(f"  ✓ Categories ready for Phase 3 reconstitution")

    return grouper, expanded


# ---------------------------------------------------------------------------
# Phase 3: Full-Doc Reconstitution (URL-Based)
# ---------------------------------------------------------------------------

def run_phase3(
    retriever: BBAORetriever,
    eo_cache: list[dict],
    media_cache: list[dict],
    expanded: dict[str, dict[str, str]],
    category: str,
    min_confidence: float = 0.3,
    include_unlinked: bool = False,
    live: bool = False,
) -> dict[str, dict]:
    _hr(f"Phase 3: Full-Doc Reconstitution [{category}]")

    # URL-based retrieval for the category
    eo_numbers = list(expanded.get(category, {}).keys())

    print(f"\n[Step 1: Extract URLs from Workflow Cache]")
    print(f"  → Processing {len(eo_numbers)} EOs: {', '.join(sorted(eo_numbers))}")

    # Step 1: Extract primary URLs from workflow cache
    primary_urls: set[str] = set()
    total_records = 0
    total_filtered = 0
    passed_records = 0
    for eo in eo_numbers:
        urls, stats = _extract_urls_for_eo(eo_cache, eo, min_confidence)
        primary_urls |= urls
        total_records += stats["total"]
        total_filtered += stats["filtered"]
        passed_records += stats["passed"]

    print(f"  → {total_records} workflow cache records (communications about these EOs)")
    if total_filtered > 0:
        print(f"  → {total_filtered} records filtered (confidence < {min_confidence})")
        print(f"  → {passed_records} records passed filter")
    print(f"  → {len(primary_urls)} UNIQUE URLs extracted (some URLs appear in multiple records)")

    # Step 2: Get workflow media articles (PRIMARY SOURCE)
    print(f"\n[Step 2: Get Workflow Media Articles with Rationale]")
    workflow_articles = _extract_workflow_articles(media_cache, eo_numbers, min_confidence)
    workflow_urls = {a["link"] for a in workflow_articles}
    print(f"  → {len(workflow_articles)} workflow articles found")
    print(f"  → {len(workflow_urls)} UNIQUE URLs from workflow cache")

    # Step 3: Optionally enrich with BBOA media framing
    print(f"\n[Step 3: Enrich with BBOA Media Framing (Optional)]")

    # Build BBOA URL → media_framing lookup (check BOTH linked and unlinked)
    bboa_framing: dict[str, dict] = {}
    for m in retriever.linked_media + retriever.unlinked:
        url = m.get("link", "")
        if url and m.get("media_framing"):
            bboa_framing[url] = {
                "media_framing": m.get("media_framing"),
                "consumer_salience": m.get("consumer_salience"),
                "mean_search_engine_rank": m.get("mean_search_engine_rank"),
                "event_key": m.get("event_key"),
            }

    # Enrich workflow articles with BBOA framing where available
    all_linked: list[dict] = []
    enriched_count = 0
    for article in workflow_articles:
        url = article["link"]
        if url in bboa_framing:
            # Merge BBOA media framing into workflow article
            enriched = dict(article)
            enriched["media_framing"] = bboa_framing[url].get("media_framing", {})
            enriched["consumer_salience"] = bboa_framing[url].get("consumer_salience")
            enriched["mean_search_engine_rank"] = bboa_framing[url].get("mean_search_engine_rank")
            enriched["event_key"] = bboa_framing[url].get("event_key")
            all_linked.append(enriched)
            enriched_count += 1
        else:
            all_linked.append(article)

    linked_urls = {a["link"] for a in all_linked}

    print(f"  → BBOA cache: {len(bboa_framing):,} articles with media_framing available")
    print(f"  → {enriched_count}/{len(workflow_articles)} workflow articles enriched ({enriched_count/len(workflow_articles)*100:.1f}%)")
    print(f"\n[Workflow Evidence Package]")
    print(f"  → {len(all_linked)} total articles from workflow")
    print(f"  → {len([a for a in all_linked if a.get('linkage_rationale')])} with linkage rationale")
    print(f"  → {len([a for a in all_linked if a.get('relevant_events')])} with relevant events")
    print(f"  → {len([a for a in all_linked if a.get('media_framing')])} with media framing (BBOA enrichment)")

    # Step 4: NOTE - workflow cache only contains linked articles
    # (no "unlinked" concept in workflow media linking)
    all_unlinked: list[dict] = []
    unlinked_urls: set[str] = set()

    media_urls = linked_urls
    all_urls = primary_urls | media_urls

    print(f"\n[URL Summary]")
    print(f"  → {len(primary_urls)} primary URLs (workflow EO cache)")
    print(f"  → {len(linked_urls)} media URLs (workflow media linking cache)")
    print(f"  → {len(all_urls)} TOTAL unique URLs for reconstitution")

    reconstituted: dict[str, dict] = {}

    if live:
        print(f"\n[Step 5: RAG Reconstitution - LIVE MODE]")
        recon = FullDocReconstituter(RAG_URL)
        all_urls_list = sorted(all_urls)

        for idx_folder in INDEX_FOLDERS:
            remaining = [u for u in all_urls_list if u not in reconstituted]
            if not remaining:
                print(f"  ✓ All URLs already reconstituted, skipping {idx_folder}")
                break

            if recon.ensure_index(idx_folder):
                print(f"  ✓ RAG index: {idx_folder}")
            else:
                print(f"  ⚠ Could not switch to {idx_folder} — skipping")
                continue

            print(f"  → Querying {len(remaining)} URLs against {idx_folder}...")
            docs = recon.reconstitute_batch(remaining)
            new_hits = 0
            for doc in docs:
                if doc["url"] not in reconstituted:
                    reconstituted[doc["url"]] = doc
                    new_hits += 1
            print(f"  ✓ {new_hits} new documents from {idx_folder}")

        print(f"  ✓ {len(reconstituted)} TOTAL documents reconstituted across {len(INDEX_FOLDERS)} indices")
    else:
        # Dry-run: build synthetic reconstituted docs for ALL URLs
        print(f"\n[Step 5: RAG Reconstitution - DRY RUN MODE]")

        # Reconstitute ONE real article via RAG to demonstrate QA-context dedup
        _demo_recon = FullDocReconstituter(RAG_URL)
        _demo_recon_ok = False
        try:
            _demo_recon_ok = _demo_recon.ensure_index(INDEX_FOLDERS[0])
        except Exception:
            pass
        if _demo_recon_ok:
            # Pick a non-.gov media URL from the workflow articles
            _candidate_urls = [
                a["link"] for a in all_linked
                if not any(d in a.get("link", "") for d in (".gov", ".mil"))
            ][:10]
            _demo_doc = None
            for _cu in _candidate_urls:
                _demo_doc = _demo_recon.reconstitute(_cu)
                if _demo_doc and _demo_doc.get("qa_count", 0) >= 3:
                    break
                _demo_doc = None

            if _demo_doc:
                _ft = _demo_doc["full_text"]
                _chunks = _ft.split("\n\n")
                print(f"\n  ╔══════════════════════════════════════════════════════════════╗")
                print(f"  ║  LIVE RECONSTITUTION SAMPLE (1 article via RAG)             ║")
                print(f"  ╚══════════════════════════════════════════════════════════════╝")
                print(f"  URL:            {_demo_doc['url']}")
                print(f"  Title:          {_demo_doc['title']}")
                print(f"  QA pairs from RAG:  {_demo_doc['qa_count']}")
                print(f"  Unique chunks after dedup: {len(_chunks)}")
                print(f"  Full text length:   {len(_ft):,} chars")
                print(f"\n  --- Reconstituted Text (first 2 chunks) ---")
                for _i, _c in enumerate(_chunks[:2], 1):
                    _preview = _c[:400] + ("..." if len(_c) > 400 else "")
                    print(f"  [{_i}] {_preview}")
                print()
                # Inject this real doc into the reconstituted map
                reconstituted[_demo_doc["url"]] = _demo_doc
            else:
                print(f"  → No article with 3+ QA pairs found for live sample")
        else:
            print(f"  → RAG not reachable — skipping live sample")

        print(f"  → Building synthetic docs for {len(all_urls)} URLs (no actual RAG calls)")
        for url in sorted(all_urls):
            if url in reconstituted:
                continue  # keep the real one we just fetched
            reconstituted[url] = {
                "url": url,
                "title": f"[Synthetic] Article from {url[:50]}",
                "summary_text": "Synthetic summary for demo.",
                "full_text": "Synthetic full-doc text from QA context reconstitution.",
                "doc_metadata": {},
                "qa_count": 5,
            }
        print(f"  ✓ {len(reconstituted)} documents created ({1 if _demo_doc else 0} live, rest synthetic)")

    # Build evidence bundle - this combines all media articles with reconstituted full-text
    print(f"\n[Step 6: Build Analysis Package]")
    print(f"  → Packaging {len(all_linked)} workflow media articles (enriched with BBOA framing where available)...")
    recon_obj = FullDocReconstituter(RAG_URL)
    bundle = recon_obj.build_evidence_bundle(
        category=category,
        linked_media=all_linked,  # ALL workflow articles
        unlinked_media=[],  # workflow cache has no "unlinked" concept
        reconstituted_docs=reconstituted,
    )

    recon_urls = {a["url"] for a in bundle["articles"] if a.get("reconstituted")}
    bundle_urls = {a["url"] for a in bundle["articles"]}
    metadata_only = bundle_urls - recon_urls

    print(f"  ✓ {len(bundle['articles'])} total articles packaged for analysis")
    print(f"    • {len(recon_urls)} with full-text (reconstituted via RAG)")
    print(f"    • {len(metadata_only)} metadata-only (title, URL, BBAO tags)")

    # Show a full sample reconstituted article - pick one that was actually reconstituted
    reconstituted_articles = [a for a in bundle['articles'] if a.get('reconstituted')]
    sample = reconstituted_articles[0] if reconstituted_articles else bundle['articles'][0]

    if bundle['articles']:
        print(f"\n" + "=" * 80)
        if reconstituted_articles:
            print(f"SAMPLE RECONSTITUTED ARTICLE (FULL) - 1 of {len(reconstituted_articles)}")
        else:
            print("SAMPLE ARTICLE (METADATA-ONLY, NO RAG HIT)")
        print("=" * 80)

        # --- Article Metadata ---
        print(f"\n--- ARTICLE METADATA ---")
        print(f"  URL:                  {sample.get('url', 'N/A')}")
        print(f"  Title:                {sample.get('title', 'N/A')}")
        print(f"  Author:               {sample.get('author', 'N/A')}")
        print(f"  Publisher:             {sample.get('publisher', 'N/A')}")
        print(f"  Listed Publisher:      {sample.get('listed_publisher', 'N/A')}")
        print(f"  Publication Date:      {sample.get('date_publication', 'N/A')}")
        print(f"  Source Type:           {sample.get('source_type', 'N/A')}")
        print(f"  EO Identifier:         {sample.get('eo_identifier', 'N/A')}")
        print(f"  Event Key:             {sample.get('event_key', 'N/A')}")
        print(f"  Consumer Salience:     {sample.get('consumer_salience', 'N/A')}")
        print(f"  Mean Search Rank:      {sample.get('mean_search_engine_rank', 'N/A')}")
        print(f"  Reconstituted:         {sample.get('reconstituted', False)}")
        print(f"  QA Pairs Used:         {sample.get('qa_count', 0)}")

        # --- RAG doc_metadata (from the index) ---
        doc_meta = sample.get('doc_metadata', {})
        if doc_meta:
            print(f"\n--- RAG DOCUMENT METADATA ---")
            for key, value in doc_meta.items():
                if value:
                    print(f"  {key}: {value}")

        # --- Full Text (prefer QA-reconstituted, fall back to summary — never both) ---
        if sample.get('reconstituted'):
            full_text = sample.get('full_text', '')

            if sample.get('qa_count', 0) > 0:
                print(f"\nReconstitution Source: {sample.get('qa_count')} QA context chunks (deduplicated)")
            else:
                print(f"\nReconstitution Source: Summary document (no QA pairs available)")

            print(f"\n--- FULL TEXT ---")
            print(full_text)
        else:
            print(f"\n--- NOT RECONSTITUTED (no RAG hit for this URL) ---")

        # --- BBAO Media Framing (Optional Enrichment) ---
        if 'media_framing' in sample and sample['media_framing']:
            mf = sample['media_framing']
            print(f"\n--- BBOA MEDIA FRAMING (Enrichment) ---")
            for key, value in mf.items():
                if value:
                    print(f"  • {key}: {value}")

        # --- Workflow Linkage Data (Primary Source) ---
        if sample.get('linkage_rationale') or sample.get('relevant_events'):
            print(f"\n--- WORKFLOW LINKAGE DATA ---")
            if sample.get('linkage_rationale'):
                rationale = sample['linkage_rationale']
                # Truncate if too long
                if len(rationale) > 500:
                    rationale = rationale[:497] + "..."
                print(f"  Rationale: {rationale}")
            if sample.get('linked_eos'):
                print(f"\n  Linked EOs:")
                for eo in sample.get('linked_eos', []):
                    print(f"    • EO {eo.get('eo_number')}: {eo.get('eo_name', 'N/A')}")
            if sample.get('relevant_events'):
                print(f"\n  Relevant Events: {len(sample['relevant_events'])} event(s)")
                for evt in sample['relevant_events'][:3]:  # Show max 3
                    print(f"    • [{evt.get('event_date')}] {evt.get('description', 'N/A')[:80]}...")
            if sample.get('workflow_confidence'):
                print(f"\n  Workflow Confidence: {sample['workflow_confidence']:.2f}")

        if 'eo_tags' in sample:
            print(f"\nEO Tags: {sample['eo_tags']}")

        print("=" * 80)

    print(f"\n[Phase 3 Complete]")
    print(f"  ✓ {len(all_urls)} URLs traced: workflow cache → reconstitution")
    print(f"  ✓ Analysis package ready with {len(bundle['articles'])} articles for Phase 4")
    print(f"  ✓ Evidence sources: workflow media linking cache (primary), BBOA (enrichment)")

    # Extract EO event grounding data for prompt injection
    eo_grounding = _extract_eo_grounding(eo_cache, eo_numbers, min_confidence)
    print(f"  ✓ {len(eo_grounding)} EO grounding events extracted for LLM context")

    # Reconstitute EO policy documents from x_indices_eo via RAG
    eo_url_map = _load_eo_url_map()
    relevant_eo_urls: dict[str, str] = {}  # eo_number -> fed register url
    for eo_num in eo_numbers:
        if eo_num in eo_url_map:
            relevant_eo_urls[eo_num] = eo_url_map[eo_num]

    eo_policy_texts: dict[str, dict] = {}  # eo_number -> {url, title, full_text}
    if relevant_eo_urls:
        print(f"\n[Step 7: EO Policy Reconstitution]")
        print(f"  → {len(relevant_eo_urls)} EOs mapped to Federal Register URLs")
        if live:
            recon = FullDocReconstituter(RAG_URL)
            if recon.ensure_index(EO_INDEX_FOLDER):
                print(f"  ✓ RAG index: {EO_INDEX_FOLDER}")
                urls_to_query = list(relevant_eo_urls.values())
                docs = recon.reconstitute_batch(urls_to_query)
                # Map back to EO numbers
                url_to_eo = {v: k for k, v in relevant_eo_urls.items()}
                for doc in docs:
                    eo_num = url_to_eo.get(doc["url"])
                    if eo_num:
                        eo_policy_texts[eo_num] = {
                            "url": doc["url"],
                            "title": doc.get("title", ""),
                            "full_text": doc.get("full_text", ""),
                        }
                print(f"  ✓ {len(eo_policy_texts)} EO policy documents reconstituted")
            else:
                print(f"  ⚠ Could not switch to {EO_INDEX_FOLDER} — skipping EO reconstitution")
        else:
            print(f"  → DRY RUN: building synthetic EO policy texts")
            for eo_num, url in relevant_eo_urls.items():
                title = _get_eo_title(eo_cache, eo_num)
                eo_policy_texts[eo_num] = {
                    "url": url,
                    "title": title,
                    "full_text": f"[Synthetic] Full text of EO {eo_num}: {title}",
                }
            print(f"  ✓ {len(eo_policy_texts)} synthetic EO policy texts created")

        for eo_num, info in sorted(eo_policy_texts.items()):
            text_len = len(info.get('full_text', ''))
            print(f"    • EO {eo_num}: {info.get('title', '')[:60]} ({text_len:,} chars)")

    return {
        "bundle": bundle,
        "reconstituted": reconstituted,
        "primary_urls": primary_urls,
        "media_urls": media_urls,
        "eo_grounding": eo_grounding,
        "eo_policy_texts": eo_policy_texts,
    }


# ---------------------------------------------------------------------------
# Phase 4: Threat Cluster Analysis
# ---------------------------------------------------------------------------


def _resolve_kaliya_citations(
    text: str,
    articles: list[dict],
) -> tuple[str, int]:
    """Clean up Kaliya citation output.

    The unified citation convention is ``(req_*, summary)`` — these are
    the *desired* output markers and are **preserved** in the final text
    to enable data-lineage across consumers.

    Legacy ``(art_N, kaliya)`` markers are resolved to hyperlinked
    author-date citations as a backward-compatibility measure.

    Stray ``(doc_*, summary)`` markers (non-standard) are scrubbed.

    Returns (resolved_text, legacy_art_citations_resolved).
    """
    import re
    from datetime import datetime

    # Build article lookup: 1-indexed article number → metadata
    art_lookup: dict[int, dict] = {}
    for idx, a in enumerate(articles, 1):
        author = (a.get("author") or "").strip()
        if not author:
            doc_meta = a.get("doc_metadata") or {}
            authors_list = doc_meta.get("authors") or []
            author = authors_list[0] if authors_list else ""
        if not author:
            author = (a.get("publisher") or "Unknown").strip()

        date_str = (a.get("date_publication") or "").strip()
        date_display = ""
        if date_str:
            for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    date_display = dt.strftime("%B %Y")
                    break
                except ValueError:
                    continue

        art_lookup[idx] = {
            "author": author,
            "date_display": date_display,
            "url": (a.get("url") or "").strip(),
        }

    resolved = 0

    def _replace_art(m: re.Match) -> str:
        nonlocal resolved
        art_num = int(m.group(1))
        meta = art_lookup.get(art_num)
        if not meta or not meta["url"]:
            return m.group(0)
        resolved += 1
        parts = [meta["author"]] if meta["author"] else []
        if meta["date_display"]:
            parts.append(meta["date_display"])
        label = ", ".join(parts) if parts else f"Article {art_num}"
        return f"[{label}]({meta['url']})"

    # Resolve any legacy (art_N, kaliya) markers still present
    text = re.sub(r'\(art_(\d+),\s*kaliya\)', _replace_art, text)

    # Preserve (req_*, summary) markers — these are the canonical format.
    # Only scrub stray (doc_*, summary) markers which are non-standard.
    text = re.sub(r'\(doc_[A-Za-z0-9_]+,\s*summary\)', '', text)

    return text, resolved


def _resolve_article_citations(
    text: str,
    articles: list[dict],
    eo_policy_texts: dict[str, dict] | None = None,
) -> tuple[str, int, int]:
    """Replace citation markers with hyperlinked author-date citations.

    Markers:
        (art_N, cluster)  → [Author, Month Year](url)
        (eo_NNNNN, eo)    → [Executive Order NNNNN](url)

    Returns (resolved_text, article_citations_resolved, eo_citations_resolved).
    """
    import re
    from datetime import datetime

    # ── Build article lookup: 1-indexed article number → metadata ──
    art_lookup: dict[int, dict] = {}
    for idx, a in enumerate(articles, 1):
        # Derive author display name
        author = (a.get("author") or "").strip()
        if not author:
            doc_meta = a.get("doc_metadata") or {}
            authors_list = doc_meta.get("authors") or []
            author = authors_list[0] if authors_list else ""
        if not author:
            author = (a.get("publisher") or "Unknown").strip()

        # Parse publication date to "Month Year"
        date_str = (a.get("date_publication") or "").strip()
        date_display = ""
        if date_str:
            for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    date_display = dt.strftime("%B %Y")
                    break
                except ValueError:
                    continue

        art_lookup[idx] = {
            "author": author,
            "date_display": date_display,
            "url": (a.get("url") or "").strip(),
        }

    # ── Build EO lookup: eo_number → metadata ──
    eo_lookup: dict[str, dict] = {}
    if eo_policy_texts:
        for eo_num, info in eo_policy_texts.items():
            eo_lookup[eo_num] = {
                "title": info.get("title", ""),
                "url": (info.get("url") or "").strip(),
            }

    # ── Resolve article citations: (art_N, cluster) ──
    art_count = 0

    def _replace_art(m: re.Match) -> str:
        nonlocal art_count
        art_num = int(m.group(1))
        meta = art_lookup.get(art_num)
        if not meta or not meta["url"]:
            return m.group(0)  # leave unresolved if no metadata
        art_count += 1
        parts = [meta["author"]] if meta["author"] else []
        if meta["date_display"]:
            parts.append(meta["date_display"])
        label = ", ".join(parts) if parts else f"Article {art_num}"
        return f"[{label}]({meta['url']})"

    text = re.sub(r'\(art_(\d+),\s*cluster\)', _replace_art, text)

    # ── Resolve EO citations: (eo_NNNNN, eo) ──
    eo_count = 0

    def _replace_eo(m: re.Match) -> str:
        nonlocal eo_count
        eo_num = m.group(1)
        meta = eo_lookup.get(eo_num)
        if not meta or not meta["url"]:
            return m.group(0)  # leave unresolved if no URL
        eo_count += 1
        return f"[Executive Order {eo_num}]({meta['url']})"

    text = re.sub(r'\(eo_(\d+),\s*eo\)', _replace_eo, text)

    return text, art_count, eo_count


def _write_phase4_markdown(
    category: str,
    model_name: str,
    llm_output: str,
    usage_stats: dict,
    bundle: dict,
    dest_dir: Path,
    grounding_events: list[dict] | None = None,
    eo_policy_texts: dict[str, dict] | None = None,
    reasoning_content: list[str] | None = None,
) -> Path:
    """Write Phase 4 analysis result to a markdown file with reference appendices."""
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"phase4_{category}_{ts}.md"
    dest_dir.mkdir(parents=True, exist_ok=True)
    filepath = dest_dir / filename

    rates = LLM_PRICING.get(model_name, {})
    header_lines = [
        f"# Phase 4 — Threat Cluster Analysis",
        f"",
        f"- **Category:** {category}",
        f"- **Model:** {model_name}",
        f"- **Timestamp:** {ts}",
        f"- **Articles:** {bundle.get('article_count', 0)}",
        f"- **Input tokens:** {usage_stats.get('input_tokens', 0):,}",
        f"- **Output tokens:** {usage_stats.get('output_tokens', 0):,}",
        f"- **Reasoning tokens:** {usage_stats.get('reasoning_tokens', 0):,}",
        f"- **Cost:** ${usage_stats.get('cost_usd', 0.0):.6f}",
        f"- **Rates (per 1M tokens):** input=${rates.get('input', '?')} / output=${rates.get('output', '?')}",
        f"",
        f"---",
        f"",
    ]

    # Build content with LLM output
    parts = ["\n".join(header_lines), llm_output]

    # Append grounding events reference
    if grounding_events:
        parts.append("\n\n---\n\n")
        parts.append("# APPENDIX: Grounding Events Reference\n\n")
        parts.append(
            "The analysis above cites grounding events using tags like `[G1]`, `[G20]`, etc. "
            "Below is the complete reference table mapping each tag to the verified government action.\n\n"
        )
        for g_idx, ev in enumerate(grounding_events, 1):
            parts.append(
                f"## [G{g_idx}] EO {ev.get('eo_number', 'N/A')}: {ev.get('eo_title', 'Untitled')}\n\n"
                f"- **Action Type:** {ev.get('action_type', 'N/A')}\n"
                f"- **Description:** {ev.get('action_description', 'N/A')}\n"
                f"- **Official:** {ev.get('official', 'N/A')}\n"
                f"- **Date:** {ev.get('date', 'N/A')}\n"
                f"- **Source URL:** {ev.get('url', '')}\n\n"
            )

    # Append EO policy texts reference
    if eo_policy_texts:
        parts.append("\n\n---\n\n")
        parts.append("# APPENDIX: Executive Order Policy Documents\n\n")
        parts.append(
            "The following executive orders were provided as baseline policy documents "
            "for comparison against media coverage.\n\n"
        )
        for eo_num in sorted(eo_policy_texts.keys()):
            info = eo_policy_texts[eo_num]
            parts.append(
                f"## EO {eo_num}: {info.get('title', 'Untitled')}\n\n"
                f"- **Source:** {info.get('url', '')}\n"
                f"- **Length:** {len(info.get('full_text', '')):,} characters\n\n"
            )

    # Append reasoning content (chain-of-thought evidence)
    if reasoning_content:
        parts.append("\n\n---\n\n")
        parts.append("# APPENDIX: Model Reasoning (Chain-of-Thought)\n\n")
        parts.append(
            "The following reasoning blocks were produced by the model during analysis. "
            "These provide evidence for how hypotheses were assessed and conclusions drawn.\n\n"
        )
        for r_idx, reasoning_block in enumerate(reasoning_content, 1):
            parts.append(f"## Reasoning Block {r_idx}\n\n")
            parts.append(reasoning_block.strip())
            parts.append("\n\n")

    # Append article reference index (number → URL mapping for cluster map resolution)
    articles = bundle.get("articles", [])
    if articles:
        parts.append("\n\n---\n\n")
        parts.append("# APPENDIX: Article Reference Index\n\n")
        parts.append(
            "The cluster analysis above references articles by number. "
            "Below is the complete mapping from article number to URL.\n\n"
        )
        parts.append("| Art. | Author | Date | Publication | Title | URL |\n")
        parts.append("|---|---|---:|---|---|---|\n")
        for i, a in enumerate(articles, 1):
            author = (a.get("author") or "").replace("|", "/")
            date = (a.get("date_publication") or "").replace("|", "/")
            pub = (a.get("publisher") or "").replace("|", "/")
            title = (a.get("title") or "Untitled").replace("|", "/")
            url = a.get("url", "")
            parts.append(f"| {i} | {author} | {date} | {pub} | {title} | {url} |\n")

    content = "".join(parts)
    filepath.write_text(content, encoding="utf-8")

    # Persist the bundle as companion JSONL so fast-forward can reload
    # the exact article ordering that generated this analysis.
    import json as _json
    bundle_path = filepath.with_suffix(".bundle.jsonl")
    with open(bundle_path, "w", encoding="utf-8") as bf:
        for a in articles:
            bf.write(_json.dumps(a, default=str) + "\n")
    log.info("  Bundle saved: %s (%d articles)", bundle_path.name, len(articles))

    return filepath


# ---------------------------------------------------------------------------
# Phase 4 — Post-processing: extract artifacts from markdown to cache folder
# ---------------------------------------------------------------------------


def _extract_phase4_artifacts(
    md_path: Path,
    reasoning_content: list[str] | None = None,
) -> Path:
    """Extract structured artifacts from Phase 4 markdown into a companion folder.

    Creates a folder next to the markdown file containing individual files
    for each artifact type — tables as CSV, sections as markdown, and
    metadata as JSON — for human-in-the-loop analysis.

    Returns the artifact directory path.
    """
    import csv
    import io
    import json as _json
    import re

    content = md_path.read_text(encoding="utf-8")
    lines = content.split("\n")
    artifact_dir = md_path.with_suffix(".artifacts")
    artifact_dir.mkdir(parents=True, exist_ok=True)

    # ── Helper: find section by heading ───────────────────────────────
    def _find_section(heading_pattern: str, level: int = 2) -> str | None:
        """Extract everything from a heading match to the next same-or-higher level."""
        prefix = "#" * level
        start_idx = None
        for i, line in enumerate(lines):
            if start_idx is None:
                if line.startswith(prefix + " ") and re.search(heading_pattern, line, re.IGNORECASE):
                    start_idx = i
            else:
                stripped = line.lstrip()
                if stripped.startswith("#"):
                    hashes = len(stripped) - len(stripped.lstrip("#"))
                    if hashes <= level:
                        return "\n".join(lines[start_idx:i]).strip()
        if start_idx is not None:
            return "\n".join(lines[start_idx:]).strip()
        return None

    # ── Helper: extract markdown table rows to CSV ────────────────────
    def _md_table_to_csv(table_text: str) -> str:
        """Convert a markdown pipe table to CSV string."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        for line in table_text.strip().split("\n"):
            line = line.strip()
            if not line.startswith("|"):
                continue
            # Skip separator rows
            cells = [c.strip() for c in line.split("|")]
            # Remove empty first/last from leading/trailing pipes
            if cells and cells[0] == "":
                cells = cells[1:]
            if cells and cells[-1] == "":
                cells = cells[:-1]
            if all(set(c) <= set("-: ") for c in cells):
                continue  # separator row
            writer.writerow(cells)
        return buf.getvalue()

    # ── Helper: extract all table blocks from a section ───────────────
    def _extract_tables(section_text: str) -> list[str]:
        """Pull contiguous pipe-table blocks from section text."""
        tables: list[str] = []
        current: list[str] = []
        for line in section_text.split("\n"):
            if line.strip().startswith("|"):
                current.append(line)
            else:
                if current:
                    tables.append("\n".join(current))
                    current = []
        if current:
            tables.append("\n".join(current))
        return tables

    manifest: dict[str, str] = {}

    # ── 1. Metadata header ────────────────────────────────────────────
    header_data: dict[str, str] = {}
    for line in lines[:20]:
        m = re.match(r"^- \*\*(.+?):\*\*\s*(.+)$", line)
        if m:
            header_data[m.group(1)] = m.group(2)
    if header_data:
        meta_path = artifact_dir / "metadata.json"
        meta_path.write_text(_json.dumps(header_data, indent=2), encoding="utf-8")
        manifest["metadata"] = "metadata.json"

    # ── 2. Cluster Map ────────────────────────────────────────────────
    section = _find_section(r"cluster\s*map")
    if section:
        (artifact_dir / "cluster_map.md").write_text(section, encoding="utf-8")
        manifest["cluster_map_md"] = "cluster_map.md"
        tables = _extract_tables(section)
        if tables:
            csv_text = _md_table_to_csv(tables[0])
            (artifact_dir / "cluster_map.csv").write_text(csv_text, encoding="utf-8")
            manifest["cluster_map_csv"] = "cluster_map.csv"

    # ── 3. Media salience tables ──────────────────────────────────────
    section = _find_section(r"official\s+baseline\s+events\s+vs.*media\s+salience")
    if section:
        (artifact_dir / "media_salience_table.md").write_text(section, encoding="utf-8")
        manifest["media_salience_md"] = "media_salience_table.md"
        tables = _extract_tables(section)
        if tables:
            csv_text = _md_table_to_csv(tables[0])
            (artifact_dir / "media_salience_table.csv").write_text(csv_text, encoding="utf-8")
            manifest["media_salience_csv"] = "media_salience_table.csv"

    # ── 4. AI salience table ──────────────────────────────────────────
    section = _find_section(r"official\s+AI\s+baseline\s+events\s+vs.*media\s+salience")
    if section:
        (artifact_dir / "ai_salience_table.md").write_text(section, encoding="utf-8")
        manifest["ai_salience_md"] = "ai_salience_table.md"
        tables = _extract_tables(section)
        if tables:
            csv_text = _md_table_to_csv(tables[0])
            (artifact_dir / "ai_salience_table.csv").write_text(csv_text, encoding="utf-8")
            manifest["ai_salience_csv"] = "ai_salience_table.csv"

    # ── 5. Publication-house grouping ─────────────────────────────────
    section = _find_section(r"publication.house\s+and\s+authorial\s+grouping")
    if section:
        (artifact_dir / "publication_grouping.md").write_text(section, encoding="utf-8")
        manifest["publication_grouping"] = "publication_grouping.md"

    # ── 6. Individual cluster sections (A–Z) ─────────────────────────
    cluster_pattern = re.compile(
        r"^##\s+Cluster\s+([A-Z])\s+[—–-]\s+(.+)", re.IGNORECASE,
    )
    cluster_start_indices: list[tuple[int, str, str]] = []
    for i, line in enumerate(lines):
        m = cluster_pattern.match(line)
        if m:
            cluster_start_indices.append((i, m.group(1), m.group(2).strip()))

    for idx, (start, cid, title) in enumerate(cluster_start_indices):
        # End is either the next cluster or a ## that isn't a sub-heading
        end = len(lines)
        for j in range(start + 1, len(lines)):
            stripped = lines[j].lstrip()
            if stripped.startswith("## ") and not stripped.startswith("### "):
                end = j
                break
        cluster_text = "\n".join(lines[start:end]).strip()
        fname = f"cluster_{cid}.md"
        (artifact_dir / fname).write_text(cluster_text, encoding="utf-8")
        manifest[f"cluster_{cid}"] = fname

        # Extract tables within cluster (article tables, risk tables)
        tables = _extract_tables(cluster_text)
        for t_idx, table in enumerate(tables):
            csv_text = _md_table_to_csv(table)
            csv_fname = f"cluster_{cid}_table_{t_idx + 1}.csv"
            (artifact_dir / csv_fname).write_text(csv_text, encoding="utf-8")
            manifest[f"cluster_{cid}_table_{t_idx + 1}"] = csv_fname

    # ── 7. Hypothesis testing ─────────────────────────────────────────
    section = _find_section(r"testing\s+the\s+strongest\s+clustering\s+hypotheses")
    if section:
        (artifact_dir / "hypotheses.md").write_text(section, encoding="utf-8")
        manifest["hypotheses"] = "hypotheses.md"

    # ── 8. Homogeneous threat clusters ────────────────────────────────
    section = _find_section(r"threat\s+clusters\s+with\s+homogeneous")
    if section:
        (artifact_dir / "threat_clusters_homogeneous.md").write_text(section, encoding="utf-8")
        manifest["threat_clusters_homogeneous"] = "threat_clusters_homogeneous.md"

    # ── 9. Confirmation bias guardrails ───────────────────────────────
    section = _find_section(r"guarding\s+against\s+confirmation\s+bias")
    if section:
        (artifact_dir / "confirmation_bias_guardrails.md").write_text(section, encoding="utf-8")
        manifest["confirmation_bias_guardrails"] = "confirmation_bias_guardrails.md"

    # ── 10. Concluding assessment ─────────────────────────────────────
    section = _find_section(r"concluding\s+assessment")
    if section:
        (artifact_dir / "concluding_assessment.md").write_text(section, encoding="utf-8")
        manifest["concluding_assessment"] = "concluding_assessment.md"

    # ── 11. Grounding Events Reference (appendix) ────────────────────
    section = _find_section(r"grounding\s+events\s+reference", level=1)
    if section:
        (artifact_dir / "grounding_events_index.md").write_text(section, encoding="utf-8")
        manifest["grounding_events_md"] = "grounding_events_index.md"

        # Parse to structured CSV: tag, eo_number, eo_title, action_type, description, official, date, url
        g_pattern = re.compile(
            r"^##\s+\[G(\d+)\]\s+EO\s+(\d+):\s+(.+)", re.MULTILINE,
        )
        field_pattern = re.compile(r"^-\s+\*\*(.+?):\*\*\s+(.+)$")
        g_records: list[dict[str, str]] = []
        current_rec: dict[str, str] | None = None
        for line in section.split("\n"):
            gm = g_pattern.match(line)
            if gm:
                if current_rec:
                    g_records.append(current_rec)
                current_rec = {
                    "G_tag": f"G{gm.group(1)}",
                    "G_number": gm.group(1),
                    "EO_number": gm.group(2),
                    "EO_title": gm.group(3).strip(),
                }
            elif current_rec:
                fm = field_pattern.match(line.strip())
                if fm:
                    key = fm.group(1).replace(" ", "_").lower()
                    current_rec[key] = fm.group(2).strip()
        if current_rec:
            g_records.append(current_rec)

        if g_records:
            csv_buf = io.StringIO()
            fieldnames = ["G_tag", "G_number", "EO_number", "EO_title",
                          "action_type", "description", "official", "date", "source_url"]
            writer = csv.DictWriter(csv_buf, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(g_records)
            (artifact_dir / "grounding_events_index.csv").write_text(csv_buf.getvalue(), encoding="utf-8")
            manifest["grounding_events_csv"] = "grounding_events_index.csv"

    # ── 12. Article Reference Index (appendix) ────────────────────────
    section = _find_section(r"article\s+reference\s+index", level=1)
    if section:
        (artifact_dir / "article_reference_index.md").write_text(section, encoding="utf-8")
        manifest["article_reference_md"] = "article_reference_index.md"
        tables = _extract_tables(section)
        if tables:
            csv_text = _md_table_to_csv(tables[0])
            (artifact_dir / "article_reference_index.csv").write_text(csv_text, encoding="utf-8")
            manifest["article_reference_csv"] = "article_reference_index.csv"

    # ── 13. EO Policy Documents (appendix) ────────────────────────────
    section = _find_section(r"executive\s+order\s+policy\s+documents", level=1)
    if section:
        (artifact_dir / "eo_policy_documents.md").write_text(section, encoding="utf-8")
        manifest["eo_policy_documents"] = "eo_policy_documents.md"

    # ── 14. Reasoning content ─────────────────────────────────────────
    if reasoning_content:
        reasoning_text = "\n\n---\n\n".join(
            f"## Reasoning Block {i}\n\n{block.strip()}"
            for i, block in enumerate(reasoning_content, 1)
        )
        (artifact_dir / "reasoning_content.md").write_text(reasoning_text, encoding="utf-8")
        manifest["reasoning_content"] = "reasoning_content.md"

    # ── 15. Inline grounding citations used in LLM output ─────────────
    # Extract all [G###: ...] inline citations from the LLM body (before appendices)
    appendix_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith("# APPENDIX:"):
            appendix_start = i
            break
    body_text = "\n".join(lines[:appendix_start]) if appendix_start else content
    g_inline = re.findall(
        r"\[G(\d+):\s+(\d{4}-\d{2}-\d{2})\s+[—–-]\s+(.+?)\]",
        body_text,
    )
    if g_inline:
        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow(["G_number", "date", "description_snippet"])
        for g_num, g_date, g_desc in g_inline:
            writer.writerow([f"G{g_num}", g_date, g_desc.strip()])
        (artifact_dir / "grounding_citations_inline.csv").write_text(csv_buf.getvalue(), encoding="utf-8")
        manifest["grounding_citations_inline"] = "grounding_citations_inline.csv"

    # ── Write manifest ────────────────────────────────────────────────
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(
        _json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8",
    )

    return artifact_dir


def _get_llm_rates(model_name: str, input_tokens: int) -> tuple[float, float]:
    """Get per-1M-token rates for a model, accounting for tiered pricing."""
    rates = LLM_PRICING.get(model_name, {})
    # Gemini 3.1 Pro / 2.5 Pro have tiered pricing at 200K boundary
    if input_tokens <= 200_000 and "input_short" in rates:
        return rates["input_short"], rates["output_short"]
    return rates.get("input", 0), rates.get("output", 0)


# ---------------------------------------------------------------------------
# Domain greylist — domains excluded from clustering input
# ---------------------------------------------------------------------------
_GREYLIST_DOMAINS: set[str] = {
    # Government / military primary sources
    "dvidshub.net",
    # Reference / encyclopaedic — not media coverage
    "wikipedia.org",
    "en.wikipedia.org",
    "britannica.com",
    "www.britannica.com",
    "encyclopedia.com",
}

# TLD suffixes that should be excluded regardless of specific domain
_GREYLIST_SUFFIXES: tuple[str, ...] = (
    ".gov",
    ".mil",
    ".gov.uk",
    ".gov.au",
    ".gov.ca",
    ".gc.ca",
)


def _is_greylisted(url: str) -> bool:
    """Return True if the URL domain is on the clustering greylist.

    Excludes .gov/.mil primary sources, reference/encyclopaedic sites
    (Wikipedia, Britannica), and foreign government domains.
    """
    from urllib.parse import urlparse
    try:
        host = (urlparse(url).hostname or "").lower()
    except Exception:
        return False
    # Check exact domain or parent domain match
    if host in _GREYLIST_DOMAINS:
        return True
    # Check if any greylist domain is a suffix (e.g. en.wikipedia.org)
    for domain in _GREYLIST_DOMAINS:
        if host.endswith("." + domain):
            return True
    # Check TLD suffixes (.gov, .mil, .gov.uk, etc.)
    for suffix in _GREYLIST_SUFFIXES:
        if host.endswith(suffix):
            return True
    return False


# Keep legacy alias for any external callers
_is_gov_mil = _is_greylisted


def _load_prior_analysis(category: str) -> str | None:
    """Load the most recent (n-1) Phase 4 analysis for a category from working_dir."""
    import glob
    pattern = str(WORKING_DIR / f"phase4_{category}_*.md")
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    # The latest file is the last in sorted order (timestamp in filename)
    prior_path = Path(files[-1])
    try:
        content = prior_path.read_text(encoding="utf-8")
        # Strip the metadata header (everything before the first ---\n\n)
        # to get just the analysis text
        marker = "---\n\n"
        idx = content.find(marker)
        if idx >= 0:
            content = content[idx + len(marker):]
        return content
    except Exception:
        return None


# Per-category BM25 query definitions — tested in demo_article_retriever.py
_CATEGORY_BM25_QUERIES: dict[str, list[dict]] = {
    "culture_and_media": [
        dict(topic="culture_and_media", person="Hegseth"),
        dict(topic="culture_and_media", person="JD Vance"),
        dict(topic="culture_and_media", person="Dan Caine"),
        dict(topic="culture_and_media", person="Steven Feinberg"),
        dict(topic="culture_and_media", person="Christopher Mahoney"),
        dict(topic="culture_and_media", person="Robert Salesses"),
    ],
    "artificial_intelligence": [
        dict(topic="artificial_intelligence", person="Elon Musk"),
        dict(topic="artificial_intelligence", person="Sam Altman"),
        dict(topic="artificial_intelligence", person="Dario Amodei"),
        dict(topic="artificial_intelligence", person="Hegseth"),
        dict(topic="artificial_intelligence", person="Dan Caine"),
        dict(topic="artificial_intelligence", person="Steven Feinberg"),
        dict(topic="artificial_intelligence", person="Christopher Mahoney"),
        dict(topic="artificial_intelligence", person="Robert Salesses"),
        dict(topic="artificial_intelligence", title=["AI", "Artificial Intelligence"]),
        dict(topic="artificial_intelligence", title=["AI", "Golden Dome"]),
    ],
    "national_security_domestic": [
        dict(title="Border Cartels Immigrants"),
        dict(topic="national_security_domestic", operation=["Illegal Immigration", "Southern Border"], require_all=True),
        dict(operation="Illegal Immigration", title="ICE"),
        dict(title="intelligence", person="Tulsi Gabbard"),
        dict(title="intelligence", person="Kash Patel"),
        dict(topic="national_security_domestic", person="Hegseth"),
        dict(topic="national_security_domestic", person="Dan Caine"),
        dict(topic="national_security_domestic", person="Steven Feinberg"),
        dict(topic="national_security_domestic", person="Christopher Mahoney"),
        dict(topic="national_security_domestic", person="Robert Salesses"),
    ],
    "national_security_foreign": [
        dict(topic="national_security_foreign", title="Russia Ukraine", person="Trump", country=["Russia", "Ukraine"]),
        dict(topic="national_security_foreign", title="Russia Ukraine", person="Zelensky", country=["Russia", "Ukraine"]),
        dict(topic="national_security_foreign", title="Russia Ukraine", person="Putin", country=["Russia", "Ukraine"]),
        dict(topic="national_security_foreign", title="Greenland", person="Trump", country=["Greenland", "Norway"]),
        dict(topic="national_security_foreign", title="Nigeria", person="Trump", country=["Nigeria"]),
        dict(topic="national_security_foreign", title="Nigeria", person="Dan Caine", country=["Nigeria"]),
        dict(topic="national_security_foreign", person="Trump", country=["Iraq", "Qatar"]),
        dict(topic="national_security_foreign", person="Dan Caine", country=["Iraq", "Qatar"]),
        dict(topic="national_security_foreign", title="NATO", person="Trump", country=["EU", "Russia", "Ukraine"]),
        dict(topic="national_security_foreign", person="Trump", country=["China", "Qatar"]),
        dict(topic="national_security_foreign", person="Hegseth"),
        dict(topic="national_security_foreign", person="Steven Feinberg"),
        dict(topic="national_security_foreign", person="Christopher Mahoney"),
        dict(topic="national_security_foreign", person="Robert Salesses"),
        dict(operation="Epic Fury", country="Iran"),
        dict(operation="Midnight Hammer", country="Iran"),
        dict(operation="Rough Rider", country="Yemen", title="Houthis"),
    ],
    "energy_and_minerals": [
        dict(topic=["energy", "federal_procurement"], country=["China", "Canada", "Mexico"]),
        dict(topic=["energy", "federal_procurement"], title=["oil"]),
        dict(topic=["energy", "federal_procurement"], title=["gas"]),
        dict(topic=["energy", "federal_procurement"], title=["phosphorous"]),
        dict(topic=["energy", "federal_procurement"], person="Hegseth"),
        dict(topic=["energy", "federal_procurement"], person="Dan Caine"),
        dict(topic=["energy", "federal_procurement"], person="Steven Feinberg"),
        dict(topic=["energy", "federal_procurement"], person="Christopher Mahoney"),
        dict(topic=["energy", "federal_procurement"], person="Robert Salesses"),
    ],
    "federal_procurement": [
        dict(topic="artificial_intelligence", title=["AI", "Golden Dome", "Anthropic", "OpenAI", "Google", "Microsoft"], person="Elon Musk"),
        dict(topic="artificial_intelligence", title=["AI", "Golden Dome", "Anthropic", "OpenAI", "Google", "Microsoft"], person="Sam Altman"),
        dict(topic="artificial_intelligence", title=["AI", "Golden Dome", "Anthropic", "OpenAI", "Google", "Microsoft"], person="Dario Amodei"),
        dict(topic="artificial_intelligence", title=["AI", "Golden Dome", "Anthropic", "OpenAI", "Google", "Microsoft"], person="Gates"),
        dict(topic="federal_procurement", person="Hegseth"),
        dict(topic="federal_procurement", person="Dan Caine"),
        dict(topic="federal_procurement", person="Steven Feinberg"),
        dict(topic="federal_procurement", person="Christopher Mahoney"),
        dict(topic="federal_procurement", person="Robert Salesses"),
    ],
}


def _inject_bm25_grounding(bundle: dict, category: str) -> tuple[list[dict], "ArticleRetriever"]:
    """Retrieve tagged unlinked articles via BM25 and inject into the evidence bundle.

    Runs the category-specific queries defined in _CATEGORY_BM25_QUERIES,
    deduplicates across queries and against existing bundle URLs.
    Returns (new_articles, retriever) — the retriever is reused for cluster supplementation.
    """
    _hr(f"BM25 Grounding Injection [{category}]")

    retriever = ArticleRetriever.from_tagged_files()
    print(f"  Loaded {retriever.article_count} tagged articles ({retriever.source_counts()})")

    queries = _CATEGORY_BM25_QUERIES.get(category, [dict(topic=category)])
    print(f"  Running {len(queries)} targeted BM25 quer{'y' if len(queries) == 1 else 'ies'} for [{category}]")

    # Deduplicate against existing bundle URLs
    existing_urls = {a.get("url", "") for a in bundle.get("articles", [])}
    seen_urls: set[str] = set(existing_urls)
    new_articles: list[dict] = []

    for i, qkwargs in enumerate(queries, 1):
        results = retriever.query(source_filter="unlinked", top_k=100, **qkwargs)
        added = 0
        for art in results:
            url = art.get("url", "")
            if url and url not in seen_urls:
                bundle_art = _bm25_to_bundle_article(art)
                new_articles.append(bundle_art)
                seen_urls.add(url)
                added += 1
        # Compact query description
        desc = ", ".join(f"{k}={v!r}" for k, v in qkwargs.items())
        print(f"    Q{i}: {len(results)} hits, {added} new  ({desc})")

    # Inject into bundle
    bundle["articles"].extend(new_articles)
    bundle["article_count"] = len(bundle["articles"])

    print(f"  → {len(new_articles)} new articles injected (deduplicated against {len(existing_urls)} existing)")
    print(f"  → Bundle now: {bundle['article_count']} total articles")

    return new_articles, retriever


# BBOA media cache for enriching BM25 articles (merged index + cache dirs)
_BBOA_MERGED_LINKED = Path.cwd() / "x_indices_merged" / "linked_media_combined.jsonl"
_BBOA_MERGED_UNLINKED = Path.cwd() / "x_indices_merged" / "unlinked_media_combined.jsonl"

_bboa_url_lookup: dict[str, dict] | None = None


def _get_bboa_url_lookup() -> dict[str, dict]:
    """Lazy-load BBOA media analysis keyed by URL for media_framing enrichment.

    Reads from both the merged index media files AND the 4 BBOA cache
    session dirs (linked + unlinked media).  Later sources overwrite
    earlier ones so the most recent analysis wins.
    """
    global _bboa_url_lookup
    if _bboa_url_lookup is not None:
        return _bboa_url_lookup
    _bboa_url_lookup = {}

    # 1) Merged index media files (baseline)
    for path in (_BBOA_MERGED_LINKED, _BBOA_MERGED_UNLINKED):
        if not path.exists():
            continue
        with open(path) as fh:
            for line in fh:
                rec = json.loads(line)
                url = rec.get("link", "")
                if url:
                    _bboa_url_lookup[url] = rec

    # 2) BBOA cache session dirs (later = higher priority, overwrites)
    for cache_dir in BBAO_CACHE_SOURCES:
        cache_path = Path(cache_dir)
        for pattern in ("linked_media_*.jsonl", "unlinked_media_*.jsonl"):
            for jf in sorted(cache_path.glob(pattern)):
                with open(jf) as fh:
                    for line in fh:
                        try:
                            rec = json.loads(line)
                            url = rec.get("link", "")
                            if url and rec.get("media_framing"):
                                _bboa_url_lookup[url] = rec
                        except (json.JSONDecodeError, KeyError):
                            continue
    return _bboa_url_lookup


def _bm25_to_bundle_article(art: dict) -> dict:
    """Convert a BM25 result dict to bundle article format.

    Enriches with media_framing and consumer_salience from the BBOA
    merged index cache when available.
    """
    bundle_art = {
        "url": art.get("url", ""),
        "title": art.get("title", ""),
        "publisher": art.get("publisher", ""),
        "date_publication": art.get("date", ""),
        "source_type": f"bm25_{art.get('source', 'unknown')}",
        "reconstituted": False,
        "full_text": "",
        "summary_text": "",
        "tags": art.get("tags", {}),
        "bm25_score": art.get("_score", 0),
    }
    content = art.get("content", "")
    if content:
        idx = content.find("## Summary")
        snippet = content[idx + 10:].strip()[:1000] if idx >= 0 else content[:500]
        bundle_art["summary_text"] = snippet
    rationale = art.get("linkage_rationale", "")
    if rationale:
        bundle_art["linkage_rationale"] = rationale

    # Enrich with BBOA media_framing + risk from merged cache
    bboa = _get_bboa_url_lookup().get(art.get("url", ""), {})
    if bboa:
        bundle_art["media_framing"] = bboa.get("media_framing", {})
        bundle_art["consumer_salience"] = bboa.get("consumer_salience")
        bundle_art["mean_search_engine_rank"] = bboa.get("mean_search_engine_rank")
    return bundle_art


def _supplement_clusters_bm25(
    cluster_result: dict,
    retriever: "ArticleRetriever",
    category: str,
    per_cluster_k: int = 20,
) -> dict:
    """Supplement each Phase 4 cluster with additional unlinked articles from BM25.

    For each cluster, extracts persons/countries/operations from existing articles
    and the cluster description, then queries BM25 for matching unlinked articles.
    New articles are deduplicated across all clusters.
    """
    _hr(f"BM25 Cluster Supplementation [{category}]")

    clusters = cluster_result.get("clusters", [])
    if not clusters:
        print("  No clusters to supplement")
        return cluster_result

    # Collect all URLs already in any cluster
    all_cluster_urls: set[str] = set()
    for c in clusters:
        for a in c.get("articles", []):
            if a.get("url"):
                all_cluster_urls.add(a["url"])

    total_added = 0

    for cluster in clusters:
        cid = cluster["cluster_id"]
        desc = cluster.get("cluster_description", "")
        existing_articles = cluster.get("articles", [])

        # Extract search signals from cluster's existing articles
        persons: set[str] = set()
        countries: set[str] = set()
        operations: set[str] = set()
        for a in existing_articles:
            tags = a.get("tags", {})
            persons.update(tags.get("person", []))
            countries.update(tags.get("country", []))
            operations.update(tags.get("operation", []))

        # Build query from cluster signals
        query_kwargs: dict = {"source_filter": "unlinked", "top_k": per_cluster_k}

        # Always include the category topic
        query_kwargs["topic"] = category

        # Use cluster description words as title search
        if desc:
            # Take the first ~6 meaningful words from the description
            desc_words = [w for w in desc.split() if len(w) > 3][:6]
            if desc_words:
                query_kwargs["title"] = " ".join(desc_words)

        # Add tag-based signals if present
        if persons:
            query_kwargs["person"] = list(persons)[:5]
        if countries:
            query_kwargs["country"] = list(countries)[:5]
        if operations:
            query_kwargs["operation"] = list(operations)[:3]

        results = retriever.query(**query_kwargs)

        # Deduplicate against all cluster URLs
        new_for_cluster = []
        for art in results:
            url = art.get("url", "")
            if url and url not in all_cluster_urls:
                bundle_art = _bm25_to_bundle_article(art)
                bundle_art["source_type"] = "bm25_cluster_supplement"
                new_for_cluster.append(bundle_art)
                all_cluster_urls.add(url)

        # Inject into cluster
        cluster["articles"].extend(new_for_cluster)
        cluster["article_count"] = len(cluster["articles"])
        total_added += len(new_for_cluster)

        print(f"  {cid}: {len(existing_articles)} existing + {len(new_for_cluster)} new = {cluster['article_count']} total")
        if new_for_cluster:
            for a in new_for_cluster[:3]:
                print(f"    + {a.get('title', 'Untitled')[:70]}")
            if len(new_for_cluster) > 3:
                print(f"    ... +{len(new_for_cluster) - 3} more")

    # Update totals
    all_urls = set()
    for c in clusters:
        for a in c.get("articles", []):
            if a.get("url"):
                all_urls.add(a["url"])
    cluster_result["article_count"] = len(all_urls)

    print(f"\n  → {total_added} total articles supplemented across {len(clusters)} clusters")
    print(f"  → {len(all_urls)} unique articles in cluster result")

    return cluster_result


def run_phase4(
    bundle: dict,
    category: str,
    live: bool = False,
    model_name: str = "gpt-5.4",
    grounding_events: list[dict] | None = None,
    eo_policy_texts: dict[str, dict] | None = None,
    prior_analysis: str | None = None,
    measure_only: bool = False,
) -> dict:
    _hr(f"Phase 4: Threat Cluster Analysis [{category}]")

    # Filter out greylisted domains — .gov/.mil, Wikipedia, Britannica, etc.
    all_articles = bundle.get("articles", [])
    greylisted = [a for a in all_articles if _is_greylisted(a.get("url", ""))]
    media_articles = [a for a in all_articles if not _is_greylisted(a.get("url", ""))]

    # Split into linked, BM25-queried, and remaining unlinked
    linked = [a for a in media_articles if a.get("source_type") == "linked"]
    bm25_queried = [a for a in media_articles if (a.get("source_type") or "").startswith("bm25_")]
    unlinked = [a for a in media_articles if a.get("source_type") != "linked" and not (a.get("source_type") or "").startswith("bm25_")]

    import random
    from datetime import datetime
    rng = random.Random(category)          # deterministic per-category seed

    # --- Time-window filter: restrict unlinked to the date range of linked articles ---
    linked_dates = []
    for a in linked:
        d = a.get("date", "") or a.get("article_date", "")
        if d:
            try:
                linked_dates.append(datetime.strptime(d[:10], "%Y-%m-%d"))
            except (ValueError, TypeError):
                pass

    if linked_dates:
        earliest = max(min(linked_dates).replace(day=1), datetime(2025, 1, 1))  # hard floor: Jan 2025
        latest_raw = max(linked_dates)
        if latest_raw.month == 12:
            latest = latest_raw.replace(year=latest_raw.year + 1, month=1, day=1)
        else:
            latest = latest_raw.replace(month=latest_raw.month + 1, day=1)  # ceil to 1st of next month

        def _in_window(art: dict) -> bool:
            d = art.get("date", "") or art.get("article_date", "")
            if not d:
                return False
            try:
                dt = datetime.strptime(d[:10], "%Y-%m-%d")
                return earliest <= dt <= latest
            except (ValueError, TypeError):
                return False

        unlinked_in_window = [a for a in unlinked if _in_window(a)]
        window_label = f"{earliest.strftime('%Y-%m')} → {latest.strftime('%Y-%m')}"
    else:
        unlinked_in_window = unlinked
        window_label = "no linked dates — no filter"

    # Token-based packing: build full prompt, measure with tiktoken, trim if needed
    TOKEN_LIMIT = 900_000

    import tiktoken
    _enc = tiktoken.encoding_for_model("gpt-4o")  # o200k_base

    analyser = ThreatClusterAnalyser()

    # Start with all essential articles + small random buffer
    sample_pool = min(150, len(unlinked_in_window)) if unlinked_in_window else 0
    unlinked_sample = rng.sample(unlinked_in_window, sample_pool) if sample_pool else []

    prompt_articles = linked + bm25_queried + unlinked_sample

    # Sort buffer by date descending so we trim latest first
    unlinked_sample.sort(
        key=lambda a: a.get("date_published") or a.get("date") or "",
        reverse=True,
    )
    # Sort BM25 by relevance ascending so we trim lowest-relevance first
    bm25_queried.sort(key=lambda a: a.get("bm25_score", 0))

    # Build prompt once, measure, then batch-trim if needed
    def _build_and_measure(arts):
        _b = dict(bundle)
        _b["articles"] = arts
        _b["article_count"] = len(arts)
        _p = analyser.build_prompt(
            category, _b,
            grounding_events=grounding_events,
            prior_analysis=prior_analysis,
        )
        return _p, len(_enc.encode(_p))

    prompt_articles = linked + bm25_queried + unlinked_sample
    _trial_prompt, _prompt_tokens = _build_and_measure(prompt_articles)

    if _prompt_tokens > TOKEN_LIMIT:
        overshoot = _prompt_tokens - TOKEN_LIMIT
        print(f"\n[Token Trimming] {_prompt_tokens:,} tokens > {TOKEN_LIMIT:,} limit (over by {overshoot:,})")

        # Estimate per-article token cost for each trimmable pool
        # Use conservative estimate: overshoot / avg_tokens_per_article * 1.2 safety margin
        trimmed_buffer = 0
        trimmed_bm25 = 0

        # Phase 1: trim buffer articles (latest date first)
        if unlinked_sample and overshoot > 0:
            # Estimate tokens per buffer article
            buf_token_est = []
            for a in unlinked_sample:
                art_text = (a.get("full_text") or "") + (a.get("summary_text") or "")
                buf_token_est.append(len(_enc.encode(art_text)) + 200)
            # Drop from front (latest date) until estimated overshoot covered
            running = 0
            cut_idx = 0
            for i, t in enumerate(buf_token_est):
                running += t
                cut_idx = i + 1
                if running >= overshoot * 1.1:  # 10% safety margin
                    break
            trimmed_buffer = cut_idx
            unlinked_sample = unlinked_sample[cut_idx:]
            overshoot -= running
            print(f"  → Dropped {trimmed_buffer} buffer articles (est {running:,} tokens)")

        # Phase 2: trim BM25 articles (lowest relevance first)
        if bm25_queried and overshoot > 0:
            bm25_token_est = []
            for a in bm25_queried:
                art_text = (a.get("full_text") or "") + (a.get("summary_text") or "")
                bm25_token_est.append(len(_enc.encode(art_text)) + 200)
            running = 0
            cut_idx = 0
            for i, t in enumerate(bm25_token_est):
                running += t
                cut_idx = i + 1
                if running >= overshoot * 1.1:
                    break
            trimmed_bm25 = cut_idx
            bm25_queried = bm25_queried[cut_idx:]
            print(f"  → Dropped {trimmed_bm25} BM25 articles (est {running:,} tokens)")

        # Rebuild and verify
        prompt_articles = linked + bm25_queried + unlinked_sample
        _trial_prompt, _prompt_tokens = _build_and_measure(prompt_articles)
        print(f"  → After trim: {_prompt_tokens:,} tokens ({len(prompt_articles)} articles)")

        # If still over (estimation error), do a second pass one-at-a-time
        _extra_dropped = 0
        while _prompt_tokens > TOKEN_LIMIT and bm25_queried:
            bm25_queried.pop(0)
            _extra_dropped += 1
            prompt_articles = linked + bm25_queried + unlinked_sample
            _trial_prompt, _prompt_tokens = _build_and_measure(prompt_articles)
        if _extra_dropped:
            print(f"  → Fine-tune: dropped {_extra_dropped} more BM25 articles → {_prompt_tokens:,} tokens")

    print(f"\n[Prompt Packing — Token Budget]")
    print(f"  → Final prompt: {_prompt_tokens:,} tokens ({len(prompt_articles)} articles)")
    print(f"  → Token limit: {TOKEN_LIMIT:,}")
    print(f"  → Linked: {len(linked)}, BM25: {len(bm25_queried)}, Buffer: {len(unlinked_sample)}")

    if measure_only:
        print(f"\n[Measure Only] Skipping LLM call. Prompt: {_prompt_tokens:,} tokens, {len(prompt_articles)} articles.")
        return {"clusters": [], "meta": {"measure_only": True, "prompt_tokens": _prompt_tokens, "articles": len(prompt_articles)}}

    media_bundle = dict(bundle)
    media_bundle["articles"] = prompt_articles
    media_bundle["article_count"] = len(prompt_articles)

    note = f" window={window_label}, {len(unlinked_in_window)}/{len(unlinked)} in window"
    # Report greylisted domains for transparency
    from collections import Counter
    from urllib.parse import urlparse as _urlparse
    grey_domains = Counter()
    for a in greylisted:
        try:
            h = (_urlparse(a.get("url", "")).hostname or "").lower()
            grey_domains[h] += 1
        except Exception:
            grey_domains["(parse error)"] += 1

    print(f"\n[Source Filtering — Domain Greylist]")
    print(f"  → {len(all_articles)} total articles in evidence bundle")
    print(f"  → {len(greylisted)} greylisted sources EXCLUDED:")
    for dom, cnt in grey_domains.most_common():
        print(f"    • {dom}: {cnt}")
    print(f"  → {len(media_articles)} media articles after greylist filter")
    print(f"    • {len(linked)} linked articles (all included)")
    print(f"    • {len(bm25_queried)} BM25-queried articles (all included)")
    print(f"    • {len(unlinked)} remaining unlinked articles, {note}")
    print(f"    • {len(unlinked_sample)} randomly sampled from date window (+150 buffer)")
    print(f"  → {len(prompt_articles)} total articles sent to LLM")

    # Count enrichment features
    with_rationale = [a for a in prompt_articles if a.get("linkage_rationale")]
    with_events = [a for a in prompt_articles if a.get("relevant_events")]
    with_framing = [a for a in prompt_articles if a.get("media_framing")]
    print(f"\n[Article Enrichment]")
    print(f"  → {len(with_rationale)} articles with linkage rationale")
    print(f"  → {len(with_events)} articles with relevant events")
    print(f"  → {len(with_framing)} articles with media framing")

    if grounding_events:
        print(f"\n[Context Injection]")
        print(f"  → {len(grounding_events)} EO grounding events injected into prompt")
    if eo_policy_texts:
        print(f"  → {len(eo_policy_texts)} EO policy documents injected into prompt")
    if prior_analysis:
        print(f"  → Prior analysis (n-1) injected: {len(prior_analysis):,} chars")

    # Ranked article char counts
    print(f"\n[Article Char Counts — Ranked by full_text length]")
    article_sizes = []
    for a in prompt_articles:
        ft_len = len(a.get("full_text") or "")
        article_sizes.append((ft_len, a.get("source_type", ""), (a.get("title") or "Untitled")[:80], a.get("url", "")))
    article_sizes.sort(key=lambda x: x[0], reverse=True)
    total_chars = 0
    for rank, (sz, stype, title, url) in enumerate(article_sizes, 1):
        total_chars += sz
        domain = url.split("/")[2] if url.count("/") >= 2 else url
        print(f"  {rank:3}. {sz:>8,} chars [{stype:8}] {domain[:30]:30} {title}")
    print(f"  {'':3}  {'--------':>8}")
    print(f"  {'':3}  {total_chars:>8,} chars TOTAL ({total_chars // 4:,} est. tokens)")

    analyser = ThreatClusterAnalyser()
    prompt = analyser.build_prompt(
        category, media_bundle,
        grounding_events=grounding_events,
        prior_analysis=prior_analysis,
    )

    # Dump prompt to PROMPT_INSPECTION.md if env var is set
    import os as _os
    _prompt_dump = _os.environ.get("DUMP_PROMPT")
    if _prompt_dump:
        Path(_prompt_dump).write_text(prompt, encoding="utf-8")
        print(f"\n[Prompt Dump] Written to {_prompt_dump} ({len(prompt):,} chars)")

    print(f"\n[LLM Analysis]")
    print(f"  → Prompt: {len(prompt):,} characters, {media_bundle['article_count']} media articles")
    print(f"  → Model: {model_name}")

    # Estimate input tokens (rough: 4 chars per token)
    estimated_input_tokens = len(prompt) // 4
    in_rate, out_rate = _get_llm_rates(model_name, estimated_input_tokens)
    estimated_cost = estimated_input_tokens / 1_000_000 * in_rate
    print(f"  → Estimated input: ~{estimated_input_tokens:,} tokens")
    print(f"  → Estimated input cost: ~${estimated_cost:.4f} (rate: ${in_rate}/1M)")

    llm_output = ""
    reasoning_content: list[str] = []
    usage_stats = {"input_tokens": 0, "output_tokens": 0, "reasoning_tokens": 0, "cost_usd": 0.0}

    if live:
        # Skip LLM call if real phase 4 markdown already exists
        import glob
        real_md_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{category}_*.md")))
        if real_md_files:
            real_md_path = real_md_files[-1]
            llm_output = Path(real_md_path).read_text()
            print(f"  → Reusing existing phase 4 markdown: {real_md_path} ({len(llm_output):,} chars)")
            live = False  # skip LLM, fall through to parse

    if live:
        print(f"  → Calling {model_name}...")
        try:
            import asyncio

            if model_name.startswith("gpt-"):
                from src.clients.llm_openai_client import OpenAILLMClient
                client = OpenAILLMClient(model_name=model_name)
                call_kwargs = dict(
                    reasoning_effort="high",
                    verbosity="high",
                    max_tokens=64_000,
                )
            else:
                from src._clients.llm_gcp_client import GeminiLLMClient
                client = GeminiLLMClient(model_name=model_name)
                call_kwargs = dict(
                    temperature=0.5,
                    max_tokens=64_000,
                )

            loop = asyncio.get_event_loop()
            response = loop.run_until_complete(
                client.generate_content_with_usage(
                    prompt,
                    **call_kwargs,
                )
            )

            llm_output = response["text"]
            usage_stats["input_tokens"] = response["usage"]["input_tokens"]
            usage_stats["output_tokens"] = response["usage"]["output_tokens"]
            usage_stats["reasoning_tokens"] = response["usage"].get("reasoning_tokens", 0)

            # Capture reasoning content (chain-of-thought evidence)
            reasoning_content = response.get("reasoning_content", [])

            # Calculate cost from actual usage (tiered)
            in_rate, out_rate = _get_llm_rates(model_name, usage_stats["input_tokens"])
            usage_stats["cost_usd"] = (
                usage_stats["input_tokens"] / 1_000_000 * in_rate
                + usage_stats["output_tokens"] / 1_000_000 * out_rate
            )

            print(f"  ✓ Response received")
            print(f"    • Input tokens:    {usage_stats['input_tokens']:,}")
            print(f"    • Output tokens:   {usage_stats['output_tokens']:,}")
            print(f"    • Reasoning tokens: {usage_stats['reasoning_tokens']:,}")
            print(f"    • Reasoning blocks: {len(reasoning_content)}")
            print(f"    • Cost:            ${usage_stats['cost_usd']:.6f} (in=${in_rate}/1M, out=${out_rate}/1M)")

        except Exception as e:
            print(f"  ⚠ LLM call failed: {e}")
            print(f"  → Falling back to synthetic response")
            live = False

    if not live:
        print(f"  → DRY RUN mode: generating synthetic response")
        # Try to load a real phase 4 markdown from working_dir first
        import glob
        real_md_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{category}_*.md")))
        if real_md_files:
            real_md_path = real_md_files[-1]  # most recent
            llm_output = Path(real_md_path).read_text()
            print(f"  → Loaded real phase 4 markdown: {real_md_path} ({len(llm_output):,} chars)")
        else:
            # Build synthetic clusters of varying sizes so filtering is visible
            articles = media_bundle["articles"]
            cluster_defs = [
                ("Convergent Critical Coverage", 15),
                ("Editorial Alignment on Policy", 5),
                ("Institutional Narrative Framing", 12),
            ]
            llm_output = (
                f"## Threat Cluster Analysis: {category.replace('_', ' ').title()}\n\n"
                f"Analysis of {media_bundle['article_count']} media articles "
                f"reveals the following patterns:\n\n"
            )
            idx = 0
            for cnum, (desc, size) in enumerate(cluster_defs, 1):
                chunk = articles[idx:idx + size]
                if not chunk:
                    break
                llm_output += (
                    f"# Cluster {cnum} — {desc}\n\n"
                    f"Multiple publications demonstrate aligned framing...\n\n"
                    f"| Author | Date | Publication | Title | URL |\n"
                    f"|---|---|---|---|---|\n"
                )
                for a in chunk:
                    llm_output += (
                        f"| {a.get('author', 'Unknown')} "
                        f"| {a.get('date_publication', '')} "
                        f"| {a.get('listed_publisher') or a.get('publisher', '')} "
                        f"| {a.get('title', 'Untitled')} "
                        f"| {a.get('url', '')} |\n"
                    )
                llm_output += "\n"
                idx += size

    # ── Resolve citation markers → hyperlinked author-date references ──
    llm_output, art_cites, eo_cites = _resolve_article_citations(
        llm_output,
        media_bundle.get("articles", []),
        eo_policy_texts=eo_policy_texts,
    )
    if art_cites or eo_cites:
        print(f"\n[Citation Resolution]")
        print(f"  → {art_cites} article citations resolved (art_N, cluster) → [Author, Month Year](url)")
        print(f"  → {eo_cites} EO citations resolved (eo_N, eo) → [Executive Order N](url)")
        # Count any unresolved markers remaining
        import re as _re_cite
        unresolved_art = len(_re_cite.findall(r'\(art_\d+,\s*cluster\)', llm_output))
        unresolved_eo = len(_re_cite.findall(r'\(eo_\d+,\s*eo\)', llm_output))
        if unresolved_art or unresolved_eo:
            print(f"  ⚠ {unresolved_art} article + {unresolved_eo} EO citations unresolved (missing metadata)")

    # Skip writing if LLM returned zero tokens (failed / empty response)
    if usage_stats.get("input_tokens", 0) == 0 and usage_stats.get("output_tokens", 0) == 0:
        print("  ⚠ Skipping file write: input and output tokens are both 0")
    else:
        # Write output to markdown in working_dir (and cache)
        bboa_md_path: Path | None = None
        for dest in (WORKING_DIR, CACHE_DIR):
            md_path = _write_phase4_markdown(
                category, model_name, llm_output, usage_stats, media_bundle, dest,
                grounding_events=grounding_events,
                eo_policy_texts=eo_policy_texts,
                reasoning_content=reasoning_content,
            )
            print(f"  ✓ Saved: {md_path}")
            if dest == WORKING_DIR:
                bboa_md_path = md_path

        # Post-process: extract artifacts only to BBOA cache
        if bboa_md_path is not None:
            artifact_dir = _extract_phase4_artifacts(
                bboa_md_path, reasoning_content=reasoning_content,
            )
            n_artifacts = len(list(artifact_dir.iterdir()))
            print(f"  ✓ Artifacts: {artifact_dir} ({n_artifacts} files)")

    result = analyser.parse_result(category, llm_output, media_bundle)
    result["usage"] = usage_stats

    # URL linkage: verify URLs survive into the parsed result
    result_urls = set()
    for cluster in result.get("clusters", []):
        for a in cluster.get("articles", []):
            if a.get("url"):
                result_urls.add(a["url"])
    bundle_urls = {a["url"] for a in bundle.get("articles", []) if a.get("url")}

    log.info("")
    log.info("--- URL Linkage (Phase 4) ---")
    log.info("  URLs in evidence bundle (input): %d", len(bundle_urls))
    log.info("  URLs in parsed result (output): %d", len(result_urls))
    log.info("  Clusters parsed: %d", result.get("cluster_count", 0))
    for cluster in result.get("clusters", []):
        log.info("    %s: %s — %d articles",
                 cluster["cluster_id"],
                 cluster["cluster_description"][:60],
                 cluster["article_count"])
    log.info(
        "  Lineage: %s", json.dumps(result["lineage"]),
    )
    log.info("Phase 4 PASSED — %d clusters, %d URLs parsed from LLM output",
             result.get("cluster_count", 0), len(result_urls))

    _validate_cluster_fidelity(llm_output, result, category)

    return result


def _validate_cluster_fidelity(
    md_text: str,
    cluster_result: dict,
    category: str,
) -> None:
    """Cross-reference the markdown document against parsed clusters.

    Raises RuntimeError if the parsed output does not faithfully
    represent the clusters in the document.  Called before Phase 5/6
    to guarantee that what Kaliya receives matches the document.
    """
    import re as _re

    # 1. Count cluster headings in the raw markdown
    heading_pat = _re.compile(
        r'^#{1,2}\s+(?:\d+\.\s+)?Cluster\s+([A-Za-z0-9]{1,2})\s*[—–-]\s*(.+?)$',
        _re.MULTILINE,
    )
    md_clusters = heading_pat.findall(md_text)
    md_ids = {g[0] for g in md_clusters}

    # 2. Parsed cluster IDs
    parsed = cluster_result.get("clusters", [])
    parsed_ids = set()
    for c in parsed:
        # cluster_id is 'cluster_X' — extract X
        raw_id = c["cluster_id"].replace("cluster_", "")
        parsed_ids.add(raw_id)

    # 3. Every cluster heading in the document should appear in the parsed output
    missing = md_ids - parsed_ids
    extra = parsed_ids - md_ids

    errors = []
    if missing:
        errors.append(
            f"Clusters in document but MISSING from parsed output: {sorted(missing)}"
        )
    if extra:
        errors.append(
            f"Clusters in parsed output but NOT in document: {sorted(extra)}"
        )

    # 4. Every parsed cluster must have > 0 articles with a URL
    for c in parsed:
        urls = [a for a in c.get("articles", []) if a.get("url")]
        if not urls:
            errors.append(
                f"Cluster {c['cluster_id']} has 0 articles with URLs"
            )

    if errors:
        header = f"[{category}] CLUSTER FIDELITY CHECK FAILED"
        detail = "\n  • ".join(errors)
        msg = f"{header}:\n  • {detail}"
        print(f"\n❌ {msg}")
        raise RuntimeError(msg)

    # Passed — summary
    total_urls = sum(
        sum(1 for a in c.get("articles", []) if a.get("url"))
        for c in parsed
    )
    print(f"  ✓ Cluster fidelity validated: {len(md_ids)} doc clusters = {len(parsed_ids)} parsed clusters")
    print(f"  ✓ {total_urls} total article-cluster assignments")
    for c in parsed:
        url_count = sum(1 for a in c.get("articles", []) if a.get("url"))
        print(f"    {c['cluster_id']}: {url_count} URLs")


# ---------------------------------------------------------------------------
# Phase 5: Rapporteur CLI Threat Cluster Execution
# ---------------------------------------------------------------------------

def run_phase5(
    result: dict,
    category: str,
    output_dir: Path,
) -> list[dict]:
    _hr(f"Phase 5: Rapporteur Execution [{category}]")

    executor = RapporteurClusterExecution(output_dir, DEMO_SESSION_ID)
    path = executor.save_result(category, result)

    log.info("Saved to: %s", path)

    # Verify round-trip
    loaded = executor.load_results()
    log.info("Loaded back: %d records", len(loaded))

    for rec in loaded:
        cluster_count = len(rec.get("clusters", []))
        total_arts = sum(c.get("article_count", 0) for c in rec.get("clusters", []))
        log.info(
            "  record_id=%s, category=%s, %d clusters, %d articles",
            rec.get("record_id"),
            rec.get("category"),
            cluster_count,
            total_arts,
        )

    log.info("")
    log.info("--- URL Linkage (Phase 5) ---")
    all_persisted = set()
    for rec in loaded:
        for cluster in rec.get("clusters", []):
            for a in cluster.get("articles", []):
                if a.get("url"):
                    all_persisted.add(a["url"])
    log.info("  URLs persisted to JSONL: %d", len(all_persisted))
    log.info("Phase 5 PASSED — %d URLs persisted with data lineage",
             len(all_persisted))
    return loaded


# ---------------------------------------------------------------------------
# Phase 6: Kaliya Handoff
# ---------------------------------------------------------------------------


def _build_kaliya_supplementary(
    category: str,
    cluster_results: list[dict],
    grounding_events: list[dict] | None = None,
    eo_policy_summaries: list[dict[str, str]] | None = None,
) -> dict[str, str]:
    """Build per-cluster supplementary grounding packages for Kaliya.

    For each cluster, writes a markdown file combining:
      1. The cluster analysis text from Phase 4 (hypotheses, findings)
      2. Executive order policy summaries (from Federal Register)
      3. The relevant grounding events (government actions timeline)

    Files are written to the BBOA clustering cache alongside the Phase 4
    artifacts and the path is returned so it can be passed to Kaliya
    via ``supplementary_file`` in the Kafka message.

    Returns mapping of cluster_id → absolute path to the supplementary file.
    """
    import re as _re

    supp_dir = KALIYA_CACHE_DIR / f"kaliya_supplementary_{category}"
    supp_dir.mkdir(parents=True, exist_ok=True)

    # Collect grounding events into a renderable block
    grounding_block = ""
    if grounding_events:
        lines: list[str] = ["## Grounding Events — Verified Government Actions\n"]
        for g_idx, ev in enumerate(grounding_events, 1):
            lines.append(
                f"- **[G{g_idx}] EO {ev.get('eo_number', 'N/A')}: "
                f"{ev.get('eo_title', '')}**\n"
                f"  - Action: {ev.get('action_type', 'N/A')} — "
                f"{ev.get('action_description', 'N/A')}\n"
                f"  - Official: {ev.get('official', 'N/A')}\n"
                f"  - Date: {ev.get('date', 'N/A')}\n"
                f"  - URL: {ev.get('url', '')}\n"
            )
        grounding_block = "\n".join(lines)

    # Also try to load the latest Phase 4 markdown from BBOA cache
    # so we can extract per-cluster sections
    import glob
    phase4_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{category}_*.md")))
    phase4_text = ""
    if phase4_files:
        phase4_text = Path(phase4_files[-1]).read_text(encoding="utf-8")

    # Parse per-cluster sections from Phase 4 markdown
    cluster_sections: dict[str, str] = {}
    if phase4_text:
        cluster_pat = _re.compile(
            r"^##\s+Cluster\s+([A-Za-z0-9]+)\s+[—–-]\s+(.+)", _re.MULTILINE,
        )
        starts: list[tuple[int, str, str]] = []
        for m in cluster_pat.finditer(phase4_text):
            starts.append((m.start(), m.group(1), m.group(2).strip()))
        for idx, (start, cid, _title) in enumerate(starts):
            end = starts[idx + 1][0] if idx + 1 < len(starts) else len(phase4_text)
            # Trim at next top-level ## that isn't a cluster heading
            search_start = start + 1
            for m2 in _re.finditer(r"^## (?!Cluster\s)", phase4_text[search_start:end], _re.MULTILINE):
                end = search_start + m2.start()
                break
            cluster_sections[cid] = phase4_text[start:end].strip()

    # Extract hypothesis testing section if available
    hypothesis_section = ""
    hyp_match = _re.search(
        r"(^##.*[Tt]esting\s+.*[Hh]ypothes.*?\n.*?)(?=\n## |\Z)",
        phase4_text, _re.MULTILINE | _re.DOTALL,
    )
    if hyp_match:
        hypothesis_section = hyp_match.group(1).strip()

    supp_files: dict[str, str] = {}

    for result in cluster_results:
        for cluster in result.get("clusters", []):
            cid = cluster.get("cluster_id", "unknown")
            # Normalize cluster_id for section lookup: "cluster_A" → "A"
            cid_letter = cid.replace("cluster_", "") if cid.startswith("cluster_") else cid
            desc = cluster.get("cluster_description", "")
            cluster_text = cluster.get("cluster_text", "")

            parts: list[str] = [
                f"# Supplementary Grounding — Cluster {cid_letter}\n",
                f"**Category:** {category.replace('_', ' ').title()}\n",
                f"**Cluster:** {cid_letter} — {desc}\n\n",
                "---\n\n",
            ]

            # 1. Cluster analysis from Phase 4
            section = cluster_sections.get(cid_letter, "")
            if section:
                parts.append("## Cluster Analysis (from Phase 4 Threat Clustering)\n\n")
                parts.append(
                    "The following is the threat cluster analysis produced by the "
                    "Phase 4 model. Use these findings as priors — test, refine, "
                    "and challenge them with the deep multi-modal analysis of the "
                    "individual articles. Pay attention to the hypotheses, "
                    "alternative explanations, and convergence/divergence patterns.\n\n"
                )
                parts.append(section)
                parts.append("\n\n---\n\n")
            elif cluster_text:
                parts.append("## Cluster Summary\n\n")
                parts.append(cluster_text[:2000])
                parts.append("\n\n---\n\n")

            # 2. Hypothesis testing priors
            if hypothesis_section:
                parts.append("## Hypothesis Testing Priors\n\n")
                parts.append(
                    "The following hypotheses were tested in the clustering phase. "
                    "Evaluate whether your deep analysis supports, refutes, or "
                    "adds nuance to these findings.\n\n"
                )
                parts.append(hypothesis_section)
                parts.append("\n\n---\n\n")

            # 3. Executive Order policy context
            if eo_policy_summaries:
                parts.append("## Executive Orders — Policy Context\n\n")
                parts.append(
                    "The following executive orders are the authoritative legal and "
                    "policy framework relevant to this category. They represent the "
                    "government's stated justification and legal basis for the actions "
                    "under analysis. Evaluate whether media articles acknowledge, "
                    "omit, or misrepresent these policy positions.\n\n"
                )
                for eo_info in eo_policy_summaries:
                    parts.append(
                        f"### EO {eo_info['eo_number']}: {eo_info['title']}\n\n"
                        f"Source: {eo_info['url']}\n\n"
                        f"{eo_info['summary_text']}\n\n"
                        "---\n\n"
                    )

            # 4. Grounding events
            if grounding_block:
                parts.append(grounding_block)
                parts.append("\n\n---\n\n")

            # 5. Article metadata for this cluster
            articles = cluster.get("articles", [])
            if articles:
                parts.append("## Article Metadata for This Cluster\n\n")
                parts.append("| art_# | Author | Date | Publication | URL |\n")
                parts.append("|-------|--------|------|-------------|-----|\n")
                for i, a in enumerate(articles, 1):
                    author = (a.get("author") or "").replace("|", "/")
                    date = (a.get("date_publication") or "").replace("|", "/")
                    pub = (a.get("publisher") or "").replace("|", "/")
                    url = a.get("url", "")
                    parts.append(f"| art_{i} | {author} | {date} | {pub} | {url} |\n")
                parts.append("\n")

            # 6. Citation rules
            parts.append("## Citation Rules\n\n")
            parts.append(
                "IMPORTANT: Every document in the Grounding Data has a "
                "\"Cite as\" field immediately below its URL. You MUST use this "
                "exact citation label for ALL in-text references. "
                "The format is: (document_id, summary) — for example: "
                "(req_20260107_221154_000, summary).\n\n"
                "Rules:\n"
                "- Always copy the document_id EXACTLY as shown in the "
                "\"Cite as\" field — it will look like req_YYYYMMDD_HHMMSS_NNN.\n"
                "- NEVER use incremental numbers like [1], [2], [3] or "
                "(doc_1) or (art_1). These are meaningless to the reader.\n"
                "- NEVER use author names, publication names, or URLs as citations.\n"
                "- NEVER hyperlink the citation — render as plain text in parentheses.\n"
                "- When quoting or paraphrasing from a document, place the citation "
                "immediately after the quoted passage.\n"
                "- Multiple citations for the same claim: "
                "(req_20260303_090925_005, summary)(req_20260303_090925_006, summary).\n\n"
            )

            content = "".join(parts)
            fname = f"supp_cluster_{cid}.md"
            filepath = supp_dir / fname
            filepath.write_text(content, encoding="utf-8")
            supp_files[cid] = str(filepath)

    return supp_files


def run_phase6(
    cluster_results: list[dict],
    category: str,
    live: bool = False,
    grounding_events: list[dict] | None = None,
) -> list[dict]:
    _hr(f"Phase 6: Kaliya Handoff [{category}]")

    # Load EO policy summaries for this category's anchor EOs
    from src.components.analysis.media_linkage import CATEGORY_ANCHORS
    anchor_eos = CATEGORY_ANCHORS.get(category, [])
    eo_policy_summaries: list[dict[str, str]] = []
    if anchor_eos:
        eo_policy_summaries = _load_eo_policy_summaries(anchor_eos)
        if eo_policy_summaries:
            print(f"\n[EO Policy Context] {len(eo_policy_summaries)} executive orders loaded for '{category}':")
            for info in eo_policy_summaries:
                print(f"  → EO {info['eo_number']}: {info['title'][:60]}")

    # Build supplementary grounding packages for each cluster
    supp_files = _build_kaliya_supplementary(
        category, cluster_results,
        grounding_events=grounding_events,
        eo_policy_summaries=eo_policy_summaries,
    )
    if supp_files:
        print(f"\n[Kaliya Supplementary Packages]")
        for cid, path in sorted(supp_files.items()):
            size = Path(path).stat().st_size
            print(f"  → Cluster {cid}: {path} ({size:,} bytes)")

    handoff = KaliyaHandoff()
    payloads = handoff.build_all_handoffs(
        cluster_results, supplementary_files=supp_files,
    )

    log.info("")
    log.info("--- URL Linkage (Phase 6) ---")
    all_handoff_urls: set[str] = set()
    for p in payloads:
        log.info(
            "  Handoff: category=%s, cluster=%s, %d URLs → Kaliya (model=%s, images=%s, supp=%s)",
            p.get("category"),
            p.get("cluster_id"),
            p.get("url_count", 0),
            p.get("model"),
            p.get("retrieve_images"),
            bool(p.get("supplementary_file")),
        )
        for u in p.get("urls", [])[:3]:
            log.info("    url: %s", u[:80])
        if p.get("url_count", 0) > 3:
            log.info("    ... and %d more", p["url_count"] - 3)
        all_handoff_urls.update(p.get("urls", []))

    # Dispatch to Kafka or simulate
    kaliya_results = []
    if live:
        log.info("")
        log.info("  → LIVE MODE: dispatching %d payloads to Kafka 'kaliya-requests'...", len(payloads))
        sent = handoff.send_to_kafka(payloads)
        log.info("  → Sent %d/%d messages to Kafka", len(sent), len(payloads))

        for s in sent:
            kaliya_results.append({
                "category": s["category"],
                "cluster_id": s["cluster_id"],
                "request_id": s["request_id"],
                "urls_analysed": s["url_count"],
                "status": "dispatched",
                "analysis_text": (
                    f"[Kaliya request dispatched] request_id={s['request_id']}, "
                    f"{s['url_count']} URLs sent for agenda analysis."
                ),
            })

        # Poll for Kaliya completion and collect narratives
        log.info("")
        log.info("  → Polling for Kaliya completion...")
        completed = handoff.poll_results(sent, timeout=3600)
        if completed:
            log.info("  → %d/%d requests completed, collecting narratives...", len(completed), len(sent))
            written = handoff.collect_narratives(completed, WORKING_DIR)
            for w in written:
                log.info("  → Written: %s", w)

            # Post-process citations in collected narratives
            cid_articles: dict[str, list[dict]] = {}
            for result in cluster_results:
                for cluster in result.get("clusters", []):
                    cid_articles[cluster.get("cluster_id", "")] = cluster.get("articles", [])

            for w in written:
                if not str(w).endswith("_narrative.md"):
                    continue
                # Extract cluster_id from filename: kaliya-{cat}-{cluster_id}-{ts}_narrative.md
                fname = Path(w).stem  # e.g. kaliya-culture_and_media-cluster_A-20260329_071108_c067f1_narrative
                # Find the cluster_id by matching against known IDs
                matched_cid = ""
                for cid in cid_articles:
                    if f"-{cid}-" in fname:
                        matched_cid = cid
                        break
                if not matched_cid or not cid_articles.get(matched_cid):
                    continue

                narrative_text = Path(w).read_text(encoding="utf-8")
                resolved_text, count = _resolve_kaliya_citations(
                    narrative_text, cid_articles[matched_cid],
                )
                if count > 0:
                    Path(w).write_text(resolved_text, encoding="utf-8")
                    log.info("  → Citations resolved in %s: %d citations", Path(w).name, count)
                else:
                    log.info("  → No (art_N, kaliya) citations found in %s", Path(w).name)
        else:
            log.warning("  → No Kaliya results completed within timeout")
    else:
        log.info("")
        log.info("  → DRY RUN: simulating Kaliya responses (use --live to dispatch to Kafka)")
        for p in payloads:
            kaliya_results.append({
                "category": p["category"],
                "cluster_id": p["cluster_id"],
                "analysis_text": (
                    f"[Synthetic Kaliya output] Agenda analysis of "
                    f"{p['url_count']} URLs in {p['category']} / {p['cluster_id']}. "
                    f"No coordinated agenda detected beyond normal "
                    f"editorial convergence on newsworthy events."
                ),
                "urls_analysed": p["url_count"],
                "status": "simulated",
            })

    log.info("Phase 6 PASSED — %d URLs handed off to Kaliya across %d payloads",
             len(all_handoff_urls), len(payloads))
    return kaliya_results


# ---------------------------------------------------------------------------
# Phase 7: Conclusions
# ---------------------------------------------------------------------------

def run_phase7(
    cluster_result: dict,
    kaliya_result: dict | None,
    category: str,
    output_dir: Path,
) -> Path:
    _hr(f"Phase 7: Conclusions [{category}]")

    generator = ConclusionGenerator(output_dir)
    prompt = generator.build_prompt(category, cluster_result, kaliya_result)

    log.info(
        "Conclusion prompt: %d chars, includes Kaliya=%s",
        len(prompt), kaliya_result is not None,
    )

    # Count URLs referenced in the conclusion prompt
    prompt_url_count = sum(
        1 for a in cluster_result.get("articles", []) if a.get("url")
    )

    # Synthetic conclusion
    conclusion_parts = [
        f"# Conclusion: {category.replace('_', ' ').title()}\n\n",
        f"## Summary\n\n",
        f"Analysis of media coverage across {cluster_result.get('article_count', 0)} "
        f"articles in this category.\n\n",
        f"## Threat Assessment\n\n",
        f"[Synthetic] No coordinated threat pattern detected. Media coverage "
        f"shows normal editorial variance consistent with competitive news "
        f"markets rather than coordinated disinformation.\n\n",
        f"## Source URLs Analysed\n\n",
    ]
    for a in cluster_result.get("articles", []):
        conclusion_parts.append(
            f"- [{a.get('listed_publisher') or a.get('publisher', 'Unknown')}]"
            f"({a.get('url', '')}) — {a.get('author', 'Unknown')}, "
            f"{a.get('date_publication', 'undated')}\n"
        )
    conclusion_parts.append(
        f"\n## Recommendations\n\n"
        f"- Continue monitoring publication clusters for convergence shifts\n"
        f"- Cross-reference author networks with known influence operations\n"
    )
    conclusion = "".join(conclusion_parts)

    path = generator.save_conclusion(category, conclusion, DEMO_SESSION_ID)
    log.info("Saved conclusion: %s", path)

    # Verify the file was written
    content = path.read_text(encoding="utf-8")
    log.info("Conclusion file: %d chars, %d lines", len(content),
             content.count("\n"))

    log.info("")
    log.info("--- URL Linkage (Phase 7) ---")
    log.info("  URLs cited in conclusion prompt: %d", prompt_url_count)
    log.info("  URLs listed in conclusion output: %d",
             conclusion.count("](http"))

    log.info("Phase 7 PASSED — conclusion cites %d URLs", prompt_url_count)
    return path


# ---------------------------------------------------------------------------
# End-to-end orchestration
# ---------------------------------------------------------------------------

def run_all(
    max_phase: int = 7,
    category: str | None = None,
    min_confidence: float = 0.0,
    include_unlinked: bool = False,
    live: bool = False,
    output_dir: Path | None = None,
    start_phase: int = 1,
    measure_only: bool = False,
) -> None:
    """Run phases start_phase through max_phase, passing outputs phase-to-phase."""
    if output_dir is None:
        output_dir = Path(tempfile.mkdtemp(prefix="demo_media_linkage_"))
    print(f"\n{'=' * 70}")
    print(f"BBOA Media Linkage Demo")
    print(f"Output directory: {output_dir}")
    print(f"={'=' * 70}")

    target_categories = (
        [category] if category else ["culture_and_media"]
    )

    # URL tracking across all phases
    url_tracking: dict[str, dict[str, set[str]]] = {}

    # --- Fast-forward: start at phase 5+ by loading saved markdown ---
    # Fast-forward: load saved Phase 4 markdown + its companion bundle.
    # The bundle JSONL guarantees 100% positional match (same article
    # ordering that the LLM saw).  If the bundle file is missing, fall
    # back to a deterministic rebuild via phases 1-3 + BM25.
    if start_phase >= 5:
        import glob
        import json as _json

        # Lazy: only rebuild grounding package if a bundle file is missing
        _rebuilt_grounding = None  # (retriever, expanded) — built at most once

        def _rebuild_grounding_once():
            nonlocal _rebuilt_grounding
            if _rebuilt_grounding is not None:
                return _rebuilt_grounding
            _hr("Rebuilding Grounding Package (fallback — no saved bundle)")
            _ret, _eo, _mc = run_phase1(min_confidence, include_unlinked)
            _grp, _exp = run_phase2(_ret, _eo, _mc, min_confidence)
            _rebuilt_grounding = (_ret, _eo, _mc, _exp)
            return _rebuilt_grounding

        for cat in target_categories:
            url_tracking[cat] = {}
            _eo_grounding_ff: list[dict] | None = None

            # Locate saved Phase 4 markdown
            real_md_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{cat}_*.md")))
            if not real_md_files:
                print(f"ERROR: No phase 4 markdown found for '{cat}' in {WORKING_DIR}")
                continue
            md_path = real_md_files[-1]
            md_text = Path(md_path).read_text()
            print(f"\n  Loaded phase 4 markdown: {md_path} ({len(md_text):,} chars)")

            # Try loading the companion bundle (exact article ordering)
            bundle_path = Path(md_path).with_suffix(".bundle.jsonl")
            if bundle_path.exists():
                articles = []
                with open(bundle_path, encoding="utf-8") as bf:
                    for line in bf:
                        if line.strip():
                            articles.append(_json.loads(line))
                bundle = {"articles": articles, "article_count": len(articles)}
                print(f"  Loaded companion bundle: {bundle_path.name} ({len(articles)} articles)")
            else:
                # Fallback: deterministic rebuild
                print(f"  No companion bundle found — rebuilding from phases 1-3 + BM25")
                ret, eo, mc, exp = _rebuild_grounding_once()
                if cat not in exp:
                    print(f"  WARN: Category '{cat}' not in expanded graph — parsing without bundle")
                    bundle = {"articles": []}
                else:
                    phase3_out = run_phase3(ret, eo, mc, exp, cat,
                                            min_confidence, include_unlinked, live=False)
                    bundle = phase3_out["bundle"]
                    _eo_grounding_ff = phase3_out.get("eo_grounding")
                    _inject_bm25_grounding(bundle, cat)

            # Parse with the bundle so article numbers resolve to URLs
            analyser = ThreatClusterAnalyser()
            cluster_result = analyser.parse_result(cat, md_text, bundle)
            total_arts = sum(c['article_count'] for c in cluster_result.get("clusters", []))
            for c in cluster_result.get("clusters", []):
                print(f"    {c['cluster_id']}: {c['article_count']} articles — {c['cluster_description'][:60]}")
            print(f"  → {cluster_result['cluster_count']} clusters, {total_arts} total articles")

            _validate_cluster_fidelity(md_text, cluster_result, cat)

            cluster_results = run_phase5(cluster_result, cat, output_dir)
            if max_phase >= 6:
                kaliya_results = run_phase6(
                    cluster_results, cat, live=live,
                    grounding_events=_eo_grounding_ff,
                )

        _hr("ALL PHASES COMPLETE")
        print(f"\nDemo finished. Outputs in: {output_dir}\n")
        return

    # --- Phase 1 ---
    retriever, eo_cache, media_cache = run_phase1(min_confidence, include_unlinked)
    if max_phase < 2:
        return

    # --- Phase 2 ---
    grouper, expanded = run_phase2(retriever, eo_cache, media_cache, min_confidence)
    if max_phase < 3:
        return

    for cat in target_categories:
        if cat not in expanded:
            print(f"ERROR: Category '{cat}' not found. Available: {list(expanded.keys())}")
            continue

        url_tracking[cat] = {}

        # --- Phase 3 ---
        phase3_out = run_phase3(
            retriever, eo_cache, media_cache, expanded, cat,
            min_confidence, include_unlinked, live
        )
        bundle = phase3_out["bundle"]
        url_tracking[cat]["primary_urls"] = phase3_out["primary_urls"]
        url_tracking[cat]["media_urls"] = phase3_out["media_urls"]
        url_tracking[cat]["bundle_urls"] = {
            a["url"] for a in bundle["articles"] if a.get("url")
        }
        if max_phase < 4:
            continue

        # --- BM25 Grounding Injection (pre-Phase 4) ---
        bm25_grounding, bm25_retriever = _inject_bm25_grounding(bundle, cat)

        # --- Reconstitute BM25 articles ---
        bm25_urls = [a["url"] for a in bm25_grounding if a.get("url") and not a.get("reconstituted")]
        if bm25_urls:
            if live:
                print(f"\n[BM25 Reconstitution - LIVE]")
                recon = FullDocReconstituter(RAG_URL)
                recon_count = 0
                for idx_folder in INDEX_FOLDERS:
                    remaining = [u for u in bm25_urls if not any(
                        a["url"] == u and a.get("reconstituted") for a in bundle["articles"]
                    )]
                    if not remaining:
                        break
                    if not recon.ensure_index(idx_folder):
                        continue
                    docs = recon.reconstitute_batch(remaining)
                    doc_map = {d["url"]: d for d in docs}
                    for art in bundle["articles"]:
                        if art["url"] in doc_map:
                            d = doc_map[art["url"]]
                            art["full_text"] = d.get("full_text", "")
                            art["summary_text"] = d.get("summary_text", art.get("summary_text", ""))
                            art["reconstituted"] = True
                            art["qa_count"] = d.get("qa_count", 0)
                            recon_count += 1
                print(f"  → {recon_count}/{len(bm25_urls)} BM25 articles reconstituted via RAG")
            else:
                print(f"\n[BM25 Reconstitution - DRY RUN]")
                for art in bundle["articles"]:
                    if art["url"] in bm25_urls and not art.get("reconstituted"):
                        art["full_text"] = "Synthetic full-doc text from QA context reconstitution."
                        art["reconstituted"] = True
                        art["qa_count"] = 5
                print(f"  → {len(bm25_urls)} BM25 articles given synthetic reconstitution")

        # --- Phase 4 ---
        prior = _load_prior_analysis(cat)
        if prior:
            print(f"\n  → Loaded n-1 prior analysis: {len(prior):,} chars")
        cluster_result = run_phase4(
            bundle, cat, live=live,
            grounding_events=phase3_out.get("eo_grounding"),
            eo_policy_texts=phase3_out.get("eo_policy_texts"),
            prior_analysis=prior,
            measure_only=measure_only,
        )

        # --- BM25 Cluster Supplementation (post-Phase 4) ---
        cluster_result = _supplement_clusters_bm25(
            cluster_result, bm25_retriever, cat, per_cluster_k=20,
        )

        url_tracking[cat]["cluster_urls"] = set()
        for cluster in cluster_result.get("clusters", []):
            for a in cluster.get("articles", []):
                if a.get("url"):
                    url_tracking[cat]["cluster_urls"].add(a["url"])
        if max_phase < 5:
            continue

        # --- Phase 5 ---
        cluster_results = run_phase5(cluster_result, cat, output_dir)
        url_tracking[cat]["persisted_urls"] = set()
        for rec in cluster_results:
            for cluster in rec.get("clusters", []):
                for a in cluster.get("articles", []):
                    if a.get("url"):
                        url_tracking[cat]["persisted_urls"].add(a["url"])
        if max_phase < 6:
            continue

        # --- Phase 6 ---
        kaliya_results = run_phase6(
            cluster_results, cat, live=live,
            grounding_events=phase3_out.get("eo_grounding"),
        )
        url_tracking[cat]["handoff_urls"] = set()
        for rec in cluster_results:
            for cluster in rec.get("clusters", []):
                for a in cluster.get("articles", []):
                    if a.get("url"):
                        url_tracking[cat]["handoff_urls"].add(a["url"])
        if max_phase < 7:
            continue

        # --- Phase 7 ---
        kaliya_for_cat = next(
            (k for k in kaliya_results if k.get("category") == cat), None
        )
        run_phase7(cluster_result, kaliya_for_cat, cat, output_dir)
        url_tracking[cat]["conclusion_urls"] = set()
        for cluster in cluster_result.get("clusters", []):
            for a in cluster.get("articles", []):
                if a.get("url"):
                    url_tracking[cat]["conclusion_urls"].add(a["url"])

    # --- Final URL Linkage Report ---
    _hr("URL LINKAGE REPORT")
    for cat, tracking in url_tracking.items():
        print(f"\nCategory: {cat}")
        print(f"  Phase 1+2 → primary URLs (workflow):   {len(tracking.get('primary_urls', set())):4}")
        print(f"  Phase 1+2 → media URLs (BBAO):         {len(tracking.get('media_urls', set())):4}")
        print(f"  Phase 3   → evidence bundle URLs:      {len(tracking.get('bundle_urls', set())):4}")
        print(f"  Phase 4   → cluster analysis URLs:     {len(tracking.get('cluster_urls', set())):4}")
        print(f"  Phase 5   → persisted URLs (JSONL):    {len(tracking.get('persisted_urls', set())):4}")
        print(f"  Phase 6   → Kaliya handoff URLs:       {len(tracking.get('handoff_urls', set())):4}")
        print(f"  Phase 7   → conclusion cited URLs:     {len(tracking.get('conclusion_urls', set())):4}")

        # Verify URL continuity: bundle → cluster → persisted → handoff → conclusion
        bundle_u = tracking.get("bundle_urls", set())
        cluster_u = tracking.get("cluster_urls", set())
        if bundle_u and cluster_u:
            if cluster_u == bundle_u:
                print(f"  \u2713 URL CONTINUITY: COMPLETE — all bundle URLs preserved through phases 4-7")
            else:
                lost = bundle_u - cluster_u
                print(f"  \u26a0 URL CONTINUITY: PARTIAL — {len(lost)} URLs lost after Phase 3")

    _hr("ALL PHASES COMPLETE")
    print(f"\nDemo finished. Outputs in: {output_dir}\n")


# ---------------------------------------------------------------------------
# Collect already-dispatched Kaliya results
# ---------------------------------------------------------------------------

def _collect_kaliya_results(category: str) -> None:
    """Scan Kaliya cache for completed results matching category, collect to working_dir."""
    from pathlib import Path as _Path

    cache_dir = KALIYA_CACHE_DIR
    _hr(f"Collecting Kaliya results for '{category}'")

    # Find all request dirs matching this category, keep only latest per cluster
    prefix = f"kaliya-{category}-"
    request_dirs = sorted(
        d for d in cache_dir.iterdir()
        if d.is_dir() and d.name.startswith(prefix)
    )
    if not request_dirs:
        print(f"  No Kaliya result dirs found matching '{prefix}*' in {cache_dir}")
        return

    # Deduplicate: group by cluster, keep the latest (sorted last) per cluster
    import re as _re
    cluster_latest: dict[str, str] = {}
    for d in request_dirs:
        # e.g. kaliya-culture_and_media-cluster_1-20260324_121309_7698e8
        #      kaliya-culture_and_media-cluster_A-20260330_170920_0cc617
        m = _re.match(rf"kaliya-{_re.escape(category)}-(cluster_[A-Za-z0-9]+)-(.+)", d.name)
        if m:
            cluster_key = m.group(1)
            # Sorted order means later entries overwrite earlier ones
            cluster_latest[cluster_key] = d.name
        else:
            cluster_latest[d.name] = d.name

    latest_dirs = sorted(cluster_latest.values())
    print(f"  Found {len(request_dirs)} total result dirs, keeping {len(latest_dirs)} latest:")
    for name in latest_dirs:
        print(f"    {name}")

    handoff = KaliyaHandoff()

    # Build sent-like dicts for poll_results
    sent = [{"request_id": name} for name in latest_dirs]

    completed = handoff.poll_results(sent, str(cache_dir), poll_interval=2, timeout=30)
    if completed:
        print(f"\n  {len(completed)}/{len(sent)} completed, collecting narratives...")
        written = handoff.collect_narratives(completed, WORKING_DIR)
        for w in written:
            print(f"  → {w}")
            if str(w).endswith(".md"):
                content = _Path(w).read_text(encoding="utf-8")
                print(f"    ({len(content):,} chars, {content.count(chr(10))} lines)")
    else:
        print("  No completed results found.")

    _hr("COLLECTION COMPLETE")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demo for BBOA Media Linkage pipeline (Phases 1-7).",
    )
    parser.add_argument(
        "--phase", type=int, default=7, choices=range(1, 8),
        help="Run up to and including this phase (default: 7).",
    )
    parser.add_argument(
        "--category", type=str, default=None,
        choices=list(CATEGORY_ANCHORS.keys()),
        help="Run for a single category (default: culture_and_media).",
    )
    parser.add_argument(
        "--min-confidence", type=float, default=0.0,
        help="Minimum confidence score for workflow cache records (default: 0.0).",
    )
    parser.add_argument(
        "--include-unlinked", action="store_true",
        help="Include unlinked media from BBAO time windows (default: False).",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Enable live RAG service calls for Phase 3 reconstitution.",
    )
    parser.add_argument(
        "--start-phase", type=int, default=1, choices=range(1, 8),
        help="Start from this phase (default: 1). Use 5 to skip phases 1-4.",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for Phase 5-7 artifacts.",
    )
    parser.add_argument(
        "--collect", action="store_true",
        help="Poll for already-dispatched Kaliya results and collect narratives to working_dir.",
    )
    parser.add_argument(
        "--measure", action="store_true",
        help="Reconstitute articles and measure prompt tokens, but skip LLM call.",
    )
    args = parser.parse_args()

    if args.collect:
        _collect_kaliya_results(args.category or "culture_and_media")
        return

    out = Path(args.output_dir) if args.output_dir else None
    run_all(
        max_phase=args.phase,
        category=args.category,
        min_confidence=args.min_confidence,
        include_unlinked=args.include_unlinked,
        live=args.live or args.measure,
        output_dir=out,
        start_phase=args.start_phase,
        measure_only=args.measure,
    )


if __name__ == "__main__":
    main()
