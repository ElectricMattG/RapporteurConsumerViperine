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

from src.components.analysis.media_linkage import (  # noqa: E402
    CATEGORY_ANCHORS, BBAORetriever, CategoryGrouper, ConclusionGenerator,
    FullDocReconstituter, KaliyaHandoff, RapporteurClusterExecution,
    ThreatClusterAnalyser)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BBAO_CACHE_DIR = (
    "/mnt/workspace_2/bboa/cache/bboa_trump_20260311_220355"
)
EO_CACHE_PATH = (
    "/mnt/workspace_2/bboa/workflows/eo_linking/eo_implementation_cache.jsonl"
)
RAG_URL = "http://localhost:8000"
INDEX_FOLDERS = ["x_indices_hegseth", "x_indices_trump"]
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
WORKING_DIR = PROJECT_ROOT / "working_dir"
CACHE_DIR = PROJECT_ROOT / "cache"

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


def _extract_urls_for_eo(
    eo_cache: list[dict],
    eo_number: str,
    min_confidence: float = 0.5,
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


def _extract_eo_grounding(
    eo_cache: list[dict],
    eo_numbers: list[str],
    min_confidence: float = 0.5,
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
    min_confidence: float = 0.5,
    include_unlinked: bool = False,
) -> tuple[BBAORetriever, list[dict]]:
    _hr("Phase 1: URL-Based Retrieval (Workflow → BBAO)")

    # Step 1: Load workflow EO cache
    eo_cache = _load_eo_cache()

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

    # Step 3: Load BBAO cache
    retriever = BBAORetriever(BBAO_CACHE_DIR)
    retriever.load()

    print(f"\n[BBAO Cache] Loaded:")
    summary = retriever.summary()
    print(f"  → {summary['eo_events']} events, {summary['linked_media']} linked media, "
          f"{summary['unlinked']} unlinked articles")

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

    return retriever, eo_cache


# ---------------------------------------------------------------------------
# Phase 2: Category Grouping and Graph Expansion
# ---------------------------------------------------------------------------

def run_phase2(
    retriever: BBAORetriever,
    eo_cache: list[dict],
    min_confidence: float = 0.5,
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
    expanded: dict[str, dict[str, str]],
    category: str,
    min_confidence: float = 0.5,
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

    # Step 2: Match URLs in BBAO to get events
    print(f"\n[Step 2: Match URLs in BBAO Cache]")
    matched_events: list[dict] = []
    matched_event_keys: set[str] = set()

    for url in primary_urls:
        for ev in retriever.eo_events:
            if ev.get("source_url") == url:
                matched_events.append(ev)
                if ev.get("event_key"):
                    matched_event_keys.add(ev["event_key"])

    print(f"  → {len(primary_urls)} unique URLs → matched {len(matched_events)} BBAO event entries")
    print(f"  → {len(matched_event_keys)} unique event_keys (some URLs have multiple events)")

    # Step 3: Get linked media for matched events
    print(f"\n[Step 3: Get Linked Media Coverage]")
    all_linked: list[dict] = []
    linked_urls: set[str] = set()

    for m in retriever.linked_media:
        if m.get("event_key") in matched_event_keys:
            all_linked.append(m)
            if m.get("link"):
                linked_urls.add(m["link"])

    print(f"  → {len(all_linked)} linked_media entries found")
    print(f"  → {len(linked_urls)} UNIQUE media URLs (multiple entries can share same URL)")

    # Step 4: Get unlinked media in time windows (if enabled)
    print(f"\n[Step 4: Get Unlinked Media (Optional)]")
    all_unlinked: list[dict] = []
    unlinked_urls: set[str] = set()

    if include_unlinked:
        windows: set[tuple[str, str]] = set()
        for ev in matched_events:
            if ev.get("window_start") and ev.get("window_end"):
                windows.add((ev["window_start"], ev["window_end"]))

        print(f"  → {len(windows)} time windows from matched events")
        for w_start, w_end in windows:
            unlinked = retriever.unlinked_for_window(w_start, w_end)
            for u in unlinked:
                if u.get("link") and u["link"] not in linked_urls:
                    unlinked_urls.add(u["link"])
                    all_unlinked.append(u)
        print(f"  → {len(all_unlinked)} unlinked_media entries found")
        print(f"  → {len(unlinked_urls)} UNIQUE unlinked URLs (excluding already-linked URLs)")
    else:
        print(f"  → Unlinked media: DISABLED (use --include-unlinked to enable)")

    media_urls = linked_urls | unlinked_urls
    all_urls = primary_urls | media_urls

    print(f"\n[URL Summary]")
    print(f"  → {len(primary_urls)} primary URLs (workflow cache)")
    print(f"  → {len(linked_urls)} linked media URLs (BBAO)")
    if include_unlinked:
        print(f"  → {len(unlinked_urls)} unlinked media URLs (BBAO)")
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
        print(f"  → Building synthetic docs for {len(all_urls)} URLs (no actual RAG calls)")
        for url in sorted(all_urls):
            reconstituted[url] = {
                "url": url,
                "title": f"[Synthetic] Article from {url[:50]}",
                "summary_text": "Synthetic summary for demo.",
                "full_text": "Synthetic full-doc text from QA context reconstitution.",
                "doc_metadata": {},
                "qa_count": 5,
            }
        print(f"  ✓ {len(reconstituted)} synthetic documents created")

    # Build evidence bundle - this combines all media articles with reconstituted full-text
    print(f"\n[Step 6: Build Analysis Package]")
    print(f"  → Combining {len(all_linked)} linked media + {len(all_unlinked)} unlinked media...")
    recon_obj = FullDocReconstituter(RAG_URL)
    bundle = recon_obj.build_evidence_bundle(
        category=category,
        linked_media=all_linked,  # ALL linked media, no cap
        unlinked_media=all_unlinked,  # ALL unlinked media, no cap
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

        # --- BBAO Media Framing ---
        if 'media_framing' in sample and sample['media_framing']:
            mf = sample['media_framing']
            print(f"\n--- BBAO MEDIA FRAMING ---")
            for key, value in mf.items():
                if value:
                    print(f"  • {key}: {value}")

        if 'eo_tags' in sample:
            print(f"\nEO Tags: {sample['eo_tags']}")

        print("=" * 80)

    print(f"\n[Phase 3 Complete]")
    print(f"  ✓ {len(all_urls)} URLs traced: workflow cache → BBAO → reconstitution")
    print(f"  ✓ Analysis package ready with {len(bundle['articles'])} articles for Phase 4")

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

def _write_phase4_markdown(
    category: str,
    model_name: str,
    llm_output: str,
    usage_stats: dict,
    bundle: dict,
    dest_dir: Path,
    grounding_events: list[dict] | None = None,
    eo_policy_texts: dict[str, dict] | None = None,
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

    content = "".join(parts)
    filepath.write_text(content, encoding="utf-8")
    return filepath


def _get_llm_rates(model_name: str, input_tokens: int) -> tuple[float, float]:
    """Get per-1M-token rates for a model, accounting for tiered pricing."""
    rates = LLM_PRICING.get(model_name, {})
    # Gemini 3.1 Pro / 2.5 Pro have tiered pricing at 200K boundary
    if input_tokens <= 200_000 and "input_short" in rates:
        return rates["input_short"], rates["output_short"]
    return rates.get("input", 0), rates.get("output", 0)


def _is_gov_mil(url: str) -> bool:
    """Return True if the URL belongs to a .gov, .mil, or dvidshub.net domain."""
    from urllib.parse import urlparse
    try:
        host = urlparse(url).hostname or ""
    except Exception:
        return False
    return (
        host.endswith(".gov")
        or host.endswith(".mil")
        or host.endswith("dvidshub.net")
    )


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


def run_phase4(
    bundle: dict,
    category: str,
    live: bool = False,
    model_name: str = "gpt-5.4",
    grounding_events: list[dict] | None = None,
    eo_policy_texts: dict[str, dict] | None = None,
    prior_analysis: str | None = None,
) -> dict:
    _hr(f"Phase 4: Threat Cluster Analysis [{category}]")

    # Filter out .gov/.mil primary sources — keep only media articles
    all_articles = bundle.get("articles", [])
    gov_mil = [a for a in all_articles if _is_gov_mil(a.get("url", ""))]
    media_articles = [a for a in all_articles if not _is_gov_mil(a.get("url", ""))]

    # Split into linked vs unlinked, then sample 10% of unlinked
    linked = [a for a in media_articles if a.get("source_type") == "linked"]
    unlinked = [a for a in media_articles if a.get("source_type") != "linked"]

    import random
    sample_size = max(1, len(unlinked) // 10) if unlinked else 0
    unlinked_sample = random.sample(unlinked, sample_size) if sample_size else []

    prompt_articles = linked + unlinked_sample

    media_bundle = dict(bundle)
    media_bundle["articles"] = prompt_articles
    media_bundle["article_count"] = len(prompt_articles)

    print(f"\n[Source Filtering]")
    print(f"  → {len(all_articles)} total articles in evidence bundle")
    print(f"  → {len(gov_mil)} .gov/.mil/dvidshub.net primary sources EXCLUDED")
    print(f"  → {len(media_articles)} media articles after gov/mil filter")
    print(f"    • {len(linked)} linked articles (all included)")
    print(f"    • {len(unlinked)} unlinked articles → {len(unlinked_sample)} sampled (10%)")
    print(f"  → {len(prompt_articles)} total articles sent to LLM")
    if grounding_events:
        print(f"  → {len(grounding_events)} EO grounding events injected into prompt")
    if eo_policy_texts:
        print(f"  → {len(eo_policy_texts)} EO policy documents injected into prompt")
    if prior_analysis:
        print(f"  → Prior analysis (n-1) injected: {len(prior_analysis):,} chars")

    # Ranked article char counts
    print(f"\n[Article Char Counts — Ranked by full_text length]")
    article_sizes = []
    for a in prompt_articles:
        ft_len = len(a.get("full_text", ""))
        article_sizes.append((ft_len, a.get("source_type", ""), a.get("title", "Untitled")[:80], a.get("url", "")))
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
    usage_stats = {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}

    if live:
        # Skip LLM call if real phase 4 markdown already exists
        import glob
        real_md_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{category}_*.md")))
        if real_md_files:
            real_md_path = real_md_files[-1]
            llm_output = Path(real_md_path).read_text()
            print(f"  → Reusing existing phase 4 markdown: {real_md_path} ({len(llm_output):,} chars)")
            live = False  # skip LLM, fall through to parse
        else:
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

            # Calculate cost from actual usage (tiered)
            in_rate, out_rate = _get_llm_rates(model_name, usage_stats["input_tokens"])
            usage_stats["cost_usd"] = (
                usage_stats["input_tokens"] / 1_000_000 * in_rate
                + usage_stats["output_tokens"] / 1_000_000 * out_rate
            )

            print(f"  ✓ Response received")
            print(f"    • Input tokens:  {usage_stats['input_tokens']:,}")
            print(f"    • Output tokens: {usage_stats['output_tokens']:,}")
            print(f"    • Cost:          ${usage_stats['cost_usd']:.6f} (in=${in_rate}/1M, out=${out_rate}/1M)")

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

    # Skip writing if LLM returned zero tokens (failed / empty response)
    if usage_stats.get("input_tokens", 0) == 0 and usage_stats.get("output_tokens", 0) == 0:
        print("  ⚠ Skipping file write: input and output tokens are both 0")
    else:
        # Write output to markdown in working_dir (and cache)
        for dest in (WORKING_DIR, CACHE_DIR):
            md_path = _write_phase4_markdown(
                category, model_name, llm_output, usage_stats, media_bundle, dest,
                grounding_events=grounding_events,
                eo_policy_texts=eo_policy_texts,
            )
            print(f"  ✓ Saved: {md_path}")

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

    return result


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

def run_phase6(
    cluster_results: list[dict],
    category: str,
    live: bool = False,
) -> list[dict]:
    _hr(f"Phase 6: Kaliya Handoff [{category}]")

    handoff = KaliyaHandoff()
    payloads = handoff.build_all_handoffs(cluster_results)

    log.info("")
    log.info("--- URL Linkage (Phase 6) ---")
    all_handoff_urls: set[str] = set()
    for p in payloads:
        log.info(
            "  Handoff: category=%s, cluster=%s, %d URLs → Kaliya (model=%s, images=%s)",
            p.get("category"),
            p.get("cluster_id"),
            p.get("url_count", 0),
            p.get("model"),
            p.get("retrieve_images"),
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
        completed = handoff.poll_results(sent)
        if completed:
            log.info("  → %d/%d requests completed, collecting narratives...", len(completed), len(sent))
            written = handoff.collect_narratives(completed, WORKING_DIR)
            for w in written:
                log.info("  → Written: %s", w)
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
    min_confidence: float = 0.5,
    include_unlinked: bool = False,
    live: bool = False,
    output_dir: Path | None = None,
    start_phase: int = 1,
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
    if start_phase >= 5:
        import glob
        for cat in target_categories:
            url_tracking[cat] = {}
            real_md_files = sorted(glob.glob(str(WORKING_DIR / f"phase4_{cat}_*.md")))
            if not real_md_files:
                print(f"ERROR: No phase 4 markdown found for '{cat}' in {WORKING_DIR}")
                continue
            md_path = real_md_files[-1]
            md_text = Path(md_path).read_text()
            print(f"\n  Loaded phase 4 markdown: {md_path} ({len(md_text):,} chars)")

            analyser = ThreatClusterAnalyser()
            cluster_result = analyser.parse_result(cat, md_text, {"articles": []})
            for c in cluster_result.get("clusters", []):
                print(f"    {c['cluster_id']}: {c['article_count']} articles")

            cluster_results = run_phase5(cluster_result, cat, output_dir)
            if max_phase >= 6:
                kaliya_results = run_phase6(cluster_results, cat, live=live)

        _hr("ALL PHASES COMPLETE")
        print(f"\nDemo finished. Outputs in: {output_dir}\n")
        return

    # --- Phase 1 ---
    retriever, eo_cache = run_phase1(min_confidence, include_unlinked)
    if max_phase < 2:
        return

    # --- Phase 2 ---
    grouper, expanded = run_phase2(retriever, eo_cache, min_confidence)
    if max_phase < 3:
        return

    for cat in target_categories:
        if cat not in expanded:
            print(f"ERROR: Category '{cat}' not found. Available: {list(expanded.keys())}")
            continue

        url_tracking[cat] = {}

        # --- Phase 3 ---
        phase3_out = run_phase3(
            retriever, eo_cache, expanded, cat,
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

        # --- Phase 4 ---
        prior = _load_prior_analysis(cat)
        if prior:
            print(f"\n  → Loaded n-1 prior analysis: {len(prior):,} chars")
        cluster_result = run_phase4(
            bundle, cat, live=live,
            grounding_events=phase3_out.get("eo_grounding"),
            eo_policy_texts=phase3_out.get("eo_policy_texts"),
            prior_analysis=prior,
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
        kaliya_results = run_phase6(cluster_results, cat, live=live)
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

    cache_dir = _Path("/mnt/workspace_2/bboa/cache/kaliya_agenda_cache")
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
        m = _re.match(rf"kaliya-{_re.escape(category)}-(cluster_\d+)-(.+)", d.name)
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
        "--min-confidence", type=float, default=0.5,
        help="Minimum confidence score for workflow cache records (default: 0.5).",
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
        live=args.live,
        output_dir=out,
        start_phase=args.start_phase,
    )


if __name__ == "__main__":
    main()
