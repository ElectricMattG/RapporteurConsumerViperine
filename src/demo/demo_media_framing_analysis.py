#!/usr/bin/env python3
"""
demo_media_framing_analysis.py — Media-only threat analysis demo (no EO linkage).

This demo isolates media risk analysis from EO linkages. All articles are
treated as "unlinked" and analyzed purely for media framing patterns,
coordination signals, and threat clusters.

Phases:
  1. Load BBOA cache (linked + unlinked) with media_framing
  2. Reconstitute full-text from RAG indices
  3. Threat cluster analysis (media framing only)
  4. Persist results and generate conclusions

Run from the project root:
    python -m src.demo.demo_media_framing_analysis
    python -m src.demo.demo_media_framing_analysis --live
    python -m src.demo.demo_media_framing_analysis --sample 500
"""
from __future__ import annotations

import argparse
import asyncio
import glob
import json
import logging
import random
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.components.analysis.media_linkage import BBAORetriever  # noqa: E402
from src.components.analysis.media_linkage import (FullDocReconstituter,
                                                   ThreatClusterAnalyser)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BBAO_CACHE_SOURCES = [
    "/mnt/workspace_2/bboa/cache/bboa_full_20260310_194954",
    "/mnt/workspace_2/bboa/cache/bboa_trump_20260311_220355",
    "/mnt/workspace_2/bboa/cache/bboa_combined_20260326_153800",
]
RAG_URL = "http://localhost:8000"
INDEX_FOLDERS = ["x_indices_merged"]
WORKING_DIR = PROJECT_ROOT / "working_dir"
CACHE_DIR = PROJECT_ROOT / "cache"

LLM_PRICING = {
    "gpt-5.4": {"input": 2.50, "output": 10.00},
    "gpt-5": {"input": 2.00, "output": 8.00},
    "gemini-2.5-pro": {"input": 2.50, "output": 15.00, "input_short": 1.25, "output_short": 10.00},
}

logging.basicConfig(level=logging.WARNING, format="%(message)s")
log = logging.getLogger("demo_media_framing")
log.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hr(title: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"{title}")
    print(f"{'=' * 70}")


def _get_llm_rates(model_name: str, input_tokens: int) -> tuple[float, float]:
    """Get per-1M-token rates for a model, accounting for tiered pricing."""
    rates = LLM_PRICING.get(model_name, {})
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


# ---------------------------------------------------------------------------
# Phase 1: Load BBOA Media Articles
# ---------------------------------------------------------------------------

def run_phase1(sample_size: int | None = None) -> tuple[BBAORetriever, list[dict]]:
    """Load all BBOA articles (linked + unlinked) with media_framing."""
    _hr("Phase 1: Load BBOA Media Corpus")

    retriever = BBAORetriever(BBAO_CACHE_SOURCES)
    retriever.load()

    summary = retriever.summary()
    print(f"\n[BBOA Cache] Loaded from {len(BBAO_CACHE_SOURCES)} sources:")
    print(f"  → Sessions: {', '.join(summary['session_ids'])}")
    print(f"  → {summary['linked_media']} linked_media (with EO events)")
    print(f"  → {summary['unlinked']} unlinked articles (time-window matched)")
    print(f"  → {summary['eo_events']} eo_events (for reference)")

    # Combine linked + unlinked, treating ALL as unlinked for analysis
    all_articles = []
    for m in retriever.linked_media + retriever.unlinked:
        if not m.get("link"):
            continue
        if not m.get("media_framing"):
            continue

        # Normalize to analysis schema (remove EO linkage)
        article = {
            "url": m.get("link"),
            "title": m.get("title", ""),
            "author": m.get("author", ""),
            "publisher": m.get("publisher", ""),
            "listed_publisher": m.get("listed_publisher") or m.get("publisher", ""),
            "date_publication": m.get("date_publication", ""),
            "source_type": "unlinked",  # Treat all as unlinked
            "media_framing": m.get("media_framing", {}),
            "consumer_salience": m.get("consumer_salience"),
            "mean_search_engine_rank": m.get("mean_search_engine_rank"),
        }
        all_articles.append(article)

    print(f"\n[Media Corpus]")
    print(f"  → {len(all_articles)} total articles with media_framing")

    # Sample if requested
    if sample_size and sample_size < len(all_articles):
        sampled = random.sample(all_articles, sample_size)
        print(f"  → Sampling {sample_size} articles for analysis")
        all_articles = sampled

    # Filter out .gov/.mil
    media_only = [a for a in all_articles if not _is_gov_mil(a["url"])]
    gov_mil_count = len(all_articles) - len(media_only)

    print(f"  → {gov_mil_count} .gov/.mil sources excluded")
    print(f"  → {len(media_only)} media articles for analysis")

    # Show media_framing breakdown
    with_factualness = sum(1 for a in media_only if a.get("media_framing", {}).get("factualness"))
    with_spin = sum(1 for a in media_only if a.get("media_framing", {}).get("spin_rating"))
    with_risk = sum(1 for a in media_only if a.get("media_framing", {}).get("risk_rating"))

    print(f"\n[Media Framing Coverage]")
    print(f"  → {with_factualness} articles with factualness rating")
    print(f"  → {with_spin} articles with spin rating")
    print(f"  → {with_risk} articles with risk rating")

    print(f"\n[Phase 1 Complete]")
    print(f"  ✓ {len(media_only)} media URLs ready for reconstitution")

    return retriever, media_only


# ---------------------------------------------------------------------------
# Phase 2: RAG Reconstitution
# ---------------------------------------------------------------------------

def run_phase2(
    articles: list[dict],
    live: bool = False,
) -> tuple[list[dict], dict[str, dict]]:
    """Reconstitute full-text from RAG indices."""
    _hr("Phase 2: Full-Document Reconstitution")

    urls = [a["url"] for a in articles]
    reconstituted: dict[str, dict] = {}

    if live:
        print(f"\n[RAG Reconstitution - LIVE MODE]")
        recon = FullDocReconstituter(RAG_URL)

        for idx_folder in INDEX_FOLDERS:
            remaining = [u for u in urls if u not in reconstituted]
            if not remaining:
                print(f"  ✓ All URLs reconstituted, skipping {idx_folder}")
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

        print(f"\n  ✓ {len(reconstituted)} TOTAL documents reconstituted")
    else:
        print(f"\n[RAG Reconstitution - DRY RUN MODE]")
        print(f"  → Building synthetic docs for {len(urls)} URLs")
        for url in urls:
            reconstituted[url] = {
                "url": url,
                "title": f"[Synthetic] Article from {url[:50]}",
                "summary_text": "Synthetic summary for demo.",
                "full_text": "Synthetic full-doc text from QA context reconstitution.",
                "doc_metadata": {},
                "qa_count": 5,
            }
        print(f"  ✓ {len(reconstituted)} synthetic documents created")

    # Merge reconstituted content into articles
    enriched_articles = []
    for article in articles:
        url = article["url"]
        if url in reconstituted:
            enriched = dict(article)
            enriched["full_text"] = reconstituted[url].get("full_text", "")
            enriched["summary_text"] = reconstituted[url].get("summary_text", "")
            enriched["doc_metadata"] = reconstituted[url].get("doc_metadata", {})
            enriched["qa_count"] = reconstituted[url].get("qa_count", 0)
            enriched["reconstituted"] = True
            enriched_articles.append(enriched)
        else:
            # Keep article with metadata only
            enriched = dict(article)
            enriched["full_text"] = ""
            enriched["reconstituted"] = False
            enriched_articles.append(enriched)

    recon_count = sum(1 for a in enriched_articles if a.get("reconstituted"))

    print(f"\n[Phase 2 Complete]")
    print(f"  ✓ {recon_count}/{len(enriched_articles)} articles with full-text")
    print(f"  ✓ {len(enriched_articles) - recon_count} metadata-only articles")

    return enriched_articles, reconstituted


# ---------------------------------------------------------------------------
# Phase 3: Threat Cluster Analysis
# ---------------------------------------------------------------------------

def run_phase3(
    articles: list[dict],
    live: bool = False,
    model_name: str = "gpt-5.4",
) -> dict:
    """Run threat cluster analysis on media framing patterns."""
    _hr("Phase 3: Media Framing Threat Analysis")

    # Build analysis bundle
    bundle = {
        "category": "media_framing_analysis",
        "articles": articles,
        "article_count": len(articles),
    }

    print(f"\n[Analysis Dataset]")
    print(f"  → {len(articles)} media articles")
    print(f"  → {sum(1 for a in articles if a.get('reconstituted'))} with full-text")
    print(f"  → {sum(1 for a in articles if a.get('media_framing'))} with media framing")

    # Ranked article char counts
    print(f"\n[Article Sizes — Top 20]")
    article_sizes = []
    for a in articles:
        ft_len = len(a.get("full_text", ""))
        article_sizes.append((ft_len, a.get("publisher", "Unknown")[:30], a.get("title", "")[:60]))
    article_sizes.sort(key=lambda x: x[0], reverse=True)

    total_chars = 0
    for rank, (sz, pub, title) in enumerate(article_sizes[:20], 1):
        total_chars += sz
        print(f"  {rank:2}. {sz:>8,} chars | {pub:30} | {title}")

    full_total = sum(x[0] for x in article_sizes)
    print(f"  {'':2}  {'--------':>8}")
    print(f"  {'':2}  {full_total:>8,} chars TOTAL ({full_total // 4:,} est. tokens)")

    # Build prompt (no EO grounding, just media framing)
    analyser = ThreatClusterAnalyser()
    prompt = analyser.build_prompt(
        "media_framing_analysis",
        bundle,
        grounding_events=None,  # No EO context
        prior_analysis=None,
    )

    print(f"\n[LLM Analysis]")
    print(f"  → Model: {model_name}")
    print(f"  → Prompt: {len(prompt):,} chars")

    estimated_input_tokens = len(prompt) // 4
    in_rate, out_rate = _get_llm_rates(model_name, estimated_input_tokens)
    estimated_cost = estimated_input_tokens / 1_000_000 * in_rate
    print(f"  → Estimated input: ~{estimated_input_tokens:,} tokens")
    print(f"  → Estimated cost: ~${estimated_cost:.4f}")

    llm_output = ""
    usage_stats = {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0.0}

    if live:
        print(f"  → Calling {model_name}...")
        try:
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
                client.generate_content_with_usage(prompt, **call_kwargs)
            )

            llm_output = response["text"]
            usage_stats["input_tokens"] = response["usage"]["input_tokens"]
            usage_stats["output_tokens"] = response["usage"]["output_tokens"]

            in_rate, out_rate = _get_llm_rates(model_name, usage_stats["input_tokens"])
            usage_stats["cost_usd"] = (
                usage_stats["input_tokens"] / 1_000_000 * in_rate
                + usage_stats["output_tokens"] / 1_000_000 * out_rate
            )

            print(f"  ✓ Response received")
            print(f"    • Input tokens:  {usage_stats['input_tokens']:,}")
            print(f"    • Output tokens: {usage_stats['output_tokens']:,}")
            print(f"    • Cost: ${usage_stats['cost_usd']:.6f}")

        except Exception as e:
            print(f"  ⚠ LLM call failed: {e}")
            live = False

    if not live:
        print(f"  → DRY RUN: generating synthetic analysis")
        llm_output = (
            "## Media Framing Threat Analysis\n\n"
            f"Analysis of {len(articles)} media articles reveals coordinated framing patterns "
            "across multiple publications.\n\n"
            "# Cluster 1 — Convergent Critical Coverage\n\n"
            "Multiple outlets demonstrate aligned editorial framing on policy implementations.\n\n"
        )

    # Parse result
    result = analyser.parse_result("media_framing_analysis", llm_output, bundle)
    result["usage"] = usage_stats

    # Save to markdown
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    for dest_dir in (WORKING_DIR, CACHE_DIR):
        dest_dir.mkdir(parents=True, exist_ok=True)
        md_path = dest_dir / f"media_framing_analysis_{ts}.md"

        rates = LLM_PRICING.get(model_name, {})
        content = [
            "# Media Framing Threat Analysis\n\n",
            f"- **Model:** {model_name}\n",
            f"- **Timestamp:** {ts}\n",
            f"- **Articles:** {len(articles)}\n",
            f"- **Input tokens:** {usage_stats.get('input_tokens', 0):,}\n",
            f"- **Output tokens:** {usage_stats.get('output_tokens', 0):,}\n",
            f"- **Cost:** ${usage_stats.get('cost_usd', 0.0):.6f}\n",
            f"- **Rates:** input=${rates.get('input', '?')} / output=${rates.get('output', '?')}\n\n",
            "---\n\n",
            llm_output,
        ]
        md_path.write_text("".join(content))
        print(f"  ✓ Saved: {md_path}")

    print(f"\n[Phase 3 Complete]")
    print(f"  ✓ {result.get('cluster_count', 0)} threat clusters identified")
    print(f"  ✓ Analysis saved to {WORKING_DIR}")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Media-only threat analysis (no EO linkage)"
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in live mode (call RAG + LLM, default: dry-run)",
    )
    parser.add_argument(
        "--sample",
        type=int,
        help="Sample N articles (default: use all)",
    )
    parser.add_argument(
        "--model",
        default="gpt-5.4",
        help="LLM model to use (default: gpt-5.4)",
    )
    args = parser.parse_args()

    print(f"\n{'=' * 70}")
    print(f"Media Framing Threat Analysis Demo")
    print(f"Mode: {'LIVE' if args.live else 'DRY RUN'}")
    print(f"{'=' * 70}")

    # Phase 1: Load BBOA articles
    retriever, articles = run_phase1(sample_size=args.sample)

    # Phase 2: Reconstitute full-text
    enriched_articles, reconstituted = run_phase2(articles, live=args.live)

    # Phase 3: Threat cluster analysis
    result = run_phase3(enriched_articles, live=args.live, model_name=args.model)

    _hr("ANALYSIS COMPLETE")
    print(f"\nResults saved to: {WORKING_DIR}")
    print(f"  → {result.get('cluster_count', 0)} threat clusters identified")
    print(f"  → {len(enriched_articles)} articles analyzed")
    print(f"  → Cost: ${result.get('usage', {}).get('cost_usd', 0.0):.6f}\n")


if __name__ == "__main__":
    main()
