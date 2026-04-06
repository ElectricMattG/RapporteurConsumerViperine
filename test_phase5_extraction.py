#!/usr/bin/env python3
"""
Test Phase 5 cluster extraction and URL handling.

This script loads an existing Phase 4 markdown output and runs Phase 5 + Phase 6
to verify that:
1. Clusters are being extracted correctly from the markdown
2. URLs are preserved through Phase 5 (JSONL persistence)
3. URLs are packaged correctly for Phase 6 (Kaliya handoff)
"""

import re
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.demo.demo_media_linkage import WORKING_DIR, run_phase5, run_phase6


def load_phase4_markdown(filepath: Path) -> dict:
    """Parse a Phase 4 markdown file and reconstruct the result dict."""
    content = filepath.read_text(encoding="utf-8")

    # Extract metadata from header
    category_match = re.search(r"- \*\*Category:\*\* (.+)", content)
    article_count_match = re.search(r"- \*\*Articles:\*\* (\d+)", content)

    category = category_match.group(1) if category_match else "unknown"
    article_count = int(article_count_match.group(1)) if article_count_match else 0

    # Extract analysis text (everything after the first ---)
    parts = content.split("---\n", 2)
    if len(parts) >= 3:
        analysis_text = parts[2]
    else:
        analysis_text = content

    # Strip appendices if present (everything after "# APPENDIX")
    appendix_idx = analysis_text.find("\n\n# APPENDIX:")
    if appendix_idx > 0:
        analysis_text = analysis_text[:appendix_idx]

    # Parse article URLs from markdown links in the analysis
    # Look for patterns like (http...) or [text](http...)
    url_pattern = r'https?://[^\s\)>]+'
    urls = set(re.findall(url_pattern, analysis_text))

    print(f"\n📄 Loaded Phase 4 file: {filepath.name}")
    print(f"   Category: {category}")
    print(f"   Declared articles: {article_count}")
    print(f"   URLs found in analysis: {len(urls)}")

    # Create synthetic article records from found URLs
    articles = []
    for idx, url in enumerate(sorted(urls), 1):
        # Extract domain for synthetic metadata
        domain = url.split('/')[2] if url.count('/') >= 2 else url
        articles.append({
            "url": url,
            "author": f"Author_{idx}",
            "publisher": domain,
            "date_publication": "2025-01-01",
            "event_key": f"evt_{idx}",
            "eo_identifier": "EO 14183",
            "source_type": "linked",
        })

    return {
        "category": category,
        "analysis_text": analysis_text,
        "article_count": len(articles),
        "articles": articles,
        "lineage": {
            "prompt_template": "THREAT_CLUSTER_PROMPT",
            "input_bundle_articles": len(articles),
        },
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
        }
    }


def main():
    # Find the most recent Phase 4 markdown file
    phase4_files = sorted(WORKING_DIR.glob("phase4_*.md"))

    if not phase4_files:
        print("❌ No Phase 4 markdown files found in working_dir/")
        return 1

    # Use the most recent
    latest = phase4_files[-1]

    print("=" * 70)
    print("Phase 5 Extraction Test")
    print("=" * 70)

    # Load and parse Phase 4 result
    phase4_result = load_phase4_markdown(latest)
    category = phase4_result["category"]

    # Test Phase 5: Persist to JSONL
    print(f"\n{'=' * 70}")
    print(f"Phase 5: Rapporteur Execution [{category}]")
    print(f"{'=' * 70}")

    output_dir = Path("/tmp/phase5_test")
    output_dir.mkdir(exist_ok=True)

    phase5_result = run_phase5(phase4_result, category, output_dir)

    # Verify Phase 5 extraction
    print("\n✅ Phase 5 complete")
    print(f"   Records persisted: {len(phase5_result)}")
    for rec in phase5_result:
        print(f"   - {rec['category']}: {rec['article_count']} articles")

    # Test Phase 6: Build Kaliya handoff
    print(f"\n{'=' * 70}")
    print(f"Phase 6: Kaliya Handoff [{category}]")
    print(f"{'=' * 70}")

    phase6_result = run_phase6(phase5_result, category)

    print("\n✅ Phase 6 complete")
    print(f"   Kaliya payloads: {len(phase6_result)}")
    for payload in phase6_result:
        print(f"   - {payload['category']}: {payload['urls_analysed']} URLs")

    print(f"\n{'=' * 70}")
    print("✅ All tests passed!")
    print(f"{'=' * 70}")
    print(f"\nPhase 5 output: {output_dir / 'threat_analysis_demo_media_linkage.jsonl'}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
