#!/usr/bin/env python3
"""Test script for BBOA multi-source integration.

Usage:
    python test_bboa_multi_source.py
"""

import json
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.components.analysis.media_linkage import (CATEGORY_ANCHORS,
                                                   BBAORetriever,
                                                   CategoryGrouper,
                                                   ClusteringOutputManager)


def test_multi_source_loading():
    """Test loading from multiple BBOA cache sources."""
    print("=" * 80)
    print("TEST 1: Multi-Source BBOA Cache Loading")
    print("=" * 80)

    cache_sources = [
        "/mnt/workspace_2/bboa/cache/bboa_full_20260310_194954",
        "/mnt/workspace_2/bboa/cache/bboa_trump_20260311_220355",
        "/mnt/workspace_2/bboa/cache/bboa_combined_20260326_153800",
    ]

    # Check which sources exist
    existing_sources = []
    for source in cache_sources:
        if Path(source).exists():
            existing_sources.append(source)
            print(f"✓ Found: {source}")
        else:
            print(f"✗ Missing: {source}")

    if not existing_sources:
        print("\n❌ No BBOA cache sources found. Exiting.")
        return False

    print(f"\n📦 Loading {len(existing_sources)} cache source(s)...")
    retriever = BBAORetriever(existing_sources)
    retriever.load()

    # Display summary
    summary = retriever.summary()
    print(f"\n📊 Summary:")
    print(f"   Cache count: {summary['cache_count']}")
    print(f"   Session IDs: {', '.join(summary['session_ids'])}")
    print(f"   EO events: {summary['eo_events']:,}")
    print(f"   Linked media: {summary['linked_media']:,}")
    print(f"   Merged events: {summary['merged_events']:,}")
    print(f"   Timeline: {summary['timeline']:,}")
    print(f"   Unlinked: {summary['unlinked']:,}")
    print(f"   Unique EOs: {summary['unique_eo_identifiers']}")

    # Check media_framing availability
    media_with_framing = sum(
        1 for m in retriever.linked_media
        if m.get("media_framing")
    )
    print(f"\n📰 Media Framing:")
    print(f"   Articles with media_framing: {media_with_framing}/{len(retriever.linked_media)} ({100*media_with_framing//len(retriever.linked_media) if retriever.linked_media else 0}%)")

    # Sample a few articles
    if retriever.linked_media:
        print(f"\n🔍 Sample Articles:")
        for i, article in enumerate(retriever.linked_media[:3], 1):
            framing = article.get("media_framing", {})
            risk = framing.get("risk")
            print(f"\n   [{i}] {article.get('publisher', 'Unknown')} | {article.get('date_publication', '')}")
            print(f"       Title: {article.get('summary', '')[:80]}...")
            print(f"       EO: {article.get('eo_identifier', '')}")
            if isinstance(risk, dict):
                print(f"       Risk: {risk.get('rating', 'N/A')} - {risk.get('reasoning', '')[:60]}...")
            elif risk:
                print(f"       Risk: {risk}")

    print(f"\n✅ Multi-source loading test PASSED")
    return True


def test_category_bundles():
    """Test creating evidence bundles for categories."""
    print("\n" + "=" * 80)
    print("TEST 2: Category Evidence Bundle Creation")
    print("=" * 80)

    cache_sources = [
        "/mnt/workspace_2/bboa/cache/bboa_full_20260310_194954",
        "/mnt/workspace_2/bboa/cache/bboa_trump_20260311_220355",
        "/mnt/workspace_2/bboa/cache/bboa_combined_20260326_153800",
    ]

    existing_sources = [s for s in cache_sources if Path(s).exists()]
    if not existing_sources:
        print("❌ No cache sources available")
        return False

    retriever = BBAORetriever(existing_sources)
    retriever.load()

    # Test culture_and_media category
    category = "culture_and_media"
    print(f"\n📁 Category: {category}")
    print(f"   Anchor EOs: {', '.join(CATEGORY_ANCHORS[category])}")

    # Collect linked media for this category
    bundle_articles = []
    for eo_num in CATEGORY_ANCHORS[category]:
        media = retriever.linked_media_for(eo_num)
        print(f"   EO {eo_num}: {len(media)} articles")
        for m in media:
            bundle_articles.append({
                "url": m.get("link", ""),
                "title": m.get("summary", "")[:100] if m.get("summary") else "",
                "author": m.get("author", ""),
                "publisher": m.get("publisher", ""),
                "date_publication": m.get("date_publication", ""),
                "eo_identifier": m.get("eo_identifier", ""),
                "event_key": m.get("event_key", ""),
                "media_framing": m.get("media_framing", {}),
                "full_text": m.get("summary", ""),
            })

    print(f"\n📦 Bundle Summary:")
    print(f"   Total articles: {len(bundle_articles)}")

    # Count articles with media_framing
    with_framing = sum(1 for a in bundle_articles if a.get("media_framing"))
    print(f"   With media_framing: {with_framing}/{len(bundle_articles)}")

    # Publisher distribution
    publishers = {}
    for a in bundle_articles:
        pub = a.get("publisher", "Unknown")
        publishers[pub] = publishers.get(pub, 0) + 1

    print(f"\n📊 Top Publishers:")
    for pub, count in sorted(publishers.items(), key=lambda x: -x[1])[:5]:
        print(f"   {pub}: {count}")

    print(f"\n✅ Category bundle test PASSED")
    return True


def test_clustering_output():
    """Test ClusteringOutputManager."""
    print("\n" + "=" * 80)
    print("TEST 3: Clustering Output Management")
    print("=" * 80)

    output_dir = "/mnt/workspace_2/bboa/documents/clustering"
    print(f"\n📂 Output directory: {output_dir}")

    output_mgr = ClusteringOutputManager(output_dir)

    # Test session creation
    category = "culture_and_media"
    session_id = "test_20260327_120000"

    # Save test metadata
    metadata = {
        "timestamp": "2026-03-27T12:00:00Z",
        "test_run": True,
        "category": category,
    }
    metadata_path = output_mgr.save_metadata(category, session_id, metadata)
    print(f"✓ Saved metadata: {metadata_path}")

    # Save test clusters
    clusters_data = {
        "category": category,
        "clusters": [
            {
                "cluster_id": "TEST_A",
                "description": "Test cluster",
                "articles": [],
            }
        ],
    }
    clusters_path = output_mgr.save_clusters(category, session_id, clusters_data)
    print(f"✓ Saved clusters: {clusters_path}")

    # Save test analysis
    analysis_md = "# Test Cluster Analysis\n\nThis is a test."
    analysis_path = output_mgr.save_cluster_analysis(category, session_id, analysis_md)
    print(f"✓ Saved analysis: {analysis_path}")

    # List sessions
    sessions = output_mgr.list_sessions(category)
    print(f"\n📋 Found {len(sessions)} session(s) for {category}:")
    for sess in sessions:
        print(f"   {sess['session_id']}: {sess['path']}")

    print(f"\n✅ Clustering output test PASSED")
    return True


def main():
    """Run all tests."""
    print("\n🧪 BBOA Multi-Source Integration Test Suite\n")

    results = []

    try:
        results.append(("Multi-Source Loading", test_multi_source_loading()))
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Multi-Source Loading", False))

    try:
        results.append(("Category Bundles", test_category_bundles()))
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Category Bundles", False))

    try:
        results.append(("Clustering Output", test_clustering_output()))
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Clustering Output", False))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {test_name}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests PASSED!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
