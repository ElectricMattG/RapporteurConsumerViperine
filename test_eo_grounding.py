#!/usr/bin/env python3
"""
Test script for Phase 8.1 EO grounding functionality.

Demonstrates the fixed _fetch_eo_grounding() implementation.
"""

import json

import requests

RAG_URL = "http://localhost:8000"

def test_eo_overlay_status():
    """Check if EO overlay is enabled and what data is loaded."""
    print("=== Checking EO Overlay Status ===")
    response = requests.get(f"{RAG_URL}/ready", timeout=5)
    data = response.json()

    eo_overlay = data.get("eo_overlay", {})
    print(f"EO overlay enabled: {eo_overlay.get('eo_overlay_enabled')}")
    print(f"EO summaries loaded: {eo_overlay.get('eo_summaries')}")
    print(f"EO QA pairs loaded: {eo_overlay.get('eo_qa_pairs')}")
    print(f"EO top_k: {eo_overlay.get('eo_top_k')}")
    print(f"EO grounding mode: {eo_overlay.get('eo_grounding_mode')}")
    print()
    return eo_overlay.get("eo_overlay_enabled", False)


def enable_eo_overlay():
    """Enable EO overlay on RAG service."""
    print("=== Enabling EO Overlay ===")
    response = requests.post(
        f"{RAG_URL}/toggle-eo-events",
        json={"eo_overlay_enabled": True},
        timeout=10
    )
    response.raise_for_status()
    data = response.json()
    print(f"EO overlay enabled: {data.get('eo_overlay_enabled')}")
    print(f"EO summaries: {data.get('eo_summaries_loaded')}")
    print()
    return data


def test_eo_query(eo_number: str):
    """Query for a specific EO and show results."""
    print(f"=== Querying EO {eo_number} ===")
    query = f"Executive Order {eo_number}"

    try:
        response = requests.post(
            f"{RAG_URL}/search",
            json={"query": query, "top_k": 2},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        print(f"Query: {query}")
        print(f"Main docs retrieved: {len(data.get('retrieved_docs', []))}")

        eo_results = data.get("eo_results", [])
        print(f"EO overlay results: {len(eo_results)}")

        if eo_results:
            print("\n--- First EO Result ---")
            eo = eo_results[0]
            print(f"EO IDs: {eo.get('eo_ids')}")
            print(f"Similarity: {eo.get('similarity_score', 0):.4f}")

            doc_meta = eo.get("doc_metadata", {})
            title = doc_meta.get("title", "N/A")
            print(f"Title: {title[:150]}")

            summary = eo.get("summary_text", "")
            print(f"\nSummary length: {len(summary)} chars")
            print(f"Summary (first 400 chars):\n{summary[:400]}...")

            return eo
        else:
            print("No EO results found.")
            print("This may indicate:")
            print("  - Embedding service connection issue (temporary)")
            print("  - EO overlay needs to be enabled")
            print("  - EO not in index")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        if "connection" in str(e).lower():
            print("Note: Embedding service may be reconnecting")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def test_multiple_eos():
    """Test querying multiple EOs as the ConcludeStage does."""
    eo_numbers = ["14148", "14347", "14151"]

    print("=== Testing Multiple EO Queries ===")
    print(f"EO numbers to query: {', '.join(eo_numbers)}\n")

    results = []
    for eo_num in eo_numbers:
        result = test_eo_query(eo_num)
        if result:
            results.append((eo_num, result))
        print()

    print(f"Successfully retrieved {len(results)} / {len(eo_numbers)} EOs")
    return results


if __name__ == "__main__":
    print("Phase 8.1 EO Grounding Test\n")
    print("=" * 60)
    print()

    # Check current status
    overlay_enabled = test_eo_overlay_status()

    # Enable if not already
    if not overlay_enabled:
        enable_eo_overlay()

    # Test single EO query
    print()
    test_eo_query("14347")

    print("\n" + "=" * 60)
    print()

    # Test multiple EOs
    test_multiple_eos()

    print("\n" + "=" * 60)
    print("\nFIXES APPLIED:")
    print("1. Removed incorrect 'enrich' and 'enrich_types' parameters")
    print("2. Now correctly uses EO overlay results from /search endpoint")
    print("3. Fixed title extraction from doc_metadata")
    print("4. Added EO overlay enablement before queries")
    print("5. Simplified query to just 'Executive Order XXXXX'")
    print("\nThe fixed _fetch_eo_grounding() method will:")
    print("- Enable EO overlay on RAG service")
    print("- Query each EO by number")
    print("- Extract eo_results field from response")
    print("- Format as markdown grounding section")
    print("- Inject into conclude prompt before document context")
