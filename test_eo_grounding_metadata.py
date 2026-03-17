#!/usr/bin/env python3
"""
Test script for Phase 8.1 EO grounding with metadata query approach.

Tests the complete workflow:
1. Extract EO reference URLs from chapters
2. Switch RAG service to EO index
3. Query each URL via /query_metadata
4. Format as grounding text
"""

import json
import re
from pathlib import Path

import requests

RAG_URL = "http://localhost:8000"
CHAPTERS_DIR = Path("/mnt/workspace_2/bboa/documents/atrum_volentia/chapters")


def extract_eo_urls():
    """Extract EO reference URLs from chapters (same as _extract_eo_references)."""
    url_pattern = re.compile(
        r'https://(?:www\.)?(?:federalregister\.gov/[^\s\)]+|whitehouse\.gov/[^\s\)]+)',
        re.IGNORECASE
    )

    eo_urls = set()
    for chapter_file in sorted(CHAPTERS_DIR.glob('0[1-9]*.md')):
        text = chapter_file.read_text(encoding='utf-8')
        for match in url_pattern.finditer(text):
            url = match.group(0)
            # Filter for EO-related URLs
            if any(term in url.lower() for term in [
                'executive-order', 'presidential-documents/executive-orders',
                '/documents/2025/'  # Recent EOs
            ]):
                eo_urls.add(url)

    return sorted(eo_urls)


def check_active_index():
    """Check which index is currently active."""
    response = requests.get(f"{RAG_URL}/ready", timeout=5)
    data = response.json()
    active_folder = data.get("active_folder", "")
    print(f"Active index: {active_folder}")
    return active_folder


def switch_to_eo_index():
    """Switch RAG service to EO index."""
    print("\n=== Switching to EO Index ===")
    response = requests.post(
        f"{RAG_URL}/switch-index",
        json={"folder": "x_indices_eo"},
        timeout=30
    )
    if response.status_code == 200:
        data = response.json()
        print(f"Successfully switched to: {data.get('active_folder')}")
        print(f"Summaries loaded: {data.get('total_summaries')}")
        print(f"QA pairs loaded: {data.get('total_qa_pairs')}")
        return True
    else:
        print(f"Failed to switch: {response.status_code}")
        print(response.text)
        return False


def query_eo_by_url(url):
    """Query a single EO by exact source URL."""
    print(f"\n=== Querying: {url[:80]}... ===")

    try:
        response = requests.post(
            f"{RAG_URL}/query_metadata",
            json={
                "source_url": url,
                "include_summaries": True,
                "include_qa": True,
                "max_results": 100
            },
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        total_summaries = data.get("total_summaries", 0)
        total_qa = data.get("total_qa", 0)

        print(f"Found: {total_summaries} summaries, {total_qa} QA pairs")

        if total_summaries > 0:
            doc = data["retrieved_docs"][0]
            title = doc.get("doc_metadata", {}).get("title", "N/A")
            summary_text = doc.get("summary_text", "")

            print(f"\nTitle: {title}")
            print(f"Summary length: {len(summary_text)} chars")
            print(f"Summary (first 250 chars):\n{summary_text[:250]}...")

            if total_qa > 0:
                print(f"\nFirst 2 QA pairs:")
                for i, qa in enumerate(data["retrieved_qa"][:2], 1):
                    print(f"\n  {i}. Q: {qa.get('question', '')[:100]}")
                    print(f"     A: {qa.get('answer', '')[:100]}")
                    context = qa.get('context', '')
                    print(f"     Context: {len(context)} chars")

            return doc, data["retrieved_qa"]
        else:
            print("No content found for this URL")
            return None, []

    except Exception as e:
        print(f"Error: {e}")
        return None, []


def format_grounding(doc, qas, max_qa=3):
    """Format EO document as grounding text (simulates _fetch_eo_grounding output)."""
    if not doc:
        return ""

    title = doc.get("doc_metadata", {}).get("title", "Executive Order")
    summary_text = doc.get("summary_text", "")

    parts = [f"### {title}\n"]

    if summary_text:
        parts.append(f"\n**Summary:**\n{summary_text}\n")

    if qas:
        parts.append(f"\n**Detailed Content:**\n")
        for qa in qas[:max_qa]:
            context = qa.get("context", "")
            if context:
                parts.append(f"\n{context}\n")

    return "".join(parts)


def main():
    print("=" * 80)
    print("Phase 8.1 EO Grounding Test - Metadata Query Approach")
    print("=" * 80)

    # Step 1: Extract EO URLs from chapters
    print("\n=== Step 1: Extract EO URLs from Chapters ===")
    eo_urls = extract_eo_urls()
    print(f"Found {len(eo_urls)} unique EO reference URLs:")
    for url in eo_urls[:5]:
        print(f"  - {url[:90]}")
    if len(eo_urls) > 5:
        print(f"  ... and {len(eo_urls) - 5} more")

    # Step 2: Check/switch to EO index
    print("\n=== Step 2: Check Active Index ===")
    active = check_active_index()

    if "eo" not in active.lower():
        if not switch_to_eo_index():
            print("\nCannot proceed without EO index")
            return
    else:
        print("Already on EO index")

    # Step 3: Query first 3 EO URLs
    print("\n=== Step 3: Query EO Documents ===")
    grounding_parts = []

    for url in eo_urls[:3]:  # Test first 3
        doc, qas = query_eo_by_url(url)
        if doc:
            grounding_text = format_grounding(doc, qas, max_qa=2)
            grounding_parts.append(grounding_text)
            print(f"\n✓ Retrieved and formatted ({len(grounding_text)} chars)")

    # Step 4: Show final grounding output
    if grounding_parts:
        print("\n" + "=" * 80)
        print("=== Step 4: Final Grounding Output ===")
        print("=" * 80)

        header = (
            "## LEGAL FRAMEWORK GROUNDING\n\n"
            "Full Executive Order documents referenced in this analysis:\n\n"
        )
        full_grounding = header + "\n\n".join(grounding_parts)

        print(f"\nTotal grounding text: {len(full_grounding)} chars")
        print(f"Number of EOs included: {len(grounding_parts)}")
        print("\nFirst 500 chars of grounding:")
        print("-" * 80)
        print(full_grounding[:500])
        print("...")
    else:
        print("\n✗ No grounding retrieved")

    print("\n" + "=" * 80)
    print("IMPLEMENTATION SUMMARY:")
    print("=" * 80)
    print("1. Extract EO URLs from chapter reference sections (regex)")
    print("2. Check RAG service active index, switch to x_indices_eo if needed")
    print("3. Query each URL via /query_metadata (exact source_url match)")
    print("4. Retrieve both summary AND QA pairs (QA context = full text sections)")
    print("5. Format as markdown grounding section")
    print("6. Inject into conclude prompt before document context")
    print("\nThis approach:")
    print("  ✓ No semantic search needed (direct metadata lookup)")
    print("  ✓ Gets complete document including all QA pair contexts")
    print("  ✓ Works with RAG service index switching")
    print(" ✓ Gracefully handles missing documents")


if __name__ == "__main__":
    main()
