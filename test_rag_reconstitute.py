#!/usr/bin/env python3
"""Test RAG reconstitution for EO documents.

Usage:
    python test_rag_reconstitute.py [URL] [--index FOLDER]

Examples:
    python test_rag_reconstitute.py
    python test_rag_reconstitute.py https://www.federalregister.gov/d/2025-15822
    python test_rag_reconstitute.py https://www.federalregister.gov/d/2025-15822 --index x_indices_eo
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from src.components.analysis.media_linkage import FullDocReconstituter
from src.components.analysis.media_linking_stage import \
    _deduplicate_text_blocks

# Default test URLs from the grounding package
DEFAULT_TEST_URLS = {
    "EO 14335 (Space)": "https://www.federalregister.gov/d/2025-15822",
    "EO 13358 (Military Appointments)": "https://www.federalregister.gov/d/04-22212",
    "EO 13806 (Manufacturing)": "https://www.federalregister.gov/d/2017-15860",
}

DEFAULT_RAG_URL = "http://localhost:8001"
DEFAULT_INDEX = "x_indices_eo"


def print_section(title: str, width: int = 80) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * width)
    print(title.center(width))
    print("=" * width)


def print_subsection(title: str, width: int = 80) -> None:
    """Print a formatted subsection header."""
    print("\n" + "-" * width)
    print(title)
    print("-" * width)


def analyze_text_quality(text: str) -> dict[str, any]:
    """Analyze text for quality issues."""
    lines = text.split('\n')
    blocks = text.split('\n\n')

    # Count repeated blocks
    block_counts = {}
    substantial_blocks = [b.strip() for b in blocks if len(b.strip()) > 200]
    for block in substantial_blocks:
        block_counts[block] = block_counts.get(block, 0) + 1

    duplicates = {b: c for b, c in block_counts.items() if c > 1}

    return {
        "total_chars": len(text),
        "total_lines": len(lines),
        "total_blocks": len(blocks),
        "substantial_blocks": len(substantial_blocks),
        "duplicate_blocks": len(duplicates),
        "max_repetitions": max(duplicates.values()) if duplicates else 0,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Test RAG reconstitution for EO documents",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Default test URLs:
{chr(10).join(f"  {name}: {url}" for name, url in DEFAULT_TEST_URLS.items())}
""",
    )
    parser.add_argument(
        "url",
        nargs="?",
        help="URL to reconstitute (default: EO 14335)",
    )
    parser.add_argument(
        "--index",
        default=DEFAULT_INDEX,
        help=f"RAG index folder (default: {DEFAULT_INDEX})",
    )
    parser.add_argument(
        "--rag-url",
        default=DEFAULT_RAG_URL,
        help=f"RAG service URL (default: {DEFAULT_RAG_URL})",
    )
    parser.add_argument(
        "--save",
        help="Save full_text to file",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Test all default URLs",
    )
    parser.add_argument(
        "--show-deduped",
        action="store_true",
        help="Show full deduplicated text",
    )
    parser.add_argument(
        "--dedup-threshold",
        type=int,
        default=200,
        help="Minimum block size for deduplication (default: 200)",
    )

    args = parser.parse_args()

    # Determine which URLs to test
    if args.all:
        urls_to_test = list(DEFAULT_TEST_URLS.items())
    elif args.url:
        urls_to_test = [("User Provided", args.url)]
    else:
        # Default to first test URL
        name, url = next(iter(DEFAULT_TEST_URLS.items()))
        urls_to_test = [(name, url)]

    print_section("RAG RECONSTITUTION TEST", 80)
    print(f"RAG Service: {args.rag_url}")
    print(f"Index Folder: {args.index}")
    print(f"Testing {len(urls_to_test)} URL(s)")

    # Initialize reconstituter
    reconstituter = FullDocReconstituter(
        rag_url=args.rag_url,
        timeout=30,
        max_results=100,
    )

    # Switch index
    print_subsection(f"Switching to index: {args.index}")
    if reconstituter.ensure_index(args.index):
        print(f"✓ Successfully switched to {args.index}")
    else:
        print(f"✗ Failed to switch to {args.index}")
        print("⚠ Continuing with current active index...")

    # Test each URL
    for name, url in urls_to_test:
        print_section(f"Testing: {name}", 80)
        print(f"URL: {url}")

        result = reconstituter.reconstitute(url)

        if not result:
            print("\n✗ RECONSTITUTION FAILED - No results returned")
            continue

        print("\n✓ RECONSTITUTION SUCCESSFUL")

        # Show metadata
        print_subsection("Metadata")
        print(f"Title: {result['title'][:100]}...")
        print(f"QA Pairs: {result['qa_count']}")
        print(f"Summary Length: {len(result['summary_text'])} chars")
        print(f"Full Text Length: {len(result['full_text'])} chars")

        # Show doc_metadata
        if result['doc_metadata']:
            print("\nDocument Metadata:")
            for key, value in result['doc_metadata'].items():
                value_str = str(value)[:100]
                print(f"  {key}: {value_str}")

        # Analyze quality
        print_subsection("Text Quality Analysis")
        summary_quality = analyze_text_quality(result['summary_text'])
        full_quality = analyze_text_quality(result['full_text'])

        print("Summary Text:")
        for key, value in summary_quality.items():
            print(f"  {key}: {value}")

        print("\nFull Text:")
        for key, value in full_quality.items():
            print(f"  {key}: {value}")

        # Show sample
        print_subsection("Full Text Sample (first 500 chars)")
        print(result['full_text'][:500])
        print("...")

        if full_quality['duplicate_blocks'] > 0:
            print_subsection("⚠ DUPLICATE BLOCKS DETECTED")
            print(f"Found {full_quality['duplicate_blocks']} duplicate substantial blocks")
            print(f"Max repetitions: {full_quality['max_repetitions']}")
            print("\nThis suggests the RAG service is returning duplicated content.")

        # Apply deduplication and show results
        if args.show_deduped or full_quality['duplicate_blocks'] > 0:
            print_subsection(f"Applying Deduplication (threshold: {args.dedup_threshold} chars)")
            deduped_text = _deduplicate_text_blocks(
                result['full_text'],
                min_block_size=args.dedup_threshold
            )
            deduped_quality = analyze_text_quality(deduped_text)

            print(f"Original: {len(result['full_text'])} chars, {full_quality['total_blocks']} blocks")
            print(f"Deduped:  {len(deduped_text)} chars, {deduped_quality['total_blocks']} blocks")
            print(f"Reduction: {100 * (1 - len(deduped_text)/len(result['full_text'])):.1f}%")
            print(f"Duplicate blocks removed: {full_quality['duplicate_blocks']}")

            if args.show_deduped:
                print_section("FULL DEDUPLICATED TEXT", 80)
                print(deduped_text)
                print_section("END OF DEDUPLICATED TEXT", 80)

        # Save if requested
        if args.save:
            output_file = Path(args.save)
            output_file.write_text(
                json.dumps(result, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            print(f"\n✓ Saved full result to: {output_file}")

    print_section("TEST COMPLETE", 80)


if __name__ == "__main__":
    main()
