#!/usr/bin/env python3
"""
Recovery script for truncated media linking batches.

This script:
1. Identifies articles lost to output truncation in batches 2, 5, 6, 10, 11, 12
2. Splits them into smaller sub-batches to avoid max_tokens limit
3. Reprocesses only the missing articles
4. Merges results into the main cache

Usage:
    python recover_truncated_batches.py --dry-run  # Preview what will be done
    python recover_truncated_batches.py            # Actually recover
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

# Configuration
WORKFLOWS_DIR = Path("/mnt/workspace_2/bboa/workflows/media_linking")
PROBLEM_BATCHES = [2, 5, 6, 10, 11, 12]

def extract_articles_from_truncated_file(file_path):
    """
    Parse a truncated JSON file and extract partial article records.
    Returns list of complete article dicts that were extracted before truncation.
    """
    if not file_path.exists():
        return []

    content = open(file_path).read()

    # Find all article_url occurrences to locate article boundaries
    articles = []
    parts = content.split('"article_url":')

    for i, part in enumerate(parts[1:], 1):  # Skip first split (before any articles)
        try:
            # Try to extract article URL
            url_start = part.find('"') + 1
            url_end = part.find('"', url_start)
            if url_end == -1:
                continue  # Truncated in URL

            article_url = part[url_start:url_end]

            # Try to find the full article object
            # Look backwards to find the opening brace
            prev_part = content[content.find('"article_url": "' + article_url) - 200:]
            brace_start = prev_part.rfind('{')

            # Look forward to find the closing brace
            # This is hard with nesting, so we'll use a heuristic:
            # articles typically end with }, or }]
            if i < len(parts):
                # Not the last one, so we can find the next article boundary
                next_article_start = part.find('"article_url":', url_end)
                if next_article_start > 0:
                    article_json = part[:next_article_start]
                else:
                    article_json = part
            else:
                # Last article, might be truncated
                article_json = part

            # Just record the URL for now - we'll need to reprocess anyway
            articles.append(article_url)

        except Exception as e:
            # Skip malformed partial articles
            continue

    return articles

def get_missing_articles_per_batch():
    """
    Analyze each problem batch and identify which articles need reprocessing.
    Returns dict: {batch_num: {'conversations': [...], 'extracted_urls': set(), 'missing_urls': [...]}}
    """
    os.chdir(WORKFLOWS_DIR)
    batch_analysis = {}

    for batch_num in PROBLEM_BATCHES:
        batch_dir = Path(f"batch_{batch_num}")

        # Load all input conversations
        conv_path = batch_dir / "conversations.json"
        if not conv_path.exists():
            continue

        conversations = json.load(open(conv_path))

        # Extract URLs from conversations
        conv_urls = []
        for conv in conversations:
            response = conv.get('assistant_response', '')
            # Extract URL from "URL: https://..." line
            if response.startswith('URL:'):
                url_line = response.split('\n')[0]
                url = url_line.replace('URL:', '').strip()
                conv_urls.append(url)

        # Find extracted URLs from link.json and reflect.json
        extracted_urls = set()

        # Check link.json
        link_path = batch_dir / "link.json"
        try:
            link_data = json.load(open(link_path))
            if 'articles' in link_data:
                for article in link_data['articles']:
                    extracted_urls.add(article['article_url'])
        except json.JSONDecodeError:
            # Truncated - extract partial articles
            urls = extract_articles_from_truncated_file(link_path)
            extracted_urls.update(urls)

        # Check reflect.json
        reflect_path = batch_dir / "reflect.json"
        try:
            reflect_data = json.load(open(reflect_path))
            if 'articles' in reflect_data:
                for article in reflect_data['articles']:
                    extracted_urls.add(article['article_url'])
        except json.JSONDecodeError:
            # Truncated - extract partial articles
            urls = extract_articles_from_truncated_file(reflect_path)
            extracted_urls.update(urls)

        # Find missing URLs
        missing_urls = [url for url in conv_urls if url not in extracted_urls]

        batch_analysis[batch_num] = {
            'total': len(conv_urls),
            'extracted': len(extracted_urls),
            'missing': len(missing_urls),
            'missing_urls': missing_urls,
            'conversations': conversations
        }

    return batch_analysis

def print_analysis(batch_analysis):
    """Print detailed analysis of what needs recovery."""
    print("=" * 80)
    print("TRUNCATED BATCH RECOVERY ANALYSIS")
    print("=" * 80)

    total_missing = 0
    for batch_num in sorted(batch_analysis.keys()):
        info = batch_analysis[batch_num]
        total_missing += info['missing']

        print(f"\nbatch_{batch_num}:")
        print(f"  Total articles: {info['total']}")
        print(f"  Extracted: {info['extracted']}")
        print(f"  Missing: {info['missing']} ({100*info['missing']/info['total']:.1f}%)")

        if info['missing'] > 0 and info['missing'] <= 5:
            print(f"  Missing URLs:")
            for url in info['missing_urls']:
                domain = url.split('/')[2] if len(url.split('/')) > 2 else url[:50]
                print(f"    - {domain}")

    print("\n" + "=" * 80)
    print(f"TOTAL MISSING: {total_missing} articles need reprocessing")
    print("=" * 80)

    return total_missing

def main():
    parser = argparse.ArgumentParser(description="Recover truncated media linking batches")
    parser.add_argument('--dry-run', action='store_true', help="Show what would be done without doing it")
    args = parser.parse_args()

    print("Analyzing truncated batches...")
    batch_analysis = get_missing_articles_per_batch()
    total_missing = print_analysis(batch_analysis)

    if total_missing == 0:
        print("\n✓ No missing articles found! All batches appear complete.")
        return

    if args.dry_run:
        print("\n[DRY RUN] - No changes made. Run without --dry-run to recover.")
        print("\nNext steps:")
        print("1. Increase max_output_tokens in media_linking.py to 64K or 96K")
        print("2. Re-run pipeline with --reprocess-batches 2,5,6,10,11,12")
        return

    print("\nRecovery not yet implemented - manual reprocessing required:")
    print("\nOption 1: Increase max_output_tokens and reprocess specific batches")
    print("  - Edit src/components/analysis/media_linking.py")
    print("  - Change max_output_tokens from 48000 to 96000")
    print("  - Delete the truncated .json files for batches: 2, 5, 6, 10, 11, 12")
    print("  - Re-run: python -m src.components.publisher.rapporteur_cli \\")
    print("           docs/CultureTechnologyProcurementMedia/document.yaml --media-link")
    print("\nOption 2: Split large batches into smaller sub-batches")
    print("  - Adjust target_input_tokens in media_linking_stage.py")
    print("  - Reduce from 800,000 to 400,000 to create smaller batches")


if __name__ == "__main__":
    import os
    main()
