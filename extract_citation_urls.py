#!/usr/bin/env python3
"""Extract URLs from citations in a JSONL file."""

import json
from pathlib import Path

# Input file
input_file = "/mnt/workspace_2/rapporteur_indices/staging/backups/idf_staging/archive/time_series_idf_20231112_20231119/req_20260107_233910_005/chunked_document_enriched.jsonl"

# Output file
output_file = "/mnt/workspace_2/rapporteur_indices/staging/backups/idf_staging/archive/time_series_idf_20231112_20231119/req_20260107_233910_005/citation_urls.txt"

# Collect all URLs
urls = []

print(f"Reading from: {input_file}")

with open(input_file, 'r', encoding='utf-8') as f:
    for line_num, line in enumerate(f, 1):
        line = line.strip()
        if not line:
            continue

        try:
            data = json.loads(line)

            # Extract URLs from citations array
            citations = data.get('citations', [])
            for citation in citations:
                url = citation.get('url')
                if url:
                    urls.append(url)

        except json.JSONDecodeError as e:
            print(f"Error parsing line {line_num}: {e}")
            continue

# Remove duplicates while preserving order
unique_urls = list(dict.fromkeys(urls))

# Write to output file
with open(output_file, 'w', encoding='utf-8') as f:
    for url in unique_urls:
        f.write(url + '\n')

print(f"\nExtracted {len(urls)} total URLs")
print(f"Found {len(unique_urls)} unique URLs")
print(f"Saved to: {output_file}")
