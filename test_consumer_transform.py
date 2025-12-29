#!/usr/bin/env python3
"""Test retrofitted citation extraction in consumer"""
import sys
sys.path.insert(0, '/Users/home/workspace/ScrapeLinkedWebSources/src')

from consumers.kafka_citation_extractor import KafkaCitationExtractorConsumer
import json
from pathlib import Path

# Create consumer instance
consumer = KafkaCitationExtractorConsumer()

# Test with chunks
test_file = Path('cache/test_citation/chunked_document.jsonl')
chunks = []
with open(test_file, 'r') as f:
    for line in f:
        if line.strip():
            chunks.append(json.loads(line))

print(f'📚 Loaded {len(chunks)} chunks')
print('=' * 80)

# Test on chunk with citations (chunk index 2)
chunk = chunks[2]
print('ORIGINAL CHUNK:')
print(f'Content preview: {chunk.get("content", "")[:250]}...\n')

# Extract and transform
base_url = 'https://www.jpost.com'
enriched = consumer._enrich_chunk_with_citations(chunk, base_url)

print('TRANSFORMED CHUNK:')
print(f'Content preview: {enriched.get("content", "")[:250]}...\n')
print(f'✅ Has citations: {enriched.get("chunk_metadata", {}).get("has_citations", False)}')
print(f'📊 Citation count: {enriched.get("chunk_metadata", {}).get("citation_count", 0)}\n')

citations = enriched.get('chunk_metadata', {}).get('citations', [])
if citations:
    print(f'Extracted {len(citations)} citations:')
    for cite in citations:
        print(f'  [{cite["citation_number"]}] {cite["anchor_text"][:50]}... -> {cite["url"]}')

print('\n' + '=' * 80)
print('GLOBAL CITATION METADATA:')
for num, meta in consumer.global_citation_metadata.items():
    print(f'  [{num}] {meta["anchor_text"][:50]}...')
    print(f'       URL: {meta["url"]}')
