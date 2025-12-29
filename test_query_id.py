#!/usr/bin/env python3
"""Test query_id preservation through citation extraction"""
import sys
sys.path.insert(0, 'src')
from consumers.kafka_citation_extractor import KafkaCitationExtractorConsumer

# Create consumer
consumer = KafkaCitationExtractorConsumer()

# Test chunk with query_id
test_chunk = {
    'content': 'This is [a test link](https://example.com/test).',
    'document_metadata': {
        'query_id': 'israel_pm',
        'document_id': 'doc_test_123',
        'source_url': 'https://example.com'
    },
    'chunk_metadata': {
        'chunk_id': 'chunk_test_456'
    }
}

# Enrich the chunk
enriched = consumer._enrich_chunk_with_citations(test_chunk, 'https://example.com')

print('=' * 80)
print('QUERY_ID PRESERVATION TEST')
print('=' * 80)
print()
print('ORIGINAL CHUNK:')
print(f"  query_id: {test_chunk['document_metadata']['query_id']}")
print(f"  document_id: {test_chunk['document_metadata']['document_id']}")
print(f"  content: {test_chunk['content']}")
print()
print('ENRICHED CHUNK:')
print(f"  query_id: {enriched['document_metadata']['query_id']}")
print(f"  document_id: {enriched['document_metadata']['document_id']}")
print(f"  content: {enriched['content']}")
print(f"  has_citations: {enriched['chunk_metadata']['has_citations']}")
print(f"  citation_count: {enriched['chunk_metadata']['citation_count']}")
print()
print('✅ query_id PRESERVED' if enriched['document_metadata']['query_id'] == 'israel_pm' else '❌ query_id LOST')
print('=' * 80)
