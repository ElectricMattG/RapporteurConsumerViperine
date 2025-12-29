#!/usr/bin/env python3
"""Test punctuation handling in citation extraction"""
import sys
sys.path.insert(0, '/Users/home/workspace/ScrapeLinkedWebSources/src')

from consumers.kafka_citation_extractor import KafkaCitationExtractorConsumer

# Create consumer instance
consumer = KafkaCitationExtractorConsumer()

# Test cases with different punctuation scenarios
test_cases = [
    {
        "name": "Period at end",
        "content": "This is a fact from [the comprehensive source](https://example.com/article).",
    },
    {
        "name": "Comma after",
        "content": "According to [this detailed report](https://example.com/report), the data shows trends.",
    },
    {
        "name": "Exclamation",
        "content": "This was groundbreaking [research findings](https://example.com/research)!",
    },
    {
        "name": "Question mark",
        "content": "Is this from [the published study](https://example.com/study)?",
    },
    {
        "name": "No punctuation",
        "content": "See [this article here](https://example.com/article) for details",
    },
    {
        "name": "Multiple citations",
        "content": "First [source here](https://example.com/one). Second [source there](https://example.com/two). Third [another source](https://example.com/three).",
    },
]

print("=" * 80)
print("PUNCTUATION HANDLING TEST")
print("=" * 80)

for test in test_cases:
    print(f"\n📝 Test: {test['name']}")
    print(f"Before: {test['content']}")
    
    chunk = {
        'content': test['content'],
        'chunk_metadata': {'chunk_id': 'test_chunk'}
    }
    
    enriched = consumer._enrich_chunk_with_citations(chunk, 'https://example.com')
    
    print(f"After:  {enriched['content']}")
    
    citations = enriched.get('chunk_metadata', {}).get('citations', [])
    if citations:
        nums = [f"[{c['citation_number']}]" for c in citations]
        print(f"Citations: {nums}")

print("\n" + "=" * 80)
