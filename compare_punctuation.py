#!/usr/bin/env python3
"""Compare original vs transformed content for punctuation handling"""
import json
from pathlib import Path

# Load original chunks
original_chunks = []
with open('cache/test_citation/chunked_document.jsonl', 'r') as f:
    for line in f:
        if line.strip():
            original_chunks.append(json.loads(line))

# Load transformed chunks
transformed_chunks = []
with open('cache/test_citation/output/citation_ready/chunked_document_content.jsonl', 'r') as f:
    for line in f:
        if line.strip():
            transformed_chunks.append(json.loads(line))

print("=" * 80)
print("PUNCTUATION PRESERVATION EXAMPLES")
print("=" * 80)

# Compare chunks with citations
for i, (orig, trans) in enumerate(zip(original_chunks, transformed_chunks)):
    orig_content = orig.get('content', '')
    trans_content = trans.get('content', '')
    
    # Look for markdown links in original
    if '](/i' in orig_content:
        print(f"\n📄 Chunk {i + 1}:")
        print("\nBEFORE (markdown with links):")
        
        # Show relevant excerpt with context
        lines = orig_content.split('\n')
        for line in lines:
            if '](/i' in line:
                print(f"  {line[:100]}...")
        
        print("\nAFTER (transformed with citation numbers):")
        lines = trans_content.split('\n')
        for line in lines:
            if '[' in line and ']' in line and any(c.isdigit() for c in line):
                print(f"  {line[:100]}...")
        
        # Show if there are trailing punctuation marks
        citation_count = trans.get('chunk_metadata', {}).get('citation_count', 0)
        if citation_count > 0:
            print(f"\n  ✅ Citations: {citation_count}")
            
            # Check for punctuation after citation numbers
            import re
            pattern = r'\[\d+\]([.,;:!?])'
            matches = re.findall(pattern, trans_content)
            if matches:
                print(f"  ✅ Trailing punctuation preserved: {matches}")

print("\n" + "=" * 80)
