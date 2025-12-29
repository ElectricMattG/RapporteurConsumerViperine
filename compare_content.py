#!/usr/bin/env python3
"""Compare content side by side for gov.il URLs."""

import json

def load_chunks_by_url(filepath, url):
    """Load all chunks for a specific URL."""
    chunks = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                chunk = json.loads(line)
                if chunk.get('document_metadata', {}).get('source_url') == url:
                    chunks.append(chunk)
    return chunks

def compare_url_content(url_name, url_full, sess1_path, sess2_path):
    """Compare content for a specific URL."""
    print(f"\n{'='*100}")
    print(f"🔍 COMPARING: {url_name}")
    print(f"URL: {url_full}")
    print('='*100)
    
    chunks1 = load_chunks_by_url(sess1_path, url_full)
    chunks2 = load_chunks_by_url(sess2_path, url_full)
    
    print(f"\nChunks:")
    print(f"  sess_e0fc67c87f0lc: {len(chunks1)} chunks")
    print(f"  external_test: {len(chunks2)} chunks")
    
    # Get full content
    content1 = "\n\n".join([c['content'] for c in chunks1])
    content2 = "\n\n".join([c['content'] for c in chunks2])
    
    print(f"\nTotal content length:")
    print(f"  sess_e0fc67c87f0lc: {len(content1)} chars")
    print(f"  external_test: {len(content2)} chars")
    
    # Show document metadata
    if chunks1:
        meta1 = chunks1[0]['document_metadata']
        print(f"\nsess_e0fc67c87f0lc metadata:")
        print(f"  Title: {meta1.get('title')}")
        print(f"  Total chars: {meta1.get('total_chars')}")
        print(f"  Extraction timestamp: {meta1.get('extraction_timestamp')}")
    
    if chunks2:
        meta2 = chunks2[0]['document_metadata']
        print(f"\nexternal_test metadata:")
        print(f"  Title: {meta2.get('title')}")
        print(f"  Total chars: {meta2.get('total_chars')}")
        print(f"  Extraction timestamp: {meta2.get('extraction_timestamp')}")
    
    # Show content comparison
    print(f"\n{'─'*100}")
    print("CONTENT COMPARISON:")
    print('─'*100)
    
    # Split into lines for comparison
    lines1 = content1.split('\n')
    lines2 = content2.split('\n')
    
    print(f"\nFirst 20 lines from sess_e0fc67c87f0lc ({len(lines1)} total lines):")
    print("─" * 50)
    for i, line in enumerate(lines1[:20], 1):
        print(f"{i:3d}: {line[:97]}")
    
    print(f"\nFirst 20 lines from external_test ({len(lines2)} total lines):")
    print("─" * 50)
    for i, line in enumerate(lines2[:20], 1):
        print(f"{i:3d}: {line[:97]}")
    
    # Check if content2 is a prefix of content1
    if content1.startswith(content2):
        print(f"\n⚠️  external_test content is a PREFIX of sess_e0fc67c87f0lc content")
        print(f"    Missing {len(content1) - len(content2)} chars ({len(lines1) - len(lines2)} lines)")
        print(f"\n📄 MISSING CONTENT (last 10 lines from sess_e0fc67c87f0lc):")
        print("─" * 50)
        for i, line in enumerate(lines1[-10:], len(lines1)-9):
            print(f"{i:3d}: {line[:97]}")
    elif content2.startswith(content1):
        print(f"\n⚠️  sess_e0fc67c87f0lc content is a PREFIX of external_test content")
    else:
        print(f"\n⚠️  Contents differ (not a simple prefix relationship)")
        # Find first difference
        min_len = min(len(content1), len(content2))
        for i in range(min_len):
            if content1[i] != content2[i]:
                print(f"\n    First difference at character {i}:")
                print(f"    sess_e0fc67c87f0lc: ...{content1[max(0,i-20):i+50]}...")
                print(f"    external_test:     ...{content2[max(0,i-20):i+50]}...")
                break

# Main comparison
base_dir = "/tmp/session_compare"
sess1_chunks = f"{base_dir}/sess_e0fc67c87f0lc/chunks_aggregated.jsonl"
sess2_chunks = f"{base_dir}/external_test_20251218_171758/chunks_aggregated.jsonl"

# Compare both gov.il URLs
compare_url_content(
    "spoke-statement251023",
    "https://www.gov.il/en/pages/spoke-statement251023",
    sess1_chunks,
    sess2_chunks
)

compare_url_content(
    "statement-by-pm-netanyahu-28-oct-2023",
    "https://www.gov.il/en/pages/statement-by-pm-netanyahu-28-oct-2023",
    sess1_chunks,
    sess2_chunks
)

print(f"\n{'='*100}")
print("📊 SUMMARY")
print('='*100)
print("\nBoth gov.il URLs show partial content in external_test session.")
print("This indicates:")
print("  - Web scraper received incomplete page content")
print("  - Possible causes: timeout, rate limiting, incomplete page load")
print("  - NOT a 403 error (content was received, just incomplete)")
