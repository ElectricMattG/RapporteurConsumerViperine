#!/usr/bin/env python3
"""Compare two session backup files to find differences."""

import json
from collections import defaultdict

def load_jsonl(filepath):
    """Load JSONL file into list of dicts."""
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line))
    return data

def compare_chunks(sess1_path, sess2_path):
    """Compare chunks from two sessions."""
    print("\n" + "="*80)
    print("📦 COMPARING CHUNKS")
    print("="*80)
    
    chunks1 = load_jsonl(sess1_path)
    chunks2 = load_jsonl(sess2_path)
    
    print(f"\nTotal chunks:")
    print(f"  sess_e0fc67c87f0lc: {len(chunks1)}")
    print(f"  external_test_20251218_171758: {len(chunks2)}")
    print(f"  Difference: {abs(len(chunks1) - len(chunks2))}")
    
    # Group by source URI
    chunks1_by_uri = defaultdict(list)
    chunks2_by_uri = defaultdict(list)
    
    for chunk in chunks1:
        uri = chunk.get('document_metadata', {}).get('source_url', 'unknown')
        chunks1_by_uri[uri].append(chunk)
    
    for chunk in chunks2:
        uri = chunk.get('document_metadata', {}).get('source_url', 'unknown')
        chunks2_by_uri[uri].append(chunk)
    
    print(f"\nUnique URIs:")
    print(f"  sess_e0fc67c87f0lc: {len(chunks1_by_uri)}")
    print(f"  external_test_20251218_171758: {len(chunks2_by_uri)}")
    
    # Compare chunks per URI
    all_uris = set(chunks1_by_uri.keys()) | set(chunks2_by_uri.keys())
    
    print(f"\nChunks per source URL:")
    differences = []
    for uri in sorted(all_uris):
        count1 = len(chunks1_by_uri.get(uri, []))
        count2 = len(chunks2_by_uri.get(uri, []))
        
        domain = uri.split('/')[2] if uri.startswith('http') else uri[:50]
        
        if count1 != count2:
            print(f"  ⚠️  {domain}")
            print(f"      sess_e0fc67c87f0lc: {count1} chunks")
            print(f"      external_test: {count2} chunks")
            differences.append((uri, count1, count2))
        else:
            print(f"  ✅ {domain}: {count1} chunks (identical)")
    
    return differences

def compare_qa_pairs(sess1_path, sess2_path):
    """Compare Q&A pairs from two sessions."""
    print("\n" + "="*80)
    print("❓ COMPARING Q&A PAIRS")
    print("="*80)
    
    qa1 = load_jsonl(sess1_path)
    qa2 = load_jsonl(sess2_path)
    
    print(f"\nTotal Q&A pairs:")
    print(f"  sess_e0fc67c87f0lc: {len(qa1)}")
    print(f"  external_test_20251218_171758: {len(qa2)}")
    print(f"  Difference: {abs(len(qa1) - len(qa2))}")
    
    # Group by source URL (from qa_metadata.source_url)
    qa1_by_url = defaultdict(list)
    qa2_by_url = defaultdict(list)
    
    for qa in qa1:
        url = qa.get('source_url', 'unknown')
        qa1_by_url[url].append(qa)
    
    for qa in qa2:
        url = qa.get('source_url', 'unknown')
        qa2_by_url[url].append(qa)
    
    print(f"\nUnique source URLs:")
    print(f"  sess_e0fc67c87f0lc: {len(qa1_by_url)}")
    print(f"  external_test_20251218_171758: {len(qa2_by_url)}")
    
    # Compare Q&A per URL
    all_urls = set(qa1_by_url.keys()) | set(qa2_by_url.keys())
    
    print(f"\nQ&A pairs per URL:")
    differences = []
    for url in sorted(all_urls):
        count1 = len(qa1_by_url.get(url, []))
        count2 = len(qa2_by_url.get(url, []))
        
        domain = url.split('/')[2] if url.startswith('http') else url[:50]
        
        if count1 != count2:
            print(f"  ⚠️  {domain}")
            print(f"      sess_e0fc67c87f0lc: {count1} Q&A pairs")
            print(f"      external_test: {count2} Q&A pairs")
            differences.append((url, count1, count2))
        else:
            print(f"  ✅ {domain}: {count1} Q&A pairs (identical)")
    
    return differences

def main():
    """Main comparison."""
    base_dir = "/tmp/session_compare"
    
    sess1_chunks = f"{base_dir}/sess_e0fc67c87f0lc/chunks_aggregated.jsonl"
    sess2_chunks = f"{base_dir}/external_test_20251218_171758/chunks_aggregated.jsonl"
    
    sess1_qa = f"{base_dir}/sess_e0fc67c87f0lc/qa_pairs_aggregated.jsonl"
    sess2_qa = f"{base_dir}/external_test_20251218_171758/qa_pairs_aggregated.jsonl"
    
    print("\n" + "="*80)
    print("🔍 SESSION COMPARISON")
    print("="*80)
    print(f"\nSession 1: sess_e0fc67c87f0lc (local test)")
    print(f"Session 2: external_test_20251218_171758 (external test)")
    
    # Compare chunks
    chunk_diffs = compare_chunks(sess1_chunks, sess2_chunks)
    
    # Compare Q&A pairs
    qa_diffs = compare_qa_pairs(sess1_qa, sess2_qa)
    
    # Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    print(f"\nChunk differences: {len(chunk_diffs)}")
    print(f"Q&A differences: {len(qa_diffs) if len(qa_diffs) < 10 else 'Many (10+ shown)'}")
    
    if not chunk_diffs and not qa_diffs:
        print("\n✅ Sessions are identical!")
    else:
        print("\n⚠️  Sessions have differences - likely due to:")
        print("  - Different processing times")
        print("  - Non-deterministic LLM outputs")
        print("  - Concurrent processing variations")
        print("  - Web content changes between scrapes")

if __name__ == '__main__':
    main()
