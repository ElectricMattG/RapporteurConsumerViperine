#!/usr/bin/env python3
"""
Check query_id propagation through the pipeline by examining cache files.
Verifies that query_id is present in ALL pipeline stages with NO EXCEPTIONS.
"""

import json
from pathlib import Path
from typing import List, Tuple


def check_extraction_files(cache_dir: Path) -> Tuple[int, int]:
    """Check extracted_linked_text.jsonl files for query_id."""
    print("\n" + "="*80)
    print("1. EXTRACTION FILES (web-scraper output)")
    print("="*80)
    
    extraction_files = list(cache_dir.glob("*/extracted_linked_text.jsonl"))
    
    if not extraction_files:
        print("⚠️  No extraction files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in extraction_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        query_id_root = data.get('query_id')
                        query_id_meta = data.get('metadata', {}).get('query_id')
                        
                        if query_id_root or query_id_meta:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {request_id}/extracted_linked_text.jsonl")
        except Exception as e:
            print(f"   ❌ Error reading {request_id}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} extraction records have query_id")
    return total_checked, total_with_query_id


def check_summary_files(cache_dir: Path) -> Tuple[int, int]:
    """Check summary.jsonl files for query_id."""
    print("\n" + "="*80)
    print("2. SUMMARY FILES (summarizer output)")
    print("="*80)
    
    summary_files = list(cache_dir.glob("*/summary.jsonl"))
    
    if not summary_files:
        print("⚠️  No summary files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in summary_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        query_id = data.get('query_id')
                        
                        if query_id:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {request_id}/summary.jsonl")
        except Exception as e:
            print(f"   ❌ Error reading {request_id}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} summary records have query_id")
    return total_checked, total_with_query_id


def check_chunked_files(cache_dir: Path) -> Tuple[int, int]:
    """Check chunked_document.jsonl files for query_id."""
    print("\n" + "="*80)
    print("3. CHUNKED FILES (chunker output)")
    print("="*80)
    
    chunked_files = list(cache_dir.glob("*/chunked_document.jsonl"))
    
    if not chunked_files:
        print("⚠️  No chunked files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in chunked_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        query_id = data.get('document_metadata', {}).get('query_id')
                        
                        if query_id:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {request_id}/chunked_document.jsonl")
        except Exception as e:
            print(f"   ❌ Error reading {request_id}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} chunk records have query_id")
    return total_checked, total_with_query_id


def check_embedded_files(cache_dir: Path) -> Tuple[int, int]:
    """Check embedded files for query_id."""
    print("\n" + "="*80)
    print("4. EMBEDDED CHUNKS (chunk embedder output)")
    print("="*80)
    
    embedded_files = list(cache_dir.glob("*/chunked_document_enriched_embedded.jsonl"))
    
    if not embedded_files:
        print("⚠️  No embedded files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in embedded_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        query_id = data.get('document_metadata', {}).get('query_id')
                        
                        if query_id:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {request_id}/chunked_document_enriched_embedded.jsonl")
        except Exception as e:
            print(f"   ❌ Error reading {request_id}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} embedded chunk records have query_id")
    return total_checked, total_with_query_id


def check_qa_files(cache_dir: Path) -> Tuple[int, int]:
    """Check Q&A pair files (before embedding) for query_id."""
    print("\n" + "="*80)
    print("5. Q&A PAIRS (qa-generator output)")
    print("="*80)
    
    # Match files like: chunked_document_enriched_embedded_qa_pairs_TIMESTAMP.jsonl
    qa_files = list(cache_dir.glob("*/chunked_document_enriched_embedded_qa_pairs_*.jsonl"))
    
    if not qa_files:
        print("⚠️  No Q&A pair files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in qa_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        # Q&A pairs have flat structure
                        query_id = data.get('query_id')
                        
                        if query_id:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {file_path.name}")
        except Exception as e:
            print(f"   ❌ Error reading {file_path.name}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} Q&A pair records have query_id")
    return total_checked, total_with_query_id


def check_qa_embedded_files(cache_dir: Path) -> Tuple[int, int]:
    """Check enriched embedded Q&A pair files for query_id."""
    print("\n" + "="*80)
    print("6. EMBEDDED Q&A PAIRS (qa-embedder output)")
    print("="*80)
    
    qa_files = list(cache_dir.glob("*/*_qa_pairs_enriched_embedded.jsonl"))
    
    if not qa_files:
        print("⚠️  No embedded Q&A pair files found")
        return 0, 0
    
    total_checked = 0
    total_with_query_id = 0
    
    for file_path in qa_files:
        request_id = file_path.parent.name
        
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if line.strip():
                        total_checked += 1
                        data = json.loads(line.strip())
                        
                        # Q&A pairs have flat structure
                        query_id = data.get('query_id')
                        
                        if query_id:
                            total_with_query_id += 1
                        else:
                            print(f"   ❌ MISSING query_id: {file_path.name}")
        except Exception as e:
            print(f"   ❌ Error reading {file_path.name}: {e}")
    
    status = "✅" if total_with_query_id == total_checked else "❌"
    print(f"{status} {total_with_query_id}/{total_checked} embedded Q&A pair records have query_id")
    return total_checked, total_with_query_id


def main():
    """Main entry point."""
    cache_dir = Path("./cache/web_scrapes")
    
    if not cache_dir.exists():
        print(f"❌ Cache directory not found: {cache_dir}")
        return
    
    print(f"\n🔍 Checking query_id propagation in: {cache_dir.absolute()}")
    print("📋 Verifying ALL files have query_id field - NO EXCEPTIONS ALLOWED")
    
    # Check each stage and track totals
    results = []
    
    results.append(("Extractions", *check_extraction_files(cache_dir)))
    results.append(("Summaries", *check_summary_files(cache_dir)))
    results.append(("Chunks", *check_chunked_files(cache_dir)))
    results.append(("Embedded Chunks", *check_embedded_files(cache_dir)))
    results.append(("Q&A Pairs", *check_qa_files(cache_dir)))
    results.append(("Embedded Q&A Pairs", *check_qa_embedded_files(cache_dir)))
    
    # Summary
    print("\n" + "="*80)
    print("📊 SUMMARY")
    print("="*80)
    
    total_records = 0
    total_with_query_id = 0
    all_passed = True
    
    for stage, checked, with_query_id in results:
        if checked > 0:
            total_records += checked
            total_with_query_id += with_query_id
            percentage = (with_query_id / checked * 100) if checked > 0 else 0
            status = "✅" if with_query_id == checked else "❌"
            all_passed = all_passed and (with_query_id == checked)
            print(f"{status} {stage:20} {with_query_id:4}/{checked:4} ({percentage:6.2f}%)")
    
    print("-" * 80)
    total_percentage = (total_with_query_id / total_records * 100) if total_records > 0 else 0
    print(f"{'✅' if all_passed else '❌'} {'TOTAL':20} {total_with_query_id:4}/{total_records:4} ({total_percentage:6.2f}%)")
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ ALL FILES HAVE query_id - PROPAGATION SUCCESSFUL")
    else:
        print("❌ SOME FILES MISSING query_id - PROPAGATION INCOMPLETE")
    print("="*80)


if __name__ == '__main__':
    main()
