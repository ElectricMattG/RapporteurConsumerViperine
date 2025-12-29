"""
Test the MarkdownChunkProcessor on existing chunks
"""

import json
from src.components.chunkers.markdown_chunk_processor import (
    MarkdownChunkProcessor,
    ChunkMergerConfig
)


def test_processor(input_file: str):
    """Test chunk processor on existing chunked document."""
    
    print("=" * 80)
    print("Testing MarkdownChunkProcessor")
    print("=" * 80)
    
    # Load chunks
    print(f"\n📥 Loading chunks from: {input_file}")
    chunks = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            chunks.append(json.loads(line))
    
    print(f"✅ Loaded {len(chunks)} chunks\n")
    
    # Show sample before
    print("-" * 80)
    print("BEFORE PROCESSING (first 5):")
    print("-" * 80)
    for i, chunk in enumerate(chunks[:5], 1):
        content = chunk.get('content', '')[:80]
        meta = chunk.get('chunk_metadata', {})
        print(f"{i}. Words: {meta.get('word_count', 0):3d} | {content}...")
    
    # Configure processor
    config = ChunkMergerConfig(
        min_words=5,
        max_chunk_words=350,
        filter_standalone_links=True,
        filter_html_comments=True,
        filter_date_only=True,
        filter_youtube_headers=True
    )
    
    # Process
    print("\n🔄 Processing chunks...")
    processor = MarkdownChunkProcessor(config=config)
    processed_chunks = processor.process_chunks(chunks)
    
    print(f"✅ Generated {len(processed_chunks)} chunks (from {len(chunks)})\n")
    
    # Show sample after
    print("-" * 80)
    print("AFTER PROCESSING (first 5):")
    print("-" * 80)
    for i, chunk in enumerate(processed_chunks[:5], 1):
        content = chunk.get('content', '')[:100]
        meta = chunk.get('chunk_metadata', {})
        merged_count = meta.get('merged_from_count', 1)
        print(f"{i}. Words: {meta.get('word_count', 0):3d} | Merged: {merged_count} | {content}...")
    
    # Stats
    print("\n" + "=" * 80)
    print("STATS:")
    print("=" * 80)
    
    original_words = sum(c.get('chunk_metadata', {}).get('word_count', 0) for c in chunks)
    processed_words = sum(c.get('chunk_metadata', {}).get('word_count', 0) for c in processed_chunks)
    
    print(f"\nChunks: {len(chunks)} → {len(processed_chunks)} ({len(chunks) - len(processed_chunks)} removed/merged)")
    print(f"Total words preserved: {processed_words:,} / {original_words:,} ({processed_words/original_words*100:.1f}%)")
    
    # Word count distribution
    word_counts = [c.get('chunk_metadata', {}).get('word_count', 0) for c in processed_chunks]
    if word_counts:
        print(f"\nWord count stats:")
        print(f"  Min: {min(word_counts)}")
        print(f"  Max: {max(word_counts)}")
        print(f"  Average: {sum(word_counts)/len(word_counts):.1f}")
    
    # Merge stats
    merge_counts = [c.get('chunk_metadata', {}).get('merged_from_count', 1) for c in processed_chunks]
    merged_chunks = [c for c in processed_chunks if c.get('chunk_metadata', {}).get('merged_from_count', 1) > 1]
    
    print(f"\nMerging stats:")
    print(f"  Single chunks: {len([c for c in merge_counts if c == 1])}")
    print(f"  Merged chunks: {len(merged_chunks)}")
    if merged_chunks:
        print(f"  Max merged: {max(merge_counts)}")
    
    # Save output
    output_file = input_file.replace('.jsonl', '_processed.jsonl')
    with open(output_file, 'w', encoding='utf-8') as f:
        for chunk in processed_chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
    
    print(f"\n💾 Saved to: {output_file}")
    
    return processed_chunks


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python test_chunk_processor.py <chunked_document.jsonl>")
        print("\nExample:")
        print("  python test_chunk_processor.py cache/web_scrapes/req_20251212_175343_010/chunked_document.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    test_processor(input_file)
