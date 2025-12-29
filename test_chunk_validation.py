#!/usr/bin/env python3
"""
Test script for LLM-based chunk content validation
"""
import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from components.chunkers.markdown_chunk_processor import MarkdownChunkProcessor, ChunkMergerConfig
from clients.llm_gcp_client import GeminiLLMClient


async def main():
    """Test chunk validation with sample content"""
    
    # Initialize Gemini client
    print("🔧 Initializing Gemini client...")
    llm_client = GeminiLLMClient()
    
    # Initialize chunk processor with LLM validation
    config = ChunkMergerConfig(min_words=3, max_chunk_words=350)
    processor = MarkdownChunkProcessor(
        config=config,
        llm_client=llm_client,
        enable_llm_validation=True
    )
    
    # Test chunks - mix of valid, mixed, and invalid content
    test_chunks = [
        {
            "content": "The democracy index declined by 3 points in 2023, marking the worst performance since the organization began tracking. Experts cite rising authoritarianism and declining press freedom as key factors.",
            "document_metadata": {"url": "https://example.com/democracy-report"},
            "chunk_metadata": {
                "chunk_id": "chunk_valid_1",
                "chunk_index": 0,
                "word_count": 35,
                "char_count": 200
            }
        },
        {
            "content": "- Democracy Tracker [1] - Country Profiles [2] - All Event Reports [3] - GSoD Indices [4] - Democracy Notes [5] Asia and the Pacific [6] View Regional page [7]",
            "document_metadata": {"url": "https://example.com/navigation"},
            "chunk_metadata": {
                "chunk_id": "chunk_invalid_1",
                "chunk_index": 1,
                "word_count": 30,
                "char_count": 150
            }
        },
        {
            "content": "The Syrian conflict has displaced millions of people. - Terms and Conditions [49] - Privacy Policy [50] - Contact Us [51]",
            "document_metadata": {"url": "https://example.com/news"},
            "chunk_metadata": {
                "chunk_id": "chunk_mixed_1",
                "chunk_index": 2,
                "word_count": 20,
                "char_count": 120
            }
        },
        {
            "content": "The new legislation aims to strengthen human rights protections and establish independent oversight mechanisms for law enforcement agencies.",
            "document_metadata": {"url": "https://example.com/legal-reform"},
            "chunk_metadata": {
                "chunk_id": "chunk_valid_2",
                "chunk_index": 3,
                "word_count": 22,
                "char_count": 140
            }
        }
    ]
    
    print(f"\n📝 Testing validation with {len(test_chunks)} chunks:")
    print("-" * 80)
    for i, chunk in enumerate(test_chunks, 1):
        print(f"{i}. [{chunk['chunk_metadata']['chunk_id']}] {chunk['content'][:80]}...")
    print("-" * 80)
    
    # Process chunks
    print("\n🔍 Running LLM validation...")
    validated_chunks = await processor.process_chunks(test_chunks, source_url="https://example.com")
    
    # Display results
    print(f"\n✅ Validation complete!")
    print(f"   Original chunks: {len(test_chunks)}")
    print(f"   Validated chunks: {len(validated_chunks)}")
    print(f"   Filtered out: {len(test_chunks) - len(validated_chunks)}")
    
    print("\n📊 Validation Results:")
    print("-" * 80)
    for chunk in validated_chunks:
        chunk_id = chunk['chunk_metadata']['chunk_id']
        status = chunk['chunk_metadata'].get('validation_status', 'unknown')
        reason = chunk['chunk_metadata'].get('validation_reason', 'N/A')
        content_preview = chunk['content'][:100]
        
        status_icon = "✅" if status == "valid" else "🔄" if status == "mixed" else "❓"
        print(f"{status_icon} {chunk_id}")
        print(f"   Status: {status}")
        print(f"   Reason: {reason}")
        print(f"   Content: {content_preview}...")
        print()
    
    print("=" * 80)
    print("✨ Test complete!")


if __name__ == "__main__":
    asyncio.run(main())
