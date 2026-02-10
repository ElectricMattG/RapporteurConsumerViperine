#!/usr/bin/env python3
"""Test the embedding service connectivity and response."""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from clients.embedding_client import EmbeddingServiceClient


async def test_embedding_service():
    """Test embedding service with a simple text."""

    print("=" * 80)
    print("🧪 Testing Embedding Service")
    print("=" * 80)

    # Test text
    test_texts = [
        "This is a test sentence for embedding.",
        "The Israeli Defense Forces conducted operations in Gaza.",
        "Hamas and Israel agreed to a temporary ceasefire."
    ]

    print(f"\n📝 Test texts ({len(test_texts)} items):")
    for i, text in enumerate(test_texts, 1):
        print(f"   {i}. {text[:60]}...")

    try:
        print("\n🔌 Initializing EmbeddingServiceClient...")
        async with EmbeddingServiceClient() as client:
            print(f"   ✅ Connected to: {client.service_url}")
            print(f"   ⏱️  Timeout: {client.timeout}s")
            print(f"   🔄 Max retries: {client.max_retries}")

            print("\n📊 Sending embedding request...")
            result = await client.embed(test_texts)

            print(f"\n✅ SUCCESS - Received embeddings")
            print(f"   📦 Result type: {type(result)}")
            print(f"   📦 Result shape: {result.shape if hasattr(result, 'shape') else 'N/A'}")

            # Handle numpy array response
            if hasattr(result, 'shape'):
                num_embeddings, embedding_dim = result.shape
                print(f"   📦 Number of embeddings: {num_embeddings}")
                print(f"   📏 Embedding dimension: {embedding_dim}")

                # Show first embedding sample
                print(f"\n🔍 First embedding sample:")
                print(f"   First 10 values: {result[0][:10].tolist()}")
                print(f"   Norm: {float((result[0]**2).sum()**0.5):.4f}")
            else:
                print(f"   Result: {result}")

            print("\n" + "=" * 80)
            print("✅ ALL TESTS PASSED")
            print("=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print(f"   Type: {type(e).__name__}")
        import traceback
        print("\nFull traceback:")
        traceback.print_exc()
        print("\n" + "=" * 80)
        print("❌ TEST FAILED")
        print("=" * 80)
        return False

    return True


if __name__ == "__main__":
    success = asyncio.run(test_embedding_service())
    sys.exit(0 if success else 1)
