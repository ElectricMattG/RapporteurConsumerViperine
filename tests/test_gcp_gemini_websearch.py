#!/usr/bin/env python3
"""
Test GCP Gemini API with Web Search Grounding

Test Vertex AI Gemini with web search grounding to get URLs and citations.
"""
import asyncio
import json
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

from clients.llm_gcp_client import GeminiLLMClient


async def test_gemini_websearch():
    """Test Gemini API with web search grounding"""
    print("=" * 80)
    print("🧪 Testing GCP Gemini API with Web Search Grounding")
    print("=" * 80)

    try:
        # Initialize client
        print("\n📋 Step 1: Initializing Gemini client...")
        client = GeminiLLMClient()
        print("✅ Client initialized successfully")
        print(f"   Project: {client.project_id}")
        print(f"   Location: {client.location}")
        print(f"   Model: {client.model_name}")

        # Test web search grounding with a current events query
        print("\n📋 Step 2: Testing web search grounding...")
        test_query = "What are the latest developments in US-Venezuela relations?"

        print(f"   Query: {test_query}")
        print("   Sending request with web search grounding...")

        # Call the API with grounding enabled
        response = await client.generate_content(
            prompt=test_query,
            temperature=0.7,
            max_tokens=2000,
            enable_search=True,
            return_grounding_metadata=True
        )

        print(f"\n✅ Response received:")
        print(f"\n{'-' * 80}")
        print("RESPONSE TEXT:")
        print(f"{'-' * 80}")
        print(response.get('text', '')[:500])
        print(f"{'-' * 80}\n")

        # Display grounding metadata
        if 'grounding_metadata' in response:
            metadata = response['grounding_metadata']

            print(f"\n📚 GROUNDING METADATA:")
            print(f"{'-' * 80}")

            # Display citations
            if 'citations' in metadata and metadata['citations']:
                print(f"\n✅ Citations found: {len(metadata['citations'])}")
                for i, citation in enumerate(metadata['citations'][:5], 1):
                    print(f"\n   Citation {i}:")
                    if 'uri' in citation:
                        print(f"      URL: {citation['uri']}")
                    if 'title' in citation:
                        print(f"      Title: {citation['title']}")
                    if 'start_index' in citation:
                        print(f"      Text range: {citation.get('start_index')} - {citation.get('end_index')}")
            else:
                print("   No citations found")

            # Display grounding supports
            if 'grounding_supports' in metadata and metadata['grounding_supports']:
                print(f"\n✅ Grounding supports found: {len(metadata['grounding_supports'])}")
                for i, support in enumerate(metadata['grounding_supports'][:3], 1):
                    print(f"\n   Support {i}:")
                    if 'segment' in support:
                        segment = support['segment']
                        print(f"      Text: {segment.get('text', '')[:100]}...")
                    if 'grounding_chunk_indices' in support:
                        print(f"      Chunk indices: {support['grounding_chunk_indices']}")
                    if 'confidence_scores' in support:
                        print(f"      Confidence: {support['confidence_scores']}")
            else:
                print("   No grounding supports found")

            # Display search queries used
            if 'search_entry_point' in metadata:
                print(f"\n✅ Search entry point:")
                print(f"   {metadata['search_entry_point']}")

            print(f"\n{'-' * 80}")
        else:
            print("\n⚠️  No grounding metadata in response")

        # Save full response to file
        output_file = Path(__file__).parent.parent / "research_results" / "test_websearch_output.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            json.dump(response, f, indent=2)

        print(f"\n💾 Full response saved to: {output_file}")

        print("\n" + "=" * 80)
        print("✅ WEB SEARCH TEST PASSED - Citations and URLs retrieved")
        print("=" * 80)

        return True

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print(f"   Type: {type(e).__name__}")

        # Check for specific error types
        error_str = str(e)
        if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
            print("\n⚠️  Rate Limit Error (429 RESOURCE_EXHAUSTED)")
            print("   The API is rate limiting requests.")
        elif "403" in error_str or "PERMISSION_DENIED" in error_str:
            print("\n⚠️  Permission Error (403)")
            print("   Check project permissions and API enablement.")
        elif "401" in error_str or "UNAUTHENTICATED" in error_str:
            print("\n⚠️  Authentication Error (401)")
            print("   Check credentials and service account setup.")

        print("\n" + "=" * 80)
        print("❌ TEST FAILED - Web search grounding not working")
        print("=" * 80)

        import traceback
        traceback.print_exc()

        return False


if __name__ == "__main__":
    result = asyncio.run(test_gemini_websearch())
    sys.exit(0 if result else 1)
