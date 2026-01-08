#!/usr/bin/env python3
"""
Test GCP Gemini API Connectivity

Quick test to verify if Vertex AI Gemini is accepting requests.
"""
import asyncio
import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src'))

from clients.llm_gcp_client import GeminiLLMClient


async def test_gemini_connection():
    """Test basic Gemini API connectivity"""
    print("=" * 80)
    print("🧪 Testing GCP Gemini API Connectivity")
    print("=" * 80)

    try:
        # Initialize client
        print("\n📋 Step 1: Initializing Gemini client...")
        client = GeminiLLMClient()
        print("✅ Client initialized successfully")
        print(f"   Project: {client.project_id}")
        print(f"   Location: {client.location}")
        print(f"   Model: {client.model_name}")

        # Test simple generation
        print("\n📋 Step 2: Testing simple text generation...")
        test_prompt = "Generate a brief greeting message (one sentence only)."

        print(f"   Prompt: {test_prompt}")
        print("   Sending request...")

        response = await client.generate_content(
            prompt=test_prompt,
            temperature=0.7,
            max_tokens=50
        )

        print(f"✅ Response received:")
        print(f"   {response[:200]}")

        # Test Q&A format (similar to what qa_generator does)
        print("\n📋 Step 3: Testing Q&A generation format...")
        qa_prompt = """Based on this text, generate ONE question-answer pair in JSON format:

Text: "The capital of France is Paris. It is known for the Eiffel Tower."

Generate a JSON object with 'question' and 'answer' fields."""

        print("   Sending Q&A format request...")
        qa_response = await client.generate_content(
            prompt=qa_prompt,
            temperature=0.7,
            max_tokens=150
        )

        print(f"✅ Q&A Response received:")
        print(f"   {qa_response[:300]}")

        print("\n" + "=" * 80)
        print("✅ ALL TESTS PASSED - GCP Gemini API is accepting requests")
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
            print("   This is the same error seen in production.")
        elif "403" in error_str or "PERMISSION_DENIED" in error_str:
            print("\n⚠️  Permission Error (403)")
            print("   Check project permissions and API enablement.")
        elif "401" in error_str or "UNAUTHENTICATED" in error_str:
            print("\n⚠️  Authentication Error (401)")
            print("   Check credentials and service account setup.")

        print("\n" + "=" * 80)
        print("❌ TEST FAILED - API is not accepting requests")
        print("=" * 80)

        import traceback
        traceback.print_exc()

        return False


if __name__ == "__main__":
    result = asyncio.run(test_gemini_connection())
    sys.exit(0 if result else 1)
