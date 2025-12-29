#!/usr/bin/env python3
"""
Test script for LLM content validation.
Tests the Gemini client directly with sample noisy content to debug JSON parsing issues.
"""

import asyncio
import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from clients.llm_gcp_client import GeminiLLMClient


# Sample problematic content from actual chunks
SAMPLE_CHUNKS = [
    {
        "chunk_id": "chunk_test_001",
        "content": "- Terms Of Service [3] - Privacy Policy [4] - JPost Jobs [5] - Cancel Subscription [6] - Sitemap [7] - פרסום בג'רוזלם פוסט [8] - Customer Service [9] - Advertise with Us [10]"
    },
    {
        "chunk_id": "chunk_test_002", 
        "content": "- World News [11] - Iran News [12] - MDA Heroes [13] - The Jerusalem Report [14] - Jerusalem Post Lite [15] - Precious Metals [16] - Astrology [17] - Consumerism [18] - JPost Newsletter [19] - Ivrit Talk [20] - RSS feed [21] - Digital Library [22]"
    },
    {
        "chunk_id": "chunk_test_003",
        "content": "list of 4 items - list 1 of 4 Netanyahu is drawing the US into war with Iran [1] - list 3 of 4 Can Netanyahu survive Hamas's attack on Israel? [2] end of list"
    },
    {
        "chunk_id": "chunk_test_004",
        "content": "Former prime minister Naftali Bennett speaks at the annual Cyber Week, at Tel Aviv University, December 9, 2025 <!-- image -->"
    },
    {
        "chunk_id": "chunk_test_005",
        "content": "Prime Minister Benjamin Netanyahu has told Israelis to prepare for a 'long and hard' offensive as Israeli troops and armoured vehicles push into Gaza in an expansion of ground operations."
    }
]


def build_validation_prompt(chunks_data):
    """Build the validation prompt for LLM."""
    prompt = f"""Classify these content chunks as VALID (substantive news/legal content), MIXED (has content but also UI noise), or INVALID (pure navigation/UI).

INVALID patterns:
- Multiple image comments
- Lists with "list of X items"
- 5+ numbered citation links like [1] [2] [3]
- UI prompts: "Already a member", "Sign in", "Subscribe"
- Repeated logos/branding

For MIXED chunks: Extract ONLY the substantive content, removing UI elements, image tags, and navigation noise.
For VALID chunks: Set extracted_content to null.
For INVALID chunks: Set extracted_content to null.

Respond with ONLY a JSON array, no markdown:
[{{"chunk_id": "id", "classification": "valid|mixed|invalid", "extracted_content": "clean text or null", "reason": "brief reason"}}]

Chunks:
"""
    for i, chunk_data in enumerate(chunks_data):
        prompt += f"\n{i+1}. ID: {chunk_data['chunk_id']}\nContent: {chunk_data['content'][:800]}\n"
    
    return prompt


async def test_llm_validation():
    """Test LLM validation with sample content."""
    print("=" * 80)
    print("Testing LLM Content Validation")
    print("=" * 80)
    print()
    
    # Initialize LLM client
    print("1. Initializing Gemini LLM client...")
    try:
        llm_client = GeminiLLMClient()
        print("   ✓ Client initialized successfully")
    except Exception as e:
        print(f"   ✗ Failed to initialize client: {e}")
        return
    print()
    
    # Build prompt
    print("2. Building validation prompt...")
    prompt = build_validation_prompt(SAMPLE_CHUNKS)
    print(f"   Prompt length: {len(prompt)} characters")
    print(f"   Testing with {len(SAMPLE_CHUNKS)} sample chunks")
    print()
    print("   Sample chunks:")
    for chunk in SAMPLE_CHUNKS:
        print(f"   - {chunk['chunk_id']}: {chunk['content'][:80]}...")
    print()
    
    # Call LLM with JSON mode
    print("3. Calling Gemini with JSON mode...")
    print("   Parameters:")
    print("   - temperature: 0.0")
    print("   - max_tokens: 3000")
    print("   - response_mime_type: application/json")
    print()
    
    try:
        response = await llm_client.generate_content(
            prompt=prompt,
            temperature=0.0,
            max_tokens=3000,
            response_mime_type='application/json'
        )
        print("   ✓ LLM call succeeded")
        print()
    except Exception as e:
        print(f"   ✗ LLM call failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Show raw response
    print("4. Raw LLM Response:")
    print("-" * 80)
    print(response)
    print("-" * 80)
    print()
    
    # Try to parse as JSON
    print("5. Parsing JSON...")
    try:
        results = json.loads(response)
        print("   ✓ JSON parsing succeeded!")
        print()
        
        # Validate structure
        print("6. Validating response structure...")
        if not isinstance(results, list):
            print(f"   ✗ Response is not a list: {type(results)}")
            return
        
        print(f"   ✓ Response is a list with {len(results)} items")
        print()
        
        # Show results with content
        print("7. Validation Results:")
        print("=" * 80)
        
        # Match results to original chunks
        chunks_by_id = {c['chunk_id']: c for c in SAMPLE_CHUNKS}
        
        for i, result in enumerate(results, 1):
            chunk_id = result.get('chunk_id', 'N/A')
            classification = result.get('classification', 'N/A')
            reason = result.get('reason', 'N/A')
            extracted = result.get('extracted_content')
            
            # Get original content
            original_chunk = chunks_by_id.get(chunk_id, {})
            content = original_chunk.get('content', 'N/A')
            
            # Format classification with color indicator
            status_icon = {
                'valid': '✓ VALID',
                'invalid': '✗ INVALID',
                'mixed': '⚠ MIXED'
            }.get(classification.lower(), f'? {classification.upper()}')
            
            print(f"\n{'─' * 80}")
            print(f"Chunk {i}: {chunk_id}")
            print(f"{'─' * 80}")
            print(f"Status: {status_icon}")
            print(f"\nOriginal Content:")
            print(f"  {content}")
            print(f"\nLLM Classification: {classification}")
            print(f"Reason: {reason}")
            if extracted:
                print(f"\nExtracted Content:")
                print(f"  {extracted}")
        
        print()
        print("=" * 80)
        print()
        
        # Summary
        classifications = [r.get('classification', 'unknown') for r in results]
        print("8. Summary:")
        print(f"   Valid: {classifications.count('valid')}")
        print(f"   Mixed: {classifications.count('mixed')}")
        print(f"   Invalid: {classifications.count('invalid')}")
        print(f"   Unknown: {classifications.count('unknown')}")
        print()
        
    except json.JSONDecodeError as e:
        print(f"   ✗ JSON parsing failed: {e}")
        print(f"   Error at position: {e.pos}")
        print(f"   Error message: {e.msg}")
        print()
        print("   Attempting to show problematic area...")
        try:
            start = max(0, e.pos - 100)
            end = min(len(response), e.pos + 100)
            print(f"   Context around position {e.pos}:")
            print(f"   ...{response[start:end]}...")
        except:
            pass
        print()
    except Exception as e:
        print(f"   ✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        print()
    
    print("=" * 80)
    print("Test complete!")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(test_llm_validation())
