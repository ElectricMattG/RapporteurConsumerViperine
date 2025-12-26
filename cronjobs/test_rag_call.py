#!/usr/bin/env python3
"""Test RAG API call with cronjob configuration"""
import json
import os
import sys

import requests
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load config from environment (simulating what the cronjob does)
RAG_API_URL = os.getenv("RAG_API_URL", "https://rag.rapporteur.ai/query")
RAG_API_KEY = os.getenv("RAG_API_KEY", "32d160f623efa11ebd88d34ca0b9f4093eb1f866a640d4b70b0fbafb6c22e398")

print("=" * 80)
print("Testing RAG API Call")
print("=" * 80)
print(f"URL: {RAG_API_URL}")
print(f"API Key: {RAG_API_KEY[:20]}..." if len(RAG_API_KEY) > 20 else RAG_API_KEY)
print("=" * 80)

# Test query
test_query = "Get the latest news from the Israeli Ministry of Defense from 2023-09-01 to 2023-09-15"

headers = {
    "Content-Type": "application/json",
    "Host": "rag.rapporteur.ai",
    "X-API-Key": RAG_API_KEY
}

payload = {
    "query": test_query,
    "processing_mode": "research",
    "enable_query_refinement": False,
    "top_k": 3
}

print(f"\nQuery: {test_query}")
print(f"Processing mode: research")
print(f"Top K: 3\n")

try:
    response = requests.post(
        RAG_API_URL,
        headers=headers,
        json=payload,
        timeout=180,
        verify=False
    )

    print(f"Status Code: {response.status_code}")

    if response.status_code == 200:
        result = response.json()
        print("\n✅ Success!")
        print(f"Session ID: {result.get('session_id', 'N/A')}")
        print(f"Request ID: {result.get('request_id', 'N/A')}")

        grounding = result.get('grounding_metadata', {})
        citations = grounding.get('citations', [])
        print(f"Citations found: {len(citations)}")

        if citations:
            print("\nFirst 3 citations:")
            for i, citation in enumerate(citations[:3], 1):
                print(f"  {i}. {citation.get('title', 'No title')}")
                print(f"     URL: {citation.get('uri', 'No URL')}")

        # Show full response (truncated)
        response_str = json.dumps(result, indent=2)
        if len(response_str) > 1000:
            print(f"\nFull response (first 1000 chars):\n{response_str[:1000]}...")
        else:
            print(f"\nFull response:\n{response_str}")
    else:
        print(f"\n❌ Error: HTTP {response.status_code}")
        print(f"Response: {response.text}")

except requests.exceptions.RequestException as e:
    print(f"\n❌ Request failed: {e}")
    sys.exit(1)
