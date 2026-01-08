#!/usr/bin/env python3
"""
Test client for Summary Query Service API.

Usage:
    # Health check
    python test_query_service.py --health

    # Readiness check
    python test_query_service.py --ready

    # Search only
    python test_query_service.py --search "Gaza ceasefire" --top-k 5

    # Query with LLM
    python test_query_service.py --query "Gaza ceasefire" --top-k 3 --temperature 0.7
"""

import argparse
import json
from typing import Optional

import requests


def health_check(base_url: str):
    """Check service health."""
    response = requests.get(f"{base_url}/health")
    print("=" * 80)
    print("🏥 HEALTH CHECK")
    print("=" * 80)
    print(json.dumps(response.json(), indent=2))
    print("=" * 80)


def readiness_check(base_url: str):
    """Check service readiness."""
    response = requests.get(f"{base_url}/ready")
    print("=" * 80)
    print("✅ READINESS CHECK")
    print("=" * 80)
    print(json.dumps(response.json(), indent=2))
    print("=" * 80)


def search(base_url: str, query: str, top_k: int = 10, start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Search the index."""
    payload = {
        "query": query,
        "top_k": top_k
    }
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date

    response = requests.post(f"{base_url}/search", json=payload)

    if response.status_code != 200:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
        return

    result = response.json()

    print("=" * 80)
    print("🔍 SEARCH RESULTS")
    print("=" * 80)
    print(f"Request ID: {result['request_id']}")
    print(f"Query: {result['query']}")
    print(f"Total Retrieved: {result['total_retrieved']}")
    print(f"Execution Time: {result['execution_time_ms']:.2f}ms")
    print(f"Index: {result['index_queried']}")
    print("-" * 80)

    for i, doc in enumerate(result['retrieved_docs'], 1):
        print(f"\n{i}. Similarity: {doc['similarity_score']:.4f}")
        print(f"   Document ID: {doc['document_id']}")
        print(f"   Source: {doc['source_url']}")
        print(f"   Date: {doc['publication_date']}")
        print(f"   Summary: {doc['summary_text'][:200]}...")

    print("=" * 80)


def query_llm(base_url: str, query: str, top_k: int = 10, temperature: float = 0.7, max_tokens: int = 4000,
              start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Query index and generate LLM response."""
    payload = {
        "query": query,
        "top_k": top_k,
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date

    response = requests.post(f"{base_url}/query_llm", json=payload)

    if response.status_code != 200:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
        return

    result = response.json()

    print("=" * 80)
    print("🤖 QUERY LLM RESULTS")
    print("=" * 80)
    print(f"Request ID: {result['request_id']}")
    print(f"Query: {result['query']}")
    print(f"Total Retrieved: {result['total_retrieved']}")
    print(f"Execution Time: {result['execution_time_ms']:.2f}ms")
    print(f"Index: {result['index_queried']}")
    print(f"Model: {result['llm_model']}")
    print(f"LLM Params: {json.dumps(result['llm_params'], indent=2)}")
    print("-" * 80)

    print("\n📄 RETRIEVED DOCUMENTS:")
    for i, doc in enumerate(result['retrieved_docs'], 1):
        print(f"\n{i}. Similarity: {doc['similarity_score']:.4f}")
        print(f"   Document ID: {doc['document_id']}")
        print(f"   Source: {doc['source_url']}")
        print(f"   Date: {doc['publication_date']}")

    print("\n" + "-" * 80)
    print("\n✨ LLM RESPONSE:")
    print("-" * 80)
    print(result['llm_response'])
    print("-" * 80)

    if '--show-prompt' in sys.argv:
        print("\n📝 FULL PROMPT:")
        print("-" * 80)
        print(result['prompt'])
        print("-" * 80)

    print("=" * 80)


def main():
    import sys

    parser = argparse.ArgumentParser(description="Test Summary Query Service API")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Service base URL")
    parser.add_argument("--health", action="store_true", help="Run health check")
    parser.add_argument("--ready", action="store_true", help="Run readiness check")
    parser.add_argument("--search", type=str, help="Search query")
    parser.add_argument("--query", type=str, help="Query with LLM")
    parser.add_argument("--top-k", type=int, default=10, help="Number of results")
    parser.add_argument("--temperature", type=float, default=0.7, help="LLM temperature")
    parser.add_argument("--max-tokens", type=int, default=4000, help="Max LLM tokens")
    parser.add_argument("--start-date", type=str, help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--show-prompt", action="store_true", help="Show full prompt (with --query)")

    args = parser.parse_args()

    if args.health:
        health_check(args.base_url)
    elif args.ready:
        readiness_check(args.base_url)
    elif args.search:
        search(args.base_url, args.search, args.top_k, args.start_date, args.end_date)
    elif args.query:
        query_llm(args.base_url, args.query, args.top_k, args.temperature, args.max_tokens, args.start_date, args.end_date)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
