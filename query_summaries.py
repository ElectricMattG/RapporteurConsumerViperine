#!/usr/bin/env python3
"""Query summaries index using vector similarity and generate overview with Gemini."""

import asyncio
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.clients.embedding_client import EmbeddingServiceClient
from src.clients.llm_gcp_client import GeminiLLMClient


def extract_qa_from_content(content: str) -> Dict[str, str]:
    """Extract question and answer from content field using regex.

    Args:
        content: Content field containing QUESTION, ANSWER, and CONTEXT

    Returns:
        Dict with 'question' and 'answer' fields
    """
    # Pattern to match QUESTION and ANSWER, ignoring CONTEXT
    question_pattern = r'QUESTION:\s*(.+?)(?=\n\n(?:ANSWER|CONTEXT)|$)'
    answer_pattern = r'ANSWER:\s*(.+?)(?=\n\n(?:CONTEXT)|$)'

    question_match = re.search(question_pattern, content, re.DOTALL)
    answer_match = re.search(answer_pattern, content, re.DOTALL)

    return {
        'question': question_match.group(1).strip() if question_match else '',
        'answer': answer_match.group(1).strip() if answer_match else ''
    }

def load_summaries_index(index_path: str) -> List[Dict]:
    """Load summaries from combined index."""
    summaries = []
    with open(index_path, 'r') as f:
        for line in f:
            if line.strip():
                summaries.append(json.loads(line))
    return summaries


def dot_product_similarity(query_embedding: List[float], doc_embedding: List[float]) -> float:
    """Calculate dot product similarity between query and document embeddings."""
    result = np.dot(query_embedding, doc_embedding)
    # Convert to scalar if it's an array
    return float(result) if isinstance(result, np.ndarray) else result


def filter_by_date(summaries: List[Dict], start_date: Optional[str] = None, end_date: Optional[str] = None, mode: str = 'summaries') -> List[Dict]:
    """Filter summaries by date range using doc_metadata.publication_date or document_metadata.

    Args:
        summaries: List of summary documents
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        mode: 'summaries' or 'qa' to determine which metadata field to use
    """
    if not start_date and not end_date:
        return summaries

    filtered = []
    for summary in summaries:
        # Get publication_date based on mode
        if mode == 'qa':
            doc_metadata = summary.get('document_metadata', {})
        else:
            doc_metadata = summary.get('doc_metadata', {})

        pub_date_str = doc_metadata.get('publication_date')

        if not pub_date_str:
            continue

        try:
            # Parse publication_date (format: YYYY-MM-DD)
            doc_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
            start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
            end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None

            if (not start or doc_date >= start) and (not end or doc_date <= end):
                filtered.append(summary)
        except (ValueError, AttributeError):
            continue

    return filtered


def query_summaries(
    query: str,
    index_path: str,
    top_k: int = 10,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    post_filter_top_k: Optional[int] = None,
    mode: str = 'summaries'
) -> List[Dict]:
    """Query summaries or Q&A pairs using vector similarity.

    Args:
        query: Natural language query
        index_path: Path to combined summaries index
        top_k: Number of top results to return from similarity search
        start_date: Filter start date (YYYY-MM-DD)
        end_date: Filter end date (YYYY-MM-DD)
        post_filter_top_k: If specified, filter top-k results by date and limit to this number
    """
    print(f"📚 Loading summaries from: {index_path}")
    summaries = load_summaries_index(index_path)
    print(f"   Loaded {len(summaries)} summaries")

    # Filter by date if specified and NOT using post-filter
    if (start_date or end_date) and not post_filter_top_k:
        summaries = filter_by_date(summaries, start_date, end_date, mode)
        print(f"   Filtered to {len(summaries)} summaries (date range: {start_date or 'any'} to {end_date or 'any'})")

    # Generate query embedding
    print(f"\n🔍 Generating embedding for query: '{query}'")
    embedding_client = EmbeddingServiceClient()
    query_embedding = asyncio.run(embedding_client.embed(query))

    # Flatten if it's a batch (2D array)
    if isinstance(query_embedding, np.ndarray) and query_embedding.ndim > 1:
        query_embedding = query_embedding[0]

    # Calculate similarities
    print("   Calculating similarities...")
    # Choose embedding field based on mode
    embedding_field = 'qa_embedding' if mode == 'qa' else 'summary_embedding'

    results = []
    for summary in summaries:
        if embedding_field not in summary:
            continue

        similarity = dot_product_similarity(query_embedding, summary[embedding_field])
        results.append({
            'summary': summary,
            'similarity': similarity
        })

    # Sort by similarity and get top k
    results.sort(key=lambda x: x['similarity'], reverse=True)
    top_results = results[:top_k]

    # Apply post-filtering if requested
    if post_filter_top_k:
        print(f"   Found {len(results)} results, filtering top {top_k} by date...")
        # Filter the top results by date using doc_metadata.publication_date or document_metadata
        filtered_results = []
        for result in top_results:
            summary = result['summary']
            # Handle both summary and Q&A metadata structures
            if mode == 'qa':
                doc_metadata = summary.get('document_metadata', {})
            else:
                doc_metadata = summary.get('doc_metadata', {})
            pub_date_str = doc_metadata.get('publication_date')

            if pub_date_str:
                try:
                    doc_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
                    start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
                    end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None

                    if (not start or doc_date >= start) and (not end or doc_date <= end):
                        filtered_results.append(result)
                except (ValueError, AttributeError):
                    pass

        # Limit to post_filter_top_k
        top_results = filtered_results[:post_filter_top_k]
        print(f"   After date filtering: {len(top_results)} results")
    else:
        print(f"   Found {len(results)} results, returning top {top_k}")

    return top_results


async def generate_overview(query: str, top_summaries: List[Dict], mode: str = 'summaries') -> str:
    """Generate overview of summaries or Q&A pairs using Gemini.

    Args:
        query: Original query
        top_summaries: List of top k summaries/qa pairs with similarity scores
        mode: 'summaries' or 'qa'
    """
    # Format summaries for context
    summaries_text = []
    for i, result in enumerate(top_summaries, 1):
        summary = result['summary']
        score = result['similarity']

        doc_id = summary.get('document_id', 'unknown')
        source_url = summary.get('source_url', 'N/A')
        # Handle both metadata structures
        if mode == 'qa':
            doc_metadata = summary.get('document_metadata', {})
            chunk_metadata = summary.get('chunk_metadata', {})
            doc_id = chunk_metadata.get('chunk_id', 'unknown')
            source_url = chunk_metadata.get('source_url', 'N/A')
        else:
            doc_metadata = summary.get('doc_metadata', {})
        pub_date = doc_metadata.get('publication_date', 'N/A')

        if mode == 'qa':
            # Extract Q&A from content field
            content = summary.get('content', '')
            qa_data = extract_qa_from_content(content)
            summaries_text.append(
                f"Q&A Pair {i} (similarity: {score:.4f}):\n"
                f"Document ID: {doc_id}\n"
                f"Publication Date: {pub_date}\n"
                f"Source: {source_url}\n"
                f"QUESTION: {qa_data['question']}\n"
                f"ANSWER: {qa_data['answer']}\n"
            )
        else:
            # Regular summary mode
            title = summary.get('title', 'N/A')
            summary_text = summary.get('summary_text', '')
            summaries_text.append(
                f"Summary {i} (similarity: {score:.4f}):\n"
                f"Document ID: {doc_id}\n"
                f"Publication Date: {pub_date}\n"
                f"Title: {title}\n"
                f"Source: {source_url}\n"
                f"Content: {summary_text}\n"
            )

    context = "\n---\n".join(summaries_text)

    # Print summaries being sent
    print("\n" + "="*80)
    print("📋 SUMMARIES BEING SENT TO GEMINI:")
    print("="*80)
    for summary_text in summaries_text:
        print(summary_text)
        print("-"*80)

    prompt = f"""Given these summaries, provide a comprehensive overview that synthesizes the key themes, findings, and insights. When you cite eacg summary use the: publihser/author and a stub of the url (Al-Jazeer, Nov 2023, www.al-jazeera)

Original Query: {query}

Top {len(top_summaries)} Summaries:

{context}

Please provide:
1. A high-level overview of the main themes across these summaries
2. Key findings or insights that emerge
3. Any patterns or connections between the summaries
4. Notable differences or contrasting viewpoints if any

Overview:"""

    # Print the full prompt
    print("\n" + "="*80)
    print("📝 FULL PROMPT SENT TO GEMINI:")
    print("="*80)
    print(prompt)
    print("="*80 + "\n")

    print("🤖 Generating overview with Gemini...")
    llm_client = GeminiLLMClient()
    response = await llm_client.generate_content(prompt, max_tokens=4000)

    print("\n" + "="*80)
    print("✨ GEMINI RESPONSE:")
    print("="*80)
    print(response)
    print("="*80 + "\n")

    return response


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Query summaries index and generate overview")
    parser.add_argument("query", type=str, help="Natural language query")
    parser.add_argument(
        "--index",
        type=str,
        default="/private/var/folders/7n/g0ktgtm92rd5l7thxj8zqhz80000gn/T/17675183546430/research_summaries_with_embeddings_combined.jsonl",
        help="Path to combined summaries index"
    )
    parser.add_argument("--top-k", type=int, default=10, help="Number of top results to return from similarity search")
    parser.add_argument("--start-date", type=str, help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--post-filter-top-k", type=int, help="If specified, filter top-k results by date and limit to this number")
    parser.add_argument("--show-summaries", action="store_true", help="Show individual summaries before overview")
    parser.add_argument("--mode", type=str, default="summaries", choices=["summaries", "qa"], help="Query mode: summaries or qa pairs")
    args = parser.parse_args()

    # Query summaries
    top_results = query_summaries(
        query=args.query,
        index_path=args.index,
        top_k=args.top_k,
        start_date=args.start_date,
        end_date=args.end_date,
        post_filter_top_k=args.post_filter_top_k,
        mode=args.mode
    )

    if not top_results:
        print("\n⚠️  No results found!")
        return

    # Show individual summaries if requested
    if args.show_summaries:
        print("\n" + "=" * 80)
        print("TOP SUMMARIES")
        print("=" * 80)
        for i, result in enumerate(top_results, 1):
            summary = result['summary']
            score = result['similarity']
            doc_metadata = summary.get('doc_metadata', {})
            pub_date = doc_metadata.get('publication_date', 'N/A')
            print(f"\n{i}. Similarity: {score:.4f}")
            print(f"   Document ID: {summary.get('document_id', 'unknown')}")
            print(f"   Publication Date: {pub_date}")
            print(f"   Source: {summary.get('source_url', 'N/A')}")
    else:
        print(f"\n📊 Selected {len(top_results)} summaries for overview generation")

    # Generate overview
    overview = asyncio.run(generate_overview(args.query, top_results, args.mode))

    print("\n" + "=" * 80)
    print("OVERVIEW")
    print("=" * 80)
    print(overview)
    print("\n" + "=" * 80)
    print(f"Based on {len(top_results)} most relevant summaries")
    print("=" * 80)


if __name__ == "__main__":
    main()
