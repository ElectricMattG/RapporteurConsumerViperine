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
        content: Content field containing QUESTION, ANSWER, and SOURCE CONTEXT

    Returns:
        Dict with 'question' and 'answer' fields
    """
    # Pattern to match QUESTION and ANSWER, stopping before SOURCE CONTEXT
    question_pattern = r'QUESTION:\s*(.+?)(?=\n*ANSWER:)'
    answer_pattern = r'ANSWER:\s*(.+?)(?=\n*SOURCE CONTEXT:|\Z)'

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
    mode: str = 'summaries',
    qa_index_path: Optional[str] = None,
    qa_top_k: Optional[int] = None
) -> List[Dict]:
    """Query summaries or Q&A pairs using vector similarity.

    Args:
        query: Natural language query
        index_path: Path to combined summaries index
        top_k: Number of top results to return from similarity search
        start_date: Filter start date (YYYY-MM-DD)
        end_date: Filter end date (YYYY-MM-DD)
        post_filter_top_k: If specified, filter top-k results by date and limit to this number
        mode: 'summaries', 'qa', or 'hybrid'
        qa_index_path: Path to Q&A index (required for hybrid mode)
        qa_top_k: Number of Q&A pairs to retrieve (defaults to top_k in qa mode, top_k//2 in hybrid)
    """
    if mode == 'hybrid':
        # Query both indices and combine results
        print(f"🔀 HYBRID MODE: Querying both summaries and Q&A pairs")
        print(f"📚 Loading summaries from: {index_path}")
        summaries = load_summaries_index(index_path)
        print(f"   Loaded {len(summaries)} summaries")

        if not qa_index_path:
            raise ValueError("qa_index_path is required for hybrid mode")
        print(f"❓ Loading Q&A pairs from: {qa_index_path}")
        qa_pairs = load_summaries_index(qa_index_path)
        print(f"   Loaded {len(qa_pairs)} Q&A pairs")

        # Query summaries
        summary_k = top_k if qa_top_k is None else top_k
        qa_k = qa_top_k if qa_top_k is not None else (top_k // 2 if mode == 'hybrid' else top_k)

        summary_results = _query_single_index(query, summaries, summary_k, start_date, end_date, post_filter_top_k, 'summaries')
        # Query Q&A pairs
        qa_results = _query_single_index(query, qa_pairs, qa_k, start_date, end_date, post_filter_top_k, 'qa')

        # Combine and mark type
        for r in summary_results:
            r['result_type'] = 'summary'
        for r in qa_results:
            r['result_type'] = 'qa'

        return summary_results + qa_results
    else:
        print(f"📚 Loading summaries from: {index_path}")
        summaries = load_summaries_index(index_path)
        print(f"   Loaded {len(summaries)} summaries")
        return _query_single_index(query, summaries, top_k, start_date, end_date, post_filter_top_k, mode)


def _query_single_index(
    query: str,
    summaries: List[Dict],
    top_k: int,
    start_date: Optional[str],
    end_date: Optional[str],
    post_filter_top_k: Optional[int],
    mode: str
) -> List[Dict]:
    """Helper function to query a single index."""

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

    results = []
    for summary in summaries:
        # Get embedding based on mode
        if mode == 'qa':
            # Q&A pairs use nested embedding_enrichment.embedding_vector
            embedding_enrichment = summary.get('embedding_enrichment', {})
            doc_embedding = embedding_enrichment.get('embedding_vector')
        else:
            # Summaries use flat summary_embedding
            doc_embedding = summary.get('summary_embedding')

        if doc_embedding is None:
            continue

        similarity = dot_product_similarity(query_embedding, doc_embedding)
        results.append({
            'summary': summary,
            'similarity': similarity
        })

    # Sort by similarity and get top k
    results.sort(key=lambda x: x['similarity'], reverse=True)
    top_results = results[:top_k]

    # Apply post-filtering if requested
    if post_filter_top_k:
        # Only filter by date if dates are provided
        if start_date or end_date:
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
            # No date filtering, just limit to post_filter_top_k
            print(f"   Found {len(results)} results, limiting to top {post_filter_top_k}")
            top_results = top_results[:post_filter_top_k]
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
    qa_text = []

    for i, result in enumerate(top_summaries, 1):
        summary = result['summary']
        score = result['similarity']
        result_type = result.get('result_type', mode)  # Use result_type if hybrid, else use mode

        if result_type == 'qa':
            # Extract Q&A from content field - only QUESTION and ANSWER, no SOURCE CONTEXT
            content = summary.get('content', '')
            qa_data = extract_qa_from_content(content)
            doc_metadata = summary.get('document_metadata', {})
            source_url = doc_metadata.get('source_url', 'N/A')
            title = doc_metadata.get('title', 'N/A')
            authors = doc_metadata.get('authors', [])
            author_str = ', '.join(authors) if authors else 'N/A'
            qa_text.append(
                f"Q&A Pair {len(qa_text) + 1} (relevance: {score:.4f}):\n"
                f"Source: {source_url}\n"
                f"Title: {title}\n"
                f"Author(s): {author_str}\n"
                f"QUESTION: {qa_data['question']}\n"
                f"ANSWER: {qa_data['answer']}\n"
            )
        else:
            # Regular summary mode
            doc_id = summary.get('document_id', 'unknown')
            source_url = summary.get('source_url', 'N/A')
            doc_metadata = summary.get('doc_metadata', {})
            pub_date = doc_metadata.get('publication_date', 'N/A')
            summary_text = summary.get('summary_text', '')
            summaries_text.append(
                f"Summary {len(summaries_text) + 1} (similarity: {score:.4f}):\n"
                f"Document ID: {doc_id}\n"
                f"Publication Date: {pub_date}\n"
                f"Source: {source_url}\n"
                f"Content: {summary_text}\n"
            )

    # Print content being sent
    print("\n" + "="*80)
    if mode == 'hybrid':
        print("🔀 HYBRID CONTENT BEING SENT TO GEMINI:")
        print("="*80)
        if summaries_text:
            print("\n📚 SUMMARIES:")
            for s in summaries_text:
                print(s)
                print("-"*80)
        if qa_text:
            print("\n❓ Q&A PAIRS:")
            for q in qa_text:
                print(q)
                print("-"*80)
    elif mode == 'qa':
        print("📋 Q&A PAIRS BEING SENT TO GEMINI:")
        print("="*80)
        for q in qa_text:
            print(q)
            print("-"*80)
    else:
        print("📋 SUMMARIES BEING SENT TO GEMINI:")
        print("="*80)
        for s in summaries_text:
            print(s)
            print("-"*80)

    # Build prompt based on mode
    if mode == 'hybrid':
        summaries_context = "\n\n".join(summaries_text) if summaries_text else "None"
        qa_context = "\n\n".join(qa_text) if qa_text else "None"
        prompt = f"""Given these research summaries and question-answer pairs, provide a comprehensive overview that synthesizes the key themes, findings, and insights. When citing summaries, use the publisher/author and URL stub.

Original Query: {query}

Research Summaries:
{summaries_context}

Question-Answer Pairs:
{qa_context}

Please provide:
1. A high-level overview of the main themes across all sources
2. Key findings or insights that emerge from both summaries and Q&A pairs
3. Any patterns or connections between the different sources
4. Notable differences or contrasting viewpoints if any

Overview:"""
    elif mode == 'qa':
        context = "\n\n".join(qa_text)
        prompt = f"""Given these question-answer pairs, provide a comprehensive overview that synthesizes the key themes, findings, and insights.

Original Query: {query}

Question-Answer Pairs:

{context}

Please provide:
1. A high-level overview of the main themes across these Q&A pairs
2. Key findings or insights that emerge
3. Any patterns or connections between the answers
4. Notable differences or contrasting viewpoints if any

Overview:"""
    else:
        context = "\n\n".join(summaries_text)
        prompt = f"""Given these summaries, provide a comprehensive overview that synthesizes the key themes, findings, and insights. When you cite each summary use the: publisher/author and a stub of the url (Al-Jazeer, Nov 2023, www.al-jazeera)

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
    parser.add_argument("--mode", type=str, default="summaries", choices=["summaries", "qa", "hybrid"], help="Query mode: summaries, qa pairs, or hybrid (both)")
    parser.add_argument("--qa-index", type=str, help="Path to Q&A pairs index (required for hybrid mode)")
    parser.add_argument("--qa-top-k", type=int, help="Number of Q&A pairs to retrieve (defaults to top-k in qa mode, top-k//2 in hybrid)")
    args = parser.parse_args()

    # Validate hybrid mode requirements
    if args.mode == 'hybrid' and not args.qa_index:
        parser.error("--qa-index is required when using hybrid mode")

    # Query summaries
    top_results = query_summaries(
        query=args.query,
        index_path=args.index,
        top_k=args.top_k,
        start_date=args.start_date,
        end_date=args.end_date,
        post_filter_top_k=args.post_filter_top_k,
        mode=args.mode,
        qa_index_path=args.qa_index,
        qa_top_k=args.qa_top_k
    )

    if not top_results:
        print("\n⚠️  No results found!")
        return

    # Show individual summaries if requested
    if args.show_summaries:
        print("\n" + "=" * 80)
        print("TOP RESULTS")
        print("=" * 80)
        for i, result in enumerate(top_results, 1):
            summary = result['summary']
            score = result['similarity']

            # Handle metadata based on mode
            if args.mode == 'qa':
                doc_metadata = summary.get('document_metadata', {})
                chunk_metadata = summary.get('chunk_metadata', {})
                doc_id = chunk_metadata.get('chunk_id', 'unknown')
                source_url = chunk_metadata.get('source_url', 'N/A')
            else:
                doc_metadata = summary.get('doc_metadata', {})
                doc_id = summary.get('document_id', 'unknown')
                source_url = summary.get('source_url', 'N/A')

            pub_date = doc_metadata.get('publication_date', 'N/A')
            print(f"\n{i}. Similarity: {score:.4f}")
            print(f"   Document ID: {doc_id}")
            print(f"   Publication Date: {pub_date}")
            print(f"   Source: {source_url}")
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
