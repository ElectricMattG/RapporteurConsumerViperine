#!/usr/bin/env python3
"""
Generate Citation Summaries for DOC_CODE Documents

This script:
1. Reads unique_documents.jsonl from deduplication output
2. Filters for documents grouped by DOC_CODE (validated documents)
3. Uses llm_citation_researcher to generate structured summaries
4. Progressively saves summaries to JSONL (append after each)
"""
import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import citation researcher
from src.components.summarise.llm_citation_researcher import \
    CitationResearchClient


def load_unique_documents(input_file: Path) -> List[Dict[str, Any]]:
    """Load unique documents from JSONL file.

    Args:
        input_file: Path to unique_documents.jsonl

    Returns:
        List of document dictionaries
    """
    documents = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                documents.append(json.loads(line))
    return documents


def filter_doc_code_documents(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter for documents grouped by DOC_CODE.

    These are documents where the canonical_doc_id starts with "DOC_CODE:"
    (e.g., UN documents, DOIs, ISBNs, validated URLs)

    Args:
        documents: List of all unique documents

    Returns:
        Filtered list of DOC_CODE documents
    """
    doc_code_docs = []

    for doc in documents:
        canonical_id = doc.get('canonical_doc_id', '')

        # Only keep if grouped by DOC_CODE
        if canonical_id.startswith('DOC_CODE:'):
            doc_code_docs.append(doc)

    return doc_code_docs


def extract_doc_code(canonical_id: str) -> str:
    """Extract the actual doc code from canonical_doc_id.

    Args:
        canonical_id: String like "DOC_CODE:A/HRC/22/63" or "DOC_CODE:https://..."

    Returns:
        The doc code part (e.g., "A/HRC/22/63" or URL)
    """
    if canonical_id.startswith('DOC_CODE:'):
        return canonical_id[9:]  # Remove "DOC_CODE:" prefix
    return canonical_id


def build_citation_string(doc: Dict[str, Any]) -> str:
    """Build a citation string from document metadata.

    Priority:
    1. Use source_text if substantive
    2. Construct from metadata (title + author + publisher + date)
    3. Use doc_code as last resort

    Args:
        doc: Document dictionary with metadata

    Returns:
        Citation string for research
    """
    metadata = doc.get('metadata', {})
    source_text = doc.get('source_text', '').strip()

    # Check if source_text is substantive
    is_substantive = (
        len(source_text) > 30 and
        not source_text.startswith(('UNGP', 'Art.', 'Art ', 'para.', 'para ', 'pp.', 'pp ')) and
        ',' in source_text
    )

    # Use source_text if substantive
    if is_substantive:
        return source_text

    # Otherwise construct from metadata
    parts = []

    # Author
    author = metadata.get('author')
    if author and author not in ['unknown', 'Unknown', None]:
        parts.append(str(author))

    # Title (most important)
    title = metadata.get('title')
    if title and title not in ['unknown', 'Unknown', None]:
        title_clean = str(title).strip()
        if not title_clean.startswith('"'):
            title_clean = f'"{title_clean}"'
        parts.append(title_clean)

    # Publisher
    publisher = metadata.get('publisher')
    if publisher and publisher not in ['unknown', 'Unknown', None]:
        parts.append(str(publisher))

    # Date
    pub_date = metadata.get('published_date')
    if pub_date and pub_date not in ['unknown', 'Unknown', None]:
        year = str(pub_date).split('-')[0] if '-' in str(pub_date) else str(pub_date)
        parts.append(f'({year})')

    # If we have good metadata, use it
    if len(parts) >= 2:
        return ', '.join(parts)

    # Fallback: Use doc_code if it's a UN document or looks like a title
    doc_code = extract_doc_code(doc.get('canonical_doc_id', ''))
    if doc_code and not doc_code.startswith('http'):
        # It's a UN doc code or similar
        if title:
            return f"{doc_code} - {title}"
        return doc_code

    # Last resort: URL or source_text
    if source_text:
        return source_text

    return doc_code if doc_code else 'Unknown citation'


async def process_single_document(
    doc: Dict[str, Any],
    index: int,
    total: int,
    researcher: CitationResearchClient
) -> Dict[str, Any]:
    """Process a single document and generate summary.

    Args:
        doc: Document dictionary
        index: Current document index (1-based)
        total: Total number of documents
        researcher: Citation research client

    Returns:
        Result record with summary or error
    """
    canonical_id = doc.get('canonical_doc_id', 'unknown')
    doc_code = extract_doc_code(canonical_id)

    print(f"[{index}/{total}] Processing: {canonical_id[:80]}...")
    print(f"   📝 Citation count: {doc.get('citation_count', 0)}")
    print(f"   🔑 Doc code: {doc_code[:60]}...")

    # Build citation string
    citation = build_citation_string(doc)
    print(f"   📖 Citation: {citation[:150]}...")

    # Skip if citation is too short/poor quality
    if len(citation) < 15 or citation == 'Unknown citation':
        print(f"   ⚠️  Skipping - insufficient citation information")

        skip_record = {
            'canonical_doc_id': canonical_id,
            'doc_code': doc_code,
            'citation_count': doc.get('citation_count', 0),
            'citation_ids': doc.get('citation_ids', []),
            'document_type': doc.get('document_type'),
            'original_source_text': doc.get('source_text'),
            'research_summary': {
                'skipped': True,
                'reason': 'Insufficient citation information',
                'citation_text': citation
            }
        }
        print(f"   ℹ️  Skip record created")
        print()
        return skip_record

    # Get URLs from document
    urls = []
    if doc.get('fetchable_url'):
        urls.append(doc['fetchable_url'])
    if doc.get('canonical_url') and doc['canonical_url'] != doc.get('fetchable_url'):
        urls.append(doc['canonical_url'])

    # Generate summary
    try:
        print(f"   🤖 Generating summary...")
        summary_result = await researcher.generate_summary(
            citation=citation,
            urls=urls,
            context=None
        )

        # Validate response
        if not summary_result or not isinstance(summary_result, dict):
            raise ValueError("API returned empty or invalid response")

        if 'markdown_summary' not in summary_result:
            raise ValueError("Response missing markdown_summary field")

        if summary_result.get('markdown_summary', '').startswith('# Error'):
            raise ValueError(f"API returned error summary")

        # Enrich with document info
        output_record = {
            'canonical_doc_id': canonical_id,
            'doc_code': doc_code,
            'citation_count': doc.get('citation_count', 0),
            'citation_ids': doc.get('citation_ids', []),
            'document_type': doc.get('document_type'),
            'original_source_text': doc.get('source_text'),
            'research_summary': summary_result
        }

        summary_words = len(summary_result.get('markdown_summary', '').split())
        metadata_fields = len([v for v in summary_result.get('metadata', {}).values() if v])
        print(f"   ✅ Summary generated ({summary_words} words, {metadata_fields} metadata fields)")
        print()
        return output_record

    except Exception as e:
        print(f"   ❌ Failed to generate summary: {e}")

        error_record = {
            'canonical_doc_id': canonical_id,
            'doc_code': doc_code,
            'citation_count': doc.get('citation_count', 0),
            'citation_ids': doc.get('citation_ids', []),
            'document_type': doc.get('document_type'),
            'original_source_text': doc.get('source_text'),
            'research_summary': {
                'error': str(e),
                'citation_text': citation,
                'urls': urls
            }
        }
        print(f"   ⚠️  Error record created")
        print()
        return error_record


async def generate_summaries_progressively(
    documents: List[Dict[str, Any]],
    output_file: Path,
    researcher: CitationResearchClient,
    max_concurrent: int = 3
):
    """Generate summaries progressively with concurrent processing and append to JSONL.

    Args:
        documents: List of documents to research
        output_file: Path to output JSONL file
        researcher: Citation research client
        max_concurrent: Maximum number of concurrent API calls (default: 3)
    """
    print(f"\n📊 Processing {len(documents)} documents...")
    print(f"💾 Output: {output_file}")
    print(f"⚡ Max concurrent: {max_concurrent}")
    print(f"{'='*80}\n")

    # Process in batches to control concurrency
    from asyncio import Semaphore

    semaphore = Semaphore(max_concurrent)

    async def process_with_semaphore(doc, idx, total):
        async with semaphore:
            return await process_single_document(doc, idx, total, researcher)

    # Open output file in append mode
    with open(output_file, 'a', encoding='utf-8') as f:
        # Process all documents concurrently but with semaphore limit
        tasks = [
            process_with_semaphore(doc, i, len(documents))
            for i, doc in enumerate(documents, 1)
        ]

        # Process and write results as they complete
        for coro in asyncio.as_completed(tasks):
            result = await coro
            # Write immediately to disk
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
            f.flush()
            print(f"   💾 Saved to {output_file}")


async def main():
    parser = argparse.ArgumentParser(
        description='Generate citation summaries for DOC_CODE documents'
    )
    parser.add_argument(
        'input_file',
        type=Path,
        help='Path to unique_documents.jsonl'
    )
    parser.add_argument(
        'output_file',
        type=Path,
        help='Path to output JSONL file (will append)'
    )
    parser.add_argument(
        '--llm-provider',
        choices=['openai', 'gemini'],
        default='gemini',
        help='LLM provider to use (default: gemini)'
    )
    parser.add_argument(
        '--model',
        help='Model name (e.g., gpt-5.1 for openai, gemini-1.5-pro-002 for gemini). If not specified, uses provider default.'
    )
    parser.add_argument(
        '--max-docs',
        type=int,
        help='Maximum number of documents to process (for testing)'
    )
    parser.add_argument(
        '--skip-web-search',
        action='store_true',
        help='Skip web search (faster but less accurate, only for OpenAI gpt-5 models)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=3,
        help='Maximum number of concurrent API calls (default: 3)'
    )
    parser.add_argument(
        '--citation-type',
        choices=['DOC_CODE', 'all'],
        default='DOC_CODE',
        help='Type of citations to process: DOC_CODE (default) or all'
    )

    args = parser.parse_args()

    # Validate input file
    if not args.input_file.exists():
        print(f"❌ Input file not found: {args.input_file}")
        sys.exit(1)

    # Load documents
    print(f"📖 Loading documents from {args.input_file}...")
    all_documents = load_unique_documents(args.input_file)
    print(f"   ✅ Loaded {len(all_documents)} documents")

    # Filter documents based on citation type
    if args.citation_type == 'DOC_CODE':
        print(f"\n🔍 Filtering for DOC_CODE documents...")
        filtered_docs = filter_doc_code_documents(all_documents)
        print(f"   ✅ Found {len(filtered_docs)} DOC_CODE documents")
    else:
        print(f"\n🔍 Processing all documents...")
        filtered_docs = all_documents
        print(f"   ✅ Processing {len(filtered_docs)} documents")

    # Apply max limit if specified
    if args.max_docs:
        filtered_docs = filtered_docs[:args.max_docs]
        print(f"   ℹ️  Limited to {args.max_docs} documents for testing")

    # Initialize citation researcher (no web searcher needed - we already have URLs)
    print(f"\n🤖 Initializing citation researcher...")
    print(f"   Provider: {args.llm_provider}")
    print(f"   Model: {args.model or 'default'}")
    print(f"   Web search: {'Enabled' if not args.skip_web_search else 'Disabled'}")
    researcher = CitationResearchClient(
        llm_provider=args.llm_provider,
        model_name=args.model,
        web_searcher=None  # Not needed - we already have validated URLs
    )
    print(f"   ✅ Citation researcher ready")

    # Create output directory if needed
    args.output_file.parent.mkdir(parents=True, exist_ok=True)

    # Check if output file exists
    if args.output_file.exists():
        print(f"\n⚠️  Output file exists: {args.output_file}")
        print(f"   Will append to existing file")
    else:
        print(f"\n💾 Creating new output file: {args.output_file}")

    # Generate summaries progressively
    await generate_summaries_progressively(
        documents=filtered_docs,
        output_file=args.output_file,
        researcher=researcher,
        max_concurrent=args.max_concurrent
    )

    print(f"\n{'='*80}")
    print(f"✅ Completed processing {len(filtered_docs)} documents")
    print(f"💾 Results saved to: {args.output_file}")


if __name__ == '__main__':
    asyncio.run(main())
