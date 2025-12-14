#!/usr/bin/env python3
"""
Generate Q&A Pairs from Documents

This script fetches full document content via citation research, converts to markdown,
and generates Q&A pairs from paragraph chunks.
"""
import asyncio
import json
import os
import re
import sys
from asyncio import Semaphore
from pathlib import Path
from typing import Any, Dict, List

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../..'))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../src'))

from src.components.summarise.llm_citation_researcher import \
    CitationResearchClient


def load_unique_documents(input_file: Path) -> List[Dict[str, Any]]:
    """Load documents from unique_documents.jsonl."""
    documents = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                documents.append(json.loads(line))
    return documents


def filter_doc_code_documents(documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter documents where citation_type is DOC_CODE."""
    return [doc for doc in documents if doc.get('citation_type') == 'DOC_CODE']


def extract_paragraphs_from_markdown(markdown_text: str) -> List[str]:
    """Extract paragraph chunks from markdown, excluding titles, headers, and footnotes.

    Returns only substantive paragraph content.
    """
    paragraphs = []

    # Split by double newlines to get blocks
    blocks = re.split(r'\n\s*\n', markdown_text)

    for block in blocks:
        block = block.strip()

        # Skip if empty
        if not block:
            continue

        # Skip headers (lines starting with #)
        if block.startswith('#'):
            continue

        # Skip if it looks like a title (all caps, short)
        if len(block) < 100 and block.isupper():
            continue

        # Skip footnotes (lines starting with [^] or just numbers/citations)
        if re.match(r'^\[\^?\d+\]', block) or re.match(r'^\d+\.?\s*$', block):
            continue

        # Skip if it's just a list marker or citation
        if re.match(r'^[\*\-\+]\s*$', block):
            continue

        # Accept all remaining blocks to keep pipeline flowing
        # Short content can be filtered downstream if needed
        paragraphs.append(block)

    return paragraphs


async def generate_qa_pairs_for_paragraph(paragraph: str, researcher: CitationResearchClient) -> List[Dict[str, str]]:
    """Generate Q&A pairs for a single paragraph chunk using LLM.

    Returns list of {'question': str, 'answer': str, 'content': str}
    where 'content' is the paragraph itself.
    """
    prompt = f"""Generate 2-3 question-answer pairs from the following paragraph.

IMPORTANT: The 'content' field should contain the EXACT paragraph text provided below, not a truncated version.

Each Q&A pair should:
- Question: Extract a specific factual question that can be answered from this paragraph
- Answer: Provide a concise answer (1-3 sentences) based ONLY on the paragraph content

Output ONLY valid JSON in this format:
```json
{{
  "qa_pairs": [
    {{"question": "...", "answer": "..."}},
    {{"question": "...", "answer": "..."}}
  ]
}}
```

Paragraph:
{paragraph}

Generate questions that:
1. Are specific and factual
2. Can be answered directly from this paragraph
3. Are useful for understanding key concepts or facts
4. Avoid generic questions like "What is this about?"

Output ONLY the JSON, no other text.
"""

    try:
        # Use the researcher's generate_content method
        if researcher.llm_provider == 'openai':
            response = await researcher._generate_with_openai(prompt)
        else:
            response = await researcher._generate_with_gemini(prompt)

        # Extract JSON from response
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if json_match:
            json_text = json_match.group(1)
        else:
            # Try to find raw JSON
            json_text = response

        # Parse JSON
        result = json.loads(json_text.strip())
        qa_pairs = result.get('qa_pairs', [])

        # Add the full content to each pair
        for pair in qa_pairs:
            pair['content'] = paragraph

        return qa_pairs

    except Exception as e:
        researcher._logger.error(f"   ❌ Failed to generate Q&A pairs: {e}")
        return []


async def process_single_document(doc: Dict[str, Any], index: int, total: int,
                                  researcher: CitationResearchClient) -> Dict[str, Any]:
    """Fetch document, convert to markdown, and generate Q&A pairs."""

    canonical_doc_id = doc.get('canonical_doc_id', 'unknown')

    print(f"\n[{index}/{total}] Processing: {canonical_doc_id[:80]}...")
    print(f"   📝 Citation count: {doc.get('citation_count', 0)}")
    print(f"   🔑 Doc code: {canonical_doc_id[:60]}...")

    # Build citation string
    citation = doc.get('citation', 'Unknown citation')
    print(f"   📖 Citation: {citation[:120]}...")

    # Get URLs
    urls = [doc.get('fetchable_url'), doc.get('canonical_url')]
    urls = [u for u in urls if u]

    print(f"   🌐 Fetching full document content using LLM grounding...")

    # Fetch document using LLM with web search grounding
    try:
        # Use LLM's web search capability to ground and extract the full document
        fetch_prompt = f"""You are accessing this document URL to extract its FULL TEXT content:

URL: {urls[0] if urls else citation}

TASK: Use web search to access this URL and extract the COMPLETE article/document text.

CRITICAL INSTRUCTIONS:
1. Use web search to fetch the actual page content from the URL
2. Extract ALL paragraphs of substantive text from the document
3. Return the full text in clean markdown format
4. Include all sections, paragraphs, and content
5. Do NOT summarize, paraphrase, or condense - return the EXACT text from the source
6. Exclude navigation menus, headers, footers, ads, and sidebars
7. Focus on the main article/document body content

Return the complete document text as markdown."""

        if researcher.llm_provider == 'openai':
            markdown_content = await researcher._generate_with_openai(fetch_prompt)
        else:
            markdown_content = await researcher._generate_with_gemini(fetch_prompt)

        if not markdown_content or len(markdown_content) < 100:
            print(f"   ⚠️  Retrieved content too short ({len(markdown_content)} chars)")
            return {
                'canonical_doc_id': canonical_doc_id,
                'citation': citation,
                'qa_pairs': [],
                'error': 'Document content too short or empty'
            }

        print(f"   ✅ Retrieved full document ({len(markdown_content)} chars)")

        # Extract paragraphs
        paragraphs = extract_paragraphs_from_markdown(markdown_content)
        print(f"   📄 Extracted {len(paragraphs)} paragraph chunks")

        # Generate Q&A pairs for each paragraph
        all_qa_pairs = []

        for i, paragraph in enumerate(paragraphs[:10], 1):  # Limit to first 10 paragraphs
            print(f"   🤖 Generating Q&A for paragraph {i}/{min(len(paragraphs), 10)}...")
            qa_pairs = await generate_qa_pairs_for_paragraph(paragraph, researcher)
            all_qa_pairs.extend(qa_pairs)
            print(f"      ✅ Generated {len(qa_pairs)} Q&A pairs")

        print(f"   💾 Total Q&A pairs: {len(all_qa_pairs)}")

        return {
            'canonical_doc_id': canonical_doc_id,
            'citation': citation,
            'urls': urls,
            'paragraph_count': len(paragraphs),
            'qa_pairs': all_qa_pairs
        }

    except Exception as e:
        print(f"   ❌ Failed to process document: {e}")
        return {
            'canonical_doc_id': canonical_doc_id,
            'citation': citation,
            'qa_pairs': [],
            'error': str(e)
        }


async def generate_qa_progressively(documents: List[Dict[str, Any]], output_file: Path,
                                   researcher: CitationResearchClient, max_concurrent: int = 3):
    """Generate Q&A pairs with controlled concurrency."""

    semaphore = Semaphore(max_concurrent)

    async def process_with_semaphore(doc, idx, total):
        async with semaphore:
            return await process_single_document(doc, idx, total, researcher)

    # Create tasks
    tasks = [process_with_semaphore(doc, i, len(documents)) for i, doc in enumerate(documents, 1)]

    # Process and write results as they complete
    print(f"\n📊 Processing {len(documents)} documents...")
    print(f"💾 Output: {output_file}")
    print(f"⚡ Max concurrent: {max_concurrent}")
    print(f"{'='*80}\n")

    with open(output_file, 'a', encoding='utf-8') as f:
        for coro in asyncio.as_completed(tasks):
            result = await coro

            # Write result
            f.write(json.dumps(result, ensure_ascii=False) + '\n')
            f.flush()
            print(f"   💾 Saved result with {len(result.get('qa_pairs', []))} Q&A pairs")


async def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate Q&A pairs from full documents')
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
        default='openai',
        help='LLM provider to use (default: openai)'
    )
    parser.add_argument(
        '--model',
        help='Model name (e.g., gpt-5.1 for openai, gemini-2.5-pro for gemini)'
    )
    parser.add_argument(
        '--max-docs',
        type=int,
        help='Maximum number of documents to process (for testing)'
    )
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=2,
        help='Maximum number of concurrent API calls (default: 2, lower because we do multiple calls per doc)'
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

    # Initialize citation researcher
    print(f"\n🤖 Initializing citation researcher...")
    print(f"   Provider: {args.llm_provider}")
    print(f"   Model: {args.model or 'default'}")
    researcher = CitationResearchClient(
        llm_provider=args.llm_provider,
        model_name=args.model,
        web_searcher=None  # Uses internal web search
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

    # Generate Q&A pairs progressively
    await generate_qa_progressively(
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
