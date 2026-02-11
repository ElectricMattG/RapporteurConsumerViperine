#!/usr/bin/env python3
"""
PDF Generator for RAG Query Results.

Generates professionally formatted PDF reports with preamble, introduction,
assistant response, and citations.
"""

import json
import logging
import os
import re
from datetime import datetime
from io import BytesIO
from urllib.parse import unquote, urlparse

import markdown
import requests
from jinja2 import Template
from weasyprint import HTML

logger = logging.getLogger(__name__)

# Load preamble from prompts module
try:
    import sys
    sys.path.insert(0, '/app')
    from src.prompts.preamble import PREAMBLE
except ImportError:
    logger.warning("Could not import preamble, using fallback")
    PREAMBLE = "Rapporteur AI Intelligence Platform - Research Report"


def generate_introduction(query: str, assistant_response: str, rag_service_url: str) -> str:
    """
    Generate an introduction for the PDF report.

    Args:
        query: The user's query
        assistant_response: The assistant's response (first 500 chars for context)
        rag_service_url: URL of the RAG service (unused, for future LLM integration)

    Returns:
        Introduction text (HTML formatted)
    """
    # Use fallback introduction for now
    # TODO: Implement LLM-generated introduction when /generate_text endpoint is available
    return generate_fallback_introduction(query)


def generate_fallback_introduction(query: str) -> str:
    """Generate a simple fallback introduction when LLM call fails."""
    return f"""<p>This report provides a comprehensive analysis in response to the following research query:</p>
<p><em>"{query}"</em></p>
<p>The analysis draws upon relevant source documents and question-answer pairs from the research corpus,
providing detailed insights and evidence-based findings. All sources are cited and listed in the references section.</p>"""


def format_author_name(name: str) -> str:
    """
    Format author name with edge case handling:
    - Long names (>4 words): take first 2, unless starts with initial (N., M.) then take 3
    - All-caps multi-word names: convert to Title Case
    - Single-word all-caps: keep as-is (acronyms like IDF)
    - Organizational names and non-Latin scripts preserved

    Args:
        name: Raw author name

    Returns:
        Formatted author name
    """
    if not name or not name.strip():
        return ""

    # Decode URL-encoded characters (e.g. %D8%A7 → Arabic)
    author = unquote(name.strip())
    words = author.split()

    # Handle long names (>4 words) - likely includes extra text
    if len(words) > 4:
        # Check if first OR second word is an initial (single letter followed by period)
        # Examples: "N. M. Peter..." or "Nancy M. Smith..."
        has_initial = False
        if len(words[0]) == 2 and words[0][1] == '.':
            has_initial = True  # First word is initial
        elif len(words) > 1 and len(words[1]) == 2 and words[1][1] == '.':
            has_initial = True  # Second word is initial (e.g., Nancy M.)

        if has_initial:
            # Take first 3 words (e.g., "N. M. Peter" or "Nancy M. Smith")
            author = ' '.join(words[:3])
            words = author.split()
        else:
            # Take first 2 words (e.g., "Michael Bachner" from long string)
            author = ' '.join(words[:2])
            words = author.split()

    # Handle all-caps multi-word names (convert to Title Case)
    # But keep single-word all-caps (acronyms like IDF)
    if len(words) > 1:
        # Check if all words are uppercase (ignoring periods)
        all_caps = all(word.replace('.', '').isupper() for word in words if word.replace('.', '').isalpha())
        if all_caps:
            # Convert to Title Case: PETER SMITH -> Peter Smith
            author = ' '.join(word.title() for word in words)

    return author


def format_citation(doc_metadata: dict, source_url: str, index: int, is_qa: bool = False) -> dict:
    """
    Format a citation entry.

    Args:
        doc_metadata: Document metadata dictionary
        source_url: Source URL of the document
        index: Citation index number
        is_qa: Whether this is a Q&A citation (unused in new format)

    Returns:
        Dictionary with formatted citation fields and complete citation text
    """
    # Extract author with fallback chain
    # First try plural 'authors' array (correct field for summaries)
    authors_list = doc_metadata.get('authors', [])

    if authors_list and isinstance(authors_list, list) and len(authors_list) > 0:
        # Filter out null/empty values and get only the first author
        valid_authors = [a for a in authors_list if a and str(a).lower() not in ['null', 'nan', '', 'none']]

        if valid_authors:
            # Use only the first author (simplified)
            author = valid_authors[0]
        else:
            author = None
    else:
        # Fallback to singular 'author' field (legacy Q&A format)
        author = doc_metadata.get('author')
        if author and str(author).lower() not in ['null', 'nan', '', 'none']:
            # Keep as-is, no formatting
            pass
        else:
            author = None

    # Apply formatting to clean up author name (handles long names, all-caps, etc.)
    if author:
        author = format_author_name(author)

    # If no author found after formatting, fall back to publisher
    if not author:
        author = doc_metadata.get('publisher')
        if author:
            author = format_author_name(author)

    # Final fallback to domain name
    if not author or str(author).lower() in ['null', 'nan', '', 'none']:
        try:
            parsed = urlparse(source_url)
            author = parsed.netloc.replace('www.', '').split('.')[0].title()
        except:
            author = "Unknown"

    # Extract publisher with fallback chain
    publisher = doc_metadata.get('publisher')
    if not publisher or publisher == 'null' or publisher == 'nan':
        publisher = doc_metadata.get('author')
    if not publisher or publisher == 'null' or publisher == 'nan':
        # Fallback to domain from URL
        try:
            parsed = urlparse(source_url)
            publisher = parsed.netloc.replace('www.', '')
        except:
            publisher = "Unknown"

    # Apply formatting to clean up publisher name (handles long names, extra text, etc.)
    if publisher and publisher != "Unknown":
        publisher = format_author_name(publisher)

    # Extract and format dates
    date_field = (doc_metadata.get('publication_date') or
                  doc_metadata.get('creation_date') or
                  doc_metadata.get('published_date'))

    month_year = "ND"
    full_date = "ND"

    if date_field and str(date_field).lower() not in ['null', 'nan', 'none', '']:
        try:
            if isinstance(date_field, str):
                # Handle ISO 8601 format
                dt = datetime.fromisoformat(date_field.replace('Z', '+00:00'))
                month_year = dt.strftime("%B %Y")
                full_date = dt.strftime("%d %B %Y")
        except:
            # Fallback to partial date parsing
            try:
                date_str = str(date_field)[:10]  # Get YYYY-MM-DD portion
                if len(date_str) >= 7:  # At least YYYY-MM
                    parts = date_str.split('-')
                    if len(parts) >= 2:
                        year = parts[0]
                        month_num = int(parts[1])
                        month_name = datetime(2000, month_num, 1).strftime("%B")
                        month_year = f"{month_name} {year}"
                        if len(parts) == 3:
                            day = parts[2]
                            full_date = f"{day} {month_name} {year}"
                        else:
                            full_date = month_year
            except:
                pass

    # Extract title
    title = doc_metadata.get('title', 'Untitled Document')

    # Extract document ID
    doc_id = doc_metadata.get('document_id') or doc_metadata.get('doc_id') or 'unknown'

    # Build formatted citation string (doc_id suppressed from display)
    formatted_text = f"{author} ({month_year}). {title}. Published by *{publisher}* on {full_date}."

    return {
        'index': index,
        'author': author,
        'publisher': publisher,
        'month_year': month_year,
        'full_date': full_date,
        'title': title,
        'url': source_url,
        'doc_id': doc_id,
        'formatted_text': formatted_text
    }


def markdown_to_html(md_text: str) -> str:
    """Convert markdown to HTML."""
    md = markdown.Markdown(extensions=[
        'extra',
        'codehilite',
        'tables',
        'fenced_code',
        'nl2br'
    ])
    return md.convert(md_text)


def resolve_citations_for_pdf(text: str, citation_map: dict) -> str:
    """
    Resolve (doc_id, qa-N|summary) placeholders in the assistant response
    into hyperlinked (Author, Date) citations for PDF export.

    Mirrors the frontend renderCitationsAsLinks logic but outputs HTML <a> tags
    linking to the source URL instead of card anchors.

    Args:
        text: Raw assistant response text with citation placeholders
        citation_map: Map of doc_id/composite_key to {author, date, url, ...}

    Returns:
        Text with citations resolved as hyperlinked (Author, Date)
    """
    if not citation_map:
        return text

    # Step 1: Split semicolon-separated citation groups into individual citations
    # e.g. "(doc_XXX, qa-7; doc_YYY, qa-15)" -> "(doc_XXX, qa-7); (doc_YYY, qa-15)"
    def split_groups(match):
        inner = match.group(1)
        if not re.search(r'doc_|req_', inner, re.IGNORECASE):
            return match.group(0)
        parts = [f'({p.strip()})' for p in inner.split(';')]
        return '; '.join(parts)

    processed = re.sub(r'\(([^)]*;[^)]*)\)', split_groups, text)

    # Step 2: Match individual citations: (doc_XXX, qa-N) or (doc_XXX, summary)
    citation_pattern = re.compile(r'\(([a-z0-9_]+),\s*(qa-\d+|summary)\)', re.IGNORECASE)

    def replace_citation(match):
        doc_id = match.group(1)
        ref_type = match.group(2)
        composite_key = f"{doc_id}__{ref_type}"
        info = citation_map.get(composite_key) or citation_map.get(doc_id)
        if not info:
            return match.group(0)  # Leave unresolved
        author = unquote(info.get('author', 'Unknown'))
        date = info.get('date', 'N.D.')
        url = info.get('url', '')
        # No parens here — sentinels handle grouping
        display = f"{author}, {date}"
        if url:
            inner = f'<a href="{url}">{display}</a>'
        else:
            inner = display
        return f'\u27e6{inner}\u27e7'  # Sentinel markers ⟦…⟧

    resolved = citation_pattern.sub(replace_citation, processed)

    # Step 3: Merge adjacent sentinels into grouped parentheticals
    # ⟧; ⟦ or ⟧, ⟦ within same run → collapse into single group
    resolved = re.sub(r'\u27e7\s*[;,]\s*\u27e6', '; ', resolved)
    # Wrap remaining sentinels as parentheses
    resolved = resolved.replace('\u27e6', '(').replace('\u27e7', ')')

    return resolved


def merge_adjacent_pdf_citations(html: str) -> str:
    """
    Legacy merge function — kept for backwards compatibility.
    Merging is now handled via sentinel markers in resolve_citations_for_pdf.
    """
    return html


def load_index_metadata() -> dict:
    """
    Load index metadata from config file.

    Returns:
        Dictionary with index metadata or defaults
    """
    metadata_path = '/app/src/services/frontend/config/metadata.jsonl'

    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                return json.loads(f.readline())
    except Exception as e:
        logger.warning(f"Could not load metadata config: {e}")

    # Return defaults
    return {
        'index_name': 'research_summaries_with_embeddings_combined',
        'index_version': datetime.now().strftime('%Y-%m-%d'),
        'description': 'RAG Intelligence Platform - Research Document Analysis'
    }


def sanitize_filename(name: str, max_length: int = 100) -> str:
    """
    Sanitize string for use as filename.

    Args:
        name: Raw filename
        max_length: Maximum length

    Returns:
        Clean filename string
    """
    # Remove/replace invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    name = re.sub(r'\s+', '_', name)
    name = name.strip('._')

    # Truncate if too long
    if len(name) > max_length:
        name = name[:max_length]

    return name or 'report'


def generate_document_title(query: str, assistant_response: str, rag_service_url: str) -> str:
    """
    Generate a descriptive document title using LLM.

    Args:
        query: User's original query
        assistant_response: First ~500 chars of response for context
        rag_service_url: URL of RAG service

    Returns:
        Generated title or fallback
    """
    try:
        # Use first 500 chars of response for context
        segment = assistant_response[:500]

        prompt = f"""Given this document segment, create a concise title that captures the key focus and purpose.

User query: {query}

Document excerpt: {segment}

Title:"""

        # Try to call RAG service /generate_text endpoint
        response = requests.post(
            f"{rag_service_url}/generate_text",
            json={"prompt": prompt, "max_tokens": 50, "temperature": 0.3},
            timeout=10
        )

        if response.ok:
            title = response.json().get('text', '').strip()
            if title:
                # Remove quotes and trailing punctuation
                title = title.strip('"\'.').strip()
                return sanitize_filename(title)

    except Exception as e:
        logger.debug(f"Could not generate title via LLM: {e}")

    # Fallback: use first few words of query
    words = query.split()[:6]
    return sanitize_filename('_'.join(words))


def build_pdf_html(
    query: str,
    assistant_response: str,
    retrieved_docs: list,
    retrieved_qa: list,
    introduction: str,
    template_path: str,
    css_path: str,
    citation_map: dict = None
) -> str:
    """
    Build complete HTML for PDF from template.

    Args:
        query: User query (used as document name)
        assistant_response: Markdown formatted response
        retrieved_docs: List of retrieved summary documents
        retrieved_qa: List of retrieved Q&A pairs
        introduction: Introduction HTML
        template_path: Path to HTML template
        css_path: Path to CSS file

    Returns:
        Complete HTML string
    """
    # Load template
    with open(template_path, 'r') as f:
        template = Template(f.read())

    # Load CSS
    with open(css_path, 'r') as f:
        styles = f.read()

    # Load index metadata
    index_metadata = load_index_metadata()

    # Format preamble
    current_date = datetime.now().strftime("%B %d, %Y")
    preamble_text = PREAMBLE.replace('[Document_Name]', query[:100])
    preamble_text = preamble_text.replace('[Current_Date]', current_date)

    # Convert assistant response markdown to HTML
    # First resolve citation placeholders if citation_map is available
    resolved_response = assistant_response
    if citation_map:
        logger.info(f"Resolving {len(citation_map)} citation map entries for PDF")
        resolved_response = resolve_citations_for_pdf(assistant_response, citation_map)
    response_html = markdown_to_html(resolved_response)

    # Format all citations (summary + QA)
    all_citations = []
    seen_doc_ids = set()

    # Add summary citations
    for doc in retrieved_docs:
        # Check root level first (summaries), then nested (Q&A pairs)
        doc_id = doc.get('document_id') or doc.get('doc_metadata', {}).get('document_id') or doc.get('doc_metadata', {}).get('doc_id')
        source_url = doc.get('source_url', '')

        # Use doc_id for deduplication, fallback to source_url
        dedup_key = doc_id if doc_id and doc_id != 'unknown' else source_url

        if dedup_key not in seen_doc_ids:
            seen_doc_ids.add(dedup_key)
            # Get metadata and inject document_id if it's at root level
            metadata = doc.get('doc_metadata', {}).copy()
            if doc_id and 'document_id' not in metadata:
                metadata['document_id'] = doc_id
            all_citations.append({
                'metadata': metadata,
                'url': source_url
            })

    # Add QA citations (deduplicate against summaries)
    for qa in retrieved_qa:
        # Check root level first (if Q&A structure changes), then nested
        doc_id = qa.get('document_id') or qa.get('doc_metadata', {}).get('document_id') or qa.get('doc_metadata', {}).get('doc_id')
        source_url = qa.get('source_url', '')

        # Use doc_id for deduplication, fallback to source_url
        dedup_key = doc_id if doc_id and doc_id != 'unknown' else source_url

        if dedup_key not in seen_doc_ids:
            seen_doc_ids.add(dedup_key)
            # Get metadata and inject document_id if it's at root level
            metadata = qa.get('doc_metadata', {}).copy()
            if doc_id and 'document_id' not in metadata:
                metadata['document_id'] = doc_id
            all_citations.append({
                'metadata': metadata,
                'url': source_url
            })

    # Format citations with sequential numbering
    formatted_citations = [
        format_citation(cit['metadata'], cit['url'], i+1)
        for i, cit in enumerate(all_citations)
    ]

    # Prepare metadata for template
    doc_metadata = {
        'index_name': index_metadata.get('index_name', 'Unknown Index'),
        'model': 'gemini-2.5-flash',  # Could be passed as parameter
        'index_version': index_metadata.get('index_version', 'Unknown'),
        'generated_date': current_date,
        'description': index_metadata.get('description', '')
    }

    # Render template
    html = template.render(
        document_name=query[:100],
        preamble=preamble_text,
        metadata=doc_metadata,
        introduction=introduction,
        assistant_response=response_html,
        citations=formatted_citations,
        styles=styles
    )

    return html


def convert_to_pdf(html_content: str) -> bytes:
    """
    Convert HTML to PDF using WeasyPrint.

    Args:
        html_content: Complete HTML string

    Returns:
        PDF as bytes
    """
    try:
        # Create PDF
        pdf_file = BytesIO()
        HTML(string=html_content).write_pdf(pdf_file)
        pdf_bytes = pdf_file.getvalue()

        logger.info(f"Generated PDF: {len(pdf_bytes)} bytes")
        return pdf_bytes

    except Exception as e:
        logger.error(f"Error converting HTML to PDF: {e}")
        raise
