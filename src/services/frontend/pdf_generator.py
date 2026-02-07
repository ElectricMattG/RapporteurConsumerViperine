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


def format_citation(doc_metadata: dict, source_url: str, index: int, is_qa: bool = False) -> dict:
    """
    Format a citation entry.

    Args:
        doc_metadata: Document metadata dictionary
        source_url: Source URL of the document
        index: Citation index number
        is_qa: Whether this is a Q&A citation

    Returns:
        Dictionary with formatted citation fields
    """
    # Extract author
    author = doc_metadata.get('author') or doc_metadata.get('publisher')
    if not author or author == 'null':
        # Fallback to domain name
        try:
            parsed = urlparse(source_url)
            author = parsed.netloc.replace('www.', '').split('.')[0].title()
        except:
            author = "Unknown"

    # Extract and format date
    date = doc_metadata.get('creation_date') or doc_metadata.get('publication_date')
    if date:
        try:
            # Try to parse and format as "Month Year"
            if isinstance(date, str):
                dt = datetime.fromisoformat(date.replace('Z', '+00:00'))
                date_str = dt.strftime("%B %Y")
            else:
                date_str = date
        except:
            date_str = str(date)[:10]  # Fallback to YYYY-MM-DD
    else:
        date_str = "n.d."

    # Extract title
    title = doc_metadata.get('title', 'Untitled Document')

    # Decode URL for display (handles Arabic/special characters)
    display_url = unquote(source_url)

    # Extract document ID
    doc_id = doc_metadata.get('document_id', 'unknown')

    return {
        'index': index,
        'author': author,
        'date': date_str,
        'title': title,
        'url': source_url,
        'display_url': display_url,
        'doc_id': doc_id
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
    css_path: str
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
    response_html = markdown_to_html(assistant_response)

    # Format citations
    summary_citations = [
        format_citation(doc.get('doc_metadata', {}), doc.get('source_url', ''), i+1, is_qa=False)
        for i, doc in enumerate(retrieved_docs)
    ]

    qa_citations = [
        format_citation(qa.get('doc_metadata', {}), qa.get('source_url', ''), i+1, is_qa=True)
        for i, qa in enumerate(retrieved_qa)
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
        summary_citations=summary_citations,
        qa_citations=qa_citations,
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
