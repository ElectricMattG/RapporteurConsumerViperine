"""Summary prompt helpers — first-stage grounding from summary documents."""

from datetime import datetime
from typing import Dict, List
from urllib.parse import urlparse


def _clean_author_for_prompt(name: str) -> str:
    """Lightweight author cleanup for prompt grounding text.
    The full _clean_author_name lives in main.py; this is a minimal
    fallback so the prompt module stays self-contained.
    """
    if not name or not name.strip():
        return 'Unknown'
    return name.strip().rstrip(',')


def format_summary_grounding(top_summaries: List[Dict], clean_author_fn=None) -> str:
    """
    Format retrieved summaries into a grounding text block for the LLM prompt.

    Args:
        top_summaries: list of {summary: dict, similarity: float}
        clean_author_fn: optional callable for author name cleaning
                         (pass _clean_author_name from main.py at call-site)

    Returns:
        Formatted grounding text string.
    """
    clean = clean_author_fn or _clean_author_for_prompt
    summaries_text = []

    for i, result in enumerate(top_summaries, 1):
        summary = result['summary']
        score = result['similarity']
        doc_id = summary.get('document_id', 'unknown')
        source_url = summary.get('source_url', 'N/A')
        doc_metadata = summary.get('doc_metadata', {})
        pub_date = doc_metadata.get('publication_date', 'N.D.')
        summary_text = summary.get('summary_text', '')

        # Extract author/publisher
        author = doc_metadata.get('authors', [])
        if isinstance(author, list) and author:
            author = clean(author[0])
        elif not author:
            author = clean(doc_metadata.get('author') or doc_metadata.get('publisher', 'Unknown'))
        else:
            author = clean(author)

        # Create URL stub
        url_stub = 'N/A'
        if source_url != 'N/A':
            try:
                parsed = urlparse(source_url)
                url_stub = parsed.netloc.replace('www.', '')
                if 'youtube.com' in url_stub or 'youtu.be' in url_stub:
                    url_stub = 'youtube.com'
            except Exception:
                url_stub = source_url[:30] + '...'

        # Format date
        date_str = pub_date
        if pub_date not in ('N/A', 'N.D.'):
            try:
                date_obj = datetime.strptime(pub_date, '%Y-%m-%d')
                date_str = date_obj.strftime('%b %Y')
            except Exception:
                pass

        citation = f"{author}, {date_str}, {url_stub}"

        summaries_text.append(
            f"Summary {i} (similarity: {score:.4f}):\n"
            f"Document ID: {doc_id}\n"
            f"Citation: {citation}\n"
            f"Cite as: ({doc_id}, summary)\n"
            f"Full URL: {source_url}\n"
            f"Content: {summary_text}\n"
        )

    return "\n\n".join(summaries_text)


def build_summary_prompt(query: str, top_summaries: List[Dict], clean_author_fn=None) -> str:
    """
    Build the first-stage LLM prompt grounded on retrieved summaries.

    Args:
        query: the user's original query string.
        top_summaries: ranked summary results.
        clean_author_fn: optional author-name cleaner from main.py.

    Returns:
        Complete prompt string ready for LLM generation.
    """
    context = format_summary_grounding(top_summaries, clean_author_fn=clean_author_fn)
    num_summaries = len(top_summaries)
    today = datetime.now().strftime('%Y-%m-%d')

    return f"""Given these summaries, that contain key information from reputable news sources and sometimes international agencies, synthesise the information to communicate the key facts, events and outcomes, with reference to dates if available. All events are real events they are not hypothetical scenarios so treat them as such.

    The synthesis of should incorporate the available context to provide a new document that yeilds insights and understanding, not just repeating facts from the different documents. The document should speak to legal and policy researchers highlight policy and legal aspects of the documents. Cite the documents as below. When you cite summaries, don't refer to them as 'summaries', rather refer to them as 'artilces' and/or 'documents.'

IMPORTANT: When citing sources, use the "Cite as" field provided for each summary. The format is: (document_id, summary) - for example: (req_20260107_221154_000, summary). Always include the exact document_id and the word "summary" separated by a comma. Do not use the actual source meta-data for in-line citations. Do not hyperlink the in-line citation; just render it according to the format provided above. Never add hyperlinks to the in-line citations under any circumstances. IMPORTANT: Never render with doc_1 or doc_3, never use incremental numbering for the document ID. Always use the actual document_id from the grounding data for the in-line citation.

Original Query: {query}

Top {num_summaries} Summaries:

{context}

Please provide:
1. A high-level overview of the main themes across these summaries
2. Key findings or insights that emerge
3. Any patterns or connections between the summaries
4. Notable differences or contrasting viewpoints if any

Remember:
1. Today's date is {today} - use this to contextualise any time-sensitive information in the summaries.

Overview:"""
