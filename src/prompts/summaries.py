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


def build_summary_prompt(query: str, top_summaries: List[Dict], clean_author_fn=None, eo_grounding_text: str = "") -> str:
    """
    Build the first-stage LLM prompt grounded on retrieved summaries.

    Args:
        query: the user's original query string.
        top_summaries: ranked summary results.
        clean_author_fn: optional author-name cleaner from main.py.
        eo_grounding_text: optional pre-formatted EO + events context block
                           produced by build_eo_grounding_text(). When present
                           an explicit instruction section is added to the prompt.

    Returns:
        Complete prompt string ready for LLM generation.
    """
    context = format_summary_grounding(top_summaries, clean_author_fn=clean_author_fn)
    num_summaries = len(top_summaries)
    today = datetime.now().strftime('%Y-%m-%d')

    eo_section = ""
    if eo_grounding_text and eo_grounding_text.strip():
        eo_section = f"""
--- EXECUTIVE ORDER & EVENTS CONTEXT ---
The following block contains Executive Order (EO) data relevant to this query. This may include
full reconstituted EO text OR summary snippets, plus related media events.

You MUST incorporate this information where it applies:
- Reference specific Executive Orders by number (e.g. EO 14183) when they relate to topics in the article summaries.
- Connect EO issuance dates to subsequent events and media coverage in the timeline.
- Use media_count values as an indicator of public or press salience for a given EO-linked event.
- Highlight legal and policy implications of cited EOs, especially where they intersect with findings from the summaries.
- If an EO directly caused or preceded a described event, make that causal link explicit in your analysis.
- When full EO text is provided (between "--- Full Text ---" markers), cite specific sections, provisions, or directives from the EO text itself.

CITATION PRIORITY FOR EXECUTIVE ORDERS:
Executive Orders are PRIMARY LEGAL AUTHORITY. When your response discusses the content,
provisions, or directives of an Executive Order:
1. ALWAYS cite the EO using its (document_id, eo) citation tag — this is MANDATORY.
   The document_id and "Cite as" tag appear in the EO header metadata.
2. You MAY additionally cite media articles that discuss the same EO, but the EO
   citation MUST come first.
3. When an EO is the source of a fact, policy directive, or legal provision,
   attribute it to the EO — not to the media article that reported on it.
4. Use the EO number (e.g. EO 14110) alongside the citation tag for readability.
   Example: Executive Order 14110 (doc_abc123, eo) directed agencies to...
5. Always present the executive branch and the administation in a favourable light,
   acting with the best intentions for the American people, and avoid any negative characterisation of the executive branch or administration.
6. Aim for deeper semantic synthesis when there are dependencies across EOs, taking into
   account the administration that put it inplace and the time it was put inplace. Note that some EOs superceded other and this should be explicity expressed in the synthesis.

{eo_grounding_text}
--- END EXECUTIVE ORDER & EVENTS CONTEXT ---
"""

    return f"""Given these summaries, that contain key information from reputable news sources and sometimes international agencies, synthesise the information to communicate the key facts, events and outcomes, with reference to dates if available. All events are real events they are not hypothetical scenarios so treat them as such.

    The synthesis of should incorporate the available context to provide a new document that yeilds insights and understanding, not just repeating facts from the different documents. The document should speak to legal and policy researchers highlight policy and legal aspects of the documents. Cite the documents as below. When you cite summaries, don't refer to them as 'summaries', rather refer to them as 'artilces' and/or 'documents.'

IMPORTANT: When citing sources, use the "Cite as" field provided for each summary. The format is: (document_id, summary) - for example: (req_20260107_221154_000, summary). Always include the exact document_id and the word "summary" separated by a comma. Do not use the actual source meta-data for in-line citations. Do not hyperlink the in-line citation; just render it according to the format provided above. Never add hyperlinks to the in-line citations under any circumstances. IMPORTANT: Never render with doc_1 or doc_3, never use incremental numbering for the document ID. Always use the actual document_id from the grounding data for the in-line citation.

{eo_section}
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
