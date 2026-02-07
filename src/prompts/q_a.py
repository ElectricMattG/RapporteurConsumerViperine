"""QA refinement prompt helpers."""

from datetime import datetime
from typing import Dict, List


def build_qa_refinement_prompt(query: str, draft_response: str, grounding_text: str) -> str:
    """Compose the second-stage prompt that enriches the summary-based draft with QA grounding."""
    return f"""
You already produced a draft answer based on summaries for the user query: {query}

Draft response:
{draft_response}

Additional Q&A pairs (may include Arabic) and contexts with finer-grained details:
{grounding_text}

Task: Enrich and refine the draft response by integrating the Q&A pairs according to these principles:

1. COHESIVENESS: Weave Q&A information seamlessly into existing sections rather than appending it. The information should build on or enhance previous facts and statements, adding depth and value. Use the Q&A contexts to create textual cohesion—not just additional surface data, but integrated insights that flow naturally with the draft.

2. COMPLETENESS: Use Q&A pairs to provide a more complete response that drives greater insight. Don't merely add more surface structure; deepen arguments and enhance analytical depth. Where summaries list key personnel or events, add any additional personnel or details surfaced by the Q&A, expanding sections with substantive new information.

3. ACCURACY: Use Q&A pairs to substantiate and justify previous statements by building more reliable information to support facts. When Q&A offers specific details, numbers, or clarifications, use them to strengthen the credibility of broader claims in the draft.

Citation Format: When citing Q&A pairs, use the format provided in the Citation field: (Author [qa-N], Month Year). For example: (Al-Arabiya [qa-1], Nov 2024). This differentiates Q&A sources from summary sources while maintaining consistency. When you create a citation don't add any hyperlink to the inline citation just render the in-line citation according to the guideline provided. Don't hyperlink under any circumstances

Even though we have citations in the document and a plan to add these citations, do not generate a citation list at the end. Use the plan to create in-line citations, but do not generate a citations list at the end of the response/output. This means any form of list: references, citations, bibliography, etc. should not be generated at the end of the response/output. Instead, only use in-line citations as per the format provided above."

Goal: Produce a refined response where Q&A details are seamlessly integrated throughout—enriching context, deepening analysis, and substantiating claims—not merely appended as additional information.
"""
