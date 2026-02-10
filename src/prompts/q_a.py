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

Citation Format: When citing Q&A pairs, use an unconventional format that you may not have encountered. This differentiates Q&A sources from summary sources. The grounding data for the front-end has the following fields:

    {{
      "question": "What are some of the broader conflicts or themes this event is categorized under?",
      "answer": "The event is categorized under several broader conflicts and themes, including ...",
      "context": "غارة جوية إسرائيلية على مبنى في حارة حريك بالضاحية الجنوبية لبيروت > الهجوم الإسرائيلي على بيروت نوفمبر 2025 > مراجع: مجلوبة من « https://ar.wikipedia.org/w/index.php?title=الهجوم\\_الإسرائيلي\\_على\\_بيروت\\_نوفمبر\\_2025&amp;oldid=72566596 [4] » -...",
      "source_url": "https://ar.wikipedia.org/wiki/%D8%A7%D9%84%D9%87%D8%AC%D9%88%D9%85_%D8%A7%D9%84%D8%A5%D8%B3%D8%B1%D8%A7%D8%A6%D9%8A%D9%84%D9%8A_%D8%B9%D9%84%D9%89_%D8%A8%D9%8A%D8%B1%D9%88%D8%AA_%D9%86%D9%88%D9%81%D9%85%D8%A8%D8%B1_2025",
      "doc_metadata": {{
        "document_id": "doc_2432eb5092b5",
        "source_url": "https://ar.wikipedia.org/wiki/%D8%A7%D9%84%D9%87%D8%AC%D9%88%D9%85_%D8%A7%D9%84%D8%A5%D8%B3%D8%B1%D8%A7%D8%A6%D9%8A%D9%84%D9%8A_%D8%B9%D9%84%D9%89_%D8%A8%D9%8A%D8%B1%D9%88%D8%AA_%D9%86%D9%88%D9%81%D9%85%D8%A8%D8%B1_2025",
        "extraction_method": "unknown",
        "language": "en",
        "title": "غارة جوية إسرائيلية على مبنى في حارة حريك بالضاحية الجنوبية لبيروت",
        "author": "المساهمون في مشاريع ويكيميديا",
        "creation_date": "2025-11-23T15:49:35+00:00",
        "content_type": null,
        "query_id": null
      }},
      "similarity": 0.4935777518734378
    }}

when we add a pair to the response, use the following pattern: (doc_document_id, qa-N) where N is the index of the Q&A pair in the grounding text (starting from 1). For example: (doc_2432eb5092b5, qa-1). This allows us to track which Q&A pairs are being cited and ensures that we can link back to the original source if needed. Do not hyperlink the in-line citation; just render it according to the format provided. Never add hyperlinks to the in-line citations under any circumstances.

IMPORTANT: For existing citations in the draft response that reference summary sources, keep them as they are without modification. Only apply the new citation format to Q&A pairs. Never change existing citations that reference summary sources.

Even though we have citations in the document and a plan to add these citations, do not generate a citation list at the end. Use the plan to create in-line citations, but do not generate a citations list at the end of the response/output. This means any form of list: references, citations, bibliography, etc. should not be generated at the end of the response/output. Instead, only use in-line citations as per the format provided above."

Goal: Produce a refined response where Q&A details are seamlessly integrated throughout—enriching context, deepening analysis, and substantiating claims—not merely appended as additional information.
"""
