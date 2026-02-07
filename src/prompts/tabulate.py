"""QA refinement prompt helpers."""

from datetime import datetime
from typing import Dict, List


def build_tabulate_refinement_prompt(query: str, draft_response: str, grounding_text: str) -> str:
    """Compose the second-stage prompt that enriches the summary-based draft with QA grounding."""
    return f"""
You already produced a draft answer based on summaries for the user query: {query}

Draft response:
{draft_response}

Additional Q&A pairs (may include Arabic) and contexts with finer-grained details:
{grounding_text}

Task:

Goal: Produce a refined response where tables details are seamlessly integrated throughout—enriching context, deepening analysis, and substantiating claims—not merely appended as additional information.
"""
