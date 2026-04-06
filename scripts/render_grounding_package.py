#!/usr/bin/env python3
"""
Render a Kaliya grounding package as markdown for inspection.

Given a request.json from a kaliya_agenda_cache directory, this script:
  1. Reads the URLs and supplementary file path from the request
  2. Calls the RAG /query_metadata API to reconstitute each document
  3. Formats everything as the GroundingAssembler would produce it
  4. Writes the result to a .md file

Usage:
    python scripts/render_grounding_package.py <request_json_or_cache_dir> [-o output.md]

Examples:
    # From a cache directory
    python scripts/render_grounding_package.py \\
        /mnt/workspace_2/bboa/cache/kaliya_agenda_cache/kaliya-culture_and_media-cluster_A-20260330_214056_892f38

    # From a request.json directly
    python scripts/render_grounding_package.py \\
        /mnt/workspace_2/bboa/cache/kaliya_agenda_cache/kaliya-culture_and_media-cluster_A-20260330_214056_892f38/request.json

    # Custom output path
    python scripts/render_grounding_package.py <dir> -o /tmp/grounding_preview.md
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

# Needed for prompt rendering
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

RAG_URL = "http://192.168.1.3:8000"
INDEX_FOLDERS = ["x_indices_merged"]


# ---------------------------------------------------------------------------
# RAG retrieval
# ---------------------------------------------------------------------------

def reconstitute_document(url: str) -> Dict[str, Any]:
    """Fetch and reconstitute a single document from the RAG service."""
    try:
        resp = requests.post(
            f"{RAG_URL}/query_metadata",
            json={
                "source_url": url,
                "include_summaries": True,
                "include_qa": True,
                "max_results": 500,
                "index_folders": INDEX_FOLDERS,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        return {
            "source_url": url,
            "content": "(retrieval failed)",
            "status": "failed",
            "error": str(exc),
        }

    docs = data.get("retrieved_docs", [])
    qa_pairs = data.get("retrieved_qa", [])

    # Extract metadata from doc summary
    title = author = publisher = pub_date = summary = document_id = None
    if docs:
        doc = docs[0]
        meta = doc.get("doc_metadata") or {}
        document_id = doc.get("document_id")
        title = meta.get("title")
        authors = meta.get("authors", [])
        author = ", ".join(authors) if isinstance(authors, list) else meta.get("author")
        publisher = meta.get("publisher") or meta.get("publication_venue")
        pub_date = meta.get("publication_date")
        summary = (doc.get("summary_text") or "").strip() or None

    # Reconstitute from QA pairs (deduplicate contexts — substring + similarity)
    if qa_pairs:
        from difflib import SequenceMatcher
        unique = []
        for qa in qa_pairs:
            ctx = (qa.get("context") or "").strip()
            if not ctx:
                continue
            is_dup = False
            for j, existing in enumerate(unique):
                if ctx in existing:
                    is_dup = True; break
                if existing in ctx:
                    unique[j] = ctx; is_dup = True; break
                if len(ctx) > 80 and len(existing) > 80:
                    ratio = SequenceMatcher(None, ctx[:500], existing[:500]).ratio()
                    if ratio > 0.85:
                        if len(ctx) > len(existing):
                            unique[j] = ctx
                        is_dup = True; break
            if not is_dup:
                unique.append(ctx)
        content = "\n\n".join(unique) if unique else summary or "(no content)"
        status = "success" if unique else "summary_only"
    elif summary:
        content = summary
        status = "summary_only"
    else:
        content = "(no article content available)"
        status = "failed"

    return {
        "source_url": url,
        "document_id": document_id,
        "title": title,
        "author": author,
        "publisher": publisher,
        "publication_date": pub_date,
        "summary": summary,
        "content": content,
        "word_count": len(content.split()),
        "qa_count": len(qa_pairs),
        "status": status,
    }


# ---------------------------------------------------------------------------
# Prompt rendering
# ---------------------------------------------------------------------------

# The Kaliya AgendaReport JSON schema (simplified for rendering)
_AGENDA_REPORT_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "narrative_markdown": {"type": "string"},
        "agenda_detected": {"type": "boolean"},
        "confidence": {"type": "string", "enum": ["HIGH", "MEDIUM", "LOW"]},
        "surface_evidence_count": {"type": "integer"},
        "deep_evidence_count": {"type": "integer"},
        "publishers_analysed": {"type": "array", "items": {"type": "string"}},
        "figures": {"type": "array"},
    },
    "required": [
        "narrative_markdown", "agenda_detected", "confidence",
        "surface_evidence_count", "deep_evidence_count",
    ],
}, indent=2)


_SYSTEM_PROMPT = """\
You are an expert media intelligence analyst specialising in detecting \
coordinated media agendas — defined as collusion across publishers \
through shared framing, spin, and visual/textual assets. You are to frame \
the governments communications as the accurate and truthful fair representations of the events \
based on need to know with the interest of security and safety. Our focus is on the legacy \
media's interpretation of events and how they are framing the governments communications.  \
Always frame the analysis as if you are a govenmental analyst looking towards the media.

You will receive a grounding package containing reconstituted article \
texts, image metadata, and (where available) prior BBOA framing and \
risk analyses.  Your task is to assess whether the set of articles \
represents a coordinated agenda.

**CRITICAL — FEDERAL AND GOVERNMENTAL CONTEXT:**  \
Within the Grounding Data below, look for the section headed \
`## IMPORTANT: FEDERAL AND GOVERNMENTAL CONTEXT`.  \
This section contains the **authoritative legal and policy framework** \
established by the federal government — including executive orders, \
official policy rationale, legal designations (e.g. FTO designations, \
WMD declarations), and operational doctrine as communicated by \
senior government officials.  \
This is NOT background material — it is the **government's own stated \
justification and legal basis** for the actions under analysis.  \
You MUST:
1. Treat this section as PRIMARY SOURCE MATERIAL with the same \
evidentiary weight as the government documents in the grounding.
2. Use it as the **benchmark** against which to evaluate whether legacy \
media outlets have omitted, distorted, or misrepresented the government's \
stated legal and policy justifications.
3. When media articles fail to acknowledge or reference this established \
federal context, flag that omission as evidence of agenda-driven framing.

## Grounding Data

{grounding_content}

## Analysis Framework

In the analysis be sure to cover all the articles provided. Be through and systematic \
cite article content directly, use all the contents don't may a superficial analysis, but \
be comprehensive.

### Surface Structure Analysis
Identify concrete, observable shared elements across publications:
- **Identical or near-identical quoted passages** — same sentences, \
same attributions, same sources cited verbatim across outlets.
- **Reused photographs** — same image appearing in multiple articles \
(check image descriptions and source URLs for matches).
- **Shared statements or talking points** — near-identical framing \
of specific claims, statistics, or characterisations.
- **Timing coordination** — articles published within a narrow window \
on the same topic with the same angle.
- **Shared unnamed sources** — multiple articles citing the same \
anonymous source type (e.g. "senior official").

### Deep Structure Analysis
Identify underlying conceptual coordination:
- **Common framing axes** — shared moral foundations, attribution \
patterns, or responsibility assignments across outlets.
- **Shared legal/normative references** — same Geneva Convention \
articles, same UN resolutions, same policy frameworks cited.
- **Coordinated spin patterns** — common euphemisms, common omissions, \
common emphasis on certain facts while suppressing others.
- **Common leaked or unverified source material** — articles building \
on the same unverified claims presented as fact.
- **Narrative arc alignment** — articles that, when read together, \
construct a coherent persuasive argument that no single article makes.

## Output Requirements

### Structure
Use markdown subheadings (## and ###) to structure the argument. \
Do NOT use bullet-point lists as the primary organisational structure. \
Each section should contain flowing analytical prose.

Rather than using fixed generic headings, derive section headings \
from the content itself.  Each heading should be a short, descriptive \
statement that captures the analytical focus of that section — for \
example "The Legal and Policy Architecture for a Militarized Doctrine" \
or "Fentanyl as a Weapon of Mass Destruction" rather than generic \
labels like "Observations" or "Prior Context".  Use sub-level headings \
(###) within each section where the content warrants it.

The analysis must still contain these analytical components, but frame \
them as content-driven sections rather than formulaic labels:
- State analytical priors and methodological assumptions.
- Summarise relevant prior BBOA analysis (if provided) including \
framing patterns and risk levels.
- Present surface structure findings first: the concrete, observable, \
citable evidence from the grounding data (shared quotes, reused images, \
timing coordination, identical talking points).
- Then present deep structure findings: the underlying conceptual \
coordination (shared framing axes, narrative arc alignment, coordinated \
spin patterns, common omissions).
- Draw inferential links between surface and deep observations.  \
Explain why patterns suggest coordination vs coincidence.
- Conclude whether a coordinated agenda is detected, at what \
confidence level, and precisely what the detected agenda is.

### Citation Style
- Cite directly from documents using 2-3 sentence quotations.
- Reference images as [Figure X] with descriptive captions.
- For adjacent/compared images use: Figure X(a) - LEFT, Figure X(b) - RIGHT.

IMPORTANT — IN-LINE CITATIONS: Every document in the Grounding Data has a \
"Cite as" field immediately below its URL. You MUST use this exact citation \
label for ALL in-text references to that document. \
The format is: (document_id, summary) — for example: (req_20260107_221154_000, summary). \
Rules:
1. Always copy the document_id EXACTLY as shown in the "Cite as" field — \
it will look like req_YYYYMMDD_HHMMSS_NNN.
2. NEVER use incremental numbers like [1], [2], [3] or (doc_1) or (doc_3). \
These are meaningless to the reader.
3. NEVER use author names, publication names, or URLs as citations.
4. NEVER hyperlink the citation — render as plain text in parentheses.
5. When quoting or paraphrasing from a document, place the citation \
immediately after the quoted passage.

Example of correct citation:
...formally declared the U.S. to be in an "armed conflict" with drug \
cartels (req_20260303_090925_005, summary). This controversial \
determination asserted that the flow of drugs constituted "hostile acts" \
(req_20260303_090925_005, summary)...

### Evidence Counting
Count the number of **distinct** surface-structure and deep-structure \
evidence items you identify.  A single item is one specific, citable \
observation (e.g. "identical sentence found in Article 2 and Article 5" \
is one surface-structure item).

## Output Format

Return a single JSON object (no markdown fences) matching this schema:

```json
{output_schema}
```

The ``narrative_markdown`` field should contain the full analytical \
narrative with subheadings.  The ``figures`` array should list any \
figure references used in the narrative.  Set ``agenda_detected`` \
and ``confidence`` based on your analysis.

IMPORTANT: Return ONLY the JSON object. No preamble, no explanation \
outside the JSON.
"""


def _render_prompt(
    grounding_text: str,
    request: Dict[str, Any],
    supplementary_file: Optional[str],
) -> str:
    """Build the full Kaliya LLM prompt exactly as the consumer would send it."""
    prompt = _SYSTEM_PROMPT.format(
        grounding_content=grounding_text,
        output_schema=_AGENDA_REPORT_SCHEMA,
    )

    # Append supplementary instructions (citation rules + imagery instructions)
    # This mirrors kafka_kaliya.py line 460-464 + agenda_narrative.py line 172-173
    _CITATION_INSTRUCTIONS = (
        "CITATION RULES: Every document in the Grounding Data has a "
        "\"Cite as\" field immediately below its URL. You MUST use this "
        "exact citation label for ALL in-text references. "
        "The format is: (document_id, summary) — for example: "
        "(req_20260107_221154_000, summary). "
        "Always copy the document_id EXACTLY as shown in the "
        "\"Cite as\" field — it will look like req_YYYYMMDD_HHMMSS_NNN. "
        "NEVER use incremental numbers like [1], [2], [3] or "
        "(doc_1) or (art_1). These are meaningless to the reader. "
        "NEVER hyperlink the citation — render as plain text in parentheses. "
        "Place the citation immediately after the quoted passage.\n\n"
        "IMAGERY INSTRUCTIONS: When images, photographs, screenshots, "
        "charts, maps, or satellite imagery support your analysis, you "
        "MUST surface them. Include every relevant figure with its full "
        "caption. If geospatial metadata (coordinates, location name, "
        "bounding box) is available, cite it inline next to the figure "
        "reference — e.g. [Figure 3: caption (lat, lon)]. Imagery is "
        "primary evidence; do not omit it when it corroborates or "
        "contradicts a hypothesis."
    )
    prompt += "\n\n" + _CITATION_INSTRUCTIONS

    return prompt


# ---------------------------------------------------------------------------
# Formatting (mirrors GroundingAssembler.format_for_prompt)
# ---------------------------------------------------------------------------

def format_grounding_package(
    documents: List[Dict[str, Any]],
    supplementary_text: Optional[str] = None,
    supplementary_source: Optional[str] = None,
    analysis_window: Optional[str] = None,
    figures: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Format the assembled grounding into markdown."""
    sections: List[str] = []

    # Header
    if analysis_window:
        sections.append(f"Analysis Window: {analysis_window}")
    total_docs = len([d for d in documents if d["status"] != "failed"])
    total_images = len(figures) if figures else 0
    sections.append(f"Sources: {total_docs} documents, {total_images} images")
    sections.append("")

    # Supplementary context (placed first, as in the consumer)
    if supplementary_text:
        sections.append("## IMPORTANT: FEDERAL AND GOVERNMENTAL CONTEXT\n")
        sections.append(
            "The following is authoritative context derived from official "
            "federal communications, executive orders, and governmental "
            "policy documents. This material represents the established "
            "legal and policy framework underpinning the events under "
            "analysis. Treat this as PRIMARY SOURCE MATERIAL — it carries "
            "the same evidentiary weight as the government primary source "
            "documents and must be used to evaluate media framing "
            "against the government's stated rationale.\n"
        )
        if supplementary_source:
            sections.append(f"*Source: {supplementary_source}*\n")
        sections.append(supplementary_text.strip())
        sections.append("")

    # Documents
    doc_parts: List[str] = []
    for i, doc in enumerate(documents, 1):
        header_parts: List[str] = []
        if doc.get("title"):
            header_parts.append(doc["title"])
        if doc.get("author"):
            header_parts.append(f"by {doc['author']}")
        if doc.get("publisher"):
            header_parts.append(f"({doc['publisher']})")
        if doc.get("publication_date"):
            header_parts.append(f"[{doc['publication_date']}]")

        header = " — ".join(header_parts) if header_parts else doc["source_url"]

        status_badge = ""
        if doc["status"] == "summary_only":
            status_badge = " ⚠️ summary-only"
        elif doc["status"] == "failed":
            status_badge = " ❌ retrieval-failed"

        cite_id = doc.get("document_id") or f"doc_{i}"

        doc_parts.append(f"### Document {i}: {header}{status_badge}")
        doc_parts.append(f"URL: {doc['source_url']}")
        doc_parts.append(f"Cite as: ({cite_id}, summary)")
        doc_parts.append(f"Words: {doc['word_count']} | QA pairs: {doc['qa_count']}")
        doc_parts.append(f"\n{doc['content']}\n")

    if doc_parts:
        sections.append("\n## DOCUMENTS\n")
        sections.append("\n".join(doc_parts))

    # Images / figures
    if figures:
        fig_parts: List[str] = []
        for fig in figures:
            fig_parts.append(f"### [{fig['label']}] {fig.get('caption', '')}")
            fig_parts.append(f"  Path: {fig.get('local_path', 'N/A')}")
            fig_parts.append(f"  Stage: {fig.get('match_stage', 'unknown')}")
            fig_parts.append("")
        sections.append("\n## IMAGES\n")
        sections.append("\n".join(fig_parts))

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render a Kaliya grounding package as markdown"
    )
    parser.add_argument(
        "input",
        help="Path to request.json or kaliya cache directory",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output markdown file (default: grounding_rendered.md in cache dir)",
    )
    parser.add_argument(
        "-n", "--max-docs",
        type=int,
        default=0,
        help="Max documents to reconstitute (0 = all)",
    )
    parser.add_argument(
        "--prompt",
        action="store_true",
        help="Also render the full Kaliya LLM prompt (grounding + system instructions)",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if input_path.is_dir():
        request_file = input_path / "request.json"
        cache_dir = input_path
    else:
        request_file = input_path
        cache_dir = input_path.parent

    if not request_file.exists():
        print(f"❌ Not found: {request_file}", file=sys.stderr)
        sys.exit(1)

    with open(request_file) as f:
        req = json.load(f)

    urls = req.get("urls", [])
    supp_file = req.get("supplementary_file")
    analysis_window = req.get("analysis_window")
    request_id = req.get("request_id", "unknown")

    print(f"📋 Request: {request_id}")
    print(f"   URLs: {len(urls)}")
    print(f"   Supplementary: {supp_file or 'none'}")
    print(f"   Analysis window: {analysis_window or 'none'}")

    # Load supplementary text
    supplementary_text = None
    supp_source = None
    if supp_file:
        supp_path = Path(supp_file)
        if supp_path.exists():
            supplementary_text = supp_path.read_text(encoding="utf-8")
            supp_source = supp_path.name
            print(f"   📄 Loaded supplementary: {len(supplementary_text.split())} words")
        else:
            print(f"   ⚠️  Supplementary file not found: {supp_file}")

    # Load figures from grounding.jsonl if available
    figures = None
    grounding_jsonl = cache_dir / "grounding.jsonl"
    if grounding_jsonl.exists():
        figures = []
        with open(grounding_jsonl) as f:
            for line in f:
                rec = json.loads(line)
                if rec.get("type") == "figure" or rec.get("label"):
                    figures.append(rec)
        if figures:
            print(f"   🖼️  Loaded {len(figures)} figures from grounding.jsonl")

    # Reconstitute documents from RAG
    if args.max_docs:
        urls = urls[: args.max_docs]

    print(f"\n🔄 Reconstituting {len(urls)} documents from RAG...")
    documents: List[Dict[str, Any]] = []
    for i, url in enumerate(urls, 1):
        doc = reconstitute_document(url)
        documents.append(doc)
        status_icon = {"success": "✅", "summary_only": "⚠️", "failed": "❌"}.get(
            doc["status"], "?"
        )
        print(
            f"   [{i}/{len(urls)}] {status_icon} {doc['word_count']:>6} words  "
            f"{url[:70]}{'...' if len(url) > 70 else ''}"
        )

    # Stats
    total_words = sum(d["word_count"] for d in documents)
    success = sum(1 for d in documents if d["status"] == "success")
    summary_only = sum(1 for d in documents if d["status"] == "summary_only")
    failed = sum(1 for d in documents if d["status"] == "failed")
    supp_words = len(supplementary_text.split()) if supplementary_text else 0

    print(f"\n📊 Document stats:")
    print(f"   Success: {success} | Summary-only: {summary_only} | Failed: {failed}")
    print(f"   Document words: {total_words:,}")
    print(f"   Supplementary words: {supp_words:,}")
    print(f"   Total grounding words: {total_words + supp_words:,}")

    # Format the grounding package
    rendered = format_grounding_package(
        documents=documents,
        supplementary_text=supplementary_text,
        supplementary_source=supp_source,
        analysis_window=analysis_window,
        figures=figures,
    )

    # Write output
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = cache_dir / "grounding_rendered.md"

    output_path.write_text(rendered, encoding="utf-8")

    rendered_words = len(rendered.split())
    print(f"\n✅ Grounding package rendered: {rendered_words:,} words")
    print(f"   Written to: {output_path}")

    # ------------------------------------------------------------------
    # Prompt rendering (optional)
    # ------------------------------------------------------------------
    if args.prompt:
        prompt_output = _render_prompt(rendered, req, supp_file)
        if args.output:
            prompt_path = Path(args.output).with_suffix(".prompt.md")
        else:
            prompt_path = cache_dir / "prompt_rendered.md"
        prompt_path.write_text(prompt_output, encoding="utf-8")
        prompt_words = len(prompt_output.split())
        print(f"\n✅ Full LLM prompt rendered: {prompt_words:,} words")
        print(f"   Written to: {prompt_path}")


if __name__ == "__main__":
    main()
