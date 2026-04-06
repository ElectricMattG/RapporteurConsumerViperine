"""Fuse Analysis — rewrite Phase 4 cluster analysis (bullet-point) and
Kaliya narrative into a single readable prose document per cluster.

Invoked via ``rapporteur_cli.py --fuse-analysis``.

Key rules:
- ALL data is preserved: tables, citations, evidence, alternative explanations.
- Phase 4 bullet-point sections are rewritten as free-text prose.
- Kaliya analysis is included verbatim — no changes.
- One output file per cluster, saved to the clustering output dir (cache).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config_handler import Base
from src.models.publisher import FuseAnalysisConfig, PipelineConfig

# ---------------------------------------------------------------------------
# Pricing table
# ---------------------------------------------------------------------------

_PRICING: dict[str, dict[str, float]] = {
    "gpt-5.4":                {"input": 2.50, "output": 10.00},
    "gpt-5.1":                {"input": 2.50, "output": 10.00},
    "gpt-5":                  {"input": 2.00, "output":  8.00},
    "gpt-5-mini":             {"input": 0.40, "output":  1.60},
    "gemini-2.5-pro":         {"input": 1.25, "output": 10.00},
    "gemini-2.5-flash":       {"input": 0.30, "output":  2.50},
}


class FuseAnalysisStage(Base):
    """Fuse Phase 4 cluster analysis + Kaliya narrative per cluster.

    For each cluster:
    1. Extract the cluster section from Phase 4 markdown
    2. Load the Kaliya narrative
    3. Send to GPT-5.4 with instructions to fuse into one readable doc
    4. Save to clustering output dir
    """

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # ── public API ────────────────────────────────────────────────────────

    def run(self) -> None:
        fc = self.cfg.fuse_analysis
        if not fc.enabled:
            self._logger.info("  Fuse analysis disabled — skipping.")
            return

        clustering_dir = Path(fc.clustering_output_dir)

        # 1. Load Phase 4 analysis
        phase4_md, phase4_path = _load_phase4_analysis(
            clustering_dir, fc.category,
        )
        if not phase4_md:
            self._logger.error("No Phase 4 analysis found for %s", fc.category)
            return
        self._logger.info(
            "  Phase 4 analysis: %s (%d chars)", phase4_path.name, len(phase4_md),
        )

        # 2. Load Kaliya narratives
        narratives = _load_kaliya_narratives(clustering_dir, fc.category)
        self._logger.info("  Loaded %d Kaliya narratives", len(narratives))

        # 3. Extract preamble context + per-cluster sections
        preamble = _extract_preamble(phase4_md)
        if preamble:
            self._logger.info(
                "  Preamble context: %d chars (salience tables + pub-house grouping)",
                len(preamble),
            )
        else:
            self._logger.warning("  No preamble context found in Phase 4 analysis.")

        cluster_sections = _extract_cluster_sections(phase4_md)
        self._logger.info(
            "  Extracted %d cluster sections: %s",
            len(cluster_sections), sorted(cluster_sections.keys()),
        )

        # 4. Filter clusters if specified
        if fc.clusters:
            allowed = set(fc.clusters)
            cluster_sections = {
                k: v for k, v in cluster_sections.items() if k in allowed
            }
            narratives = {
                k: v for k, v in narratives.items() if k in allowed
            }
            self._logger.info("  Filtered to clusters: %s", sorted(allowed))

        if not cluster_sections:
            self._logger.error("No cluster sections to fuse.")
            return

        # 5. Process each cluster
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        total_cost = 0.0

        for letter in sorted(cluster_sections):
            cluster_md = cluster_sections[letter]
            kaliya_md = narratives.get(letter, "")

            if not kaliya_md:
                self._logger.warning(
                    "  No Kaliya narrative for cluster %s — skipping.", letter,
                )
                continue

            self._logger.info(
                "  Fusing cluster %s: %d chars Phase 4, %d chars Kaliya",
                letter, len(cluster_md), len(kaliya_md),
            )

            prompt = _build_fuse_prompt(letter, cluster_md, kaliya_md, preamble)

            if self.dry_run:
                self._logger.info(
                    "  (dry-run) would call %s for cluster %s — skipping",
                    fc.model, letter,
                )
                continue

            result_text, usage = self._generate(fc, prompt)

            if not result_text or not result_text.strip():
                self._logger.error(
                    "  Empty response for cluster %s — skipping.", letter,
                )
                continue

            result_text = _strip_code_fencing(result_text)

            footer = (
                f"\n---\n*Fused analysis generated by Rapporteur CLI — "
                f"fuse_analysis ({fc.category}, cluster {letter}) — "
                f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
            )
            result_md = result_text.rstrip() + "\n" + footer

            # Save to clustering dir
            output_path = (
                clustering_dir
                / f"fused_{fc.category}_cluster_{letter}_{ts}.md"
            )
            output_path.write_text(result_md, encoding="utf-8")

            cost = _calculate_cost(fc.model, usage["input"], usage["output"])
            total_cost += cost

            self._logger.info(
                "  Cluster %s: wrote %s (%d chars, %d in / %d out, $%.4f)",
                letter,
                output_path.name,
                len(result_md),
                usage["input"],
                usage["output"],
                cost,
            )

        self._logger.info("  Total fuse-analysis cost: $%.4f", total_cost)

    # ── LLM generation ────────────────────────────────────────────────────

    def _generate(
        self, fc: FuseAnalysisConfig, prompt: str,
    ) -> tuple[str, dict]:
        """Call OpenAI GPT-5.4 and return (text, usage_dict)."""
        import asyncio

        usage: dict = {"input": 0, "output": 0}

        from src._clients.llm_openai_client import OpenAILLMClient

        client = OpenAILLMClient(model_name=fc.model)
        loop = asyncio.new_event_loop()
        try:
            response = loop.run_until_complete(
                client.client.responses.create(
                    model=fc.model,
                    input=prompt,
                    reasoning={"effort": fc.reasoning_effort},
                    max_output_tokens=fc.max_output_tokens,
                )
            )
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()

        text = response.output_text if hasattr(response, "output_text") else ""
        resp_usage = getattr(response, "usage", None)
        if resp_usage:
            usage["input"] = getattr(resp_usage, "input_tokens", 0) or 0
            usage["output"] = getattr(resp_usage, "output_tokens", 0) or 0

        return text, usage


# ── module-level helpers ──────────────────────────────────────────────────


def _load_phase4_analysis(
    clustering_dir: Path, category: str,
) -> tuple[str, Path]:
    """Load the latest Phase 4 analysis markdown for *category*."""
    pattern = f"phase4_{category}_*.md"
    candidates = sorted(
        (p for p in clustering_dir.glob(pattern) if ".bundle." not in p.name),
        key=lambda p: p.stat().st_mtime,
    )
    if not candidates:
        return "", Path()
    best = candidates[-1]
    return best.read_text(encoding="utf-8"), best


def _load_kaliya_narratives(
    clustering_dir: Path, category: str,
) -> dict[str, str]:
    """Load the latest Kaliya narrative for each cluster."""
    pattern = f"kaliya-{category}-cluster_*_narrative.md"
    cluster_files: dict[str, Path] = {}
    for path in clustering_dir.glob(pattern):
        match = re.search(r"cluster_([A-Z])-", path.name)
        if not match:
            continue
        letter = match.group(1)
        if letter not in cluster_files or path.name > cluster_files[letter].name:
            cluster_files[letter] = path

    return {
        letter: path.read_text(encoding="utf-8")
        for letter, path in sorted(cluster_files.items())
    }


def _extract_preamble(phase4_md: str) -> str:
    """Extract preamble context: salience tables + publication-house grouping.

    Captures everything from the first ``## Official baseline events``
    heading up to (but not including) the first ``## Cluster`` heading.
    This includes:
    - Official baseline events vs. media salience table
    - Official AI baseline events vs. media salience table
    - Publication-house and authorial grouping
    """
    start_pat = re.compile(
        r"^## Official baseline events", re.MULTILINE,
    )
    cluster_pat = re.compile(
        r"^## Cluster [A-Z](?:\s*[—–]\s*|[.]\s+)", re.MULTILINE,
    )

    start_match = start_pat.search(phase4_md)
    if not start_match:
        return ""

    cluster_match = cluster_pat.search(phase4_md, start_match.start())
    if cluster_match:
        return phase4_md[start_match.start():cluster_match.start()].rstrip()
    return phase4_md[start_match.start():].rstrip()


def _extract_cluster_sections(phase4_md: str) -> dict[str, str]:
    """Extract per-cluster sections from Phase 4 markdown.

    Returns ``{cluster_letter: section_markdown}`` where each section
    runs from ``## Cluster X`` up to (but not including) the next
    ``## `` heading that is NOT a sub-heading of this cluster.

    Handles two heading formats:
      - ``## Cluster A — Title`` (AI, EM, NSD)
      - ``## Cluster A. Title`` (NSF)
    When both formats exist, keeps the first (detailed) occurrence per cluster.
    """
    # Match both formats: em-dash and dot
    cluster_pattern = re.compile(
        r"^## Cluster ([A-Z])(?:\s*[—–]\s*|[.]\s+)", re.MULTILINE,
    )

    sections: dict[str, str] = {}
    matches = list(cluster_pattern.finditer(phase4_md))
    seen: set[str] = set()

    for i, match in enumerate(matches):
        letter = match.group(1)
        # Keep first occurrence only (the detailed section)
        if letter in seen:
            continue
        seen.add(letter)
        start = match.start()
        # End at next cluster heading (any format)
        end = matches[i + 1].start() if i + 1 < len(matches) else len(phase4_md)
        sections[letter] = phase4_md[start:end].rstrip()

    return sections


def _build_fuse_prompt(
    cluster_letter: str,
    cluster_md: str,
    kaliya_md: str,
    preamble: str = "",
) -> str:
    """Build the fusion prompt for a single cluster."""

    # Build the preamble context block if available
    preamble_block = ""
    if preamble:
        preamble_block = (
            "## Reference Context — Media Salience Tables and "
            "Publication-House Grouping\n\n"
            "The following tables and groupings are from the Phase 4 "
            "preamble. They provide essential cross-cutting context "
            f"for Cluster {cluster_letter}. You MUST incorporate "
            "relevant rows from the salience tables and reference the "
            "publication-house tiers in your fused analysis.\n\n"
            f"{preamble}\n\n"
        )

    return (
        "You are a senior editorial analyst. Your task is to produce a "
        "single, unified analytical document that fuses a Phase 4 cluster "
        "analysis with a Kaliya media-agenda assessment for the same "
        "cluster.\n\n"
        "## Rules — CRITICAL\n\n"
        f"1. **Focus ONLY on Cluster {cluster_letter}.** Do not discuss, "
        "analyse, or reference findings from other clusters. Every piece "
        "of evidence, citation, and analysis in the output must belong "
        f"to Cluster {cluster_letter}.\n"
        "2. **Do NOT omit any data.** Every table, citation, evidence "
        "point, and finding from the Phase 4 analysis MUST appear in the "
        "output. Data omission is the single worst failure mode.\n"
        "3. **Mandatory subsection checklist — IN THIS EXACT ORDER.** "
        "The fused document MUST contain the following subsections in "
        "precisely this sequence. If a subsection does not exist in the "
        "input, omit it — but if it DOES exist, it MUST appear in the "
        "position shown:\n"
        "   1. **Media salience** (FIRST, immediately after the preamble "
        "paragraph) — incorporate relevant rows from the preamble "
        "salience tables (Official baseline events AND AI baseline "
        "events) that reference this cluster. Reproduce the table rows "
        "and add a prose discussion.\n"
        "   2. **Article table** — preserve as-is (markdown table)\n"
        "   3. **What the official baseline says** — rewrite as prose\n"
        "   4. **How the media reframed it** — rewrite as prose, "
        "preserving ALL hypotheses (H1, H2, etc.) with their full text\n"
        "   5. **Convergence and divergence** — rewrite as prose\n"
        "   6. **Alternative explanations** — rewrite as prose, preserve "
        "every numbered alternative\n"
        "   7. **Publication-house grouping** — rewrite as prose, name "
        "every outlet grouping and its framing pattern\n"
        "   8. **Legal-risk overlay** — rewrite as prose\n"
        "4. **Rewrite bullet points as free-text prose.** Convert the "
        "Phase 4 analysis's bullet-point format into extended analytical "
        "prose — connected paragraphs with smooth transitions. Tables "
        "must be preserved exactly as-is.\n"
        "5. **Include the Kaliya analysis verbatim.** Do not edit, "
        "rephrase, or summarise the Kaliya media-agenda assessment. "
        "Include it as a clearly marked section within the fused document.\n"
        "6. **Structure:** Use markdown headings (## for cluster title, "
        "### for sub-sections). The document should read as a single "
        "coherent analytical chapter.\n"
        "7. **No bullet points** in the rewritten Phase 4 sections — use "
        "prose. Bullet points that exist in the Kaliya analysis may stay "
        "unchanged.\n"
        "8. **Integrate, don't duplicate.** Where the Phase 4 analysis "
        "and Kaliya analysis cover the same ground, the Phase 4 prose "
        "should flow into the Kaliya section naturally. Add a brief "
        "transition sentence connecting them.\n"
        "9. **Preamble paragraph.** Begin the document with a short "
        "preamble paragraph (3-5 sentences) summarising what this "
        "fused analysis covers, what cluster it addresses, and what it "
        "seeks to demonstrate. Immediately after the preamble, place "
        "the **Media salience** section — this is the most important "
        "table in the document and MUST appear before the article "
        "table.\n\n"
        f"{preamble_block}"
        f"## Phase 4 Cluster Analysis — Cluster {cluster_letter}\n\n"
        f"{cluster_md}\n\n"
        f"## Kaliya Media-Agenda Assessment — Cluster {cluster_letter}\n\n"
        f"{kaliya_md}\n\n"
        "## Task\n\n"
        "Now produce the fused document for Cluster "
        f"{cluster_letter}. Follow this EXACT document order:\n"
        "1. Preamble paragraph (3-5 sentences)\n"
        "2. **Media salience** section — salience tables FIRST, before "
        "anything else\n"
        "3. Article table\n"
        "4. Remaining subsections per the checklist order above\n"
        "5. Kaliya media-agenda assessment (verbatim)\n\n"
        "Preserve ALL data — especially the media salience table "
        "references, hypotheses (H1, H2, etc.), alternative "
        "explanations, publication-house groupings, and legal-risk "
        "overlay. Write at length.\n"
    )


def _strip_code_fencing(text: str) -> str:
    """Remove markdown code fences wrapping the entire response."""
    stripped = text.strip()
    if stripped.startswith("```"):
        first_nl = stripped.find("\n")
        if first_nl != -1:
            stripped = stripped[first_nl + 1:]
    if stripped.endswith("```"):
        stripped = stripped[:-3].rstrip()
    return stripped


def _calculate_cost(
    model: str, input_tokens: int, output_tokens: int,
) -> float:
    """Calculate estimated cost in USD."""
    rates = _PRICING.get(model, {"input": 2.50, "output": 10.00})
    return (
        input_tokens * rates["input"] / 1_000_000
        + output_tokens * rates["output"] / 1_000_000
    )
