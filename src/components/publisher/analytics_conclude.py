"""Analytics Conclusion — integrate cluster analysis, Kaliya narratives,
EO policy summaries, and sampled media articles into an overarching
analytical conclusion about media response to EO implementation.

Invoked via ``rapporteur_cli.py --analytics-conclude``.
"""
from __future__ import annotations

import json
import random
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config_handler import Base
from src.models.publisher import AnalyticsConcludeConfig, PipelineConfig

# ---------------------------------------------------------------------------
# Pricing table (shared with ConcludeStage)
# ---------------------------------------------------------------------------

_PRICING: dict[str, dict[str, float]] = {
    "gpt-5.4":                {"input": 2.50, "output": 10.00},
    "gpt-5.1":                {"input": 2.50, "output": 10.00},
    "gpt-5":                  {"input": 2.00, "output":  8.00},
    "gpt-5-mini":             {"input": 0.40, "output":  1.60},
    "gemini-2.5-pro":         {"input": 1.25, "output": 10.00},
    "gemini-2.5-flash":       {"input": 0.30, "output":  2.50},
    "gemini-3.1-pro-preview": {"input": 1.25, "output": 10.00},
}


class AnalyticsConcludeStage(Base):
    """Integrate cluster analyses + Kaliya narratives into overarching
    analytical conclusions about media response to EO implementation.

    Assembles a token-budgeted grounding package from:
    - Phase 4 cluster analysis markdown
    - Per-cluster Kaliya narrative markdown
    - EO policy summaries
    - Randomly sampled reconstituted media articles from the bundle

    Calls OpenAI GPT-5.4 (default) with reasoning high to produce a
    structured markdown conclusion.  Saves the reasoning tokens,
    grounding package, and output markdown.
    """

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    # ── public API ────────────────────────────────────────────────────────

    def run(self) -> None:
        ac = self.cfg.analytics_conclude
        if not ac.enabled:
            self._logger.info("  Analytics conclusion disabled — skipping.")
            return

        clustering_dir = Path(ac.clustering_output_dir)
        output_dir = Path(ac.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Phase 4 cluster analysis
        phase4_md, phase4_path = self._load_phase4_analysis(
            clustering_dir, ac.category,
        )
        if not phase4_md:
            self._logger.error("No Phase 4 analysis found for %s", ac.category)
            return
        self._logger.info(
            "  Phase 4 analysis: %s (%d chars)", phase4_path.name, len(phase4_md),
        )

        # 2. Kaliya narratives
        narratives = self._load_kaliya_narratives(clustering_dir, ac.category)
        self._logger.info("  Loaded %d Kaliya narratives", len(narratives))

        # 3. EO policy summaries
        from src.components.analysis.media_linkage import CATEGORY_ANCHORS

        eo_numbers = CATEGORY_ANCHORS.get(ac.category, [])
        eo_summaries = self._load_eo_policy_summaries(
            eo_numbers, ac.eo_index_folder,
        )
        self._logger.info("  Loaded %d EO policy summaries", len(eo_summaries))

        # 4. Bundle articles
        bundle_path = self._find_bundle(clustering_dir, ac.category)
        if not bundle_path:
            bundle_path = self._find_bundle(
                Path.cwd() / "cache", ac.category,
            )
        articles = self._load_bundle(bundle_path) if bundle_path else []
        self._logger.info(
            "  Loaded %d bundle articles%s",
            len(articles),
            f" from {bundle_path.name}" if bundle_path else "",
        )

        # Resolve document_ids from the merged index for data lineage
        url_to_docid = self._build_url_to_docid(ac.index_folder)
        if articles and url_to_docid:
            self._inject_document_ids(articles, url_to_docid)
            resolved = sum(1 for a in articles if a.get("document_id"))
            self._logger.info(
                "  Resolved %d/%d article document_ids (req_*)",
                resolved, len(articles),
            )

        # 5. Cluster map from Phase 4
        cluster_map = self._parse_cluster_map(phase4_md)

        # Filter to requested clusters if specified
        if ac.clusters:
            allowed = set(ac.clusters)
            cluster_map = {k: v for k, v in cluster_map.items() if k in allowed}
            narratives = {k: v for k, v in narratives.items() if k in allowed}
            self._logger.info(
                "  Filtered to clusters: %s", sorted(allowed),
            )

        self._logger.info(
            "  Cluster map: %s",
            {k: len(v) for k, v in cluster_map.items()},
        )

        # 6. Token budgeting
        import tiktoken

        enc = tiktoken.encoding_for_model("gpt-4o")

        fixed_parts = self._build_fixed_grounding(
            phase4_md, narratives, eo_summaries, ac.category,
        )
        fixed_tokens = len(enc.encode(fixed_parts))
        self._logger.info("  Fixed grounding: %d tokens", fixed_tokens)

        prompt_overhead = 4_000  # system prompt + instructions
        article_budget = ac.target_tokens - fixed_tokens - prompt_overhead
        if article_budget < 10_000:
            self._logger.warning(
                "  Article budget very small (%d tokens) — "
                "only fixed grounding will be included", article_budget,
            )
            article_budget = 0

        # 7. Sample articles per cluster within budget
        sampled_text = ""
        sampled_count = 0
        if article_budget > 0 and articles and cluster_map:
            sampled_text, sampled_count = self._sample_articles(
                articles, cluster_map, article_budget, enc,
            )
            self._logger.info(
                "  Sampled %d articles (%d tokens)",
                sampled_count, len(enc.encode(sampled_text)),
            )

        # 8. Assemble full grounding package
        grounding_package = fixed_parts
        if sampled_text:
            grounding_package += (
                "\n\n# PART 4 — SAMPLED MEDIA ARTICLES (EVIDENCE BASE)\n\n"
                "The following articles are a representative random sample "
                "from across all clusters. Use these as primary evidence "
                "for citations and direct quotation.\n\n"
                + sampled_text
            )

        total_tokens = len(enc.encode(grounding_package))
        self._logger.info("  Total grounding package: %d tokens", total_tokens)

        # 9. Build prompt
        prompt = self._build_analytics_prompt(grounding_package)

        # 10. Save artefacts
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        prefix = f"analytics_conclude_{ac.category}_{ts}"

        grounding_path = output_dir / f"{prefix}_grounding.md"
        grounding_path.write_text(grounding_package, encoding="utf-8")
        self._logger.info("  Saved grounding: %s", grounding_path.name)

        prompt_path = output_dir / f"{prefix}_prompt.md"
        prompt_path.write_text(prompt, encoding="utf-8")
        self._logger.info(
            "  Saved prompt: %s (%d chars)", prompt_path.name, len(prompt),
        )

        if self.dry_run:
            self._logger.info(
                "  (dry-run) would call %s — skipping LLM", ac.model,
            )
            return

        # 11. Call LLM
        self._logger.info(
            "  Calling %s (provider=%s, reasoning=%s)…",
            ac.model, ac.provider, ac.reasoning_effort,
        )
        result_text, usage = self._generate(ac, prompt)

        if not result_text or not result_text.strip():
            self._logger.error("  Empty response from LLM — aborting.")
            return

        result_text = _strip_code_fencing(result_text)

        footer = (
            f"\n---\n*Generated by Rapporteur CLI — analytics_conclude "
            f"({ac.category}) — "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}*\n"
        )
        result_md = result_text.rstrip() + "\n" + footer

        # 12. Write outputs
        output_path = output_dir / f"{prefix}.md"
        output_path.write_text(result_md, encoding="utf-8")
        self._logger.info(
            "  Wrote conclusion: %s (%d chars)", output_path.name, len(result_md),
        )

        cost = _calculate_cost(ac.model, usage["input"], usage["output"])
        usage_data = {
            "model": ac.model,
            "provider": ac.provider,
            "category": ac.category,
            "timestamp": ts,
            "usage": {
                "input_tokens": usage["input"],
                "output_tokens": usage["output"],
                "reasoning_tokens": usage["thinking"],
                "total_tokens": usage["input"] + usage["output"],
                "estimated_cost_usd": round(cost, 4),
            },
            "grounding_package_tokens": total_tokens,
            "articles_sampled": sampled_count,
            "articles_total": len(articles),
        }
        usage_path = output_dir / f"{prefix}_usage.json"
        usage_path.write_text(
            json.dumps(usage_data, indent=2) + "\n", encoding="utf-8",
        )

        # Save reasoning content if available
        reasoning_text = usage.get("reasoning_text", "")
        if reasoning_text:
            reasoning_path = output_dir / f"{prefix}_reasoning.md"
            reasoning_header = (
                f"# Reasoning Trace — {ac.category}\n\n"
                f"Model: {ac.model} | Reasoning effort: {ac.reasoning_effort}\n"
                f"Reasoning tokens: {usage['thinking']:,}\n\n---\n\n"
            )
            reasoning_path.write_text(
                reasoning_header + reasoning_text + "\n", encoding="utf-8",
            )
            self._logger.info(
                "  Saved reasoning: %s (%d chars)",
                reasoning_path.name, len(reasoning_text),
            )

        self._logger.info("  ─── Cost Summary ───")
        self._logger.info("    Model:            %s", ac.model)
        self._logger.info("    Input tokens:     %s", f"{usage['input']:,}")
        self._logger.info("    Output tokens:    %s", f"{usage['output']:,}")
        self._logger.info("    Reasoning tokens: %s", f"{usage['thinking']:,}")
        self._logger.info("    Estimated cost:   $%.4f", cost)

    # ── data loaders ──────────────────────────────────────────────────────

    @staticmethod
    def _load_phase4_analysis(
        clustering_dir: Path, category: str,
    ) -> tuple[str, Path]:
        """Load the latest Phase 4 analysis markdown for *category*.

        Returns (markdown_text, path).
        """
        pattern = f"phase4_{category}_*.md"
        candidates = sorted(
            (p for p in clustering_dir.glob(pattern) if ".bundle." not in p.name),
            key=lambda p: p.stat().st_mtime,
        )
        if not candidates:
            return "", Path()
        best = candidates[-1]
        return best.read_text(encoding="utf-8"), best

    @staticmethod
    def _load_kaliya_narratives(
        clustering_dir: Path, category: str,
    ) -> dict[str, str]:
        """Load the latest Kaliya narrative for each cluster.

        Returns ``{cluster_letter: narrative_markdown}``.
        """
        pattern = f"kaliya-{category}-cluster_*_narrative.md"
        # Group by cluster letter, keep latest per cluster (by filename sort)
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

    @staticmethod
    def _load_eo_policy_summaries(
        eo_numbers: list[str], eo_index_folder: str,
    ) -> list[dict[str, str]]:
        """Load EO policy summaries from ``x_indices_eo`` JSONL."""
        eo_path = Path.cwd() / eo_index_folder / "research_summaries_with_embeddings_combined.jsonl"
        if not eo_path.exists():
            return []

        target = set(eo_numbers)
        found: dict[str, dict[str, str]] = {}

        with open(eo_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                url = rec.get("source_url", "")
                summary = rec.get("summary_text", "")
                doc_meta = rec.get("doc_metadata", {})
                title = doc_meta.get("title", "")
                for m in re.findall(
                    r"(?:EO|Executive Order)\s+(\d{5})", summary + " " + title,
                ):
                    if m in target and m not in found:
                        found[m] = {
                            "eo_number": m,
                            "title": title,
                            "url": url,
                            "summary_text": summary,
                        }
        return [found[eo] for eo in eo_numbers if eo in found]

    @staticmethod
    def _find_bundle(
        search_dir: Path, category: str,
    ) -> Path | None:
        """Find the latest bundle JSONL for *category*."""
        pattern = f"phase4_{category}_*.bundle.jsonl"
        candidates = sorted(
            search_dir.glob(pattern), key=lambda p: p.stat().st_mtime,
        )
        return candidates[-1] if candidates else None

    @staticmethod
    def _load_bundle(path: Path) -> list[dict[str, Any]]:
        """Load all records from a bundle JSONL."""
        records: list[dict[str, Any]] = []
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    @staticmethod
    def _build_url_to_docid(index_folder: str) -> dict[str, str]:
        """Build a URL → document_id mapping from the merged index.

        Reads ``research_summaries_combined.jsonl`` from *index_folder*
        and returns ``{source_url: document_id}``.
        """
        summaries_path = (
            Path.cwd() / index_folder / "research_summaries_combined.jsonl"
        )
        if not summaries_path.exists():
            return {}
        mapping: dict[str, str] = {}
        with open(summaries_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                url = rec.get("source_url", "")
                doc_id = rec.get("document_id", "")
                if url and doc_id:
                    mapping[url] = doc_id
        return mapping

    @staticmethod
    def _inject_document_ids(
        articles: list[dict[str, Any]],
        url_to_docid: dict[str, str],
    ) -> None:
        """Add ``document_id`` to each article that matches by URL."""
        for art in articles:
            url = art.get("url", "")
            doc_id = url_to_docid.get(url, "")
            art["document_id"] = doc_id

    # ── cluster map parsing ───────────────────────────────────────────────

    @staticmethod
    def _parse_cluster_map(phase4_md: str) -> dict[str, list[int]]:
        """Parse the Cluster Map table from Phase 4 markdown.

        Returns ``{cluster_letter: [article_numbers]}`` (1-based).
        """
        cluster_map: dict[str, list[int]] = {}
        in_table = False

        for line in phase4_md.splitlines():
            if "## Cluster Map" in line:
                in_table = True
                continue
            if in_table and line.startswith("---"):
                break
            if not in_table or not line.startswith("|"):
                continue
            # Skip header/separator rows
            if "Cluster" in line and "Theme" in line:
                continue
            if re.match(r"^\|[-\s|]+\|$", line):
                continue

            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 3:
                continue

            letter = cells[0].strip()
            if not re.match(r"^[A-Z]$", letter):
                continue

            articles_str = cells[2].strip()
            nums = _parse_article_numbers(articles_str)
            if nums:
                cluster_map[letter] = nums

        return cluster_map

    # ── grounding assembly ────────────────────────────────────────────────

    @staticmethod
    def _build_fixed_grounding(
        phase4_md: str,
        narratives: dict[str, str],
        eo_summaries: list[dict[str, str]],
        category: str,
    ) -> str:
        """Build the fixed (non-article) grounding sections."""
        parts: list[str] = []

        # Part 1 — Phase 4 cluster analysis (exclude appendix to save tokens)
        appendix_idx = phase4_md.find("# APPENDIX:")
        analysis_text = phase4_md[:appendix_idx].rstrip() if appendix_idx > 0 else phase4_md
        parts.append(
            "# PART 1 — PHASE 4 CLUSTER ANALYSIS\n\n"
            f"Category: {category}\n\n"
            f"{analysis_text}\n"
        )

        # Part 2 — Kaliya per-cluster narratives
        if narratives:
            parts.append("\n\n# PART 2 — KALIYA MEDIA AGENDA ANALYSIS (PER-CLUSTER)\n")
            for letter in sorted(narratives):
                parts.append(
                    f"\n## Cluster {letter} — Kaliya Analysis\n\n"
                    f"{narratives[letter]}\n"
                )

        # Part 3 — EO policy summaries
        if eo_summaries:
            parts.append(
                "\n\n# PART 3 — EXECUTIVE ORDER POLICY SUMMARIES\n\n"
                "These are the authoritative legal and policy framework "
                "documents referenced throughout the analyses.\n"
            )
            for eo in eo_summaries:
                parts.append(
                    f"\n## Executive Order {eo['eo_number']}: {eo['title']}\n"
                    f"**URL:** {eo['url']}\n\n"
                    f"{eo['summary_text']}\n"
                )

        return "".join(parts)

    @staticmethod
    def _sample_articles(
        articles: list[dict[str, Any]],
        cluster_map: dict[str, list[int]],
        budget_tokens: int,
        enc: Any,
    ) -> tuple[str, int]:
        """Sample articles per cluster to fit within *budget_tokens*.

        Returns (formatted_text, article_count).
        Uses proportional sampling: each cluster gets a share of the
        budget proportional to its article count.
        """
        # Group articles by cluster (1-based article number → 0-based index)
        cluster_articles: dict[str, list[tuple[int, dict]]] = {}
        for letter, nums in cluster_map.items():
            items = []
            for n in nums:
                idx = n - 1
                if 0 <= idx < len(articles):
                    items.append((n, articles[idx]))
            cluster_articles[letter] = items

        total_articles = sum(len(v) for v in cluster_articles.values())
        if total_articles == 0:
            return "", 0

        # Estimate average tokens per article from a sample
        sample_size = min(20, total_articles)
        flat = [(n, a) for items in cluster_articles.values() for n, a in items]
        sample = random.sample(flat, sample_size) if len(flat) >= sample_size else flat
        sample_tokens = [
            len(enc.encode(_format_article(n, a))) for n, a in sample
        ]
        avg_tokens = sum(sample_tokens) / len(sample_tokens) if sample_tokens else 500

        # Calculate how many articles fit
        max_articles = max(1, int(budget_tokens / avg_tokens))
        sample_ratio = min(1.0, max_articles / total_articles)

        # Sample proportionally from each cluster, dedup across clusters
        parts: list[str] = []
        count = 0
        tokens_used = 0
        emitted: set[int] = set()  # track article numbers already output

        for letter in sorted(cluster_articles):
            items = cluster_articles[letter]
            n_sample = max(1, int(len(items) * sample_ratio))
            sampled = random.sample(items, min(n_sample, len(items)))
            sampled.sort(key=lambda x: x[0])

            parts.append(f"\n## Cluster {letter} Articles\n")
            for art_num, art in sampled:
                if art_num in emitted:
                    continue
                formatted = _format_article(art_num, art)
                art_tokens = len(enc.encode(formatted))
                if tokens_used + art_tokens > budget_tokens:
                    break
                parts.append(formatted)
                tokens_used += art_tokens
                count += 1
                emitted.add(art_num)

        return "".join(parts), count

    # ── prompt builder ────────────────────────────────────────────────────

    @staticmethod
    def _build_analytics_prompt(grounding_package: str) -> str:
        """Build the analytics conclusion prompt."""
        return (
            "You are a senior media intelligence analyst tasked with "
            "producing an overarching analytical conclusion that integrates "
            "the cluster-level analyses and Kaliya media-agenda assessments "
            "below.\n\n"
            "## Objective\n\n"
            "Take the results and the analyses of the clusters and integrate "
            "them to provide overarching conclusions about the media's "
            "response to Executive Order implementation and media "
            "representation. Look back on the grounding data and the "
            "analyses and findings and explain the finer semantic patterns "
            "and behaviours of the legacy media in relation to the "
            "administration's efforts.\n\n"
            "Reason through and consider the results, motivations of the "
            "legacy media. Explore evidence-based understanding:\n\n"
            "Does the media, through shared framing, build a narrative "
            "arc longitudinally — are there long-term trends in framing "
            "and semantics that point to specific objectives or broader "
            "goals of the corporate media complex? What perceptions are "
            "being built and intended meaning constructed through long-term "
            "trends? Infer if there are intended behaviours, opinions, "
            "attitudes or beliefs being constructed through stable "
            "reframing patterns over time. Are specific senior members "
            "being targeted, and why? Use logic to build up to "
            "conclusions, guard against confirmation bias, and reason to "
            "remove uncertainty and ambiguity to find the most robustly "
            "structured conclusions.\n\n"
            "## Citation Rules\n\n"
            "Use **(req_YYYYMMDD_HHMMSS_NNN, summary)** citations — each "
            "article includes a **Cite as** line with its unique "
            "document identifier. These identifiers trace directly to the "
            "RAG retrieval index for full data lineage. For any article "
            "without a req_* identifier, a bundle_NNN fallback is shown. "
            "Also cite by **author name and publication outlet** in the "
            "prose — be generous with citations and make clear how each "
            "source contributes to the finding. For Executive Orders, "
            "cite as: Executive Order NNNNN (\"Title\"). For Kaliya "
            "analysis findings, cite as: (kaliya_cluster_X, analysis).\n\n"
            "## Output Format — CRITICAL\n\n"
            "Write in **extended analytical prose**. Structure the "
            "document with markdown headings (## for major sections, "
            "### for subsections) where each heading is a "
            "**content-derived analytical statement** — not a generic "
            "label. Derive headings from the substance of your findings, "
            "exactly as the Kaliya per-cluster narratives do "
            "(e.g. \"The Coordinated Suppression of Strategic "
            "Imperatives\", \"Delegitimization of Lawful Civilian "
            "Oversight\").\n\n"
            "Each section must be **sustained free-text analysis** — "
            "multiple paragraphs of connected reasoning with inline "
            "citations. Do NOT use bullet points or numbered lists "
            "unless presenting a compact quantitative comparison that "
            "genuinely requires tabular or list form. Default to prose "
            "at all times. Short bullet-list summaries are explicitly "
            "prohibited.\n\n"
            "Include evidence tables only where quantitative patterns "
            "exist (e.g. publication timing, article counts). "
            "Draw final conclusions at the end in their own section.\n\n"
            "## Grounding Package\n\n"
            f"{grounding_package}\n\n"
            "## Task\n\n"
            "Now produce the overarching analytical conclusion. "
            "Be thorough, systematic, and evidence-based. Write at "
            "length — this is a comprehensive analytical document, not "
            "a summary. Draw final conclusions.\n"
        )

    # ── LLM generation ────────────────────────────────────────────────────

    def _generate(
        self, ac: AnalyticsConcludeConfig, prompt: str,
    ) -> tuple[str, dict]:
        """Call the configured LLM provider and return (text, usage_dict).

        The ``usage_dict`` includes ``reasoning_text`` — the summarised
        chain-of-thought from the model's reasoning effort (OpenAI
        Responses API only).
        """
        import asyncio

        usage: dict = {"input": 0, "output": 0, "thinking": 0, "reasoning_text": ""}

        if ac.provider == "openai":
            from src._clients.llm_openai_client import OpenAILLMClient

            client = OpenAILLMClient(model_name=ac.model)
            loop = asyncio.new_event_loop()
            try:
                response = loop.run_until_complete(
                    client.client.responses.create(
                        model=ac.model,
                        input=prompt,
                        reasoning={"effort": ac.reasoning_effort},
                        max_output_tokens=ac.max_output_tokens,
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
                details = getattr(resp_usage, "output_tokens_details", None)
                if details:
                    usage["thinking"] = (
                        getattr(details, "reasoning_tokens", 0) or 0
                    )

            # Extract reasoning content from response output items
            reasoning_parts: list[str] = []
            for item in getattr(response, "output", []):
                if getattr(item, "type", "") == "reasoning":
                    for summary in getattr(item, "summary", []):
                        text_content = getattr(summary, "text", "")
                        if text_content:
                            reasoning_parts.append(text_content)
            usage["reasoning_text"] = "\n\n".join(reasoning_parts)

            return text, usage

        # Gemini fallback
        from google.genai import types as genai_types

        from src._clients.llm_gcp_client import GeminiLLMClient

        client = GeminiLLMClient(model_name=ac.model)
        config = genai_types.GenerateContentConfig(
            temperature=0.2,
            top_p=client.top_p,
            max_output_tokens=ac.max_output_tokens,
        )
        response = client.client.models.generate_content(
            model=client.model_name,
            contents=prompt,
            config=config,
        )
        text = response.text if response.text else ""
        um = getattr(response, "usage_metadata", None)
        if um:
            usage["input"] = getattr(um, "prompt_token_count", 0) or 0
            usage["output"] = getattr(um, "candidates_token_count", 0) or 0
            usage["thinking"] = getattr(um, "thoughts_token_count", 0) or 0

        return text, usage


# ── module-level helpers ──────────────────────────────────────────────────


def _parse_article_numbers(text: str) -> list[int]:
    """Parse comma-separated article number ranges like ``1-5, 14, 16-17``.

    Returns a sorted list of unique article numbers.
    """
    nums: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        m = re.match(r"(\d+)\s*-\s*(\d+)", part)
        if m:
            start, end = int(m.group(1)), int(m.group(2))
            nums.update(range(start, end + 1))
        elif re.match(r"\d+$", part):
            nums.add(int(part))
    return sorted(nums)


def _deduplicate_chunks(full_text: str, title: str) -> str:
    """Remove duplicate chunk artefacts from reconstituted full_text.

    The reconstitution pipeline concatenates retrieval chunks that each
    carry a ``{title}: `` prefix.  This strips the repeated prefixes and
    deduplicates any paragraph-level overlap between chunks.
    """
    if not full_text or not title:
        return full_text

    prefix = title + ": "
    if prefix not in full_text:
        return full_text

    # Split on the title prefix (chunk boundary marker)
    chunks = full_text.split(prefix)
    # Rejoin without the repeated title prefix
    cleaned = chunks[0]
    for chunk in chunks[1:]:
        cleaned += chunk

    # Deduplicate paragraphs (>50 chars) that appear more than once
    seen: set[str] = set()
    out_lines: list[str] = []
    for line in cleaned.split("\n"):
        stripped = line.strip()
        if len(stripped) > 50:
            if stripped in seen:
                continue
            seen.add(stripped)
        out_lines.append(line)

    return "\n".join(out_lines)


def _format_article(art_num: int, article: dict[str, Any]) -> str:
    """Format a single article for inclusion in the grounding package."""
    title = article.get("title") or "(untitled)"
    url = article.get("url", "")
    author = article.get("author", "") or ", ".join(
        article.get("doc_metadata", {}).get("authors") or [],
    )
    publisher = (
        article.get("publisher", "")
        or article.get("listed_publisher", "")
        or article.get("doc_metadata", {}).get("publisher", "")
    )
    date = (
        article.get("date_publication", "")
        or article.get("doc_metadata", {}).get("publication_date", "")
    )
    full_text = _deduplicate_chunks(article.get("full_text", ""), title)

    parts = [f"\n### Article {art_num}: {title}\n"]
    meta_parts = []
    if author:
        meta_parts.append(f"**Author:** {author}")
    if publisher:
        meta_parts.append(f"**Publisher:** {publisher}")
    if date:
        meta_parts.append(f"**Date:** {date}")
    if meta_parts:
        parts.append(" | ".join(meta_parts) + "\n")
    if url:
        parts.append(f"**URL:** {url}\n")
    doc_id = article.get("document_id", "")
    cite_key = doc_id if doc_id else f"bundle_{art_num:03d}"
    parts.append(f"**Cite as:** ({cite_key}, summary)\n\n")
    if full_text:
        parts.append(full_text.strip() + "\n")
    return "\n".join(parts)


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
