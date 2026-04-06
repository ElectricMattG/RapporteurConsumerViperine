"""Tests for src/components/publisher/fuse_analysis.py"""
from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.components.publisher.fuse_analysis import (_build_fuse_prompt,
                                                    _calculate_cost,
                                                    _extract_cluster_sections,
                                                    _extract_preamble,
                                                    _load_kaliya_narratives,
                                                    _load_phase4_analysis,
                                                    _strip_code_fencing)

# ---------------------------------------------------------------------------
# _extract_cluster_sections
# ---------------------------------------------------------------------------

class TestExtractClusterSections:
    def test_extracts_two_clusters(self):
        md = textwrap.dedent("""\
            # Phase 4 Analysis

            Preamble text.

            ## Cluster A — Leadership purges

            Table and analysis for A.

            ### What the official baseline says

            Some content.

            ## Cluster B — Anti-DEI

            Table and analysis for B.

            ## Testing the strongest clustering hypotheses

            Post-cluster content.
        """)
        sections = _extract_cluster_sections(md)
        assert set(sections.keys()) == {"A", "B"}
        assert "Leadership purges" in sections["A"]
        assert "official baseline" in sections["A"]
        assert "Anti-DEI" in sections["B"]
        # Should NOT include the post-cluster section
        assert "Testing the strongest" not in sections["B"]

    def test_single_cluster(self):
        md = "## Cluster C — Renaming\n\nContent.\n\n## Concluding assessment\n\nEnd."
        sections = _extract_cluster_sections(md)
        assert set(sections.keys()) == {"C"}
        assert "Renaming" in sections["C"]
        assert "Concluding" not in sections["C"]

    def test_no_clusters(self):
        md = "# No clusters here\n\nJust text."
        assert _extract_cluster_sections(md) == {}

    def test_cluster_at_end(self):
        md = "## Cluster E — AI rollback\n\nFinal cluster content."
        sections = _extract_cluster_sections(md)
        assert "E" in sections
        assert "AI rollback" in sections["E"]


# ---------------------------------------------------------------------------
# _load_phase4_analysis
# ---------------------------------------------------------------------------

class TestLoadPhase4Analysis:
    def test_finds_latest(self, tmp_path: Path):
        old = tmp_path / "phase4_culture_and_media_20260328_181239.md"
        old.write_text("old")
        new = tmp_path / "phase4_culture_and_media_20260328_183618.md"
        new.write_text("new")
        # Bundle should be excluded
        (tmp_path / "phase4_culture_and_media_20260328_183618.bundle.jsonl").write_text("{}")

        text, path = _load_phase4_analysis(tmp_path, "culture_and_media")
        assert text == "new"
        assert path == new

    def test_not_found(self, tmp_path: Path):
        text, path = _load_phase4_analysis(tmp_path, "nope")
        assert text == ""


# ---------------------------------------------------------------------------
# _load_kaliya_narratives
# ---------------------------------------------------------------------------

class TestLoadKaliyaNarratives:
    def test_finds_latest_per_cluster(self, tmp_path: Path):
        (tmp_path / "kaliya-culture_and_media-cluster_A-20260330_v1_narrative.md").write_text("A old")
        (tmp_path / "kaliya-culture_and_media-cluster_A-20260330_v2_narrative.md").write_text("A new")
        (tmp_path / "kaliya-culture_and_media-cluster_B-20260330_v1_narrative.md").write_text("B only")

        result = _load_kaliya_narratives(tmp_path, "culture_and_media")
        assert "A" in result and "B" in result
        assert result["A"] == "A new"
        assert result["B"] == "B only"


# ---------------------------------------------------------------------------
# _build_fuse_prompt
# ---------------------------------------------------------------------------

class TestExtractPreamble:
    def test_extracts_salience_and_pubhouse(self):
        md = textwrap.dedent("""\
            # Phase 4 Analysis

            Preamble text.

            ## Official baseline events vs. media salience

            | Event | Date | Coverage? |
            |---|---|---|
            | EO1 | 2025-01-27 | Yes — Cluster A |

            ## Official AI baseline events vs. media salience

            | AI Event | Date | Coverage? |
            |---|---|---|
            | AI1 | 2025-03-07 | No |

            ## Publication-house and authorial grouping

            1. Wire cluster
            2. Institutionalist cluster

            ## Cluster A — Leadership purges

            Content for A.
        """)
        preamble = _extract_preamble(md)
        assert "Official baseline events" in preamble
        assert "AI baseline events" in preamble
        assert "Publication-house" in preamble
        assert "Cluster A — Leadership" not in preamble

    def test_no_preamble(self):
        md = "## Cluster A — Foo\n\nContent."
        assert _extract_preamble(md) == ""

    def test_preamble_without_cluster(self):
        md = "## Official baseline events vs. media salience\n\nTable content."
        preamble = _extract_preamble(md)
        assert "Official baseline events" in preamble


class TestBuildFusePrompt:
    def test_includes_both_inputs(self):
        prompt = _build_fuse_prompt("A", "Phase 4 content", "Kaliya content")
        assert "Phase 4 content" in prompt
        assert "Kaliya content" in prompt
        assert "Cluster A" in prompt
        assert "Do NOT omit any data" in prompt
        assert "verbatim" in prompt

    def test_no_bullet_instruction(self):
        prompt = _build_fuse_prompt("B", "stuff", "kaliya")
        assert "No bullet points" in prompt

    def test_includes_preamble_context(self):
        preamble = "| Event | Date | Coverage? |\n|---|---|---|\n| EO1 | 2025-01-27 | Yes — Cluster A |"
        prompt = _build_fuse_prompt("A", "cluster content", "kaliya", preamble)
        assert "EO1" in prompt
        assert "Reference Context" in prompt
        assert "Media Salience Tables" in prompt

    def test_no_preamble_still_works(self):
        prompt = _build_fuse_prompt("A", "content", "kaliya", "")
        assert "Reference Context" not in prompt
        assert "content" in prompt

    def test_subsection_checklist(self):
        prompt = _build_fuse_prompt("C", "content", "kaliya")
        assert "Article table" in prompt
        assert "official baseline says" in prompt
        assert "hypotheses (H1, H2" in prompt
        assert "Alternative explanations" in prompt
        assert "Publication-house grouping" in prompt
        assert "Legal-risk overlay" in prompt
        assert "Media salience" in prompt

    def test_anti_cross_cluster_bleed(self):
        prompt = _build_fuse_prompt("B", "content", "kaliya")
        assert "Focus ONLY on Cluster B" in prompt

    def test_preamble_paragraph_requirement(self):
        prompt = _build_fuse_prompt("A", "content", "kaliya")
        assert "Preamble paragraph" in prompt


# ---------------------------------------------------------------------------
# _strip_code_fencing
# ---------------------------------------------------------------------------

class TestStripCodeFencing:
    def test_strips_fences(self):
        assert _strip_code_fencing("```markdown\ncontent\n```") == "content"

    def test_no_fences(self):
        assert _strip_code_fencing("just text") == "just text"


# ---------------------------------------------------------------------------
# _calculate_cost
# ---------------------------------------------------------------------------

class TestCalculateCost:
    def test_gpt54(self):
        cost = _calculate_cost("gpt-5.4", 100_000, 10_000)
        expected = 100_000 * 2.50 / 1_000_000 + 10_000 * 10.00 / 1_000_000
        assert abs(cost - expected) < 0.001
