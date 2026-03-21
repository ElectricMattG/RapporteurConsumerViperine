"""Tests for ConcludeStage, IntroduceStage, SectionConcludeStage, SectionIntroduceStage.

Focus: testable helper methods (prompt building, context assembly, cost calc,
style extraction, EO reference extraction, chapter summaries, index rebuild).
LLM calls are NOT tested — only the surrounding logic.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from src.components.publisher.rapporteur_cli import (ConcludeStage,
                                                     IntroduceStage,
                                                     SectionConcludeStage,
                                                     SectionIntroduceStage)

# -- helpers ----------------------------------------------------------------

def _make_stage(cls):
    """Build a stage without hitting Base.__init__'s config_handler."""
    stage = object.__new__(cls)
    stage._logger = logging.getLogger(f"test.{cls.__name__}")
    stage.dry_run = False

    class _Proj:
        title = "Test Document"
        author = "CI"
        date = "2026-03-21"

    class _Sections:
        enabled = False
        definitions = []

    class _Cfg:
        force = False
        project = _Proj()
        sections = _Sections()
    stage.cfg = _Cfg()
    return stage


def _write_chapters(chapters_dir: Path, entries: list[dict]) -> list[dict]:
    """Write stub chapters and return manifest entries."""
    chapters_dir.mkdir(parents=True, exist_ok=True)
    manifest_entries = []
    for e in entries:
        fn = e["filename"]
        (chapters_dir / fn).write_text(e.get("content", f"# {fn}\n\nBody."), encoding="utf-8")
        manifest_entries.append({"filename": fn, "title": e.get("title", fn)})
    return manifest_entries


# ═══════════════════════════════════════════════════════════════════════════
# ConcludeStage
# ═══════════════════════════════════════════════════════════════════════════

class TestConcludeStripCodeFencing:
    def test_strips(self):
        assert ConcludeStage._strip_code_fencing("```md\nHello\n```") == "Hello"

    def test_no_fences(self):
        assert ConcludeStage._strip_code_fencing("Hello\nWorld") == "Hello\nWorld"


class TestConcludeCalculateCost:
    def test_gpt5_cost(self):
        stage = _make_stage(ConcludeStage)
        cost = stage._calculate_cost("gpt-5", 1_000_000, 1_000_000)
        assert cost == pytest.approx(2.00 + 8.00)

    def test_unknown_fallback(self):
        stage = _make_stage(ConcludeStage)
        cost = stage._calculate_cost("unknown", 1_000_000, 1_000_000)
        assert cost == pytest.approx(2.50 + 10.00)


class TestConcludeBuildDocumentContext:
    def test_concatenates(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {"filename": "01.md", "content": "Chapter 1"},
            {"filename": "02.md", "content": "Chapter 2"},
        ])
        result = stage._build_document_context(chapters_dir, entries, False)
        assert "Chapter 1" in result
        assert "Chapter 2" in result

    def test_excludes_conclusion(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {"filename": "01.md", "content": "Chapter 1"},
            {"filename": "16-conclusion.md", "content": "Conclusion text"},
        ])
        result = stage._build_document_context(chapters_dir, entries, False)
        assert "Chapter 1" in result
        assert "Conclusion text" not in result


class TestConcludeBuildPrompt:
    def test_contains_key_sections(self):
        stage = _make_stage(ConcludeStage)
        prompt = stage._build_conclude_prompt(
            "Full document", "Conclusion rules", "Style rules",
        )
        assert "CONCLUSION GUIDELINES" in prompt
        assert "STYLE CONVENTIONS" in prompt
        assert "Full document" in prompt

    def test_with_eo_grounding(self):
        stage = _make_stage(ConcludeStage)
        prompt = stage._build_conclude_prompt(
            "Doc", "Rules", "Style", eo_grounding="## LEGAL FRAMEWORK\nEO details"
        )
        assert "LEGAL FRAMEWORK" in prompt
        assert "CITE these Executive Orders" in prompt


class TestConcludeBuildReflectPrompt:
    def test_contains_draft_and_context(self):
        stage = _make_stage(ConcludeStage)
        prompt = stage._build_reflect_prompt(
            "Draft conclusion", "Document context", "Conclusion rules",
        )
        assert "Draft conclusion" in prompt
        assert "Document context" in prompt
        assert "Mermaid" in prompt

    def test_with_eo_grounding(self):
        stage = _make_stage(ConcludeStage)
        prompt = stage._build_reflect_prompt(
            "Draft", "Context", "Rules", eo_grounding="## EO\nDetails",
        )
        assert "Executive Orders cited correctly" in prompt


class TestConcludeExtractEoReferences:
    def test_extracts_federal_register_urls(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01.md",
                "content": (
                    "[EO](https://www.federalregister.gov/documents/2025/01/22/2025-01234/title)"
                ),
            },
        ])
        urls = stage._extract_eo_references(chapters_dir, entries)
        assert len(urls) == 1
        assert "federalregister.gov" in urls[0]

    def test_excludes_conclusion(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "16-conclusion.md",
                "content": "[EO](https://www.federalregister.gov/documents/2025/01/22/x/y)",
            },
        ])
        urls = stage._extract_eo_references(chapters_dir, entries)
        assert len(urls) == 0


class TestConcludeLoadConclusionRules:
    def test_extracts_section(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        guide = tmp_path / "style.md"
        guide.write_text(
            "# Guide\n\n## CONCLUSION PROTOCOL\n\nRule 1\n\n## OTHER\nStuff\n"
        )
        result = stage._load_conclusion_rules(guide)
        assert "Rule 1" in result
        assert "OTHER" not in result

    def test_missing_file(self, tmp_path):
        stage = _make_stage(ConcludeStage)
        assert stage._load_conclusion_rules(tmp_path / "missing.md") == ""


# ═══════════════════════════════════════════════════════════════════════════
# IntroduceStage
# ═══════════════════════════════════════════════════════════════════════════

class TestIntroduceBuildChapterSummaries:
    def test_extracts_first_paragraphs(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01-chapter.md",
                "title": "EO Analysis",
                "content": (
                    "# EO Analysis\n\n"
                    "First paragraph of the chapter.\n\n"
                    "Second paragraph continues.\n\n"
                    "Third paragraph.\n\n"
                    "Fourth paragraph (should be excluded).\n"
                ),
            },
        ])
        result = stage._build_chapter_summaries(chapters_dir, entries)
        assert "### EO Analysis" in result
        assert "First paragraph" in result
        assert "Third paragraph" in result

    def test_excludes_intro_and_conclusion(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {"filename": "00-introduction.md", "content": "# Intro\n\nIntro text."},
            {"filename": "01-chapter.md", "content": "# Ch\n\nChapter text."},
            {"filename": "16-conclusion.md", "content": "# Conclusion\n\nConc text."},
        ])
        result = stage._build_chapter_summaries(chapters_dir, entries)
        assert "Chapter text" in result
        assert "Intro text" not in result
        assert "Conc text" not in result

    def test_skips_markdown_elements(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01.md",
                "content": (
                    "# Title\n\n"
                    "## Sub Heading\n\n"
                    "Real content here.\n\n"
                    "![image](img.png)\n\n"
                    "```python\ncode\n```\n\n"
                    "More content.\n"
                ),
            },
        ])
        result = stage._build_chapter_summaries(chapters_dir, entries)
        assert "Real content" in result
        assert "## Sub" not in result
        assert "```" not in result


class TestIntroduceBuildPrompt:
    def test_contains_key_elements(self):
        stage = _make_stage(IntroduceStage)
        prompt = stage._build_introduce_prompt(
            "Chapter summaries here", "Style rules",
        )
        assert "Test Document" in prompt
        assert "CI" in prompt
        assert "Chapter summaries" in prompt
        assert "STYLE CONVENTIONS" in prompt

    def test_with_citable_eo_list(self):
        stage = _make_stage(IntroduceStage)
        prompt = stage._build_introduce_prompt(
            "Summaries", "Style",
            citable_eo_list="CITABLE EXECUTIVE ORDERS:\n  - Exec. Order No. 14,148 (2025)",
        )
        assert "CITABLE EXECUTIVE ORDERS" in prompt
        assert "CITATION CONSTRAINT" in prompt


class TestIntroduceRebuildIndexMd:
    def test_generates_index(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        entries = [
            {"filename": "00-introduction.md", "title": "Introduction"},
            {"filename": "01-chapter.md", "title": "Chapter 1", "word_count": 5000},
            {"filename": "16-conclusion.md", "title": "Conclusion"},
        ]
        stage._rebuild_index_md(chapters_dir, entries)
        index = (chapters_dir / "index.md").read_text(encoding="utf-8")
        assert "# Test Document" in index
        assert "Introduction" in index
        assert "Chapter 1" in index
        assert "5,000 words" in index
        assert "Generated by Rapporteur CLI" in index


class TestIntroduceBuildCitableEoList:
    def test_extracts_linked_refs(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01.md",
                "content": "[Exec. Order No. 14,148, 2025](https://a.gov/doc)",
            },
        ])
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14,148" in result

    def test_excludes_introduction(self, tmp_path):
        stage = _make_stage(IntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "00-introduction.md",
                "content": "[Exec. Order No. 99,999, 2025](https://a.gov/fake)",
            },
        ])
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert result == ""


class TestIntroduceCalculateCost:
    def test_known_model(self):
        stage = _make_stage(IntroduceStage)
        cost = stage._calculate_cost("gpt-5-mini", 1_000_000, 1_000_000)
        assert cost == pytest.approx(0.40 + 1.60)


# ═══════════════════════════════════════════════════════════════════════════
# SectionConcludeStage
# ═══════════════════════════════════════════════════════════════════════════

class TestSectionConcludeBuildContext:
    def test_builds_section_context(self, tmp_path):
        stage = _make_stage(SectionConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {"filename": "01.md", "content": "Section 1 text"},
            {"filename": "02.md", "content": "Section 2 text"},
        ])
        result = stage._build_section_context(chapters_dir, entries, False)
        assert "Section 1 text" in result
        assert "Section 2 text" in result


class TestSectionConcludeBuildPrompt:
    def test_contains_section_scope(self):
        stage = _make_stage(SectionConcludeStage)
        prompt = stage._build_conclude_prompt(
            "Policy Analysis", 1, 3,
            "Chapter text", "Conclusion rules", "Style rules",
        )
        assert "Part 1 of 3" in prompt
        assert "Policy Analysis" in prompt
        assert "ONLY the chapters in this section" in prompt

    def test_with_custom_prompt(self):
        stage = _make_stage(SectionConcludeStage)
        prompt = stage._build_conclude_prompt(
            "Title", 1, 2, "Text", "Rules", "Style",
            custom_prompt="Focus on economic impact",
        )
        assert "AUTHOR GUIDANCE" in prompt
        assert "economic impact" in prompt


class TestSectionConcludeBuildReflectPrompt:
    def test_contains_draft(self):
        stage = _make_stage(SectionConcludeStage)
        prompt = stage._build_reflect_prompt(
            "Section Title", "Draft conclusion", "Chapter context",
            "Conclusion rules",
        )
        assert "Draft conclusion" in prompt
        assert "Section Title" in prompt


class TestSectionConcludeCitableEoList:
    def test_scoped_to_entries(self, tmp_path):
        stage = _make_stage(SectionConcludeStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01.md",
                "content": "[Exec. Order No. 14,148, 2025](https://a.gov/d)",
            },
        ])
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14,148" in result
        assert "CITABLE" in result


class TestSectionConcludeCalculateCost:
    def test_known_model(self):
        stage = _make_stage(SectionConcludeStage)
        cost = stage._calculate_cost("gemini-2.5-pro", 500_000, 500_000)
        assert cost == pytest.approx((500_000 / 1_000_000 * 1.25) + (500_000 / 1_000_000 * 10.00))


# ═══════════════════════════════════════════════════════════════════════════
# SectionIntroduceStage
# ═══════════════════════════════════════════════════════════════════════════

class TestSectionIntroduceBuildPrompt:
    def test_contains_section_scope(self):
        stage = _make_stage(SectionIntroduceStage)
        prompt = stage._build_introduce_prompt(
            "Personnel Management", 2, 4,
            "Chapter summaries", "Style rules",
        )
        assert "Part 2 of 4" in prompt
        assert "Personnel Management" in prompt
        assert "ONLY the chapters in this section" in prompt

    def test_with_eo_list(self):
        stage = _make_stage(SectionIntroduceStage)
        prompt = stage._build_introduce_prompt(
            "T", 1, 1, "S", "R",
            citable_eo_list="CITABLE EXECUTIVE ORDERS:\n  - Exec. Order No. 14,148",
        )
        assert "CITABLE EXECUTIVE ORDERS" in prompt
        assert "CITATION CONSTRAINT" in prompt


class TestSectionIntroduceBuildReflectPrompt:
    def test_contains_draft(self):
        stage = _make_stage(SectionIntroduceStage)
        prompt = stage._build_reflect_prompt(
            "Title", "Draft intro", "Summaries", "Style",
        )
        assert "Draft intro" in prompt
        assert "Title" in prompt


class TestSectionIntroduceChapterSummaries:
    def test_extracts_paragraphs(self, tmp_path):
        stage = _make_stage(SectionIntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "03.md",
                "title": "AI Policy",
                "content": "# AI Policy\n\nFirst para.\n\nSecond para.\n",
            },
        ])
        result = stage._build_chapter_summaries(chapters_dir, entries)
        assert "### AI Policy" in result
        assert "First para" in result


class TestSectionIntroduceCitableEoList:
    def test_scoped(self, tmp_path):
        stage = _make_stage(SectionIntroduceStage)
        chapters_dir = tmp_path / "chapters"
        entries = _write_chapters(chapters_dir, [
            {
                "filename": "01.md",
                "content": 'Executive Order 14151, "Ending Programs" reference.',
            },
        ])
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14151" in result


class TestSectionIntroduceRebuildIndex:
    def test_generates_index(self, tmp_path):
        stage = _make_stage(SectionIntroduceStage)
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        entries = [
            {"filename": "01a-part-1-introduction.md", "title": "Part 1 Intro"},
            {"filename": "01-chapter.md", "title": "Chapter 1"},
        ]
        stage._rebuild_index_md(chapters_dir, entries)
        index = (chapters_dir / "index.md").read_text(encoding="utf-8")
        assert "Part 1 Intro" in index
        assert "Chapter 1" in index
        assert "Generated by Rapporteur CLI" in index


class TestSectionIntroduceCalculateCost:
    def test_known_model(self):
        stage = _make_stage(SectionIntroduceStage)
        cost = stage._calculate_cost("gpt-5.4", 1_000_000, 1_000_000)
        assert cost == pytest.approx(2.50 + 10.00)
