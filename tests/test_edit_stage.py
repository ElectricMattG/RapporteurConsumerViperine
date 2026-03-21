"""Tests for EditStage — parse_edit_log, is_llm_generated, citable EO list, prompts."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from src.components.publisher.rapporteur_cli import EditStage

# -- helpers ----------------------------------------------------------------

def _make_edit_stage() -> EditStage:
    stage = object.__new__(EditStage)
    stage._logger = logging.getLogger("test.edit")
    stage.dry_run = False
    return stage


# ── _parse_edit_log ──────────────────────────────────────────────────────

class TestParseEditLog:
    def test_valid_json(self):
        stage = _make_edit_stage()
        log = '{"document_score": {"stylistic_consistency": 8}, "edits": []}'
        result = stage._parse_edit_log(log)
        assert result["document_score"]["stylistic_consistency"] == 8

    def test_json_with_code_fences(self):
        stage = _make_edit_stage()
        log = '```json\n{"edits": [{"chapter": 1}]}\n```'
        result = stage._parse_edit_log(log)
        assert len(result["edits"]) == 1

    def test_json_with_surrounding_text(self):
        stage = _make_edit_stage()
        log = 'Here is the review:\n{"edits": []}\nEnd of review.'
        result = stage._parse_edit_log(log)
        assert result is not None
        assert "edits" in result

    def test_invalid_json(self):
        stage = _make_edit_stage()
        result = stage._parse_edit_log("not json at all")
        assert result is None

    def test_empty_string(self):
        stage = _make_edit_stage()
        result = stage._parse_edit_log("")
        assert result is None


# ── _is_llm_generated_chapter ──────────────────────────────────────────

class TestIsLlmGeneratedChapter:
    def test_source_generated(self):
        assert EditStage._is_llm_generated_chapter({"source": "generated"}, "ch.md")

    def test_source_llm(self):
        assert EditStage._is_llm_generated_chapter({"source": "llm"}, "ch.md")

    def test_conclusion_filename(self):
        assert EditStage._is_llm_generated_chapter({}, "16-conclusion.md")

    def test_introduction_keyword(self):
        assert EditStage._is_llm_generated_chapter({}, "00-introduction.md")

    def test_appendix_keyword(self):
        assert EditStage._is_llm_generated_chapter({}, "appendix-a.md")

    def test_synthesis_keyword(self):
        assert EditStage._is_llm_generated_chapter({}, "synthesis-overview.md")

    def test_normal_chapter(self):
        assert not EditStage._is_llm_generated_chapter({"source": "conversation"}, "03-chapter.md")

    def test_high_numbered_chapter(self):
        assert EditStage._is_llm_generated_chapter({}, "99-endnotes.md")

    def test_chapter_17(self):
        assert EditStage._is_llm_generated_chapter({}, "17-appendix.md")


# ── _build_citable_eo_list ─────────────────────────────────────────────

class TestBuildCitableEoList:
    def test_extracts_eo_refs(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        ch = chapters_dir / "01-chapter.md"
        ch.write_text(
            "Analysis referencing "
            "[Exec. Order No. 14,148, 2025](https://www.federalregister.gov/doc1) and "
            "[Exec. Order No. 14,192, 2025](https://www.federalregister.gov/doc2)."
        )
        entries = [{"filename": "01-chapter.md"}]
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14,148" in result
        assert "14,192" in result
        assert "CITABLE" in result

    def test_excludes_llm_generated(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        # Source chapter
        ch1 = chapters_dir / "01-chapter.md"
        ch1.write_text("[Exec. Order No. 14,148, 2025](https://a.gov/doc1)")
        # LLM-generated chapter — should be excluded from extraction
        ch2 = chapters_dir / "16-conclusion.md"
        ch2.write_text('[Exec. Order No. 99,999, 2025](https://a.gov/fake)')
        entries = [
            {"filename": "01-chapter.md"},
            {"filename": "16-conclusion.md", "source": "generated"},
        ]
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14,148" in result
        assert "99,999" not in result

    def test_plain_text_eo_refs(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        ch = chapters_dir / "01-chapter.md"
        ch.write_text(
            'Executive Order 14151, "Ending Radical Programs" was signed.'
        )
        entries = [{"filename": "01-chapter.md"}]
        result = stage._build_citable_eo_list(chapters_dir, entries)
        assert "14151" in result
        assert "Ending Radical Programs" in result

    def test_empty_chapters(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        result = stage._build_citable_eo_list(chapters_dir, [])
        assert result == ""


# ── _strip_code_fencing ──────────────────────────────────────────────────

class TestEditStripCodeFencing:
    def test_strips_fences(self):
        assert EditStage._strip_code_fencing("```md\nContent\n```") == "Content"

    def test_no_fences(self):
        assert EditStage._strip_code_fencing("Content") == "Content"


# ── _calculate_cost ──────────────────────────────────────────────────────

class TestEditCalculateCost:
    def test_gpt5_cost(self):
        stage = _make_edit_stage()
        cost = stage._calculate_cost("gpt-5", 1_000_000, 1_000_000)
        assert cost == pytest.approx(2.00 + 8.00)

    def test_gemini_cost(self):
        stage = _make_edit_stage()
        cost = stage._calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
        assert cost == pytest.approx(0.30 + 2.50)

    def test_unknown_model_fallback(self):
        stage = _make_edit_stage()
        cost = stage._calculate_cost("unknown", 1_000_000, 1_000_000)
        assert cost == pytest.approx(2.50 + 10.00)


# ── _build_document_context ─────────────────────────────────────────────

class TestBuildDocumentContext:
    def test_concatenates_chapters(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        (chapters_dir / "01.md").write_text("Chapter 1 content")
        (chapters_dir / "02.md").write_text("Chapter 2 content")
        entries = [{"filename": "01.md"}, {"filename": "02.md"}]
        result = stage._build_document_context(chapters_dir, entries)
        assert "Chapter 1 content" in result
        assert "Chapter 2 content" in result
        assert "---" in result

    def test_skips_missing_files(self, tmp_path):
        stage = _make_edit_stage()
        chapters_dir = tmp_path / "chapters"
        chapters_dir.mkdir()
        (chapters_dir / "01.md").write_text("Chapter 1")
        entries = [{"filename": "01.md"}, {"filename": "missing.md"}]
        result = stage._build_document_context(chapters_dir, entries)
        assert "Chapter 1" in result


# ── _load_editorial_principles ──────────────────────────────────────────

class TestLoadEditorialPrinciples:
    def test_extracts_section(self, tmp_path):
        stage = _make_edit_stage()
        guide = tmp_path / "style.md"
        guide.write_text(
            "# Guide\n\n## EDITORIAL PRINCIPLES\n\nPrinciple 1\nPrinciple 2\n\n## OTHER\nStuff\n"
        )
        result = stage._load_editorial_principles(guide)
        assert "Principle 1" in result
        assert "OTHER" not in result

    def test_missing_file(self, tmp_path):
        stage = _make_edit_stage()
        result = stage._load_editorial_principles(tmp_path / "nonexistent.md")
        assert result == ""

    def test_no_section(self, tmp_path):
        stage = _make_edit_stage()
        guide = tmp_path / "style.md"
        guide.write_text("Just text.")
        result = stage._load_editorial_principles(guide)
        assert result == ""


# ── _build_apply_prompt ──────────────────────────────────────────────────

class TestBuildApplyPrompt:
    def test_includes_edits(self):
        stage = _make_edit_stage()
        edits = [{"chapter": 1, "issue": "Weak argument", "recommendation": "Add evidence"}]
        prompt = stage._build_apply_prompt("# Chapter\nText", edits, "", False)
        assert "EDIT LOG" in prompt
        assert "Weak argument" in prompt
        assert "# Chapter" in prompt

    def test_includes_executive_summary(self):
        stage = _make_edit_stage()
        prompt = stage._build_apply_prompt(
            "# Chapter\nText", [], "This chapter covers...", True,
        )
        assert "EXECUTIVE SUMMARY" in prompt
        assert "This chapter covers" in prompt

    def test_no_edits_message(self):
        stage = _make_edit_stage()
        prompt = stage._build_apply_prompt("# Chapter", [], "", False)
        assert "No structural edits" in prompt


# ── _build_review_prompt ────────────────────────────────────────────────

class TestBuildReviewPrompt:
    def test_contains_key_elements(self):
        stage = _make_edit_stage()
        entries = [{"filename": "01.md", "title": "Chapter 1"}]
        prompt = stage._build_review_prompt(
            "Full document text", "Editorial principles", "Style rules", entries,
        )
        assert "EDITORIAL PRINCIPLES" in prompt
        assert "STYLE CONVENTIONS" in prompt
        assert "Chapter 1" in prompt
        assert "Full document text" in prompt
        assert "JSON" in prompt
