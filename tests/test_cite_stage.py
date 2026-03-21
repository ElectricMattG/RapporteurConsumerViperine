"""Tests for CiteStage — citation resolution and EO normalisation.

CiteStage has ZERO direct LLM calls; all methods are pure text transformation
/ regex / JSON logic, making every method fully testable without mocking.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from src.components.publisher.rapporteur_cli import CiteStage
from src.models.publisher import CiteConfig, PipelineConfig

# -- helpers ----------------------------------------------------------------

def _make_cite_stage(tmp_path: Path) -> CiteStage:
    """Build a CiteStage without hitting Base.__init__'s config_handler."""
    stage = object.__new__(CiteStage)
    stage._logger = logging.getLogger("test.cite")
    stage.dry_run = False

    # Minimal PipelineConfig-like attrs
    class _Cfg:
        build_dir = tmp_path
        force = False
        cite = CiteConfig()
        project = type("P", (), {"title": "Test", "author": "CI", "date": "2026"})()
    stage.cfg = _Cfg()
    stage._url_to_eo = {}
    return stage


# ── _extract_eo_number ────────────────────────────────────────────────────

class TestExtractEoNumber:
    def test_title_with_executive_order(self):
        assert CiteStage._extract_eo_number("Executive Order 14148 - something") == "14148"

    def test_title_with_5digit(self):
        assert CiteStage._extract_eo_number("Rescissions under 14151 blah") == "14151"

    def test_no_match(self):
        assert CiteStage._extract_eo_number("No numbers here") == ""

    def test_old_4digit_eo(self):
        assert CiteStage._extract_eo_number("Executive Order 12396 revision") == "12396"


# ── _format_eo_number ────────────────────────────────────────────────────

class TestFormatEoNumber:
    def test_comma_separated(self):
        assert CiteStage._format_eo_number("14148") == "14,148"

    def test_short_number(self):
        assert CiteStage._format_eo_number("999") == "999"

    def test_empty(self):
        assert CiteStage._format_eo_number("") == ""

    def test_none(self):
        assert CiteStage._format_eo_number(None) == ""

    def test_non_numeric(self):
        assert CiteStage._format_eo_number("abc") == "abc"


# ── _format_eo_label ────────────────────────────────────────────────────

class TestFormatEoLabel:
    def test_subsequent_mention(self):
        label = CiteStage._format_eo_label(
            "Initial Rescissions", "14148", 80, year="2025", first_mention=False,
        )
        assert label == "Exec. Order No. 14,148, 2025"

    def test_first_mention_with_title(self):
        label = CiteStage._format_eo_label(
            "Initial Rescissions of Harmful EOs", "14148", 80,
            year="2025", first_mention=True,
        )
        assert label.startswith("Exec. Order No. 14,148, Initial")
        assert label.endswith(", 2025")

    def test_first_mention_truncates(self):
        long_title = "A" * 200
        label = CiteStage._format_eo_label(
            long_title, "14148", 60, year="2025", first_mention=True,
        )
        assert "…" in label
        assert len(label) <= 65  # some tolerance for unicode char

    def test_no_eo_number_falls_back_to_title(self):
        label = CiteStage._format_eo_label("Some Title", "", 80, year="2025")
        assert label == "Some Title, 2025"

    def test_no_year(self):
        label = CiteStage._format_eo_label("Title", "14148", 80)
        assert label == "Exec. Order No. 14,148"

    def test_first_mention_budget_too_small(self):
        label = CiteStage._format_eo_label(
            "Title", "14148", 30, year="2025", first_mention=True,
        )
        # Should fall back to short form when budget <= 5
        assert "14,148" in label


# ── _is_downstream_chapter ──────────────────────────────────────────────

class TestIsDownstreamChapter:
    def test_source_generated(self):
        assert CiteStage._is_downstream_chapter({"source": "generated"}, "ch.md")

    def test_source_llm(self):
        assert CiteStage._is_downstream_chapter({"source": "llm"}, "ch.md")

    def test_conclusion_filename(self):
        assert CiteStage._is_downstream_chapter({}, "16-conclusion.md")

    def test_introduction_keyword(self):
        assert CiteStage._is_downstream_chapter({}, "00-introduction.md")

    def test_appendix_keyword(self):
        assert CiteStage._is_downstream_chapter({}, "appendix-a.md")

    def test_normal_chapter(self):
        assert not CiteStage._is_downstream_chapter({"source": "conversation"}, "03-chapter.md")

    def test_filename_without_dash(self):
        assert not CiteStage._is_downstream_chapter({}, "chapter3.md")


# ── _resolve_chapter ────────────────────────────────────────────────────

class TestResolveChapter:
    def _make_stage_with_eo(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        stage._url_to_eo = {
            "www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions": {
                "title": "Initial Rescissions of Harmful EOs",
                "eo_number": "14148",
                "year": "2025",
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            },
        }
        return stage

    def test_resolves_eo_token(self, tmp_path):
        stage = self._make_stage_with_eo(tmp_path)
        text = "Policy analysis (doc_abcdef12, eo) shows impact."
        citation_map = {
            "doc_abcdef12": {
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            },
        }
        eo_index = {
            "doc_abcdef12": {
                "title": "Initial Rescissions of Harmful EOs",
                "eo_number": "14148",
                "year": "2025",
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            },
        }
        result, stats = stage._resolve_chapter(text, citation_map, eo_index, stage.cfg.cite)
        assert "[Exec. Order No. 14,148" in result
        assert stats["eo"] == 1
        assert stats["resolved"] == 1

    def test_unresolved_token_left_intact(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = "Analysis (doc_unknown00, eo) continues."
        result, stats = stage._resolve_chapter(text, {}, {}, stage.cfg.cite)
        assert "(doc_unknown00, eo)" in result
        assert stats["resolved"] == 0

    def test_multiple_tokens_semicolon(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = "Content (doc_aaa, eo; doc_bbb, eo) here."
        eo_index = {
            "doc_aaa": {"title": "T1", "eo_number": "14148", "year": "2025", "url": "https://example.gov/a"},
            "doc_bbb": {"title": "T2", "eo_number": "14192", "year": "2025", "url": "https://example.gov/b"},
        }
        result, stats = stage._resolve_chapter(text, {}, eo_index, stage.cfg.cite)
        assert stats["eo"] == 2
        assert "14,148" in result
        assert "14,192" in result


# ── _dedup_section_citations ────────────────────────────────────────────

class TestDedupSectionCitations:
    def test_merges_same_url(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = (
            "([Exec. Order No. 14,148, 2025]"
            "(https://www.federalregister.gov/documents/2025/01/22/ex1)), "
            "Section 2; "
            "([Exec. Order No. 14,148, 2025]"
            "(https://www.federalregister.gov/documents/2025/01/22/ex1))"
        )
        result, count = stage._dedup_section_citations(text)
        assert count == 1
        assert "Section 2" in result
        # Should be a single consolidated link
        assert result.count("federalregister.gov") == 1

    def test_different_urls_not_merged(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = (
            "([EO A](https://a.gov/doc1)), Section 2; ([EO B](https://b.gov/doc2))"
        )
        result, count = stage._dedup_section_citations(text)
        assert count == 0


# ── _consolidate_adjacent_citations ────────────────────────────────────

class TestConsolidateAdjacent:
    def test_merges_two_adjacent(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = (
            "([Exec. Order No. 14,148, 2025](https://a.gov/1)); "
            "([Exec. Order No. 14,192, 2025](https://b.gov/2))"
        )
        result, count = stage._consolidate_adjacent_citations(text)
        assert count == 1
        # Should be one parenthetical with semicolon
        assert "14,148" in result
        assert "14,192" in result

    def test_deduplicates_same_eo(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = (
            "([Exec. Order No. 14,148, Title (2025)](https://a.gov/1)); "
            "([Exec. Order No. 14,148, 2025](https://a.gov/1))"
        )
        result, count = stage._consolidate_adjacent_citations(text)
        assert count == 1
        # Deduplicated — only one 14,148 link
        assert result.count("14,148") == 1


# ── _merge_trailing_sections ──────────────────────────────────────────

class TestMergeTrailingSections:
    def test_trailing_section_merged(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = "([Exec. Order No. 14,148, 2025](https://a.gov/1)), Section 3"
        result, count = stage._merge_trailing_sections(text)
        assert count >= 1
        assert "Section 3" in result
        # Section ref should be inside the link text
        assert "Section 3]" in result

    def test_section_outside_link_merged(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        text = "([Exec. Order No. 14,148, 2025](https://a.gov/1), Section 2)"
        result, count = stage._merge_trailing_sections(text)
        assert count >= 1
        assert "Section 2]" in result


# ── _normalise_existing_eo_links ────────────────────────────────────────

class TestNormaliseExistingEoLinks:
    def test_rewrites_bare_url_label(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        stage._url_to_eo = {
            "www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions": {
                "title": "Initial Rescissions",
                "eo_number": "14148",
                "year": "2025",
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            },
        }
        text = "[Click here](https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions)"
        cc = CiteConfig()
        seen = set()
        result, count = stage._normalise_existing_eo_links(text, cc, seen)
        assert count == 1
        assert "Exec. Order No. 14,148" in result

    def test_already_correct_not_rewritten(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        stage._url_to_eo = {
            "www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions": {
                "title": "Initial Rescissions",
                "eo_number": "14148",
                "year": "2025",
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            },
        }
        text = "[Exec. Order No. 14,148, 2025](https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions)"
        cc = CiteConfig()
        seen = set()
        result, count = stage._normalise_existing_eo_links(text, cc, seen)
        assert count == 0

    def test_no_url_to_eo_returns_unchanged(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        stage._url_to_eo = {}
        text = "[link](https://www.federalregister.gov/documents/2025/01/22/foo)"
        cc = CiteConfig()
        result, count = stage._normalise_existing_eo_links(text, cc, set())
        assert count == 0
        assert result == text


# ── _build_merged_citation_map ──────────────────────────────────────────

class TestBuildMergedCitationMap:
    def test_merges_from_conversations(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        convs = [
            {"metadata": {"citation_map": {"doc_aa": {"url": "https://a.gov"}}}},
            {"metadata": {"citation_map": {"doc_bb": {"url": "https://b.gov"}}}},
        ]
        (raw_dir / "conversations.json").write_text(json.dumps(convs))
        result = stage._build_merged_citation_map()
        assert "doc_aa" in result
        assert "doc_bb" in result

    def test_missing_conversations_file(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        result = stage._build_merged_citation_map()
        assert result == {}


# ── _build_eo_title_index ──────────────────────────────────────────────

class TestBuildEoTitleIndex:
    def test_maps_doc_keys(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        convs = [
            {
                "eo_results": [
                    {
                        "source_url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
                        "doc_metadata": {"title": "Initial Rescissions of Harmful EOs", "publication_date": "2025-01-22"},
                        "eo_ids": ["14148"],
                    }
                ],
                "metadata": {},
            }
        ]
        (raw_dir / "conversations.json").write_text(json.dumps(convs))

        cm = {
            "doc_aabbccdd": {
                "url": "https://www.federalregister.gov/documents/2025/01/22/2025-01234/initial-rescissions",
            }
        }
        result = stage._build_eo_title_index(cm)
        assert "doc_aabbccdd" in result
        assert result["doc_aabbccdd"]["eo_number"] == "14148"

    def test_no_eo_results(self, tmp_path):
        stage = _make_cite_stage(tmp_path)
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "conversations.json").write_text(json.dumps([{"metadata": {}}]))
        result = stage._build_eo_title_index({})
        assert result == {}
