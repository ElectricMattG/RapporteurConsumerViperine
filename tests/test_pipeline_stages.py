"""Tests for core pipeline stages: ExportStage, ParseStage, ChapterStage, PublishStage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.models.publisher import PipelineConfig


def _make_config(tmp_path: Path, **overrides) -> PipelineConfig:
    """Build a PipelineConfig with build_dir pointing to tmp_path."""
    cfg_dict = {
        "project": {"title": "Test Document", "author": "CI", "date": "2026-01-01"},
        "paths": {"build_dir": str(tmp_path / "build")},
        "export": {"static_file": str(tmp_path / "conversations.json")},
        "sessions": {"include": [0, 1]},
        "render": {
            "chapters": [
                {"title": "Chapter A", "sessions": [0]},
                {"title": "Chapter B", "sessions": [1]},
            ],
        },
        "publish": {"formats": ["pdf"]},
    }
    cfg_dict.update(overrides)
    yaml_path = tmp_path / "doc.yaml"
    yaml_path.write_text(yaml.dump(cfg_dict, default_flow_style=False), encoding="utf-8")
    return PipelineConfig.from_yaml(yaml_path)


def _write_conversations(path: Path, convs: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(convs, indent=2), encoding="utf-8")


def _sample_conversations() -> list[dict]:
    return [
        {
            "user_query": "Analyze EO 14148",
            "timestamp": "2026-01-01T10:00:00Z",
            "assistant_response": "EO 14148 covers initial rescissions of harmful orders.",
            "retrieved_docs": [{"doc_id": "d1", "summary_text": "Summary"}],
            "retrieved_qa": [],
            "eo_results": [],
            "event_results": [],
            "enrichment_results": [],
            "metadata": {"citation_map": {"d1": {"url": "https://a.gov/1", "author": "A"}}},
        },
        {
            "user_query": "What about EO 14192?",
            "timestamp": "2026-01-01T10:05:00Z",
            "assistant_response": "EO 14192 addresses deregulation measures.",
            "retrieved_docs": [{"doc_id": "d2", "summary_text": "Summary 2"}],
            "retrieved_qa": [],
            "eo_results": [],
            "event_results": [],
            "enrichment_results": [],
            "metadata": {"citation_map": {"d2": {"url": "https://b.gov/2", "author": "B"}}},
        },
    ]


# ===================================================================
# ExportStage
# ===================================================================


class TestExportStage:
    def test_copy_static(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        convs = _sample_conversations()
        static_file = tmp_path / "conversations.json"
        _write_conversations(static_file, convs)

        cfg = _make_config(tmp_path)
        stage = ExportStage(cfg)
        result = stage.run()
        assert result.exists()
        data = json.loads(result.read_text())
        assert len(data) == 2

    def test_copy_static_creates_symlink(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        _write_conversations(tmp_path / "conversations.json", _sample_conversations())
        cfg = _make_config(tmp_path)
        stage = ExportStage(cfg)
        stage.run()
        link = cfg.build_dir / "raw" / "conversations.json"
        assert link.exists()

    def test_copy_static_missing_file(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        cfg = _make_config(tmp_path)
        # static_file points to non-existent file
        with pytest.raises(FileNotFoundError):
            ExportStage(cfg).run()

    def test_dry_run(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        _write_conversations(tmp_path / "conversations.json", _sample_conversations())
        cfg = _make_config(tmp_path)
        stage = ExportStage(cfg, dry_run=True)
        result = stage.run()
        # Dry run returns the path but doesn't write
        assert not result.exists()

    def test_fetch_api(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        cfg_dict = {
            "project": {"title": "Test", "author": "CI", "date": "2026-01-01"},
            "paths": {"build_dir": str(tmp_path / "build")},
            "export": {"frontend_url": "http://localhost:3000"},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")
        cfg = PipelineConfig.from_yaml(yaml_path)

        mock_resp = MagicMock()
        mock_resp.json.return_value = _sample_conversations()
        mock_resp.raise_for_status = MagicMock()

        with patch("src.components.publisher.rapporteur_cli.requests.get", return_value=mock_resp):
            stage = ExportStage(cfg)
            result = stage.run()
            assert result.exists()

    def test_write_counts(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ExportStage

        convs = _sample_conversations()
        _write_conversations(tmp_path / "conversations.json", convs)
        cfg = _make_config(tmp_path)
        stage = ExportStage(cfg)
        result = stage.run()
        data = json.loads(result.read_text())
        assert len(data) == len(convs)


# ===================================================================
# ParseStage
# ===================================================================


class TestParseStage:
    def _setup(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ParseStage

        convs = _sample_conversations()
        _write_conversations(tmp_path / "conversations.json", convs)
        cfg = _make_config(tmp_path)
        # First run export to create raw/conversations.json
        from src.components.publisher.rapporteur_cli import ExportStage
        ExportStage(cfg).run()
        return cfg, ParseStage(cfg)

    def test_run_creates_parsed_dir(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        result = stage.run()
        assert result.exists()
        assert result.is_dir()

    def test_run_creates_session_files(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        stage.run()
        parsed = cfg.build_dir / "parsed"
        sessions = list(parsed.glob("session_*.json"))
        assert len(sessions) == 2

    def test_run_creates_footnotes(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        stage.run()
        fn = cfg.build_dir / "parsed" / "footnotes.json"
        assert fn.exists()

    def test_run_creates_unified_citations(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        stage.run()
        uc = cfg.build_dir / "parsed" / "unified_citations.json"
        assert uc.exists()
        data = json.loads(uc.read_text())
        assert len(data) > 0

    def test_dry_run(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ParseStage

        _write_conversations(tmp_path / "conversations.json", _sample_conversations())
        cfg = _make_config(tmp_path)
        from src.components.publisher.rapporteur_cli import ExportStage
        ExportStage(cfg).run()
        stage = ParseStage(cfg, dry_run=True)
        result = stage.run()
        parsed = cfg.build_dir / "parsed"
        # Dry run should not create session files
        assert not list(parsed.glob("session_*.json")) if parsed.exists() else True

    def test_no_raw_file_raises(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ParseStage

        cfg = _make_config(tmp_path)
        stage = ParseStage(cfg)
        with pytest.raises(FileNotFoundError):
            stage.run()

    def test_build_footnotes(self, tmp_path):
        from src.components.publisher.rapporteur_cli import ParseStage

        cfg = _make_config(tmp_path)
        stage = ParseStage(cfg)
        conversations = [
            {"assistant_response": "First sentence. Second sentence. Third."},
        ]
        footnote_turns = [{"index": 0, "user_query": "Q"}]
        footnotes = stage._build_footnotes(conversations, footnote_turns)
        assert len(footnotes) == 1
        assert "First sentence" in footnotes[0]["footnote_text"]


# ===================================================================
# ChapterStage
# ===================================================================


class TestChapterStage:
    def _setup(self, tmp_path):
        from src.components.publisher.rapporteur_cli import (ChapterStage,
                                                             ExportStage,
                                                             ParseStage)

        convs = _sample_conversations()
        _write_conversations(tmp_path / "conversations.json", convs)
        cfg = _make_config(tmp_path)
        ExportStage(cfg).run()
        ParseStage(cfg).run()
        return cfg, ChapterStage(cfg)

    def test_run_creates_chapters_dir(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        result = stage.run()
        assert result.exists()
        assert result.is_dir()

    def test_run_creates_chapter_files(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        stage.run()
        chapters = list((cfg.build_dir / "chapters").glob("*.md"))
        # Should have chapter files + index.md
        assert len(chapters) >= 3  # 2 chapters + index.md

    def test_run_creates_manifest(self, tmp_path):
        cfg, stage = self._setup(tmp_path)
        stage.run()
        manifest = cfg.build_dir / "chapters" / "manifest.json"
        assert manifest.exists()
        data = json.loads(manifest.read_text())
        assert len(data["chapters"]) == 2

    def test_title_from_query(self):
        from src.components.publisher.rapporteur_cli import ChapterStage

        assert ChapterStage._title_from_query("What is EO 14148?", 0) == "What is EO 14148"
        assert ChapterStage._title_from_query("", 5) == "Session 5"

    def test_title_truncation(self):
        from src.components.publisher.rapporteur_cli import ChapterStage

        long_query = "x" * 200
        result = ChapterStage._title_from_query(long_query, 0)
        assert len(result) <= 80

    def test_slugify(self):
        from src.components.publisher.rapporteur_cli import ChapterStage

        assert ChapterStage._slugify("Hello World!") == "hello_world"
        assert ChapterStage._slugify("A" * 100)[:60] == "a" * 60

    def test_dry_run(self, tmp_path):
        from src.components.publisher.rapporteur_cli import (ChapterStage,
                                                             ExportStage,
                                                             ParseStage)

        _write_conversations(tmp_path / "conversations.json", _sample_conversations())
        cfg = _make_config(tmp_path)
        ExportStage(cfg).run()
        ParseStage(cfg).run()
        stage = ChapterStage(cfg, dry_run=True)
        stage.run()
        chapters_dir = cfg.build_dir / "chapters"
        # No chapter files written in dry run (only the directory might not exist)
        md_files = list(chapters_dir.glob("[0-9]*.md")) if chapters_dir.exists() else []
        assert len(md_files) == 0


# ===================================================================
# PublishStage
# ===================================================================


class TestPublishStage:
    def test_build_conf_py(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)
        conf = stage._build_conf_py("test_doc")
        assert 'project = "Test Document"' in conf
        assert 'author = "CI"' in conf
        assert "myst_parser" in conf
        assert "xelatex" in conf

    def test_build_index_md(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)
        index = stage._build_index_md(["01-intro", "02-body"])
        assert "toctree" in index
        assert "01-intro" in index
        assert "02-body" in index

    def test_strip_leading_blockquote(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        text = "> This is a blockquote\n\n# Heading\n\nBody"
        result = PublishStage._strip_leading_blockquote(text)
        assert result.startswith("# Heading")

    def test_strip_leading_blockquote_no_blockquote(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        text = "# Heading\n\nBody"
        assert PublishStage._strip_leading_blockquote(text) == text

    def test_demote_extra_chapter_headings(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        text = "# First\n\nContent\n\n# Second\n\nMore"
        result = PublishStage._demote_extra_chapter_headings(text)
        assert result.count("## Second") == 1
        assert result.startswith("# First")

    def test_render_mermaid_blocks(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)
        src_dir = tmp_path / "_sphinx_src"
        src_dir.mkdir()

        text = "Before\n\n```mermaid\ngraph LR\nA --> B\n```\n\nAfter"
        result = stage._render_mermaid_blocks(text, src_dir)
        assert "```mermaid" not in result
        assert "Before" in result
        assert "After" in result
        # Either rendered to image or fell back to placeholder
        assert ("_diagrams/" in result or "Diagram" in result)

    def test_no_manifest_returns(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)
        # No manifest.json — should return without error
        (cfg.build_dir / "chapters").mkdir(parents=True, exist_ok=True)
        stage.run()

    def test_dry_run(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        convs = _sample_conversations()
        _write_conversations(tmp_path / "conversations.json", convs)
        cfg = _make_config(tmp_path)

        # Setup chapters dir with manifest
        chapters_dir = cfg.build_dir / "chapters"
        chapters_dir.mkdir(parents=True, exist_ok=True)
        manifest = {
            "chapters": [
                {"filename": "01-test.md", "title": "Test", "word_count": 100},
            ]
        }
        (chapters_dir / "manifest.json").write_text(json.dumps(manifest))
        (chapters_dir / "01-test.md").write_text("# Test\n\nBody content\n")

        stage = PublishStage(cfg, dry_run=True)
        stage.run()
        # Should not invoke sphinx-build

    def test_pdf_page_count_not_available(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        result = PublishStage._pdf_page_count(tmp_path / "nonexistent.pdf")
        assert result is None


# ══════════════════════════════════════════════════════════════════════
# PublishStage — chapter ordering
# ══════════════════════════════════════════════════════════════════════


class TestSortChapterEntries:
    def test_intro_comes_first(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        entries = [
            {"filename": "03-body.md"},
            {"filename": "00-introduction.md"},
            {"filename": "01-start.md"},
        ]
        result = PublishStage._sort_chapter_entries(entries)
        assert result[0]["filename"] == "00-introduction.md"

    def test_natural_numeric_order(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        entries = [
            {"filename": "02-second.md"},
            {"filename": "01-first.md"},
            {"filename": "03-third.md"},
        ]
        result = PublishStage._sort_chapter_entries(entries)
        fnames = [e["filename"] for e in result]
        assert fnames == ["01-first.md", "02-second.md", "03-third.md"]

    def test_introduction_by_name(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        entries = [
            {"filename": "05-conclusion.md"},
            {"filename": "00-introduction.md"},
            {"filename": "01-chapter.md"},
        ]
        result = PublishStage._sort_chapter_entries(entries)
        assert result[0]["filename"] == "00-introduction.md"

    def test_section_intros_after_intro(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        entries = [
            {"filename": "03-body.md"},
            {"filename": "01a-part-1-intro.md"},
            {"filename": "00-introduction.md"},
        ]
        result = PublishStage._sort_chapter_entries(entries)
        assert result[0]["filename"] == "00-introduction.md"
        assert result[1]["filename"] == "01a-part-1-intro.md"
        assert result[2]["filename"] == "03-body.md"

    def test_empty_list(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        assert PublishStage._sort_chapter_entries([]) == []


# ══════════════════════════════════════════════════════════════════════
# PublishStage — reference extraction and APA formatting
# ══════════════════════════════════════════════════════════════════════


class TestExtractReferences:
    def _make_stage(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage
        cfg = _make_config(tmp_path)
        return PublishStage(cfg)

    def test_extracts_ref_lines(self, tmp_path):
        stage = self._make_stage(tmp_path)
        text = (
            "# Chapter\n\nBody text.\n\n"
            "## References\n\n"
            "- **Author A** (2025). *Title A*. Publisher. [url](url)\n"
            "- **Author B** (2026). *Title B*. Publisher. [url2](url2)\n"
        )
        clean, refs = stage._extract_references(text)
        assert len(refs) == 2
        assert "Author A" in refs[0]
        assert "## References" not in clean

    def test_strips_top_sources(self, tmp_path):
        stage = self._make_stage(tmp_path)
        text = (
            "# Chapter\n\nBody.\n\n"
            "## Top Sources\n\n"
            "1. Source one\n2. Source two\n\n"
            "## References\n\n"
            "- **Author** (2025). *Title*. Pub. [u](u)\n"
        )
        clean, refs = stage._extract_references(text)
        assert "## Top Sources" not in clean
        assert "## References" not in clean
        assert len(refs) == 1

    def test_no_refs_returns_empty(self, tmp_path):
        stage = self._make_stage(tmp_path)
        text = "# Chapter\n\nJust body text.\n"
        clean, refs = stage._extract_references(text)
        assert refs == []
        assert "Just body text" in clean

    def test_preserves_body_text(self, tmp_path):
        stage = self._make_stage(tmp_path)
        text = (
            "# Chapter\n\nImportant content here.\n\n"
            "## Section\n\nMore content.\n\n"
            "## References\n\n"
            "- **A** (2025). *T*. P. [u](u)\n"
        )
        clean, refs = stage._extract_references(text)
        assert "Important content here" in clean
        assert "More content" in clean


class TestParseRefLine:
    def test_full_line(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        line = "- **John Doe** (March 2025). *Article Title*. The Publisher. [https://example.com](https://example.com)"
        result = PublishStage._parse_ref_line(line)
        assert result["author"] == "John Doe"
        assert result["date"] == "March 2025"
        assert result["title"] == "Article Title"
        assert result["publisher"] == "The Publisher"
        assert result["url"] == "https://example.com"

    def test_no_date(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        line = "- **Unknown**. *Some Title*. Publisher. [https://x.com](https://x.com)"
        result = PublishStage._parse_ref_line(line)
        assert result["author"] == "Unknown"
        assert "date" not in result

    def test_no_url(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        line = "- **Author** (2025). *Title*. Publisher Name"
        result = PublishStage._parse_ref_line(line)
        assert result["author"] == "Author"
        assert "url" not in result
        assert result["publisher"] == "Publisher Name"

    def test_minimal_line(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        line = "- **Author** (2025)."
        result = PublishStage._parse_ref_line(line)
        assert result["author"] == "Author"
        assert result["date"] == "2025"


class TestFormatApaReferences:
    def _make_stage(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage
        cfg = _make_config(tmp_path)
        return PublishStage(cfg)

    def test_deduplicates_by_url(self, tmp_path):
        stage = self._make_stage(tmp_path)
        refs = [
            "- **Author** (2025). *Title*. Pub. [https://example.com/a](https://example.com/a)",
            "- **Author** (2025). *Title*. Pub. [https://example.com/a](https://example.com/a)",
            "- **Other** (2026). *Other*. Pub. [https://example.com/b](https://example.com/b)",
        ]
        result = stage._format_apa_references(refs)
        # Should have 2 unique refs, not 3
        assert result.count("\n- ") == 2

    def test_sorted_by_surname(self, tmp_path):
        stage = self._make_stage(tmp_path)
        refs = [
            "- **Zoe Smith** (2025). *Title*. Pub. [https://a.com](https://a.com)",
            "- **Alice Jones** (2025). *Title*. Pub. [https://b.com](https://b.com)",
        ]
        result = stage._format_apa_references(refs)
        lines = [l for l in result.split('\n') if l.startswith('- ')]
        assert "Alice Jones" in lines[0]
        assert "Zoe Smith" in lines[1]

    def test_apa_format(self, tmp_path):
        stage = self._make_stage(tmp_path)
        refs = [
            "- **Author Name** (March 2025). *Article Title*. The Guardian. [https://x.com](https://x.com)",
        ]
        result = stage._format_apa_references(refs)
        assert "# References" in result
        assert "APA 7th" in result
        assert "Author Name. (March 2025)." in result
        assert "*Article Title.*" in result

    def test_empty_refs(self, tmp_path):
        stage = self._make_stage(tmp_path)
        result = stage._format_apa_references([])
        assert "# References" in result


# ══════════════════════════════════════════════════════════════════════
# PublishStage — diagram repositioning
# ══════════════════════════════════════════════════════════════════════


class TestRepositionDiagrams:
    def test_moves_impl_record_after_exec_summary(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        text = (
            "# Chapter Title\n"
            "> **Executive Summary:** This is the summary.\n"
            "\n"
            "## Section 1\n\nBody text.\n"
            "\n"
            "## Implementation Record\n"
            "\n"
            "### EO Precedence\n"
            "\n"
            "```mermaid\ngraph LR\nA --> B\n```\n"
        )
        result = stage._reposition_diagrams(text)
        # Implementation Record should appear before Section 1
        impl_pos = result.find("## Implementation Record")
        section_pos = result.find("## Section 1")
        assert impl_pos < section_pos
        # And after the exec summary
        summary_pos = result.find("Executive Summary")
        assert impl_pos > summary_pos

    def test_no_impl_record_unchanged(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        text = "# Chapter\n\nBody text only.\n"
        assert stage._reposition_diagrams(text) == text

    def test_no_exec_summary_fallback(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        text = (
            "# Chapter\n"
            "## Section 1\n\nBody.\n"
            "\n"
            "## Implementation Record\n"
            "\n"
            "### EO Precedence\n"
            "\n"
            "Diagram here\n"
        )
        result = stage._reposition_diagrams(text)
        # Should still contain both sections
        assert "## Implementation Record" in result
        assert "## Section 1" in result

    def test_multiple_blockquotes(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        text = (
            "# Chapter Title\n"
            "> **Executive Summary:** First summary.\n"
            "\n"
            "> **Executive Summary:** Second summary.\n"
            "\n"
            "## Section 1\n\nBody.\n"
            "\n"
            "## Implementation Record\n"
            "\n"
            "Diagram content.\n"
        )
        result = stage._reposition_diagrams(text)
        impl_pos = result.find("## Implementation Record")
        section_pos = result.find("## Section 1")
        assert impl_pos < section_pos


class TestAddDiagramCaptions:
    def test_adds_caption_to_mermaid(self, tmp_path):
        from unittest.mock import patch

        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)

        text = "# Chapter\n\n```mermaid\ngraph LR\nA --> B\n```\n\nAfter"
        with patch.object(stage, '_generate_diagram_caption', return_value="Test caption"):
            result = stage._add_diagram_captions(text)
        assert "*Test caption*" in result
        assert "```mermaid" in result

    def test_no_mermaid_unchanged(self, tmp_path):
        from src.components.publisher.rapporteur_cli import PublishStage

        cfg = _make_config(tmp_path)
        stage = PublishStage(cfg)

        text = "# Chapter\n\nNo diagrams here.\n"
        result = stage._add_diagram_captions(text)
        assert result == text


class TestCalculateCostPublish:
    def test_flash_pricing(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        stage._logger = __import__("logging").getLogger("test")
        cost = stage._calculate_cost("gemini-2.5-flash", 1_000_000, 1_000_000)
        assert cost == pytest.approx(0.30 + 2.50)

    def test_unknown_model_fallback(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        stage = object.__new__(PublishStage)
        stage._logger = __import__("logging").getLogger("test")
        cost = stage._calculate_cost("unknown-model", 1_000_000, 0)
        assert cost == pytest.approx(0.30)  # falls back to flash pricing
