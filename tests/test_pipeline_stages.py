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

    def test_strip_mermaid_blocks(self):
        from src.components.publisher.rapporteur_cli import PublishStage

        text = "Before\n\n```mermaid\ngraph LR\nA --> B\n```\n\nAfter"
        result = PublishStage._strip_mermaid_blocks(text)
        assert "mermaid" not in result
        assert "Before" in result
        assert "After" in result
        assert "Diagram omitted" in result

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
