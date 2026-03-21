"""Tests for CLI argument parsing, STAGES list, helpers, and pipeline orchestration."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.components.publisher.rapporteur_cli import (STAGES,
                                                     _first_n_sentences,
                                                     _gemini_location,
                                                     _human_size,
                                                     _normalise_url,
                                                     build_parser,
                                                     run_pipeline)
from src.models.publisher import PipelineConfig

# ===================================================================
# build_parser()
# ===================================================================


class TestBuildParser:
    def test_has_all_stage_flags(self):
        parser = build_parser()
        # Should be able to parse every stage flag without error
        args = parser.parse_args([
            "config.yaml",
            "--export", "--parse", "--chapters", "--kaliya",
            "--format", "--conclude", "--edit", "--cite",
            "--introduce", "--publish", "--eo-link",
            "--section-conclude", "--section-introduce",
        ])
        assert args.export is True
        assert args.parse is True
        assert args.chapters is True
        assert args.publish is True

    def test_all_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--all"])
        assert args.all is True

    def test_dry_run_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--export", "--dry-run"])
        assert args.dry_run is True

    def test_force_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--export", "--force"])
        assert args.force is True

    def test_config_positional(self):
        parser = build_parser()
        args = parser.parse_args(["my_config.yaml", "--export"])
        assert args.config == "my_config.yaml"

    def test_no_format_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-format", "--publish"])
        assert args.format is False

    def test_no_conclude_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-conclude", "--publish"])
        assert args.conclude is False

    def test_no_edit_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-edit", "--publish"])
        assert args.edit is False

    def test_no_cite_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-cite", "--publish"])
        assert args.cite is False

    def test_no_introduce_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-introduce", "--publish"])
        assert args.introduce is False

    def test_no_eo_link_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-eo-link", "--publish"])
        assert args.eo_link is False

    def test_verbose_flag(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--export", "-v"])
        assert args.verbose is True

    def test_build_dir_override(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--export", "--build-dir", "/tmp/b"])
        assert args.build_dir == "/tmp/b"

    def test_exclude_eos(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--export", "--exclude-eos", "14165", "14370"])
        assert args.exclude_eos == ["14165", "14370"]

    def test_default_none_for_optional_stages(self):
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--publish"])
        assert args.format is None
        assert args.conclude is None
        assert args.edit is None
        assert args.cite is None
        assert args.introduce is None


# ===================================================================
# STAGES list
# ===================================================================


class TestStagesList:
    def test_stages_complete(self):
        names = [name for name, _ in STAGES]
        expected = [
            "export", "parse", "chapters", "kaliya", "eo_link",
            "format", "conclude", "section_conclude",
            "edit", "cite", "introduce", "section_introduce", "publish",
        ]
        assert names == expected

    def test_stages_order(self):
        names = [name for name, _ in STAGES]
        assert names.index("export") < names.index("parse")
        assert names.index("parse") < names.index("chapters")
        assert names.index("chapters") < names.index("kaliya")
        assert names.index("format") < names.index("conclude")
        assert names.index("conclude") < names.index("edit")
        assert names.index("edit") < names.index("cite")
        assert names.index("cite") < names.index("introduce")
        assert names.index("introduce") < names.index("publish")

    def test_stages_have_callable_classes(self):
        for name, cls in STAGES:
            assert callable(cls), f"Stage {name} class is not callable"


# ===================================================================
# Helper functions
# ===================================================================


class TestHumanSize:
    def test_bytes(self, tmp_path):
        f = tmp_path / "tiny.txt"
        f.write_text("hi")
        assert "B" in _human_size(f)

    def test_kilobytes(self, tmp_path):
        f = tmp_path / "medium.txt"
        f.write_text("x" * 2048)
        assert "KB" in _human_size(f)

    def test_missing_file(self, tmp_path):
        assert _human_size(tmp_path / "nope") == "?"


class TestNormaliseUrl:
    def test_strips_scheme(self):
        assert _normalise_url("https://example.gov/page") == "example.gov/page"

    def test_strips_trailing_slash(self):
        assert _normalise_url("https://example.gov/page/") == "example.gov/page"

    def test_strips_fragment(self):
        assert _normalise_url("https://example.gov/page#sec") == "example.gov/page"

    def test_strips_query(self):
        assert _normalise_url("https://example.gov/page?q=1") == "example.gov/page"

    def test_empty(self):
        assert _normalise_url("") == ""

    def test_lowercase(self):
        assert _normalise_url("HTTPS://EXAMPLE.GOV/Page") == "example.gov/page"


class TestFirstNSentences:
    def test_two_sentences(self):
        text = "First sentence. Second sentence. Third one."
        result = _first_n_sentences(text, 2)
        assert "First sentence." in result
        assert "Second sentence." in result
        assert "Third" not in result

    def test_strips_headings(self):
        text = "# Heading\n\nContent here. More stuff."
        result = _first_n_sentences(text, 1)
        assert "Heading" not in result
        assert "Content here." in result

    def test_empty(self):
        assert _first_n_sentences("", 2) == ""


class TestGeminiLocation:
    def test_preview_model(self):
        assert _gemini_location("gemini-3.1-pro-preview") == "global"

    def test_31_model(self):
        assert _gemini_location("gemini-3.1-flash") == "global"

    def test_regular_model(self):
        assert _gemini_location("gemini-2.5-pro") is None


# ===================================================================
# run_pipeline()
# ===================================================================


class TestRunPipeline:
    def _make_config(self, tmp_path):
        build_dir = tmp_path / "build"
        build_dir.mkdir()
        cfg_dict = {
            "project": {"title": "Pipeline Test"},
            "paths": {"build_dir": str(build_dir)},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")
        return PipelineConfig.from_yaml(yaml_path)

    def test_runs_selected_stages_only(self, tmp_path):
        cfg = self._make_config(tmp_path)
        run_calls = []

        with patch.dict("sys.modules", {}):
            # Mock the ExportStage to track calls
            from src.components.publisher.rapporteur_cli import ExportStage
            original_run = ExportStage.run

            def tracked_run(self):
                run_calls.append("export")
                return tmp_path / "build" / "raw" / "conv.json"

            ExportStage.run = tracked_run
            try:
                run_pipeline(cfg, ["export"], dry_run=True)
            finally:
                ExportStage.run = original_run

        assert "export" in run_calls

    def test_skips_unselected_stages(self, tmp_path):
        cfg = self._make_config(tmp_path)
        run_calls = []

        from src.components.publisher.rapporteur_cli import ParseStage
        original_run = ParseStage.run

        def tracked_run(self):
            run_calls.append("parse")

        ParseStage.run = tracked_run
        try:
            run_pipeline(cfg, ["export"], dry_run=True)
        finally:
            ParseStage.run = original_run

        assert "parse" not in run_calls
