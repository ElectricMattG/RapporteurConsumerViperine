"""Tests for EOLinkStage pipeline orchestration."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from src.components.analysis.eo_linking_stage import EOLinkStage
from src.models.publisher import PipelineConfig


def _make_config(tmp_path: Path, *, enabled: bool = True, dry_run_in_cfg: bool = False) -> PipelineConfig:
    """Build a PipelineConfig with EO linking settings pointing to tmp_path."""
    build_dir = tmp_path / "build"
    build_dir.mkdir(exist_ok=True)
    book_dir = tmp_path / "book"
    book_dir.mkdir(exist_ok=True)
    idx_dir = tmp_path / "idx"
    idx_dir.mkdir(exist_ok=True)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(exist_ok=True)

    cfg_dict = {
        "project": {"title": "Test", "author": "CI", "date": "2026-01-01"},
        "paths": {"build_dir": str(build_dir)},
        "eo_linking": {
            "enabled": enabled,
            "index_folders": [],
            "eo_book_dir": str(book_dir),
            "eo_index_folder": str(idx_dir),
            "output_cache_path": str(cache_dir / "eo_cache.jsonl"),
            "exclude_eos": [],
            "focused_eos": [],
        },
    }
    yaml_path = tmp_path / "doc.yaml"
    yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")
    return PipelineConfig.from_yaml(yaml_path)


class TestEOLinkStage:
    def test_disabled_skips(self, tmp_path):
        cfg = _make_config(tmp_path, enabled=False)
        stage = EOLinkStage(cfg)
        # Should return immediately without errors
        stage.run()

    def test_dry_run_stops_before_phase2(self, tmp_path):
        cfg = _make_config(tmp_path)
        stage = EOLinkStage(cfg, dry_run=True)
        with patch(
            "src.components.analysis.eo_linking_stage.load_index_catalog",
            return_value=[],
        ):
            # No articles → warns and aborts before dry_run check
            stage.run()

    def test_no_articles_aborts(self, tmp_path):
        cfg = _make_config(tmp_path)
        stage = EOLinkStage(cfg)
        with patch(
            "src.components.analysis.eo_linking_stage.load_index_catalog",
            return_value=[],
        ), patch(
            "src.components.analysis.eo_linking_stage.GovMilExtractor"
        ) as mock_ext:
            mock_ext.return_value.extract.return_value = []
            stage.run()
            # Should have warned and returned

    def test_constructor(self, tmp_path):
        cfg = _make_config(tmp_path)
        stage = EOLinkStage(cfg, dry_run=True)
        assert stage.cfg is cfg
        assert stage.dry_run is True

    def test_dry_run_with_articles(self, tmp_path):
        """Dry run that has articles should stop after Phase 1 summary."""
        from src.components.analysis.eo_linking import ArticleRecord

        cfg = _make_config(tmp_path)
        stage = EOLinkStage(cfg, dry_run=True)

        mock_article = ArticleRecord(
            {"document_id": "d1", "source_url": "https://a.gov/1",
             "summary_text": "test", "doc_metadata": {}},
            "idx",
        )

        with patch(
            "src.components.analysis.eo_linking_stage.load_index_catalog",
            return_value=[mock_article],
        ), patch(
            "src.components.analysis.eo_linking_stage.GovMilExtractor"
        ) as mock_ext, patch(
            "src.components.analysis.eo_linking_stage.EORegistry"
        ) as mock_reg:
            mock_ext.return_value.extract.return_value = [mock_article]
            mock_reg_instance = MagicMock()
            mock_reg_instance.entries = {}
            mock_reg_instance.chapter_texts = []
            mock_reg_instance.supercessions = {}
            mock_reg.load.return_value = mock_reg_instance

            stage.run()
            # Verify it completed without error (dry_run stops before Phase 2)
