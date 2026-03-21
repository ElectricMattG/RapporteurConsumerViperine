"""Tests for all config dataclasses and PipelineConfig.from_yaml()."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from src.models.publisher import (ChapterSpec, CiteConfig, ConcludeConfig,
                                  ConfluenceConfig, EditConfig,
                                  EOLinkingConfig, ExportConfig, FormatConfig,
                                  IntroduceConfig, KaliyaConfig, PathsConfig,
                                  PipelineConfig, ProjectConfig, PublishConfig,
                                  RenderConfig, SectionDef, SectionsConfig,
                                  SessionsConfig, _resolve)

# ===================================================================
# Individual dataclass defaults
# ===================================================================


class TestProjectConfig:
    def test_defaults(self):
        c = ProjectConfig()
        assert c.title == "Untitled"
        assert c.author == ""
        assert c.date == ""


class TestPathsConfig:
    def test_defaults(self):
        c = PathsConfig()
        assert c.build_dir == ""
        assert c.image_cache == ""
        assert c.kaliya_cache == ""


class TestExportConfig:
    def test_defaults(self):
        c = ExportConfig()
        assert c.frontend_url == "http://localhost:5001"
        assert c.static_file == ""


class TestSessionsConfig:
    def test_defaults(self):
        c = SessionsConfig()
        assert c.include is None

    def test_explicit_include(self):
        c = SessionsConfig(include=[0, 1, 2])
        assert c.include == [0, 1, 2]


class TestChapterSpec:
    def test_construction(self):
        c = ChapterSpec(title="Test Chapter", sessions=[0, 1])
        assert c.title == "Test Chapter"
        assert c.sessions == [0, 1]

    def test_defaults(self):
        c = ChapterSpec()
        assert c.title == ""
        assert c.sessions == []


class TestRenderConfig:
    def test_defaults(self):
        c = RenderConfig()
        assert c.top_sources_n == 10
        assert c.abstract_max_words == 220
        assert c.postprocess is True
        assert c.chapters == []

    def test_with_chapters(self):
        c = RenderConfig(chapters=[ChapterSpec(title="A", sessions=[0])])
        assert len(c.chapters) == 1
        assert c.chapters[0].title == "A"


class TestKaliyaConfig:
    def test_defaults(self):
        c = KaliyaConfig()
        assert c.bootstrap_servers == "192.168.1.3:30094"
        assert c.topic == "kaliya-requests"
        assert c.poll_interval_s == 15
        assert c.poll_timeout_s == 600
        assert c.include_images is True


class TestFormatConfig:
    def test_defaults(self):
        c = FormatConfig()
        assert c.enabled is False
        assert c.style_guide == "docs/style_guide.md"
        assert c.provider == "gemini"
        assert c.model == "gemini-2.5-pro"


class TestCiteConfig:
    def test_defaults(self):
        c = CiteConfig()
        assert c.enabled is False
        assert c.eo_name_max_chars == 60
        assert c.resolve_web_citations is True
        assert c.apa_first_mention_title is True


class TestIntroduceConfig:
    def test_defaults(self):
        c = IntroduceConfig()
        assert c.enabled is False
        assert c.provider == "openai"
        assert c.model == "gpt-5.4"
        assert c.reasoning_effort == "high"
        assert c.include_kaliya is False
        assert c.rag_service_url == "http://localhost:8000"


class TestSectionDef:
    def test_construction(self):
        s = SectionDef(title="Part I", chapters=[1, 2], prompt="custom")
        assert s.title == "Part I"
        assert s.chapters == [1, 2]
        assert s.prompt == "custom"


class TestSectionsConfig:
    def test_defaults(self):
        c = SectionsConfig()
        assert c.enabled is False
        assert c.definitions == []


class TestConfluenceConfig:
    def test_defaults(self):
        c = ConfluenceConfig()
        assert c.server_url == ""
        assert c.space_key == ""
        assert c.parent_page == ""


class TestPublishConfig:
    def test_defaults(self):
        c = PublishConfig()
        assert c.formats == ["pdf"]
        assert c.pdf_engine == "pdflatex"
        assert isinstance(c.confluence, ConfluenceConfig)


class TestEOLinkingConfig:
    def test_defaults(self):
        c = EOLinkingConfig()
        assert c.enabled is False
        assert c.batch_size == 150
        assert c.analysis_provider == "gemini"
        assert c.exclude_eos == []
        assert c.focused_eos == []
        assert c.domain_include == [".gov", ".mil", ".gov.il", "dirco.gov.za"]

    def test_focused_eos(self):
        c = EOLinkingConfig(focused_eos=["14148", "14192"])
        assert c.focused_eos == ["14148", "14192"]


# ===================================================================
# PipelineConfig
# ===================================================================


class TestPipelineConfig:
    def test_defaults(self):
        c = PipelineConfig()
        assert c.project.title == "Untitled"
        assert c.force is False
        assert isinstance(c.eo_linking, EOLinkingConfig)

    def test_build_dir_property(self, tmp_path):
        c = PipelineConfig()
        c.paths.build_dir = str(tmp_path)
        assert c.build_dir == tmp_path


# ===================================================================
# from_yaml()
# ===================================================================


class TestFromYaml:
    def test_minimal(self, tmp_path):
        cfg_dict = {"project": {"title": "Min"}, "paths": {"build_dir": str(tmp_path)}}
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.project.title == "Min"
        assert cfg.conclude.enabled is False  # default
        assert cfg.edit.enabled is False

    def test_all_sections(self, sample_document_yaml):
        cfg = PipelineConfig.from_yaml(sample_document_yaml)
        assert cfg.project.title == "Full Test"
        assert cfg.format.enabled is True
        assert cfg.conclude.enabled is True
        assert cfg.edit.enabled is True
        assert cfg.cite.enabled is True
        assert cfg.introduce.enabled is True
        assert cfg.eo_linking.enabled is True
        assert cfg.eo_linking.exclude_eos == ["14165"]
        assert cfg.eo_linking.focused_eos == ["14148"]
        assert cfg.sections.enabled is True
        assert len(cfg.sections.definitions) == 1
        assert cfg.sections.definitions[0].title == "Part I"

    def test_relative_paths_resolved(self, tmp_path):
        cfg_dict = {
            "project": {"title": "Rel"},
            "paths": {"build_dir": "output"},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert Path(cfg.paths.build_dir).is_absolute()
        assert cfg.paths.build_dir == str((tmp_path / "output").resolve())

    def test_absolute_paths_preserved(self, tmp_path):
        abs_dir = str(tmp_path / "absolute_build")
        cfg_dict = {
            "project": {"title": "Abs"},
            "paths": {"build_dir": abs_dir},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.paths.build_dir == abs_dir

    def test_missing_sections_use_defaults(self, tmp_path):
        cfg_dict = {"project": {"title": "Sparse"}}
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        # Empty build_dir resolves to "." via _resolve()
        assert cfg.paths.build_dir in ("", ".")
        assert cfg.conclude.enabled is False
        assert cfg.edit.enabled is False
        assert cfg.format.enabled is False

    def test_eo_linking_section(self, tmp_path):
        cfg_dict = {
            "project": {"title": "EO"},
            "eo_linking": {
                "enabled": True,
                "exclude_eos": ["14165", "14370"],
                "focused_eos": ["14148"],
                "batch_size": 50,
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.eo_linking.enabled is True
        assert cfg.eo_linking.exclude_eos == ["14165", "14370"]
        assert cfg.eo_linking.focused_eos == ["14148"]
        assert cfg.eo_linking.batch_size == 50

    def test_render_chapters(self, tmp_path):
        cfg_dict = {
            "project": {"title": "Ch"},
            "render": {
                "chapters": [
                    {"title": "Alpha", "sessions": [0, 1]},
                    {"title": "Beta", "sessions": [2]},
                ],
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert len(cfg.render.chapters) == 2
        assert cfg.render.chapters[0].title == "Alpha"
        assert cfg.render.chapters[0].sessions == [0, 1]

    def test_sections_definitions(self, tmp_path):
        cfg_dict = {
            "project": {"title": "Sec"},
            "sections": {
                "definitions": [
                    {"title": "Part A", "chapters": [1, 2], "prompt": "p1"},
                    {"title": "Part B", "chapters": [3]},
                ],
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.sections.enabled is True
        assert len(cfg.sections.definitions) == 2
        assert cfg.sections.definitions[0].prompt == "p1"
        assert cfg.sections.definitions[1].chapters == [3]

    def test_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            PipelineConfig.from_yaml(tmp_path / "nonexistent.yaml")

    def test_style_guide_relative_resolved(self, tmp_path):
        cfg_dict = {
            "project": {"title": "SG"},
            "format": {"enabled": True, "style_guide": "my_style.md"},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        cfg = PipelineConfig.from_yaml(yaml_path)
        assert Path(cfg.format.style_guide).is_absolute()


# ===================================================================
# _resolve helper
# ===================================================================


class TestResolveHelper:
    def test_empty_value(self, tmp_path):
        result = _resolve(tmp_path, "")
        assert result == Path("")

    def test_relative(self, tmp_path):
        result = _resolve(tmp_path, "subdir/file.txt")
        assert result == (tmp_path / "subdir" / "file.txt").resolve()

    def test_absolute(self, tmp_path):
        result = _resolve(tmp_path, "/abs/path")
        assert result == Path("/abs/path")
