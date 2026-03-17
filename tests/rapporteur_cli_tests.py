"""Phase-6 integration tests for the Rapporteur CLI pipeline.

Tests 6.1 (end-to-end pipeline) and 6.2 (chapter reference validation)
from the RAPPORTEUR_CLI_PLAN.  Run with:

    pytest tests/rapporteur_cli_tests.py -v
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
STATIC_EXPORT = REPO_ROOT / "conversations_export_20260304_063912.json"
DOCUMENT_YAML = REPO_ROOT / "docs" / "AtrumVolentia" / "document.yaml"
KALIYA_CACHE = Path("/mnt/workspace_2/bboa/cache/kaliya_agenda_cache")
IMAGE_CACHE = Path("/mnt/workspace_2/bboa/cache/image_cache")

# The live export that includes threat-cluster conversations
LIVE_RAW = Path("/mnt/workspace_2/bboa/documents/atrum_volentia/raw/conversations.json")


def _have_static_export() -> bool:
    return STATIC_EXPORT.exists()


def _have_live_export() -> bool:
    return LIVE_RAW.exists()


def _have_kaliya_cache() -> bool:
    return KALIYA_CACHE.is_dir() and any(KALIYA_CACHE.iterdir())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_test_config(
    tmp_dir: Path,
    *,
    static_file: str,
    build_dir: str,
    sessions_include: list[int] | None = None,
    with_kaliya: bool = False,
) -> Path:
    """Write a minimal document.yaml for testing."""
    cfg: dict = {
        "project": {
            "title": "Integration Test Document",
            "author": "CI",
            "date": "2026-03-16",
        },
        "paths": {
            "build_dir": build_dir,
            "image_cache": str(IMAGE_CACHE),
            "kaliya_cache": str(KALIYA_CACHE),
        },
        "export": {
            "static_file": static_file,
        },
        "render": {
            "top_sources_n": 5,
            "abstract_max_words": 100,
            "postprocess": True,
        },
        "publish": {
            "formats": ["pdf"],
            "pdf_engine": "pdflatex",
        },
    }
    if sessions_include is not None:
        cfg["sessions"] = {"include": sessions_include}
    if with_kaliya:
        cfg["kaliya"] = {
            "output_path": str(KALIYA_CACHE),
        }

    yaml_path = tmp_dir / "test_document.yaml"
    yaml_path.write_text(yaml.dump(cfg, default_flow_style=False), encoding="utf-8")
    return yaml_path


def _run_cli(config_path: Path, *extra_args: str) -> None:
    """Import and invoke the CLI main() directly (no subprocess)."""
    from src.components.publisher.rapporteur_cli import main

    argv = [str(config_path), *extra_args]
    main(argv)


# ===================================================================
# 6.1 — End-to-end integration test (static export, no Kaliya)
# ===================================================================

@pytest.mark.skipif(not _have_static_export(), reason="Static export not available")
class TestEndToEndStaticExport:
    """Run export → parse → chapters → publish with a static file."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path):
        """Create a temp build dir and run the pipeline once."""
        self.build_dir = tmp_path / "build"
        self.build_dir.mkdir()

        self.config_path = _write_test_config(
            tmp_path,
            static_file=str(STATIC_EXPORT),
            build_dir=str(self.build_dir),
            sessions_include=[0, 1, 2],  # 3 conversations, quick
        )
        _run_cli(
            self.config_path,
            "--export", "--parse", "--chapters", "--publish",
        )

    def test_raw_export_written(self):
        raw_dir = self.build_dir / "raw"
        assert raw_dir.is_dir()
        convs_file = raw_dir / "conversations.json"
        assert convs_file.exists()
        convs = json.loads(convs_file.read_text(encoding="utf-8"))
        assert len(convs) == 11  # all 11 from the static export

    def test_parsed_sessions(self):
        parsed_dir = self.build_dir / "parsed"
        assert parsed_dir.is_dir()
        session_files = sorted(parsed_dir.glob("session_*.json"))
        assert len(session_files) == 3  # sessions 0, 1, 2

    def test_chapters_written(self):
        chapters_dir = self.build_dir / "chapters"
        assert chapters_dir.is_dir()
        md_files = sorted(f for f in chapters_dir.iterdir() if f.suffix == ".md" and f.name[0].isdigit())
        assert len(md_files) == 3

    def test_manifest_valid(self):
        manifest_path = self.build_dir / "chapters" / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert "chapters" in manifest
        assert len(manifest["chapters"]) == 3
        for ch in manifest["chapters"]:
            assert "filename" in ch
            assert "word_count" in ch
            assert ch["word_count"] > 0

    def test_index_md_exists(self):
        idx = self.build_dir / "chapters" / "index.md"
        assert idx.exists()
        text = idx.read_text(encoding="utf-8")
        assert "Table of Contents" in text or "Integration Test Document" in text

    def test_pdf_generated(self):
        pdf_files = list(self.build_dir.glob("*.pdf"))
        assert len(pdf_files) == 1
        pdf = pdf_files[0]
        assert pdf.stat().st_size > 10_000  # > 10 KB minimum

    def test_manifest_has_publish_metadata(self):
        manifest = json.loads(
            (self.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "publish" in manifest
        pub = manifest["publish"]
        assert "pdf" in pub
        assert pub["word_count"] > 0


# ===================================================================
# 6.1b — Full pipeline with Kaliya (threat-cluster conversations)
# ===================================================================

@pytest.mark.skipif(
    not (_have_live_export() and _have_kaliya_cache()),
    reason="Live export or Kaliya cache not available",
)
class TestEndToEndWithKaliya:
    """Run chapters → kaliya → publish using the live export with clusters."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path):
        """Copy the live export, run chapters → kaliya → publish."""
        self.build_dir = tmp_path / "build"
        self.build_dir.mkdir()

        # Copy the live export as a static file for the test
        raw_dir = self.build_dir / "raw"
        raw_dir.mkdir()
        static_copy = tmp_path / "live_conversations.json"
        import shutil
        shutil.copy2(LIVE_RAW, static_copy)

        self.config_path = _write_test_config(
            tmp_path,
            static_file=str(static_copy),
            build_dir=str(self.build_dir),
            sessions_include=[0, 7, 8],  # include threat-cluster sessions
            with_kaliya=True,
        )
        _run_cli(
            self.config_path,
            "--export", "--parse", "--chapters", "--kaliya", "--publish",
        )

    def test_kaliya_images_created(self):
        img_dir = self.build_dir / "kaliya" / "images"
        assert img_dir.is_dir()
        images = list(img_dir.glob("*.jpeg"))
        assert len(images) > 0

    def test_threat_clusters_extracted(self):
        tc_file = self.build_dir / "parsed" / "threat_clusters.json"
        assert tc_file.exists()
        clusters = json.loads(tc_file.read_text(encoding="utf-8"))
        assert len(clusters) > 0

    def test_kaliya_sections_injected(self):
        chapters_dir = self.build_dir / "chapters"
        md_files = sorted(
            f for f in chapters_dir.iterdir()
            if f.suffix == ".md" and f.name[0].isdigit()
        )
        kaliya_count = 0
        for f in md_files:
            text = f.read_text(encoding="utf-8")
            kaliya_count += text.count("#### Kaliya Agenda Analysis:")
        assert kaliya_count > 0

    def test_pdf_includes_images(self):
        pdf_files = list(self.build_dir.glob("*.pdf"))
        assert len(pdf_files) == 1
        # PDF with images should be substantially larger than text-only
        assert pdf_files[0].stat().st_size > 100_000

    def test_manifest_has_kaliya_metadata(self):
        manifest = json.loads(
            (self.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "kaliya" in manifest
        assert manifest["kaliya"]["injected"] is True


# ===================================================================
# 6.2 — Chapter reference validation
# ===================================================================

@pytest.mark.skipif(not _have_static_export(), reason="Static export not available")
class TestChapterReferences:
    """Validate chapter reference sections are correctly structured."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path):
        self.build_dir = tmp_path / "build"
        self.build_dir.mkdir()

        self.config_path = _write_test_config(
            tmp_path,
            static_file=str(STATIC_EXPORT),
            build_dir=str(self.build_dir),
            sessions_include=[0, 1],
        )
        _run_cli(self.config_path, "--export", "--parse", "--chapters")

    def _chapter_files(self) -> list[Path]:
        chapters_dir = self.build_dir / "chapters"
        return sorted(
            f for f in chapters_dir.iterdir()
            if f.suffix == ".md" and f.name[0].isdigit()
        )

    def test_references_section_exists_once_per_chapter(self):
        """Each chapter has exactly one '## References' section."""
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            ref_count = len(re.findall(r'^## References\s*$', text, re.MULTILINE))
            assert ref_count == 1, (
                f"{f.name}: expected 1 '## References', found {ref_count}"
            )

    def test_no_per_section_references(self):
        """References only at chapter end, not after each section."""
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            # Split on '## References' — anything after should be refs + footer only
            parts = text.split("## References")
            assert len(parts) == 2, f"{f.name}: unexpected split on '## References'"
            # The part before should be the main content
            before = parts[0]
            # No sub-section references
            sub_refs = re.findall(r'^### References\s*$', before, re.MULTILINE)
            assert len(sub_refs) == 0, (
                f"{f.name}: found {len(sub_refs)} '### References' sub-sections"
            )

    def test_references_contain_urls(self):
        """Reference entries should contain URLs."""
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            parts = text.split("## References")
            if len(parts) < 2:
                continue
            ref_section = parts[1]
            urls = re.findall(r'https?://\S+', ref_section)
            assert len(urls) > 0, f"{f.name}: References section has no URLs"

    def test_top_sources_exists_once_per_chapter(self):
        """Each chapter has exactly one '## Top Sources' section."""
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            count = len(re.findall(r'^## Top Sources', text, re.MULTILINE))
            assert count == 1, (
                f"{f.name}: expected 1 '## Top Sources', found {count}"
            )


# ===================================================================
# 6.2b — Reference-only validation on the live build
# ===================================================================

@pytest.mark.skipif(
    not Path("/mnt/workspace_2/bboa/documents/atrum_volentia/chapters").exists(),
    reason="Live build chapters not available",
)
class TestLiveBuildReferences:
    """Validate chapter references on the actual build output."""

    CHAPTERS_DIR = Path("/mnt/workspace_2/bboa/documents/atrum_volentia/chapters")

    def _chapter_files(self) -> list[Path]:
        return sorted(
            f for f in self.CHAPTERS_DIR.iterdir()
            if f.suffix == ".md" and f.name[0].isdigit()
        )

    def test_no_duplicate_reference_sections(self):
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            ref_count = len(re.findall(r'^## References\s*$', text, re.MULTILINE))
            assert ref_count == 1, (
                f"{f.name}: expected 1 '## References', found {ref_count}"
            )

    def test_no_orphaned_sub_references(self):
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            sub_refs = re.findall(r'^### References\s*$', text, re.MULTILINE)
            assert len(sub_refs) == 0, (
                f"{f.name}: found {len(sub_refs)} orphaned '### References'"
            )

    def test_chapter_count_matches_manifest(self):
        manifest_path = self.CHAPTERS_DIR / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected = len(manifest["chapters"])
        actual = len(self._chapter_files())
        assert actual == expected, f"Manifest says {expected}, found {actual} chapter files"

    def test_all_chapters_have_abstract(self):
        """Each chapter starts with a # heading (Sphinx-style structure)."""
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            lines = text.strip().splitlines()
            has_heading = any(line.strip().startswith("#") for line in lines[:3])
            assert has_heading, f"{f.name}: no # heading found"

    def test_footer_present(self):
        for f in self._chapter_files():
            text = f.read_text(encoding="utf-8")
            assert "Generated by Rapporteur CLI" in text, (
                f"{f.name}: missing footer"
            )


# ===================================================================
# 6.3 — Kaliya round-trip (structural validation of cached results)
# ===================================================================

@pytest.mark.skipif(not _have_kaliya_cache(), reason="Kaliya cache not available")
class TestKaliyaCacheStructure:
    """Validate structure of cached Kaliya analyses (no live Kafka needed)."""

    def _analysis_dirs(self) -> list[Path]:
        return [
            d for d in KALIYA_CACHE.iterdir()
            if d.is_dir() and (d / "report.json").exists()
        ]

    def test_analyses_have_required_files(self):
        for d in self._analysis_dirs():
            assert (d / "report.json").exists(), f"{d.name}: missing report.json"
            assert (d / "narrative.md").exists(), f"{d.name}: missing narrative.md"
            assert (d / "figures.json").exists(), f"{d.name}: missing figures.json"

    def test_report_json_structure(self):
        for d in self._analysis_dirs():
            report = json.loads((d / "report.json").read_text(encoding="utf-8"))
            assert "report_id" in report or "request_id" in report
            assert "source_urls" in report
            assert isinstance(report["source_urls"], list)
            assert "confidence" in report

    def test_figures_json_structure(self):
        for d in self._analysis_dirs():
            figs = json.loads((d / "figures.json").read_text(encoding="utf-8"))
            assert isinstance(figs, list)
            for fig in figs:
                assert "image_id" in fig
                assert "local_path" in fig
                assert "caption" in fig

    def test_figure_images_exist(self):
        """All figure images referenced in figures.json exist on disk."""
        missing = []
        for d in self._analysis_dirs():
            figs = json.loads((d / "figures.json").read_text(encoding="utf-8"))
            for fig in figs:
                local = Path(fig["local_path"])
                if not local.exists():
                    missing.append(f"{d.name}/{fig['image_id']}: {local}")
        assert len(missing) == 0, f"Missing images:\n" + "\n".join(missing[:10])

    def test_narrative_not_empty(self):
        for d in self._analysis_dirs():
            narrative = (d / "narrative.md").read_text(encoding="utf-8")
            assert len(narrative) > 100, (
                f"{d.name}: narrative too short ({len(narrative)} chars)"
            )


# ===================================================================
# 8 — ConcludeStage unit tests
# ===================================================================

class TestConcludeConfig:
    """8.1 / 8.2 — ConcludeConfig dataclass and from_yaml() parsing."""

    def test_defaults(self):
        from src.models.publisher import ConcludeConfig
        co = ConcludeConfig()
        assert co.enabled is False
        assert co.provider == "openai"
        assert co.model == "gpt-5.4"
        assert co.reasoning_effort == "high"
        assert co.include_kaliya is True

    def test_from_yaml_with_conclude(self, tmp_path):
        """from_yaml() parses the conclude section correctly."""
        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(tmp_path)},
            "conclude": {
                "enabled": True,
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "reasoning_effort": "medium",
                "include_kaliya": False,
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.conclude.enabled is True
        assert cfg.conclude.provider == "gemini"
        assert cfg.conclude.model == "gemini-2.5-pro"
        assert cfg.conclude.reasoning_effort == "medium"
        assert cfg.conclude.include_kaliya is False

    def test_from_yaml_defaults_without_conclude(self, tmp_path):
        """from_yaml() uses defaults when conclude section is absent."""
        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(tmp_path)},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.conclude.enabled is False
        assert cfg.conclude.provider == "openai"
        assert cfg.conclude.model == "gpt-5.4"


class TestConcludeStageUnit:
    """8.3–8.7 — ConcludeStage unit tests (no LLM calls)."""

    def _make_config(self, tmp_path, *, enabled=True):
        """Create a PipelineConfig with a chapters dir and manifest."""
        build_dir = tmp_path / "build"
        chapters_dir = build_dir / "chapters"
        chapters_dir.mkdir(parents=True)

        # Write two stub chapters
        (chapters_dir / "01-chapter.md").write_text(
            "# Chapter One\n\nSome analysis content.\n", encoding="utf-8",
        )
        (chapters_dir / "02-chapter.md").write_text(
            "# Chapter Two\n\nMore analysis.\n", encoding="utf-8",
        )

        manifest = {
            "chapters": [
                {"filename": "01-chapter.md", "title": "Chapter One", "format_status": "done"},
                {"filename": "02-chapter.md", "title": "Chapter Two", "format_status": "done"},
            ],
        }
        (chapters_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8",
        )

        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(build_dir)},
            "conclude": {
                "enabled": enabled,
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "reasoning_effort": "high",
                "include_kaliya": False,
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        return PipelineConfig.from_yaml(yaml_path)

    def test_skips_when_disabled(self, tmp_path):
        """Stage does nothing when conclude.enabled is false."""
        cfg = self._make_config(tmp_path, enabled=False)

        from src.components.publisher.rapporteur_cli import ConcludeStage
        stage = ConcludeStage(cfg, dry_run=False)
        stage.run()

        manifest = json.loads(
            (cfg.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "conclude" not in manifest

    def test_skips_when_already_generated(self, tmp_path):
        """Stage is idempotent — skips if conclusion was already generated."""
        cfg = self._make_config(tmp_path, enabled=True)
        manifest_path = cfg.build_dir / "chapters" / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["conclude"] = {"generated": True, "model": "test", "provider": "test"}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        from src.components.publisher.rapporteur_cli import ConcludeStage
        stage = ConcludeStage(cfg, dry_run=False)
        stage.run()

        # Should not have added a new chapter file
        chapter_files = sorted(
            (cfg.build_dir / "chapters").glob("[0-9]*.md")
        )
        assert len(chapter_files) == 2

    def test_dry_run_skips_generation(self, tmp_path):
        """Dry run logs but does not write files."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import ConcludeStage
        stage = ConcludeStage(cfg, dry_run=True)
        stage.run()

        manifest = json.loads(
            (cfg.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "conclude" not in manifest

    def test_strip_code_fencing(self):
        from src.components.publisher.rapporteur_cli import ConcludeStage
        assert ConcludeStage._strip_code_fencing("```markdown\n# Hi\n```") == "# Hi"
        assert ConcludeStage._strip_code_fencing("# Hi") == "# Hi"

    def test_load_conclusion_rules(self, tmp_path):
        """_load_conclusion_rules extracts the CONCLUSION section."""
        style = (
            "## STYLE CONVENTIONS\nStyle stuff.\n\n"
            "## CONCLUSION - PRO\nDraw conclusions.\n\n"
            "## EDITORIAL PRINCIPLES\nFinal review.\n"
        )
        sg = tmp_path / "style.md"
        sg.write_text(style, encoding="utf-8")

        from src.components.publisher.rapporteur_cli import ConcludeStage
        from src.models.publisher import PipelineConfig
        stage = ConcludeStage(PipelineConfig(), dry_run=True)
        rules = stage._load_conclusion_rules(sg)
        assert "CONCLUSION" in rules
        assert "Draw conclusions" in rules
        assert "EDITORIAL" not in rules

    def test_build_document_context(self, tmp_path):
        """_build_document_context concatenates chapter files."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import ConcludeStage
        stage = ConcludeStage(cfg, dry_run=True)
        chapters_dir = cfg.build_dir / "chapters"
        manifest = json.loads(
            (chapters_dir / "manifest.json").read_text(encoding="utf-8")
        )
        ctx = stage._build_document_context(
            chapters_dir, manifest["chapters"], include_kaliya=False,
        )
        assert "Chapter One" in ctx
        assert "Chapter Two" in ctx
        assert "---" in ctx  # separator

    def test_conclude_prompt_contains_constraints(self):
        from src.components.publisher.rapporteur_cli import ConcludeStage
        from src.models.publisher import PipelineConfig
        stage = ConcludeStage(PipelineConfig(), dry_run=True)
        prompt = stage._build_conclude_prompt("doc text", "conclusion rules", "style rules")
        assert "ONE # heading" in prompt
        assert "numeric prefixes" in prompt
        assert "doc text" in prompt

    def test_reflect_prompt_contains_criteria(self):
        from src.components.publisher.rapporteur_cli import ConcludeStage
        from src.models.publisher import PipelineConfig
        stage = ConcludeStage(PipelineConfig(), dry_run=True)
        prompt = stage._build_reflect_prompt("draft", "doc context", "rules")
        assert "protective-function" in prompt
        assert "Mermaid timeline" in prompt
        assert "draft" in prompt


class TestConcludeCLIFlags:
    """8.8 — CLI flag handling for --conclude/--no-conclude."""

    def test_conclude_flag_registered(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        # Should not raise
        args = parser.parse_args(["config.yaml", "--conclude"])
        assert args.conclude is True

    def test_no_conclude_flag(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-conclude", "--publish"])
        assert args.conclude is False

    def test_default_conclude_is_none(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--publish"])
        assert args.conclude is None


class TestConcludeStageRegistration:
    """8.9 — ConcludeStage is in the STAGES list."""

    def test_stages_contains_conclude(self):
        from src.components.publisher.rapporteur_cli import STAGES
        names = [name for name, _ in STAGES]
        assert "conclude" in names

    def test_conclude_after_format_before_publish(self):
        from src.components.publisher.rapporteur_cli import STAGES
        names = [name for name, _ in STAGES]
        fmt_idx = names.index("format")
        conclude_idx = names.index("conclude")
        pub_idx = names.index("publish")
        assert fmt_idx < conclude_idx < pub_idx


# ===========================================================================
# Phase 9 — EditStage tests
# ===========================================================================

class TestEditConfig:
    """9.1 — EditConfig dataclass and from_yaml() parsing."""

    def test_defaults(self):
        from src.models.publisher import EditConfig
        ec = EditConfig()
        assert ec.enabled is False
        assert ec.style_guide == "docs/style_guide.md"
        assert ec.provider == "openai"
        assert ec.model == "gpt-5.4"
        assert ec.reasoning_effort == "high"
        assert ec.edit_log is True
        assert ec.executive_summary is True

    def test_from_yaml_with_edit(self, tmp_path):
        """from_yaml() parses the edit section correctly."""
        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(tmp_path)},
            "edit": {
                "enabled": True,
                "provider": "gemini",
                "model": "gemini-2.5-pro",
                "reasoning_effort": "medium",
                "style_guide": "custom/style.md",
                "edit_log": False,
                "executive_summary": False,
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.edit.enabled is True
        assert cfg.edit.provider == "gemini"
        assert cfg.edit.model == "gemini-2.5-pro"
        assert cfg.edit.reasoning_effort == "medium"
        assert cfg.edit.edit_log is False
        assert cfg.edit.executive_summary is False

    def test_from_yaml_defaults_without_edit(self, tmp_path):
        """from_yaml() uses defaults when edit section is absent."""
        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(tmp_path)},
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        cfg = PipelineConfig.from_yaml(yaml_path)
        assert cfg.edit.enabled is False
        assert cfg.edit.provider == "openai"
        assert cfg.edit.model == "gpt-5.4"


class TestEditStageUnit:
    """9.2–9.7 — EditStage unit tests (no LLM calls)."""

    def _make_config(self, tmp_path, *, enabled=True):
        """Create a PipelineConfig with chapters, conclusion, and manifest."""
        build_dir = tmp_path / "build"
        chapters_dir = build_dir / "chapters"
        chapters_dir.mkdir(parents=True)

        # Write stub chapters
        (chapters_dir / "01-chapter.md").write_text(
            "# Chapter One\n\nSome analysis content.\n", encoding="utf-8",
        )
        (chapters_dir / "02-chapter.md").write_text(
            "# Chapter Two\n\nMore analysis.\n", encoding="utf-8",
        )
        # Conclusion chapter
        (chapters_dir / "10-conclusion.md").write_text(
            "# Conclusion\n\nFinal thoughts.\n", encoding="utf-8",
        )

        manifest = {
            "chapters": [
                {"filename": "01-chapter.md", "title": "Chapter One", "format_status": "done"},
                {"filename": "02-chapter.md", "title": "Chapter Two", "format_status": "done"},
                {"filename": "10-conclusion.md", "title": "Conclusion", "format_status": "done"},
            ],
        }
        (chapters_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2), encoding="utf-8",
        )

        # Write a style guide with EDITORIAL PRINCIPLES
        style_guide = tmp_path / "style.md"
        style_guide.write_text(
            "## STYLE CONVENTIONS\nUse formal tone.\n\n"
            "## EDITORIAL PRINCIPLES\nProtective-function premise.\n"
            "Confirmation bias guard.\n\n"
            "## CONCLUSION - PRO\nDraw conclusions.\n",
            encoding="utf-8",
        )

        cfg_dict = {
            "project": {"title": "Test"},
            "paths": {"build_dir": str(build_dir)},
            "edit": {
                "enabled": enabled,
                "provider": "openai",
                "model": "gpt-5.4",
                "style_guide": str(style_guide),
                "edit_log": True,
                "executive_summary": True,
            },
        }
        yaml_path = tmp_path / "doc.yaml"
        yaml_path.write_text(yaml.dump(cfg_dict), encoding="utf-8")

        from src.models.publisher import PipelineConfig
        return PipelineConfig.from_yaml(yaml_path)

    def test_skips_when_disabled(self, tmp_path):
        """Stage does nothing when edit.enabled is false."""
        cfg = self._make_config(tmp_path, enabled=False)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=False)
        stage.run()

        manifest = json.loads(
            (cfg.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "edit" not in manifest

    def test_skips_when_already_reviewed(self, tmp_path):
        """Stage is idempotent — skips if already reviewed."""
        cfg = self._make_config(tmp_path, enabled=True)
        manifest_path = cfg.build_dir / "chapters" / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["edit"] = {"reviewed": True, "model": "test", "provider": "test"}
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=False)
        stage.run()

        # Chapters should be unchanged
        ch1 = (cfg.build_dir / "chapters" / "01-chapter.md").read_text(encoding="utf-8")
        assert ch1 == "# Chapter One\n\nSome analysis content.\n"

    def test_dry_run_skips_review(self, tmp_path):
        """Dry run logs but does not write edit log or modify chapters."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=True)
        stage.run()

        manifest = json.loads(
            (cfg.build_dir / "chapters" / "manifest.json").read_text(encoding="utf-8")
        )
        assert "edit" not in manifest
        assert not (cfg.build_dir / "editorial_edit_log.json").exists()

    def test_strip_code_fencing(self):
        from src.components.publisher.rapporteur_cli import EditStage
        assert EditStage._strip_code_fencing("```markdown\n# Hi\n```") == "# Hi"
        assert EditStage._strip_code_fencing("# Hi") == "# Hi"
        assert EditStage._strip_code_fencing("```json\n{}\n```") == "{}"

    def test_load_editorial_principles(self, tmp_path):
        """_load_editorial_principles extracts the EDITORIAL PRINCIPLES section."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=True)
        principles = stage._load_editorial_principles(Path(cfg.edit.style_guide))
        assert "EDITORIAL PRINCIPLES" in principles
        assert "Protective-function premise" in principles
        assert "Confirmation bias guard" in principles
        assert "STYLE CONVENTIONS" not in principles

    def test_load_style_rules(self, tmp_path):
        """_load_style_rules extracts the STYLE CONVENTIONS section."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=True)
        rules = stage._load_style_rules(Path(cfg.edit.style_guide))
        assert "STYLE CONVENTIONS" in rules
        assert "formal tone" in rules
        assert "EDITORIAL PRINCIPLES" not in rules

    def test_build_document_context_includes_conclusion(self, tmp_path):
        """_build_document_context includes all chapters incl. conclusion."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=True)
        chapters_dir = cfg.build_dir / "chapters"
        manifest = json.loads(
            (chapters_dir / "manifest.json").read_text(encoding="utf-8")
        )
        ctx = stage._build_document_context(chapters_dir, manifest["chapters"])
        assert "Chapter One" in ctx
        assert "Chapter Two" in ctx
        assert "Conclusion" in ctx
        assert "Final thoughts" in ctx
        assert "---" in ctx

    def test_review_prompt_contains_criteria(self, tmp_path):
        """Review prompt includes assessment criteria and chapter list."""
        cfg = self._make_config(tmp_path, enabled=True)

        from src.components.publisher.rapporteur_cli import EditStage
        stage = EditStage(cfg, dry_run=True)
        prompt = stage._build_review_prompt(
            "document text",
            "editorial principles text",
            "style rules text",
            [{"filename": "01-chapter.md", "title": "Test Chapter"}],
        )
        assert "confirmation bias" in prompt.lower()
        assert "protective-function" in prompt
        assert "alternative hypotheses" in prompt
        assert "document_score" in prompt
        assert "executive_summaries" in prompt
        assert "Chapter 1: Test Chapter" in prompt
        assert "document text" in prompt
        assert "editorial principles text" in prompt

    def test_apply_prompt_contains_rules(self):
        """Apply prompt includes preservation rules and edit log."""
        from src.components.publisher.rapporteur_cli import EditStage
        from src.models.publisher import PipelineConfig
        stage = EditStage(PipelineConfig(), dry_run=True)

        edits = [{"chapter": 1, "section": "Intro", "issue": "tone", "recommendation": "fix"}]
        prompt = stage._build_apply_prompt(
            "# Test\n\nContent here.\n", edits, "Chapter summary.", True,
        )
        assert "Preserve ALL" in prompt
        assert "citation" in prompt.lower()
        assert "ONE # heading" in prompt
        assert "Executive Summary" in prompt
        assert "Chapter summary." in prompt
        assert json.dumps(edits, indent=2) in prompt

    def test_apply_prompt_without_executive_summary(self):
        """Apply prompt omits executive summary when disabled."""
        from src.components.publisher.rapporteur_cli import EditStage
        from src.models.publisher import PipelineConfig
        stage = EditStage(PipelineConfig(), dry_run=True)

        prompt = stage._build_apply_prompt(
            "# Test\n\nContent.\n", [], "", False,
        )
        assert "Executive Summary" not in prompt
        assert "No structural edits needed" in prompt

    def test_parse_edit_log_valid_json(self):
        """_parse_edit_log handles clean JSON."""
        from src.components.publisher.rapporteur_cli import EditStage
        from src.models.publisher import PipelineConfig
        stage = EditStage(PipelineConfig(), dry_run=True)

        valid_json = json.dumps({
            "document_score": {
                "stylistic_consistency": 8,
                "argument_cohesiveness": 7,
                "bias_risk": 3,
                "logical_coherence": 8,
            },
            "executive_summaries": [
                {"chapter": 1, "summary": "Summary one."},
            ],
            "edits": [
                {"chapter": 1, "section": "Intro", "issue": "tone",
                 "recommendation": "be formal", "severity": "low",
                 "type": "stylistic_consistency"},
            ],
        })
        result = stage._parse_edit_log(valid_json)
        assert result is not None
        assert result["document_score"]["stylistic_consistency"] == 8
        assert len(result["executive_summaries"]) == 1
        assert len(result["edits"]) == 1

    def test_parse_edit_log_with_fences(self):
        """_parse_edit_log handles JSON wrapped in code fences."""
        from src.components.publisher.rapporteur_cli import EditStage
        from src.models.publisher import PipelineConfig
        stage = EditStage(PipelineConfig(), dry_run=True)

        fenced = '```json\n{"document_score": {"stylistic_consistency": 9}, "edits": []}\n```'
        result = stage._parse_edit_log(fenced)
        assert result is not None
        assert result["document_score"]["stylistic_consistency"] == 9

    def test_parse_edit_log_invalid(self):
        """_parse_edit_log returns None for garbage input."""
        from src.components.publisher.rapporteur_cli import EditStage
        from src.models.publisher import PipelineConfig
        stage = EditStage(PipelineConfig(), dry_run=True)

        result = stage._parse_edit_log("not json at all")
        assert result is None


class TestEditCLIFlags:
    """9.8 — CLI flag handling for --edit/--no-edit."""

    def test_edit_flag_registered(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--edit"])
        assert args.edit is True

    def test_no_edit_flag(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--no-edit", "--publish"])
        assert args.edit is False

    def test_default_edit_is_none(self):
        from src.components.publisher.rapporteur_cli import build_parser
        parser = build_parser()
        args = parser.parse_args(["config.yaml", "--publish"])
        assert args.edit is None


class TestEditStageRegistration:
    """9.9 — EditStage is in the STAGES list in correct position."""

    def test_stages_contains_edit(self):
        from src.components.publisher.rapporteur_cli import STAGES
        names = [name for name, _ in STAGES]
        assert "edit" in names

    def test_edit_after_conclude_before_publish(self):
        from src.components.publisher.rapporteur_cli import STAGES
        names = [name for name, _ in STAGES]
        conclude_idx = names.index("conclude")
        edit_idx = names.index("edit")
        pub_idx = names.index("publish")
        assert conclude_idx < edit_idx < pub_idx
