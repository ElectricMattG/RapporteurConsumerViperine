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
