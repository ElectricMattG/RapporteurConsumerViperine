"""Publisher pipeline configuration models.

Dataclasses for the ``document.yaml`` schema used by
``src.components.publisher.rapporteur_cli``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

# ---------------------------------------------------------------------------
# Sub-configs
# ---------------------------------------------------------------------------

@dataclass
class ProjectConfig:
    title: str = "Untitled"
    author: str = ""
    date: str = ""


@dataclass
class PathsConfig:
    build_dir: str = ""
    image_cache: str = ""
    kaliya_cache: str = ""


@dataclass
class ExportConfig:
    frontend_url: str = "http://localhost:5001"
    static_file: str = ""


@dataclass
class SessionsConfig:
    include: list[int] | None = None


@dataclass
class ChapterSpec:
    """A chapter definition — groups one or more sessions under a title."""
    title: str = ""
    sessions: list[int] = field(default_factory=list)


@dataclass
class RenderConfig:
    top_sources_n: int = 10
    abstract_max_words: int = 220
    postprocess: bool = True
    chapters: list[ChapterSpec] = field(default_factory=list)


@dataclass
class KaliyaConfig:
    bootstrap_servers: str = "192.168.1.3:30094"
    topic: str = "kaliya-requests"
    output_path: str = "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"
    supplementary_file: str = ""
    analysis_window: str = ""
    poll_interval_s: int = 15
    poll_timeout_s: int = 600
    include_images: bool = True


@dataclass
class FormatConfig:
    """Style formatter configuration — Phase 7."""
    enabled: bool = False
    style_guide: str = "docs/style_guide.md"
    provider: str = "gemini"
    model: str = "gemini-2.5-pro"


@dataclass
class ConfluenceConfig:
    server_url: str = ""
    space_key: str = ""
    parent_page: str = ""


@dataclass
class PublishConfig:
    formats: list[str] = field(default_factory=lambda: ["pdf"])
    pdf_engine: str = "pdflatex"
    confluence: ConfluenceConfig = field(default_factory=ConfluenceConfig)


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """Centralised config loaded from ``document.yaml``."""

    config_path: Path = field(default_factory=lambda: Path("."))
    project: ProjectConfig = field(default_factory=ProjectConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    export: ExportConfig = field(default_factory=ExportConfig)
    sessions: SessionsConfig = field(default_factory=SessionsConfig)
    render: RenderConfig = field(default_factory=RenderConfig)
    kaliya: KaliyaConfig = field(default_factory=KaliyaConfig)
    format: FormatConfig = field(default_factory=FormatConfig)
    publish: PublishConfig = field(default_factory=PublishConfig)

    @property
    def build_dir(self) -> Path:
        return Path(self.paths.build_dir)

    @classmethod
    def from_yaml(cls, path: str | Path) -> PipelineConfig:
        """Load configuration from a YAML file.

        Relative paths in the YAML are resolved against the YAML file's
        directory.
        """
        yaml_path = Path(path).resolve()
        if not yaml_path.exists():
            raise FileNotFoundError(f"Config not found: {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as fh:
            raw: dict = yaml.safe_load(fh) or {}

        yaml_dir = yaml_path.parent
        cfg = cls(config_path=yaml_path)

        # --- project ---
        p = raw.get("project") or {}
        cfg.project = ProjectConfig(
            title=p.get("title", "Untitled"),
            author=p.get("author", ""),
            date=p.get("date", ""),
        )

        # --- paths (resolve relative to yaml dir) ---
        pa = raw.get("paths") or {}
        cfg.paths = PathsConfig(
            build_dir=str(_resolve(yaml_dir, pa.get("build_dir", ""))),
            image_cache=str(_resolve(yaml_dir, pa.get("image_cache", ""))),
            kaliya_cache=str(_resolve(yaml_dir, pa.get("kaliya_cache", ""))),
        )

        # --- export ---
        e = raw.get("export") or {}
        static = e.get("static_file", "")
        if static:
            static = str(_resolve(yaml_dir, static))
        cfg.export = ExportConfig(
            frontend_url=e.get("frontend_url", "http://localhost:5001"),
            static_file=static,
        )

        # --- sessions ---
        s = raw.get("sessions") or {}
        cfg.sessions = SessionsConfig(include=s.get("include"))

        # --- render ---
        r = raw.get("render") or {}
        chapter_specs = []
        for ch in r.get("chapters") or []:
            chapter_specs.append(ChapterSpec(
                title=ch.get("title", ""),
                sessions=ch.get("sessions", []),
            ))
        cfg.render = RenderConfig(
            top_sources_n=r.get("top_sources_n", 10),
            abstract_max_words=r.get("abstract_max_words", 220),
            postprocess=r.get("postprocess", True),
            chapters=chapter_specs,
        )

        # --- kaliya ---
        k = raw.get("kaliya") or {}
        sup = k.get("supplementary_file", "")
        if sup:
            sup = str(_resolve(yaml_dir, sup))
        cfg.kaliya = KaliyaConfig(
            bootstrap_servers=k.get("bootstrap_servers", "192.168.1.3:30094"),
            topic=k.get("topic", "kaliya-requests"),
            output_path=k.get("output_path", "/mnt/workspace_2/bboa/cache/kaliya_agenda_cache"),
            supplementary_file=sup,
            analysis_window=k.get("analysis_window", ""),
            poll_interval_s=k.get("poll_interval_s", 15),
            poll_timeout_s=k.get("poll_timeout_s", 600),
            include_images=k.get("include_images", True),
        )

        # --- format ---
        fm = raw.get("format") or {}
        sg = fm.get("style_guide", "docs/style_guide.md")
        if sg and not Path(sg).is_absolute():
            sg = str((yaml_dir / sg).resolve())
        cfg.format = FormatConfig(
            enabled=fm.get("enabled", False),
            style_guide=sg,
            provider=fm.get("provider", "gemini"),
            model=fm.get("model", "gemini-2.5-flash"),
        )

        # --- publish ---
        pu = raw.get("publish") or {}
        conf = pu.get("confluence") or {}
        cfg.publish = PublishConfig(
            formats=pu.get("formats", ["pdf"]),
            pdf_engine=pu.get("pdf_engine", "pdflatex"),
            confluence=ConfluenceConfig(
                server_url=conf.get("server_url", ""),
                space_key=conf.get("space_key", ""),
                parent_page=conf.get("parent_page", ""),
            ),
        )

        return cfg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve(base: Path, value: str) -> Path:
    """Resolve a path relative to *base* unless it's already absolute."""
    if not value:
        return Path("")
    p = Path(value)
    return p if p.is_absolute() else (base / p).resolve()
