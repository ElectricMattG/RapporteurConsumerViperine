"""EO Event Linking — Pipeline Stage.

Orchestrates the EO linking pipeline:
  Phase 1: extract .gov/.mil articles, build EO registry
  Phase 2+: batching, RAG chat, LINK-REFLECT (to be implemented)
"""

from __future__ import annotations

import json
from pathlib import Path

from src.components.analysis.eo_linking import (ArticleRecord, EORegistry,
                                                GovMilExtractor, IndexLoader,
                                                load_index_catalog)
from src.config_handler import Base
from src.models.publisher import PipelineConfig


class EOLinkStage(Base):
    """Pipeline stage for EO Event Linking.

    Phase 1 implementation — loads cross-index catalog, filters to
    .gov/.mil articles, builds EO registry, and validates readiness
    for the LINK-REFLECT pipeline.
    """

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        eo_cfg = self.cfg.eo_linking
        if not eo_cfg.enabled:
            self._logger.info("  EO linking disabled — skipping.")
            return

        # -- Phase 1.3: Load cross-index catalog and extract .gov/.mil ------
        self._logger.info("Loading cross-index catalog from %d indices...",
                          len(eo_cfg.index_folders))

        catalog = load_index_catalog(
            index_folders=eo_cfg.index_folders,
            base_dir=Path.cwd(),
        )

        extractor = GovMilExtractor(
            domain_include=eo_cfg.domain_include,
            domain_exclude=eo_cfg.domain_exclude,
        )
        gov_articles = extractor.extract(catalog)

        if not gov_articles:
            self._logger.warning("No .gov/.mil articles found — aborting.")
            return

        # -- Phase 1.4: Build EO registry -----------------------------------
        self._logger.info("Building EO registry from %s ...", eo_cfg.eo_book_dir)

        eo_index_path = Path.cwd() / eo_cfg.eo_index_folder
        registry = EORegistry.load(
            eo_book_dir=eo_cfg.eo_book_dir,
            eo_index_folder=eo_index_path,
        )

        # -- Ensure output directory exists ---------------------------------
        output_dir = Path(eo_cfg.output_cache_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # -- Summary ---------------------------------------------------------
        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 1 Summary")
        self._logger.info("  Total catalog:      %d articles", len(catalog))
        self._logger.info("  .gov/.mil filtered: %d articles", len(gov_articles))
        self._logger.info("  EO registry:        %d entries", len(registry.entries))
        self._logger.info("  Chapter files:      %d", len(registry.chapter_texts))
        self._logger.info("  Supercessions:      %d links",
                          sum(len(v) for v in registry.supercessions.values()))
        self._logger.info("  Output path:        %s", eo_cfg.output_cache_path)
        self._logger.info("=" * 60)

        if self.dry_run:
            self._logger.info("  Dry run — stopping before Phase 2.")
            return

        # -- Phase 2+ placeholder -------------------------------------------
        # TODO: TokenBudgetBatcher, EOChatDriver, LinkReflectStage
        self._logger.info("Phase 2+ not yet implemented. "
                          "Phase 1 extraction complete — %d articles ready.",
                          len(gov_articles))
