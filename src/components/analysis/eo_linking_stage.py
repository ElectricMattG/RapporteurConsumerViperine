"""EO Event Linking — Pipeline Stage.

Orchestrates the EO linking pipeline:
  Phase 1: extract .gov/.mil articles, build EO registry
  Phase 2: batch articles, query RAG, build conversation JSON
  Phase 3: LINK-REFLECT prompt tuple per batch via Gemini
"""

from __future__ import annotations

import json
from pathlib import Path

from src.components.analysis.eo_linking import (_RAG_PRICING, ArticleRecord,
                                                ConversationBuilder,
                                                CostTracker, EOChatDriver,
                                                EORegistry, GovMilExtractor,
                                                IndexLoader, LinkReflectDriver,
                                                TokenBudgetBatcher,
                                                load_index_catalog)
from src.config_handler import Base
from src.models.publisher import PipelineConfig


class EOLinkStage(Base):
    """Pipeline stage for EO Event Linking.

    Phase 1 — loads cross-index catalog, filters to .gov/.mil articles,
    builds EO registry.
    Phase 2 — batches articles, queries RAG per article, writes
    per-batch conversation JSON.
    Phase 3 — LINK-REFLECT two-call LLM synthesis per batch.
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

        # -- Phase 2: Token-Budget Batching & RAG Chat ----------------------
        batcher = TokenBudgetBatcher(batch_size=eo_cfg.batch_size)
        batches = batcher.batch(gov_articles)

        driver = EOChatDriver(
            rag_service_url=eo_cfg.rag_service_url,
            top_k=eo_cfg.rag_summaries,
            qa_top_k=eo_cfg.rag_qa_pairs,
        )

        output_dir = Path(eo_cfg.output_cache_path).parent
        builder = ConversationBuilder(output_dir=output_dir)
        tracker = CostTracker()

        for batch_idx, batch in enumerate(batches):
            self._logger.info(
                "Processing batch %d/%d (%d articles)...",
                batch_idx + 1, len(batches), len(batch),
            )
            chat_results = driver.query_batch(batch)
            tracker.record(chat_results)
            builder.build_and_save(batch_idx, chat_results)

        # -- Phase 2 Summary ------------------------------------------------
        cost = tracker.summary(model=eo_cfg.analysis_model)
        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 2 Summary")
        self._logger.info("  Batches processed:  %d", len(batches))
        self._logger.info("  RAG calls:          %d", cost["total_calls"])
        self._logger.info("  Total time:         %.1fs", cost["total_time_s"])
        self._logger.info("  Estimated cost:     $%.4f", cost["estimated_cost_usd"])
        self._logger.info("=" * 60)

        # -- Phase 3: LINK-REFLECT per batch --------------------------------
        self._logger.info("Starting Phase 3 — LINK-REFLECT on %d batches...",
                          len(batches))

        lr_driver = LinkReflectDriver(
            model_name=eo_cfg.analysis_model,
            location=eo_cfg.analysis_location,
            output_dir=output_dir,
        )

        eo_grounding = registry.full_grounding_text
        completed = 0
        skipped = 0

        for batch_idx in range(len(batches)):
            conv_path = output_dir / f"batch_{batch_idx}" / "conversation.json"
            batch_convs = lr_driver.format_conversations_for_prompt(conv_path)
            if not batch_convs:
                self._logger.warning(
                    "Batch %d — no conversation data, skipping.", batch_idx,
                )
                continue

            result = lr_driver.process_batch(
                batch_index=batch_idx,
                eo_grounding=eo_grounding,
                batch_conversations=batch_convs,
            )
            if result.get("status") == "complete":
                completed += 1
            else:
                skipped += 1

        # -- Phase 3 Summary ------------------------------------------------
        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 3 Summary")
        self._logger.info("  Batches completed:  %d", completed)
        self._logger.info("  Batches skipped:    %d", skipped)
        self._logger.info("  LLM input tokens:   %d", lr_driver.total_usage["input"])
        self._logger.info("  LLM output tokens:  %d", lr_driver.total_usage["output"])
        self._logger.info("  LLM thinking tokens:%d", lr_driver.total_usage["thinking"])

        pricing = _RAG_PRICING.get(eo_cfg.analysis_model, {"input": 0, "output": 0})
        est_cost = (
            (lr_driver.total_usage["input"] / 1_000_000) * pricing["input"]
            + (lr_driver.total_usage["output"] / 1_000_000) * pricing["output"]
        )
        self._logger.info("  Estimated cost:     $%.4f", est_cost)
        self._logger.info("=" * 60)

        # -- Phase 4+ placeholder -------------------------------------------
        # TODO: LinkageExtractor, EOImplementationCache
        self._logger.info(
            "Phase 3 complete — %d batches linked. "
            "Phase 4 (Linkage Extraction) not yet implemented.",
            completed,
        )
