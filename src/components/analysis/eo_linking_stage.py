"""EO Event Linking — Pipeline Stage.

Orchestrates the EO linking pipeline:
  Phase 1: extract .gov/.mil articles, build EO registry
  Phase 2: batch articles, query RAG, build conversation JSON
  Phase 3: LINK-REFLECT prompt tuple per batch via Gemini
  Phase 4: extract linkage records from REFLECT, build implementation cache
  Phase 5: generate Mermaid precedence diagrams per chapter
  Phase 6: generate implementation tables per chapter + cross-EO actions
  Phase 7: inject implementation sections into EO book chapters
"""

from __future__ import annotations

import json
from pathlib import Path

from src.components.analysis.eo_linking import (_RAG_PRICING, ArticleRecord,
                                                ChapterEnricher,
                                                ConversationBuilder,
                                                CostTracker, EOChatDriver,
                                                EODiagramGenerator,
                                                EOImplementationCache,
                                                EONameLookup, EORegistry,
                                                GovMilExtractor,
                                                ImplementationTableGenerator,
                                                IndexLoader, LinkageExtractor,
                                                LinkReflectDriver,
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
    Phase 4 — Linkage extraction and implementation cache build.
    Phase 5 — Mermaid precedence diagram generation per chapter.
    Phase 6 — implementation tables per chapter + cross-EO actions.
    Phase 7 — inject implementation sections into EO book chapters.
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

        # Phase 5.5: Reconstitute full EO index via /query_metadata for REFLECT
        from src.components.publisher.rapporteur_cli import \
            _fetch_full_eo_grounding
        eo_article_grounding = _fetch_full_eo_grounding(eo_cfg.rag_service_url)
        if eo_article_grounding:
            self._logger.info(
                "  Reconstituted full EO article grounding (%d chars)",
                len(eo_article_grounding),
            )

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
                eo_article_grounding=eo_article_grounding,
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

        # -- Phase 4: Linkage Extraction & Cache Build ----------------------
        self._logger.info("Starting Phase 4 — Linkage Extraction on %d batches...",
                          len(batches))

        extractor_p4 = LinkageExtractor()
        cache = EOImplementationCache(cache_path=eo_cfg.output_cache_path)

        total_extracted = 0
        total_new = 0
        for batch_idx in range(len(batches)):
            batch_dir = output_dir / f"batch_{batch_idx}"
            reflect_path = batch_dir / "reflect.md"
            if not reflect_path.exists():
                self._logger.warning("Batch %d — no reflect.md, skipping.", batch_idx)
                continue

            # Build article_lookup from conversation.json so the extractor
            # can hydrate URLs, dates, and publisher for every communication.
            article_lookup: dict[str, dict[str, str]] = {}
            conv_path = batch_dir / "conversation.json"
            if conv_path.exists():
                try:
                    conv_items = json.loads(conv_path.read_text(encoding="utf-8"))
                    for item in conv_items:
                        meta = item.get("metadata", {})
                        entry = {
                            "url": meta.get("article_url", ""),
                            "title": meta.get("article_title", ""),
                            "date": meta.get("date", ""),
                            "domain": meta.get("domain", ""),
                            "publisher": meta.get("publisher", ""),
                            "response_text": item.get(
                                "assistant_response", "",
                            )[:5000],
                        }
                        # Key by URL, title, and normalised title
                        if entry["url"]:
                            article_lookup[entry["url"]] = entry
                        if entry["title"]:
                            article_lookup[entry["title"]] = entry
                            article_lookup[entry["title"].strip().lower()] = entry
                except (json.JSONDecodeError, KeyError):
                    self._logger.warning(
                        "Batch %d — could not parse conversation.json", batch_idx,
                    )

            reflect_text = reflect_path.read_text(encoding="utf-8")
            records = extractor_p4.extract(
                reflect_text,
                batch_id=f"batch_{batch_idx}",
                article_lookup=article_lookup or None,
            )
            total_extracted += len(records)
            if records:
                new_count = cache.append(records)
                total_new += new_count

        # Precedence validation
        conflicts = cache.validate_precedence()
        for warning in conflicts:
            self._logger.warning("  Precedence: %s", warning)

        summary = cache.summary()
        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 4 Summary")
        self._logger.info("  Records extracted:  %d", total_extracted)
        self._logger.info("  New records cached: %d", total_new)
        self._logger.info("  Unique EOs:         %d", summary["unique_eos"])
        self._logger.info("  Records with comms: %d", summary["records_with_comms"])
        self._logger.info("  Unique domains:     %d", summary["unique_domains"])
        self._logger.info("  Cache path:         %s", eo_cfg.output_cache_path)
        self._logger.info("  Precedence conflicts: %d", len(conflicts))
        self._logger.info("=" * 60)

        # -- Phase 5: Mermaid Diagram Generation ----------------------------
        self._logger.info("Starting Phase 5 — Mermaid Diagram Generation...")

        # Build EO name lookup (official titles)
        names_path = output_dir / "eo_names.json"
        name_lookup = EONameLookup(cache_path=names_path)
        name_lookup.load()
        eo_index_path = Path.cwd() / eo_cfg.eo_index_folder
        name_lookup.populate(
            impl_cache_path=eo_cfg.output_cache_path,
            eo_index_dir=eo_index_path,
            chapters_dir=eo_cfg.eo_book_dir,
        )
        name_lookup.save()

        diagram_gen = EODiagramGenerator()
        prec_graph = cache.precedence_graph()
        exclude = set(eo_cfg.exclude_eos) if eo_cfg.exclude_eos else None
        manifest = diagram_gen.generate_for_chapters(
            registry=registry,
            precedence_graph=prec_graph,
            output_dir=output_dir,
            name_lookup=name_lookup,
            exclude_eos=exclude,
        )

        # Unified diagram (all EOs in one graph)
        unified = diagram_gen.generate_unified(
            prec_graph, registry, output_dir=output_dir,
            name_lookup=name_lookup, exclude_eos=exclude,
        )

        # Focused diagrams (configurable per-EO subgraphs)
        focused_manifest: list = []
        if eo_cfg.focused_eos:
            diagrams_dir = Path(output_dir) / "diagrams"
            for eo in eo_cfg.focused_eos:
                body = diagram_gen.generate_focused(
                    eo, prec_graph, registry,
                    name_lookup=name_lookup, exclude_eos=exclude,
                )
                out = diagrams_dir / f"focused_{eo}.md"
                header = f"# EO {eo} — Focused Dependency Diagram\n\n"
                out.write_text(
                    header + "```mermaid\n" + body + "\n```\n",
                    encoding="utf-8",
                )
                focused_manifest.append({"eo": eo, "path": str(out)})

        total_warnings = sum(len(m["warnings"]) for m in manifest)
        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 5 Summary")
        self._logger.info("  Per-chapter diagrams: %d", len(manifest))
        self._logger.info("  Total nodes:        %d",
                          sum(m["nodes"] for m in manifest))
        self._logger.info("  Unified diagram:    %d nodes, %d edges",
                          unified["nodes"], unified["edges"])
        self._logger.info("  Focused diagrams:   %d", len(focused_manifest))
        self._logger.info("  Warnings:           %d", total_warnings)
        self._logger.info("  Output dir:         %s/diagrams/", output_dir)
        self._logger.info("=" * 60)

        # -- Phase 6: Implementation Table Generation -----------------------
        self._logger.info("Starting Phase 6 — Implementation Tables...")

        table_gen = ImplementationTableGenerator()
        table_manifest = table_gen.generate_for_chapters(
            registry=registry,
            cache=cache,
            output_dir=output_dir,
            name_lookup=name_lookup,
            exclude_eos=exclude,
        )

        total_rows = sum(e["rows"] for e in table_manifest)

        cross_path = table_gen.generate_cross_eo(
            cache=cache,
            registry=registry,
            output_dir=output_dir,
            name_lookup=name_lookup,
        )

        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 6 Summary")
        self._logger.info("  Chapter tables:     %d", len(table_manifest))
        self._logger.info("  Total rows:         %d", total_rows)
        self._logger.info("  Cross-EO table:     %s", cross_path)
        self._logger.info("  Output dir:         %s/tables/", output_dir)
        self._logger.info("=" * 60)

        # -- Phase 7: Chapter Integration -----------------------------------
        self._logger.info("Starting Phase 7 — Chapter Integration...")

        enricher = ChapterEnricher(
            registry, cache, diagram_gen, table_gen,
            name_lookup=name_lookup,
            exclude_eos=exclude,
        )
        enricher.enrich_chapters(eo_cfg.eo_book_dir)

        self._logger.info("=" * 60)
        self._logger.info("EO Linking — Phase 7 Summary")
        self._logger.info("  Chapters enriched:  %d", len(registry.chapter_texts))
        self._logger.info("  Book dir:           %s", eo_cfg.eo_book_dir)
        self._logger.info("=" * 60)
