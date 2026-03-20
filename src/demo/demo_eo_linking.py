#!/usr/bin/env python3
"""
demo_eo_linking.py — standalone demo for EO Event Linking (Phase 1 + 2 + 3 + 4 + 5).

Runs against the real x_indices_hegseth / x_indices_trump data and the
live EO book chapters.  Phase 2 optionally queries the RAG service at
localhost:8080 (skipped with --dry-run).  Phase 3 runs LINK-REFLECT
via Gemini.  Phase 4 extracts linkage records and builds the cache.
Phase 5 generates Mermaid precedence diagrams per chapter.

Run from the project root:
    python -m src.demo.demo_eo_linking              # Phase 1 only (dry run)
    python -m src.demo.demo_eo_linking --live        # Phase 1 + Phase 2 (needs RAG up)
    python -m src.demo.demo_eo_linking --live -n 5   # Phase 2 limited to first 5 articles
    python -m src.demo.demo_eo_linking --phase3      # Phase 3 only (needs batch_*/conversation.json)
    python -m src.demo.demo_eo_linking --phase4      # Phase 4 only (needs batch_*/reflect.md)
    python -m src.demo.demo_eo_linking --phase5      # Phase 5 only (needs eo_implementation_cache.jsonl)
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path bootstrap
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.components.analysis.eo_linking import ArticleRecord  # noqa: E402
from src.components.analysis.eo_linking import (ChapterEnricher,
                                                ConversationBuilder,
                                                EOChatDriver,
                                                EODiagramGenerator,
                                                EOImplementationCache,
                                                EONameLookup, EORegistry,
                                                GovMilExtractor,
                                                ImplementationTableGenerator,
                                                LinkageExtractor,
                                                LinkReflectDriver,
                                                TokenBudgetBatcher,
                                                load_index_catalog)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INDEX_FOLDERS = ["x_indices_hegseth", "x_indices_trump"]
DOMAIN_INCLUDE = [".gov", ".mil", ".gov.il", "dirco.gov.za"]
EO_BOOK_DIR = "/mnt/workspace_2/bboa/documents/culture_technology_procurement_media/chapters"
EO_INDEX_FOLDER = "x_indices_eo"
RAG_URL = "http://localhost:8000"
BATCH_SIZE = 150
OUTPUT_DIR = "/mnt/workspace_2/bboa/workflows/eo_linking"

# Rough token estimate: ~4 chars per token for English text
_CHARS_PER_TOKEN = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
)
log = logging.getLogger("demo_eo_linking")


def _estimate_batch_tokens(batch: list[ArticleRecord]) -> int:
    """Estimate input tokens for a batch based on summary text length."""
    total_chars = sum(len(a.summary_text or "") + len(a.title or "") for a in batch)
    return total_chars // _CHARS_PER_TOKEN


# ---------------------------------------------------------------------------
# Phase 1
# ---------------------------------------------------------------------------

def run_phase1() -> tuple[list[ArticleRecord], EORegistry]:
    """Load catalog, filter .gov/.mil, build EO registry."""
    log.info("=" * 60)
    log.info("PHASE 1 — Extraction & Registry")
    log.info("=" * 60)

    # 1. Load cross-index catalog
    catalog = load_index_catalog(
        index_folders=INDEX_FOLDERS,
        base_dir=PROJECT_ROOT,
    )

    # 2. Extract .gov/.mil
    extractor = GovMilExtractor(
        domain_include=DOMAIN_INCLUDE,
        domain_exclude=[],
    )
    gov_articles = extractor.extract(catalog)

    # 3. Build EO registry
    eo_index_path = PROJECT_ROOT / EO_INDEX_FOLDER
    registry = EORegistry.load(
        eo_book_dir=EO_BOOK_DIR,
        eo_index_folder=eo_index_path,
    )

    # 4. Print summary
    log.info("-" * 60)
    log.info("Phase 1 Results:")
    log.info("  Total catalog:      %d articles", len(catalog))
    log.info("  .gov/.mil filtered: %d articles", len(gov_articles))
    log.info("  EO registry:        %d EOs", len(registry.entries))
    log.info("  Chapters loaded:    %d", len(registry.chapter_texts))
    sup_count = sum(len(v) for v in registry.supercessions.values())
    log.info("  Supercession links: %d", sup_count)
    log.info("-" * 60)

    # Sample articles
    log.info("Sample .gov/.mil articles (first 10):")
    for i, art in enumerate(gov_articles[:10]):
        log.info("  [%d] %-50s  %s", i, art.domain, art.title[:60])

    # Sample EOs
    log.info("Sample EOs (first 10):")
    for eo_num, entry in list(registry.entries.items())[:10]:
        log.info("  EO %s  %-50s  events=%d",
                 eo_num, entry.title[:50], len(entry.events))

    return gov_articles, registry


# ---------------------------------------------------------------------------
# Phase 2
# ---------------------------------------------------------------------------

def run_phase2(
    gov_articles: list[ArticleRecord],
    max_articles: int | None = None,
) -> None:
    """Batch, query RAG, build conversation JSON."""
    log.info("=" * 60)
    log.info("PHASE 2 — Token-Budget Batching & RAG Chat")
    log.info("=" * 60)

    articles = gov_articles[:max_articles] if max_articles else gov_articles
    log.info("Running Phase 2 on %d articles (of %d total)",
             len(articles), len(gov_articles))

    # 1. Batch
    batcher = TokenBudgetBatcher(batch_size=BATCH_SIZE)
    batches = batcher.batch(articles)

    # 2. RAG driver
    driver = EOChatDriver(
        rag_service_url=RAG_URL,
        top_k=3,
        qa_top_k=3,
    )

    # 3. Conversation builder
    builder = ConversationBuilder(output_dir=OUTPUT_DIR)

    for batch_idx, batch in enumerate(batches):
        est_tokens = _estimate_batch_tokens(batch)
        log.info("Batch %d/%d — %d articles  (~%dk tokens)",
                 batch_idx + 1, len(batches), len(batch), est_tokens // 1000)
        folders = {a.index_folder for a in batch}
        log.info("  Index folders in batch: %s", ", ".join(sorted(folders)))

        chat_results = driver.query_batch(batch)
        conv_path = builder.build_and_save(batch_idx, chat_results)

        # Show a sample result from this batch
        for r in chat_results[:2]:
            content_len = len(r.get("article_content") or "")
            log.info("  ├─ %s", r.get("article_title", "")[:60])
            log.info("  │  domain=%s  publisher=%s  date=%s  content=%d chars",
                     r.get("domain", ""), r.get("publisher", ""),
                     r.get("date", ""), content_len)

    # 4. Summary
    log.info("-" * 60)
    log.info("Phase 2 Results (metadata-only, no LLM calls):")
    log.info("  Batches:          %d", len(batches))
    log.info("  Articles queried: %d", len(articles))
    log.info("-" * 60)


# ---------------------------------------------------------------------------
# Phase 3
# ---------------------------------------------------------------------------

def run_phase3(
    registry: EORegistry,
    max_batches: int | None = None,
) -> None:
    """Run LINK-REFLECT on existing batch conversation JSON files."""
    log.info("=" * 60)
    log.info("PHASE 3 — LINK-REFLECT Prompt Tuple")
    log.info("=" * 60)

    output_dir = Path(OUTPUT_DIR)
    # Discover existing batch dirs
    batch_dirs = sorted(output_dir.glob("batch_*"))
    if not batch_dirs:
        log.warning("No batch_* directories found in %s — run Phase 2 first.", OUTPUT_DIR)
        return

    if max_batches:
        batch_dirs = batch_dirs[:max_batches]

    total_found = len(sorted(output_dir.glob("batch_*")))
    log.info("Found %d batch directories, processing %d", total_found, len(batch_dirs))

    driver = LinkReflectDriver(
        model_name="gemini-3.1-pro-preview",
        location="global",
        output_dir=output_dir,
    )

    eo_grounding = registry.full_grounding_text
    log.info("EO grounding: %d chars (~%dk tokens)",
             len(eo_grounding), len(eo_grounding) // _CHARS_PER_TOKEN // 1000)

    completed = 0
    for batch_dir in batch_dirs:
        batch_idx = int(batch_dir.name.split("_")[1])
        conv_path = batch_dir / "conversation.json"
        batch_convs = driver.format_conversations_for_prompt(conv_path)
        if not batch_convs:
            log.warning("Batch %d — no conversation data, skipping.", batch_idx)
            continue

        log.info("Batch %d: conversation=%dk chars, grounding=%dk chars",
                 batch_idx, len(batch_convs) // 1000, len(eo_grounding) // 1000)

        result = driver.process_batch(
            batch_index=batch_idx,
            eo_grounding=eo_grounding,
            batch_conversations=batch_convs,
        )
        if result.get("status") == "complete":
            completed += 1
            link_tok = result.get("link_tokens", {})
            refl_tok = result.get("reflect_tokens", {})
            log.info("  └─ LINK:    %d chars, %d in / %d out tokens",
                     result.get("link_chars", 0),
                     link_tok.get("input", 0), link_tok.get("output", 0))
            log.info("  └─ REFLECT: %d chars, %d in / %d out tokens",
                     result.get("reflect_chars", 0),
                     refl_tok.get("input", 0), refl_tok.get("output", 0))

    log.info("-" * 60)
    log.info("Phase 3 Results:")
    log.info("  Batches completed:   %d / %d", completed, len(batch_dirs))
    log.info("  Total input tokens:  %d", driver.total_usage["input"])
    log.info("  Total output tokens: %d", driver.total_usage["output"])
    log.info("  Thinking tokens:     %d", driver.total_usage["thinking"])
    log.info("-" * 60)


# ---------------------------------------------------------------------------
# Phase 4
# ---------------------------------------------------------------------------

CACHE_PATH = Path(OUTPUT_DIR) / "eo_implementation_cache.jsonl"


def run_phase4(articles: list[ArticleRecord] | None = None) -> None:
    """Extract linkage records from REFLECT outputs and build cache.

    If `articles` is provided, their metadata (url, title, date, publisher) is used
    to hydrate missing fields in the LLM output.
    """
    log.info("=" * 60)
    log.info("PHASE 4 — Linkage Extraction & Cache Build")
    log.info("=" * 60)

    # Build lookup map from passed articles
    article_lookup: dict[str, dict[str, str]] = {}
    if articles:
        for a in articles:
            meta = {
                "url": a.source_url,
                "title": a.title,
                "domain": a.domain,
                "date": a.doc_metadata.get("publication_date") or a.doc_metadata.get("date", ""),
                "publisher": a.doc_metadata.get("publisher", "") or a.doc_metadata.get("author", ""),
            }
            if a.source_url:
                article_lookup[a.source_url] = meta
            if a.title:
                article_lookup[a.title] = meta
                norm_title = a.title.strip().lower()
                if norm_title:
                    article_lookup[norm_title] = meta
        log.info("Built article lookup map with %d entries from %d articles",
                 len(article_lookup), len(articles))

    output_dir = Path(OUTPUT_DIR)
    batch_dirs = sorted(output_dir.glob("batch_*"))
    if not batch_dirs:
        log.warning("No batch_* directories found in %s", OUTPUT_DIR)
        return

    extractor = LinkageExtractor()
    cache = EOImplementationCache(cache_path=CACHE_PATH)

    total_extracted = 0
    total_new = 0
    for batch_dir in batch_dirs:
        batch_id = batch_dir.name
        reflect_path = batch_dir / "reflect.md"
        if not reflect_path.exists():
            log.warning("%s — no reflect.md, skipping.", batch_id)
            continue

        # Merge per-batch conversation.json metadata into article_lookup
        batch_lookup = dict(article_lookup)
        conv_path = batch_dir / "conversation.json"
        if conv_path.exists():
            try:
                conv_entries = json.loads(conv_path.read_text(encoding="utf-8"))
                for entry in conv_entries:
                    meta = entry.get("metadata", {})
                    a_url = meta.get("article_url", "")
                    a_title = meta.get("article_title", "")
                    m = {
                        "url": a_url,
                        "title": a_title,
                        "domain": meta.get("domain", ""),
                        "date": meta.get("date", ""),
                        "publisher": meta.get("publisher", ""),
                    }
                    if a_url and a_url not in batch_lookup:
                        batch_lookup[a_url] = m
                    if a_title and a_title not in batch_lookup:
                        batch_lookup[a_title] = m
                        norm = a_title.strip().lower()
                        if norm and norm not in batch_lookup:
                            batch_lookup[norm] = m
            except (json.JSONDecodeError, OSError):
                pass

        reflect_text = reflect_path.read_text(encoding="utf-8")
        records = extractor.extract(
            reflect_text,
            batch_id=batch_id,
            article_lookup=batch_lookup,
        )
        total_extracted += len(records)

        if records:
            new_count = cache.append(records)
            total_new += new_count

            # Show a few sample records
            for rec in records[:3]:
                comm = rec.get("communication")
                if comm:
                    log.info("  ├─ EO %s  ← %s  [%s]  conf=%.1f",
                             rec["eo_number"],
                             (comm.get("title") or "")[:50],
                             comm.get("action_type", ""),
                             rec.get("confidence", 0))
                else:
                    log.info("  ├─ EO %s  (precedence only)", rec["eo_number"])

    # Precedence validation
    conflicts = cache.validate_precedence()
    for warning in conflicts:
        log.warning("  %s", warning)

    # Summary
    summary = cache.summary()
    log.info("-" * 60)
    log.info("Phase 4 Results:")
    log.info("  Records extracted:   %d", total_extracted)
    log.info("  New records cached:  %d", total_new)
    log.info("  Unique EOs:          %d", summary["unique_eos"])
    log.info("  Records with comms:  %d", summary["records_with_comms"])
    log.info("  Unique domains:      %d", summary["unique_domains"])
    log.info("  Precedence conflicts: %d", len(conflicts))
    log.info("  Cache path:          %s", CACHE_PATH)
    log.info("-" * 60)

    # Show precedence graph
    graph = cache.precedence_graph()
    if graph:
        log.info("Precedence graph (%d EOs):", len(graph))
        for eo, prec in sorted(graph.items()):
            parts = []
            if prec.get("rescinds"):
                parts.append(f"rescinds={prec['rescinds']}")
            if prec.get("derived_from"):
                parts.append(f"derived={prec['derived_from']}")
            if prec.get("read_with"):
                parts.append(f"read_with={prec['read_with']}")
            if parts:
                log.info("  EO %s: %s", eo, ", ".join(parts))


# ---------------------------------------------------------------------------
# Phase 5
# ---------------------------------------------------------------------------


def run_phase5(registry: EORegistry, exclude_eos: set[str] | None = None) -> None:
    """Generate Mermaid precedence diagrams per chapter."""
    log.info("=" * 60)
    log.info("PHASE 5 — Mermaid Diagram Generation")
    log.info("=" * 60)

    cache = EOImplementationCache(cache_path=CACHE_PATH)
    if not cache.load():
        log.warning("Cache is empty — run Phase 4 first.")

    prec_graph = cache.precedence_graph()
    log.info("Precedence graph: %d EOs from cache", len(prec_graph))

    # Build EO name lookup (official titles)
    names_path = Path(OUTPUT_DIR) / "eo_names.json"
    name_lookup = EONameLookup(cache_path=names_path)
    name_lookup.load()
    name_lookup.populate(
        impl_cache_path=CACHE_PATH,
        eo_index_dir=PROJECT_ROOT / EO_INDEX_FOLDER,
        chapters_dir=EO_BOOK_DIR,
    )
    name_lookup.save()
    log.info("EO name lookup: %d names", len(name_lookup.names))

    gen = EODiagramGenerator()
    manifest = gen.generate_for_chapters(
        registry=registry,
        precedence_graph=prec_graph,
        output_dir=OUTPUT_DIR,
        name_lookup=name_lookup,
        exclude_eos=exclude_eos,
    )

    total_nodes = 0
    total_warnings = 0
    for entry in manifest:
        total_nodes += entry["nodes"]
        total_warnings += len(entry["warnings"])
        log.info("  ├─ %s  EO %s  %d nodes",
                 entry["chapter_num"], entry["eo_number"], entry["nodes"])
        for w in entry["warnings"]:
            log.warning("     └─ %s", w)

    log.info("-" * 60)
    log.info("Phase 5 Results:")
    log.info("  Diagrams generated: %d", len(manifest))
    log.info("  Total nodes:        %d", total_nodes)
    log.info("  Warnings:           %d", total_warnings)
    log.info("  Output dir:         %s/diagrams/", OUTPUT_DIR)
    log.info("-" * 60)

    # Unified (chapter-independent) diagram
    unified = gen.generate_unified(
        precedence_graph=prec_graph,
        registry=registry,
        output_dir=OUTPUT_DIR,
        name_lookup=name_lookup,
        exclude_eos=exclude_eos,
    )
    log.info("Unified diagram: %d nodes, %d edges  →  %s",
             unified["nodes"], unified["edges"], unified["path"])

    # Focused dependency diagrams for key EOs
    diagrams_dir = Path(OUTPUT_DIR) / "diagrams"
    merged = gen._merge_supercessions(prec_graph, registry)
    for focus_eo in ["14192", "14157", "14183"]:
        diagram = gen.generate_focused(
            root_eo=focus_eo,
            precedence_graph=prec_graph,
            registry=registry,
            name_lookup=name_lookup,
            exclude_eos=exclude_eos,
        )
        out_path = diagrams_dir / f"focused_{focus_eo}.md"
        header = f"# EO {focus_eo} — Focused Dependency Diagram\n\n"
        out_path.write_text(
            header + "```mermaid\n" + diagram + "\n```\n",
            encoding="utf-8",
        )
        node_count = gen._count_nodes(diagram)
        log.info("Focused EO %s: %d nodes  →  %s", focus_eo, node_count, out_path)

    # Show a sample diagram
    if manifest:
        sample = manifest[0]
        sample_path = Path(sample["path"])
        if sample_path.exists():
            content = sample_path.read_text(encoding="utf-8")
            log.info("Sample diagram (%s):", sample_path.name)
            for line in content.split("\n"):
                log.info("  %s", line)


# ---------------------------------------------------------------------------
# Phase 6
# ---------------------------------------------------------------------------

def run_phase6(registry: EORegistry, exclude_eos: set[str] | None = None) -> None:
    """Generate implementation tables per chapter + cross-EO actions table."""
    log.info("=" * 60)
    log.info("PHASE 6 — Implementation Table Generation")
    log.info("=" * 60)

    cache = EOImplementationCache(cache_path=CACHE_PATH)
    if not cache.load():
        log.warning("Cache is empty — run Phase 4 first.")

    # Build EO name lookup
    names_path = Path(OUTPUT_DIR) / "eo_names.json"
    name_lookup = EONameLookup(cache_path=names_path)
    name_lookup.load()
    name_lookup.populate(
        impl_cache_path=CACHE_PATH,
        eo_index_dir=PROJECT_ROOT / EO_INDEX_FOLDER,
        chapters_dir=EO_BOOK_DIR,
    )
    name_lookup.save()

    gen = ImplementationTableGenerator()

    # Per-chapter tables
    manifest = gen.generate_for_chapters(
        registry=registry,
        cache=cache,
        output_dir=OUTPUT_DIR,
        name_lookup=name_lookup,
        exclude_eos=exclude_eos,
    )

    total_rows = sum(e["rows"] for e in manifest)
    for entry in manifest:
        log.info("  ├─ %s  EO %s  %d rows  →  %s",
                 entry["chapter_num"], entry["eo_number"],
                 entry["rows"], entry["path"])

    # Cross-EO actions table
    cross_path = gen.generate_cross_eo(
        cache=cache,
        registry=registry,
        output_dir=OUTPUT_DIR,
        name_lookup=name_lookup,
    )

    log.info("-" * 60)
    log.info("Phase 6 Results:")
    log.info("  Chapter tables:     %d", len(manifest))
    log.info("  Total rows:         %d", total_rows)
    log.info("  Cross-EO table:     %s", cross_path)
    log.info("  Output dir:         %s/tables/", OUTPUT_DIR)
    log.info("-" * 60)

    # Show sample
    if manifest:
        sample_path = Path(manifest[0]["path"])
        if sample_path.exists():
            content = sample_path.read_text(encoding="utf-8")
            log.info("Sample table (%s):", sample_path.name)
            for line in content.split("\n")[:15]:
                log.info("  %s", line)


# ---------------------------------------------------------------------------
# Phase 7
# ---------------------------------------------------------------------------

def run_phase7(registry: EORegistry, exclude_eos: set[str] | None = None) -> None:
    """Inject implementation sections into EO book chapters."""
    log.info("=" * 60)
    log.info("PHASE 7 — Chapter Integration")
    log.info("=" * 60)

    # 1. Load Cache
    cache = EOImplementationCache(str(CACHE_PATH))
    if not cache.load():
        log.error("Cache file not found or empty: %s", CACHE_PATH)
        return

    # 2. Init Generators
    diagram_gen = EODiagramGenerator()
    table_gen = ImplementationTableGenerator()
    names_path = Path(OUTPUT_DIR) / "eo_names.json"
    name_lookup = EONameLookup(cache_path=names_path)
    name_lookup.load()
    name_lookup.populate(
        impl_cache_path=CACHE_PATH,
        eo_index_dir=PROJECT_ROOT / EO_INDEX_FOLDER,
        chapters_dir=EO_BOOK_DIR,
    )
    name_lookup.save()

    # 3. Run Enricher
    enricher = ChapterEnricher(registry, cache, diagram_gen, table_gen,
                               name_lookup=name_lookup,
                               exclude_eos=exclude_eos)
    enricher.enrich_chapters(EO_BOOK_DIR)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demo for EO Event Linking — Phase 1 + 2 + 3 + 4 + 5 + 6",
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Run Phase 2 (requires RAG service at localhost:8080)",
    )
    parser.add_argument(
        "--phase3", action="store_true",
        help="Run Phase 3 LINK-REFLECT (requires Gemini credentials)",
    )
    parser.add_argument(
        "--phase4", action="store_true",
        help="Run Phase 4 linkage extraction and cache build",
    )
    parser.add_argument(
        "--phase5", action="store_true",
        help="Run Phase 5 Mermaid diagram generation",
    )
    parser.add_argument(
        "--phase6", action="store_true",
        help="Run Phase 6 implementation table generation",
    )
    parser.add_argument(
        "--phase7", action="store_true",
        help="Run Phase 7 chapter integration (inject sections)",
    )
    parser.add_argument(
        "-n", type=int, default=None,
        help="Limit Phase 2 to first N articles, or Phase 3 to first N batches",
    )
    parser.add_argument(
        "--exclude-eos", nargs="*", default=None,
        help="EO numbers to exclude from diagrams and tables (e.g. --exclude-eos 14165 14370)",
    )
    args = parser.parse_args()

    gov_articles, registry = run_phase1()

    exclude = set(args.exclude_eos) if args.exclude_eos else None
    if exclude:
        log.info("Excluding EOs from output: %s", ", ".join(sorted(exclude)))

    if args.live:
        run_phase2(gov_articles, max_articles=args.n)
        if args.phase3:
            run_phase3(registry, max_batches=args.n)
        if args.phase4:
            run_phase4(gov_articles)
        if args.phase5:
            run_phase5(registry, exclude_eos=exclude)
        if args.phase6:
            run_phase6(registry, exclude_eos=exclude)
        if args.phase7:
            run_phase7(registry, exclude_eos=exclude)
    elif args.phase3:
        run_phase3(registry, max_batches=args.n)
        if args.phase4:
            run_phase4(gov_articles)
        if args.phase5:
            run_phase5(registry, exclude_eos=exclude)
        if args.phase6:
            run_phase6(registry, exclude_eos=exclude)
        if args.phase7:
            run_phase7(registry, exclude_eos=exclude)
    elif args.phase4:
        run_phase4(gov_articles)
        if args.phase5:
            run_phase5(registry, exclude_eos=exclude)
        if args.phase6:
            run_phase6(registry, exclude_eos=exclude)
        if args.phase7:
            run_phase7(registry, exclude_eos=exclude)
    elif args.phase5:
        run_phase5(registry, exclude_eos=exclude)
        if args.phase6:
            run_phase6(registry, exclude_eos=exclude)
        if args.phase7:
            run_phase7(registry, exclude_eos=exclude)
    elif args.phase6:
        run_phase6(registry, exclude_eos=exclude)
        if args.phase7:
            run_phase7(registry, exclude_eos=exclude)
    elif args.phase7:
        run_phase7(registry, exclude_eos=exclude)
    else:
        # Dry-run: show what Phase 2 would do
        batcher = TokenBudgetBatcher(batch_size=BATCH_SIZE)
        batches = batcher.batch(gov_articles)
        log.info("=" * 60)
        log.info("PHASE 2 — Dry Run (use --live to query RAG)")
        log.info("  Would process %d batches of ≤%d articles", len(batches), BATCH_SIZE)
        total_tokens = 0
        for i, batch in enumerate(batches):
            est_tokens = _estimate_batch_tokens(batch)
            total_tokens += est_tokens
            folders = {a.index_folder for a in batch}
            log.info("  Batch %d: %d articles  ~%dk tokens  [%s]",
                     i, len(batch), est_tokens // 1000, ", ".join(sorted(folders)))
        log.info("  Total estimated tokens: ~%dk", total_tokens // 1000)
        log.info("=" * 60)


if __name__ == "__main__":
    main()
