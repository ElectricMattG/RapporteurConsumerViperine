"""Media Linking — Pipeline Stage.

Orchestrates the media-to-EO linking pipeline:
  Phase 1: load URLs from summaries + QA pairs, filter .gov/.mil, dedup,
           reconstitute articles via RAG /query_metadata
  Phase 2: reconstitute 39 EO grounding via RAG, load EO impl events,
           count all tokens via provider tokenizer
  Phase 3: token-budget batch (800K target), LINK-REFLECT per batch
  Phase 4: extract media linkage records from REFLECT, build cache
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.components.analysis.eo_linking import (_RAG_PRICING, EORegistry,
                                                load_index_catalog)
from src.components.analysis.media_linkage import (ChunkBasedEOReconstituter,
                                                   FullDocReconstituter)
from src.components.analysis.media_linking import (
    MediaExtractor, MediaLinkageCache, MediaLinkageExtractor,
    MediaLinkReflectDriver, TokenCounter, _format_eo_cache_as_grounding)
from src.config_handler import Base
from src.models.publisher import PipelineConfig


def _deduplicate_text_blocks(text: str, min_block_size: int = 200) -> str:
    """Remove duplicate text blocks within a document.

    Splits text by double newlines, tracks blocks > min_block_size,
    and removes repeated blocks while preserving order.
    """
    if not text or len(text) < min_block_size * 2:
        return text

    # Split into paragraphs/blocks
    blocks = text.split('\n\n')
    seen_blocks: set[str] = set()
    unique_blocks: list[str] = []

    for block in blocks:
        block_stripped = block.strip()
        # Only dedupe substantial blocks (not short metadata lines)
        if len(block_stripped) >= min_block_size:
            if block_stripped not in seen_blocks:
                seen_blocks.add(block_stripped)
                unique_blocks.append(block)
            # else: skip duplicate block
        else:
            # Keep short blocks (headers, etc.) without dedup check
            unique_blocks.append(block)

    return '\n\n'.join(unique_blocks)


class MediaLinkStage(Base):
    """Pipeline stage for Media-to-EO Linkage.

    Same pipeline shape as ``EOLinkStage`` but with inverted domain
    filter (media articles, NOT .gov/.mil) and media-specific
    LINK-REFLECT prompts.  Injects EO linkage results from prior
    ``--eo-link`` run as grounding.

    Requires: ``eo_implementation_cache.jsonl`` from prior --eo-link run.
    """

    def __init__(self, cfg: PipelineConfig, dry_run: bool = False) -> None:
        super().__init__()
        self.cfg = cfg
        self.dry_run = dry_run

    def run(self) -> None:
        ml_cfg = self.cfg.media_linking
        if not ml_cfg.enabled:
            self._logger.info("  Media linking disabled — skipping.")
            return

        # -- Gate: EO cache must exist --------------------------------------
        eo_cache_path = Path(ml_cfg.eo_cache_path)
        if not eo_cache_path.exists():
            self._logger.error(
                "EO cache not found at %s — run --eo-link first",
                ml_cfg.eo_cache_path,
            )
            return

        # Ensure output directory exists
        output_dir = Path(ml_cfg.output_cache_path).parent
        output_dir.mkdir(parents=True, exist_ok=True)

        # ==================================================================
        # Phase 1 — Load, Filter, Dedup & Reconstitute Media Articles
        # ==================================================================
        self._logger.info(
            "Phase 1 — Loading catalog from %d indices (summaries + QA pairs)...",
            len(ml_cfg.index_folders),
        )

        catalog = load_index_catalog(
            index_folders=ml_cfg.index_folders,
            base_dir=Path.cwd(),
        )

        extractor = MediaExtractor(domain_exclude=ml_cfg.domain_exclude)
        media_articles = extractor.extract(catalog)

        if not media_articles:
            self._logger.warning("No media articles found — aborting.")
            return

        # Group articles by index_folder to minimise /switch-index calls
        by_index: dict[str, list] = defaultdict(list)
        for article in media_articles:
            by_index[article.index_folder].append(article)

        # Initialize chunk-based EO reconstituter (for complete EOs)
        chunks_file = Path(ml_cfg.eo_index_folder) / "eo_chunks_combined.jsonl"
        try:
            eo_reconstituter = ChunkBasedEOReconstituter(
                chunks_file_path=str(chunks_file)
            )
            self._logger.info("✓ Chunk-based EO reconstituter ready: %s", chunks_file.name)
        except FileNotFoundError:
            self._logger.warning(
                "⚠ eo_chunks_combined.jsonl not found, will use RAG fallback for EOs"
            )
            eo_reconstituter = None

        # Reconstitute articles via RAG to get full document text
        # This is expensive but provides complete context for EO detection
        reconstituter = FullDocReconstituter(rag_url=ml_cfg.rag_service_url)

        self._logger.info("Reconstituting %d articles via RAG (by index folder)...", len(media_articles))
        reconstituted: dict[str, dict] = {}  # url → {full_text, summary_text, title, metadata}

        # Reconstitute by index folder to batch /switch-index calls
        for index_folder, articles in by_index.items():
            article_urls = [a.source_url for a in articles]
            self._logger.info("  Reconstituting %d articles from %s...", len(article_urls), index_folder)

            # Call RAG to get full documents
            rag_docs = reconstituter.reconstitute_batch(
                article_urls,
                index_folder=index_folder
            )

            # Store reconstituted documents
            for doc in rag_docs:
                reconstituted[doc["url"]] = doc

            self._logger.info("    ✓ Got %d/%d documents", len(rag_docs), len(article_urls))

        # -- Phase 1 Summary -----------------------------------------------
        self._logger.info("=" * 60)
        self._logger.info("Media Linking — Phase 1 Summary")
        self._logger.info("  Total catalog:        %d records", len(catalog))
        self._logger.info("  Media filtered:       %d articles", len(media_articles))
        self._logger.info("  RAG reconstituted:    %d articles", len(reconstituted))
        self._logger.info("  Output path:          %s", ml_cfg.output_cache_path)
        self._logger.info("=" * 60)

        if self.dry_run:
            self._logger.info("  Dry run — stopping before Phase 2.")
            return

        if not reconstituted:
            self._logger.warning("No articles in catalog — aborting.")
            return

        # ==================================================================
        # Phase 2 — Reconstitute EO Grounding via RAG & Count Tokens
        # ==================================================================
        self._logger.info("Phase 2 — Reconstituting EO grounding via RAG...")

        # Parse 39 EO numbers from chapter headings
        eo_index_path = Path.cwd() / ml_cfg.eo_index_folder
        registry = EORegistry.load(
            eo_book_dir=ml_cfg.eo_book_dir,
            eo_index_folder=eo_index_path,
        )

        # Collect Federal Register URLs from EO summaries for reconstitution
        eo_urls: list[str] = []
        seen_urls: set[str] = set()
        for entry in registry.entries.values():
            for summary in entry.summaries:
                url = summary.get("source_url", "")
                if url and url not in seen_urls:
                    eo_urls.append(url)
                    seen_urls.add(url)
                    break  # one URL per EO is sufficient

        # Reconstitute EO texts from chunks file (complete, clean)
        eo_reconstituted: dict[str, dict] = {}
        if eo_urls and eo_reconstituter:
            self._logger.info(
                "  Reconstituting %d unique EO URLs from chunks file...",
                len(eo_urls),
            )
            eo_reconstituted = eo_reconstituter.reconstitute_batch(eo_urls)
        elif eo_urls:
            # Fallback to RAG if chunks file not available
            self._logger.info(
                "  Reconstituting %d unique EO URLs from RAG fallback...",
                len(eo_urls),
            )
            eo_docs = reconstituter.reconstitute_batch(
                eo_urls, index_folder=ml_cfg.eo_index_folder,
            )
            for doc in eo_docs:
                eo_reconstituted[doc["url"]] = doc

        # Build eo_grounding: prefer RAG-reconstituted text, fall back
        # to chapter .md text for EOs without RAG results
        eo_grounding_parts: list[str] = []
        eo_from_rag = 0
        eo_from_chapter = 0
        used_urls: set[str] = set()  # Track URLs already added to grounding
        used_chapters: set[str] = set()  # Track chapter files already added

        for eo_num, entry in sorted(registry.entries.items()):
            # Try RAG-reconstituted text first
            rag_text = None
            source_url = None
            for summary in entry.summaries:
                url = summary.get("source_url", "")
                if url and url in eo_reconstituted:
                    # Check if this URL was already used
                    if url not in used_urls:
                        doc = eo_reconstituted[url]
                        # Use full_text from chunks for complete EO content (8.5K chars)
                        rag_text = doc.get("full_text", "")
                        source_url = url
                        break
                    else:
                        self._logger.debug(
                            "Skipping duplicate URL for EO %s: %s", eo_num, url
                        )

            if rag_text and source_url:
                used_urls.add(source_url)
                # Deduplicate repeated blocks within the RAG text
                cleaned_text = _deduplicate_text_blocks(rag_text, min_block_size=200)
                eo_grounding_parts.append(
                    f"## EO {eo_num} — {entry.title}\n\n{cleaned_text}"
                )
                eo_from_rag += 1
                if len(cleaned_text) < len(rag_text) * 0.8:
                    # Log if significant deduplication occurred
                    self._logger.debug(
                        "EO %s: deduplicated RAG text from %d to %d chars",
                        eo_num, len(rag_text), len(cleaned_text)
                    )
            elif entry.chapter_file and entry.chapter_file not in used_chapters:
                # Fall back to chapter text (only if not already used)
                for filename, content in registry.chapter_texts:
                    if filename == entry.chapter_file:
                        used_chapters.add(entry.chapter_file)
                        # Deduplicate blocks in chapter text too
                        cleaned_content = _deduplicate_text_blocks(content, min_block_size=200)
                        eo_grounding_parts.append(
                            f"## {filename}\n\n{cleaned_content}"
                        )
                        eo_from_chapter += 1
                        break
            elif entry.chapter_file in used_chapters:
                self._logger.debug(
                    "Skipping duplicate chapter for EO %s: %s",
                    eo_num, entry.chapter_file
                )

        eo_grounding = "\n\n---\n\n".join(eo_grounding_parts)

        self._logger.info(
            "  EO grounding built: %d unique texts from %d EO entries (%d deduplicated)",
            len(eo_grounding_parts), len(registry.entries),
            len(registry.entries) - len(eo_grounding_parts),
        )

        # Build EO implementation events grounding
        eo_linkage_grounding = _format_eo_cache_as_grounding(
            ml_cfg.eo_cache_path,
        )

        # Initialise TokenCounter for exact token measurement
        tc = TokenCounter(
            provider=ml_cfg.analysis_provider,
            model=ml_cfg.analysis_model,
        )

        # Count grounding tokens
        eo_grounding_tokens = tc.count(eo_grounding)
        eo_impl_grounding_tokens = tc.count(eo_linkage_grounding)
        grounding_tokens_total = eo_grounding_tokens + eo_impl_grounding_tokens

        self._logger.info("=" * 60)
        self._logger.info("Media Linking — Phase 2 Summary")
        self._logger.info("  EO registry:          %d entries", len(registry.entries))
        self._logger.info("  EOs from RAG:         %d", eo_from_rag)
        self._logger.info("  EOs from chapters:    %d", eo_from_chapter)
        self._logger.info(
            "  EO grounding:         %d tokens (%d chars)",
            eo_grounding_tokens, len(eo_grounding),
        )
        self._logger.info(
            "  EO impl grounding:    %d tokens (%d chars)",
            eo_impl_grounding_tokens, len(eo_linkage_grounding),
        )
        self._logger.info(
            "  Grounding total:      %d tokens",
            grounding_tokens_total,
        )
        self._logger.info("=" * 60)

        # -- DEBUG: Write grounding package for inspection -----------------
        grounding_debug_path = output_dir / "grounding_package_debug.txt"
        grounding_debug_content = f"""# GROUNDING PACKAGE DEBUG OUTPUT
# Generated: {datetime.now(timezone.utc).isoformat()}
# Total grounding tokens: {grounding_tokens_total:,}
#
# NOTE: This file shows the GROUNDING CONTEXT (EO framework + implementation events)
#       that is provided to the LLM for all batches. The ARTICLES being analyzed are
#       shown in articles_debug.txt (generated during Phase 3 batching).

{'=' * 80}
SECTION 1: EO LEGAL FRAMEWORK
{'=' * 80}
Tokens: {eo_grounding_tokens:,}
Chars: {len(eo_grounding):,}

{eo_grounding}

{'=' * 80}
SECTION 2: EO IMPLEMENTATION EVENTS (verified .gov/.mil linkages)
{'=' * 80}
Tokens: {eo_impl_grounding_tokens:,}
Chars: {len(eo_linkage_grounding):,}

{eo_linkage_grounding}

{'=' * 80}
END OF GROUNDING PACKAGE
{'=' * 80}

# The articles being analyzed are batched separately and shown in articles_debug.txt
"""
        grounding_debug_path.write_text(grounding_debug_content, encoding="utf-8")
        self._logger.info(
            "📦 DEBUG: Grounding package written to %s (%d KB)",
            grounding_debug_path, len(grounding_debug_content) // 1024,
        )

        # ==================================================================
        # Phase 3 — Token-Budget Batch & LINK-REFLECT
        # ==================================================================

        # Format reconstituted articles for prompt injection AND build metadata lookup
        article_texts: list[tuple[str, str]] = []  # (url, formatted_text)
        article_metadata: dict[str, dict[str, Any]] = {}  # url → metadata for conversations.json

        for article in media_articles:
            doc = reconstituted.get(article.source_url)
            if not doc:
                continue
            meta = doc.get("doc_metadata", {})

            # Build formatted text for prompt
            parts = [
                f"URL: {article.source_url}",
                f"Title: {doc.get('title', '') or article.title}",
                f"Domain: {article.domain}",
            ]
            date = meta.get("publication_date", "")
            if date:
                parts.append(f"Date: {date}")
            publisher = meta.get("publisher", "")
            if publisher:
                parts.append(f"Publisher: {publisher}")
            authors = meta.get("authors", [])
            if authors:
                parts.append(f"Author: {', '.join(authors)}")

            # Use FULL TEXT for comprehensive linkage analysis
            # This increases token usage but may improve linkage rate
            full_text = doc.get('full_text', '').strip()
            summary_text = doc.get('summary_text', '').strip()

            if full_text:
                # Use complete article text for better EO detection
                parts.append(f"\n## Article\n{full_text}")
            elif summary_text:
                # Fallback to summary if full text not available
                parts.append(f"\n## Summary\n{summary_text}")

            article_texts.append((article.source_url, "\n".join(parts)))

            # Store metadata for conversation.json
            article_metadata[article.source_url] = {
                "article_url": article.source_url,
                "article_title": doc.get('title', '') or article.title,
                "document_id": article.document_id,
                "publisher": publisher,
                "date": date,
                "domain": article.domain,
                "index_folder": article.index_folder,
                "authors": authors,
            }

        # Token-budget batch: pack articles up to target_input_tokens
        OVERHEAD_TOKENS = 5_000  # prompt template + framing
        article_budget_tokens = (
            ml_cfg.target_input_tokens - grounding_tokens_total - OVERHEAD_TOKENS
        )

        batches: list[list[tuple[str, str]]] = []
        current_batch: list[tuple[str, str]] = []
        current_tokens = 0
        for url, text in article_texts:
            text_tokens = tc.count(text)
            if current_batch and current_tokens + text_tokens > article_budget_tokens:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0
            current_batch.append((url, text))
            current_tokens += text_tokens
        if current_batch:
            batches.append(current_batch)

        self._logger.info(
            "Phase 3 — LINK-REFLECT on %d batches (%d articles)...",
            len(batches), len(article_texts),
        )
        for i, batch in enumerate(batches):
            batch_tokens = sum(tc.count(t) for _, t in batch)
            total_tokens = grounding_tokens_total + batch_tokens + OVERHEAD_TOKENS
            self._logger.info(
                "  Batch %d: %d articles, %d article tokens, %d total tokens",
                i, len(batch), batch_tokens, total_tokens,
            )

        # -- DEBUG: Write article preview for inspection -------------------
        articles_debug_path = output_dir / "articles_debug.txt"
        articles_debug_lines = [
            "# ARTICLES DEBUG OUTPUT",
            f"# Generated: {datetime.now(timezone.utc).isoformat()}",
            f"# Total articles: {len(article_texts):,}",
            f"# Total batches: {len(batches)}",
            "",
            "=" * 80,
            "ARTICLE PREVIEW (First 500 chars of each article)",
            "=" * 80,
            ""
        ]

        for idx, (url, text) in enumerate(article_texts[:50], 1):  # Limit to first 50
            meta = article_metadata.get(url, {})
            articles_debug_lines.extend([
                f"\n--- Article {idx}/{len(article_texts)} ---",
                f"URL: {url}",
                f"Title: {meta.get('article_title', 'N/A')}",
                f"Domain: {meta.get('domain', 'N/A')}",
                f"Date: {meta.get('date', 'N/A')}",
                f"Publisher: {meta.get('publisher', 'N/A')}",
                f"Full length: {len(text):,} chars, ~{tc.count(text)} tokens",
                "",
                "Text preview:",
                text[:500] + ("..." if len(text) > 500 else ""),
                ""
            ])

        if len(article_texts) > 50:
            articles_debug_lines.append(f"\n... and {len(article_texts) - 50} more articles")

        articles_debug_lines.extend([
            "",
            "=" * 80,
            "END OF ARTICLES DEBUG",
            "=" * 80,
        ])

        articles_debug_content = "\n".join(articles_debug_lines)
        articles_debug_path.write_text(articles_debug_content, encoding="utf-8")
        self._logger.info(
            "📰 DEBUG: Articles preview written to %s (%d KB)",
            articles_debug_path, len(articles_debug_content) // 1024,
        )

        lr_driver = MediaLinkReflectDriver(
            model_name=ml_cfg.analysis_model,
            location=ml_cfg.analysis_location,
            output_dir=output_dir,
            provider=ml_cfg.analysis_provider,
            reasoning_effort=ml_cfg.analysis_reasoning_effort,
            token_counter=tc,
        )

        completed = 0
        skipped = 0

        for batch_idx, batch in enumerate(batches):
            # Create batch directory and write conversations.json BEFORE LLM calls
            batch_dir = output_dir / f"batch_{batch_idx}"
            batch_dir.mkdir(parents=True, exist_ok=True)

            # Build conversation records in ExportStage-compatible schema
            conversations_path = batch_dir / "conversations.json"
            conversations_data = []
            for url, text in batch:
                meta = article_metadata.get(url, {})
                entry = {
                    "user_query": "",  # No query for media articles
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "assistant_response": text,
                    "metadata": meta,
                }
                conversations_data.append(entry)

            conversations_path.write_text(
                json.dumps(conversations_data, indent=2, ensure_ascii=False, default=str),
                encoding="utf-8",
            )
            self._logger.info(
                "  Batch %d: wrote %d articles to %s",
                batch_idx, len(batch), conversations_path,
            )

            batch_text = "\n---\n\n".join(text for _, text in batch)

            result = lr_driver.process_batch(
                batch_index=batch_idx,
                batch_conversations=batch_text,
                eo_grounding=eo_grounding,
                eo_linkage_grounding=eo_linkage_grounding,
            )
            if result.get("status") == "complete":
                completed += 1
            else:
                skipped += 1

        # -- Phase 3 Summary -----------------------------------------------
        self._logger.info("=" * 60)
        self._logger.info("Media Linking — Phase 3 Summary")
        self._logger.info("  Batches completed:  %d", completed)
        self._logger.info("  Batches skipped:    %d", skipped)
        self._logger.info(
            "  LLM input tokens:   %d", lr_driver.total_usage["input"],
        )
        self._logger.info(
            "  LLM output tokens:  %d", lr_driver.total_usage["output"],
        )
        self._logger.info(
            "  LLM thinking tokens:%d", lr_driver.total_usage["thinking"],
        )

        pricing = _RAG_PRICING.get(
            ml_cfg.analysis_model, {"input": 0, "output": 0},
        )

        # Use actual usage if available, otherwise use estimated
        if lr_driver.total_usage["input"] > 0:
            est_cost = (
                (lr_driver.total_usage["input"] / 1_000_000) * pricing["input"]
                + (lr_driver.total_usage["output"] / 1_000_000) * pricing["output"]
            )
            self._logger.info("  Actual cost:        $%.4f", est_cost)
        elif lr_driver.estimated_usage["input"] > 0:
            est_cost = (
                (lr_driver.estimated_usage["input"] / 1_000_000) * pricing["input"]
            )
            self._logger.info("  Estimated cost:     $%.4f (calls failed, input-only estimate)", est_cost)
        else:
            est_cost = 0.0
            self._logger.info("  Estimated cost:     $%.4f", est_cost)

        self._logger.info("=" * 60)

        # ==================================================================
        # Phase 4 — Linkage Extraction & Cache Build
        # ==================================================================
        self._logger.info(
            "Phase 4 — Extracting linkage records from %d batches...",
            len(batches),
        )

        extractor_p4 = MediaLinkageExtractor()
        cache = MediaLinkageCache(cache_path=ml_cfg.output_cache_path)

        # Build article_lookup from reconstituted data for metadata hydration
        article_lookup: dict[str, dict[str, str]] = {}
        for article in media_articles:
            doc = reconstituted.get(article.source_url)
            if not doc:
                continue
            meta = doc.get("doc_metadata", {})
            entry = {
                "url": article.source_url,
                "title": doc.get("title", "") or article.title,
                "date": meta.get("publication_date", ""),
                "domain": article.domain,
                "publisher": meta.get("publisher", ""),
            }
            article_lookup[article.source_url] = entry
            if entry["title"]:
                article_lookup[entry["title"]] = entry

        total_extracted = 0
        total_new = 0
        for batch_idx in range(len(batches)):
            batch_dir = output_dir / f"batch_{batch_idx}"
            reflect_path = batch_dir / "reflect.json"
            if not reflect_path.exists():
                self._logger.warning(
                    "Batch %d — no reflect.json, skipping.", batch_idx,
                )
                continue

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

        summary = cache.summary()
        self._logger.info("=" * 60)
        self._logger.info("Media Linking — Phase 4 Summary")
        self._logger.info("  Records extracted:  %d", total_extracted)
        self._logger.info("  New records cached: %d", total_new)
        self._logger.info("  Unique EOs:         %d", summary["unique_eos"])
        self._logger.info(
            "  Unique domains:     %d", summary["unique_domains"],
        )
        self._logger.info("  Cache path:         %s", ml_cfg.output_cache_path)
        self._logger.info("=" * 60)
