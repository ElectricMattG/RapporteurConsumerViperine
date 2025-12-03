#!/usr/bin/env python3
"""
Chunk-Level Content Embedding CLI

Generates embeddings for markdown chunks with section_hierarchy concatenation.
The embedding text is constructed as: [section_hierarchy] + [content]

This enables context-aware embeddings where chunks inherit semantic meaning
from their position in the document structure.

Document-Level Aggregation:
    ✓ Implemented: markdown_document_aggregation.py (completed)

    This script aggregates chunk embeddings to document level using max pooling.

    Usage:
        python markdown_document_aggregation.py enriched_chunks.jsonl output.json

    Features:
    - Input: enriched_chunks.jsonl (output from this script)
    - Output: markdown_document_level_embeddings.json
    - Aggregation: Max pooling across chunk embeddings per document
    - Schema: Matches existing document_level_embeddings.json format
    - Grouping: Uses document_metadata.document_id to group chunks
    - Two-level aggregation: Handles sub-chunks → chunks → documents
    - Metadata: Extracts section hierarchy statistics (depth, section counts, top sections)
    - Quality metrics: Embedding norm, sparsity, dimension validation

    Implementation: Option A (Flat max pooling with hierarchy metadata)
    Tested: 687 documents, avg 17.3 chunks/doc, 384-dim embeddings

Usage:
    python chunk_embedding_cli.py <chunk_jsonl> <output_dir> [options]

Examples:
    # Basic chunk embedding
    python chunk_embedding_cli.py \
        document_chunks/merged_markdown_chunks/citations/merged_markdown_chunks_content.jsonl \
        chunk_embeddings/

    # With filtering and custom separator
    python chunk_embedding_cli.py \
        document_chunks/merged_markdown_chunks/citations/merged_markdown_chunks_content.jsonl \
        chunk_embeddings/ \
        --min-chunk-length 50 \
        --hierarchy-separator " > " \
        --batch-size 100
"""

import argparse
import asyncio
import hashlib
import json
import logging
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add paths for imports
current_dir = Path(__file__).parent
components_dir = current_dir.parent
project_root = components_dir.parent

for p in [str(current_dir), str(components_dir), str(project_root)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Import embedding client
sys.path.insert(0, str(components_dir / '__clients'))
from embedding_client import EmbeddingServiceClient


@dataclass
class ChunkEmbeddingRecord:
    """Schema for chunk-level embeddings with section hierarchy."""

    # Identifiers
    embedding_id: str                    # Unique ID for this embedding
    chunk_id: str                        # Original chunk_id from source
    source_article_id: Optional[int]     # Document-level foreign key

    # Content information
    original_content: str                # Raw chunk content
    section_hierarchy: List[str]         # Hierarchical context
    enriched_text: str                   # Concatenated: hierarchy + content

    # Sub-chunking for long content
    is_sub_chunked: bool = False         # Whether content was split
    sub_chunk_texts: Optional[List[str]] = None  # Individual sub-chunk texts
    num_sub_chunks: int = 1              # Number of sub-chunks

    # Metadata
    chunk_type: str = "unknown"          # paragraph, heading, list, etc.
    chunk_index: int = 0                 # Position in document
    word_count: int = 0
    char_count: int = 0

    # Source tracking
    source_url: Optional[str] = None
    source_title: Optional[str] = None

    # Processing info
    hierarchy_depth: int = 0
    has_hierarchy: bool = False
    concatenation_method: str = "hierarchy_first"
    processing_timestamp: str = ""

    def __post_init__(self):
        if not self.processing_timestamp:
            self.processing_timestamp = datetime.now().isoformat()
        self.hierarchy_depth = len(self.section_hierarchy)
        self.has_hierarchy = self.hierarchy_depth > 0


class ChunkEmbeddingProcessor:
    """Processes chunked markdown for embedding with section_hierarchy concatenation."""

    def __init__(
        self,
        hierarchy_separator: str = " > ",
        min_chunk_length: int = 20,
        include_empty_hierarchy: bool = True
    ):
        """
        Initialize chunk embedding processor.

        Args:
            hierarchy_separator: String to join hierarchy elements (default: " > ")
            min_chunk_length: Minimum content length to process
            include_empty_hierarchy: Whether to process chunks with empty hierarchy
        """
        self._logger = logging.getLogger(__name__)
        self.hierarchy_separator = hierarchy_separator
        self.min_chunk_length = min_chunk_length
        self.include_empty_hierarchy = include_empty_hierarchy

    def concatenate_hierarchy_content(
        self,
        section_hierarchy: List[str],
        content: str
    ) -> str:
        """
        Concatenate section hierarchy with content.

        Format: [Section > Subsection > ...]: Content text

        Args:
            section_hierarchy: List of section names from root to current
            content: Chunk content

        Returns:
            Enriched text with hierarchy context
        """
        if not section_hierarchy:
            return content

        hierarchy_str = self.hierarchy_separator.join(section_hierarchy)
        # Place hierarchy first, then content
        return f"{hierarchy_str}: {content}"

    def split_long_text(
        self,
        text: str,
        max_chunk_size: int = 7500,  # Safe limit: embedding model supports ~8000 chars
        overlap: int = 200
    ) -> List[str]:
        """
        Split long text into overlapping sub-chunks.

        The embedding service has a hard limit of ~8000 characters per text.
        We use 7500 as a safe buffer.

        Args:
            text: Text to split
            max_chunk_size: Maximum size per sub-chunk (default: 7500 chars)
            overlap: Overlap between consecutive chunks for context (default: 200)

        Returns:
            List of text sub-chunks (non-empty)
        """
        if len(text) <= max_chunk_size:
            return [text]

        sub_chunks = []
        start = 0

        while start < len(text):
            end = start + max_chunk_size

            # If not the last chunk, try to break at sentence/word boundary
            if end < len(text):
                # Look for sentence boundary (., !, ?) in last 200 chars
                last_segment = text[max(end-200, start):end]
                sentence_breaks = [i for i, c in enumerate(last_segment) if c in '.!?\n']

                if sentence_breaks:
                    # Use the last sentence break
                    break_point = sentence_breaks[-1] + 1
                    end = max(end-200, start) + break_point
                else:
                    # Fall back to word boundary
                    last_segment = text[max(end-100, start):end]
                    space_breaks = [i for i, c in enumerate(last_segment) if c == ' ']
                    if space_breaks:
                        break_point = space_breaks[-1]
                        end = max(end-100, start) + break_point

            # Extract chunk and add if non-empty
            chunk = text[start:end].strip()
            if chunk:  # Only add non-empty chunks
                sub_chunks.append(chunk)

            # Next chunk starts with overlap for context continuity
            start = end - overlap if end < len(text) else end

        return sub_chunks if sub_chunks else [text]  # Fallback to original if all empty

    def process_chunk(self, chunk_data: Dict[str, Any]) -> Optional[ChunkEmbeddingRecord]:
        """
        Process a single chunk and create embedding record.

        Args:
            chunk_data: Raw chunk dictionary from JSONL

        Returns:
            ChunkEmbeddingRecord or None if filtered
        """
        try:
            # Extract chunk metadata
            chunk_metadata = chunk_data.get('chunk_metadata', {})
            content = chunk_data.get('content', '').strip()

            # Filter by length
            if len(content) < self.min_chunk_length:
                self._logger.debug(f"Skipping short chunk: {len(content)} chars")
                return None

            # Extract section hierarchy
            section_hierarchy = chunk_metadata.get('section_hierarchy', [])

            # Filter if no hierarchy and configured to exclude
            if not section_hierarchy and not self.include_empty_hierarchy:
                self._logger.debug(f"Skipping chunk without hierarchy")
                return None

            # Extract identifiers
            chunk_id = chunk_metadata.get('chunk_id', '')
            source_article_id = chunk_data.get('source_article_id')

            # Create enriched text
            enriched_text = self.concatenate_hierarchy_content(
                section_hierarchy,
                content
            )

            # Check if sub-chunking is needed
            # Embedding model has ~8000 char limit, use 7500 as safe buffer
            sub_chunk_texts = None
            is_sub_chunked = False
            num_sub_chunks = 1

            if len(enriched_text) > 7500:
                # Split into sub-chunks to preserve semantic information
                sub_chunk_texts = self.split_long_text(enriched_text, max_chunk_size=7500, overlap=200)
                is_sub_chunked = True
                num_sub_chunks = len(sub_chunk_texts)
                self._logger.info(
                    f"Chunk {chunk_id}: Split {len(enriched_text)} chars into {num_sub_chunks} sub-chunks"
                )

            # Extract metadata
            chunk_type = chunk_metadata.get('chunk_type', 'unknown')
            chunk_index = chunk_metadata.get('chunk_index', 0)
            word_count = chunk_metadata.get('word_count', 0)
            char_count = chunk_metadata.get('char_count', len(content))

            # Document-level info
            doc_metadata = chunk_data.get('document_metadata', {})
            source_url = doc_metadata.get('source_url') or chunk_data.get('source_url')
            source_title = doc_metadata.get('title') or chunk_data.get('title')

            # Generate embedding ID
            embedding_id = f"emb_{chunk_id}" if chunk_id else hashlib.md5(
                f"{source_article_id}_{chunk_index}".encode()
            ).hexdigest()[:16]

            # Create record
            record = ChunkEmbeddingRecord(
                embedding_id=embedding_id,
                chunk_id=chunk_id,
                source_article_id=source_article_id,
                original_content=content,
                section_hierarchy=section_hierarchy,
                enriched_text=enriched_text,
                is_sub_chunked=is_sub_chunked,
                sub_chunk_texts=sub_chunk_texts,
                num_sub_chunks=num_sub_chunks,
                chunk_type=chunk_type,
                chunk_index=chunk_index,
                word_count=word_count,
                char_count=char_count,
                source_url=source_url,
                source_title=source_title
            )

            return record

        except Exception as e:
            self._logger.error(f"Error processing chunk: {e}")
            return None

    def process_jsonl_file(self, jsonl_path: Path) -> List[ChunkEmbeddingRecord]:
        """
        Process entire JSONL file of chunks.

        Args:
            jsonl_path: Path to chunk JSONL file

        Returns:
            List of ChunkEmbeddingRecords
        """
        records = []
        processed = 0
        skipped = 0

        self._logger.info(f"Processing chunks from: {jsonl_path}")

        with open(jsonl_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    chunk_data = json.loads(line)
                    record = self.process_chunk(chunk_data)

                    if record:
                        records.append(record)
                        processed += 1
                    else:
                        skipped += 1

                    if line_num % 10000 == 0:
                        self._logger.info(f"Processed {line_num} lines: {processed} records, {skipped} skipped")

                except json.JSONDecodeError as e:
                    self._logger.warning(f"Line {line_num}: JSON parse error: {e}")
                    skipped += 1
                except Exception as e:
                    self._logger.error(f"Line {line_num}: Error: {e}")
                    skipped += 1

        self._logger.info(f"Completed: {processed} records, {skipped} skipped from {line_num} total lines")
        return records


async def generate_chunk_embeddings(
    chunk_jsonl: Path,
    output_dir: Path,
    hierarchy_separator: str = " > ",
    min_chunk_length: int = 20,
    batch_size: int = 50,
    include_empty_hierarchy: bool = True,
    enable_clustering: bool = False,
    similarity_threshold: float = 0.6
) -> None:
    """
    Main function to generate chunk-level embeddings.

    Enriches original chunk schema by adding embedding fields while preserving
    all original chunk metadata and structure.

    Args:
        chunk_jsonl: Path to chunked JSONL file
        output_dir: Directory for outputs
        hierarchy_separator: String to join hierarchy elements
        min_chunk_length: Minimum content length
        batch_size: Embedding batch size
        include_empty_hierarchy: Process chunks without hierarchy
        enable_clustering: Perform clustering analysis
        similarity_threshold: Threshold for similarity clustering
    """
    logger = logging.getLogger(__name__)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize processor
    processor = ChunkEmbeddingProcessor(
        hierarchy_separator=hierarchy_separator,
        min_chunk_length=min_chunk_length,
        include_empty_hierarchy=include_empty_hierarchy
    )

    # Load original chunks and process
    logger.info("Step 1: Loading original chunks and creating enriched text")

    original_chunks = []
    records = []
    processed = 0
    skipped = 0

    with open(chunk_jsonl, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                chunk_data = json.loads(line)
                record = processor.process_chunk(chunk_data)

                if record:
                    original_chunks.append(chunk_data)  # Store original
                    records.append(record)
                    processed += 1
                else:
                    skipped += 1

                if line_num % 10000 == 0:
                    logger.info(f"Processed {line_num} lines: {processed} records, {skipped} skipped")

            except Exception as e:
                logger.error(f"Line {line_num}: Error: {e}")
                skipped += 1

    if not records:
        logger.error("No valid chunks to process!")
        return

    logger.info(f"Processed {len(records)} chunks (skipped {skipped})")

    # Analyze hierarchy and sub-chunking distribution
    with_hierarchy = sum(1 for r in records if r.has_hierarchy)
    hierarchy_depths = [r.hierarchy_depth for r in records if r.has_hierarchy]
    sub_chunked_count = sum(1 for r in records if r.is_sub_chunked)
    total_sub_chunks = sum(r.num_sub_chunks for r in records)

    logger.info(f"Chunks with hierarchy: {with_hierarchy}/{len(records)} ({100*with_hierarchy/len(records):.1f}%)")
    if hierarchy_depths:
        logger.info(f"Hierarchy depth range: {min(hierarchy_depths)}-{max(hierarchy_depths)}, avg: {sum(hierarchy_depths)/len(hierarchy_depths):.1f}")

    if sub_chunked_count > 0:
        logger.info(f"Sub-chunked records: {sub_chunked_count}/{len(records)} ({100*sub_chunked_count/len(records):.1f}%)")
        logger.info(f"Total embeddings to generate: {total_sub_chunks} (from {len(records)} chunks)")

    # Generate embeddings
    logger.info("Step 2: Generating embeddings for enriched text")

    embedding_client = EmbeddingServiceClient()

    # Collect all texts to embed (including sub-chunks)
    texts_to_embed = []
    record_mapping = []  # Maps embedding index back to record index

    sub_chunked_added = 0
    regular_added = 0

    for idx, record in enumerate(records):
        if record.is_sub_chunked and record.sub_chunk_texts:
            # Add each sub-chunk (skip empty)
            for sub_idx, sub_text in enumerate(record.sub_chunk_texts):
                if sub_text and sub_text.strip():
                    texts_to_embed.append(sub_text)
                    record_mapping.append((idx, True, sub_idx))  # (record_idx, is_sub_chunk, sub_chunk_index)
                    sub_chunked_added += 1

                    # Log details for problematic chunk
                    if record.chunk_id == "chunk_7ad4ac4ed333":
                        logger.info(f"Adding sub-chunk {sub_idx}/{len(record.sub_chunk_texts)} for chunk_7ad4ac4ed333: {len(sub_text)} chars")
        else:
            # Add single enriched text (skip empty)
            if record.enriched_text and record.enriched_text.strip():
                texts_to_embed.append(record.enriched_text)
                record_mapping.append((idx, False, 0))
                regular_added += 1

                # Log if this is unexpectedly long
                if len(record.enriched_text) > 8000:
                    logger.warning(f"Regular (non-sub-chunked) text is {len(record.enriched_text)} chars for chunk {record.chunk_id}")

    logger.info(f"Added {sub_chunked_added} sub-chunk texts and {regular_added} regular texts")

    # Validate all texts before embedding
    oversized_texts = []
    for i, text in enumerate(texts_to_embed):
        if len(text) > 8000:
            oversized_texts.append((i, len(text), record_mapping[i]))

    if oversized_texts:
        logger.error(f"Found {len(oversized_texts)} oversized texts before embedding:")
        for text_idx, length, (record_idx, is_sub, sub_idx) in oversized_texts[:10]:  # Show first 10
            rec = records[record_idx]
            logger.error(f"  Text {text_idx}: {length} chars, chunk_id={rec.chunk_id}, "
                        f"is_sub_chunk={is_sub}, sub_idx={sub_idx}, "
                        f"record.is_sub_chunked={rec.is_sub_chunked}, "
                        f"record.num_sub_chunks={rec.num_sub_chunks}")
        raise ValueError(f"Found {len(oversized_texts)} texts exceeding 8000 chars. Cannot proceed.")

    if not texts_to_embed:
        logger.error("No valid texts to embed after filtering!")
        return

    # Analyze text lengths before embedding
    max_text_len = max(len(t) for t in texts_to_embed)
    min_text_len = min(len(t) for t in texts_to_embed)
    avg_text_len = sum(len(t) for t in texts_to_embed) / len(texts_to_embed)

    logger.info(f"Text length stats - Min: {min_text_len}, Max: {max_text_len}, Avg: {avg_text_len:.0f} chars")

    # Find and log texts over 50k
    long_texts = [(i, len(t)) for i, t in enumerate(texts_to_embed) if len(t) > 50000]
    if long_texts:
        logger.warning(f"Found {len(long_texts)} texts over 50k characters:")
        for idx, length in long_texts[:20]:  # Show first 20
            rec_idx, is_sub = record_mapping[idx]
            logger.warning(f"  Embed index {idx}: {length} chars - chunk_id: {records[rec_idx].chunk_id}, sub_chunk: {is_sub}")

        # Save problematic texts for manual testing
        debug_file = output_dir / "long_texts_debug.jsonl"
        logger.warning(f"Saving long texts to {debug_file} for manual testing")
        with open(debug_file, 'w', encoding='utf-8') as f:
            for idx, length in long_texts[:50]:  # Save first 50
                rec_idx, is_sub, sub_idx = record_mapping[idx]
                f.write(json.dumps({
                    'embed_index': idx,
                    'record_index': rec_idx,
                    'chunk_id': records[rec_idx].chunk_id,
                    'is_sub_chunk': is_sub,
                    'sub_chunk_index': sub_idx,
                    'text_length': length,
                    'text': texts_to_embed[idx]
                }, ensure_ascii=False) + '\n')

    logger.info(f"Total texts to embed: {len(texts_to_embed)}")

    # Save a sample of texts for debugging (first 500)
    debug_sample_file = output_dir / "embedding_texts_sample.jsonl"
    logger.info(f"Saving first 500 texts to {debug_sample_file} for debugging")
    with open(debug_sample_file, 'w', encoding='utf-8') as f:
        for idx in range(min(500, len(texts_to_embed))):
            rec_idx, is_sub, sub_idx = record_mapping[idx]
            f.write(json.dumps({
                'global_index': idx,
                'record_index': rec_idx,
                'chunk_id': records[rec_idx].chunk_id,
                'is_sub_chunk': is_sub,
                'sub_chunk_index': sub_idx,
                'text_length': len(texts_to_embed[idx]),
                'text_preview': texts_to_embed[idx][:500],
                'text_full': texts_to_embed[idx]
            }, ensure_ascii=False) + '\n')

    logger.info(f"Generating {len(texts_to_embed)} embeddings for {len(records)} chunks...")
    embeddings_array = await embedding_client.embed(texts_to_embed, batch_size=batch_size, show_progress=True)

    # Convert to list
    all_embeddings = embeddings_array.tolist()

    if not all_embeddings or len(all_embeddings) != len(texts_to_embed):
        logger.error(f"Embedding mismatch: got {len(all_embeddings) if all_embeddings else 0}, expected {len(texts_to_embed)}")
        return

    logger.info(f"Successfully generated {len(all_embeddings)} embeddings")

    # Aggregate embeddings back to original chunks
    logger.info("Step 3: Aggregating sub-chunk embeddings")

    chunk_embeddings = []  # Final embeddings per chunk (aggregated if needed)
    embedding_idx = 0

    for record in records:
        if record.is_sub_chunked and record.sub_chunk_texts:
            # Collect sub-chunk embeddings
            num_subs = record.num_sub_chunks
            sub_embeddings = all_embeddings[embedding_idx:embedding_idx + num_subs]

            # Aggregate via mean (preserves semantic information)
            import numpy as np
            aggregated = np.mean(sub_embeddings, axis=0).tolist()
            chunk_embeddings.append({
                'aggregated_embedding': aggregated,
                'sub_embeddings': sub_embeddings,
                'aggregation_method': 'mean',
                'num_sub_chunks': num_subs
            })

            embedding_idx += num_subs
        else:
            # Single embedding
            chunk_embeddings.append({
                'aggregated_embedding': all_embeddings[embedding_idx],
                'sub_embeddings': None,
                'aggregation_method': 'none',
                'num_sub_chunks': 1
            })
            embedding_idx += 1

    logger.info(f"Aggregated embeddings for {len(chunk_embeddings)} chunks")

    logger.info(f"Aggregated embeddings for {len(chunk_embeddings)} chunks")

    # Save enriched chunks (original schema + embedding fields)
    enriched_path = output_dir / "enriched_chunks.jsonl"
    logger.info(f"Step 4: Enriching original chunks with embeddings")
    logger.info(f"Saving enriched chunks to: {enriched_path}")

    with open(enriched_path, 'w', encoding='utf-8') as f:
        for original_chunk, record, emb_data in zip(original_chunks, records, chunk_embeddings):
            # Start with original chunk (preserves all fields)
            enriched_chunk = original_chunk.copy()

            # Add embedding enrichment as new nested object
            enriched_chunk['embedding_enrichment'] = {
                'embedding_id': record.embedding_id,
                'enriched_text': record.enriched_text,
                'enriched_text_preview': record.enriched_text[:200],
                'hierarchy_separator': hierarchy_separator,
                'concatenation_method': 'hierarchy_first',

                # Sub-chunking info
                'is_sub_chunked': record.is_sub_chunked,
                'num_sub_chunks': record.num_sub_chunks,

                # Embeddings (aggregated for sub-chunked content)
                'embedding_vector': emb_data['aggregated_embedding'],
                'embedding_dim': len(emb_data['aggregated_embedding']),
                'aggregation_method': emb_data['aggregation_method'],

                # Optional: Store individual sub-chunk embeddings for advanced use
                'sub_chunk_embeddings': emb_data['sub_embeddings'] if emb_data['sub_embeddings'] else None,

                'embedding_timestamp': record.processing_timestamp
            }

            f.write(json.dumps(enriched_chunk, ensure_ascii=False) + '\n')

    # Optional clustering
    if enable_clustering:
        logger.info("Step 5: Performing clustering analysis")

        # Simple similarity-based clustering using cosine similarity
        import numpy as np
        from sklearn.metrics.pairwise import cosine_similarity

        # Use aggregated embeddings for clustering
        aggregated_embeddings = [emb['aggregated_embedding'] for emb in chunk_embeddings]
        embeddings_np = np.array(aggregated_embeddings)
        similarity_matrix = cosine_similarity(embeddings_np)

        # Find similar chunks above threshold
        clusters = []
        processed_indices = set()

        for i in range(len(aggregated_embeddings)):
            if i in processed_indices:
                continue

            # Find all chunks similar to this one
            similar_indices = np.where(similarity_matrix[i] >= similarity_threshold)[0]

            if len(similar_indices) > 1:  # At least 2 chunks (including itself)
                cluster = {
                    'cluster_id': len(clusters),
                    'size': len(similar_indices),
                    'representative_chunk_id': records[i].chunk_id,
                    'representative_text': records[i].enriched_text[:200],
                    'member_chunk_ids': [records[idx].chunk_id for idx in similar_indices],
                    'avg_similarity': float(np.mean(similarity_matrix[i][similar_indices]))
                }
                clusters.append(cluster)
                processed_indices.update(similar_indices)

        if clusters:
            clusters_path = output_dir / "chunk_clusters.json"
            logger.info(f"Found {len(clusters)} clusters, saving to: {clusters_path}")

            with open(clusters_path, 'w', encoding='utf-8') as f:
                json.dump({
                    'total_clusters': len(clusters),
                    'similarity_threshold': similarity_threshold,
                    'clusters': clusters
                }, f, indent=2, ensure_ascii=False)
        else:
            logger.info("No clusters found above similarity threshold")

    # Generate summary statistics
    summary = {
        'input_file': str(chunk_jsonl),
        'output_directory': str(output_dir),
        'processing_timestamp': datetime.now().isoformat(),
        'total_chunks_processed': len(records),
        'chunks_with_hierarchy': with_hierarchy,
        'chunks_without_hierarchy': len(records) - with_hierarchy,
        'chunks_sub_chunked': sub_chunked_count,
        'total_embeddings_generated': len(all_embeddings),
        'hierarchy_separator': hierarchy_separator,
        'min_chunk_length': min_chunk_length,
        'embedding_dimension': len(chunk_embeddings[0]['aggregated_embedding']) if chunk_embeddings else 0,
        'clustering_enabled': enable_clustering,
        'hierarchy_depth_stats': {
            'min': min(hierarchy_depths) if hierarchy_depths else 0,
            'max': max(hierarchy_depths) if hierarchy_depths else 0,
            'avg': sum(hierarchy_depths) / len(hierarchy_depths) if hierarchy_depths else 0
        },
        'sub_chunking_stats': {
            'chunks_requiring_split': sub_chunked_count,
            'total_sub_chunks': total_sub_chunks,
            'aggregation_method': 'mean'
        },
        'output_schema': {
            'description': 'Original chunk schema enriched with embedding_enrichment field',
            'original_fields': ['content', 'chunk_metadata', 'document_metadata', 'processing_timestamp'],
            'enrichment_field': 'embedding_enrichment',
            'enrichment_subfields': [
                'embedding_id',
                'enriched_text',
                'enriched_text_preview',
                'hierarchy_separator',
                'concatenation_method',
                'is_sub_chunked',
                'num_sub_chunks',
                'embedding_vector',
                'embedding_dim',
                'aggregation_method',
                'sub_chunk_embeddings',
                'embedding_timestamp'
            ]
        }
    }

    summary_path = output_dir / "processing_summary.json"
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    logger.info("=" * 70)
    logger.info("Processing Complete!")
    logger.info(f"  Enriched chunks: {enriched_path}")
    logger.info(f"  Summary: {summary_path}")
    if enable_clustering and 'clusters' in locals() and clusters:
        logger.info(f"  Clusters: {output_dir / 'chunk_clusters.json'}")
    logger.info("=" * 70)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Chunk-Level Embedding Generator with Section Hierarchy Concatenation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Basic usage:
    %(prog)s chunks.jsonl output_dir/

  Custom hierarchy separator:
    %(prog)s chunks.jsonl output_dir/ --hierarchy-separator " → "

  With clustering:
    %(prog)s chunks.jsonl output_dir/ --enable-clustering --similarity-threshold 0.7

  Only chunks with hierarchy:
    %(prog)s chunks.jsonl output_dir/ --no-include-empty-hierarchy
        """
    )

    # Required arguments
    parser.add_argument(
        'chunk_jsonl',
        type=str,
        help='Path to chunked JSONL file (e.g., merged_markdown_chunks_content.jsonl)'
    )

    parser.add_argument(
        'output_dir',
        type=str,
        help='Directory to save embedding outputs'
    )

    # Processing options
    parser.add_argument(
        '--hierarchy-separator',
        type=str,
        default=' > ',
        help='String to join section hierarchy elements (default: " > ")'
    )

    parser.add_argument(
        '--min-chunk-length',
        type=int,
        default=20,
        help='Minimum chunk content length in characters (default: 20)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=50,
        help='Batch size for embedding generation (default: 50)'
    )

    parser.add_argument(
        '--no-include-empty-hierarchy',
        action='store_false',
        dest='include_empty_hierarchy',
        help='Exclude chunks without section_hierarchy'
    )

    # Clustering options
    parser.add_argument(
        '--enable-clustering',
        action='store_true',
        help='Perform clustering analysis on embeddings'
    )

    parser.add_argument(
        '--similarity-threshold',
        type=float,
        default=0.6,
        help='Similarity threshold for clustering (default: 0.6)'
    )

    # Logging
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()

    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logger = logging.getLogger(__name__)

    # Validate inputs
    chunk_jsonl = Path(args.chunk_jsonl)
    if not chunk_jsonl.exists():
        logger.error(f"Input file not found: {chunk_jsonl}")
        sys.exit(1)

    output_dir = Path(args.output_dir)

    logger.info("=" * 70)
    logger.info("Chunk-Level Embedding Generator")
    logger.info("=" * 70)
    logger.info(f"Input: {chunk_jsonl}")
    logger.info(f"Output: {output_dir}")
    logger.info(f"Hierarchy separator: '{args.hierarchy_separator}'")
    logger.info(f"Min chunk length: {args.min_chunk_length}")
    logger.info(f"Include empty hierarchy: {args.include_empty_hierarchy}")
    logger.info("=" * 70)

    # Run async processing
    asyncio.run(
        generate_chunk_embeddings(
            chunk_jsonl=chunk_jsonl,
            output_dir=output_dir,
            hierarchy_separator=args.hierarchy_separator,
            min_chunk_length=args.min_chunk_length,
            batch_size=args.batch_size,
            include_empty_hierarchy=args.include_empty_hierarchy,
            enable_clustering=args.enable_clustering,
            similarity_threshold=args.similarity_threshold
        )
    )


if __name__ == '__main__':
    main()
