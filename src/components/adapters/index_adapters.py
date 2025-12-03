#!/usr/bin/env python3
"""
Index Schema Adapter

Provides schema transformation between different index formats to maintain
compatibility with the RAG service's expected schema.

Supports:
- Q&A index format (flattened with question/answer/combined embeddings)
- Standard enriched chunks format (nested metadata with single embedding)

Usage:
    from src.components.utils.index_adapter import transform_qa_index_to_chunks

    # Transform Q&A index to chunks format
    transform_qa_index_to_chunks(
        input_path="qa_index.jsonl",
        output_path="enriched_chunks.jsonl",
        embedding_field="combined_embedding"
    )
"""

import json
import logging
from pathlib import Path
from typing import Dict, Literal, Optional

logger = logging.getLogger(__name__)


def transform_qa_index_to_chunks(
    input_path: str,
    output_path: str,
    embedding_field: Literal["question_embedding", "answer_embedding", "combined_embedding", "pair_embedding"] = "combined_embedding",
    include_qa_in_content: bool = True
) -> int:
    """
    Transform Q&A index format to enriched chunks format.

    The Q&A index has a flattened schema with fields like:
    - chunk_content, question, answer
    - question_embedding, answer_embedding, combined_embedding
    - chunk_id, document_id, title, source_url (all at top level)

    The enriched chunks format has nested schema:
    - content, chunk_metadata, document_metadata, embedding_enrichment

    Args:
        input_path: Path to Q&A index JSONL file
        output_path: Path to write transformed chunks JSONL file
        embedding_field: Which embedding to use ("question_embedding", "answer_embedding", or "combined_embedding")
        include_qa_in_content: If True, prepend Q&A to content field for enhanced retrieval (default: True)

    Returns:
        Number of records transformed

    Example Q&A Index Record:
        {
            "id": "qa_chunk_d449c8b35a79_1",
            "type": "qa_pair",
            "question": "What were the contrasting outcomes...",
            "answer": "In South Africa, the state was democratized...",
            "chunk_content": "In recent years, as peace between...",
            "question_embedding": [...],
            "answer_embedding": [...],
            "combined_embedding": [...],
            "chunk_id": "chunk_d449c8b35a79",
            "document_id": "doc_dac01b4e87c7",
            "title": "The Character Gap: How Good Are We?",
            "source_url": "https://doi.org/...",
            "chunk_type": "paragraph",
            "chunk_index": 8,
            "word_count_chunk": 129,
            ...
        }
    """
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info(f"Transforming Q&A index: {input_file.name}")
    logger.info(f"Using embedding field: {embedding_field}")
    logger.info(f"Include Q&A in content: {include_qa_in_content}")
    logger.info(f"Output: {output_file.name}")

    count = 0
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if not line.strip():
                continue

            try:
                qa_record = json.loads(line.strip())

                # Build content field - optionally include Q&A for enhanced retrieval
                # Support both 'chunk_content' and 'context' field names
                chunk_content = qa_record.get("chunk_content") or qa_record.get("context", "")
                if include_qa_in_content and "question" in qa_record and "answer" in qa_record:
                    # Prepend Q&A to chunk content for better LLM context
                    enhanced_content = (
                        f"QUESTION: {qa_record['question']}\n\n"
                        f"ANSWER: {qa_record['answer']}\n\n"
                        f"SOURCE CONTEXT:\n{chunk_content}"
                    )
                else:
                    enhanced_content = chunk_content

                # Calculate accurate metadata from actual content
                word_count = len(enhanced_content.split()) if enhanced_content else 0
                char_count = len(enhanced_content) if enhanced_content else 0
                line_count = enhanced_content.count('\n') + 1 if enhanced_content else 0

                # Get section hierarchy for enriched text
                section_hierarchy = qa_record.get("section_hierarchy", [])
                hierarchy_text = " > ".join(section_hierarchy) if section_hierarchy else ""

                # Build enriched text with hierarchy (matching primary chunks schema)
                if hierarchy_text:
                    enriched_text = f"{hierarchy_text}: {enhanced_content}"
                else:
                    enriched_text = enhanced_content

                # Generate chunk ID for embedding_id
                chunk_id = qa_record.get("chunk_id", "unknown")

                # Clean title - remove author prefix if present
                raw_title = qa_record.get("title", "")
                author = qa_record.get("author", "Unknown")
                clean_title = raw_title

                # Remove "Author - " prefix from title if it exists
                if author and author != "Unknown" and raw_title.startswith(f"{author} - "):
                    clean_title = raw_title[len(author) + 3:]

                # Transform to enriched chunks format
                chunk_record = {
                    "content": enhanced_content,
                    "chunk_metadata": {
                        "chunk_id": chunk_id,
                        "chunk_index": qa_record.get("chunk_index"),
                        "chunk_type": qa_record.get("chunk_type", "paragraph"),
                        "word_count": word_count,
                        "char_count": char_count,
                        "line_count": line_count,
                        "start_position": qa_record.get("start_position"),
                        "end_position": qa_record.get("end_position"),
                        "page_number": qa_record.get("page_number"),
                        "section_hierarchy": section_hierarchy,
                        "parent_section": qa_record.get("parent_section"),
                        "prev_chunk_id": qa_record.get("prev_chunk_id"),
                        "next_chunk_id": qa_record.get("next_chunk_id"),
                        "is_first_in_section": qa_record.get("is_first_in_section", False),
                        "is_last_in_section": qa_record.get("is_last_in_section", False),
                        "contains_urls": qa_record.get("contains_urls", False),
                        "contains_references": qa_record.get("contains_references", False),
                        "table_rows": qa_record.get("table_rows"),
                        "table_cols": qa_record.get("table_cols"),
                        "image_alt_text": qa_record.get("image_alt_text"),
                        "list_item_count": qa_record.get("list_item_count"),
                        "citation_number": qa_record.get("citation_number"),
                        "citation_type": qa_record.get("citation_type"),
                        "references_citations": qa_record.get("references_citations", []),
                        "cited_by_chunks": qa_record.get("cited_by_chunks", []),
                        "citations": qa_record.get("citations", {}),
                        "citation_count": qa_record.get("citation_count", 0),
                        "has_citations": qa_record.get("has_citations", False),
                    },
                    "document_metadata": {
                        "document_id": qa_record.get("document_id"),
                        "source_url": qa_record.get("source_url"),
                        "extraction_method": qa_record.get("extraction_method", "unknown"),
                        "file_size_bytes": qa_record.get("file_size_bytes"),
                        "total_pages": qa_record.get("total_pages"),
                        "extraction_timestamp": qa_record.get("extraction_timestamp"),
                        "language": qa_record.get("language"),
                        "title": clean_title,
                        "author": author,
                        "creation_date": qa_record.get("creation_date"),
                        "modification_date": qa_record.get("modification_date"),
                        "pdf_version": qa_record.get("pdf_version"),
                        "has_images": qa_record.get("has_images"),
                        "has_tables": qa_record.get("has_tables"),
                        "total_chars": qa_record.get("total_chars", 0),
                        "metadata_id": qa_record.get("metadata_id", 0),
                        "content_type": qa_record.get("content_type"),
                        "document_word_count": qa_record.get("document_word_count"),
                        "source_type": qa_record.get("source_type"),
                        "classification_confidence": qa_record.get("classification_confidence"),
                        "classification_reasoning": qa_record.get("classification_reasoning"),
                        "content_hash": qa_record.get("content_hash"),
                    },
                    "embedding_enrichment": {
                        "embedding_id": f"emb_{chunk_id}",
                        "enriched_text": enriched_text,
                        "enriched_text_preview": enriched_text[:200] if len(enriched_text) > 200 else enriched_text,
                        "hierarchy_separator": " > ",
                        "concatenation_method": "hierarchy_first",
                        "is_sub_chunked": False,
                        "num_sub_chunks": 1,
                        "embedding_vector": qa_record.get(embedding_field, []),
                        "embedding_dim": qa_record.get("embedding_dimension", 384),
                        "aggregation_method": "none",
                        "sub_chunk_embeddings": None,
                        "embedding_timestamp": qa_record.get("enrichment_timestamp"),
                    },
                    "processing_timestamp": qa_record.get("enrichment_timestamp"),
                }

                # Optionally preserve Q&A fields for reference
                if "question" in qa_record and "answer" in qa_record:
                    chunk_record["qa_metadata"] = {
                        "question": qa_record.get("question"),
                        "answer": qa_record.get("answer"),
                        "word_count_question": qa_record.get("word_count_question"),
                        "word_count_answer": qa_record.get("word_count_answer"),
                        "qa_index": qa_record.get("qa_index"),
                        "qa_total_for_source": qa_record.get("qa_total_for_source"),
                        "original_chunk_content": chunk_content,  # Store original without Q&A
                    }

                # Write transformed record
                outfile.write(json.dumps(chunk_record, ensure_ascii=False) + '\n')
                count += 1

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error at line {line_num}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error transforming line {line_num}: {e}")
                continue

    logger.info(f"✓ Transformed {count} records")
    return count


def transform_chunks_to_qa_index(
    input_path: str,
    output_path: str,
) -> int:
    """
    Transform enriched chunks format to Q&A index format (reverse transformation).

    Note: This only works if the chunks have qa_metadata preserved.
    For chunks without Q&A data, this will only flatten the metadata structure.

    Args:
        input_path: Path to enriched chunks JSONL file
        output_path: Path to write Q&A index JSONL file

    Returns:
        Number of records transformed
    """
    input_file = Path(input_path)
    output_file = Path(output_path)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    logger.info(f"Transforming chunks to Q&A index: {input_file.name}")
    logger.info(f"Output: {output_file.name}")

    count = 0
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:

        for line_num, line in enumerate(infile, 1):
            if not line.strip():
                continue

            try:
                chunk_record = json.loads(line.strip())

                # Extract nested metadata
                chunk_meta = chunk_record.get("chunk_metadata", {})
                doc_meta = chunk_record.get("document_metadata", {})
                embed_meta = chunk_record.get("embedding_enrichment", {})
                qa_meta = chunk_record.get("qa_metadata", {})

                # Flatten to Q&A index format
                qa_record = {
                    "id": f"qa_{chunk_meta.get('chunk_id', 'unknown')}_{qa_meta.get('qa_index', 1)}",
                    "type": "qa_pair",
                    "chunk_content": chunk_record.get("content", ""),
                    "chunk_id": chunk_meta.get("chunk_id"),
                    "document_id": doc_meta.get("document_id"),
                    "source_url": doc_meta.get("source_url"),
                    "title": doc_meta.get("title"),
                    "content_type": doc_meta.get("content_type"),
                    "classification_confidence": doc_meta.get("classification_confidence"),
                    "total_chars": doc_meta.get("total_chars"),
                    "author": doc_meta.get("author"),
                    "creation_date": doc_meta.get("creation_date"),
                    "section_hierarchy": chunk_meta.get("section_hierarchy", []),
                    "parent_section": chunk_meta.get("parent_section"),
                    "chunk_type": chunk_meta.get("chunk_type"),
                    "chunk_index": chunk_meta.get("chunk_index"),
                    "word_count_chunk": chunk_meta.get("word_count"),
                    "char_count_chunk": chunk_meta.get("char_count"),
                    "line_count_chunk": chunk_meta.get("line_count"),
                    "prev_chunk_id": chunk_meta.get("prev_chunk_id"),
                    "next_chunk_id": chunk_meta.get("next_chunk_id"),
                    "cited_by_chunks": chunk_meta.get("cited_by_chunks", []),
                    "references_citations": chunk_meta.get("references_citations", []),
                    "contains_references": chunk_meta.get("contains_references", False),
                    "contains_urls": chunk_meta.get("contains_urls", False),
                    "is_first_in_section": chunk_meta.get("is_first_in_section", False),
                    "is_last_in_section": chunk_meta.get("is_last_in_section", False),
                    "combined_embedding": embed_meta.get("embedding_vector", []),
                    "embedding_model": embed_meta.get("embedding_model"),
                    "embedding_dimension": embed_meta.get("embedding_dimension"),
                    "enrichment_timestamp": embed_meta.get("enrichment_timestamp"),
                }

                # Add Q&A fields if available
                if qa_meta:
                    qa_record.update({
                        "question": qa_meta.get("question"),
                        "answer": qa_meta.get("answer"),
                        "word_count_question": qa_meta.get("word_count_question"),
                        "word_count_answer": qa_meta.get("word_count_answer"),
                        "qa_index": qa_meta.get("qa_index"),
                        "qa_total_for_source": qa_meta.get("qa_total_for_source"),
                    })

                # Write transformed record
                outfile.write(json.dumps(qa_record, ensure_ascii=False) + '\n')
                count += 1

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error at line {line_num}: {e}")
                continue
            except Exception as e:
                logger.error(f"Error transforming line {line_num}: {e}")
                continue

    logger.info(f"✓ Transformed {count} records")
    return count


def validate_index_schema(file_path: str) -> Dict[str, any]:
    """
    Validate and detect the schema of an index file.

    Args:
        file_path: Path to JSONL index file

    Returns:
        Dictionary with schema information:
        - schema_type: "qa_index" or "enriched_chunks"
        - has_embeddings: bool
        - embedding_fields: list of embedding field names
        - sample_record: first record for inspection
        - total_records: total number of records
    """
    file = Path(file_path)
    if not file.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file, 'r', encoding='utf-8') as f:
        # Read first record
        first_line = f.readline().strip()
        if not first_line:
            raise ValueError("Empty file")

        sample = json.loads(first_line)

        # Count total records
        total = 1
        for _ in f:
            total += 1

    # Detect schema type
    schema_type = "unknown"
    embedding_fields = []

    # Check for Q&A index schema (flattened)
    if "chunk_content" in sample and "chunk_id" in sample and "document_id" in sample:
        schema_type = "qa_index"

        # Check for embedding fields
        for field in ["question_embedding", "answer_embedding", "combined_embedding"]:
            if field in sample:
                embedding_fields.append(field)

    # Check for enriched chunks schema (nested)
    elif "content" in sample and "chunk_metadata" in sample and "document_metadata" in sample:
        schema_type = "enriched_chunks"

        # Check for embedding
        if "embedding_enrichment" in sample and "embedding_vector" in sample["embedding_enrichment"]:
            embedding_fields.append("embedding_vector")

    return {
        "schema_type": schema_type,
        "file_path": str(file),
        "file_size_mb": file.stat().st_size / (1024 * 1024),
        "total_records": total,
        "has_embeddings": len(embedding_fields) > 0,
        "embedding_fields": embedding_fields,
        "embedding_dimension": sample.get("embedding_dimension") or sample.get("embedding_enrichment", {}).get("embedding_dimension"),
        "sample_keys": list(sample.keys())[:20],  # First 20 keys for inspection
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Transform index schemas")
    parser.add_argument("command", choices=["qa-to-chunks", "chunks-to-qa", "validate"],
                       help="Transformation command")
    parser.add_argument("--input", "-i", required=True, help="Input JSONL file")
    parser.add_argument("--output", "-o", help="Output JSONL file (not needed for validate)")
    parser.add_argument("--embedding", "-e",
                       choices=["question_embedding", "answer_embedding", "combined_embedding", "pair_embedding"],
                       default="combined_embedding",
                       help="Which embedding field to use (for qa-to-chunks)")
    parser.add_argument("--no-qa-in-content", action="store_true",
                       help="Do not include Q&A in content field (for qa-to-chunks)")

    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    if args.command == "validate":
        info = validate_index_schema(args.input)
        print("\n" + "="*80)
        print("INDEX SCHEMA VALIDATION")
        print("="*80)
        print(f"File: {info['file_path']}")
        print(f"Size: {info['file_size_mb']:.2f} MB")
        print(f"Schema Type: {info['schema_type']}")
        print(f"Total Records: {info['total_records']:,}")
        print(f"Has Embeddings: {info['has_embeddings']}")
        print(f"Embedding Fields: {', '.join(info['embedding_fields'])}")
        print(f"Embedding Dimension: {info['embedding_dimension']}")
        print(f"\nSample Keys: {', '.join(info['sample_keys'])}")
        print("="*80 + "\n")

    elif args.command == "qa-to-chunks":
        if not args.output:
            parser.error("--output is required for qa-to-chunks")
        count = transform_qa_index_to_chunks(
            args.input,
            args.output,
            args.embedding,
            include_qa_in_content=not args.no_qa_in_content
        )
        print(f"✓ Transformed {count} records from Q&A index to enriched chunks format")

    elif args.command == "chunks-to-qa":
        if not args.output:
            parser.error("--output is required for chunks-to-qa")
        count = transform_chunks_to_qa_index(args.input, args.output)
        print(f"✓ Transformed {count} records from enriched chunks to Q&A index format")
