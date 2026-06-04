"""
tests/unit/test_index_adapter.py

Tests for src/utils/index_adapter.py

Contracts verified:
- transform_qa_index_to_chunks: writes enriched-chunks JSONL
- transform_chunks_to_qa_index: inverse transform
- round-trip preserves key fields
- validate_index_schema detects both schema types
- missing input file raises FileNotFoundError
- empty content lines are skipped silently
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from utils.index_adapter import (transform_chunks_to_qa_index,
                                 transform_qa_index_to_chunks,
                                 validate_index_schema)

# ── helpers ────────────────────────────────────────────────────────────────────

def _qa_record(**overrides):
    base = {
        "id": "qa_chunk_abc123_1",
        "type": "qa_pair",
        "question": "What happened?",
        "answer": "Nothing much.",
        "chunk_content": "The quick brown fox.",
        "combined_embedding": [0.1] * 384,
        "question_embedding": [0.2] * 384,
        "answer_embedding": [0.3] * 384,
        "chunk_id": "chunk_abc123",
        "document_id": "doc_dac01b",
        "title": "Sample Title",
        "source_url": "https://example.com/source",
        "chunk_type": "paragraph",
        "chunk_index": 1,
        "word_count_chunk": 5,
        "embedding_dimension": 384,
    }
    base.update(overrides)
    return base


def _write_qa_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ── QA → Chunks ────────────────────────────────────────────────────────────────

class TestQaToChunks:
    def test_output_file_created(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record()])
        transform_qa_index_to_chunks(str(src), str(dst))
        assert dst.exists()

    def test_count_matches_input(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record(), _qa_record(chunk_id="chunk_xyz")])
        n = transform_qa_index_to_chunks(str(src), str(dst))
        assert n == 2
        assert len(_read_jsonl(dst)) == 2

    def test_output_schema_has_required_keys(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record()])
        transform_qa_index_to_chunks(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        assert "content" in row
        assert "chunk_metadata" in row
        assert "document_metadata" in row
        assert "embedding_enrichment" in row

    def test_embedding_vector_preserved(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        rec = _qa_record()
        _write_qa_jsonl(src, [rec])
        transform_qa_index_to_chunks(str(src), str(dst), embedding_field="combined_embedding")
        row = _read_jsonl(dst)[0]
        assert row["embedding_enrichment"]["embedding_vector"] == [0.1] * 384

    def test_qa_included_in_content_by_default(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record()])
        transform_qa_index_to_chunks(str(src), str(dst), include_qa_in_content=True)
        row = _read_jsonl(dst)[0]
        assert "QUESTION:" in row["content"]
        assert "ANSWER:" in row["content"]

    def test_no_qa_in_content_when_disabled(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record()])
        transform_qa_index_to_chunks(str(src), str(dst), include_qa_in_content=False)
        row = _read_jsonl(dst)[0]
        assert "QUESTION:" not in row["content"]

    def test_document_metadata_source_url(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        _write_qa_jsonl(src, [_qa_record()])
        transform_qa_index_to_chunks(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        assert row["document_metadata"]["source_url"] == "https://example.com/source"

    def test_empty_lines_skipped(self, tmp_path):
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        with open(src, "w") as f:
            f.write("\n")
            f.write(json.dumps(_qa_record()) + "\n")
            f.write("   \n")
        n = transform_qa_index_to_chunks(str(src), str(dst))
        assert n == 1

    def test_missing_input_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            transform_qa_index_to_chunks(
                str(tmp_path / "nonexistent.jsonl"),
                str(tmp_path / "out.jsonl"),
            )

    def test_malformed_json_line_skipped(self, tmp_path):
        """A corrupt JSON line is logged and skipped, valid lines still written."""
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        with open(src, "w") as f:
            f.write(json.dumps(_qa_record()) + "\n")
            f.write("{ this is not valid json }\n")
            f.write(json.dumps(_qa_record(chunk_id="chunk_ok")) + "\n")
        n = transform_qa_index_to_chunks(str(src), str(dst))
        assert n == 2  # only the two valid lines

    def test_author_prefix_stripped_from_title(self, tmp_path):
        """Title 'Author - Real Title' has the 'Author - ' prefix removed."""
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        rec = _qa_record(author="Jane Doe", title="Jane Doe - The Real Title")
        _write_qa_jsonl(src, [rec])
        transform_qa_index_to_chunks(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        assert row["document_metadata"]["title"] == "The Real Title"

    def test_section_hierarchy_prepended_to_enriched_text(self, tmp_path):
        """When section_hierarchy is present it is prepended to enriched_text."""
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        rec = _qa_record(section_hierarchy=["Chapter 1", "Section A"])
        _write_qa_jsonl(src, [rec])
        transform_qa_index_to_chunks(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        enriched = row["embedding_enrichment"]["enriched_text"]
        assert enriched.startswith("Chapter 1 > Section A:")

    def test_context_field_used_when_chunk_content_absent(self, tmp_path):
        """Falls back to 'context' field when 'chunk_content' is missing."""
        src = tmp_path / "qa.jsonl"
        dst = tmp_path / "chunks.jsonl"
        rec = _qa_record()
        del rec["chunk_content"]
        rec["context"] = "Context body text."
        _write_qa_jsonl(src, [rec])
        transform_qa_index_to_chunks(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        assert "Context body text." in row["content"]


# ── Chunks → QA ────────────────────────────────────────────────────────────────

class TestChunksToQa:
    def _make_chunk_record(self, **overrides):
        base = {
            "content": "The quick brown fox.",
            "chunk_metadata": {
                "chunk_id": "chunk_abc123",
                "chunk_index": 1,
                "chunk_type": "paragraph",
                "word_count": 5,
                "char_count": 20,
                "line_count": 1,
                "section_hierarchy": [],
                "prev_chunk_id": None,
                "next_chunk_id": None,
                "is_first_in_section": False,
                "is_last_in_section": False,
                "contains_urls": False,
                "contains_references": False,
                "cited_by_chunks": [],
                "references_citations": [],
            },
            "document_metadata": {
                "document_id": "doc_dac01b",
                "source_url": "https://example.com/source",
                "title": "Sample Title",
                "author": "Unknown",
                "total_chars": 20,
            },
            "embedding_enrichment": {
                "embedding_id": "emb_chunk_abc123",
                "enriched_text": "The quick brown fox.",
                "enriched_text_preview": "The quick brown fox.",
                "embedding_vector": [0.5] * 384,
                "embedding_dim": 384,
            },
            "qa_metadata": {
                "question": "What happened?",
                "answer": "Nothing much.",
                "qa_index": 1,
            },
        }
        base.update(overrides)
        return base

    def _write_chunks_jsonl(self, path: Path, records: list[dict]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

    def test_output_file_created(self, tmp_path):
        src = tmp_path / "chunks.jsonl"
        dst = tmp_path / "qa.jsonl"
        self._write_chunks_jsonl(src, [self._make_chunk_record()])
        transform_chunks_to_qa_index(str(src), str(dst))
        assert dst.exists()

    def test_count_matches_input(self, tmp_path):
        src = tmp_path / "chunks.jsonl"
        dst = tmp_path / "qa.jsonl"
        self._write_chunks_jsonl(src, [self._make_chunk_record(), self._make_chunk_record()])
        n = transform_chunks_to_qa_index(str(src), str(dst))
        assert n == 2

    def test_qa_fields_in_output(self, tmp_path):
        src = tmp_path / "chunks.jsonl"
        dst = tmp_path / "qa.jsonl"
        self._write_chunks_jsonl(src, [self._make_chunk_record()])
        transform_chunks_to_qa_index(str(src), str(dst))
        row = _read_jsonl(dst)[0]
        assert row["question"] == "What happened?"
        assert row["answer"] == "Nothing much."

    def test_missing_input_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            transform_chunks_to_qa_index(
                str(tmp_path / "no.jsonl"),
                str(tmp_path / "out.jsonl"),
            )

    def test_malformed_json_line_skipped(self, tmp_path):
        src = tmp_path / "chunks.jsonl"
        dst = tmp_path / "qa.jsonl"
        with open(src, "w") as f:
            f.write(json.dumps(self._make_chunk_record()) + "\n")
            f.write("not json at all\n")
        n = transform_chunks_to_qa_index(str(src), str(dst))
        assert n == 1

    def test_chunk_without_qa_metadata_still_transforms(self, tmp_path):
        """A chunk lacking qa_metadata flattens without question/answer keys."""
        src = tmp_path / "chunks.jsonl"
        dst = tmp_path / "qa.jsonl"
        rec = self._make_chunk_record()
        del rec["qa_metadata"]
        self._write_chunks_jsonl(src, [rec])
        n = transform_chunks_to_qa_index(str(src), str(dst))
        assert n == 1
        row = _read_jsonl(dst)[0]
        assert row["document_id"] == "doc_dac01b"


# ── Round-trip ─────────────────────────────────────────────────────────────────

class TestRoundTrip:
    def test_source_url_survives_round_trip(self, tmp_path):
        qa_src = tmp_path / "qa.jsonl"
        chunks = tmp_path / "chunks.jsonl"
        qa_rt = tmp_path / "qa_rt.jsonl"

        _write_qa_jsonl(qa_src, [_qa_record()])
        transform_qa_index_to_chunks(str(qa_src), str(chunks))
        transform_chunks_to_qa_index(str(chunks), str(qa_rt))

        out = _read_jsonl(qa_rt)[0]
        assert out["source_url"] == "https://example.com/source"

    def test_document_id_survives_round_trip(self, tmp_path):
        qa_src = tmp_path / "qa.jsonl"
        chunks = tmp_path / "chunks.jsonl"
        qa_rt = tmp_path / "qa_rt.jsonl"

        _write_qa_jsonl(qa_src, [_qa_record()])
        transform_qa_index_to_chunks(str(qa_src), str(chunks))
        transform_chunks_to_qa_index(str(chunks), str(qa_rt))

        out = _read_jsonl(qa_rt)[0]
        assert out["document_id"] == "doc_dac01b"


# ── Schema validation ──────────────────────────────────────────────────────────

class TestSchemaValidation:
    def test_detects_qa_schema(self, tmp_path):
        f = tmp_path / "qa.jsonl"
        _write_qa_jsonl(f, [_qa_record()])
        info = validate_index_schema(str(f))
        assert info["schema_type"] == "qa_index"

    def test_reports_has_embeddings_true(self, tmp_path):
        f = tmp_path / "qa.jsonl"
        _write_qa_jsonl(f, [_qa_record()])
        info = validate_index_schema(str(f))
        assert info["has_embeddings"] is True

    def test_reports_total_records(self, tmp_path):
        f = tmp_path / "qa.jsonl"
        _write_qa_jsonl(f, [_qa_record(), _qa_record(), _qa_record()])
        info = validate_index_schema(str(f))
        assert info["total_records"] == 3

    def test_detects_enriched_chunks_schema(self, tmp_path):
        f = tmp_path / "chunks.jsonl"
        chunk = {
            "content": "Hello",
            "chunk_metadata": {"chunk_id": "c1"},
            "document_metadata": {"document_id": "d1"},
            "embedding_enrichment": {"embedding_vector": [0.1] * 384},
        }
        with open(f, "w") as fh:
            fh.write(json.dumps(chunk) + "\n")
        info = validate_index_schema(str(f))
        assert info["schema_type"] == "enriched_chunks"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            validate_index_schema(str(tmp_path / "nope.jsonl"))

    def test_empty_file_raises_value_error(self, tmp_path):
        f = tmp_path / "empty.jsonl"
        f.touch()
        with pytest.raises(ValueError):
            validate_index_schema(str(f))

    def test_unknown_schema_detected(self, tmp_path):
        """A record matching neither schema is reported as 'unknown'."""
        f = tmp_path / "weird.jsonl"
        with open(f, "w") as fh:
            fh.write(json.dumps({"foo": "bar", "baz": 1}) + "\n")
        info = validate_index_schema(str(f))
        assert info["schema_type"] == "unknown"
        assert info["has_embeddings"] is False

    def test_reports_sample_keys(self, tmp_path):
        f = tmp_path / "qa.jsonl"
        _write_qa_jsonl(f, [_qa_record()])
        info = validate_index_schema(str(f))
        assert "chunk_content" in info["sample_keys"]
