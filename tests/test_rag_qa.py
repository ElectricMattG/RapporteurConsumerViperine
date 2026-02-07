import json

import pytest

from src.services.rag import main


@pytest.mark.asyncio
async def test_load_qa_index_extracts_context(tmp_path):
    sample = {
        "content": "QUESTION: What happened?\n\nANSWER: Something.\n\nSOURCE CONTEXT:\nHere is the source context text.",
        "document_metadata": {"document_id": "doc1", "source_url": "https://example.com"},
        "embedding_enrichment": {"embedding_vector": [0.1, 0.2, 0.3]},
    }
    qa_file = tmp_path / "qa.jsonl"
    qa_file.write_text(json.dumps(sample) + "\n" + "{malformed}\n")

    items = main.load_qa_index(str(qa_file))

    assert len(items) == 1
    item = items[0]
    assert item["question"] == "What happened?"
    assert item["answer"] == "Something."
    assert item["context"] == "Here is the source context text."
    assert item["doc_metadata"]["source_url"] == "https://example.com"
    assert item["embedding"] == [0.1, 0.2, 0.3]


@pytest.mark.asyncio
async def test_search_qa_index_dedup_by_document(tmp_path):
    original_items = main.state.qa_items
    original_loaded = main.state.qa_index_loaded
    original_client = main.state.embedding_client

    class DummyEmbed:
        async def embed(self, _):
            return [1.0, 0.0]

    try:
        main.state.qa_index_loaded = True
        main.state.embedding_client = DummyEmbed()
        main.state.qa_items = [
            {
                "question": "Q1",
                "answer": "A1",
                "context": "Ctx1",
                "source_url": "url1",
                "doc_metadata": {"document_id": "doc1", "source_url": "url1"},
                "embedding": [1.0, 0.0],
            },
            {
                "question": "Q1-duplicate",
                "answer": "A1",
                "context": "Ctx1",
                "source_url": "url1",
                "doc_metadata": {"document_id": "doc1", "source_url": "url1"},
                "embedding": [0.1, 0.0],
            },
        ]

        result = await main.search_qa_index(query="anything", top_k=5)
        qa_items = result["qa_items"]

        assert len(qa_items) == 1
        assert qa_items[0]["context"] == "Ctx1"
        assert qa_items[0]["doc_metadata"]["document_id"] == "doc1"
    finally:
        main.state.qa_items = original_items
        main.state.qa_index_loaded = original_loaded
        main.state.embedding_client = original_client


def test_build_qa_grounding_text_includes_context():
    qa_items = [
        {"question": "Q?", "answer": "A!", "context": "CTX"},
    ]

    text = main.build_qa_grounding_text(qa_items)

    assert "Q: Q?" in text
    assert "A: A!" in text
    assert "Context: CTX" in text
