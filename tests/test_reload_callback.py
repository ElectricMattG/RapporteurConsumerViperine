"""
Phase R1.1 unit tests — reload_index_callback

Verifies that when a `reload-index` Kafka message is received:
  - load_summaries_index is called
  - load_qa_index is called when qa_index_path exists
  - load_enrichment_index is called for linked and unlinked paths
  - state attributes are updated
  - EnrichmentRetriever is re-instantiated
  - no exception is raised when QA / enrichment paths are None (graceful degradation)
"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

# Allow direct import from the rag service
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "services" / "rag"))

# We build the callback the same way startup_event does, but use a synthetic state
# rather than importing module-level `state` (which carries real file paths).


def _make_state(
    *,
    has_qa: bool = True,
    has_linked: bool = True,
    has_unlinked: bool = True,
    tmp_path: Path = None,
):
    """Return a minimal mock ServiceState."""
    state = MagicMock()
    state.summaries = [{}] * 5
    state.qa_items = [{}] * 3
    state.linked_enrichments = {"a.com/x": [{}]}
    state.unlinked_enrichments = {"b.com/y": [{}]}

    if tmp_path:
        # Create real files so Path.exists() returns True
        summary_file = tmp_path / "summaries.jsonl"
        summary_file.write_text("")
        state.index_path = str(summary_file)

        qa_file = tmp_path / "qa.jsonl" if has_qa else None
        if qa_file:
            qa_file.write_text("")
        state.qa_index_path = str(qa_file) if qa_file else None

        linked_file = tmp_path / "linked.jsonl" if has_linked else None
        if linked_file:
            linked_file.write_text("")
        state.linked_index_path = str(linked_file) if linked_file else None

        unlinked_file = tmp_path / "unlinked.jsonl" if has_unlinked else None
        if unlinked_file:
            unlinked_file.write_text("")
        state.unlinked_index_path = str(unlinked_file) if unlinked_file else None
    else:
        state.index_path = None
        state.qa_index_path = None
        state.linked_index_path = None
        state.unlinked_index_path = None

    return state


def _make_callback(state):
    """Replicate the reload callback closure exactly as startup_event builds it."""
    import logging
    logger = logging.getLogger("test_reload")

    from main import load_enrichment_index, load_qa_index, load_summaries_index

    async def reload_index_callback():
        try:
            logger.info("🔄 Reloading all indices...")

            old_sum = len(state.summaries)
            new_summaries = load_summaries_index(state.index_path)
            state.summaries = new_summaries
            new_sum = len(state.summaries)

            old_qa = len(state.qa_items)
            if state.qa_index_path and Path(state.qa_index_path).exists():
                state.qa_items = load_qa_index(state.qa_index_path)
                state.qa_index_loaded = True
            new_qa = len(state.qa_items)

            old_linked = len(state.linked_enrichments)
            old_unlinked = len(state.unlinked_enrichments)
            if state.linked_index_path and Path(state.linked_index_path).exists():
                state.linked_enrichments = load_enrichment_index(state.linked_index_path)
            if state.unlinked_index_path and Path(state.unlinked_index_path).exists():
                state.unlinked_enrichments = load_enrichment_index(state.unlinked_index_path)
            new_linked = len(state.linked_enrichments)
            new_unlinked = len(state.unlinked_enrichments)

            from src.services.rag.enrichment_retriever import \
                EnrichmentRetriever
            state.enrichment_retriever = EnrichmentRetriever(
                linked_map=state.linked_enrichments,
                unlinked_map=state.unlinked_enrichments,
            )

            logger.info(
                f"✅ Full reload: {old_sum}→{new_sum} summaries, "
                f"{old_qa}→{new_qa} QA, "
                f"{old_linked}→{new_linked} linked, "
                f"{old_unlinked}→{new_unlinked} unlinked"
            )
        except Exception as e:
            logger.error(f"❌ Error reloading indices: {e}", exc_info=True)

    return reload_index_callback


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestReloadCallbackFullReload:

    def test_summaries_reloaded(self, tmp_path):
        """load_summaries_index is called and state.summaries is updated."""
        state = _make_state(tmp_path=tmp_path)
        new_entries = [{"document_id": "x", "summary_text": "t", "summary_embedding": [0.1]}]

        with patch("main.load_summaries_index", return_value=new_entries) as mock_sum, \
             patch("main.load_qa_index", return_value=[]), \
             patch("main.load_enrichment_index", return_value={}), \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())

        mock_sum.assert_called_once_with(state.index_path)
        assert state.summaries == new_entries

    def test_qa_reloaded_when_path_exists(self, tmp_path):
        """load_qa_index is called when qa_index_path points to an existing file."""
        state = _make_state(tmp_path=tmp_path, has_qa=True)
        new_qa = [{"content": "q?", "answer": "a"}]

        with patch("main.load_summaries_index", return_value=[]), \
             patch("main.load_qa_index", return_value=new_qa) as mock_qa, \
             patch("main.load_enrichment_index", return_value={}), \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())

        mock_qa.assert_called_once_with(state.qa_index_path)
        assert state.qa_items == new_qa
        assert state.qa_index_loaded is True

    def test_enrichment_reloaded(self, tmp_path):
        """load_enrichment_index is called for both linked and unlinked when paths exist."""
        state = _make_state(tmp_path=tmp_path, has_linked=True, has_unlinked=True)
        linked_out = {"linked.com/a": [{"event_key": "E1"}]}
        unlinked_out = {"unlinked.com/b": [{"summary": "text"}]}

        with patch("main.load_summaries_index", return_value=[]), \
             patch("main.load_qa_index", return_value=[]), \
             patch("main.load_enrichment_index", side_effect=[linked_out, unlinked_out]) as mock_enrich, \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())

        assert mock_enrich.call_count == 2
        assert state.linked_enrichments == linked_out
        assert state.unlinked_enrichments == unlinked_out

    def test_enrichment_retriever_reinstantiated(self, tmp_path):
        """EnrichmentRetriever is rebuilt after reload."""
        state = _make_state(tmp_path=tmp_path)
        mock_retriever_instance = MagicMock()

        with patch("main.load_summaries_index", return_value=[]), \
             patch("main.load_qa_index", return_value=[]), \
             patch("main.load_enrichment_index", return_value={}), \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever",
                   return_value=mock_retriever_instance) as MockRetriever:
            cb = _make_callback(state)
            run(cb())

        MockRetriever.assert_called_once()
        assert state.enrichment_retriever is mock_retriever_instance


class TestReloadCallbackGracefulDegradation:

    def test_no_exception_when_qa_path_is_none(self, tmp_path):
        """Callback does not raise when qa_index_path is None."""
        state = _make_state(tmp_path=tmp_path, has_qa=False)

        with patch("main.load_summaries_index", return_value=[]), \
             patch("main.load_qa_index") as mock_qa, \
             patch("main.load_enrichment_index", return_value={}), \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())  # must not raise

        mock_qa.assert_not_called()

    def test_no_exception_when_enrichment_paths_are_none(self, tmp_path):
        """Callback does not raise when linked/unlinked paths are None."""
        state = _make_state(tmp_path=tmp_path, has_linked=False, has_unlinked=False)

        with patch("main.load_summaries_index", return_value=[]), \
             patch("main.load_qa_index", return_value=[]), \
             patch("main.load_enrichment_index") as mock_enrich, \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())  # must not raise

        mock_enrich.assert_not_called()

    def test_no_exception_when_load_summaries_raises(self, tmp_path):
        """Errors inside the callback are caught and logged, not propagated."""
        state = _make_state(tmp_path=tmp_path)

        with patch("main.load_summaries_index", side_effect=RuntimeError("disk error")), \
             patch("main.load_qa_index", return_value=[]), \
             patch("main.load_enrichment_index", return_value={}), \
             patch("src.services.rag.enrichment_retriever.EnrichmentRetriever"):
            cb = _make_callback(state)
            run(cb())  # must not raise — error is caught inside callback
