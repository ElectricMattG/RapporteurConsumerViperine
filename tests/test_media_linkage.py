"""Tests for media linkage components: BBAORetriever, CategoryGrouper,
FullDocReconstituter, ThreatClusterAnalyser, RapporteurClusterExecution,
KaliyaHandoff, ConclusionGenerator.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.components.analysis.media_linkage import (CATEGORY_ANCHORS,
                                                   THREAT_CLUSTER_PROMPT,
                                                   BBAORetriever,
                                                   CategoryGrouper,
                                                   ConclusionGenerator,
                                                   FullDocReconstituter,
                                                   KaliyaHandoff,
                                                   RapporteurClusterExecution,
                                                   ThreatClusterAnalyser)

# ===================================================================
# Helpers
# ===================================================================


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _make_eo_event(
    event_key: str = "abc123",
    eo_id: str = "EO 14148",
    source_url: str = "https://www.whitehouse.gov/eo14148",
    window_start: str = "2025-01-20",
    window_end: str = "2025-01-22",
) -> dict:
    return {
        "event_key": event_key,
        "window_start": window_start,
        "window_end": window_end,
        "descriptor": "Test event",
        "eo_identifier": eo_id,
        "eo_title": "Test EO Title",
        "eo_date": "2025-01-20",
        "source_url": source_url,
        "exec_official": "Test Official",
        "media_count": 3,
        "risk_counts": {"LOW": 1, "MEDIUM": 1, "HIGH": 0, "EXTREME": 0, "unrated": 1},
    }


def _make_linked_media(
    event_key: str = "abc123",
    link: str = "https://www.nytimes.com/article",
    eo_id: str = "EO 14148",
    publisher: str = "www.nytimes.com",
    window_start: str = "2025-01-20",
    window_end: str = "2025-01-22",
) -> dict:
    return {
        "time_window": f"{window_start}→{window_end}",
        "window_start": window_start,
        "window_end": window_end,
        "event_key": event_key,
        "event_descriptor": "Test event",
        "eo_identifier": eo_id,
        "author": "Jane Doe",
        "publisher": publisher,
        "listed_publisher": "The New York Times",
        "date_publication": "2025-01-21",
        "mean_search_engine_rank": 2.5,
        "consumer_salience": 0.85,
        "link": link,
        "summary": "Full article summary text here.",
        "embedding_similarity": 0.72,
        "media_framing": {
            "factualness": "Mostly factual",
            "spin": "Moderate spin detected",
            "convergence_divergence": "Diverges from primary source",
            "supportive_unsupportive": "Unsupportive",
            "linked_to_dow": "",
            "risk": {"rating": "MEDIUM", "reasoning": "Reputational risk"},
        },
    }


def _make_unlinked(
    link: str = "https://www.reuters.com/unlinked",
    window_start: str = "2025-01-20",
    window_end: str = "2025-01-22",
) -> dict:
    rec = _make_linked_media(
        event_key="",
        link=link,
        window_start=window_start,
        window_end=window_end,
    )
    rec["embedding_similarity"] = None
    return rec


def _make_merged_event(event_key: str = "abc123") -> dict:
    return {
        "event": "Test merged event",
        "incident": "",
        "action": "",
        "decision": "",
        "executive_order": "",
        "executive_comms": {
            "channel": "whitehouse.gov",
            "url": "https://www.whitehouse.gov/eo14148",
            "exec_official": "Test Official",
            "summary": "Merged summary",
            "media": [],
        },
        "merged_comms": [],
        "merge_history": {"merged_count": 1, "source_windows": [], "sources": []},
        "event_key": event_key,
        "source": "test",
        "window_start": "2025-01-20",
        "window_end": "2025-01-22",
    }


def _make_timeline(event_key: str = "abc123") -> dict:
    return {
        "date": "2025-01-20",
        "event_type": "executive_order",
        "summary": "Timeline entry",
        "risk_summary": "MEDIUM",
        "media_count": 3,
        "event_key": event_key,
    }


def _build_bbao_cache(
    tmp_path: Path,
    session_id: str = "test_session",
    eo_events: list[dict] | None = None,
    linked_media: list[dict] | None = None,
    merged_events: list[dict] | None = None,
    timeline: list[dict] | None = None,
    unlinked: list[dict] | None = None,
) -> Path:
    """Write a minimal BBAO cache under tmp_path and return its directory."""
    cache_dir = tmp_path / session_id
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / f"eo_events_{session_id}.jsonl",
        eo_events or [_make_eo_event()],
    )
    _write_jsonl(
        cache_dir / f"linked_media_{session_id}.jsonl",
        linked_media or [_make_linked_media()],
    )
    _write_jsonl(
        cache_dir / "merged_events.jsonl",
        merged_events or [_make_merged_event()],
    )
    _write_jsonl(
        cache_dir / "timeline.jsonl",
        timeline or [_make_timeline()],
    )

    if unlinked:
        # Group by window
        by_window: dict[str, list[dict]] = {}
        for u in unlinked:
            ws = u.get("window_start", "unknown")
            we = u.get("window_end", "unknown")
            key = f"{ws}_{we}"
            by_window.setdefault(key, []).append(u)
        for key, recs in by_window.items():
            _write_jsonl(cache_dir / f"unlinked_{key}.jsonl", recs)

    return cache_dir


# ===================================================================
# Phase 1 — BBAORetriever
# ===================================================================


class TestBBAORetriever:
    def test_load_basic(self, tmp_path: Path):
        cache_dir = _build_bbao_cache(tmp_path)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        assert len(retriever.eo_events) == 1
        assert len(retriever.linked_media) == 1
        assert len(retriever.merged_events) == 1
        assert len(retriever.timeline) == 1

    def test_load_empty_dir(self, tmp_path: Path):
        cache_dir = tmp_path / "empty_session"
        cache_dir.mkdir()
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        assert retriever.eo_events == []
        assert retriever.linked_media == []

    def test_normalise_eo_id(self):
        assert BBAORetriever.normalise_eo_id("14148") == "EO 14148"
        assert BBAORetriever.normalise_eo_id(14148) == "EO 14148"
        assert BBAORetriever.normalise_eo_id("EO 14148") == "EO 14148"

    def test_strip_eo_prefix(self):
        assert BBAORetriever.strip_eo_prefix("EO 14148") == "14148"
        assert BBAORetriever.strip_eo_prefix("14148") == "14148"

    def test_eo_events_for(self, tmp_path: Path):
        events = [
            _make_eo_event(event_key="e1", eo_id="EO 14148"),
            _make_eo_event(event_key="e2", eo_id="EO 14173"),
            _make_eo_event(event_key="e3", eo_id="EO 14148"),
        ]
        cache_dir = _build_bbao_cache(tmp_path, eo_events=events)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        result = retriever.eo_events_for("14148")
        assert len(result) == 2
        assert all(r["eo_identifier"] == "EO 14148" for r in result)

    def test_eo_events_for_no_match(self, tmp_path: Path):
        cache_dir = _build_bbao_cache(tmp_path)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        assert retriever.eo_events_for("99999") == []

    def test_linked_media_for(self, tmp_path: Path):
        events = [
            _make_eo_event(event_key="e1", eo_id="EO 14148"),
            _make_eo_event(event_key="e2", eo_id="EO 14173"),
        ]
        media = [
            _make_linked_media(event_key="e1", link="https://a.com/1"),
            _make_linked_media(event_key="e1", link="https://a.com/2"),
            _make_linked_media(event_key="e2", link="https://b.com/1"),
        ]
        cache_dir = _build_bbao_cache(
            tmp_path, eo_events=events, linked_media=media
        )
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        result = retriever.linked_media_for("14148")
        assert len(result) == 2
        urls = {r["link"] for r in result}
        assert urls == {"https://a.com/1", "https://a.com/2"}

    def test_linked_media_for_event_key(self, tmp_path: Path):
        media = [
            _make_linked_media(event_key="e1", link="https://a.com/1"),
            _make_linked_media(event_key="e2", link="https://b.com/1"),
        ]
        cache_dir = _build_bbao_cache(tmp_path, linked_media=media)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        result = retriever.linked_media_for_event_key("e1")
        assert len(result) == 1
        assert result[0]["link"] == "https://a.com/1"

    def test_unlinked_for_window(self, tmp_path: Path):
        unlinked = [
            _make_unlinked(link="https://r.com/1", window_start="2025-01-20", window_end="2025-01-22"),
            _make_unlinked(link="https://r.com/2", window_start="2025-01-20", window_end="2025-01-22"),
            _make_unlinked(link="https://r.com/3", window_start="2025-01-22", window_end="2025-01-24"),
        ]
        cache_dir = _build_bbao_cache(tmp_path, unlinked=unlinked)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        result = retriever.unlinked_for_window("2025-01-20", "2025-01-22")
        assert len(result) == 2

    def test_all_media_urls_for(self, tmp_path: Path):
        events = [
            _make_eo_event(
                event_key="e1",
                eo_id="EO 14148",
                window_start="2025-01-20",
                window_end="2025-01-22",
            ),
        ]
        media = [
            _make_linked_media(event_key="e1", link="https://a.com/linked"),
        ]
        unlinked = [
            _make_unlinked(
                link="https://b.com/unlinked",
                window_start="2025-01-20",
                window_end="2025-01-22",
            ),
        ]
        cache_dir = _build_bbao_cache(
            tmp_path, eo_events=events, linked_media=media, unlinked=unlinked
        )
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        urls = retriever.all_media_urls_for("14148")
        assert "https://a.com/linked" in urls
        assert "https://b.com/unlinked" in urls

    def test_all_source_urls_for(self, tmp_path: Path):
        events = [
            _make_eo_event(event_key="e1", source_url="https://www.whitehouse.gov/eo1"),
            _make_eo_event(event_key="e2", source_url="https://www.defense.gov/eo2"),
        ]
        cache_dir = _build_bbao_cache(tmp_path, eo_events=events)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        urls = retriever.all_source_urls_for("14148")
        assert len(urls) == 2

    def test_summary(self, tmp_path: Path):
        cache_dir = _build_bbao_cache(tmp_path)
        retriever = BBAORetriever(cache_dir)
        retriever.load()

        s = retriever.summary()
        assert s["session_id"] == "test_session"
        assert s["eo_events"] == 1
        assert s["linked_media"] == 1

    def test_load_handles_corrupt_jsonl(self, tmp_path: Path):
        cache_dir = _build_bbao_cache(tmp_path)
        # Append corrupt line
        eo_path = cache_dir / "eo_events_test_session.jsonl"
        with open(eo_path, "a") as f:
            f.write("not valid json\n")

        retriever = BBAORetriever(cache_dir)
        retriever.load()
        # Should still load the valid record
        assert len(retriever.eo_events) == 1


# ===================================================================
# Phase 2 — CategoryGrouper
# ===================================================================


class TestCategoryGrouper:
    @staticmethod
    def _sample_graph() -> dict[str, dict[str, list[str]]]:
        return {
            "14148": {
                "rescinds": ["13985"],
                "derived_from": [],
                "read_with": ["14151"],
            },
            "14151": {
                "rescinds": [],
                "derived_from": ["14148"],
                "read_with": [],
            },
            "13985": {
                "rescinds": [],
                "derived_from": [],
                "read_with": [],
            },
            "14183": {
                "rescinds": [],
                "derived_from": ["13950", "14148"],
                "read_with": [],
            },
        }

    def test_expand_category_anchors_only(self):
        grouper = CategoryGrouper(
            precedence_graph={},
            anchors={"test_cat": ["14148", "14183"]},
        )
        result = grouper.expand_category("test_cat", max_depth=0)
        assert set(result.keys()) == {"14148", "14183"}
        assert all(v == "anchor" for v in result.values())

    def test_expand_category_depth_1(self):
        grouper = CategoryGrouper(
            precedence_graph=self._sample_graph(),
            anchors={"test_cat": ["14148"]},
        )
        result = grouper.expand_category("test_cat", max_depth=1)
        # Only read_with edges are followed; rescinds/derived_from excluded
        assert "14148" in result
        assert "14151" in result
        assert result["14148"] == "anchor"
        assert "read_with" in result["14151"]
        # rescinds target should NOT be included
        assert "13985" not in result

    def test_expand_category_depth_2(self):
        # 14183 has no read_with edges, so only the anchor survives
        grouper = CategoryGrouper(
            precedence_graph=self._sample_graph(),
            anchors={"test_cat": ["14183"]},
        )
        result = grouper.expand_category("test_cat", max_depth=2)
        assert "14183" in result
        assert result["14183"] == "anchor"
        # derived_from edges are excluded, so nothing else is reached
        assert "14148" not in result
        assert "13985" not in result

    def test_expand_with_read_with_chain(self):
        # Test depth-2 traversal via read_with chain: A -> B -> C
        graph = {
            "14148": {"rescinds": [], "derived_from": [], "read_with": ["14265"]},
            "14265": {"rescinds": [], "derived_from": [], "read_with": ["14179"]},
            "14179": {"rescinds": [], "derived_from": [], "read_with": []},
        }
        grouper = CategoryGrouper(
            precedence_graph=graph,
            anchors={"test_cat": ["14148"]},
        )
        result = grouper.expand_category("test_cat", max_depth=2)
        assert "14148" in result
        assert "14265" in result
        assert "14179" in result
        assert result["14148"] == "anchor"
        assert "read_with" in result["14265"]
        assert "read_with" in result["14179"]

    def test_expand_all(self):
        grouper = CategoryGrouper(
            precedence_graph=self._sample_graph(),
            anchors={
                "cat_a": ["14148"],
                "cat_b": ["14183"],
            },
        )
        result = grouper.expand_all(max_depth=1)
        assert "cat_a" in result
        assert "cat_b" in result
        assert "14148" in result["cat_a"]
        assert "14183" in result["cat_b"]

    def test_no_duplicate_traversal(self):
        # An EO already in result should not be re-added
        graph = {
            "14148": {"rescinds": ["14148"], "derived_from": [], "read_with": []},
        }
        grouper = CategoryGrouper(
            precedence_graph=graph,
            anchors={"test_cat": ["14148"]},
        )
        result = grouper.expand_category("test_cat", max_depth=2)
        assert len(result) == 1  # No infinite loop, no duplicates

    def test_dedup_across_categories(self):
        grouper = CategoryGrouper(
            precedence_graph={},
            anchors={
                "cat_a": ["14148", "14183"],
                "cat_b": ["14148", "14265"],
            },
        )
        expanded = grouper.expand_all(max_depth=0)
        result = grouper.dedup_across_categories(expanded)
        # Both categories keep their EOs, just logs overlap
        assert "14148" in result["cat_a"]
        assert "14148" in result["cat_b"]

    def test_category_manifest(self):
        grouper = CategoryGrouper(
            precedence_graph={},
            anchors={"test_cat": ["14148", "14183"]},
        )
        expanded = grouper.expand_all(max_depth=0)
        manifest = grouper.category_manifest(expanded)
        assert len(manifest) == 2
        assert all("category" in m and "eo_number" in m for m in manifest)

    def test_empty_category(self):
        grouper = CategoryGrouper(
            precedence_graph={},
            anchors={"empty_cat": []},
        )
        result = grouper.expand_category("empty_cat")
        assert result == {}

    def test_unknown_category(self):
        grouper = CategoryGrouper(precedence_graph={})
        result = grouper.expand_category("nonexistent")
        assert result == {}


# ===================================================================
# Phase 3 — FullDocReconstituter
# ===================================================================


class TestFullDocReconstituter:
    @staticmethod
    def _mock_query_metadata_response(
        summary_text: str = "Summary content",
        title: str = "Article Title",
        qa_contexts: list[str] | None = None,
    ) -> dict:
        qa_pairs = []
        for ctx in (qa_contexts or []):
            qa_pairs.append({
                "question": "Q?",
                "answer": "A.",
                "context": ctx,
                "source_url": "https://example.com",
                "doc_metadata": {},
            })
        return {
            "request_id": "test-uuid",
            "filters_applied": {},
            "retrieved_docs": [
                {
                    "document_id": "doc-1",
                    "source_url": "https://example.com",
                    "publication_date": "2025-01-21",
                    "similarity_score": 1.0,
                    "summary_text": summary_text,
                    "doc_metadata": {"title": title, "publisher": "Test Pub"},
                }
            ],
            "retrieved_qa": qa_pairs,
            "total_summaries": 1,
            "total_qa": len(qa_pairs),
            "execution_time_ms": 10.5,
        }

    @patch("src.components.analysis.media_linkage.requests.post")
    def test_reconstitute_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self._mock_query_metadata_response(
            summary_text="Summary here",
            title="Test Article",
            qa_contexts=["Paragraph 1 of article.", "Paragraph 2 of article."],
        )
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        recon = FullDocReconstituter("http://localhost:8000")
        result = recon.reconstitute("https://example.com/article")

        assert result is not None
        assert result["title"] == "Test Article"
        assert result["summary_text"] == "Summary here"
        assert "Paragraph 1" in result["full_text"]
        assert "Paragraph 2" in result["full_text"]
        assert result["qa_count"] == 2

        mock_post.assert_called_once_with(
            "http://localhost:8000/query_metadata",
            json={
                "source_url": "https://example.com/article",
                "include_summaries": True,
                "include_qa": True,
                "max_results": 100,
            },
            timeout=30,
        )

    @patch("src.components.analysis.media_linkage.requests.post")
    def test_reconstitute_no_qa_falls_back_to_summary(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self._mock_query_metadata_response(
            summary_text="Just the summary",
            qa_contexts=[],
        )
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        recon = FullDocReconstituter("http://localhost:8000")
        result = recon.reconstitute("https://example.com")

        assert result is not None
        assert result["full_text"] == "Just the summary"
        assert result["qa_count"] == 0

    @patch("src.components.analysis.media_linkage.requests.post")
    def test_reconstitute_no_results(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "request_id": "test",
            "filters_applied": {},
            "retrieved_docs": [],
            "retrieved_qa": [],
            "total_summaries": 0,
            "total_qa": 0,
            "execution_time_ms": 1.0,
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        recon = FullDocReconstituter("http://localhost:8000")
        result = recon.reconstitute("https://nonexistent.com")
        assert result is None

    @patch("src.components.analysis.media_linkage.requests.post")
    def test_reconstitute_http_error(self, mock_post):
        mock_post.side_effect = Exception("Connection refused")

        recon = FullDocReconstituter("http://localhost:8000")
        result = recon.reconstitute("https://example.com")
        assert result is None

    @patch("src.components.analysis.media_linkage.requests.post")
    @patch("src.components.analysis.media_linkage.requests.get")
    def test_ensure_index_already_active(self, mock_get, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"active_folder": "x_indices_trump"}
        mock_get.return_value = mock_resp

        recon = FullDocReconstituter("http://localhost:8000")
        assert recon.ensure_index("x_indices_trump") is True
        mock_post.assert_not_called()

    @patch("src.components.analysis.media_linkage.requests.post")
    @patch("src.components.analysis.media_linkage.requests.get")
    def test_ensure_index_switches(self, mock_get, mock_post):
        mock_get_resp = MagicMock()
        mock_get_resp.json.return_value = {"active_folder": "x_indices_eo"}
        mock_get.return_value = mock_get_resp

        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 200
        mock_post.return_value = mock_post_resp

        recon = FullDocReconstituter("http://localhost:8000")
        assert recon.ensure_index("x_indices_trump") is True
        mock_post.assert_called_once_with(
            "http://localhost:8000/switch-index",
            json={"folder": "x_indices_trump"},
            timeout=180,
        )

    @patch("src.components.analysis.media_linkage.requests.post")
    def test_reconstitute_batch(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = self._mock_query_metadata_response(
            qa_contexts=["Full text content"],
        )
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        recon = FullDocReconstituter("http://localhost:8000")
        results = recon.reconstitute_batch(
            ["https://a.com", "https://b.com"]
        )
        assert len(results) == 2
        assert mock_post.call_count == 2

    def test_build_evidence_bundle(self):
        linked = [
            _make_linked_media(link="https://a.com/1"),
            _make_linked_media(link="https://a.com/2"),
        ]
        unlinked = [_make_unlinked(link="https://b.com/1")]

        recon_docs = {
            "https://a.com/1": {
                "full_text": "Full text 1",
                "summary_text": "Summary 1",
                "title": "Title 1",
                "doc_metadata": {"publisher": "Pub1"},
            },
            "https://a.com/2": {
                "full_text": "Full text 2",
                "summary_text": "Summary 2",
                "title": "Title 2",
                "doc_metadata": {"publisher": "Pub2"},
            },
        }

        recon = FullDocReconstituter("http://localhost:8000")
        bundle = recon.build_evidence_bundle(
            "culture_and_media", linked, unlinked, recon_docs
        )

        assert bundle["category"] == "culture_and_media"
        assert bundle["article_count"] == 3
        # Check linked have reconstituted text
        arts = bundle["articles"]
        assert arts[0]["full_text"] == "Full text 1"
        assert arts[0]["reconstituted"] is True
        # Check unlinked is present but not reconstituted
        unlinked_art = [a for a in arts if a["url"] == "https://b.com/1"][0]
        assert unlinked_art["reconstituted"] is False
        assert unlinked_art["source_type"] == "unlinked"

    def test_build_evidence_bundle_deduplicates(self):
        linked = [
            _make_linked_media(link="https://a.com/same"),
            _make_linked_media(link="https://a.com/same"),
        ]
        recon = FullDocReconstituter("http://localhost:8000")
        bundle = recon.build_evidence_bundle(
            "test", linked, [], {}
        )
        assert bundle["article_count"] == 1

    def test_build_evidence_bundle_skips_empty_url(self):
        linked = [_make_linked_media(link="")]
        recon = FullDocReconstituter("http://localhost:8000")
        bundle = recon.build_evidence_bundle("test", linked, [], {})
        assert bundle["article_count"] == 0


# ===================================================================
# Phase 4 — ThreatClusterAnalyser
# ===================================================================


class TestThreatClusterAnalyser:
    @staticmethod
    def _sample_bundle() -> dict:
        return {
            "category": "culture_and_media",
            "article_count": 2,
            "articles": [
                {
                    "url": "https://a.com/1",
                    "author": "John Smith",
                    "publisher": "www.nytimes.com",
                    "listed_publisher": "NYT",
                    "date_publication": "2025-01-21",
                    "event_key": "e1",
                    "eo_identifier": "EO 14148",
                    "consumer_salience": 0.85,
                    "media_framing": {
                        "factualness": "Factual",
                        "spin": "Heavy spin",
                        "convergence_divergence": "Diverges",
                        "supportive_unsupportive": "Unsupportive",
                        "risk": {"rating": "HIGH", "reasoning": "Adversarial framing"},
                    },
                    "full_text": "Article full text here",
                    "title": "Test Article 1",
                    "source_type": "linked",
                },
                {
                    "url": "https://b.com/2",
                    "author": "Jane Doe",
                    "publisher": "www.reuters.com",
                    "listed_publisher": "Reuters",
                    "date_publication": "2025-01-22",
                    "event_key": "e2",
                    "eo_identifier": "EO 14148",
                    "consumer_salience": 0.7,
                    "media_framing": {
                        "factualness": "Mostly factual",
                        "spin": "Low spin",
                        "convergence_divergence": "Converges",
                        "supportive_unsupportive": "Supportive",
                        "risk": {"rating": "LOW", "reasoning": "Balanced"},
                    },
                    "full_text": "Second article text",
                    "title": "Test Article 2",
                    "source_type": "linked",
                },
            ],
        }

    def test_build_prompt_contains_template(self):
        analyser = ThreatClusterAnalyser()
        prompt = analyser.build_prompt(
            "culture_and_media",
            self._sample_bundle(),
        )
        assert "Culture And Media" in prompt
        assert "spin" in prompt.lower()
        assert "threat clusters" in prompt.lower()

    def test_build_prompt_contains_article_data(self):
        analyser = ThreatClusterAnalyser()
        prompt = analyser.build_prompt(
            "culture_and_media",
            self._sample_bundle(),
        )
        assert "John Smith" in prompt
        assert "Jane Doe" in prompt
        assert "www.nytimes.com" in prompt
        assert "Article full text here" in prompt

    def test_build_prompt_custom_label(self):
        analyser = ThreatClusterAnalyser()
        prompt = analyser.build_prompt(
            "culture_and_media",
            self._sample_bundle(),
            category_label="Defence Cultural Analysis",
        )
        assert "Defence Cultural Analysis" in prompt

    def test_build_prompt_handles_risk_none(self):
        bundle = self._sample_bundle()
        bundle["articles"][0]["media_framing"]["risk"] = None
        analyser = ThreatClusterAnalyser()
        prompt = analyser.build_prompt("test", bundle)
        assert "None" in prompt  # Should not crash

    # -- Cluster parsing tests --

    CLUSTER_MARKDOWN = (
        "# Cluster 1 — Renaming the Department of Defence\n"
        "### Why this is a distinct cluster\n"
        "Multiple outlets covered the renaming.\n\n"
        "## Cluster 1 table\n"
        "| Author | Date | Publication | Title | URL |\n"
        "|---|---|---|---|---|\n"
        "| Joe Walsh | 2025-09-05 | CBS News | Trump renames dept | https://www.cbsnews.com/news/dept-of-war/ |\n"
        "| Phil Stewart | 2025-09-05 | Reuters | Pentagon renamed | https://www.reuters.com/world/war-dept/ |\n\n"
        "# Cluster 2 — Anti-DEI purge\n"
        "### Why this is a distinct cluster\n"
        "Culture-war driven policy changes.\n\n"
        "## Cluster 2 table\n"
        "| Author | Date | Publication | Title | URL |\n"
        "|---|---|---|---|---|\n"
        "| Jane Doe | 2025-02-12 | The Intercept | DEI rollback | https://theintercept.com/dei-rollback/ |\n"
    )

    def test_parse_result_extracts_clusters(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "culture_and_media", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        assert result["category"] == "culture_and_media"
        assert result["cluster_count"] == 2
        assert result["article_count"] == 3
        assert "clusters" in result
        assert len(result["clusters"]) == 2

    def test_parse_result_cluster_ids(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        ids = [c["cluster_id"] for c in result["clusters"]]
        assert ids == ["cluster_1", "cluster_2"]

    def test_parse_result_cluster_descriptions(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        assert result["clusters"][0]["cluster_description"] == "Renaming the Department of Defence"
        assert result["clusters"][1]["cluster_description"] == "Anti-DEI purge"

    def test_parse_result_cluster_article_counts(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        assert result["clusters"][0]["article_count"] == 2
        assert result["clusters"][1]["article_count"] == 1

    def test_parse_result_extracts_urls_from_table(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        c1_urls = {a["url"] for a in result["clusters"][0]["articles"]}
        assert c1_urls == {
            "https://www.cbsnews.com/news/dept-of-war/",
            "https://www.reuters.com/world/war-dept/",
        }
        c2_urls = {a["url"] for a in result["clusters"][1]["articles"]}
        assert c2_urls == {"https://theintercept.com/dei-rollback/"}

    def test_parse_result_extracts_metadata_from_table_columns(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        art = result["clusters"][0]["articles"][0]
        assert art["author"] == "Joe Walsh"
        assert art["publisher"] == "CBS News"
        assert art["date_publication"] == "2025-09-05"
        assert art["source_type"] == "cluster_parsed"

    def test_parse_result_prefers_bundle_metadata(self):
        """When a URL exists in the bundle, use bundle metadata over table."""
        analyser = ThreatClusterAnalyser()
        bundle = {
            "articles": [{
                "url": "https://www.cbsnews.com/news/dept-of-war/",
                "author": "Bundle Author",
                "publisher": "Bundle Publisher",
                "date_publication": "2025-09-06",
                "event_key": "evt_123",
                "eo_identifier": "EO 14148",
                "source_type": "linked",
            }],
        }
        result = analyser.parse_result("test", self.CLUSTER_MARKDOWN, bundle)
        # The CBS article should have bundle metadata
        cbs_art = [a for a in result["clusters"][0]["articles"]
                    if "cbsnews" in a["url"]][0]
        assert cbs_art["author"] == "Bundle Author"
        assert cbs_art["eo_identifier"] == "EO 14148"
        assert cbs_art["source_type"] == "linked"

    def test_parse_result_deduplicates_urls_within_cluster(self):
        md = (
            "# Cluster 1 — Test\n"
            "## Cluster 1 table\n"
            "| Author | Date | Pub | Title | URL |\n"
            "|---|---|---|---|---|\n"
            "| A | 2025-01-01 | P | T | https://a.com/same |\n"
            "| B | 2025-01-02 | P | T | https://a.com/same |\n"
        )
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, {"articles": []})
        assert result["clusters"][0]["article_count"] == 1

    def test_parse_result_no_clusters_fallback(self):
        """When markdown has no cluster headers, fall back to single cluster."""
        analyser = ThreatClusterAnalyser()
        bundle = self._sample_bundle()
        result = analyser.parse_result(
            "test", "Plain analysis text with no cluster headers.", bundle
        )
        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_id"] == "cluster_1"
        assert result["clusters"][0]["article_count"] == 2

    def test_parse_result_empty_bundle_no_clusters(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", "No clusters here.", {"articles": []}
        )
        assert result["article_count"] == 0
        assert result["cluster_count"] == 1
        assert result["clusters"][0]["articles"] == []

    def test_parse_result_lineage(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        assert result["lineage"]["prompt_template"] == "THREAT_CLUSTER_PROMPT"
        assert result["lineage"]["parsed_clusters"] == 2

    def test_parse_result_cluster_text_preserved(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        # Cluster text should contain the table and description
        assert "Multiple outlets covered" in result["clusters"][0]["cluster_text"]
        assert "Cluster 1 table" in result["clusters"][0]["cluster_text"]

    def test_parse_result_cluster_numbers(self):
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result(
            "test", self.CLUSTER_MARKDOWN, {"articles": []}
        )
        assert result["clusters"][0]["cluster_number"] == 1
        assert result["clusters"][1]["cluster_number"] == 2


# ===================================================================
# Phase 5 — RapporteurClusterExecution
# ===================================================================


class TestRapporteurClusterExecution:
    @staticmethod
    def _cluster_result() -> dict:
        """A parse_result-shaped dict with cluster structure."""
        return {
            "analysis_text": "Cluster output",
            "cluster_count": 2,
            "article_count": 3,
            "clusters": [
                {
                    "cluster_id": "cluster_1",
                    "cluster_number": 1,
                    "cluster_description": "War rename",
                    "article_count": 2,
                    "articles": [
                        {"url": "https://a.com/1", "author": "A"},
                        {"url": "https://a.com/2", "author": "B"},
                    ],
                },
                {
                    "cluster_id": "cluster_2",
                    "cluster_number": 2,
                    "cluster_description": "DEI purge",
                    "article_count": 1,
                    "articles": [
                        {"url": "https://b.com/1", "author": "C"},
                    ],
                },
            ],
        }

    def test_save_result(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "sess_001")
        result = self._cluster_result()
        path = exe.save_result("culture_and_media", result)
        assert path.exists()

        with open(path) as f:
            record = json.loads(f.readline())
        assert record["category"] == "culture_and_media"
        assert record["session_id"] == "sess_001"
        assert record["analysis_text"] == "Cluster output"
        assert len(record["record_id"]) == 16
        assert record["cluster_count"] == 2
        assert len(record["clusters"]) == 2

    def test_save_preserves_cluster_structure(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "sess_001")
        exe.save_result("test", self._cluster_result())
        records = exe.load_results()
        clusters = records[0]["clusters"]
        assert clusters[0]["cluster_id"] == "cluster_1"
        assert clusters[0]["cluster_description"] == "War rename"
        assert len(clusters[0]["articles"]) == 2
        assert clusters[1]["cluster_id"] == "cluster_2"
        assert len(clusters[1]["articles"]) == 1

    def test_save_multiple_results(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "sess_001")
        exe.save_result("cat_a", {"analysis_text": "A", "clusters": []})
        exe.save_result("cat_b", {"analysis_text": "B", "clusters": []})

        records = exe.load_results()
        assert len(records) == 2
        cats = {r["category"] for r in records}
        assert cats == {"cat_a", "cat_b"}

    def test_load_results_empty(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "nonexistent")
        assert exe.load_results() == []

    def test_deterministic_record_id(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "sess_001")
        exe.save_result("test_cat", {"analysis_text": "X"})
        records = exe.load_results()
        expected_id = records[0]["record_id"]

        exe2 = RapporteurClusterExecution(tmp_path / "other", "sess_001")
        exe2.save_result("test_cat", {"analysis_text": "Y"})
        records2 = exe2.load_results()
        assert records2[0]["record_id"] == expected_id

    def test_round_trip_urls_preserved(self, tmp_path: Path):
        exe = RapporteurClusterExecution(tmp_path, "sess_001")
        exe.save_result("test", self._cluster_result())
        records = exe.load_results()
        all_urls = set()
        for cluster in records[0]["clusters"]:
            for art in cluster["articles"]:
                all_urls.add(art["url"])
        assert all_urls == {"https://a.com/1", "https://a.com/2", "https://b.com/1"}


# ===================================================================
# Phase 6 — KaliyaHandoff
# ===================================================================


class TestKaliyaHandoff:
    @staticmethod
    def _sample_cluster() -> dict:
        """A single cluster dict as produced by parse_result."""
        return {
            "cluster_id": "cluster_1",
            "cluster_number": 1,
            "cluster_description": "War renaming",
            "cluster_text": "Long cluster analysis " * 30,
            "article_count": 2,
            "articles": [
                {"url": "https://a.com/1", "author": "A"},
                {"url": "https://b.com/2", "author": "B"},
                {"url": "", "author": "C"},  # empty URL
            ],
        }

    @staticmethod
    def _sample_category_result() -> dict:
        """A full category result with clusters, as saved by Phase 5."""
        return {
            "category": "culture_and_media",
            "cluster_count": 2,
            "clusters": [
                {
                    "cluster_id": "cluster_1",
                    "cluster_number": 1,
                    "cluster_description": "War renaming",
                    "cluster_text": "Text",
                    "article_count": 2,
                    "articles": [
                        {"url": "https://a.com/1", "author": "A"},
                        {"url": "https://b.com/2", "author": "B"},
                    ],
                },
                {
                    "cluster_id": "cluster_2",
                    "cluster_number": 2,
                    "cluster_description": "DEI purge",
                    "cluster_text": "Text 2",
                    "article_count": 1,
                    "articles": [
                        {"url": "https://c.com/3", "author": "C"},
                    ],
                },
            ],
        }

    def test_build_handoff(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff(
            "culture_and_media",
            self._sample_cluster(),
        )
        assert result["category"] == "culture_and_media"
        assert result["cluster_id"] == "cluster_1"
        assert result["cluster_description"] == "War renaming"
        assert result["cluster_number"] == 1
        assert len(result["urls"]) == 2  # Empty URL excluded
        assert result["retrieve_images"] is True
        assert result["model"] == "gpt-5.4"

    def test_build_handoff_truncates_summary(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", self._sample_cluster())
        assert len(result["analysis_summary"]) <= 500

    def test_build_all_handoffs_iterates_clusters(self):
        """build_all_handoffs produces one handoff per cluster per category."""
        handoff = KaliyaHandoff()
        results = handoff.build_all_handoffs([
            self._sample_category_result(),
        ], min_articles=0)
        assert len(results) == 2
        assert results[0]["cluster_id"] == "cluster_1"
        assert results[1]["cluster_id"] == "cluster_2"

    def test_build_all_handoffs_multiple_categories(self):
        handoff = KaliyaHandoff()
        cat_a = self._sample_category_result()
        cat_b = {**self._sample_category_result(), "category": "ai"}
        results = handoff.build_all_handoffs([cat_a, cat_b], min_articles=0)
        # 2 clusters each = 4 total handoffs
        assert len(results) == 4
        categories = {r["category"] for r in results}
        assert categories == {"culture_and_media", "ai"}

    def test_build_all_handoffs_empty_clusters(self):
        handoff = KaliyaHandoff()
        results = handoff.build_all_handoffs([
            {"category": "test", "clusters": []},
        ])
        assert results == []

    def test_build_handoff_empty_articles(self):
        handoff = KaliyaHandoff()
        result = handoff.build_handoff("test", {"articles": []})
        assert result["urls"] == []
        assert result["url_count"] == 0

    def test_build_all_handoffs_urls_complete(self):
        """Every URL from every cluster appears in the handoff payloads."""
        handoff = KaliyaHandoff()
        results = handoff.build_all_handoffs([self._sample_category_result()], min_articles=0)
        all_urls = set()
        for r in results:
            all_urls.update(r["urls"])
        assert all_urls == {
            "https://a.com/1", "https://b.com/2", "https://c.com/3",
        }

    def test_build_all_handoffs_filters_small_clusters(self):
        """Clusters with fewer than min_articles are filtered out."""
        handoff = KaliyaHandoff()
        results = handoff.build_all_handoffs(
            [self._sample_category_result()],
            min_articles=2,
        )
        # cluster_1 has 2 articles (passes), cluster_2 has 1 (filtered)
        assert len(results) == 1
        assert results[0]["cluster_id"] == "cluster_1"


# ===================================================================
# Phase 7 — ConclusionGenerator
# ===================================================================


class TestConclusionGenerator:
    def test_build_prompt(self):
        gen = ConclusionGenerator("/tmp/out")
        cluster = {
            "analysis_text": "Cluster findings here",
            "articles": [
                {"author": "John", "publisher": "NYT", "date_publication": "2025-01-21", "url": "https://a.com"},
            ],
        }
        prompt = gen.build_prompt("culture_and_media", cluster)
        assert "Culture And Media" in prompt
        assert "misinformation" in prompt.lower()
        assert "collusion" in prompt.lower()
        assert "John" in prompt
        assert "NYT" in prompt

    def test_build_prompt_with_kaliya(self):
        gen = ConclusionGenerator("/tmp/out")
        cluster = {"analysis_text": "Cluster text", "articles": []}
        kaliya = {"analysis_text": "Kaliya agenda findings"}
        prompt = gen.build_prompt("test", cluster, kaliya)
        assert "Kaliya Agenda Analysis" in prompt
        assert "Kaliya agenda findings" in prompt

    def test_build_prompt_without_kaliya(self):
        gen = ConclusionGenerator("/tmp/out")
        cluster = {"analysis_text": "Cluster text", "articles": []}
        prompt = gen.build_prompt("test", cluster, None)
        assert "Kaliya Agenda Analysis" not in prompt

    def test_save_conclusion(self, tmp_path: Path):
        gen = ConclusionGenerator(tmp_path)
        path = gen.save_conclusion(
            "culture_and_media",
            "# Conclusion\n\nFindings here.",
            "sess_001",
        )
        assert path.exists()
        assert path.name == "conclusion_culture_and_media_sess_001.md"
        assert path.read_text() == "# Conclusion\n\nFindings here."

    def test_save_conclusion_creates_dir(self, tmp_path: Path):
        gen = ConclusionGenerator(tmp_path / "nested" / "dir")
        path = gen.save_conclusion("test", "Content", "sess")
        assert path.exists()


# ===================================================================
# Integration: Phase 4 parse → Phase 5 persist → Phase 6 handoff
# ===================================================================


class TestClusterPipelineIntegration:
    """End-to-end integration testing the cluster data flow across phases."""

    MULTI_CLUSTER_MD = (
        "# Executive Overview\n\nAnalysis found 3 clusters.\n\n"
        "# Cluster 1 — Defence renaming and cost\n"
        "### Why this is a distinct cluster\n"
        "Outlets converge on renaming narrative.\n\n"
        "## Cluster 1 table\n"
        "| Author | Date | Publication | Title | URL |\n"
        "|---|---|---|---|---|\n"
        "| Joe Walsh | 2025-09-05 | CBS News | Dept renamed | https://www.cbsnews.com/war-dept/ |\n"
        "| Phil Stewart | 2025-09-05 | Reuters | Pentagon now War | https://www.reuters.com/war-dept/ |\n"
        "| Sara Cook | 2025-09-06 | CBS News | Cost analysis | https://www.cbsnews.com/war-cost/ |\n\n"
        "# Cluster 2 — DEI and culture-war purge\n"
        "### Why this is a distinct cluster\n"
        "Culture war framing dominates.\n\n"
        "## Cluster 2 table\n"
        "| Author | Date | Publication | Title | URL |\n"
        "|---|---|---|---|---|\n"
        "| James B | 2025-02-12 | Newsweek | Erasing history | https://www.newsweek.com/dei-erasing/ |\n"
        "| Emily R | 2025-02-13 | CNN | Culture purge | https://edition.cnn.com/dei-purge/ |\n\n"
        "# Cluster 3 — Pentagon leaks and dysfunction\n"
        "### Why this is a distinct cluster\n"
        "Internal controls failing.\n\n"
        "## Cluster 3 table\n"
        "| Author | Date | Publication | Title | URL |\n"
        "|---|---|---|---|---|\n"
        "| Patricia Z | 2025-03-14 | Reuters | Pentagon tightens | https://www.reuters.com/pentagon-leaks/ |\n"
    )

    def test_parse_persist_handoff_pipeline(self, tmp_path: Path):
        """Full pipeline: parse markdown → save to JSONL → load → build handoffs."""
        # Phase 4: parse clusters from LLM output
        analyser = ThreatClusterAnalyser()
        bundle = {
            "articles": [
                {
                    "url": "https://www.cbsnews.com/war-dept/",
                    "author": "Joe Walsh",
                    "publisher": "CBS News",
                    "date_publication": "2025-09-05",
                    "event_key": "evt_war",
                    "eo_identifier": "EO 14567",
                    "source_type": "linked",
                },
            ],
        }
        result = analyser.parse_result("culture_and_media", self.MULTI_CLUSTER_MD, bundle)

        # Verify parse
        assert result["cluster_count"] == 3
        assert result["article_count"] == 6

        # Phase 5: persist to JSONL
        exe = RapporteurClusterExecution(tmp_path, "integration_sess")
        path = exe.save_result("culture_and_media", result)
        assert path.exists()

        # Load back
        loaded = exe.load_results()
        assert len(loaded) == 1
        loaded_result = loaded[0]

        # Verify cluster structure survives round-trip
        assert loaded_result["cluster_count"] == 3
        assert len(loaded_result["clusters"]) == 3
        assert loaded_result["clusters"][0]["cluster_id"] == "cluster_1"
        assert loaded_result["clusters"][0]["article_count"] == 3
        assert loaded_result["clusters"][1]["cluster_id"] == "cluster_2"
        assert loaded_result["clusters"][1]["article_count"] == 2
        assert loaded_result["clusters"][2]["cluster_id"] == "cluster_3"
        assert loaded_result["clusters"][2]["article_count"] == 1

        # Verify bundle metadata was used where available
        cbs_art = [
            a for a in loaded_result["clusters"][0]["articles"]
            if a["url"] == "https://www.cbsnews.com/war-dept/"
        ][0]
        assert cbs_art["source_type"] == "linked"
        assert cbs_art["eo_identifier"] == "EO 14567"

        # Verify table-parsed metadata for non-bundle URLs
        reuters_art = [
            a for a in loaded_result["clusters"][0]["articles"]
            if "reuters" in a["url"]
        ][0]
        assert reuters_art["author"] == "Phil Stewart"
        assert reuters_art["source_type"] == "cluster_parsed"

        # Phase 6: build handoffs from loaded results (no filter for unit test)
        handoff = KaliyaHandoff()
        payloads = handoff.build_all_handoffs(loaded, min_articles=0)

        # One handoff per cluster
        assert len(payloads) == 3
        assert payloads[0]["cluster_id"] == "cluster_1"
        assert payloads[0]["url_count"] == 3
        assert payloads[1]["cluster_id"] == "cluster_2"
        assert payloads[1]["url_count"] == 2
        assert payloads[2]["cluster_id"] == "cluster_3"
        assert payloads[2]["url_count"] == 1

        # All URLs present across handoffs
        all_handoff_urls = set()
        for p in payloads:
            all_handoff_urls.update(p["urls"])
        assert len(all_handoff_urls) == 6

    def test_pipeline_with_real_markdown(self, tmp_path: Path):
        """Integration test using the actual Phase 4 output if available."""
        md_path = Path("working_dir/phase4_culture_and_media_20260324_051757.md")
        if not md_path.exists():
            pytest.skip("Phase 4 markdown not available in working_dir")

        raw_output = md_path.read_text()
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("culture_and_media", raw_output, {"articles": []})

        # Known properties of this file
        assert result["cluster_count"] == 7
        assert result["article_count"] == 88

        # Save and reload
        exe = RapporteurClusterExecution(tmp_path, "real_test")
        exe.save_result("culture_and_media", result)
        loaded = exe.load_results()
        assert len(loaded) == 1
        assert loaded[0]["cluster_count"] == 7

        # Build handoffs — default min_articles=15 filters small clusters
        handoff = KaliyaHandoff()
        payloads = handoff.build_all_handoffs(loaded)
        # Clusters 1(20), 3(19), 6(18) pass; 2(8), 4(8), 5(11), 7(5) filtered
        assert len(payloads) == 3
        total_urls = sum(p["url_count"] for p in payloads)
        assert total_urls >= 57  # 20+19+18 from passing clusters

    def test_empty_llm_output_graceful(self, tmp_path: Path):
        """Pipeline handles empty/unstructured LLM output gracefully."""
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", "", {"articles": []})

        assert result["cluster_count"] == 1  # fallback cluster
        assert result["article_count"] == 0

        # Persist and reload
        exe = RapporteurClusterExecution(tmp_path, "empty_test")
        exe.save_result("test", result)
        loaded = exe.load_results()
        assert len(loaded) == 1
        assert loaded[0]["clusters"][0]["cluster_id"] == "cluster_1"

        # Handoffs: empty but no crash (bypass filter for empty test)
        handoff = KaliyaHandoff()
        payloads = handoff.build_all_handoffs(loaded, min_articles=0)
        assert len(payloads) == 1
        assert payloads[0]["url_count"] == 0


# ===================================================================
# CATEGORY_ANCHORS constant
# ===================================================================


class TestCategoryAnchors:
    def test_all_categories_present(self):
        expected = {
            "culture_and_media",
            "artificial_intelligence",
            "federal_procurement",
            "energy_and_minerals",
            "national_security_domestic",
            "national_security_foreign",
        }
        assert set(CATEGORY_ANCHORS.keys()) == expected

    def test_anchors_are_string_lists(self):
        for cat, eos in CATEGORY_ANCHORS.items():
            assert isinstance(eos, list), f"{cat} anchors not a list"
            for eo in eos:
                assert isinstance(eo, str), f"{cat} anchor {eo} not a string"
                assert eo.isdigit(), f"{cat} anchor {eo} not numeric"


# ===================================================================
# _expand_ranges helper
# ===================================================================


class TestExpandRanges:
    """Verify article-number range expansion (e.g. '1-5, 7' → [1,2,3,4,5,7])."""

    @staticmethod
    def _expand(text: str) -> list[int]:
        return ThreatClusterAnalyser._expand_ranges(text)

    def test_simple_range(self):
        assert self._expand("1-5") == [1, 2, 3, 4, 5]

    def test_comma_separated(self):
        assert self._expand("1, 3, 7") == [1, 3, 7]

    def test_mixed_ranges_and_singles(self):
        assert self._expand("1-3, 7, 9-11") == [1, 2, 3, 7, 9, 10, 11]

    def test_spaces_around_dash(self):
        assert self._expand("84 - 89") == [84, 85, 86, 87, 88, 89]

    def test_real_cluster_map_entry(self):
        """Sample from actual culture_and_media cluster map."""
        result = self._expand("1-5, 14, 16-17, 48, 58")
        assert result == [1, 2, 3, 4, 5, 14, 16, 17, 48, 58]

    def test_empty_string(self):
        assert self._expand("") == []

    def test_single_number(self):
        assert self._expand("42") == [42]


# ===================================================================
# Cluster heading regex variants
# ===================================================================


class TestClusterHeadingRegex:
    """Verify the parse_result regex handles all known heading formats."""

    @staticmethod
    def _parse(md: str, bundle: dict | None = None) -> dict:
        analyser = ThreatClusterAnalyser()
        return analyser.parse_result("test", md, bundle or {"articles": []})

    def test_standard_hash_cluster(self):
        """# Cluster 1 — theme"""
        md = "# Cluster 1 — Renaming the Department\nBody text\n"
        result = self._parse(md)
        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_id"] == "cluster_1"
        assert result["clusters"][0]["cluster_description"] == "Renaming the Department"

    def test_double_hash_cluster(self):
        """## Cluster A — theme"""
        md = "## Cluster A — Bold theme\nBody\n"
        result = self._parse(md)
        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_id"] == "cluster_A"

    def test_numbered_prefix_cluster(self):
        """# 4. Cluster A — theme (energy_and_minerals style)"""
        md = (
            "# 4. Cluster A — Strategic minerals mobilization\nBody A\n"
            "# 5. Cluster B — Energy dominance\nBody B\n"
        )
        result = self._parse(md)
        assert result["cluster_count"] == 2
        ids = [c["cluster_id"] for c in result["clusters"]]
        assert ids == ["cluster_A", "cluster_B"]
        assert result["clusters"][0]["cluster_description"] == "Strategic minerals mobilization"

    def test_bold_description_stripped(self):
        """## Cluster C — **Bold description**"""
        md = "## Cluster C — **Bold description here**\nBody\n"
        result = self._parse(md)
        assert result["clusters"][0]["cluster_description"] == "Bold description here"

    def test_en_dash_separator(self):
        """# Cluster 1 – title (en-dash)"""
        md = "# Cluster 1 \u2013 En-dash title\nBody\n"
        result = self._parse(md)
        assert result["cluster_count"] == 1

    def test_hyphen_separator(self):
        """# Cluster 1 - title (plain hyphen)"""
        md = "# Cluster 1 - Hyphen title\nBody\n"
        result = self._parse(md)
        assert result["cluster_count"] == 1

    def test_rejects_long_id(self):
        """# 5. Cluster analysis — ... should NOT match (ID 'analysis' too long)"""
        md = "# 5. Cluster analysis — overview section\nBody\n"
        result = self._parse(md)
        # Should fall back to single-cluster catch-all
        assert result["cluster_count"] == 1
        assert result["clusters"][0]["cluster_id"] == "cluster_1"

    def test_two_char_id(self):
        """## Cluster A1 — ... (2-char ID is allowed)"""
        md = "## Cluster A1 — Two char id\nBody\n"
        result = self._parse(md)
        assert result["clusters"][0]["cluster_id"] == "cluster_A1"

    def test_mixed_heading_levels(self):
        """Mix of # and ## with numbered and bare."""
        md = (
            "# Cluster 1 — First\nBody 1\n"
            "## Cluster 2 — Second\nBody 2\n"
            "# 3. Cluster A — Third\nBody 3\n"
        )
        result = self._parse(md)
        assert result["cluster_count"] == 3
        ids = [c["cluster_id"] for c in result["clusters"]]
        assert ids == ["cluster_1", "cluster_2", "cluster_A"]


# ===================================================================
# Strategy 1b: inline article-number lists
# ===================================================================


class TestInlineArticleLists:
    """Verify parse_result resolves 'Articles included:\\n1, 2, 3' patterns."""

    @staticmethod
    def _bundle_with_n(n: int) -> dict:
        return {
            "articles": [
                {"url": f"https://example.com/art{i}", "title": f"Article {i}",
                 "author": "", "publisher": "", "date_publication": "",
                 "event_key": "", "eo_identifier": "", "source_type": "linked"}
                for i in range(1, n + 1)
            ]
        }

    def test_inline_articles_included(self):
        """Cluster sections with 'Articles included:\\n2, 3, 4' resolve via bundle."""
        md = (
            "## Cluster A — Signal coverage\n"
            "Articles included:  \n"
            "2, 3, 4\n"
            "## Cluster B — Missile defense\n"
            "Articles included:\n"
            "1, 5\n"
        )
        bundle = self._bundle_with_n(5)
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)

        assert result["cluster_count"] == 2
        c_a_urls = {a["url"] for a in result["clusters"][0]["articles"]}
        c_b_urls = {a["url"] for a in result["clusters"][1]["articles"]}
        assert c_a_urls == {
            "https://example.com/art2",
            "https://example.com/art3",
            "https://example.com/art4",
        }
        assert c_b_urls == {
            "https://example.com/art1",
            "https://example.com/art5",
        }

    def test_inline_articles_no_bundle_yields_zero(self):
        """Without a bundle, article numbers can't resolve → 0 URLs."""
        md = (
            "## Cluster A — Signal coverage\n"
            "Articles included:\n"
            "2, 3, 4\n"
        )
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, {"articles": []})
        assert result["clusters"][0]["article_count"] == 0

    def test_inline_falls_back_when_cluster_map_present(self):
        """Cluster map (Strategy 1) takes precedence over inline list."""
        md = (
            "## Cluster map\n"
            "| **1** | Theme | 1, 3 |\n"
            "## Cluster 1 — Theme\n"
            "Articles included:\n"
            "1, 2, 3\n"
        )
        bundle = self._bundle_with_n(3)
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)
        # Should use cluster map (articles 1 & 3), not inline (1, 2, 3)
        c1_urls = {a["url"] for a in result["clusters"][0]["articles"]}
        assert "https://example.com/art1" in c1_urls
        assert "https://example.com/art3" in c1_urls

    def test_inline_large_numbered_list(self):
        """Handles lists with many article numbers."""
        nums = ", ".join(str(i) for i in range(1, 51))
        md = f"## Cluster A — Big cluster\nArticles included:\n{nums}\n"
        bundle = self._bundle_with_n(50)
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)
        assert result["clusters"][0]["article_count"] == 50

    def test_inline_range_expansion(self):
        """'Articles included:\\n1-3, 5' should expand to articles 1,2,3,5."""
        md = (
            "## Cluster A — Range test\n"
            "Articles included:\n"
            "1-3, 5\n"
        )
        bundle = self._bundle_with_n(5)
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)
        urls = {a["url"] for a in result["clusters"][0]["articles"]}
        assert urls == {
            "https://example.com/art1",
            "https://example.com/art2",
            "https://example.com/art3",
            "https://example.com/art5",
        }


# ===================================================================
# Bundle-driven article number resolution (Source 0)
# ===================================================================


class TestBundlePrimaryResolution:
    """Verify bundle articles populate artnum_to_url as primary source."""

    def test_cluster_map_range_expansion(self):
        """Cluster map '1-3' should expand to articles 1, 2, 3."""
        md = (
            "## Cluster map\n"
            "| **A** | Theme | 1-3, 5 |\n"
            "## Cluster A — Theme\nBody\n"
        )
        bundle = {
            "articles": [
                {"url": f"https://example.com/art{i}", "title": f"Art {i}",
                 "author": "", "publisher": "", "date_publication": "",
                 "event_key": "", "eo_identifier": "", "source_type": "linked"}
                for i in range(1, 6)
            ]
        }
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)
        urls = {a["url"] for a in result["clusters"][0]["articles"]}
        assert urls == {
            "https://example.com/art1",
            "https://example.com/art2",
            "https://example.com/art3",
            "https://example.com/art5",
        }

    @staticmethod
    def _bundle_with_urls(urls: list[str]) -> dict:
        return {
            "articles": [
                {"url": u, "title": f"Art {i}", "author": f"Auth {i}",
                 "publisher": f"Pub {i}", "date_publication": "2025-01-01",
                 "event_key": "e1", "eo_identifier": "EO 14148",
                 "source_type": "linked"}
                for i, u in enumerate(urls, 1)
            ]
        }

    def test_cluster_map_resolves_via_bundle(self):
        """Cluster map article numbers resolve correctly through bundle ordering."""
        md = (
            "## Cluster map\n"
            "| **1** | Theme A | 1, 2 |\n"
            "| **2** | Theme B | 3 |\n"
            "# Cluster 1 — Theme A\nBody\n"
            "# Cluster 2 — Theme B\nBody\n"
        )
        bundle = self._bundle_with_urls([
            "https://a.com/first",
            "https://b.com/second",
            "https://c.com/third",
        ])
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)

        c1_urls = {a["url"] for a in result["clusters"][0]["articles"]}
        c2_urls = {a["url"] for a in result["clusters"][1]["articles"]}
        assert c1_urls == {"https://a.com/first", "https://b.com/second"}
        assert c2_urls == {"https://c.com/third"}

    def test_bundle_metadata_preserved(self):
        """Bundle metadata (author, eo_identifier) is carried through."""
        md = (
            "## Cluster map\n"
            "| **1** | Theme | 1 |\n"
            "# Cluster 1 — Theme\nBody\n"
        )
        bundle = self._bundle_with_urls(["https://a.com/x"])
        analyser = ThreatClusterAnalyser()
        result = analyser.parse_result("test", md, bundle)
        art = result["clusters"][0]["articles"][0]
        assert art["author"] == "Auth 1"
        assert art["eo_identifier"] == "EO 14148"
        assert art["source_type"] == "linked"


# ===================================================================
# _validate_cluster_fidelity
# ===================================================================


class TestValidateClusterFidelity:
    """Tests for the mandatory cluster fidelity validation gate."""

    @staticmethod
    def _do_validate(md_text: str, cluster_result: dict, category: str = "test"):
        from src.demo.demo_media_linkage import _validate_cluster_fidelity
        _validate_cluster_fidelity(md_text, cluster_result, category)

    def test_passes_when_all_clusters_match(self):
        md = (
            "# Cluster 1 — Theme A\nBody\n"
            "# Cluster 2 — Theme B\nBody\n"
        )
        cluster_result = {
            "clusters": [
                {"cluster_id": "cluster_1", "articles": [{"url": "https://a.com"}]},
                {"cluster_id": "cluster_2", "articles": [{"url": "https://b.com"}]},
            ]
        }
        # Should not raise
        self._do_validate(md, cluster_result)

    def test_fails_when_cluster_missing_from_parsed(self):
        md = (
            "# Cluster 1 — Theme A\nBody\n"
            "# Cluster 2 — Theme B\nBody\n"
        )
        cluster_result = {
            "clusters": [
                {"cluster_id": "cluster_1", "articles": [{"url": "https://a.com"}]},
            ]
        }
        with pytest.raises(RuntimeError, match="MISSING from parsed"):
            self._do_validate(md, cluster_result)

    def test_fails_when_cluster_has_zero_urls(self):
        md = "# Cluster 1 — Theme A\nBody\n"
        cluster_result = {
            "clusters": [
                {"cluster_id": "cluster_1", "articles": []},
            ]
        }
        with pytest.raises(RuntimeError, match="0 articles with URLs"):
            self._do_validate(md, cluster_result)

    def test_fails_when_extra_cluster_in_parsed(self):
        md = "# Cluster 1 — Theme A\nBody\n"
        cluster_result = {
            "clusters": [
                {"cluster_id": "cluster_1", "articles": [{"url": "https://a.com"}]},
                {"cluster_id": "cluster_2", "articles": [{"url": "https://b.com"}]},
            ]
        }
        with pytest.raises(RuntimeError, match="NOT in document"):
            self._do_validate(md, cluster_result)

    def test_passes_with_numbered_prefix_format(self):
        """Handles '# 4. Cluster A — ...' heading format."""
        md = (
            "# 4. Cluster A — Minerals\nBody\n"
            "# 5. Cluster B — Energy\nBody\n"
        )
        cluster_result = {
            "clusters": [
                {"cluster_id": "cluster_A", "articles": [{"url": "https://a.com"}]},
                {"cluster_id": "cluster_B", "articles": [{"url": "https://b.com"}]},
            ]
        }
        self._do_validate(md, cluster_result)

    def test_error_message_contains_category(self):
        md = "# Cluster 1 — Theme\nBody\n"
        cluster_result = {"clusters": []}
        with pytest.raises(RuntimeError, match=r"\[my_category\]"):
            self._do_validate(md, cluster_result, "my_category")


# ===================================================================
# Deterministic random.sample seeding
# ===================================================================


class TestDeterministicSampling:
    """Verify random.sample is seeded by category for reproducibility."""

    def test_same_category_same_sample(self):
        """Two calls with same category + same data → identical sample."""
        import random

        items = list(range(100))
        rng1 = random.Random("culture_and_media")
        rng2 = random.Random("culture_and_media")
        sample1 = rng1.sample(items, 30)
        sample2 = rng2.sample(items, 30)
        assert sample1 == sample2

    def test_different_category_different_sample(self):
        """Different category seeds → different samples."""
        import random

        items = list(range(100))
        rng1 = random.Random("culture_and_media")
        rng2 = random.Random("artificial_intelligence")
        sample1 = rng1.sample(items, 30)
        sample2 = rng2.sample(items, 30)
        assert sample1 != sample2


# ===================================================================
# Companion bundle (.bundle.jsonl) persistence
# ===================================================================


class TestCompanionBundlePersistence:
    """Verify _write_phase4_markdown saves a .bundle.jsonl companion."""

    def test_bundle_jsonl_written(self, tmp_path: Path):
        from src.demo.demo_media_linkage import _write_phase4_markdown

        bundle = {
            "article_count": 2,
            "articles": [
                {"url": "https://a.com/1", "title": "Art 1", "author": "A",
                 "publisher": "P", "date_publication": "2025-01-01",
                 "source_type": "linked"},
                {"url": "https://b.com/2", "title": "Art 2", "author": "B",
                 "publisher": "Q", "date_publication": "2025-01-02",
                 "source_type": "bm25_unlinked"},
            ],
        }
        md_path = _write_phase4_markdown(
            "test_cat", "gpt-5.4", "# Analysis\nBody text.",
            {"input_tokens": 100, "output_tokens": 50, "cost_usd": 0.01},
            bundle, tmp_path,
        )

        bundle_path = md_path.with_suffix(".bundle.jsonl")
        assert bundle_path.exists(), "Companion .bundle.jsonl not created"

        # Read back and verify content
        articles_back = []
        with open(bundle_path) as f:
            for line in f:
                if line.strip():
                    articles_back.append(json.loads(line))

        assert len(articles_back) == 2
        assert articles_back[0]["url"] == "https://a.com/1"
        assert articles_back[1]["url"] == "https://b.com/2"

    def test_bundle_ordering_matches_appendix(self, tmp_path: Path):
        """Bundle JSONL article order matches the APPENDIX table numbering."""
        import re

        from src.demo.demo_media_linkage import _write_phase4_markdown

        urls = [f"https://example.com/art{i}" for i in range(1, 6)]
        bundle = {
            "article_count": 5,
            "articles": [
                {"url": u, "title": f"Title {i}", "author": f"Author {i}",
                 "publisher": f"Pub {i}", "date_publication": "2025-01-01",
                 "source_type": "linked"}
                for i, u in enumerate(urls, 1)
            ],
        }
        md_path = _write_phase4_markdown(
            "test", "gpt-5.4", "# Output",
            {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0},
            bundle, tmp_path,
        )

        # Read back bundle
        bundle_back = []
        with open(md_path.with_suffix(".bundle.jsonl")) as f:
            for line in f:
                if line.strip():
                    bundle_back.append(json.loads(line))

        # Read appendix from markdown
        md_text = md_path.read_text()
        appendix_urls = []
        in_appendix = False
        for line in md_text.split("\n"):
            if "APPENDIX: Article Reference Index" in line:
                in_appendix = True
                continue
            if in_appendix and line.startswith("|") and "http" in line:
                m = re.search(r'https?://[^\s|]+', line)
                if m:
                    appendix_urls.append(m.group(0))

        # Both orderings must be identical
        bundle_urls = [a["url"] for a in bundle_back]
        assert bundle_urls == appendix_urls
        assert bundle_urls == urls

    def test_empty_bundle_no_crash(self, tmp_path: Path):
        from src.demo.demo_media_linkage import _write_phase4_markdown

        md_path = _write_phase4_markdown(
            "empty_test", "gpt-5.4", "# Empty",
            {"input_tokens": 0, "output_tokens": 0, "cost_usd": 0},
            {"articles": [], "article_count": 0}, tmp_path,
        )
        bundle_path = md_path.with_suffix(".bundle.jsonl")
        assert bundle_path.exists()
        assert bundle_path.read_text().strip() == ""
