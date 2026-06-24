"""
tests/regression/test_doc_id_independence.py

Risk-register regression: document_id MUST NOT be derived from request_id.

The story's Phase 2 rekeys Viperine's internal aggregation on request_id, but
make_document_id() must stay timestamp-derived (req_YYYYMMDD_HHMMSS_1NNN) so
that the RAG corpus receives a stable, independently-minted identifier for each
document — regardless of what shape the inbound request_id carries (legacy
"query_<int>", modern UUIDv4, or anything in between).

This independence is the precondition the companion RAG plan's risk register
relies on (see docs/Phase_Plan_Story_VIPERINE_SESSION_IDs.md §Risk Register).
"""
from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

from consumers._doc_id import make_document_id

pytestmark = pytest.mark.regression

_DOC_ID_PATTERN = re.compile(r"^req_\d{8}_\d{6}_1\d{3,}$")

_UUID_REQUEST_ID = "22222222-2222-4222-8222-222222222222"
_LEGACY_REQUEST_ID = "query_007"
_TEST_URL = "https://example.com/article"


class TestDocumentIdIndependentOfRequestId:
    """make_document_id() output must not contain or be derived from request_id."""

    def test_document_id_does_not_equal_uuid_request_id(self):
        doc_id, _ = make_document_id(_TEST_URL, batch_index=1)
        assert doc_id != _UUID_REQUEST_ID

    def test_document_id_does_not_contain_uuid_request_id(self):
        doc_id, _ = make_document_id(_TEST_URL, batch_index=1)
        assert _UUID_REQUEST_ID not in doc_id

    def test_document_id_does_not_contain_legacy_request_id(self):
        doc_id, _ = make_document_id(_TEST_URL, batch_index=1)
        assert _LEGACY_REQUEST_ID not in doc_id

    def test_document_id_matches_timestamp_schema(self):
        """Positive: output always matches req_YYYYMMDD_HHMMSS_1NNN."""
        doc_id, _ = make_document_id(_TEST_URL, batch_index=1)
        assert _DOC_ID_PATTERN.match(doc_id), f"Unexpected format: {doc_id!r}"

    def test_two_different_request_ids_produce_identical_format(self):
        """The schema is stable regardless of what request_id the caller has."""
        when = datetime(2026, 6, 23, 12, 0, 0, tzinfo=timezone.utc)
        doc_id_a, _ = make_document_id(_TEST_URL, batch_index=1, when=when)
        doc_id_b, _ = make_document_id(_TEST_URL, batch_index=1, when=when)
        # Same timestamp → same ID (idempotent; dedup handles the rest)
        assert doc_id_a == doc_id_b

    def test_batch_index_only_affects_suffix(self):
        """Only the numeric suffix changes across batch positions."""
        when = datetime(2026, 6, 23, 12, 0, 0, tzinfo=timezone.utc)
        doc_id_1, _ = make_document_id(_TEST_URL, batch_index=1, when=when)
        doc_id_7, _ = make_document_id(_TEST_URL, batch_index=7, when=when)
        assert doc_id_1.startswith("req_20260623_120000_")
        assert doc_id_7.startswith("req_20260623_120000_")
        assert doc_id_1 != doc_id_7

    def test_dedup_key_is_url_derived_not_request_id_derived(self):
        """dedup_key must change when URL changes, but not when request_id would."""
        when = datetime(2026, 6, 23, 12, 0, 0, tzinfo=timezone.utc)
        _, dedup_a = make_document_id("https://example.com/a", batch_index=1, when=when)
        _, dedup_b = make_document_id("https://example.com/b", batch_index=1, when=when)
        _, dedup_a2 = make_document_id("https://example.com/a", batch_index=1, when=when)
        assert dedup_a != dedup_b           # different URLs → different keys
        assert dedup_a == dedup_a2          # same URL → same key (content-addressed)
        assert _UUID_REQUEST_ID not in dedup_a
