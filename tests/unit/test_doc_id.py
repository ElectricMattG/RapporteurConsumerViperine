"""
tests/unit/test_doc_id.py

Full coverage of src/consumers/_doc_id.py

Contracts verified:
- document_id format  "req_YYYYMMDD_HHMMSS_1NNN"
- batch_index encoded as 1000 + n
- injected timestamp produces deterministic output
- dedup_key is 32-char lowercase hex
- same URL → same dedup_key regardless of when/batch
- different URLs → different dedup_keys
- same URL at two timestamps → same dedup_key, different document_id
"""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

import pytest

from consumers._doc_id import make_document_id
from utils.url_normalize import normalize_url

_DOC_ID_PAT = re.compile(r'^req_\d{8}_\d{6}_\d{4,}$')
_FIXED_WHEN = datetime(2026, 6, 4, 13, 1, 9, tzinfo=timezone.utc)
_URL_A = "https://example.com/article?utm_source=twitter"
_URL_B = "https://other.org/page"


class TestMakeDocumentId:
    def test_format_matches_pattern(self):
        doc_id, _ = make_document_id(_URL_A, when=_FIXED_WHEN)
        assert _DOC_ID_PAT.match(doc_id), f"Unexpected format: {doc_id}"

    def test_timestamp_embedded(self):
        doc_id, _ = make_document_id(_URL_A, when=_FIXED_WHEN)
        assert "20260604" in doc_id
        assert "130109" in doc_id

    def test_batch_index_default_is_1001(self):
        doc_id, _ = make_document_id(_URL_A, when=_FIXED_WHEN)
        # default batch_index=1  → suffix 1001
        assert doc_id.endswith("_1001")

    def test_batch_index_7_gives_1007(self):
        doc_id, _ = make_document_id(_URL_A, batch_index=7, when=_FIXED_WHEN)
        assert doc_id.endswith("_1007")

    def test_batch_index_999_gives_1999(self):
        doc_id, _ = make_document_id(_URL_A, batch_index=999, when=_FIXED_WHEN)
        assert doc_id.endswith("_1999")

    def test_two_calls_same_time_same_output(self):
        id1, dk1 = make_document_id(_URL_A, when=_FIXED_WHEN)
        id2, dk2 = make_document_id(_URL_A, when=_FIXED_WHEN)
        assert id1 == id2
        assert dk1 == dk2

    def test_returns_tuple_of_two_strings(self):
        result = make_document_id(_URL_A, when=_FIXED_WHEN)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(x, str) for x in result)


class TestDedupKey:
    def test_is_32_char_hex(self):
        _, dk = make_document_id(_URL_A, when=_FIXED_WHEN)
        assert len(dk) == 32
        assert re.fullmatch(r'[0-9a-f]{32}', dk), f"Not hex: {dk}"

    def test_same_url_same_dedup_key(self):
        _, dk1 = make_document_id(_URL_A, batch_index=1, when=_FIXED_WHEN)
        _, dk2 = make_document_id(_URL_A, batch_index=5, when=_FIXED_WHEN)
        assert dk1 == dk2

    def test_same_url_tracking_params_stripped(self):
        """utm_source should be stripped before hashing."""
        url_clean = "https://example.com/article"
        url_tracked = "https://example.com/article?utm_source=twitter&utm_medium=social"
        _, dk_clean = make_document_id(url_clean, when=_FIXED_WHEN)
        _, dk_tracked = make_document_id(url_tracked, when=_FIXED_WHEN)
        assert dk_clean == dk_tracked

    def test_different_urls_different_keys(self):
        _, dk_a = make_document_id(_URL_A, when=_FIXED_WHEN)
        _, dk_b = make_document_id(_URL_B, when=_FIXED_WHEN)
        assert dk_a != dk_b

    def test_different_timestamps_same_dedup_key(self):
        when2 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        _, dk1 = make_document_id(_URL_A, when=_FIXED_WHEN)
        _, dk2 = make_document_id(_URL_A, when=when2)
        assert dk1 == dk2

    def test_different_timestamps_different_document_id(self):
        when2 = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        id1, _ = make_document_id(_URL_A, when=_FIXED_WHEN)
        id2, _ = make_document_id(_URL_A, when=when2)
        assert id1 != id2

    def test_dedup_key_matches_manual_md5(self):
        """Verify the exact md5 formula."""
        _, dk = make_document_id(_URL_A, when=_FIXED_WHEN)
        expected = hashlib.md5(normalize_url(_URL_A).encode()).hexdigest()
        assert dk == expected

    def test_empty_url_does_not_raise(self):
        """Edge case: empty URL should return something, not raise."""
        doc_id, dk = make_document_id("", when=_FIXED_WHEN)
        assert isinstance(doc_id, str)
        assert isinstance(dk, str)
