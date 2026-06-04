"""
Document ID factory.

Format:  req_<YYYYMMDD>_<HHMMSS>_1<NNN>
Example: req_20260604_130109_1007

Properties:
  - Unique per (timestamp, batch position) — same URL re-processed later
    gets a new document_id, which is correct (it represents a new research
    event).  Deduplication is handled by dedup_key (md5 of normalized URL).
  - Lexicographically sortable by time.
  - Matches corpus convention (req_* prefix).

Returns (document_id, dedup_key) tuple so callers always carry both.
"""
import hashlib
from datetime import datetime, timezone

try:
    from src.utils.url_normalize import normalize_url
except ImportError:
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
    from src.utils.url_normalize import normalize_url


def make_document_id(
    url: str,
    batch_index: int = 1,
    when: datetime | None = None,
) -> tuple[str, str]:
    """
    Generate (document_id, dedup_key) for a summarized document.

    Args:
        url:         Source URL (used for dedup_key only).
        batch_index: 1-based position within the session batch (appended as
                     the last 3+ digits after "1").  Values 1-999 → 1001-1999.
        when:        Timestamp to embed in the ID.  Defaults to UTC now.

    Returns:
        (document_id, dedup_key)
        - document_id: "req_YYYYMMDD_HHMMSS_1NNN"
        - dedup_key:   32-char lowercase hex md5 of normalized URL
    """
    when = when or datetime.now(timezone.utc)
    stamp = when.strftime("%Y%m%d_%H%M%S")
    document_id = f"req_{stamp}_{1000 + batch_index}"
    dedup_key = hashlib.md5(normalize_url(url).encode()).hexdigest()
    return document_id, dedup_key
