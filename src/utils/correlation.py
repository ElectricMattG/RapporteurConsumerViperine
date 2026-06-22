"""
src/utils/correlation.py

Lightweight correlation-ID plumbing using Python ContextVars.

Design mirrors the companion RAG 0.2.0 implementation so that both repos
can be grep-linked via the same header names and the same sentinel value.

Sentinel ("-")
--------------
A missing or unset ID is represented by the string "-" rather than None,
matching the RAG convention.  The embedding client reads this and omits
the header when it sees the sentinel, so no "X-Session-Id: -" ever leaves
the process.

Public API
----------
    install_correlation_record_factory()   — call once at consumer start-up
    bind_correlation_ids(session_id, request_id)  — token-based bind (use with try/finally)
    reset_correlation_ids(token_pair)      — restore previous context
    get_session_id()  → str               — current value or "-"
    get_request_id()  → str               — current value or "-"

Example (consumer message loop)
---------------------------------
    from utils.correlation import bind_correlation_ids, reset_correlation_ids

    tokens = bind_correlation_ids(session_id, request_id)
    try:
        await self._process_message(msg)
    finally:
        reset_correlation_ids(tokens)
"""
from __future__ import annotations

import logging
from contextvars import ContextVar, Token
from typing import Tuple

# Sentinel matches RAG convention — never emitted as a header value
_SENTINEL = "-"

_session_id_var: ContextVar[str] = ContextVar("session_id", default=_SENTINEL)
_request_id_var: ContextVar[str] = ContextVar("request_id", default=_SENTINEL)


# ── Public setters / getters ────────────────────────────────────────────────

def bind_correlation_ids(
    session_id: str | None,
    request_id: str | None,
) -> Tuple[Token, Token]:
    """
    Bind session_id and request_id to the current async context.

    Returns a (session_token, request_token) pair that must be passed to
    reset_correlation_ids() in a finally block to restore the previous values.

    None and empty string are coerced to the sentinel "-".
    """
    sid = session_id if (session_id and session_id != _SENTINEL) else _SENTINEL
    rid = request_id if (request_id and request_id != _SENTINEL) else _SENTINEL
    return _session_id_var.set(sid), _request_id_var.set(rid)


def reset_correlation_ids(tokens: Tuple[Token, Token]) -> None:
    """Restore the context to the state before the matching bind_correlation_ids call."""
    session_token, request_token = tokens
    _session_id_var.reset(session_token)
    _request_id_var.reset(request_token)


def get_session_id() -> str:
    """Return the current session_id or the sentinel "-" if unbound."""
    return _session_id_var.get()


def get_request_id() -> str:
    """Return the current request_id or the sentinel "-" if unbound."""
    return _request_id_var.get()


# ── Log record factory ──────────────────────────────────────────────────────

def install_correlation_record_factory() -> None:
    """
    Wrap the default log record factory so every log record carries
    session_id and request_id attributes from the current ContextVar.

    Call once at consumer start-up (before any logging occurs).
    Re-entrant safe — calling twice is a no-op after the first install.
    """
    existing = logging.getLogRecordFactory()
    if getattr(existing, "_correlation_installed", False):
        return

    def _factory(*args, **kwargs):
        record = existing(*args, **kwargs)
        record.session_id = _session_id_var.get()
        record.request_id = _request_id_var.get()
        return record

    _factory._correlation_installed = True  # type: ignore[attr-defined]
    logging.setLogRecordFactory(_factory)
