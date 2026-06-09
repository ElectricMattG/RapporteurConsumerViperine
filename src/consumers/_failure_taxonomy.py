"""
Failure taxonomy for the summarizer pipeline.

Maps exceptions to:
  - category     str   One of the 9 canonical failure categories.
  - retryable    bool  Whether the caller should retry.
  - max_attempts int   Maximum total attempts (1 = no retry).
  - degrade      bool  On final model_empty attempt, emit a stub summary.

Categories
----------
transient         Network blip, timeout, 429 rate-limit.
model_empty       LLM returned finish_reason STOP with empty body.
policy_block      LLM refused (SAFETY / RECITATION / PROHIBITED_CONTENT).
auth              API key invalid / quota exhausted.
not_found         404 or file-not-found for the source URL.
bad_input         Malformed input (empty text, encoding error).
permanent_internal Unexpected internal bug (assertion, attribute error …).
embedding         Embedding service failure.
unknown           Anything else; one retry allowed.
"""
from __future__ import annotations

import re
from typing import NamedTuple


class FailureClassification(NamedTuple):
    category: str
    retryable: bool
    max_attempts: int
    degrade: bool  # emit a degraded stub on final model_empty attempt


# ── string patterns to match against exception message ────────────────────────
_EMPTY_RESPONSE_PAT = re.compile(
    r'empty response from model|finish_reason.*stop', re.IGNORECASE
)
_POLICY_PAT = re.compile(
    r'finish_reason.*(?:SAFETY|RECITATION|PROHIBITED_CONTENT)|'
    r'response was blocked|content policy',
    re.IGNORECASE,
)
_AUTH_PAT = re.compile(
    r'api[_\s]key.*invalid|authentication|quota.*exceeded|'
    r'403.*forbidden|permission denied',
    re.IGNORECASE,
)
_NOT_FOUND_PAT = re.compile(r'\b404\b|not found|no such file', re.IGNORECASE)
_TRANSIENT_PAT = re.compile(
    r'timeout|timed.out|connection.reset|connection.refused|'
    r'temporarily unavailable|503|429|rate.limit',
    re.IGNORECASE,
)
_EMBEDDING_PAT = re.compile(
    r'embedding|embed\b', re.IGNORECASE
)
_BAD_INPUT_PAT = re.compile(
    r'empty text|decode.*error|encoding.*error|invalid.*json', re.IGNORECASE
)

# ── LLM error-as-content detection ───────────────────────────────────────────
# The model sometimes returns HTTP 200 with finish_reason STOP, but the body
# is an error/refusal message rather than a valid summary. These pass through
# as "successful" unless explicitly detected.
_MODEL_ERROR_OUTPUT_PAT = re.compile(
    r'^\s*#\s*Error\b|'
    r'Failed to generate summary|'
    r'I(?:\'m| am) (?:unable|not able) to (?:generate|create|provide|summarize|summarise)|'
    r'I cannot (?:access|summarize|summarise|generate)|'
    r'(?:As an AI|I don.t have access)',
    re.IGNORECASE | re.MULTILINE
)


def validate_summary_content(text: str) -> bool:
    """
    Return False if the 'summary' is actually an LLM error/refusal output.

    Should be called BEFORE indexing to prevent garbage from being stored.

    Args:
        text: The markdown summary text returned by the LLM.

    Returns:
        True if content appears to be a valid summary.
        False if content matches known LLM error/refusal patterns.
    """
    if not text or len(text.strip()) < 20:
        return False
    return not _MODEL_ERROR_OUTPUT_PAT.search(text)


def classify_error(exc: Exception) -> FailureClassification:
    """
    Classify *exc* into a canonical failure category.

    Returns a FailureClassification named-tuple with:
        category      (str)
        retryable     (bool)
        max_attempts  (int)  total attempts incl. first
        degrade       (bool)
    """
    msg = str(exc)
    exc_type = type(exc).__name__

    if _EMPTY_RESPONSE_PAT.search(msg):
        # model_empty: retry once with identical prompt; on second failure
        # degrade to an empty-summary stub so the session can complete.
        return FailureClassification('model_empty', retryable=True, max_attempts=2, degrade=True)

    if _POLICY_PAT.search(msg):
        return FailureClassification('policy_block', retryable=False, max_attempts=1, degrade=False)

    if _AUTH_PAT.search(msg):
        return FailureClassification('auth', retryable=False, max_attempts=1, degrade=False)

    if _NOT_FOUND_PAT.search(msg):
        return FailureClassification('not_found', retryable=False, max_attempts=1, degrade=False)

    if _BAD_INPUT_PAT.search(msg):
        return FailureClassification('bad_input', retryable=False, max_attempts=1, degrade=False)

    if _EMBEDDING_PAT.search(msg):
        return FailureClassification('embedding', retryable=True, max_attempts=3, degrade=False)

    if _TRANSIENT_PAT.search(msg):
        return FailureClassification('transient', retryable=True, max_attempts=3, degrade=False)

    if exc_type in ('AttributeError', 'AssertionError', 'TypeError', 'ValueError'):
        return FailureClassification('permanent_internal', retryable=False, max_attempts=1, degrade=False)

    # unknown: give one retry
    return FailureClassification('unknown', retryable=True, max_attempts=2, degrade=False)
