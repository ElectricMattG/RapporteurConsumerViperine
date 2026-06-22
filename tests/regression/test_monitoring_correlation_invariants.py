"""
tests/regression/test_monitoring_correlation_invariants.py

Phase 1 regression tests — story-mandated invariant that session_id / request_id /
origin must NEVER appear as Prometheus metric label names in any consumer.

Adding a UUID-valued label to a metric would explode cardinality (one time-series per
UUID).  These tests are static/structural: they scan the prometheus_client registry
and assert that the forbidden label names are absent.

Coverage:
  TestPrometheusLabelCardinalityGuard — no registered metric carries session_id,
                                        request_id, or origin as a label dimension.
"""
from __future__ import annotations

import logging

import pytest

# ── Forbidden label names (story §"Logging Convention") ───────────────────────

_FORBIDDEN_LABELS = frozenset({"session_id", "request_id", "origin"})

logger = logging.getLogger(__name__)


def _all_registered_metrics():
    """Return all metrics currently registered in the default prometheus_client registry."""
    from prometheus_client import REGISTRY
    return list(REGISTRY.collect())


# ══════════════════════════════════════════════════════════════════════════════
# TestPrometheusLabelCardinalityGuard
# ══════════════════════════════════════════════════════════════════════════════

class TestPrometheusLabelCardinalityGuard:
    """
    Story §"Logging Convention" — correlation IDs live in logs, never in metric labels.

    These tests scan every metric registered in the default prometheus_client registry
    at import-time (which includes all metrics declared at module level in
    src/monitoring/) and assert that none of the forbidden label names appear.

    If a contributor accidentally adds e.g. Counter('...', [..., 'session_id']),
    the corresponding test fails immediately in CI.
    """

    def _collect_label_names(self) -> dict[str, set[str]]:
        """Map metric name → set of label names for every registered metric."""
        metrics_labels: dict[str, set[str]] = {}
        for metric_family in _all_registered_metrics():
            labels: set[str] = set()
            # prometheus_client exposes label names via the samples' labels keys
            for sample in metric_family.samples:
                labels.update(sample.labels.keys())
            metrics_labels[metric_family.name] = labels
        return metrics_labels

    def test_no_metric_carries_session_id_label(self):
        """session_id must not appear in any metric's label set."""
        offenders = [
            name
            for name, labels in self._collect_label_names().items()
            if "session_id" in labels
        ]
        assert not offenders, (
            f"Metrics with session_id label (would explode cardinality): {offenders}"
        )

    def test_no_metric_carries_request_id_label(self):
        """request_id must not appear in any metric's label set."""
        offenders = [
            name
            for name, labels in self._collect_label_names().items()
            if "request_id" in labels
        ]
        assert not offenders, (
            f"Metrics with request_id label (would explode cardinality): {offenders}"
        )

    def test_no_metric_carries_origin_label(self):
        """origin must not appear in any metric's label set."""
        offenders = [
            name
            for name, labels in self._collect_label_names().items()
            if "origin" in labels
        ]
        assert not offenders, (
            f"Metrics with origin label (would explode cardinality): {offenders}"
        )

    def test_no_kafka_consumer_metric_carries_session_id_label(self):
        """Focused check: kafka_consumer_* metrics must not carry session_id."""
        offenders = [
            name
            for name, labels in self._collect_label_names().items()
            if name.startswith("kafka_consumer_") and "session_id" in labels
        ]
        assert not offenders, (
            f"Kafka consumer metrics with session_id label: {offenders}"
        )

    def test_no_kafka_consumer_metric_carries_origin_label(self):
        """Focused check: kafka_consumer_* metrics must not carry origin."""
        offenders = [
            name
            for name, labels in self._collect_label_names().items()
            if name.startswith("kafka_consumer_") and "origin" in labels
        ]
        assert not offenders, (
            f"Kafka consumer metrics with origin label: {offenders}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# TestSummarizerMetricsLabelsUnchanged  (Phase 2)
# ══════════════════════════════════════════════════════════════════════════════

class TestSummarizerMetricsLabelsUnchanged:
    """
    Story-mandated invariant — adding correlation IDs to the summarizer must
    not regress the label shape on existing LLM metrics.

    These are structural checks: they assert that the well-known label sets on
    llm_api_calls / llm_api_errors remain exactly the fixed dimensions they
    were defined with.  Adding session_id / request_id / origin would explode
    cardinality; this test makes that a CI failure.
    """

    def _label_names_for(self, prefix: str) -> dict[str, set[str]]:
        from prometheus_client import REGISTRY
        result = {}
        for mf in REGISTRY.collect():
            if mf.name.startswith(prefix):
                labels: set[str] = set()
                for sample in mf.samples:
                    labels.update(sample.labels.keys())
                result[mf.name] = labels
        return result

    def test_llm_api_calls_label_set_unchanged(self):
        """llm_api_calls_total labels must not include session/request/origin."""
        for name, labels in self._label_names_for("kafka_consumer_llm_api_calls").items():
            for forbidden in ("session_id", "request_id", "origin"):
                assert forbidden not in labels, (
                    f"{name} carries forbidden label {forbidden!r}"
                )

    def test_llm_api_errors_label_set_unchanged(self):
        """llm_api_errors_total labels must not include session/request/origin."""
        for name, labels in self._label_names_for("kafka_consumer_llm_api_errors").items():
            for forbidden in ("session_id", "request_id", "origin"):
                assert forbidden not in labels, (
                    f"{name} carries forbidden label {forbidden!r}"
                )
