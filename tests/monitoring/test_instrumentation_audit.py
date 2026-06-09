"""
Phase 7 Instrumentation Audit

Verifies that every consumer and monitoring module has the required
'# instrumentation - <metric_name>' call-site comments, that MONITORING.md
exists with its required sections, and that every alert's runbook_url
points to a real file.

The '# instrumentation' comment convention is the project-standard way of
marking exactly where a metric is emitted.  Tests here act as a ratchet:
once a call-site comment exists it must not be silently removed.
"""

import re
from pathlib import Path

import pytest
import yaml

# ── Paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parents[2]
SRC_CONSUMERS = ROOT / "src" / "consumers"
SRC_MONITORING = ROOT / "src" / "monitoring"
ALERTS_YML = ROOT / "monitoring" / "prometheus" / "alerts.yml"
MONITORING_MD = ROOT / "monitoring" / "MONITORING.md"
RUNBOOKS_DIR = ROOT / "monitoring" / "runbooks"

CONSUMER_FILES = {
    "web_scraper": SRC_CONSUMERS / "kafka_web_scraper.py",
    "summarizer": SRC_CONSUMERS / "kafka_summarizer.py",
    "summary_indexer": SRC_CONSUMERS / "kafka_summary_indexer.py",
    "dead_letter": SRC_CONSUMERS / "kafka_dead_letter_consumer.py",
    "index_aggregator": SRC_CONSUMERS / "kafka_index_aggregator.py",
}

# Consumers that call record_publish() must document it
PRODUCING_CONSUMERS = {"web_scraper", "summarizer", "summary_indexer", "index_aggregator"}

# Every consumer must have these four call-site comments
MANDATORY_CALLSITES = {
    "startup_timestamp_seconds",
    "consumer_info",
    "last_message_processed_timestamp",
    "processing_failures_total",
}

# Metrics that must be documented inside system_metrics.py itself
SYSTEM_METRICS_CALLSITES = {
    "disk_usage_bytes",
    "disk_available_bytes",
    "open_file_descriptors",
    "thread_count",
}

# kafka_metrics.py documents its own produce helpers
KAFKA_METRICS_CALLSITES = {
    "messages_published_total",
    "publish_duration_seconds",
}

MONITORING_MD_REQUIRED_SECTIONS = [
    "Quick Start",
    "Architecture",
    "File & Config Reference",
    "Metrics Catalogue",
    "Alert Rules",
    "Troubleshooting",
    "Operational Notes",
]

# ── Helpers ───────────────────────────────────────────────────────────────────
_INSTRUMENTATION_RE = re.compile(r'#\s*instrumentation\s*-\s*(\S+)')


def callsites(path: Path) -> set[str]:
    """Return set of metric names from '# instrumentation - <name>' in *path*."""
    return set(_INSTRUMENTATION_RE.findall(path.read_text()))


def all_callsites_in_src() -> set[str]:
    """All instrumentation comment names across the entire src/ tree."""
    names: set[str] = set()
    for py in ROOT.glob("src/**/*.py"):
        names |= callsites(py)
    return names


def known_metric_suffixes() -> set[str]:
    """
    Derive the set of valid metric name suffixes by parsing every
    f'{namespace}_<name>' and bare 'kafka_consumer_<name>' string in
    both the monitoring module and consumer source files.  Consumer files
    may define their own metrics directly (e.g. llm_api_errors_total).
    """
    fstring_re = re.compile(r"f'\{(?:namespace|ns)\}_([a-z_]+)'")
    literal_re = re.compile(r"'kafka_consumer_([a-z_]+)'")
    suffixes: set[str] = set()
    search_dirs = [SRC_MONITORING, SRC_CONSUMERS]
    for directory in search_dirs:
        for py in directory.glob("**/*.py"):
            text = py.read_text()
            suffixes |= set(fstring_re.findall(text))
            suffixes |= set(literal_re.findall(text))
    # Add attribute-name aliases used in comments (e.g. 'consumer_info'
    # is the attribute name for the Info metric whose suffix is 'info').
    suffixes |= {"consumer_info", "sessions_in_progress"}
    return suffixes


# ── 1. Mandatory call-sites in every consumer ─────────────────────────────────
class TestMandatoryCallsites:

    @pytest.mark.parametrize("consumer", sorted(CONSUMER_FILES))
    def test_startup_timestamp_seconds(self, consumer):
        assert "startup_timestamp_seconds" in callsites(CONSUMER_FILES[consumer]), (
            f"{consumer}: missing '# instrumentation - startup_timestamp_seconds'"
        )

    @pytest.mark.parametrize("consumer", sorted(CONSUMER_FILES))
    def test_consumer_info(self, consumer):
        assert "consumer_info" in callsites(CONSUMER_FILES[consumer]), (
            f"{consumer}: missing '# instrumentation - consumer_info'"
        )

    @pytest.mark.parametrize("consumer", sorted(CONSUMER_FILES))
    def test_last_message_processed_timestamp(self, consumer):
        assert "last_message_processed_timestamp" in callsites(CONSUMER_FILES[consumer]), (
            f"{consumer}: missing '# instrumentation - last_message_processed_timestamp'"
        )

    @pytest.mark.parametrize("consumer", sorted(CONSUMER_FILES))
    def test_processing_failures_total(self, consumer):
        assert "processing_failures_total" in callsites(CONSUMER_FILES[consumer]), (
            f"{consumer}: missing '# instrumentation - processing_failures_total'"
        )


# ── 2. Producing consumers must document publish call-sites ───────────────────
class TestProducingConsumers:

    @pytest.mark.parametrize("consumer", sorted(PRODUCING_CONSUMERS))
    def test_messages_published_total(self, consumer):
        assert "messages_published_total" in callsites(CONSUMER_FILES[consumer]), (
            f"{consumer}: produces to Kafka but missing '# instrumentation - messages_published_total'"
        )


# ── 3. System metrics module documents its own Phase 3 call-sites ─────────────
class TestSystemMetricsCallsites:

    @pytest.fixture(scope="class")
    def system_callsites(self):
        return callsites(SRC_MONITORING / "system_metrics.py")

    @pytest.mark.parametrize("metric", sorted(SYSTEM_METRICS_CALLSITES))
    def test_present_in_system_metrics(self, system_callsites, metric):
        assert metric in system_callsites, (
            f"system_metrics.py: missing '# instrumentation - {metric}'"
        )


# ── 4. kafka_metrics.py documents its own produce helpers ────────────────────
class TestKafkaMetricsCallsites:

    @pytest.fixture(scope="class")
    def kafka_callsites(self):
        return callsites(SRC_MONITORING / "kafka_metrics.py")

    @pytest.mark.parametrize("metric", sorted(KAFKA_METRICS_CALLSITES))
    def test_present_in_kafka_metrics(self, kafka_callsites, metric):
        assert metric in kafka_callsites, (
            f"kafka_metrics.py: missing '# instrumentation - {metric}'"
        )


# ── 5. No instrumentation comment references an unknown metric name ───────────
class TestNoUnknownCallsites:

    def test_all_callsite_names_are_known(self):
        valid = known_metric_suffixes()
        unknown = []
        for py in ROOT.glob("src/**/*.py"):
            for name in callsites(py):
                if name not in valid:
                    unknown.append((py.name, name))
        assert not unknown, (
            "Found '# instrumentation' comments referencing unrecognised metric names:\n"
            + "\n".join(f"  {f}: {n}" for f, n in unknown)
        )


# ── 6. MONITORING.md exists and has required sections ────────────────────────
class TestMonitoringDoc:

    @pytest.fixture(scope="class")
    def doc_text(self):
        return MONITORING_MD.read_text()

    def test_monitoring_md_exists(self):
        assert MONITORING_MD.exists(), "monitoring/MONITORING.md not found"

    def test_monitoring_md_non_empty(self, doc_text):
        assert len(doc_text) > 500, "monitoring/MONITORING.md is suspiciously short"

    @pytest.mark.parametrize("section", MONITORING_MD_REQUIRED_SECTIONS)
    def test_required_section_present(self, doc_text, section):
        assert section in doc_text, (
            f"monitoring/MONITORING.md is missing section: '{section}'"
        )

    def test_quick_start_has_make_commands(self, doc_text):
        assert "make monitoring_deploy" in doc_text
        assert "make monitoring_status" in doc_text
        assert "make monitoring_open" in doc_text

    def test_prometheus_url_present(self, doc_text):
        assert "192.168.1.3:30090" in doc_text

    def test_grafana_url_present(self, doc_text):
        assert "192.168.1.3:30030" in doc_text


# ── 7. Every alert's runbook_url resolves to a real file ─────────────────────
class TestRunbookCoverage:

    @pytest.fixture(scope="class")
    def alert_rules(self):
        data = yaml.safe_load(ALERTS_YML.read_text())
        rules = []
        for group in data.get("groups", []):
            rules.extend(group.get("rules", []))
        return rules

    def test_all_alerts_have_runbook_url(self, alert_rules):
        missing = [
            r["alert"] for r in alert_rules
            if not r.get("annotations", {}).get("runbook_url")
        ]
        assert not missing, f"Alerts missing runbook_url: {missing}"

    def test_all_runbook_urls_resolve(self, alert_rules):
        missing_files = []
        for rule in alert_rules:
            url = rule.get("annotations", {}).get("runbook_url", "")
            # runbook_url is a repo-relative path like "monitoring/runbooks/consumer_down.md"
            runbook_path = ROOT / url
            if not runbook_path.exists():
                missing_files.append((rule["alert"], url))
        assert not missing_files, (
            "Alerts with runbook_url pointing to non-existent files:\n"
            + "\n".join(f"  {alert}: {url}" for alert, url in missing_files)
        )

    def test_runbook_count_matches_alert_count(self, alert_rules):
        runbook_files = list(RUNBOOKS_DIR.glob("*.md"))
        # Subtract README.md from the count
        runbooks = [f for f in runbook_files if f.name != "README.md"]
        assert len(runbooks) == len(alert_rules), (
            f"Runbook file count ({len(runbooks)}) != alert count ({len(alert_rules)}). "
            "Either a runbook is missing or an orphan runbook exists."
        )
