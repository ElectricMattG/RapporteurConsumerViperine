"""
Tests for Prometheus alert rules syntax and structure.

Validates:
- alerts.yml is valid YAML
- Every rule has required fields: expr, for, labels.severity, annotations.summary
- No duplicate alert names
- Every alert has a runbook_url pointing to an existing file
- All severity values are valid (critical/warning)
"""
from pathlib import Path

import pytest
import yaml

ALERTS_FILE = Path('monitoring/prometheus/alerts.yml')
RUNBOOKS_DIR = Path('monitoring/runbooks')


@pytest.fixture
def alerts_data():
    """Load and parse alerts.yml."""
    assert ALERTS_FILE.exists(), f"Alerts file not found: {ALERTS_FILE}"
    with open(ALERTS_FILE, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def all_rules(alerts_data):
    """Extract all rules from all groups."""
    rules = []
    for group in alerts_data['groups']:
        rules.extend(group['rules'])
    return rules


class TestAlertsYamlStructure:
    """alerts.yml is valid and well-structured."""

    def test_yaml_loads(self, alerts_data):
        assert alerts_data is not None
        assert 'groups' in alerts_data

    def test_has_groups(self, alerts_data):
        assert len(alerts_data['groups']) >= 2

    def test_groups_have_names(self, alerts_data):
        for group in alerts_data['groups']:
            assert 'name' in group
            assert 'rules' in group
            assert len(group['rules']) > 0

    def test_groups_have_interval(self, alerts_data):
        for group in alerts_data['groups']:
            assert 'interval' in group


class TestAlertRuleFields:
    """Every alert rule has required fields."""

    def test_every_rule_has_alert_name(self, all_rules):
        for rule in all_rules:
            assert 'alert' in rule, f"Rule missing 'alert' name: {rule}"
            assert len(rule['alert']) > 0

    def test_every_rule_has_expr(self, all_rules):
        for rule in all_rules:
            assert 'expr' in rule, f"Rule {rule.get('alert')} missing 'expr'"
            assert len(str(rule['expr']).strip()) > 0

    def test_every_rule_has_for(self, all_rules):
        for rule in all_rules:
            assert 'for' in rule, f"Rule {rule.get('alert')} missing 'for'"

    def test_every_rule_has_severity(self, all_rules):
        for rule in all_rules:
            assert 'labels' in rule, f"Rule {rule.get('alert')} missing 'labels'"
            assert 'severity' in rule['labels'], f"Rule {rule.get('alert')} missing 'labels.severity'"

    def test_every_rule_has_summary(self, all_rules):
        for rule in all_rules:
            assert 'annotations' in rule, f"Rule {rule.get('alert')} missing 'annotations'"
            assert 'summary' in rule['annotations'], f"Rule {rule.get('alert')} missing 'annotations.summary'"


class TestAlertRuleValues:
    """Alert rule values are valid."""

    def test_no_duplicate_alert_names(self, all_rules):
        names = [rule['alert'] for rule in all_rules]
        duplicates = [n for n in names if names.count(n) > 1]
        assert not duplicates, f"Duplicate alert names: {set(duplicates)}"

    def test_severity_values_valid(self, all_rules):
        valid_severities = {'critical', 'warning'}
        for rule in all_rules:
            severity = rule['labels']['severity']
            assert severity in valid_severities, (
                f"Rule {rule['alert']} has invalid severity '{severity}'"
            )

    def test_runbook_urls_point_to_existing_files(self, all_rules):
        for rule in all_rules:
            runbook = rule['annotations'].get('runbook_url')
            assert runbook, f"Rule {rule['alert']} missing runbook_url"
            runbook_path = Path(runbook)
            assert runbook_path.exists(), (
                f"Rule {rule['alert']} references non-existent runbook: {runbook}"
            )

    def test_critical_rules_count(self, alerts_data):
        critical_group = next(
            g for g in alerts_data['groups'] if g['name'] == 'viperine_critical'
        )
        assert len(critical_group['rules']) == 8

    def test_warning_rules_count(self, alerts_data):
        warning_group = next(
            g for g in alerts_data['groups'] if g['name'] == 'viperine_warning'
        )
        assert len(warning_group['rules']) == 6


class TestPrometheusConfig:
    """prometheus.yml is valid."""

    def test_prometheus_yml_loads(self):
        prom_file = Path('monitoring/prometheus/prometheus.yml')
        assert prom_file.exists()
        with open(prom_file, 'r') as f:
            data = yaml.safe_load(f)
        assert 'scrape_configs' in data
        assert 'rule_files' in data

    def test_all_five_consumers_scraped(self):
        prom_file = Path('monitoring/prometheus/prometheus.yml')
        with open(prom_file, 'r') as f:
            data = yaml.safe_load(f)
        job_names = [sc['job_name'] for sc in data['scrape_configs']]
        expected = [
            'viperine_web_scraper',
            'viperine_summarizer',
            'viperine_summary_indexer',
            'viperine_index_aggregator',
            'viperine_dead_letter',
        ]
        for name in expected:
            assert name in job_names, f"Missing scrape job: {name}"

    def test_all_jobs_have_pipeline_label(self):
        prom_file = Path('monitoring/prometheus/prometheus.yml')
        with open(prom_file, 'r') as f:
            data = yaml.safe_load(f)
        for sc in data['scrape_configs']:
            labels = sc['static_configs'][0].get('labels', {})
            assert labels.get('pipeline') == 'viperine', (
                f"Job {sc['job_name']} missing pipeline=viperine label"
            )

    def test_rule_files_reference_alerts(self):
        prom_file = Path('monitoring/prometheus/prometheus.yml')
        with open(prom_file, 'r') as f:
            data = yaml.safe_load(f)
        assert 'alerts.yml' in data['rule_files']
