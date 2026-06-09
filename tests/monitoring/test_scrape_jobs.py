"""
Tests for k8s/monitoring/viperine-scrape-jobs.yaml.

Validates:
- YAML loads correctly
- All 5 consumers have scrape jobs defined
- Targets match Helm values.yaml metricsPort
- All jobs have pipeline=viperine label
- All jobs have service=kafka_consumer label
"""
from pathlib import Path

import pytest
import yaml

SCRAPE_JOBS_FILE = Path('k8s/monitoring/viperine-scrape-jobs.yaml')
HELM_VALUES_FILE = Path('helm/rapporteur-consumer-group/values.yaml')


@pytest.fixture
def scrape_jobs():
    """Load scrape jobs YAML."""
    assert SCRAPE_JOBS_FILE.exists(), f"Scrape jobs file not found: {SCRAPE_JOBS_FILE}"
    with open(SCRAPE_JOBS_FILE, 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture
def helm_values():
    """Load Helm values.yaml."""
    assert HELM_VALUES_FILE.exists()
    with open(HELM_VALUES_FILE, 'r') as f:
        return yaml.safe_load(f)


class TestScrapeJobsStructure:
    """viperine-scrape-jobs.yaml is valid and well-structured."""

    def test_yaml_loads_as_list(self, scrape_jobs):
        assert isinstance(scrape_jobs, list)

    def test_has_five_jobs(self, scrape_jobs):
        assert len(scrape_jobs) == 5

    def test_all_jobs_have_required_fields(self, scrape_jobs):
        for job in scrape_jobs:
            assert 'job_name' in job, f"Job missing job_name"
            assert 'static_configs' in job
            assert 'metrics_path' in job
            assert job['metrics_path'] == '/metrics'


class TestConsumerCoverage:
    """All 5 consumers are represented."""

    def test_expected_job_names(self, scrape_jobs):
        job_names = [j['job_name'] for j in scrape_jobs]
        expected = [
            'viperine_web_scraper',
            'viperine_summarizer',
            'viperine_summary_indexer',
            'viperine_index_aggregator',
            'viperine_dead_letter',
        ]
        for name in expected:
            assert name in job_names, f"Missing job: {name}"


class TestLabels:
    """All jobs have correct labels."""

    def test_all_have_pipeline_viperine(self, scrape_jobs):
        for job in scrape_jobs:
            labels = job['static_configs'][0].get('labels', {})
            assert labels.get('pipeline') == 'viperine', (
                f"Job {job['job_name']} missing pipeline=viperine"
            )

    def test_all_have_service_kafka_consumer(self, scrape_jobs):
        for job in scrape_jobs:
            labels = job['static_configs'][0].get('labels', {})
            assert labels.get('service') == 'kafka_consumer', (
                f"Job {job['job_name']} missing service=kafka_consumer"
            )

    def test_all_have_consumer_label(self, scrape_jobs):
        for job in scrape_jobs:
            labels = job['static_configs'][0].get('labels', {})
            assert 'consumer' in labels, (
                f"Job {job['job_name']} missing consumer label"
            )


class TestPortAlignment:
    """Scrape job ports match Helm values.yaml metricsPort."""

    def test_ports_match_helm_values(self, scrape_jobs, helm_values):
        # Build expected port map from Helm values
        consumers = helm_values.get('consumers', {})
        expected_ports = {
            'web_scraper': consumers['webScraper']['metricsPort'],
            'summarizer': consumers['summarizer']['metricsPort'],
            'summary_indexer': consumers['summaryIndexer']['metricsPort'],
            'index_aggregator': consumers['indexAggregator']['metricsPort'],
            'dead_letter': consumers['deadLetterConsumer']['metricsPort'],
        }

        for job in scrape_jobs:
            consumer_label = job['static_configs'][0]['labels']['consumer']
            target = job['static_configs'][0]['targets'][0]
            port = int(target.split(':')[-1])
            expected = expected_ports.get(consumer_label)
            assert port == expected, (
                f"Job {job['job_name']}: port {port} != Helm metricsPort {expected}"
            )


class TestTargetDNS:
    """Targets use correct k8s DNS format."""

    def test_targets_use_cluster_dns(self, scrape_jobs):
        for job in scrape_jobs:
            target = job['static_configs'][0]['targets'][0]
            assert '.consumers.svc.cluster.local:' in target, (
                f"Job {job['job_name']} target doesn't use cluster DNS: {target}"
            )
