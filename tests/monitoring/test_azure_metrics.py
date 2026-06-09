"""
Tests for AzureMetricsCollector.

Validates:
- Gauge updates from mocked BlobServiceClient scan
- track_upload() increments success counter + observes duration
- track_upload() increments error counter on exception
- track_download() increments success counter + observes duration
- track_download() increments error counter on exception
- start() is no-op when connection_string is None
"""
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from src.monitoring.azure_metrics import AzureMetricsCollector


@pytest.fixture
def ns():
    return f"test_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def collector(ns):
    return AzureMetricsCollector(
        consumer_id='test_azure',
        connection_string='DefaultEndpointsProtocol=https;AccountName=fake;AccountKey=fake==;',
        containers=['indices', 'session-backups'],
        scan_interval=60,
        namespace=ns,
    )


@pytest.fixture
def collector_no_conn(ns):
    return AzureMetricsCollector(
        consumer_id='test_azure',
        connection_string=None,
        containers=['indices'],
        namespace=ns,
    )


class TestTrackUpload:

    def test_success_increments_counter(self, collector):
        with collector.track_upload('indices'):
            time.sleep(0.001)

        count = collector.upload_total.labels(
            consumer_id='test_azure', container='indices', result='success'
        )._value.get()
        assert count == 1.0

    def test_success_observes_duration(self, collector):
        with collector.track_upload('indices'):
            time.sleep(0.01)

        total = collector.upload_duration.labels(
            consumer_id='test_azure', container='indices'
        )._sum.get()
        assert total >= 0.01

    def test_error_increments_error_counter(self, collector):
        with pytest.raises(ValueError):
            with collector.track_upload('indices'):
                raise ValueError("upload failed")

        count = collector.upload_total.labels(
            consumer_id='test_azure', container='indices', result='error'
        )._value.get()
        assert count == 1.0

    def test_error_still_observes_duration(self, collector):
        with pytest.raises(RuntimeError):
            with collector.track_upload('session-backups'):
                time.sleep(0.005)
                raise RuntimeError("boom")

        total = collector.upload_duration.labels(
            consumer_id='test_azure', container='session-backups'
        )._sum.get()
        assert total >= 0.005


class TestTrackDownload:

    def test_success_increments_counter(self, collector):
        with collector.track_download('indices'):
            pass

        count = collector.download_total.labels(
            consumer_id='test_azure', container='indices', result='success'
        )._value.get()
        assert count == 1.0

    def test_success_observes_duration(self, collector):
        with collector.track_download('indices'):
            time.sleep(0.01)

        total = collector.download_duration.labels(
            consumer_id='test_azure', container='indices'
        )._sum.get()
        assert total >= 0.01

    def test_error_increments_error_counter(self, collector):
        with pytest.raises(IOError):
            with collector.track_download('indices'):
                raise IOError("network error")

        count = collector.download_total.labels(
            consumer_id='test_azure', container='indices', result='error'
        )._value.get()
        assert count == 1.0


class TestContainerScan:

    def test_collect_updates_gauges(self, collector):
        """Mock BlobServiceClient and verify gauge updates."""
        mock_blob1 = MagicMock()
        mock_blob1.size = 1024
        mock_blob2 = MagicMock()
        mock_blob2.size = 2048

        mock_container = MagicMock()
        mock_container.list_blobs.return_value = [mock_blob1, mock_blob2]

        mock_client = MagicMock()
        mock_client.get_container_client.return_value = mock_container

        with patch('src.monitoring.azure_metrics.AzureMetricsCollector._collect_container_stats') as mock_collect:
            # Call the real implementation manually with our mocked client
            pass

        # Directly test by calling the internal method with mocked import
        with patch.dict('sys.modules', {'azure': MagicMock(), 'azure.storage': MagicMock(), 'azure.storage.blob': MagicMock()}):
            with patch('azure.storage.blob.BlobServiceClient') as MockBSC:
                MockBSC.from_connection_string.return_value = mock_client
                collector._collect_container_stats()

        blob_count = collector.blobs_total.labels(
            consumer_id='test_azure', container='indices'
        )._value.get()
        assert blob_count == 2

        total_bytes = collector.storage_bytes.labels(
            consumer_id='test_azure', container='indices'
        )._value.get()
        assert total_bytes == 3072

    def test_scan_handles_missing_container_gracefully(self, collector):
        """Container that raises should not crash the scan."""
        mock_client = MagicMock()
        mock_client.get_container_client.side_effect = Exception("container not found")

        with patch.dict('sys.modules', {'azure': MagicMock(), 'azure.storage': MagicMock(), 'azure.storage.blob': MagicMock()}):
            with patch('azure.storage.blob.BlobServiceClient') as MockBSC:
                MockBSC.from_connection_string.return_value = mock_client
                # Should not raise
                collector._collect_container_stats()


class TestStartStop:

    def test_start_noop_without_connection_string(self, collector_no_conn):
        """start() should not create a thread if connection_string is None."""
        collector_no_conn.start()
        assert collector_no_conn._thread is None

    def test_start_noop_without_containers(self, ns):
        c = AzureMetricsCollector(
            consumer_id='test',
            connection_string='fake',
            containers=[],
            namespace=ns,
        )
        c.start()
        assert c._thread is None

    def test_stop_is_safe_without_start(self, collector):
        """stop() without start() should not raise."""
        collector.stop()
