"""
Azure Storage Metrics Collector

Background thread that periodically scans Azure Blob Storage containers
and exposes size/count metrics via Prometheus.  Also provides context managers
for tracking upload/download operations inline.

Metrics exposed:
- kafka_consumer_azure_blobs_total: Gauge (consumer_id, container)
- kafka_consumer_azure_container_storage_bytes: Gauge (consumer_id, container)
- kafka_consumer_azure_upload_total: Counter (consumer_id, container, result)
- kafka_consumer_azure_download_total: Counter (consumer_id, container, result)
- kafka_consumer_azure_upload_duration_seconds: Histogram (consumer_id, container)
- kafka_consumer_azure_download_duration_seconds: Histogram (consumer_id, container)

Usage:
    from monitoring.azure_metrics import AzureMetricsCollector

    collector = AzureMetricsCollector(
        consumer_id='index_aggregator_1',
        connection_string=conn_str,
        containers=['indices', 'session-backups'],
    )
    collector.start()

    # Wrap upload/download operations
    with collector.track_upload('indices'):
        blob_client.upload_blob(data)

    with collector.track_download('indices'):
        data = blob_client.download_blob().readall()

    collector.stop()
"""

import logging
import threading
import time
from contextlib import contextmanager
from typing import Optional, Sequence

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)


class AzureMetricsCollector:
    """
    Collects Azure Blob Storage metrics.

    Runs a background thread to periodically scan containers for blob count
    and total size. Provides context managers for timing individual operations.
    """

    def __init__(
        self,
        consumer_id: str,
        connection_string: Optional[str] = None,
        containers: Optional[Sequence[str]] = None,
        scan_interval: int = 300,
        namespace: str = "kafka_consumer",
    ):
        """
        Initialize Azure metrics collector.

        Args:
            consumer_id: Unique identifier for this consumer instance
            connection_string: Azure Storage connection string (None disables scanning)
            containers: List of container names to monitor
            scan_interval: Seconds between container stat scans (default: 300)
            namespace: Prometheus metric namespace prefix
        """
        self.consumer_id = consumer_id
        self.connection_string = connection_string
        self.containers = list(containers or [])
        self.scan_interval = scan_interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Gauges — updated by background scan
        self.blobs_total = Gauge(
            f'{namespace}_azure_blobs_total',
            'Number of blobs in Azure container',
            ['consumer_id', 'container']
        )

        self.storage_bytes = Gauge(
            f'{namespace}_azure_container_storage_bytes',
            'Total size in bytes of blobs in Azure container',
            ['consumer_id', 'container']
        )

        # Counters — incremented by track_upload/track_download
        self.upload_total = Counter(
            f'{namespace}_azure_upload_total',
            'Total Azure upload operations',
            ['consumer_id', 'container', 'result']
        )

        self.download_total = Counter(
            f'{namespace}_azure_download_total',
            'Total Azure download operations',
            ['consumer_id', 'container', 'result']
        )

        # Histograms — observed by track_upload/track_download
        self.upload_duration = Histogram(
            f'{namespace}_azure_upload_duration_seconds',
            'Time spent uploading to Azure Blob Storage',
            ['consumer_id', 'container'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
        )

        self.download_duration = Histogram(
            f'{namespace}_azure_download_duration_seconds',
            'Time spent downloading from Azure Blob Storage',
            ['consumer_id', 'container'],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0]
        )

    def start(self):
        """Start the background scanning thread."""
        if not self.connection_string or not self.containers:
            logger.info("AzureMetricsCollector: no connection_string or containers — scanning disabled")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._collection_loop,
            name=f"azure-metrics-{self.consumer_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info(f"AzureMetricsCollector started (interval={self.scan_interval}s, containers={self.containers})")

    def stop(self):
        """Stop the background scanning thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        logger.info("AzureMetricsCollector stopped")

    def _collection_loop(self):
        """Background loop that periodically scans container stats."""
        while not self._stop_event.is_set():
            try:
                self._collect_container_stats()
            except Exception as e:
                logger.warning(f"AzureMetricsCollector scan error: {e}")
            self._stop_event.wait(self.scan_interval)

    def _collect_container_stats(self):
        """Scan all monitored containers and update gauges."""
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            logger.debug("azure-storage-blob not installed — skipping scan")
            return

        client = BlobServiceClient.from_connection_string(self.connection_string)
        for container_name in self.containers:
            try:
                container_client = client.get_container_client(container_name)
                blob_count = 0
                total_bytes = 0
                for blob in container_client.list_blobs():
                    blob_count += 1
                    total_bytes += blob.size or 0

                self.blobs_total.labels(
                    consumer_id=self.consumer_id,
                    container=container_name,
                ).set(blob_count)

                self.storage_bytes.labels(
                    consumer_id=self.consumer_id,
                    container=container_name,
                ).set(total_bytes)

            except Exception as e:
                logger.warning(f"Error scanning container '{container_name}': {e}")

    @contextmanager
    def track_upload(self, container: str):
        """
        Context manager to track an Azure upload operation.

        Args:
            container: Container name being uploaded to
        """
        start = time.time()
        try:
            yield
            duration = time.time() - start
            self.upload_total.labels(
                consumer_id=self.consumer_id, container=container, result='success'
            ).inc()
            self.upload_duration.labels(
                consumer_id=self.consumer_id, container=container
            ).observe(duration)
        except Exception:
            duration = time.time() - start
            self.upload_total.labels(
                consumer_id=self.consumer_id, container=container, result='error'
            ).inc()
            self.upload_duration.labels(
                consumer_id=self.consumer_id, container=container
            ).observe(duration)
            raise

    @contextmanager
    def track_download(self, container: str):
        """
        Context manager to track an Azure download operation.

        Args:
            container: Container name being downloaded from
        """
        start = time.time()
        try:
            yield
            duration = time.time() - start
            self.download_total.labels(
                consumer_id=self.consumer_id, container=container, result='success'
            ).inc()
            self.download_duration.labels(
                consumer_id=self.consumer_id, container=container
            ).observe(duration)
        except Exception:
            duration = time.time() - start
            self.download_total.labels(
                consumer_id=self.consumer_id, container=container, result='error'
            ).inc()
            self.download_duration.labels(
                consumer_id=self.consumer_id, container=container
            ).observe(duration)
            raise
