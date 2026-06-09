"""
System Metrics Collector

Monitors CPU and memory usage for the consumer process and system.

Metrics exposed:
- process_cpu_percent: Gauge (CPU % used by process)
- process_memory_percent: Gauge (Memory % used by process)
- process_memory_rss_bytes: Gauge (Resident memory size)
- process_memory_vms_bytes: Gauge (Virtual memory size)
- system_cpu_percent: Gauge (Overall system CPU %)
- system_memory_percent: Gauge (Overall system memory %)

Usage:
    from monitoring.system_metrics import SystemMetricsCollector

    collector = SystemMetricsCollector(consumer_id='web_scraper_pod_abc123')
    collector.start()  # Starts background thread

    # Metrics collected every 60 seconds

    collector.stop()
"""

import logging
import shutil
import threading
import time
from typing import Optional

import psutil
from prometheus_client import Gauge

logger = logging.getLogger(__name__)


class SystemMetricsCollector:
    """
    Collects system and process-level metrics using psutil.

    Runs in a background thread to periodically update Prometheus gauges.
    """

    def __init__(self, consumer_id: str, collection_interval: int = 60, namespace: str = "kafka_consumer", cache_mount_path: str = '/app/cache'):
        """
        Initialize system metrics collector.

        Args:
            consumer_id: Unique identifier for this consumer instance
            collection_interval: Seconds between metric collections (default: 60)
            namespace: Metric namespace prefix
            cache_mount_path: Path to monitor for disk usage (default: /app/cache)
        """
        self.consumer_id = consumer_id
        self.collection_interval = collection_interval
        self.namespace = namespace
        self.cache_mount_path = cache_mount_path
        self._running = False
        self._thread: Optional[threading.Thread] = None

        # Get process handle
        self.process = psutil.Process()

        # Process metrics
        self.process_cpu_percent = Gauge(
            f'{namespace}_process_cpu_percent',
            'CPU percentage used by this process',
            ['consumer_id']
        )

        self.process_memory_percent = Gauge(
            f'{namespace}_process_memory_percent',
            'Memory percentage used by this process',
            ['consumer_id']
        )

        self.process_memory_rss_bytes = Gauge(
            f'{namespace}_process_memory_rss_bytes',
            'Resident memory size in bytes',
            ['consumer_id']
        )

        self.process_memory_vms_bytes = Gauge(
            f'{namespace}_process_memory_vms_bytes',
            'Virtual memory size in bytes',
            ['consumer_id']
        )

        # System metrics
        self.system_cpu_percent = Gauge(
            f'{namespace}_system_cpu_percent',
            'Overall system CPU percentage',
            ['consumer_id']
        )

        self.system_memory_percent = Gauge(
            f'{namespace}_system_memory_percent',
            'Overall system memory percentage',
            ['consumer_id']
        )

        # ── Resource metrics (Phase 3) ───────────────────────────────────────
        self.disk_usage_bytes = Gauge(
            f'{namespace}_disk_usage_bytes',
            'Disk space used in bytes on cache mount',
            ['consumer_id', 'mount_point']
        )

        self.disk_available_bytes = Gauge(
            f'{namespace}_disk_available_bytes',
            'Disk space available in bytes on cache mount',
            ['consumer_id', 'mount_point']
        )

        self.open_file_descriptors = Gauge(
            f'{namespace}_open_file_descriptors',
            'Number of open file descriptors for this process',
            ['consumer_id']
        )

        self.thread_count = Gauge(
            f'{namespace}_thread_count',
            'Number of threads in this process',
            ['consumer_id']
        )

        logger.info(f"Initialized system metrics collector for consumer: {consumer_id}")

    def collect_metrics(self):
        """Collect and update all system metrics."""
        try:
            # Process CPU (call with interval for accurate measurement)
            process_cpu = self.process.cpu_percent(interval=1)
            self.process_cpu_percent.labels(consumer_id=self.consumer_id).set(process_cpu)

            # Process memory
            mem_info = self.process.memory_info()
            mem_percent = self.process.memory_percent()

            self.process_memory_percent.labels(consumer_id=self.consumer_id).set(mem_percent)
            self.process_memory_rss_bytes.labels(consumer_id=self.consumer_id).set(mem_info.rss)
            self.process_memory_vms_bytes.labels(consumer_id=self.consumer_id).set(mem_info.vms)

            # System metrics
            system_cpu = psutil.cpu_percent(interval=1)
            system_mem = psutil.virtual_memory().percent

            self.system_cpu_percent.labels(consumer_id=self.consumer_id).set(system_cpu)
            self.system_memory_percent.labels(consumer_id=self.consumer_id).set(system_mem)

            # instrumentation - disk_usage_bytes
            # instrumentation - disk_available_bytes
            try:
                disk = shutil.disk_usage(self.cache_mount_path)
                self.disk_usage_bytes.labels(
                    consumer_id=self.consumer_id, mount_point=self.cache_mount_path
                ).set(disk.used)
                self.disk_available_bytes.labels(
                    consumer_id=self.consumer_id, mount_point=self.cache_mount_path
                ).set(disk.free)
            except (OSError, FileNotFoundError):
                pass  # mount not available (e.g. local dev without PVC)

            # instrumentation - open_file_descriptors
            self.open_file_descriptors.labels(
                consumer_id=self.consumer_id
            ).set(self.process.num_fds())

            # instrumentation - thread_count
            self.thread_count.labels(
                consumer_id=self.consumer_id
            ).set(self.process.num_threads())

            logger.debug(
                f"System metrics collected - Process CPU: {process_cpu:.2f}%, "
                f"Process Memory: {mem_percent:.2f}%, System CPU: {system_cpu:.2f}%, "
                f"System Memory: {system_mem:.2f}%"
            )

        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}", exc_info=True)

    def _collection_loop(self):
        """Background thread loop for periodic metric collection."""
        logger.info(f"System metrics collection started (interval: {self.collection_interval}s)")

        while self._running:
            try:
                self.collect_metrics()
            except Exception as e:
                logger.error(f"Error in metrics collection loop: {e}", exc_info=True)

            # Sleep in small increments to allow faster shutdown
            for _ in range(self.collection_interval):
                if not self._running:
                    break
                time.sleep(1)

        logger.info("System metrics collection stopped")

    def start(self):
        """Start the metrics collection background thread."""
        if self._running:
            logger.warning("System metrics collector already running")
            return

        self._running = True
        self._thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._thread.start()
        logger.info("System metrics collector started")

    def stop(self):
        """Stop the metrics collection background thread."""
        if not self._running:
            return

        logger.info("Stopping system metrics collector...")
        self._running = False

        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("System metrics collector thread did not stop cleanly")

        logger.info("System metrics collector stopped")
