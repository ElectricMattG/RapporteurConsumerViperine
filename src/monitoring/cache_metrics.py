"""
Cache Metrics Collector

Monitors local cache directories for file count, size, and age. Provides
hit/miss/eviction counters that consumer code increments via the public API.

Metrics
-------
Gauges (updated by background scan):
    kafka_consumer_cache_files_total{consumer_id, cache}
    kafka_consumer_cache_size_bytes{consumer_id, cache}
    kafka_consumer_cache_age_seconds{consumer_id, cache}   — oldest entry

Counters (incremented by consumer code):
    kafka_consumer_cache_hit_total{consumer_id, cache}
    kafka_consumer_cache_miss_total{consumer_id, cache}
    kafka_consumer_cache_eviction_total{consumer_id, cache}

Histograms:
    kafka_consumer_cache_cleanup_duration_seconds{consumer_id, cache}

Usage::

    from monitoring.cache_metrics import CacheMetricsCollector

    collector = CacheMetricsCollector(
        consumer_id='web_scraper_1234',
        directories={'scrapes': '/app/cache/web_scrapes'},
        scan_interval=60,
    )
    collector.start()

    # In consumer hot-path:
    if cached:
        collector.record_hit('scrapes')
    else:
        collector.record_miss('scrapes')

    # After cleanup:
    with collector.time_cleanup('scrapes'):
        shutil.rmtree(old_dir)
        collector.record_eviction('scrapes')
"""

from __future__ import annotations

import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator

from prometheus_client import Counter, Gauge, Histogram

_NS = "kafka_consumer"

# ---------------------------------------------------------------------------
# Module-level metrics (registered once)
# ---------------------------------------------------------------------------

_cache_files = Gauge(
    f"{_NS}_cache_files_total",
    "Number of files in cache directory",
    ["consumer_id", "cache"],
)

_cache_size_bytes = Gauge(
    f"{_NS}_cache_size_bytes",
    "Total size of cache directory in bytes",
    ["consumer_id", "cache"],
)

_cache_age_seconds = Gauge(
    f"{_NS}_cache_age_seconds",
    "Age of the oldest file in the cache directory in seconds",
    ["consumer_id", "cache"],
)

_cache_hit_total = Counter(
    f"{_NS}_cache_hit_total",
    "Total cache hits",
    ["consumer_id", "cache"],
)

_cache_miss_total = Counter(
    f"{_NS}_cache_miss_total",
    "Total cache misses",
    ["consumer_id", "cache"],
)

_cache_eviction_total = Counter(
    f"{_NS}_cache_eviction_total",
    "Total cache evictions",
    ["consumer_id", "cache"],
)

_cache_cleanup_duration = Histogram(
    f"{_NS}_cache_cleanup_duration_seconds",
    "Time spent on cache cleanup operations",
    ["consumer_id", "cache"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0],
)


class CacheMetricsCollector:
    """Periodically scans cache directories and exposes Prometheus metrics."""

    def __init__(
        self,
        consumer_id: str,
        directories: Dict[str, str] | None = None,
        scan_interval: int = 60,
        namespace: str = _NS,
    ) -> None:
        self.consumer_id = consumer_id
        self.directories: Dict[str, Path] = {
            name: Path(path)
            for name, path in (directories or {}).items()
        }
        self.scan_interval = scan_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the background scanning thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"cache-metrics-{self.consumer_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the background thread to stop."""
        self._stop_event.set()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.collect_metrics()
            self._stop_event.wait(self.scan_interval)

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    def collect_metrics(self) -> None:
        """Scan all registered directories and update gauge metrics."""
        now = time.time()
        for cache_name, path in self.directories.items():
            if not path.exists():
                _cache_files.labels(self.consumer_id, cache_name).set(0)
                _cache_size_bytes.labels(self.consumer_id, cache_name).set(0)
                _cache_age_seconds.labels(self.consumer_id, cache_name).set(0)
                continue
            try:
                files = list(path.rglob("*"))
                file_list = [f for f in files if f.is_file()]
                total_size = sum(f.stat().st_size for f in file_list)
                oldest_age = 0.0
                if file_list:
                    oldest_mtime = min(f.stat().st_mtime for f in file_list)
                    oldest_age = now - oldest_mtime
                _cache_files.labels(self.consumer_id, cache_name).set(len(file_list))
                _cache_size_bytes.labels(self.consumer_id, cache_name).set(total_size)
                _cache_age_seconds.labels(self.consumer_id, cache_name).set(oldest_age)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Consumer-side helpers
    # ------------------------------------------------------------------

    def record_hit(self, cache: str) -> None:
        _cache_hit_total.labels(self.consumer_id, cache).inc()

    def record_miss(self, cache: str) -> None:
        _cache_miss_total.labels(self.consumer_id, cache).inc()

    def record_eviction(self, cache: str, count: int = 1) -> None:
        _cache_eviction_total.labels(self.consumer_id, cache).inc(count)

    @contextmanager
    def time_cleanup(self, cache: str) -> Generator[None, None, None]:
        start = time.time()
        try:
            yield
        finally:
            _cache_cleanup_duration.labels(self.consumer_id, cache).observe(
                time.time() - start
            )
