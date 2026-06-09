"""
Tests for Phase 3 resource metrics (disk usage, file descriptors, thread count).

Validates:
- disk_usage_bytes and disk_available_bytes are set after collection
- open_file_descriptors is a positive integer
- thread_count is >= 1
- Non-existent mount path does not crash collection
- cache_mount_path parameter is respected
"""
import uuid

import pytest

from src.monitoring.system_metrics import SystemMetricsCollector


@pytest.fixture
def collector(tmp_path):
    """Create a SystemMetricsCollector pointing at a real tmp directory."""
    ns = f"test_{uuid.uuid4().hex[:8]}"
    return SystemMetricsCollector(
        consumer_id='test_system',
        collection_interval=60,
        namespace=ns,
        cache_mount_path=str(tmp_path),
    )


@pytest.fixture
def collector_bad_path():
    """Collector with a non-existent mount path."""
    ns = f"test_{uuid.uuid4().hex[:8]}"
    return SystemMetricsCollector(
        consumer_id='test_system_bad',
        collection_interval=60,
        namespace=ns,
        cache_mount_path='/nonexistent/path/that/does/not/exist',
    )


class TestDiskMetrics:
    """Disk usage and availability gauges are populated."""

    def test_disk_usage_positive(self, collector, tmp_path):
        collector.collect_metrics()

        value = collector.disk_usage_bytes.labels(
            consumer_id='test_system', mount_point=str(tmp_path)
        )._value.get()
        assert value > 0

    def test_disk_available_positive(self, collector, tmp_path):
        collector.collect_metrics()

        value = collector.disk_available_bytes.labels(
            consumer_id='test_system', mount_point=str(tmp_path)
        )._value.get()
        assert value > 0

    def test_nonexistent_path_does_not_crash(self, collector_bad_path):
        # Should not raise — gracefully skips
        collector_bad_path.collect_metrics()

    def test_disk_values_sum_reasonable(self, collector, tmp_path):
        collector.collect_metrics()

        used = collector.disk_usage_bytes.labels(
            consumer_id='test_system', mount_point=str(tmp_path)
        )._value.get()
        free = collector.disk_available_bytes.labels(
            consumer_id='test_system', mount_point=str(tmp_path)
        )._value.get()
        # used + free should be less than or equal to total (some reserved blocks)
        assert used + free > 0


class TestFileDescriptors:
    """open_file_descriptors gauge is populated."""

    def test_fds_positive(self, collector):
        collector.collect_metrics()

        value = collector.open_file_descriptors.labels(
            consumer_id='test_system'
        )._value.get()
        assert value > 0

    def test_fds_is_integer(self, collector):
        collector.collect_metrics()

        value = collector.open_file_descriptors.labels(
            consumer_id='test_system'
        )._value.get()
        assert value == int(value)


class TestThreadCount:
    """thread_count gauge is populated."""

    def test_threads_at_least_one(self, collector):
        collector.collect_metrics()

        value = collector.thread_count.labels(
            consumer_id='test_system'
        )._value.get()
        assert value >= 1

    def test_threads_is_integer(self, collector):
        collector.collect_metrics()

        value = collector.thread_count.labels(
            consumer_id='test_system'
        )._value.get()
        assert value == int(value)


class TestCacheMountPath:
    """cache_mount_path parameter is stored and used."""

    def test_mount_path_stored(self, collector, tmp_path):
        assert collector.cache_mount_path == str(tmp_path)

    def test_default_mount_path(self):
        ns = f"test_{uuid.uuid4().hex[:8]}"
        c = SystemMetricsCollector(consumer_id='test_default', namespace=ns)
        assert c.cache_mount_path == '/app/cache'
