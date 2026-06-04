"""
Monitoring Module for Kafka Consumers

Provides telemetry and observability through:
- Prometheus metrics (PrometheusMetrics)
- System resource monitoring (SystemMetricsCollector)
- HTTP metrics server (MetricsServer)
- Structured JSON logging (configure_logging)
- Kafka consumer metrics (coming soon)
- Cache monitoring (coming soon)
- Azure storage monitoring (coming soon)

Usage:
    from monitoring import PrometheusMetrics, SystemMetricsCollector, MetricsServer
    from monitoring.logging_config import configure_logging

    # Configure logging
    configure_logging(format='json', level='INFO', consumer_id='my_consumer')

    # Initialize metrics
    metrics = PrometheusMetrics(consumer_id='my_consumer')
    system_metrics = SystemMetricsCollector(consumer_id='my_consumer')
    server = MetricsServer(port=8001)

    # Start services
    system_metrics.start()
    server.start()
"""

from .cache_metrics import CacheMetricsCollector
from .exporters.metrics_server import MetricsServer
from .kafka_metrics import KafkaMetricsWrapper
from .logging_config import configure_logging, generate_trace_id
from .prometheus_metrics import PrometheusMetrics
from .system_metrics import SystemMetricsCollector

__all__ = [
    'CacheMetricsCollector',
    'PrometheusMetrics',
    'SystemMetricsCollector',
    'KafkaMetricsWrapper',
    'MetricsServer',
    'configure_logging',
    'generate_trace_id',
]
