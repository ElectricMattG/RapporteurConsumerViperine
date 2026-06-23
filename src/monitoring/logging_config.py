"""
Structured Logging Configuration

Configures JSON-formatted logging with standard fields for Elasticsearch/Kibana.

Standard log fields:
- timestamp: ISO 8601 format
- level: DEBUG, INFO, WARNING, ERROR, CRITICAL
- logger: Logger name
- message: Human-readable message
- consumer_id: Unique consumer instance ID
- session_id: Session identifier
- trace_id: Distributed tracing ID
- extra_fields: Additional context

Usage:
    from monitoring.logging_config import configure_logging

    configure_logging(
        format='json',
        level='INFO',
        consumer_id='web_scraper_pod_abc123',
        log_file='/var/log/consumers/web_scraper.log'
    )

    logger = logging.getLogger(__name__)
    logger.info(
        'Document scraped',
        extra={
            'document_id': 'doc_123',
            'url': 'https://example.com',
            'duration_ms': 234
        }
    )
"""

import logging
import sys
import uuid
from logging.handlers import RotatingFileHandler
from typing import Optional

from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """
    Custom JSON formatter with standard fields.

    Adds consumer_id, session_id, and other context to all log records.
    """

    def __init__(self, consumer_id: str, session_id: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consumer_id = consumer_id
        self.session_id = session_id

    def add_fields(self, log_record, record, message_dict):
        """Add custom fields to the log record."""
        super().add_fields(log_record, record, message_dict)

        # Add standard fields
        log_record['consumer_id'] = self.consumer_id

        # Dynamic correlation IDs — read from record attributes stamped by
        # install_correlation_record_factory() so every log line carries the
        # IDs bound for the current message, not the static value from __init__.
        log_record['session_id'] = getattr(record, 'session_id', '-')
        log_record['request_id'] = getattr(record, 'request_id', '-')

        # Add trace_id if not present
        if 'trace_id' not in log_record:
            log_record['trace_id'] = str(uuid.uuid4())[:8]

        # Rename fields for consistency
        if 'levelname' in log_record:
            log_record['level'] = log_record.pop('levelname')
        if 'name' in log_record:
            log_record['logger'] = log_record.pop('name')


def configure_logging(
    format: str = 'json',
    level: str = 'INFO',
    consumer_id: Optional[str] = None,
    session_id: Optional[str] = None,
    log_file: Optional[str] = None,
    max_bytes: int = 100 * 1024 * 1024,  # 100 MB
    backup_count: int = 5
):
    """
    Configure structured logging for the consumer.

    Args:
        format: 'json' or 'text' (default: json)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        consumer_id: Unique consumer instance identifier
        session_id: Session identifier (optional)
        log_file: Path to log file (optional, uses console if not specified)
        max_bytes: Maximum log file size before rotation (default: 100MB)
        backup_count: Number of backup files to keep (default: 5)
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers
    root_logger.handlers.clear()

    # Create formatter
    if format == 'json':
        if not consumer_id:
            consumer_id = f"unknown_{uuid.uuid4().hex[:8]}"

        formatter = CustomJsonFormatter(
            consumer_id=consumer_id,
            session_id=session_id,
            fmt='%(timestamp)s %(level)s %(logger)s %(message)s',
            rename_fields={'asctime': 'timestamp'}
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - session_id=%(session_id)s request_id=%(request_id)s - %(message)s'
        )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File handler (with rotation)
    if log_file:
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    logging.info(
        f"Logging configured - Format: {format}, Level: {level}, "
        f"Consumer ID: {consumer_id}, Log file: {log_file or 'console only'}"
    )


def generate_trace_id() -> str:
    """Generate a short trace ID for distributed tracing."""
    return str(uuid.uuid4())[:8]
