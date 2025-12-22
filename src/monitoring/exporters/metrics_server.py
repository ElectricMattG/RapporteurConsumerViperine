"""
HTTP Server for Prometheus Metrics Export

Serves /metrics, /health, and /ready endpoints for each consumer.

Endpoints:
- GET /metrics: Prometheus text format metrics
- GET /health: Returns 200 if consumer is alive
- GET /ready: Returns 200 if consumer is ready to process messages

Usage:
    from monitoring.exporters.metrics_server import MetricsServer

    server = MetricsServer(port=8001, health_check=my_health_func)
    server.start()

    # Run consumer

    server.stop()
"""

import json
import logging
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import Callable, Optional

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for metrics endpoints."""

    def __init__(self, consumer_name: str,
                 health_check: Optional[Callable[[], bool]],
                 ready_check: Optional[Callable[[], bool]], *args, **kwargs):
        self.consumer_name = consumer_name
        self.health_check = health_check
        self.ready_check = ready_check
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/metrics':
            self.serve_metrics()
        elif self.path == '/health':
            self.serve_health()
        elif self.path == '/ready':
            self.serve_ready()
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def serve_metrics(self):
        """Serve Prometheus metrics."""
        try:
            metrics = generate_latest()
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(metrics)
        except Exception as e:
            logger.error(f"Error serving metrics: {e}", exc_info=True)
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'Internal Server Error')

    def serve_health(self):
        """Serve health check (liveness probe) with comprehensive information."""
        try:
            request_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat() + 'Z'

            is_healthy = self.health_check is None or self.health_check()

            response_data = {
                'consumer_name': self.consumer_name,
                'request_id': request_id,
                'timestamp': timestamp,
                'status': 'healthy' if is_healthy else 'unhealthy',
                'code': 200 if is_healthy else 503
            }

            response_json = json.dumps(response_data, indent=2)

            if is_healthy:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response_json.encode('utf-8'))
            else:
                self.send_response(503)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response_json.encode('utf-8'))
        except Exception as e:
            logger.error(f"Error in health check: {e}", exc_info=True)
            error_response = {
                'consumer_name': self.consumer_name,
                'request_id': str(uuid.uuid4()),
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'status': 'error',
                'code': 500,
                'error': str(e)
            }
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def serve_ready(self):
        """Serve readiness check (readiness probe) with comprehensive information."""
        try:
            request_id = str(uuid.uuid4())
            timestamp = datetime.utcnow().isoformat() + 'Z'

            is_ready = self.ready_check is None or self.ready_check()

            response_data = {
                'consumer_name': self.consumer_name,
                'request_id': request_id,
                'timestamp': timestamp,
                'status': 'ready' if is_ready else 'not_ready',
                'code': 200 if is_ready else 503
            }

            response_json = json.dumps(response_data, indent=2)

            if is_ready:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response_json.encode('utf-8'))
            else:
                self.send_response(503)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(response_json.encode('utf-8'))
        except Exception as e:
            logger.error(f"Error in readiness check: {e}", exc_info=True)
            error_response = {
                'consumer_name': self.consumer_name,
                'request_id': str(uuid.uuid4()),
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'status': 'error',
                'code': 500,
                'error': str(e)
            }
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(error_response, indent=2).encode('utf-8'))

    def log_message(self, format, *args):
        """Override to use Python logging instead of stderr."""
        logger.debug(f"{self.address_string()} - {format % args}")


class MetricsServer:
    """
    HTTP server for exposing Prometheus metrics and health endpoints.

    Runs in a background thread to avoid blocking the main consumer loop.
    """

    def __init__(self, port: int,
                 consumer_name: str = 'unknown_consumer',
                 health_check: Optional[Callable[[], bool]] = None,
                 ready_check: Optional[Callable[[], bool]] = None):
        """
        Initialize metrics server.

        Args:
            port: Port to bind to (e.g., 8001 for web scraper)
            consumer_name: Name of the consumer (e.g., 'web_scraper', 'chunk_embedder')
            health_check: Optional function that returns True if healthy
            ready_check: Optional function that returns True if ready
        """
        self.port = port
        self.consumer_name = consumer_name
        self.health_check = health_check
        self.ready_check = ready_check
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[Thread] = None

        logger.info(f"Initialized metrics server for {consumer_name} on port {port}")

    def _handler_factory(self, *args, **kwargs):
        """Factory to create handler with consumer name and health/ready checks."""
        return MetricsHandler(self.consumer_name, self.health_check, self.ready_check, *args, **kwargs)

    def start(self):
        """Start the HTTP server in a background thread."""
        if self._server is not None:
            logger.warning("Metrics server already running")
            return

        try:
            self._server = HTTPServer(('0.0.0.0', self.port), self._handler_factory)
            self._thread = Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            logger.info(f"Metrics server started on http://0.0.0.0:{self.port}")
            logger.info(f"Endpoints: /metrics, /health, /ready")
        except Exception as e:
            logger.error(f"Failed to start metrics server on port {self.port}: {e}", exc_info=True)
            raise

    def stop(self):
        """Stop the HTTP server."""
        if self._server is None:
            return

        logger.info("Stopping metrics server...")
        self._server.shutdown()

        if self._thread:
            self._thread.join(timeout=5)
            if self._thread.is_alive():
                logger.warning("Metrics server thread did not stop cleanly")

        self._server = None
        self._thread = None
        logger.info("Metrics server stopped")
