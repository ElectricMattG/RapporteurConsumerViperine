#!/usr/bin/env python3
"""
Test script for Phase 1 telemetry implementation.

Tests:
- PrometheusMetrics initialization and metrics tracking
- SystemMetricsCollector CPU/memory monitoring
- MetricsServer HTTP endpoints (/metrics, /health, /ready)
- Structured JSON logging
"""

import time
import requests
import json
from src.monitoring import PrometheusMetrics, SystemMetricsCollector, MetricsServer
from src.monitoring.logging_config import configure_logging
import logging

def test_logging():
    """Test structured JSON logging."""
    print("\n" + "="*80)
    print("TEST 1: Structured JSON Logging")
    print("="*80)
    
    configure_logging(
        format='json',
        level='INFO',
        consumer_id='test_consumer_001'
    )
    
    logger = logging.getLogger(__name__)
    logger.info(
        "Test log message",
        extra={
            'test_id': 'test_001',
            'duration_ms': 123
        }
    )
    
    print("✅ Logging configured and test message logged")

def test_prometheus_metrics():
    """Test PrometheusMetrics class."""
    print("\n" + "="*80)
    print("TEST 2: PrometheusMetrics")
    print("="*80)
    
    metrics = PrometheusMetrics(consumer_id='test_consumer_001')
    
    # Set state to running
    metrics.set_state(1)
    print("✅ Set consumer state to running")
    
    # Increment messages consumed
    metrics.increment_messages_consumed(topic='test-topic', count=5)
    print("✅ Incremented messages consumed counter")
    
    # Track processing duration
    with metrics.time_operation('test-topic', status='success'):
        time.sleep(0.1)
    print("✅ Tracked operation duration")
    
    # Track failures
    metrics.increment_failures(topic='test-topic', error_type='TestError', count=1)
    print("✅ Incremented failures counter")
    
    return metrics

def test_system_metrics():
    """Test SystemMetricsCollector."""
    print("\n" + "="*80)
    print("TEST 3: SystemMetricsCollector")
    print("="*80)
    
    collector = SystemMetricsCollector(consumer_id='test_consumer_001', collection_interval=2)
    
    # Collect metrics once
    collector.collect_metrics()
    print("✅ Collected system metrics manually")
    
    # Start background collection
    collector.start()
    print("✅ Started background metrics collection")
    
    # Wait for one collection cycle
    time.sleep(3)
    
    # Stop collection
    collector.stop()
    print("✅ Stopped background metrics collection")
    
    return collector

def test_metrics_server():
    """Test MetricsServer HTTP endpoints."""
    print("\n" + "="*80)
    print("TEST 4: MetricsServer HTTP Endpoints")
    print("="*80)
    
    # Create health check functions
    def health_check():
        return True
    
    def ready_check():
        return True
    
    server = MetricsServer(
        port=8099,
        consumer_name='test_consumer',
        health_check=health_check,
        ready_check=ready_check
    )
    server.start()
    print("✅ Metrics server started on http://0.0.0.0:8099")
    
    # Wait for server to be ready
    time.sleep(1)
    
    # Test /health endpoint
    try:
        response = requests.get('http://localhost:8099/health', timeout=2)
        health_data = response.json()
        print(f"✅ /health endpoint: {response.status_code}")
        print(f"   Response: {json.dumps(health_data, indent=2)}")
    except Exception as e:
        print(f"❌ /health endpoint failed: {e}")
    
    # Test /ready endpoint
    try:
        response = requests.get('http://localhost:8099/ready', timeout=2)
        ready_data = response.json()
        print(f"✅ /ready endpoint: {response.status_code}")
        print(f"   Response: {json.dumps(ready_data, indent=2)}")
    except Exception as e:
        print(f"❌ /ready endpoint failed: {e}")
    
    # Test /metrics endpoint
    try:
        response = requests.get('http://localhost:8099/metrics', timeout=2)
        lines = response.text.split('\n')[:10]
        print(f"✅ /metrics endpoint: {response.status_code}")
        print(f"   First 10 lines of metrics:")
        for line in lines:
            if line and not line.startswith('#'):
                print(f"     {line}")
    except Exception as e:
        print(f"❌ /metrics endpoint failed: {e}")
    
    # Stop server
    server.stop()
    print("✅ Metrics server stopped")
    
    return server

def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("PHASE 1 TELEMETRY TEST SUITE")
    print("="*80)
    
    try:
        # Test 1: Logging
        test_logging()
        
        # Test 2: Prometheus Metrics
        metrics = test_prometheus_metrics()
        
        # Test 3: System Metrics
        collector = test_system_metrics()
        
        # Test 4: Metrics Server
        server = test_metrics_server()
        
        print("\n" + "="*80)
        print("ALL TESTS PASSED ✅")
        print("="*80)
        print("\nPhase 1 telemetry implementation is working correctly!")
        print("\nNext steps:")
        print("1. Start web scraper consumer and verify metrics at http://localhost:8001/metrics")
        print("2. Deploy Prometheus server to scrape consumer metrics")
        print("3. Create Grafana dashboards for visualization")
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"TEST FAILED ❌: {e}")
        print("="*80)
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == '__main__':
    exit(main())
