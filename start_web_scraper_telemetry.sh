#!/bin/bash

# Start Web Scraper Consumer with Telemetry for Testing
# This starts only the web scraper consumer to test Phase 1 telemetry

set -e

echo "🚀 Starting Web Scraper Consumer with Telemetry..."
echo "=================================================="

# Create logs directory if it doesn't exist
mkdir -p logs

# Check if Kafka is running
echo "🔍 Checking Kafka connection..."
if ! nc -z localhost 9092 &> /dev/null; then
    echo "⚠️  WARNING: Kafka not reachable at localhost:9092"
    echo "    The consumer will try to connect but may fail."
    echo "    To start Kafka: docker-compose -f docker-compose.kafka.yml up -d"
    echo ""
fi

# Check if monitoring stack is running
echo "🔍 Checking monitoring stack..."
if ! nc -z localhost 9090 &> /dev/null; then
    echo "⚠️  WARNING: Prometheus not reachable at localhost:9090"
    echo "    Starting monitoring stack..."
    docker-compose -f docker-compose.monitoring.yml up -d
    sleep 5
else
    echo "✅ Prometheus is running at http://localhost:9090"
fi

if ! nc -z localhost 3000 &> /dev/null; then
    echo "⚠️  WARNING: Grafana not reachable at localhost:3000"
else
    echo "✅ Grafana is running at http://localhost:3000"
fi

echo ""
echo "📡 Starting Web Scraper Consumer..."
echo "    Metrics will be exposed at: http://localhost:8001/metrics"
echo "    Health check: http://localhost:8001/health"
echo "    Ready check: http://localhost:8001/ready"
echo ""
echo "📊 Monitoring URLs:"
echo "    Prometheus: http://localhost:9090"
echo "    Grafana: http://localhost:3000 (admin/admin)"
echo "    Consumer Metrics: http://localhost:8001/metrics"
echo ""
echo "Press Ctrl+C to stop the consumer..."
echo ""

# Start web scraper with telemetry enabled
PYTHONDONTWRITEBYTECODE=1 python3 src/consumers/kafka_web_scraper.py
