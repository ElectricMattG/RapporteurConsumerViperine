# TELEMETRY & OBSERVABILITY - TODO IMPLEMENTATION CHECKLIST

## Phase 1: Core Infrastructure (Week 1)

### 1.1 Create Monitoring Module Structure
```
src/monitoring/
├── __init__.py                 # TODO: Create module
├── prometheus_metrics.py       # TODO: Base metrics classes
├── system_metrics.py          # TODO: CPU, memory collectors
├── kafka_metrics.py           # TODO: Kafka-specific metrics
├── cache_metrics.py           # TODO: Local cache monitoring
├── azure_metrics.py           # TODO: Azure storage monitoring
├── logging_config.py          # TODO: Structured logging setup
└── exporters/
    ├── __init__.py            # TODO: Create sub-module
    └── metrics_server.py      # TODO: HTTP server for /metrics
```

### 1.2 Add Dependencies
- [ ] Update requirements.txt:
  ```
  prometheus-client==0.19.0
  psutil==5.9.6
  python-json-logger==2.0.7
  ```

### 1.3 Create Base Metrics Class
File: `src/monitoring/prometheus_metrics.py`
- [ ] PrometheusMetrics base class
- [ ] Common metrics initialization
- [ ] Port allocation (8001-8009)
- [ ] Metrics registration
- [ ] Export method for HTTP endpoint

### 1.4 System Metrics Collector
File: `src/monitoring/system_metrics.py`
- [ ] CPU utilization (process and system)
- [ ] Memory utilization (process and system)
- [ ] Update interval (60s)
- [ ] Background thread for collection

### 1.5 Metrics HTTP Server
File: `src/monitoring/exporters/metrics_server.py`
- [ ] HTTP server on configurable port
- [ ] /metrics endpoint (Prometheus format)
- [ ] /health endpoint (basic health check)
- [ ] /ready endpoint (readiness check)
- [ ] Background thread management


## Phase 2: Consumer-Specific Metrics (Week 2)

### 2.1 Kafka Base Metrics
File: `src/monitoring/kafka_metrics.py`
- [ ] messages_consumed_total (counter)
- [ ] messages_consumed_per_topic (counter with topic label)
- [ ] messages_processed_successfully (counter)
- [ ] messages_failed (counter)
- [ ] message_processing_duration_seconds (histogram)
- [ ] consumer_lag (gauge)
- [ ] consumer_offset_current (gauge)
- [ ] consumer_offset_committed (gauge)
- [ ] Context manager for message processing timing

### 2.2 Web Scraper Metrics (Port 8001)
File: `src/consumers/kafka_web_scraper.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8001)
- [ ] urls_scraped_total (counter)
- [ ] scraping_duration_seconds (histogram)
- [ ] scraping_failures_total (counter)
- [ ] Wrap message processing with metrics
- [ ] Start metrics server in background thread

### 2.3 Document Chunker Metrics (Port 8002)
File: `src/consumers/kafka_document_chunker.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8002)
- [ ] documents_chunked_total (counter)
- [ ] chunks_created_total (counter)
- [ ] chunking_duration_seconds (histogram)
- [ ] Wrap message processing with metrics
- [ ] Start metrics server

### 2.4 Chunk Embedder Metrics (Port 8003)
File: `src/consumers/kafka_chunk_embedder.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8003)
- [ ] chunks_embedded_total (counter)
- [ ] embedding_api_calls_total (counter)
- [ ] embedding_duration_seconds (histogram)
- [ ] embedding_failures_total (counter)
- [ ] Wrap embedding operations with metrics
- [ ] Start metrics server

### 2.5 Q&A Generator Metrics (Port 8004)
File: `src/consumers/kafka_qa_generator.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8004)
- [ ] qa_pairs_generated_total (counter)
- [ ] qa_generation_duration_seconds (histogram)
- [ ] qa_generation_failures_total (counter)
- [ ] Wrap QA generation with metrics
- [ ] Start metrics server

### 2.6 Q&A Embedder Metrics (Port 8005)
File: `src/consumers/kafka_qa_embedder.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8005)
- [ ] qa_embedded_total (counter)
- [ ] embedding_duration_seconds (histogram)
- [ ] Wrap embedding operations with metrics
- [ ] Start metrics server

### 2.7 Index Aggregator Metrics (Port 8008)
File: `src/consumers/kafka_index_aggregator.py`
- [ ] Import PrometheusMetrics
- [ ] Initialize metrics exporter (port 8008)
- [ ] chunks_aggregated_total (counter)
- [ ] chunks_duplicates_filtered (counter)
- [ ] qa_pairs_aggregated_total (counter)
- [ ] qa_pairs_duplicates_filtered (counter)
- [ ] azure_upload_duration_seconds (histogram)
- [ ] session_backup_duration_seconds (histogram)
- [ ] Wrap aggregation operations with metrics
- [ ] Start metrics server


## Phase 3: Cache & Storage Monitoring

### 3.1 Local Cache Metrics
File: `src/monitoring/cache_metrics.py`
- [ ] CacheMetricsCollector class
- [ ] cache_files_total (gauge)
- [ ] cache_size_bytes (gauge)
- [ ] cache_age_seconds (gauge) - oldest file
- [ ] cache_hit_total (counter)
- [ ] cache_miss_total (counter)
- [ ] cache_eviction_total (counter)
- [ ] cache_cleanup_duration_seconds (histogram)
- [ ] Periodic collection (every 60s)
- [ ] Monitor directories:
  - /tmp/chunked_docs/
  - /tmp/qa_docs/

### 3.2 Azure Storage Metrics
File: `src/monitoring/azure_metrics.py`
- [ ] AzureMetricsCollector class
- [ ] azure_blobs_total (gauge per container)
- [ ] azure_storage_bytes (gauge per container)
- [ ] azure_upload_operations_total (counter)
- [ ] azure_upload_failures_total (counter)
- [ ] azure_upload_duration_seconds (histogram)
- [ ] azure_download_operations_total (counter)
- [ ] azure_download_duration_seconds (histogram)
- [ ] Periodic collection (every 5min)
- [ ] Monitor containers:
  - indices
  - session-backups
  - archive

### 3.3 Integrate Cache Metrics
- [ ] Web Scraper: Track cache usage
- [ ] Document Chunker: Track cache usage
- [ ] Index Aggregator: Track local file operations


## Phase 4: Structured Logging (Week 3)

### 4.1 Logging Configuration
File: `src/monitoring/logging_config.py`
- [ ] Configure JSON formatter
- [ ] Add standard fields:
  - timestamp (ISO 8601)
  - level
  - logger
  - message
  - consumer_id
  - session_id
  - trace_id
- [ ] File and console handlers
- [ ] Log level configuration

### 4.2 Update All Consumers
- [ ] kafka_web_scraper.py - structured logging
- [ ] kafka_document_chunker.py - structured logging
- [ ] kafka_chunk_embedder.py - structured logging
- [ ] kafka_qa_generator.py - structured logging
- [ ] kafka_qa_embedder.py - structured logging
- [ ] kafka_index_aggregator.py - structured logging
- [ ] Add trace_id generation and propagation
- [ ] Include extra fields in log entries


## Phase 5: Deployment & Configuration

### 5.1 Docker Configuration
- [ ] Update Dockerfile to expose metrics ports (8001-8009)
- [ ] Add health check commands
- [ ] Configure logging output

### 5.2 Docker Compose Updates
File: `docker-compose.kafka.yml`
- [ ] Add Prometheus service
  - Port: 9090
  - Volume mount: prometheus.yml
  - Retention: 30 days
- [ ] Add Grafana service (optional, for visualization)
  - Port: 3000
- [ ] Add Elasticsearch service (3 nodes)
  - Ports: 9200, 9300
- [ ] Add Kibana service
  - Port: 5601
- [ ] Add Filebeat service for log shipping
- [ ] Update consumer services:
  - Expose metrics ports
  - Add health check endpoints
  - Configure logging

### 5.3 Prometheus Configuration
File: `prometheus.yml`
- [ ] Create scraping config for all consumers
- [ ] Set scrape interval (15s)
- [ ] Add job labels
- [ ] Configure retention

### 5.4 Filebeat Configuration
File: `filebeat.yml`
- [ ] Configure log file paths
- [ ] Set JSON parsing
- [ ] Configure Elasticsearch output
- [ ] Add index patterns

### 5.5 Kibana Setup
- [ ] Create index pattern: logs-kafka-consumers-*
- [ ] Define field mappings
- [ ] Create index lifecycle policy (90-day retention)


## Phase 6: Dashboards & Alerting (Week 4)

### 6.1 Grafana Dashboards
- [ ] Consumer Overview dashboard
- [ ] Per-consumer performance dashboard
- [ ] System resources dashboard
- [ ] Kafka health dashboard

### 6.2 Kibana Dashboards
- [ ] Consumer logs overview
- [ ] Error logs dashboard
- [ ] Session tracking dashboard

### 6.3 Prometheus Alerts
- [ ] High CPU usage (>80% for 5min)
- [ ] High memory usage (>90% for 5min)
- [ ] Consumer lag >1000 messages
- [ ] High error rate (>5%)
- [ ] Cache size >10GB
- [ ] Azure upload failures
- [ ] Consumer stuck (no messages for 10min)

### 6.4 Elasticsearch Alerts
- [ ] Error rate spike
- [ ] CRITICAL log entries
- [ ] Consumer crash detection


## Phase 7: Testing & Documentation

### 7.1 Unit Tests
- [ ] Test metrics increment correctly
- [ ] Test histogram recording
- [ ] Test gauge updates
- [ ] Test JSON log format
- [ ] Test metrics server endpoints

### 7.2 Integration Tests
- [ ] Test Prometheus scraping
- [ ] Test log shipping to Elasticsearch
- [ ] Test metrics under load
- [ ] Test alert triggering

### 7.3 Documentation
- [ ] Metrics documentation (all metrics explained)
- [ ] Dashboard documentation (how to use)
- [ ] Runbook (troubleshooting guide)
- [ ] Setup guide (deployment steps)


## Port Allocation Summary
```
8001 - kafka_web_scraper
8002 - kafka_document_chunker
8003 - kafka_chunk_embedder
8004 - kafka_qa_generator
8005 - kafka_qa_embedder
8006 - kafka_summarizer
8007 - kafka_citation_extractor
8008 - kafka_index_aggregator
8009 - kafka_dead_letter_consumer

9090 - Prometheus server
3000 - Grafana
9200 - Elasticsearch
5601 - Kibana
```


## Configuration Additions

### config.py
```python
# TODO: Add monitoring configuration
MONITORING = {
    'prometheus': {
        'enabled': True,
        'ports': {
            'web_scraper': 8001,
            'document_chunker': 8002,
            'chunk_embedder': 8003,
            'qa_generator': 8004,
            'qa_embedder': 8005,
            'summarizer': 8006,
            'citation_extractor': 8007,
            'index_aggregator': 8008,
            'dead_letter': 8009,
        },
        'collection_interval': 60,  # seconds
    },
    'logging': {
        'format': 'json',  # or 'text'
        'level': 'INFO',
        'elasticsearch': {
            'enabled': True,
            'host': 'localhost',
            'port': 9200,
            'index_prefix': 'logs-kafka-consumers',
        }
    },
    'cache_monitoring': {
        'enabled': True,
        'scan_interval': 60,  # seconds
        'directories': ['/tmp/chunked_docs', '/tmp/qa_docs'],
    },
    'azure_monitoring': {
        'enabled': True,
        'scan_interval': 300,  # seconds (5 minutes)
        'containers': ['indices', 'session-backups', 'archive'],
    }
}
```


## Progress Tracking

### Week 1: Core Infrastructure
- [ ] Monitoring module structure
- [ ] Base metrics classes
- [ ] System metrics collector
- [ ] HTTP server for metrics
- [ ] Test with one consumer

### Week 2: Consumer Metrics
- [ ] Kafka base metrics
- [ ] All consumer-specific metrics
- [ ] Cache monitoring
- [ ] Azure storage monitoring

### Week 3: Structured Logging
- [ ] Logging configuration
- [ ] Update all consumers
- [ ] Deploy ELK stack
- [ ] Configure Filebeat

### Week 4: Dashboards & Alerts
- [ ] Create dashboards
- [ ] Configure alerts
- [ ] Documentation
- [ ] Testing
