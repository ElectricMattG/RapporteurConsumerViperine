TELEMETRY & OBSERVABILITY IMPLEMENTATION PLAN
==============================================

OVERVIEW
========
Add comprehensive monitoring to research consumers before scaling:
- Prometheus metrics for system resources and application metrics
- Kafka consumer metrics
- Local cache tracking
- Azure storage tracking
- Kibana/ELK stack for centralized logging


1. PROMETHEUS METRICS
======================

1.1 System Resource Metrics
----------------------------
Location: All consumer processes

Metrics to expose:
  - CPU utilization (%)
    • process_cpu_percent
    • system_cpu_percent

  - Memory utilization (%)
    • process_memory_percent
    • process_memory_rss_bytes
    • process_memory_vms_bytes
    • system_memory_percent
    • system_memory_available_bytes

Implementation approach:
  - Use prometheus_client Python library
  - Add psutil for system metrics
  - Create base metrics exporter class that all consumers inherit
  - Expose metrics on HTTP endpoint (e.g., :8000/metrics per consumer)
  - Use different ports for each consumer type (8001, 8002, 8003, etc.)


1.2 Kafka Consumer Metrics
---------------------------
Location: All Kafka consumers

Metrics to expose:
  - messages_consumed_total (counter)
  - messages_consumed_per_topic (counter with label: topic)
  - messages_processed_successfully (counter)
  - messages_failed (counter)
  - message_processing_duration_seconds (histogram)
  - consumer_lag (gauge) - messages behind
  - consumer_offset_current (gauge)
  - consumer_offset_committed (gauge)
  - rebalance_count (counter)
  - batch_size_messages (histogram)

Per consumer type:
  - kafka_web_scraper:
    • urls_scraped_total
    • scraping_duration_seconds
    • scraping_failures_total

  - kafka_document_chunker:
    • documents_chunked_total
    • chunks_created_total
    • chunking_duration_seconds

  - kafka_chunk_embedder:
    • chunks_embedded_total
    • embedding_api_calls_total
    • embedding_duration_seconds

  - kafka_qa_generator:
    • qa_pairs_generated_total
    • qa_generation_duration_seconds

  - kafka_qa_embedder:
    • qa_embedded_total

  - kafka_index_aggregator:
    • chunks_aggregated_total
    • chunks_duplicates_filtered
    • qa_pairs_aggregated_total
    • qa_pairs_duplicates_filtered
    • azure_upload_duration_seconds
    • session_backup_duration_seconds

Implementation approach:
  - Wrap Kafka consumer poll loop with metrics
  - Track message processing lifecycle
  - Add decorators for timing critical operations
  - Use labels for topic, consumer_group, partition


1.3 Local Cache Metrics
------------------------
Location: Components that use cache (web scraper, chunker)

Metrics to expose:
  - cache_files_total (gauge)
  - cache_size_bytes (gauge)
  - cache_age_seconds (gauge) - oldest file
  - cache_hit_total (counter)
  - cache_miss_total (counter)
  - cache_eviction_total (counter)
  - cache_cleanup_duration_seconds (histogram)

Per cache directory:
  - /tmp/chunked_docs/
  - /tmp/qa_docs/
  - /tmp/scraper_cache/ (if exists)

Implementation approach:
  - Create cache metrics collector
  - Run periodic scan (every 60s) to update gauges
  - Hook into cache cleanup operations
  - Track cache hits/misses at read time


1.4 Azure Storage Metrics
--------------------------
Location: Index aggregator, any component uploading to Azure

Metrics to expose:
  - azure_blobs_total (gauge per container)
  - azure_storage_bytes (gauge per container)
  - azure_upload_operations_total (counter)
  - azure_upload_failures_total (counter)
  - azure_upload_duration_seconds (histogram)
  - azure_download_operations_total (counter)
  - azure_download_duration_seconds (histogram)

Per container:
  - indices/
  - session-backups/
  - archive/

Implementation approach:
  - Wrap Azure BlobServiceClient operations
  - Periodic container stats collection (every 5min)
  - Track upload/download operations in real-time


1.5 Prometheus Deployment
--------------------------
Setup needed:
  1. Deploy Prometheus server (Docker container or K8s pod)
  2. Configure scraping targets (all consumer endpoints)
  3. Set scrape interval (15s recommended)
  4. Add service discovery for dynamic consumers
  5. Configure retention period (30 days recommended)

Prometheus config (prometheus.yml):
```yaml
scrape_configs:
  - job_name: 'kafka_consumers'
    static_configs:
      - targets:
          - 'kafka_web_scraper:8001'
          - 'kafka_document_chunker:8002'
          - 'kafka_chunk_embedder:8003'
          - 'kafka_qa_generator:8004'
          - 'kafka_qa_embedder:8005'
          - 'kafka_summarizer:8006'
          - 'kafka_citation_extractor:8007'
          - 'kafka_index_aggregator:8008'
    scrape_interval: 15s
```


2. KAFKA METRICS
================

2.1 Kafka Broker Metrics
-------------------------
Source: Kafka JMX metrics (already exposed by Kafka)

Key metrics to monitor:
  - kafka_server_brokertopicmetrics_messagesin_total
  - kafka_server_brokertopicmetrics_bytesin_total
  - kafka_server_brokertopicmetrics_bytesout_total
  - kafka_topic_partition_current_offset
  - kafka_consumergroup_lag

Per topic to monitor:
  - web-scraping-requests
  - document-chunking-requests
  - chunk-embedding-requests
  - qa-generation-requests
  - qa-embedding-requests
  - index-aggregation-requests

Implementation approach:
  - Use JMX Exporter for Kafka (already common pattern)
  - Or use Kafka Exporter (prometheus/kafka_exporter)
  - Configure Prometheus to scrape Kafka metrics endpoint


2.2 Consumer Group Lag Monitoring
----------------------------------
Critical for observability!

Metrics needed:
  - Consumer group lag per topic/partition
  - Consumer group offset vs high water mark
  - Time since last commit

Implementation approach:
  - Option 1: Use kafka-lag-exporter (lightweight, recommended)
  - Option 2: Burrow (more features, heavier)
  - Option 3: Built into consumer metrics (1.2 above)


3. STRUCTURED LOGGING FOR KIBANA
=================================

3.1 Log Format Standardization
-------------------------------
Current state: Logs to files in logs/ directory

Target state: Structured JSON logs

Required fields in every log entry:
  - timestamp (ISO 8601)
  - level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  - logger (consumer name)
  - message (human readable)
  - consumer_id (unique instance ID)
  - session_id (for session-based operations)
  - trace_id (for distributed tracing across consumers)
  - extra_fields (flexible JSON object)

Example log entry:
```json
{
  "timestamp": "2025-12-22T10:00:00.123Z",
  "level": "INFO",
  "logger": "kafka_document_chunker",
  "message": "Document chunked successfully",
  "consumer_id": "chunker-pod-abc123",
  "session_id": "time_series_20231007_20231021",
  "trace_id": "uuid-1234-5678",
  "document_id": "doc_abc123",
  "chunks_created": 15,
  "processing_time_ms": 234
}
```


3.2 Log Shipping to Elasticsearch
----------------------------------
Options:

Option A: Filebeat (Recommended for K8s)
  - Deployed as DaemonSet in K8s
  - Reads container logs automatically
  - Ships to Elasticsearch
  - Lightweight, battle-tested

Option B: Fluentd/Fluent Bit
  - More flexible transformation
  - Can enrich logs before shipping
  - Heavier resource usage

Option C: Direct logging to Elasticsearch
  - Use python-elasticsearch-logger
  - Logs go directly from Python to ES
  - More network overhead
  - Loses logs if ES is down

Recommended: Filebeat + structured JSON logs


3.3 Elasticsearch & Kibana Setup
---------------------------------
Components needed:
  1. Elasticsearch cluster (3 nodes for HA)
  2. Kibana instance
  3. Index pattern: logs-kafka-consumers-*
  4. Index lifecycle management (ILM):
     - Hot: 7 days (frequently accessed)
     - Warm: 30 days (less frequent)
     - Delete: 90 days

Index template to create:
```json
{
  "index_patterns": ["logs-kafka-consumers-*"],
  "template": {
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1,
      "index.lifecycle.name": "logs-90day-retention"
    },
    "mappings": {
      "properties": {
        "timestamp": { "type": "date" },
        "level": { "type": "keyword" },
        "logger": { "type": "keyword" },
        "consumer_id": { "type": "keyword" },
        "session_id": { "type": "keyword" },
        "trace_id": { "type": "keyword" },
        "document_id": { "type": "keyword" },
        "message": { "type": "text" }
      }
    }
  }
}
```


3.4 Kibana Dashboards to Create
--------------------------------
Dashboard 1: Consumer Overview
  - Active consumers count
  - Messages processed per minute
  - Error rate
  - Processing latency (p50, p95, p99)

Dashboard 2: Per-Consumer Performance
  - Web scraper: URLs/min, success rate
  - Chunker: Docs/min, chunks/doc average
  - Embedder: Embeddings/min, API latency
  - QA Generator: QA pairs/min, generation latency
  - Aggregator: Dedup rate, upload frequency

Dashboard 3: System Resources
  - CPU utilization per consumer
  - Memory utilization per consumer
  - Cache usage
  - Azure storage growth

Dashboard 4: Kafka Health
  - Topic lag per consumer group
  - Message throughput per topic
  - Consumer rebalances


4. CODE CHANGES REQUIRED
=========================

4.1 New Dependencies
--------------------
Add to requirements.txt:
  - prometheus-client==0.19.0
  - psutil==5.9.6
  - python-json-logger==2.0.7
  - opentelemetry-api==1.21.0 (optional, for tracing)
  - opentelemetry-sdk==1.21.0 (optional)


4.2 New Modules to Create
--------------------------
src/monitoring/
  __init__.py
  prometheus_metrics.py       # Base metrics class
  system_metrics.py            # CPU, memory collectors
  kafka_metrics.py             # Kafka-specific metrics
  cache_metrics.py             # Local cache monitoring
  azure_metrics.py             # Azure storage monitoring
  logging_config.py            # Structured logging setup

src/monitoring/exporters/
  __init__.py
  metrics_server.py            # HTTP server for Prometheus


4.3 Consumer Changes
--------------------
Each consumer needs:

1. Import metrics module
2. Initialize metrics exporter with unique port
3. Wrap message processing with metrics:
   ```python
   with metrics.message_processing_timer():
       try:
           process_message(msg)
           metrics.messages_processed.inc()
       except Exception as e:
           metrics.messages_failed.inc()
           raise
   ```

4. Add structured logging:
   ```python
   logger.info(
       "Document chunked",
       extra={
           "document_id": doc_id,
           "chunks_created": len(chunks),
           "processing_time_ms": elapsed
       }
   )
   ```

5. Start metrics HTTP server in background thread
6. Add health check endpoint (/health)
7. Add readiness endpoint (/ready)


4.4 Configuration Changes
--------------------------
config.py additions:
  - PROMETHEUS_PORT (per consumer)
  - PROMETHEUS_ENABLED (default: True)
  - METRICS_COLLECTION_INTERVAL (default: 60s)
  - ELASTICSEARCH_HOST
  - ELASTICSEARCH_PORT
  - ELASTICSEARCH_INDEX_PREFIX
  - LOG_LEVEL
  - LOG_FORMAT (json | text)


4.5 Docker/K8s Changes
----------------------
Dockerfile updates:
  - Expose Prometheus ports (8001-8008)
  - Add health check commands

docker-compose updates:
  - Add Prometheus service
  - Add Elasticsearch service (3 nodes)
  - Add Kibana service
  - Add Filebeat service
  - Expose metrics ports
  - Add prometheus.yml volume mount
  - Add filebeat.yml volume mount

K8s changes:
  - Add Prometheus deployment + service
  - Add ServiceMonitor CRDs (if using Prometheus Operator)
  - Add Elasticsearch StatefulSet
  - Add Kibana deployment + service
  - Add Filebeat DaemonSet
  - Update consumer deployments with:
    • Prometheus port annotations
    • Health/readiness probes
    • Log format env vars


5. IMPLEMENTATION PHASES
=========================

Phase 1: Prometheus Integration (Week 1)
-----------------------------------------
✓ Add prometheus-client dependency
✓ Create base metrics classes
✓ Add system metrics (CPU, memory)
✓ Add basic Kafka metrics (messages consumed)
✓ Deploy Prometheus server
✓ Create basic Grafana dashboards
✓ Test with 1 consumer (web scraper)

Phase 2: Expand Metrics Coverage (Week 2)
------------------------------------------
✓ Add detailed Kafka metrics to all consumers
✓ Add consumer-specific metrics (URLs scraped, chunks created, etc.)
✓ Add local cache monitoring
✓ Add Azure storage monitoring
✓ Create comprehensive Grafana dashboards

Phase 3: Structured Logging (Week 3)
-------------------------------------
✓ Standardize log format across all consumers
✓ Add trace_id and correlation IDs
✓ Deploy Elasticsearch cluster
✓ Deploy Kibana
✓ Configure Filebeat
✓ Test log shipping and querying

Phase 4: Dashboards & Alerting (Week 4)
----------------------------------------
✓ Create Kibana dashboards
✓ Set up Grafana alerts (high CPU, memory, lag)
✓ Set up Elasticsearch alerts (error rate spikes)
✓ Document monitoring runbooks
✓ Train team on using observability tools


6. METRICS PORTS ALLOCATION
============================
kafka_web_scraper:        8001
kafka_document_chunker:   8002
kafka_chunk_embedder:     8003
kafka_qa_generator:       8004
kafka_qa_embedder:        8005
kafka_summarizer:         8006
kafka_citation_extractor: 8007
kafka_index_aggregator:   8008
kafka_dead_letter:        8009

Prometheus server:        9090
Grafana:                  3000
Elasticsearch:            9200
Kibana:                   5601


7. ALERTING RULES TO CREATE
============================

Prometheus Alerts:
------------------
1. High CPU usage (>80% for 5min)
2. High memory usage (>90% for 5min)
3. Consumer lag >1000 messages
4. Message processing errors >5% rate
5. Cache size >10GB
6. Azure upload failures
7. Consumer not processing messages (stuck)

Elasticsearch Alerts:
---------------------
1. Error log rate spike (>10/min)
2. CRITICAL log entries
3. Consumer crash detected
4. Deduplication rate anomaly


8. TESTING PLAN
================

Unit Tests:
-----------
✓ Test metrics increment correctly
✓ Test metrics reset behavior
✓ Test histogram buckets
✓ Test logging JSON format
✓ Test metrics server startup/shutdown

Integration Tests:
------------------
✓ Test Prometheus scraping
✓ Test log shipping to Elasticsearch
✓ Test metrics accuracy under load
✓ Test alert triggering

Load Tests:
-----------
✓ Verify metrics overhead <1% CPU
✓ Test with 1000 msg/sec throughput
✓ Verify log shipping keeps up


9. DOCUMENTATION NEEDED
========================

1. Metrics Documentation:
   - List all metrics exposed
   - Explain what each metric means
   - Show example PromQL queries

2. Dashboard Documentation:
   - Screenshots of each dashboard
   - How to interpret the graphs
   - Common troubleshooting patterns

3. Runbook:
   - What to do when alerts fire
   - How to investigate performance issues
   - How to query logs in Kibana

4. Setup Guide:
   - How to deploy monitoring stack
   - How to add new metrics
   - How to create new dashboards


10. COST CONSIDERATIONS
========================

Storage estimates:
------------------
- Prometheus: ~500MB/day (retention: 30 days) = 15GB
- Elasticsearch: ~2GB/day (retention: 90 days) = 180GB
- Total: ~200GB storage needed

Resource requirements:
----------------------
- Prometheus: 2 CPU, 4GB RAM
- Elasticsearch: 6 CPU, 12GB RAM (3 nodes x 2 CPU, 4GB each)
- Kibana: 1 CPU, 2GB RAM
- Grafana: 0.5 CPU, 1GB RAM
- Total additional: 9.5 CPU, 19GB RAM


11. MIGRATION STRATEGY
=======================

For existing consumers:
-----------------------
1. Add metrics code without breaking changes
2. Deploy alongside existing logging
3. Verify metrics accuracy
4. Enable Prometheus scraping
5. Gradually roll out to all consumers
6. Once stable, switch logging to JSON
7. Enable Filebeat shipping
8. Verify logs in Kibana
9. Decommission old log files after 90 days


SUMMARY
=======

This plan provides:
✓ Comprehensive Prometheus metrics for all consumers
✓ System resource monitoring (CPU, memory)
✓ Kafka consumer lag and throughput tracking
✓ Local cache and Azure storage visibility
✓ Structured logging with Kibana search
✓ Alerting for critical issues
✓ Phased implementation to minimize risk
✓ Minimal performance overhead

Next steps: Review plan, approve approach, begin Phase 1 implementation.
