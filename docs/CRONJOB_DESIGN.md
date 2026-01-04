# Time-Series Research CronJob Design

## Overview

Convert the `time_series_research_queries.py` script into a Kubernetes CronJob that incrementally builds multiple research indices by sliding time windows forward. Each query maintains its own state and produces a separate, clearly-named index stored in Azure.

---

## Design Goals

1. **Incremental Processing**: Track the last processed time window per query; each run advances by one window
2. **Multiple Queries**: Support N queries executing asynchronously, each building its own index
3. **Persistent State**: Store last window timestamp per query (survives pod restarts)
4. **Configurable Queries**: Store queries in Azure Blob (updatable without code changes)
5. **Separate Indices**: Each query produces a distinct index with clear naming
6. **Buffer Period**: Default start date of September 1, 2023 (provides historical buffer)
7. **Kafka Integration**: Continue sending citations to web scraping pipeline
8. **Monitoring**: Expose metrics for CronJob execution (success/failure, windows processed)

---

## Architecture Components

### 1. State Management

**Problem**: Need to persist "last processed window" per query across CronJob runs.

**Solution: Azure Blob Storage (State Files)**

**Location**: `azure://research-indices/state/{query_id}.json`

**State File Structure**:
```json
{
  "query_id": "idf_ministry_defense",
  "query_template": "Get the latest news from the Israeli Ministry of Defense from {start_date} to {end_date}",
  "last_window_end": "2024-01-15",
  "last_window_start": "2024-01-01",
  "window_size_days": 14,
  "total_windows_processed": 87,
  "last_run_timestamp": "2024-01-15T14:32:11Z",
  "last_run_status": "success",
  "last_run_citations": 42,
  "next_window_start": "2024-01-15",
  "next_window_end": "2024-01-29"
}
```

**Why Azure Blob over Kubernetes ConfigMap?**
- ✅ Survives cluster upgrades/migrations
- ✅ Easy to inspect/modify externally (Azure Portal, CLI)
- ✅ No etcd size constraints
- ✅ Audit trail via blob versioning
- ✅ Same storage backend as indices

**Fallback Behavior**:
- If state file doesn't exist → Start from `2023-09-01` (buffer period)
- If state file corrupted → Log error, use fallback date
- If Azure unreachable → Fail CronJob (don't process without state)

---

### 2. Query Configuration

**Problem**: Need to define multiple queries without code changes; allow dynamic updates.

**Solution: Azure Blob Storage (Query Configuration File)**

**Location**: `azure://research-indices/config/queries.json`

**Configuration Structure**:
```json
{
  "version": "1.0",
  "updated_at": "2024-12-24T10:00:00Z",
  "default_window_size_days": 14,
  "default_start_date": "2023-09-01",
  "queries": [
    {
      "query_id": "idf_ministry_defense",
      "enabled": true,
      "query_template": "Get the latest news from the Israeli Ministry of Defense from {start_date} to {end_date}",
      "window_size_days": 14,
      "index_prefix": "idf_mod",
      "description": "Israeli Ministry of Defense official news",
      "tags": ["military", "israel", "government"],
      "priority": 1
    },
    {
      "query_id": "israel_prime_minister",
      "enabled": true,
      "query_template": "Get the latest news from the Israeli Prime Minister from {start_date} to {end_date}",
      "window_size_days": 14,
      "index_prefix": "israel_pm",
      "description": "Israeli Prime Minister official statements",
      "tags": ["politics", "israel", "government"],
      "priority": 1
    },
    {
      "query_id": "idf_spokesperson",
      "enabled": false,
      "query_template": "Get the latest news from the IDF Spokesperson Unit from {start_date} to {end_date}",
      "window_size_days": 7,
      "index_prefix": "idf_spox",
      "description": "IDF Spokesperson press releases",
      "tags": ["military", "israel", "communications"],
      "priority": 2
    }
  ]
}
```

**Configuration Fields**:
- `query_id`: Unique identifier (used in state files, index names)
- `enabled`: Boolean flag to enable/disable queries without deletion
- `query_template`: Query string with `{start_date}` and `{end_date}` placeholders
- `window_size_days`: Time window size (default 14, configurable per query)
- `index_prefix`: Short prefix for index filenames (e.g., `idf_mod`)
- `description`: Human-readable description
- `tags`: Categories for filtering/organizing
- `priority`: Execution priority (lower = earlier; for future optimization)

**Updates**:
- Edit `queries.json` in Azure Portal or via Azure CLI
- Next CronJob run picks up changes automatically
- No pod restart required
- Add new queries by appending to array
- Disable queries by setting `enabled: false`

---

### 3. Index Storage and Naming

**Problem**: Each query needs its own index; must be clearly identifiable.

**Solution: Hierarchical Azure Blob Structure**

**Base Path**: `azure://research-indices/indices/`

**Directory Structure**:
```
indices/
├── idf_ministry_defense/               # query_id
│   ├── idf_mod_20230901_20230915.json # index_prefix_startdate_enddate
│   ├── idf_mod_20230915_20230929.json
│   ├── idf_mod_20230929_20231013.json
│   ├── ...
│   └── idf_mod_20241201_20241215.json
│
├── israel_prime_minister/
│   ├── israel_pm_20230901_20230915.json
│   ├── israel_pm_20230915_20230929.json
│   ├── ...
│   └── israel_pm_20241201_20241215.json
│
└── idf_spokesperson/
    ├── idf_spox_20230901_20230908.json  # 7-day windows
    ├── idf_spox_20230908_20230915.json
    └── ...
```

**Index File Structure**:
```json
{
  "metadata": {
    "query_id": "idf_ministry_defense",
    "query": "Get the latest news from the Israeli Ministry of Defense from 2023-09-01 to 2023-09-15",
    "window_start": "2023-09-01",
    "window_end": "2023-09-15",
    "window_size_days": 14,
    "generated_at": "2024-12-24T10:15:32Z",
    "session_id": "time_series_20230901_20230915",
    "cronjob_run_id": "cronjob-1234567890-abcde",
    "index_filename": "idf_mod_20230901_20230915.json"
  },
  "rag_response": {
    "query": "...",
    "answer": "...",
    "processing_time_ms": 2451,
    "request_id": "req_...",
    "session_id": "time_series_20230901_20230915",
    "grounding_metadata": {
      "citations": [
        {
          "uri": "https://...",
          "title": "...",
          "snippet": "..."
        }
      ]
    }
  },
  "kafka_metadata": {
    "batch_sent": true,
    "citations_published": 42,
    "kafka_topic": "web-scrape-requests",
    "session_complete_sent": true
  }
}
```

**Naming Convention**:
- Format: `{index_prefix}_{start_date}_{end_date}.json`
- Dates: `YYYYMMDD` format (sortable, no delimiters)
- Example: `idf_mod_20230901_20230915.json`
- Guarantees: Unique per query, chronologically sortable, self-documenting

**Benefits**:
- ✅ Clear separation by query
- ✅ Easy to identify date ranges
- ✅ Sortable filenames (chronological order)
- ✅ Simple to aggregate (all files in query directory)
- ✅ Troubleshooting: Isolate issues to specific query/window
- ✅ Scalable: Millions of files supported by Blob Storage

---

### 4. CronJob Execution Workflow

**Problem**: Process multiple queries incrementally with state tracking and error handling.

**Solution: Multi-Phase Execution Pipeline**

#### **Phase 1: Initialization** (0-5 seconds)

1. **Load Query Configuration**
   - Read `azure://research-indices/config/queries.json`
   - Parse JSON, validate schema
   - Filter to `enabled: true` queries
   - Sort by `priority` (optional: for future optimization)

2. **Validate Dependencies**
   - Check RAG API reachability: `https://4.207.24.92/query`
   - Check Kafka connectivity: `kafka.rapporteur.ai:9094`
   - Check Azure Blob Storage authentication
   - Fail fast if any dependency unavailable

3. **Initialize Monitoring**
   - Start Prometheus metrics server (port 8010)
   - Expose metrics:
     - `cronjob_queries_total{query_id}`
     - `cronjob_windows_processed{query_id, status}`
     - `cronjob_execution_duration_seconds{query_id}`
     - `cronjob_citations_published{query_id}`
     - `cronjob_errors_total{query_id, error_type}`

#### **Phase 2: State Loading** (5-15 seconds)

**For each enabled query**:

1. **Read State File**
   - Path: `azure://research-indices/state/{query_id}.json`
   - If exists: Load `next_window_start`, `next_window_end`
   - If not exists: Use fallback date (`2023-09-01`)

2. **Calculate Next Window**
   - `window_start = state.next_window_start OR default_start_date`
   - `window_end = window_start + window_size_days`
   - If `window_end > today()`: Skip query (already up-to-date)
   - If `window_start >= today()`: Skip query (in the future)

3. **Validate Window**
   - Ensure `window_start < window_end`
   - Ensure window doesn't exceed `2025-11-04` (END_DATE from script)
   - Log calculated window

**Example State Loading**:
```
[INFO] Loading query configurations...
[INFO] Found 3 queries, 2 enabled
[INFO] Query: idf_ministry_defense
  ├─ Last window: 2024-01-01 to 2024-01-15
  ├─ Next window: 2024-01-15 to 2024-01-29
  └─ Status: READY
[INFO] Query: israel_prime_minister
  ├─ Last window: NONE (new query)
  ├─ Next window: 2023-09-01 to 2023-09-15 (fallback)
  └─ Status: READY
[INFO] Query: idf_spokesperson (disabled)
  └─ Status: SKIPPED
```

#### **Phase 3: Async Query Execution** (30-180 seconds per query)

**Execute all queries asynchronously using `asyncio.gather()`**

**Per-Query Workflow**:

1. **Format Query String**
   - Replace `{start_date}` and `{end_date}` in template
   - Example: `"Get the latest news from the Israeli Ministry of Defense from 2024-01-15 to 2024-01-29"`

2. **Execute RAG Query**
   - POST to RAG API with query
   - Timeout: 180 seconds (3 minutes)
   - Retry logic: 3 attempts with exponential backoff
   - Handle errors: Log and mark query as failed

3. **Process RAG Response**
   - Extract grounding citations
   - Count citations
   - Generate session_id: `time_series_{query_id}_{start_date}_{end_date}`

4. **Publish to Kafka**
   - Create batch messages from citations
   - Send to `web-scrape-requests` topic
   - Send session complete signal to `session-complete` topic
   - Track success/failure

5. **Save Index to Azure**
   - Build index JSON with metadata + RAG response + Kafka metadata
   - Filename: `{index_prefix}_{start_date}_{end_date}.json`
   - Path: `azure://research-indices/indices/{query_id}/{filename}`
   - Upload to Azure Blob
   - Verify upload success

6. **Update State**
   - Update state file with new window info
   - Set `last_window_end = current_window_end`
   - Set `next_window_start = current_window_end`
   - Calculate `next_window_end = next_window_start + window_size_days`
   - Increment `total_windows_processed`
   - Set `last_run_timestamp`, `last_run_status`, `last_run_citations`
   - Upload state file: `azure://research-indices/state/{query_id}.json`

**Async Execution Benefits**:
- ✅ Process multiple queries in parallel
- ✅ Reduce total CronJob execution time
- ✅ Independent failure (one query failure doesn't block others)
- ✅ Better resource utilization

**Example Async Execution**:
```
[INFO] Starting async execution of 2 queries...
[INFO] [idf_ministry_defense] Executing query for 2024-01-15 to 2024-01-29
[INFO] [israel_prime_minister] Executing query for 2023-09-01 to 2023-09-15
[INFO] [idf_ministry_defense] RAG response received (2.3s, 42 citations)
[INFO] [israel_prime_minister] RAG response received (3.1s, 38 citations)
[INFO] [idf_ministry_defense] Published 42 citations to Kafka
[INFO] [israel_prime_minister] Published 38 citations to Kafka
[INFO] [idf_ministry_defense] Saved index: idf_mod_20240115_20240129.json
[INFO] [israel_prime_minister] Saved index: israel_pm_20230901_20230915.json
[INFO] [idf_ministry_defense] Updated state: next window 2024-01-29 to 2024-02-12
[INFO] [israel_prime_minister] Updated state: next window 2023-09-15 to 2023-09-29
[SUCCESS] 2/2 queries completed successfully
```

#### **Phase 4: Error Handling and Retry** (varies)

**Error Categories**:

1. **Configuration Errors** (fatal, fail entire CronJob)
   - `queries.json` not found or invalid JSON
   - No enabled queries
   - Invalid query template format

2. **Transient Errors** (retry with backoff)
   - RAG API timeout or 5xx error
   - Kafka connection failure
   - Azure Blob upload failure

3. **Query-Specific Errors** (fail query, continue others)
   - RAG API returns error response
   - Invalid query format
   - No citations in response

**Retry Logic**:
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type((ConnectionError, TimeoutError))
)
async def execute_rag_query(query):
    ...
```

**Error Recovery**:
- Failed query: State file not updated (will retry next run)
- Partial Kafka failure: Log warning, continue (DLQ handles failures)
- Azure upload failure: Retry 3 times, then fail query
- All queries failed: Exit code 1 (Kubernetes marks CronJob as failed)

#### **Phase 5: Cleanup and Metrics** (0-5 seconds)

1. **Aggregate Results**
   - Count successful queries
   - Count failed queries
   - Total citations published
   - Total windows advanced

2. **Update Prometheus Metrics**
   - Increment counters
   - Record execution duration per query
   - Update last success timestamp

3. **Log Summary**
   ```
   [SUMMARY] CronJob Execution Complete
   ├─ Duration: 45.2 seconds
   ├─ Queries executed: 2
   ├─ Queries succeeded: 2
   ├─ Queries failed: 0
   ├─ Total citations: 80
   ├─ Total windows advanced: 2
   └─ Next run: 2024-12-25T01:00:00Z
   ```

4. **Exit with Status Code**
   - Exit 0: All queries succeeded
   - Exit 1: All queries failed (CronJob marked failed)
   - Exit 0 (with warnings): Some queries succeeded, some failed

---

## Kubernetes Resources

### 1. CronJob Resource

**File**: `k8s/time-series-cronjob.yaml`

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: time-series-research-indexer
  namespace: consumers
  labels:
    app: time-series-indexer
    component: research
spec:
  # Run daily at 1 AM UTC
  schedule: "0 1 * * *"

  # Keep last 3 successful and 3 failed jobs for debugging
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3

  # Prevent concurrent runs (wait for previous to finish)
  concurrencyPolicy: Forbid

  # Retry failed jobs once
  jobTemplate:
    spec:
      backoffLimit: 1
      activeDeadlineSeconds: 3600  # 1 hour timeout

      template:
        metadata:
          labels:
            app: time-series-indexer
            cronjob: time-series-research
        spec:
          restartPolicy: OnFailure
          serviceAccountName: time-series-indexer

          containers:
          - name: indexer
            image: ragserviceneacr6881.azurecr.io/time-series-indexer:latest
            imagePullPolicy: Always

            env:
            # Azure Storage
            - name: AZURE_STORAGE_CONNECTION_STRING
              valueFrom:
                secretKeyRef:
                  name: azure-storage-credentials
                  key: connection-string

            - name: AZURE_STORAGE_CONTAINER
              value: "research-indices"

            # RAG API
            - name: RAG_API_URL
              value: "https://4.207.24.92/query"

            - name: RAG_API_KEY
              valueFrom:
                secretKeyRef:
                  name: rag-api-credentials
                  key: api-key

            # Kafka
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "kafka.rapporteur.ai:9094"

            # Configuration
            - name: QUERIES_CONFIG_PATH
              value: "config/queries.json"

            - name: STATE_PATH_PREFIX
              value: "state"

            - name: INDICES_PATH_PREFIX
              value: "indices"

            - name: DEFAULT_START_DATE
              value: "2023-09-01"

            - name: DEFAULT_WINDOW_SIZE_DAYS
              value: "14"

            # Monitoring
            - name: METRICS_PORT
              value: "8010"

            - name: LOG_LEVEL
              value: "INFO"

            resources:
              requests:
                cpu: 200m
                memory: 512Mi
              limits:
                cpu: 1000m
                memory: 2Gi

            ports:
            - name: metrics
              containerPort: 8010
              protocol: TCP
```

**Schedule Options**:
- Daily: `"0 1 * * *"` (1 AM UTC daily)
- Every 12 hours: `"0 */12 * * *"`
- Every 6 hours: `"0 */6 * * *"`
- Hourly: `"0 * * * *"` (for rapid catch-up)

**Concurrency Policy**:
- `Forbid`: Only one CronJob runs at a time (recommended)
- `Allow`: Multiple jobs can run concurrently (dangerous with state updates)
- `Replace`: Cancel previous job if still running (can cause data loss)

### 2. ServiceAccount and RBAC

**File**: `k8s/time-series-serviceaccount.yaml`

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: time-series-indexer
  namespace: consumers
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: time-series-indexer
  namespace: consumers
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log"]
  verbs: ["get", "list"]
- apiGroups: ["batch"]
  resources: ["jobs"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: time-series-indexer
  namespace: consumers
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: time-series-indexer
subjects:
- kind: ServiceAccount
  name: time-series-indexer
  namespace: consumers
```

### 3. Secrets

**Azure Storage**:
```bash
kubectl create secret generic azure-storage-credentials \
  -n consumers \
  --from-literal=connection-string="DefaultEndpointsProtocol=https;..."
```

**RAG API**:
```bash
kubectl create secret generic rag-api-credentials \
  -n consumers \
  --from-literal=api-key="32d160f623efa11ebd88d34ca0b9f4093eb1f866a640d4b70b0fbafb6c22e398"
```

---

## Monitoring and Observability

### 1. Prometheus Metrics

**Metrics Exposed** (port 8010):

```python
# CronJob execution metrics
cronjob_execution_total{status="success|failure"}
cronjob_execution_duration_seconds{query_id}
cronjob_last_success_timestamp{query_id}

# Query metrics
cronjob_queries_processed_total{query_id, status="success|failure"}
cronjob_windows_advanced_total{query_id}
cronjob_citations_published_total{query_id}

# Error metrics
cronjob_errors_total{query_id, error_type}
cronjob_retries_total{query_id, operation}

# State metrics
cronjob_query_current_window_start{query_id}
cronjob_query_total_windows_processed{query_id}
```

### 2. Grafana Dashboard

**Dashboard**: "Time Series Research Indexer"

**Panels**:
1. **CronJob Execution Status** (gauge)
   - Last execution status per query
   - Time since last success

2. **Windows Processed** (counter)
   - Total windows processed per query
   - Rate of window processing

3. **Citations Published** (graph)
   - Citations published per query over time
   - Kafka publishing success rate

4. **Execution Duration** (histogram)
   - P50, P95, P99 execution time per query
   - Identify slow queries

5. **Error Rate** (graph)
   - Errors by type (RAG API, Kafka, Azure)
   - Error trends over time

6. **Query Progress** (table)
   - Query ID, current window, total windows, last success

### 3. Logging

**Structured JSON Logs**:
```json
{
  "timestamp": "2024-12-24T10:15:32Z",
  "level": "INFO",
  "logger": "time_series_indexer",
  "cronjob_run_id": "cronjob-1234567890-abcde",
  "query_id": "idf_ministry_defense",
  "window_start": "2024-01-15",
  "window_end": "2024-01-29",
  "message": "RAG query executed successfully",
  "processing_time_ms": 2451,
  "citations_count": 42,
  "kafka_published": true,
  "index_saved": true
}
```

**Log Aggregation**:
- Logs collected by Fluent Bit or Promtail
- Sent to Azure Log Analytics or Grafana Loki
- Searchable by `cronjob_run_id`, `query_id`, `window_start`

### 4. Alerting

**Alert Rules**:

1. **CronJob Failed**
   ```yaml
   alert: TimeSeriesCronJobFailed
   expr: cronjob_execution_total{status="failure"} > 0
   for: 5m
   severity: critical
   annotations:
     summary: "Time series CronJob failed"
   ```

2. **Query Behind Schedule**
   ```yaml
   alert: TimeSeriesQueryBehind
   expr: (time() - cronjob_last_success_timestamp) > 172800  # 48 hours
   for: 10m
   severity: warning
   annotations:
     summary: "Query {{ $labels.query_id }} hasn't advanced in 48 hours"
   ```

3. **High Error Rate**
   ```yaml
   alert: TimeSeriesHighErrorRate
   expr: rate(cronjob_errors_total[5m]) > 0.5
   for: 15m
   severity: warning
   annotations:
     summary: "High error rate in time series CronJob"
   ```

---

## Operational Procedures

### 1. Initial Setup

**Step 1: Create Azure Storage Structure**
```bash
# Create container
az storage container create \
  --name research-indices \
  --account-name <storage-account>

# Upload initial query configuration
az storage blob upload \
  --container research-indices \
  --name config/queries.json \
  --file queries.json

# Create directory structure (metadata only)
az storage blob upload \
  --container research-indices \
  --name state/.placeholder \
  --file /dev/null

az storage blob upload \
  --container research-indices \
  --name indices/.placeholder \
  --file /dev/null
```

**Step 2: Deploy Kubernetes Resources**
```bash
# Create namespace if doesn't exist
kubectl create namespace consumers

# Create secrets
kubectl create secret generic azure-storage-credentials \
  -n consumers \
  --from-literal=connection-string="..."

kubectl create secret generic rag-api-credentials \
  -n consumers \
  --from-literal=api-key="..."

# Deploy ServiceAccount and RBAC
kubectl apply -f k8s/time-series-serviceaccount.yaml

# Deploy CronJob
kubectl apply -f k8s/time-series-cronjob.yaml
```

**Step 3: Trigger First Run (Manual)**
```bash
# Create a one-off Job from CronJob
kubectl create job time-series-initial-run \
  --from=cronjob/time-series-research-indexer \
  -n consumers

# Watch logs
kubectl logs -f -n consumers job/time-series-initial-run
```

**Step 4: Verify First Run**
```bash
# Check Azure Blob Storage
az storage blob list \
  --container research-indices \
  --prefix state/ \
  --output table

az storage blob list \
  --container research-indices \
  --prefix indices/ \
  --output table

# Verify state files created
az storage blob download \
  --container research-indices \
  --name state/idf_ministry_defense.json \
  --file - | jq
```

### 2. Adding a New Query

**Step 1: Edit `queries.json`**
```bash
# Download current config
az storage blob download \
  --container research-indices \
  --name config/queries.json \
  --file queries.json

# Edit: Add new query to array
nano queries.json

# Upload updated config
az storage blob upload \
  --container research-indices \
  --name config/queries.json \
  --file queries.json \
  --overwrite
```

**Example Addition**:
```json
{
  "query_id": "hamas_statements",
  "enabled": true,
  "query_template": "Get the latest news from Hamas official statements from {start_date} to {end_date}",
  "window_size_days": 7,
  "index_prefix": "hamas_stmt",
  "description": "Hamas official statements and press releases",
  "tags": ["hamas", "statements", "middle-east"],
  "priority": 2
}
```

**Step 2: Verify Next Run**
- Next CronJob run will pick up new query automatically
- New query starts from `2023-09-01` (default start date)
- Watch logs: `kubectl logs -f -n consumers <pod-name>`

### 3. Disabling a Query

**Step 1: Edit `queries.json`**
```bash
# Download, edit, upload (same as adding)
az storage blob download \
  --container research-indices \
  --name config/queries.json \
  --file queries.json

# Change: "enabled": false
nano queries.json

az storage blob upload \
  --container research-indices \
  --name config/queries.json \
  --file queries.json \
  --overwrite
```

**Effect**: Query skipped on next run; state file preserved.

### 4. Manually Advancing a Query

**Scenario**: Query stuck or needs to skip ahead.

**Step 1: Edit State File**
```bash
# Download state
az storage blob download \
  --container research-indices \
  --name state/idf_ministry_defense.json \
  --file state.json

# Edit: Change next_window_start and next_window_end
nano state.json

# Upload
az storage blob upload \
  --container research-indices \
  --name state/idf_ministry_defense.json \
  --file state.json \
  --overwrite
```

**Example**: Skip from 2024-01-15 to 2024-03-01
```json
{
  "query_id": "idf_ministry_defense",
  "next_window_start": "2024-03-01",
  "next_window_end": "2024-03-15"
}
```

### 5. Resetting a Query

**Scenario**: Reindex from scratch.

**Step 1: Delete State File**
```bash
az storage blob delete \
  --container research-indices \
  --name state/idf_ministry_defense.json
```

**Step 2: (Optional) Delete Old Indices**
```bash
az storage blob delete-batch \
  --source research-indices \
  --pattern 'indices/idf_ministry_defense/*'
```

**Effect**: Next run starts from `2023-09-01` (default).

### 6. Troubleshooting Failed Runs

**Check CronJob Status**:
```bash
kubectl get cronjob -n consumers
kubectl get jobs -n consumers -l cronjob=time-series-research

# View failed job logs
kubectl logs -n consumers job/<failed-job-name>
```

**Check Metrics**:
```bash
# Port-forward metrics endpoint
kubectl port-forward -n consumers <pod-name> 8010:8010
curl http://localhost:8010/metrics | grep cronjob_errors
```

**Check State Files**:
```bash
# Verify state file integrity
az storage blob download \
  --container research-indices \
  --name state/idf_ministry_defense.json \
  --file - | jq
```

**Common Issues**:
1. **RAG API timeout**: Increase `REQUEST_TIMEOUT` env var
2. **Azure auth failure**: Check `AZURE_STORAGE_CONNECTION_STRING` secret
3. **Kafka connection failure**: Verify `KAFKA_BOOTSTRAP_SERVERS`
4. **Invalid queries.json**: Validate JSON syntax
5. **State file corrupted**: Delete and reset to default

---

## Scaling and Performance

### 1. Catch-Up Strategy

**Scenario**: New query added; needs to catch up from 2023-09-01 to today.

**Problem**: 16 months = ~35 windows at 14 days each = ~35 CronJob runs

**Solutions**:

**Option A: Increase CronJob Frequency (Recommended)**
```yaml
# Run every hour during catch-up
schedule: "0 * * * *"
```
- Processes 24 windows/day
- Catches up in ~2 days
- Revert to daily schedule after catch-up

**Option B: Parallel Window Processing**
```python
# Process multiple windows per query per run
WINDOWS_PER_RUN = 5  # Process 5 windows per CronJob
```
- Faster catch-up
- Requires more resources
- Risk: Longer CronJob execution time

**Option C: One-Off Catch-Up Job**
```bash
# Run dedicated catch-up Job
kubectl create job time-series-catchup-hamas \
  --from=cronjob/time-series-research-indexer \
  -n consumers

# Set env var: QUERY_ID_FILTER=hamas_statements
# Set env var: WINDOWS_PER_RUN=10
```

### 2. Resource Optimization

**Current Resources**:
- CPU: 200m request, 1000m limit
- Memory: 512Mi request, 2Gi limit

**Scaling Up** (for more queries or faster execution):
```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 2000m
    memory: 4Gi
```

**Horizontal Scaling** (process queries in parallel Jobs):
- Create separate CronJob per query
- Each CronJob runs independently
- No coordination needed
- Simpler state management

### 3. Query Prioritization

**Use Case**: Process high-priority queries first.

**Implementation**: Sort queries by `priority` field before execution
```python
queries = sorted(queries, key=lambda q: q['priority'])
```

**Effect**: Lower priority number = earlier execution.

---

## Migration Plan

### Phase 1: Test in Development (Week 1)

1. **Build Docker Image**
   - Convert script to async execution
   - Add Azure Blob integration
   - Add state management
   - Add Prometheus metrics
   - Test locally with Docker Compose

2. **Deploy to Dev Cluster**
   - Create dev namespace
   - Deploy CronJob with hourly schedule
   - Use test Azure Storage container
   - Run 5-10 iterations
   - Verify state updates

3. **Validate Functionality**
   - Check state files created correctly
   - Verify indices saved to Azure
   - Confirm Kafka messages published
   - Test error handling (simulate RAG failures)

### Phase 2: Staging Deployment (Week 2)

1. **Deploy to Staging**
   - Production-like Azure Storage
   - Same Kafka cluster as prod
   - Daily schedule
   - Monitor for 7 days

2. **Load Testing**
   - Add 10 test queries
   - Verify concurrent execution
   - Check resource usage (CPU, memory)
   - Measure execution time

3. **Disaster Recovery Testing**
   - Delete state file → Verify fallback to 2023-09-01
   - Corrupt queries.json → Verify error handling
   - Kill pod mid-execution → Verify no state corruption

### Phase 3: Production Rollout (Week 3)

1. **Initial Setup**
   - Upload production `queries.json`
   - Deploy CronJob with daily schedule
   - Create Grafana dashboard
   - Configure alerts

2. **Catch-Up Period**
   - Switch to hourly schedule
   - Monitor progress daily
   - Estimate completion time
   - Adjust resources if needed

3. **Steady State**
   - Revert to daily schedule once caught up
   - Monitor metrics weekly
   - Add new queries as needed

### Phase 4: Optimization (Week 4+)

1. **Performance Tuning**
   - Analyze execution times
   - Optimize slow queries
   - Adjust resource limits
   - Fine-tune retry logic

2. **Feature Additions**
   - Query-specific Kafka topics
   - Index aggregation (combine all windows)
   - Query dependencies (query B waits for query A)
   - Dynamic window sizing based on citation density

---

## Summary

### Key Decisions

| **Component** | **Solution** | **Rationale** |
|---------------|--------------|---------------|
| State Storage | Azure Blob (JSON files) | Persistent, versioned, external to cluster |
| Query Config | Azure Blob (`queries.json`) | Updatable without code changes |
| Index Storage | Azure Blob (hierarchical) | Scalable, clear separation per query |
| Execution Model | Async (asyncio.gather) | Parallel query processing |
| Schedule | Daily (1 AM UTC) | Balance freshness vs. resource usage |
| Concurrency | Forbid | Prevent state conflicts |
| Error Handling | Per-query isolation | One failure doesn't block others |
| Monitoring | Prometheus + Grafana | Standard observability stack |

### File Structure

```
ScrapeLinkedWebSources/
├── src/
│   └── cronjob/
│       ├── __init__.py
│       ├── time_series_indexer.py      # Main CronJob script
│       ├── query_config.py             # Query config loader
│       ├── state_manager.py            # State file management
│       ├── index_storage.py            # Azure index storage
│       ├── rag_client.py               # RAG API client
│       ├── kafka_publisher.py          # Kafka publishing
│       └── metrics.py                  # Prometheus metrics
│
├── k8s/
│   ├── time-series-cronjob.yaml
│   ├── time-series-serviceaccount.yaml
│   └── time-series-secrets.yaml.template
│
├── helm/
│   └── time-series-indexer/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│           ├── cronjob.yaml
│           ├── serviceaccount.yaml
│           └── secrets.yaml
│
├── docs/
│   └── CRONJOB_DESIGN.md               # This document
│
└── Dockerfile.cronjob                   # Dedicated Dockerfile
```

### Next Steps

1. **Review this design document** ✓
2. Create Azure storage structure and initial `queries.json`
3. Implement Python modules (state manager, query config, etc.)
4. Build and test Docker image locally
5. Deploy to dev cluster with hourly schedule
6. Validate 10+ iterations in dev
7. Deploy to staging for 1 week observation
8. Production rollout with catch-up period
9. Monitor and optimize

---

**Document Version**: 1.0
**Last Updated**: 2024-12-24
**Author**: Research Indexing Team
**Status**: Design Complete - Ready for Implementation
