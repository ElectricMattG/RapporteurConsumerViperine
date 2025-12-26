# Simplified CronJob Directory Structure

## Overview

Simplified structure with a single Python module, all CronJob files in one `cronjobs/` folder, and deployment scripts in repository root.

---

## Directory Structure

```
ScrapeLinkedWebSources/
├── cronjobs/                                   # NEW: All CronJob files here
│   ├── time_series_indexer.py                 # Single module - everything in one file
│   ├── Dockerfile                              # CronJob Dockerfile
│   ├── requirements.txt                        # Python dependencies
│   ├── helm/                                   # Helm chart
│   │   ├── Chart.yaml
│   │   ├── values.yaml
│   │   ├── README.md
│   │   └── templates/
│   │       ├── _helpers.tpl
│   │       ├── cronjob.yaml
│   │       ├── serviceaccount.yaml
│   │       └── secrets.yaml
│   └── config/                                 # Configuration templates
│       ├── queries.json.example                # Example query config
│       └── state.json.example                  # Example state structure
│
├── build_and_push_cron.sh                      # Build & push to ACR (repo root)
├── deploy_cron.sh                              # Deploy CronJob via Helm (repo root)
│
├── src/                                        # Existing source code
├── k8s/                                        # Existing K8s manifests
├── helm/                                       # Existing Helm charts (consumers)
├── docs/                                       # Documentation
└── [other existing files]
```

---

## Single Module: `time_series_indexer.py`

**All functionality in one file (~500-800 lines)**:

```python
#!/usr/bin/env python3
"""
Time Series Research Indexer - Kubernetes CronJob
Single-file implementation with all components.
"""

import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.parse import urlparse

import httpx
from azure.storage.blob import BlobServiceClient
from confluent_kafka import Producer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from tenacity import retry, stop_after_attempt, wait_exponential


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Configuration from environment variables"""

    # Azure Storage
    azure_connection_string: str
    azure_container: str = "research-indices"
    queries_config_path: str = "config/queries.json"
    state_path_prefix: str = "state"
    indices_path_prefix: str = "indices"

    # RAG API
    rag_api_url: str = "https://4.207.24.92/query"
    rag_api_key: str = ""
    rag_timeout: int = 180

    # Kafka
    kafka_bootstrap_servers: str = "kafka.rapporteur.ai:9094"
    kafka_enabled: bool = True

    # Time windows
    default_start_date: str = "2023-09-01"
    default_window_size_days: int = 14

    # Execution
    max_retries: int = 3

    # Monitoring
    metrics_port: int = 8010
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> 'Config':
        """Load from environment variables"""
        return cls(
            azure_connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
            azure_container=os.getenv('AZURE_STORAGE_CONTAINER', 'research-indices'),
            rag_api_key=os.getenv('RAG_API_KEY', ''),
            kafka_enabled=os.getenv('KAFKA_ENABLED', 'true').lower() == 'true',
            # ... other fields with defaults
        )


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class QueryConfig:
    """Query configuration"""
    query_id: str
    enabled: bool
    query_template: str
    window_size_days: int
    index_prefix: str
    description: str = ""
    tags: List[str] = None
    priority: int = 1


@dataclass
class WindowState:
    """State for a query's time window"""
    query_id: str
    query_template: str
    last_window_end: Optional[str]
    window_size_days: int
    total_windows_processed: int
    next_window_start: str
    next_window_end: str
    last_run_status: Optional[str] = None
    last_run_citations: int = 0


@dataclass
class TimeWindow:
    """A time window to process"""
    start_date: datetime
    end_date: datetime


# ============================================================================
# METRICS
# ============================================================================

class Metrics:
    """Prometheus metrics"""

    def __init__(self, port: int):
        self.port = port

        self.queries_processed = Counter(
            'cronjob_queries_processed_total',
            'Queries processed',
            ['query_id', 'status']
        )

        self.citations_published = Counter(
            'cronjob_citations_published_total',
            'Citations published',
            ['query_id']
        )

        self.execution_duration = Histogram(
            'cronjob_execution_duration_seconds',
            'Execution duration',
            ['query_id']
        )

        self.errors_total = Counter(
            'cronjob_errors_total',
            'Errors',
            ['query_id', 'error_type']
        )

    def start_server(self):
        """Start metrics HTTP server"""
        start_http_server(self.port)


# ============================================================================
# QUERY MANAGER
# ============================================================================

class QueryManager:
    """Load queries from Azure"""

    def __init__(self, config: Config, blob_client: BlobServiceClient):
        self.config = config
        self.blob_client = blob_client

    async def load_queries(self) -> List[QueryConfig]:
        """Load enabled queries from Azure"""
        # Download queries.json
        # Parse and filter enabled
        # Return list of QueryConfig
        pass


# ============================================================================
# STATE MANAGER
# ============================================================================

class StateManager:
    """Manage query window state"""

    def __init__(self, config: Config, blob_client: BlobServiceClient):
        self.config = config
        self.blob_client = blob_client

    async def load_state(self, query: QueryConfig) -> WindowState:
        """Load state or create initial"""
        pass

    async def save_state(self, state: WindowState):
        """Save state to Azure"""
        pass


# ============================================================================
# WINDOW CALCULATOR
# ============================================================================

class WindowCalculator:
    """Calculate time windows"""

    def __init__(self, config: Config):
        self.config = config

    def calculate_next_window(self, state: WindowState) -> Optional[TimeWindow]:
        """Calculate next window to process"""
        pass

    def format_query_string(self, template: str, window: TimeWindow) -> str:
        """Format query with dates"""
        pass


# ============================================================================
# RAG EXECUTOR
# ============================================================================

class RAGExecutor:
    """Execute RAG queries"""

    def __init__(self, config: Config):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=config.rag_timeout,
            verify=False
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    async def execute_query(self, query: str, session_id: str) -> Dict[str, Any]:
        """Execute RAG query with retry"""
        pass


# ============================================================================
# KAFKA PUBLISHER
# ============================================================================

class KafkaPublisher:
    """Publish citations to Kafka"""

    def __init__(self, config: Config):
        self.config = config
        if config.kafka_enabled:
            self.producer = Producer({
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'client.id': 'time-series-indexer',
                'acks': 'all',
            })
        else:
            self.producer = None

    async def publish_citations(self, rag_response: Dict, session_id: str) -> int:
        """Publish citations to Kafka"""
        pass


# ============================================================================
# INDEX STORAGE
# ============================================================================

class IndexStorage:
    """Store indices in Azure"""

    def __init__(self, config: Config, blob_client: BlobServiceClient):
        self.config = config
        self.blob_client = blob_client

    async def save_index(
        self,
        query: QueryConfig,
        window: TimeWindow,
        rag_response: Dict,
        kafka_metadata: Dict
    ) -> str:
        """Save index file to Azure"""
        pass


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class TimeSeriesIndexer:
    """Main CronJob orchestrator"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.metrics = Metrics(config.metrics_port)
        self.blob_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )

        self.query_manager = QueryManager(config, self.blob_client)
        self.state_manager = StateManager(config, self.blob_client)
        self.window_calculator = WindowCalculator(config)
        self.rag_executor = RAGExecutor(config)
        self.kafka_publisher = KafkaPublisher(config)
        self.index_storage = IndexStorage(config, self.blob_client)

    async def run(self) -> int:
        """
        Execute CronJob workflow
        Returns: Exit code (0=success, 1=failure)
        """
        try:
            # Start metrics server
            self.metrics.start_server()
            self.logger.info("Metrics server started on port %d", self.config.metrics_port)

            # Load queries
            queries = await self.query_manager.load_queries()
            self.logger.info("Loaded %d enabled queries", len(queries))

            if not queries:
                self.logger.warning("No enabled queries found")
                return 0

            # Process queries asynchronously
            results = await asyncio.gather(
                *[self._process_query(query) for query in queries],
                return_exceptions=True
            )

            # Count successes and failures
            successes = sum(1 for r in results if r is True)
            failures = len(results) - successes

            self.logger.info("Execution complete: %d succeeded, %d failed", successes, failures)

            return 0 if failures == 0 else 1

        except Exception as e:
            self.logger.exception("Fatal error: %s", e)
            return 1

    async def _process_query(self, query: QueryConfig) -> bool:
        """Process a single query"""
        try:
            self.logger.info("Processing query: %s", query.query_id)

            # Load state
            state = await self.state_manager.load_state(query)

            # Calculate next window
            window = self.window_calculator.calculate_next_window(state)
            if not window:
                self.logger.info("Query %s is up-to-date", query.query_id)
                return True

            self.logger.info(
                "Processing window %s to %s",
                window.start_date.strftime("%Y-%m-%d"),
                window.end_date.strftime("%Y-%m-%d")
            )

            # Format query string
            query_string = self.window_calculator.format_query_string(
                query.query_template, window
            )

            # Execute RAG query
            session_id = f"time_series_{query.query_id}_{window.start_date.strftime('%Y%m%d')}_{window.end_date.strftime('%Y%m%d')}"
            rag_response = await self.rag_executor.execute_query(query_string, session_id)

            # Publish to Kafka
            citations_published = 0
            if self.config.kafka_enabled:
                citations_published = await self.kafka_publisher.publish_citations(
                    rag_response, session_id
                )
                self.logger.info("Published %d citations to Kafka", citations_published)

            # Save index
            index_file = await self.index_storage.save_index(
                query, window, rag_response, {"citations_published": citations_published}
            )
            self.logger.info("Saved index: %s", index_file)

            # Update state
            state.last_window_end = window.end_date.strftime("%Y-%m-%d")
            state.total_windows_processed += 1
            state.last_run_status = "success"
            state.last_run_citations = citations_published
            state.next_window_start = window.end_date.strftime("%Y-%m-%d")

            await self.state_manager.save_state(state)

            # Update metrics
            self.metrics.queries_processed.labels(
                query_id=query.query_id, status='success'
            ).inc()
            self.metrics.citations_published.labels(query_id=query.query_id).inc(citations_published)

            return True

        except Exception as e:
            self.logger.exception("Error processing query %s: %s", query.query_id, e)
            self.metrics.queries_processed.labels(
                query_id=query.query_id, status='failure'
            ).inc()
            self.metrics.errors_total.labels(
                query_id=query.query_id, error_type=type(e).__name__
            ).inc()
            return False


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def setup_logging(level: str):
    """Configure logging"""
    logging.basicConfig(
        level=getattr(logging, level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    """Main entry point"""
    # Load configuration
    config = Config.from_env()

    # Setup logging
    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Starting Time Series Research Indexer")
    logger.info("Azure container: %s", config.azure_container)
    logger.info("Kafka enabled: %s", config.kafka_enabled)

    # Create indexer
    indexer = TimeSeriesIndexer(config)

    # Run
    exit_code = asyncio.run(indexer.run())

    logger.info("Exiting with code %d", exit_code)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
```

**Estimated file size**: 500-800 lines (fully implemented)

---

## Dockerfile

**Location**: `cronjobs/Dockerfile`

```dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy CronJob script
COPY time_series_indexer.py .

# Environment
ENV PYTHONUNBUFFERED=1

# Entry point
CMD ["python", "time_series_indexer.py"]
```

---

## Requirements

**Location**: `cronjobs/requirements.txt`

```txt
httpx==0.27.0
azure-storage-blob==12.19.0
confluent-kafka==2.3.0
prometheus-client==0.20.0
tenacity==8.2.3
```

---

## Helm Chart

**Location**: `cronjobs/helm/`

### Chart.yaml
```yaml
apiVersion: v2
name: time-series-indexer
description: Time Series Research Indexer CronJob
version: 1.0.0
appVersion: "1.0.0"
```

### values.yaml
```yaml
image:
  repository: ragserviceneacr6881.azurecr.io/time-series-indexer
  tag: latest
  pullPolicy: Always

imagePullSecrets:
  - name: acr-secret

schedule: "0 1 * * *"  # Daily at 1 AM UTC
concurrencyPolicy: Forbid
successfulJobsHistoryLimit: 3
failedJobsHistoryLimit: 3

# Azure Storage
azure:
  storageConnectionString: ""  # Set via --set
  container: research-indices

# RAG API
rag:
  apiUrl: "https://4.207.24.92/query"
  apiKey: ""  # Set via --set
  timeout: 180

# Kafka
kafka:
  bootstrapServers: "kafka.rapporteur.ai:9094"
  enabled: true

# Configuration paths
config:
  queriesPath: "config/queries.json"
  statePath: "state"
  indicesPath: "indices"
  defaultStartDate: "2023-09-01"
  defaultWindowSize: 14

# Resources
resources:
  requests:
    cpu: 200m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 2Gi

# Monitoring
metrics:
  port: 8010

# Logging
logging:
  level: INFO

# ServiceAccount
serviceAccount:
  create: true
  name: time-series-indexer
```

### templates/cronjob.yaml
```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ include "time-series-indexer.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "time-series-indexer.labels" . | nindent 4 }}
spec:
  schedule: {{ .Values.schedule | quote }}
  concurrencyPolicy: {{ .Values.concurrencyPolicy }}
  successfulJobsHistoryLimit: {{ .Values.successfulJobsHistoryLimit }}
  failedJobsHistoryLimit: {{ .Values.failedJobsHistoryLimit }}
  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            {{- include "time-series-indexer.labels" . | nindent 12 }}
        spec:
          serviceAccountName: {{ include "time-series-indexer.serviceAccountName" . }}
          restartPolicy: OnFailure

          {{- with .Values.imagePullSecrets }}
          imagePullSecrets:
            {{- toYaml . | nindent 12 }}
          {{- end }}

          containers:
          - name: indexer
            image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
            imagePullPolicy: {{ .Values.image.pullPolicy }}

            env:
            # Azure
            - name: AZURE_STORAGE_CONNECTION_STRING
              valueFrom:
                secretKeyRef:
                  name: {{ include "time-series-indexer.fullname" . }}-secrets
                  key: azure-connection-string
            - name: AZURE_STORAGE_CONTAINER
              value: {{ .Values.azure.container | quote }}

            # RAG
            - name: RAG_API_URL
              value: {{ .Values.rag.apiUrl | quote }}
            - name: RAG_API_KEY
              valueFrom:
                secretKeyRef:
                  name: {{ include "time-series-indexer.fullname" . }}-secrets
                  key: rag-api-key
            - name: RAG_TIMEOUT
              value: {{ .Values.rag.timeout | quote }}

            # Kafka
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: {{ .Values.kafka.bootstrapServers | quote }}
            - name: KAFKA_ENABLED
              value: {{ .Values.kafka.enabled | quote }}

            # Config
            - name: QUERIES_CONFIG_PATH
              value: {{ .Values.config.queriesPath | quote }}
            - name: STATE_PATH_PREFIX
              value: {{ .Values.config.statePath | quote }}
            - name: INDICES_PATH_PREFIX
              value: {{ .Values.config.indicesPath | quote }}
            - name: DEFAULT_START_DATE
              value: {{ .Values.config.defaultStartDate | quote }}
            - name: DEFAULT_WINDOW_SIZE_DAYS
              value: {{ .Values.config.defaultWindowSize | quote }}

            # Monitoring
            - name: METRICS_PORT
              value: {{ .Values.metrics.port | quote }}
            - name: LOG_LEVEL
              value: {{ .Values.logging.level | quote }}

            resources:
              {{- toYaml .Values.resources | nindent 14 }}

            ports:
            - name: metrics
              containerPort: {{ .Values.metrics.port }}
              protocol: TCP
```

### templates/serviceaccount.yaml
```yaml
{{- if .Values.serviceAccount.create -}}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "time-series-indexer.serviceAccountName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "time-series-indexer.labels" . | nindent 4 }}
{{- end }}
```

### templates/secrets.yaml
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "time-series-indexer.fullname" . }}-secrets
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "time-series-indexer.labels" . | nindent 4 }}
type: Opaque
stringData:
  azure-connection-string: {{ .Values.azure.storageConnectionString | quote }}
  rag-api-key: {{ .Values.rag.apiKey | quote }}
```

### templates/_helpers.tpl
```yaml
{{- define "time-series-indexer.fullname" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "time-series-indexer.labels" -}}
app.kubernetes.io/name: {{ include "time-series-indexer.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "time-series-indexer.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "time-series-indexer.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
```

---

## Configuration Examples

### queries.json.example
**Location**: `cronjobs/config/queries.json.example`

```json
{
  "version": "1.0",
  "updated_at": "2024-12-24T00:00:00Z",
  "queries": [
    {
      "query_id": "idf_ministry_defense",
      "enabled": true,
      "query_template": "Get the latest news from the Israeli Ministry of Defense from {start_date} to {end_date}",
      "window_size_days": 14,
      "index_prefix": "idf_mod",
      "description": "Israeli Ministry of Defense news",
      "tags": ["military", "israel"],
      "priority": 1
    },
    {
      "query_id": "israel_prime_minister",
      "enabled": true,
      "query_template": "Get the latest news from the Israeli Prime Minister from {start_date} to {end_date}",
      "window_size_days": 14,
      "index_prefix": "israel_pm",
      "description": "Israeli Prime Minister statements",
      "tags": ["politics", "israel"],
      "priority": 1
    }
  ]
}
```

### state.json.example
**Location**: `cronjobs/config/state.json.example`

```json
{
  "query_id": "idf_ministry_defense",
  "query_template": "Get the latest news from the Israeli Ministry of Defense from {start_date} to {end_date}",
  "last_window_end": "2024-01-15",
  "window_size_days": 14,
  "total_windows_processed": 10,
  "next_window_start": "2024-01-15",
  "next_window_end": "2024-01-29",
  "last_run_status": "success",
  "last_run_citations": 42
}
```

---

## Deployment Scripts

### build_and_push_cron.sh
**Location**: Repository root

```bash
#!/bin/bash
set -e

ACR_NAME="ragserviceneacr6881"
ACR_REGISTRY="${ACR_NAME}.azurecr.io"
IMAGE_NAME="time-series-indexer"
IMAGE_TAG="${IMAGE_TAG:-latest}"

echo "================================================"
echo "Building Time Series CronJob Image"
echo "================================================"

# Login to ACR
echo "Logging in to ACR..."
az acr login --name $ACR_NAME

# Build image
echo "Building Docker image..."
cd cronjobs/
docker buildx build \
    --platform linux/amd64 \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:latest \
    .
cd ..

# Push image
echo "Pushing image to ACR..."
docker push ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
docker push ${ACR_REGISTRY}/${IMAGE_NAME}:latest

echo ""
echo "✅ Image built and pushed successfully"
echo "   ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
echo "   ${ACR_REGISTRY}/${IMAGE_NAME}:latest"
echo ""
```

### deploy_cron.sh
**Location**: Repository root

```bash
#!/bin/bash
set -e

NAMESPACE="consumers"
CHART_PATH="./cronjobs/helm"
RELEASE_NAME="time-series-indexer"

echo "================================================"
echo "Deploying Time Series CronJob"
echo "================================================"

# Check required environment variables
if [ -z "$AZURE_CONNECTION_STRING" ]; then
    echo "❌ Error: AZURE_CONNECTION_STRING not set"
    exit 1
fi

if [ -z "$RAG_API_KEY" ]; then
    echo "❌ Error: RAG_API_KEY not set"
    exit 1
fi

# Deploy via Helm
echo "Deploying with Helm..."
helm upgrade --install $RELEASE_NAME $CHART_PATH \
    --namespace $NAMESPACE \
    --create-namespace \
    --set azure.storageConnectionString="$AZURE_CONNECTION_STRING" \
    --set rag.apiKey="$RAG_API_KEY" \
    --wait

echo ""
echo "✅ CronJob deployed successfully"
echo ""

# Show CronJob status
echo "CronJob status:"
kubectl get cronjob -n $NAMESPACE $RELEASE_NAME

echo ""
echo "Useful commands:"
echo "  View CronJob:       kubectl get cronjob -n $NAMESPACE"
echo "  Trigger manual run: kubectl create job test-run --from=cronjob/$RELEASE_NAME -n $NAMESPACE"
echo "  View logs:          kubectl logs -f -n $NAMESPACE job/<job-name>"
echo "  Delete CronJob:     helm uninstall $RELEASE_NAME -n $NAMESPACE"
echo ""
```

---

## Usage Workflow

### Initial Setup

1. **Upload queries config to Azure**:
```bash
az storage blob upload \
  --account-name <storage-account> \
  --container-name research-indices \
  --name config/queries.json \
  --file cronjobs/config/queries.json.example
```

2. **Set environment variables**:
```bash
export AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=https;..."
export RAG_API_KEY="32d160f623efa11ebd88d34ca0b9f4093eb1f866a640d4b70b0fbafb6c22e398"
```

3. **Build and push**:
```bash
./build_and_push_cron.sh
```

4. **Deploy**:
```bash
./deploy_cron.sh
```

### Manual Test Run

```bash
kubectl create job test-run \
  --from=cronjob/time-series-indexer \
  -n consumers

kubectl logs -f -n consumers job/test-run
```

### Update Query Configuration

```bash
# Download current config
az storage blob download \
  --account-name <storage-account> \
  --container-name research-indices \
  --name config/queries.json \
  --file queries.json

# Edit queries.json
nano queries.json

# Upload updated config
az storage blob upload \
  --account-name <storage-account> \
  --container-name research-indices \
  --name config/queries.json \
  --file queries.json \
  --overwrite
```

Next CronJob run will pick up changes automatically.

---

## Summary

### File Count
- **Python**: 1 file (`time_series_indexer.py`)
- **Docker**: 2 files (`Dockerfile`, `requirements.txt`)
- **Helm**: 7 files (Chart.yaml, values.yaml, 5 templates)
- **Config examples**: 2 files
- **Deployment scripts**: 2 files (in repo root)
- **Total**: ~14 files

### Benefits
✅ **Simple**: Single Python file, easy to understand and modify
✅ **Organized**: All CronJob files in one `cronjobs/` folder
✅ **Deployable**: Scripts in repo root for easy access
✅ **Maintainable**: Helm chart for configuration management
✅ **Testable**: Can run locally or trigger manual Job

### Tradeoffs
⚠️ **Large file**: 500-800 lines (but well-organized with sections)
⚠️ **Harder to unit test**: Less modular than multi-file approach
⚠️ **Less reusable**: Components not easily extracted for other uses

For a CronJob with focused purpose, this is a good balance of simplicity and functionality.
