# CronJob Directory Structure and Integration Plan

## Overview

This document outlines the directory structure for the time-series research indexer CronJob, how it integrates with the existing repository structure, and the organization of code modules.

---

## Repository Integration

### Current Repository Structure
```
ScrapeLinkedWebSources/
├── src/
│   ├── clients/           # LLM, embedding clients
│   ├── components/        # Core components (scrapers, chunkers, embedders)
│   ├── consumers/         # Kafka consumers
│   ├── demos/             # Example workflows
│   ├── models/            # Data models
│   ├── prompts/           # Prompt templates
│   └── utils/             # Utility functions
├── k8s/                   # Kubernetes manifests
├── helm/                  # Helm charts
├── docs/                  # Documentation
├── tests/                 # Test files
└── [deployment scripts]   # build-and-push.sh, deploy-monitoring.sh, etc.
```

### New CronJob Structure (Integrated)
```
ScrapeLinkedWebSources/
├── src/
│   ├── clients/
│   ├── components/
│   ├── consumers/
│   ├── cronjobs/                          # NEW: CronJob implementations
│   │   ├── __init__.py
│   │   ├── time_series/                   # Time-series research indexer
│   │   │   ├── __init__.py
│   │   │   ├── main.py                    # Entry point script
│   │   │   ├── config.py                  # Configuration constants
│   │   │   ├── models.py                  # Data models (QueryConfig, WindowState)
│   │   │   ├── query_manager.py           # Query config loader from Azure
│   │   │   ├── state_manager.py           # State persistence (Azure Blob)
│   │   │   ├── window_calculator.py       # Time window calculation logic
│   │   │   ├── rag_executor.py            # RAG API client and execution
│   │   │   ├── kafka_publisher.py         # Kafka message publishing
│   │   │   ├── index_storage.py           # Azure index file storage
│   │   │   ├── metrics.py                 # Prometheus metrics
│   │   │   └── exceptions.py              # Custom exceptions
│   │   │
│   │   └── [future_cronjobs]/             # Other CronJobs can be added here
│   │
│   ├── demos/
│   ├── models/
│   ├── prompts/
│   └── utils/
│
├── k8s/
│   ├── deployment.yaml                     # Existing consumer deployments
│   ├── cronjobs/                           # NEW: CronJob manifests
│   │   ├── time-series-cronjob.yaml       # CronJob resource
│   │   ├── time-series-serviceaccount.yaml # RBAC resources
│   │   └── time-series-secrets.yaml.template # Secret templates
│   └── monitoring/                         # Monitoring resources (if not in helm)
│
├── helm/
│   ├── rag-frontend/
│   ├── research-indexing-consumers/        # Existing consumers
│   └── time-series-indexer/                # NEW: CronJob Helm chart
│       ├── Chart.yaml
│       ├── values.yaml
│       ├── templates/
│       │   ├── cronjob.yaml
│       │   ├── serviceaccount.yaml
│       │   ├── secrets.yaml
│       │   ├── configmap.yaml              # For non-sensitive config
│       │   └── _helpers.tpl
│       └── README.md
│
├── docker/
│   ├── Dockerfile.consumer                 # Existing consumer Dockerfile
│   └── Dockerfile.cronjob                  # NEW: CronJob-specific Dockerfile
│
├── scripts/                                # Deployment scripts
│   ├── build-and-push.sh                   # Existing consumer deployment
│   ├── build-and-push-cronjob.sh           # NEW: CronJob deployment
│   ├── deploy-monitoring.sh
│   └── deploy-cronjob.sh                   # NEW: Deploy CronJob to cluster
│
├── config/                                 # NEW: Configuration templates
│   └── time-series/
│       ├── queries.json.template           # Template for Azure queries config
│       └── state.json.template             # Template for state file structure
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── CRONJOB_DESIGN.md                   # Design document (already created)
│   ├── CRONJOB_DIRECTORY_STRUCTURE.md      # This document
│   └── CRONJOB_DEPLOYMENT.md               # Deployment guide (to be created)
│
├── tests/
│   ├── unit/
│   │   └── cronjobs/
│   │       └── time_series/
│   │           ├── test_query_manager.py
│   │           ├── test_state_manager.py
│   │           ├── test_window_calculator.py
│   │           └── test_rag_executor.py
│   └── integration/
│       └── cronjobs/
│           └── test_time_series_e2e.py
│
├── time_series_research_queries.py         # Original script (keep for reference)
├── requirements.txt
├── dev-requirements.txt
└── pyproject.toml
```

---

## Module Organization

### `src/cronjobs/time_series/` - Detailed Breakdown

#### **main.py** - Entry Point
**Purpose**: Main execution script triggered by Kubernetes CronJob

**Responsibilities**:
- Parse command-line arguments and environment variables
- Initialize logging and metrics server
- Orchestrate the 5-phase execution workflow
- Handle top-level error catching and exit codes

**Structure**:
```python
#!/usr/bin/env python3
"""
Time Series Research Indexer - Kubernetes CronJob Entry Point
"""

import asyncio
import sys
from datetime import datetime
from typing import List

from .config import Config
from .metrics import CronJobMetrics
from .query_manager import QueryManager
from .state_manager import StateManager
from .window_calculator import WindowCalculator
from .rag_executor import RAGExecutor
from .kafka_publisher import KafkaPublisher
from .index_storage import IndexStorage
from .exceptions import CronJobError, ConfigurationError, StateError


class TimeSeriesIndexer:
    """Main CronJob orchestrator"""

    def __init__(self, config: Config):
        self.config = config
        self.metrics = CronJobMetrics(port=config.metrics_port)
        self.query_manager = QueryManager(config)
        self.state_manager = StateManager(config)
        self.window_calculator = WindowCalculator(config)
        self.rag_executor = RAGExecutor(config)
        self.kafka_publisher = KafkaPublisher(config)
        self.index_storage = IndexStorage(config)

    async def run(self) -> int:
        """
        Execute CronJob workflow
        Returns: Exit code (0=success, 1=failure)
        """
        try:
            # Phase 1: Initialize
            await self._initialize()

            # Phase 2: Load queries and state
            queries = await self._load_queries()

            # Phase 3: Execute queries asynchronously
            results = await self._execute_queries(queries)

            # Phase 4: Update metrics and log summary
            self._report_results(results)

            # Phase 5: Cleanup
            await self._cleanup()

            return 0 if all(r.success for r in results) else 1

        except ConfigurationError as e:
            logger.error(f"Configuration error: {e}")
            return 1
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            return 1

    async def _initialize(self):
        """Phase 1: Initialize dependencies"""
        pass

    async def _load_queries(self) -> List[QueryConfig]:
        """Phase 2: Load and validate queries"""
        pass

    async def _execute_queries(self, queries) -> List[QueryResult]:
        """Phase 3: Execute queries in parallel"""
        pass

    def _report_results(self, results):
        """Phase 4: Report results and metrics"""
        pass

    async def _cleanup(self):
        """Phase 5: Cleanup resources"""
        pass


def main():
    """Main entry point"""
    config = Config.from_env()
    indexer = TimeSeriesIndexer(config)

    # Run async event loop
    exit_code = asyncio.run(indexer.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
```

---

#### **config.py** - Configuration Management
**Purpose**: Centralize all configuration constants and environment variable parsing

**Structure**:
```python
"""Configuration constants and environment variable parsing"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    """CronJob configuration"""

    # Azure Storage
    azure_connection_string: str
    azure_container: str
    queries_config_path: str
    state_path_prefix: str
    indices_path_prefix: str

    # RAG API
    rag_api_url: str
    rag_api_key: str
    rag_timeout: int

    # Kafka
    kafka_bootstrap_servers: str
    kafka_enabled: bool

    # Time windows
    default_start_date: str
    default_window_size_days: int

    # Execution
    max_retries: int
    retry_delay_seconds: int

    # Monitoring
    metrics_port: int
    log_level: str

    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables"""
        return cls(
            # Azure
            azure_connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING'),
            azure_container=os.getenv('AZURE_STORAGE_CONTAINER', 'research-indices'),
            queries_config_path=os.getenv('QUERIES_CONFIG_PATH', 'config/queries.json'),
            state_path_prefix=os.getenv('STATE_PATH_PREFIX', 'state'),
            indices_path_prefix=os.getenv('INDICES_PATH_PREFIX', 'indices'),

            # RAG
            rag_api_url=os.getenv('RAG_API_URL', 'https://4.207.24.92/query'),
            rag_api_key=os.getenv('RAG_API_KEY'),
            rag_timeout=int(os.getenv('RAG_TIMEOUT', '180')),

            # Kafka
            kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka.rapporteur.ai:9094'),
            kafka_enabled=os.getenv('KAFKA_ENABLED', 'true').lower() == 'true',

            # Windows
            default_start_date=os.getenv('DEFAULT_START_DATE', '2023-09-01'),
            default_window_size_days=int(os.getenv('DEFAULT_WINDOW_SIZE_DAYS', '14')),

            # Execution
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay_seconds=int(os.getenv('RETRY_DELAY_SECONDS', '5')),

            # Monitoring
            metrics_port=int(os.getenv('METRICS_PORT', '8010')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
        )

    def validate(self):
        """Validate required configuration"""
        if not self.azure_connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING required")
        if not self.rag_api_key:
            raise ValueError("RAG_API_KEY required")
```

---

#### **models.py** - Data Models
**Purpose**: Define data structures for queries, state, and results

**Structure**:
```python
"""Data models for time series indexer"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class QueryConfig:
    """Query configuration from Azure"""
    query_id: str
    enabled: bool
    query_template: str
    window_size_days: int
    index_prefix: str
    description: str
    tags: List[str]
    priority: int


@dataclass
class WindowState:
    """State for a single query's time window"""
    query_id: str
    query_template: str
    last_window_end: Optional[str]
    last_window_start: Optional[str]
    window_size_days: int
    total_windows_processed: int
    last_run_timestamp: Optional[str]
    last_run_status: Optional[str]
    last_run_citations: int
    next_window_start: str
    next_window_end: str

    @classmethod
    def create_initial(cls, query: QueryConfig, default_start: str, window_days: int) -> 'WindowState':
        """Create initial state for a new query"""
        pass


@dataclass
class TimeWindow:
    """A time window to process"""
    start_date: datetime
    end_date: datetime

    def format_start(self) -> str:
        return self.start_date.strftime("%Y-%m-%d")

    def format_end(self) -> str:
        return self.end_date.strftime("%Y-%m-%d")


@dataclass
class RAGResponse:
    """RAG API response"""
    query: str
    answer: str
    processing_time_ms: int
    request_id: str
    session_id: str
    grounding_metadata: Dict[str, Any]
    citations: List[Dict[str, Any]]


@dataclass
class QueryResult:
    """Result of processing a single query window"""
    query_id: str
    window: TimeWindow
    success: bool
    rag_response: Optional[RAGResponse]
    citations_published: int
    index_saved: bool
    error: Optional[str]
    execution_time_seconds: float
```

---

#### **query_manager.py** - Query Configuration Loader
**Purpose**: Load and parse queries from Azure Blob Storage

**Key Functions**:
```python
"""Query configuration management"""

from typing import List
from azure.storage.blob import BlobServiceClient

from .models import QueryConfig
from .config import Config


class QueryManager:
    """Manage query configurations from Azure"""

    def __init__(self, config: Config):
        self.config = config
        self.blob_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )

    async def load_queries(self) -> List[QueryConfig]:
        """
        Load queries from Azure Blob Storage
        Returns only enabled queries, sorted by priority
        """
        pass

    async def download_config(self) -> dict:
        """Download queries.json from Azure"""
        pass

    def parse_queries(self, config_data: dict) -> List[QueryConfig]:
        """Parse and validate query configurations"""
        pass

    def filter_enabled(self, queries: List[QueryConfig]) -> List[QueryConfig]:
        """Filter to enabled queries only"""
        pass
```

---

#### **state_manager.py** - State Persistence
**Purpose**: Read/write state files from Azure Blob Storage

**Key Functions**:
```python
"""State persistence for query windows"""

from typing import Optional
from azure.storage.blob import BlobServiceClient

from .models import WindowState, QueryConfig
from .config import Config


class StateManager:
    """Manage query window state in Azure Blob Storage"""

    def __init__(self, config: Config):
        self.config = config
        self.blob_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )

    async def load_state(self, query: QueryConfig) -> WindowState:
        """
        Load state for a query
        Returns existing state or creates initial state with fallback date
        """
        pass

    async def save_state(self, state: WindowState):
        """Save updated state to Azure"""
        pass

    def _get_blob_path(self, query_id: str) -> str:
        """Get blob path for state file"""
        return f"{self.config.state_path_prefix}/{query_id}.json"

    def _create_initial_state(self, query: QueryConfig) -> WindowState:
        """Create initial state with fallback date"""
        pass
```

---

#### **window_calculator.py** - Time Window Logic
**Purpose**: Calculate next time window based on state

**Key Functions**:
```python
"""Time window calculation logic"""

from datetime import datetime, timedelta
from typing import Optional

from .models import TimeWindow, WindowState, QueryConfig
from .config import Config


class WindowCalculator:
    """Calculate time windows for queries"""

    def __init__(self, config: Config):
        self.config = config

    def calculate_next_window(self, state: WindowState) -> Optional[TimeWindow]:
        """
        Calculate next window to process
        Returns None if query is up-to-date (next_window_start >= today)
        """
        pass

    def should_skip_query(self, window: TimeWindow) -> bool:
        """Check if window should be skipped (too far in future)"""
        pass

    def format_query_string(self, query_template: str, window: TimeWindow) -> str:
        """Format query template with date range"""
        return query_template.format(
            start_date=window.format_start(),
            end_date=window.format_end()
        )
```

---

#### **rag_executor.py** - RAG API Client
**Purpose**: Execute RAG queries with retry logic

**Key Functions**:
```python
"""RAG API client and executor"""

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from .models import RAGResponse, TimeWindow
from .config import Config


class RAGExecutor:
    """Execute RAG queries with retry logic"""

    def __init__(self, config: Config):
        self.config = config
        self.client = httpx.AsyncClient(
            timeout=config.rag_timeout,
            verify=False  # Skip SSL verification
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30)
    )
    async def execute_query(self, query: str, session_id: str) -> RAGResponse:
        """
        Execute RAG query with retry logic
        Raises exception on failure after retries
        """
        pass

    def _build_payload(self, query: str) -> dict:
        """Build RAG API request payload"""
        return {
            "query": query,
            "processing_mode": "research",
            "enable_query_refinement": False,
            "top_k": 3
        }

    def _parse_response(self, response_data: dict) -> RAGResponse:
        """Parse RAG API response into model"""
        pass
```

---

#### **kafka_publisher.py** - Kafka Integration
**Purpose**: Publish citations to Kafka topics

**Key Functions**:
```python
"""Kafka message publishing"""

from typing import List, Dict
from confluent_kafka import Producer
from urllib.parse import urlparse

from .models import RAGResponse
from .config import Config


class KafkaPublisher:
    """Publish citations to Kafka topics"""

    def __init__(self, config: Config):
        self.config = config
        if config.kafka_enabled:
            self.producer = Producer({
                'bootstrap.servers': config.kafka_bootstrap_servers,
                'client.id': 'time-series-indexer',
                'acks': 'all',
                'retries': 3,
            })
        else:
            self.producer = None

    async def publish_citations(
        self,
        rag_response: RAGResponse,
        session_id: str
    ) -> int:
        """
        Publish citations to Kafka
        Returns: Number of citations published
        """
        pass

    def _create_messages(self, rag_response: RAGResponse) -> List[Dict]:
        """Create Kafka messages from RAG citations"""
        pass

    async def _send_session_complete(self, session_id: str, total_citations: int):
        """Send session complete signal"""
        pass
```

---

#### **index_storage.py** - Azure Index Storage
**Purpose**: Save index files to Azure Blob Storage

**Key Functions**:
```python
"""Azure Blob Storage for index files"""

import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient

from .models import QueryConfig, RAGResponse, TimeWindow
from .config import Config


class IndexStorage:
    """Store index files in Azure Blob Storage"""

    def __init__(self, config: Config):
        self.config = config
        self.blob_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )

    async def save_index(
        self,
        query: QueryConfig,
        window: TimeWindow,
        rag_response: RAGResponse,
        kafka_metadata: dict
    ) -> str:
        """
        Save index file to Azure
        Returns: Blob name (filename)
        """
        pass

    def _build_index_data(self, ....) -> dict:
        """Build index JSON structure"""
        pass

    def _get_blob_path(self, query: QueryConfig, window: TimeWindow) -> str:
        """Generate blob path for index"""
        prefix = query.index_prefix
        start = window.start_date.strftime("%Y%m%d")
        end = window.end_date.strftime("%Y%m%d")
        filename = f"{prefix}_{start}_{end}.json"
        return f"{self.config.indices_path_prefix}/{query.query_id}/{filename}"
```

---

#### **metrics.py** - Prometheus Metrics
**Purpose**: Expose CronJob metrics for monitoring

**Key Functions**:
```python
"""Prometheus metrics for CronJob"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server

from .config import Config


class CronJobMetrics:
    """Prometheus metrics server for CronJob"""

    def __init__(self, port: int):
        self.port = port

        # Define metrics
        self.execution_total = Counter(
            'cronjob_execution_total',
            'Total CronJob executions',
            ['status']
        )

        self.queries_processed = Counter(
            'cronjob_queries_processed_total',
            'Total queries processed',
            ['query_id', 'status']
        )

        self.windows_advanced = Counter(
            'cronjob_windows_advanced_total',
            'Total windows advanced',
            ['query_id']
        )

        self.citations_published = Counter(
            'cronjob_citations_published_total',
            'Total citations published to Kafka',
            ['query_id']
        )

        self.execution_duration = Histogram(
            'cronjob_execution_duration_seconds',
            'Query execution duration',
            ['query_id']
        )

        self.errors_total = Counter(
            'cronjob_errors_total',
            'Total errors',
            ['query_id', 'error_type']
        )

        self.last_success_timestamp = Gauge(
            'cronjob_last_success_timestamp',
            'Timestamp of last successful execution',
            ['query_id']
        )

    def start_server(self):
        """Start Prometheus metrics HTTP server"""
        start_http_server(self.port)

    def record_query_success(self, query_id: str, duration: float, citations: int):
        """Record successful query execution"""
        pass

    def record_query_failure(self, query_id: str, error_type: str):
        """Record query failure"""
        pass
```

---

#### **exceptions.py** - Custom Exceptions
**Purpose**: Define custom exception types

```python
"""Custom exceptions for CronJob"""


class CronJobError(Exception):
    """Base exception for CronJob errors"""
    pass


class ConfigurationError(CronJobError):
    """Configuration or setup error"""
    pass


class StateError(CronJobError):
    """State file error"""
    pass


class QueryExecutionError(CronJobError):
    """RAG query execution error"""
    pass


class StorageError(CronJobError):
    """Azure storage error"""
    pass


class KafkaError(CronJobError):
    """Kafka publishing error"""
    pass
```

---

## Docker Configuration

### **docker/Dockerfile.cronjob**

```dockerfile
# Multi-stage build for CronJob
FROM python:3.12-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc g++ make \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM python:3.12-slim

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

# Set working directory
WORKDIR /app

# Copy source code
COPY src/ ./src/
COPY config/ ./config/

# Create directories
RUN mkdir -p /app/logs

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Entry point
ENTRYPOINT ["python", "-m", "src.cronjobs.time_series.main"]
```

---

## Kubernetes Manifests

### **k8s/cronjobs/time-series-cronjob.yaml**

Location: `k8s/cronjobs/time-series-cronjob.yaml`

This file contains the CronJob resource definition with all environment variables and resource limits as specified in the design document.

---

## Helm Chart Structure

### **helm/time-series-indexer/**

```
helm/time-series-indexer/
├── Chart.yaml                    # Chart metadata
├── values.yaml                   # Default values
├── README.md                     # Usage instructions
└── templates/
    ├── _helpers.tpl              # Helper templates
    ├── cronjob.yaml              # CronJob template
    ├── serviceaccount.yaml       # ServiceAccount + RBAC
    ├── secrets.yaml              # Secrets (from values)
    └── configmap.yaml            # ConfigMap for non-sensitive config
```

**values.yaml** structure:
```yaml
image:
  repository: ragserviceneacr6881.azurecr.io/time-series-indexer
  tag: latest
  pullPolicy: Always

schedule: "0 1 * * *"  # Daily at 1 AM UTC

azure:
  storageConnectionString: ""  # Set via --set or values override
  container: research-indices

rag:
  apiUrl: "https://4.207.24.92/query"
  apiKey: ""  # Set via --set or values override
  timeout: 180

kafka:
  bootstrapServers: "kafka.rapporteur.ai:9094"
  enabled: true

config:
  queriesPath: "config/queries.json"
  statePath: "state"
  indicesPath: "indices"
  defaultStartDate: "2023-09-01"
  defaultWindowSize: 14

resources:
  requests:
    cpu: 200m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 2Gi

metrics:
  port: 8010

logging:
  level: INFO
```

---

## Deployment Scripts

### **scripts/build-and-push-cronjob.sh**

```bash
#!/bin/bash
set -e

ACR_NAME="ragserviceneacr6881"
ACR_REGISTRY="${ACR_NAME}.azurecr.io"
IMAGE_NAME="time-series-indexer"
IMAGE_TAG="${IMAGE_TAG:-latest}"
DOCKERFILE="docker/Dockerfile.cronjob"

echo "Building CronJob image..."
az acr login --name $ACR_NAME

docker buildx build \
    --platform linux/amd64 \
    -t ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
    -f $DOCKERFILE \
    .

echo "Pushing image..."
docker push ${ACR_REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}

echo "✅ CronJob image built and pushed successfully"
```

### **scripts/deploy-cronjob.sh**

```bash
#!/bin/bash
set -e

NAMESPACE="consumers"
CHART_PATH="./helm/time-series-indexer"
RELEASE_NAME="time-series-indexer"

echo "Deploying Time Series CronJob..."

helm upgrade --install $RELEASE_NAME $CHART_PATH \
    --namespace $NAMESPACE \
    --create-namespace \
    --set azure.storageConnectionString="$AZURE_CONNECTION_STRING" \
    --set rag.apiKey="$RAG_API_KEY" \
    --wait

echo "✅ CronJob deployed successfully"

kubectl get cronjob -n $NAMESPACE
```

---

## Configuration Templates

### **config/time-series/queries.json.template**

```json
{
  "version": "1.0",
  "updated_at": "2024-12-24T00:00:00Z",
  "default_window_size_days": 14,
  "default_start_date": "2023-09-01",
  "queries": [
    {
      "query_id": "example_query",
      "enabled": true,
      "query_template": "Get the latest news from [SOURCE] from {start_date} to {end_date}",
      "window_size_days": 14,
      "index_prefix": "example",
      "description": "Example query configuration",
      "tags": ["example"],
      "priority": 1
    }
  ]
}
```

### **config/time-series/state.json.template**

```json
{
  "query_id": "example_query",
  "query_template": "...",
  "last_window_end": "2024-01-15",
  "last_window_start": "2024-01-01",
  "window_size_days": 14,
  "total_windows_processed": 10,
  "last_run_timestamp": "2024-01-15T10:30:00Z",
  "last_run_status": "success",
  "last_run_citations": 42,
  "next_window_start": "2024-01-15",
  "next_window_end": "2024-01-29"
}
```

---

## Testing Structure

### **tests/unit/cronjobs/time_series/**

Unit tests for individual components:
- `test_query_manager.py` - Query config loading
- `test_state_manager.py` - State persistence
- `test_window_calculator.py` - Window calculations
- `test_rag_executor.py` - RAG API calls (mocked)
- `test_kafka_publisher.py` - Kafka publishing (mocked)
- `test_index_storage.py` - Azure storage (mocked)

### **tests/integration/cronjobs/**

Integration tests:
- `test_time_series_e2e.py` - Full workflow with real dependencies

---

## Build and Deployment Workflow

### Development Workflow

```bash
# 1. Make code changes in src/cronjobs/time_series/

# 2. Run unit tests
pytest tests/unit/cronjobs/time_series/

# 3. Build Docker image locally
docker build -f docker/Dockerfile.cronjob -t time-series-indexer:local .

# 4. Test locally with docker-compose (optional)
docker-compose -f docker-compose.cronjob.yml up

# 5. Build and push to ACR
./scripts/build-and-push-cronjob.sh

# 6. Deploy to cluster
./scripts/deploy-cronjob.sh

# 7. Trigger manual run for testing
kubectl create job time-series-test-run \
    --from=cronjob/time-series-research-indexer \
    -n consumers

# 8. Watch logs
kubectl logs -f -n consumers job/time-series-test-run
```

---

## Key Design Decisions

### 1. **Separate `cronjobs/` Directory in `src/`**
- **Why**: Clear separation from consumers (long-running) vs cronjobs (scheduled)
- **Benefit**: Can add more cronjobs in future without mixing concerns
- **Example**: `src/cronjobs/backup_indices/`, `src/cronjobs/cleanup_old_data/`

### 2. **Dedicated Dockerfile**
- **Why**: CronJob doesn't need Playwright, different dependencies
- **Benefit**: Smaller image size, faster builds
- **Tradeoff**: More Dockerfiles to maintain

### 3. **Helm Chart for CronJob**
- **Why**: Consistent deployment approach with consumers
- **Benefit**: Easy configuration overrides, version management
- **Alternative**: Could use raw K8s manifests if simpler

### 4. **Module-per-Concern Organization**
- **Why**: Each module has single responsibility (query loading, state management, etc.)
- **Benefit**: Easy to test, understand, and modify
- **Pattern**: Similar to consumer organization

### 5. **Keep Original Script**
- **Why**: Reference implementation, can run standalone
- **Benefit**: Debugging, comparison, documentation
- **Location**: Repository root as `time_series_research_queries.py`

---

## Summary

### File Count
- **Python modules**: 10 files in `src/cronjobs/time_series/`
- **Kubernetes manifests**: 3 files in `k8s/cronjobs/`
- **Helm templates**: 5 files in `helm/time-series-indexer/templates/`
- **Deployment scripts**: 2 files in `scripts/`
- **Config templates**: 2 files in `config/time-series/`
- **Test files**: ~7-10 files in `tests/`
- **Documentation**: 3 files in `docs/`

### Total New Files: ~32-35 files

### Integration Points
1. **Shares**: Kafka topics (web-scrape-requests, session-complete)
2. **Shares**: Azure Storage (same account, different container)
3. **Shares**: Monitoring stack (Prometheus, Grafana)
4. **Shares**: Kubernetes namespace (consumers)
5. **Independent**: Own Dockerfile, own Helm chart, own state management

---

**Next Step**: Start implementation with `models.py` and `config.py` as foundation, then build up from there.
