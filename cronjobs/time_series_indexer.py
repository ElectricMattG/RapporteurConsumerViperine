#!/usr/bin/env python3
"""
Time Series Research Indexer - Kubernetes CronJob
Incrementally builds research indices by sliding time windows forward.
Preserves tested logic from time_series_research_queries.py
"""

import asyncio
import hashlib
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests
from azure.storage.blob import BlobClient, BlobServiceClient
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Configuration loaded from environment variables"""

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
    processing_mode: str
    top_k: int
    enable_query_refinement: bool

    # Kafka
    kafka_bootstrap_servers: str
    kafka_enabled: bool
    kafka_web_scrape_topic: str
    kafka_session_complete_topic: str

    # Time windows
    default_start_date: str
    default_window_size_days: int

    # Config cache
    config_cache_dir: str

    # Monitoring
    metrics_port: int
    log_level: str

    @classmethod
    def from_env(cls) -> 'Config':
        """Load all configuration from environment variables"""
        return cls(
            # Azure Storage (required)
            azure_connection_string=os.getenv('AZURE_STORAGE_CONNECTION_STRING', ''),
            azure_container=os.getenv('AZURE_CONTAINER', ''),
            queries_config_path=os.getenv('QUERIES_CONFIG_PATH', ''),
            state_path_prefix=os.getenv('STATE_PATH_PREFIX', ''),
            indices_path_prefix=os.getenv('INDICES_PATH_PREFIX', ''),

            # RAG API (required)
            rag_api_url=os.getenv('RAG_API_URL', ''),
            rag_api_key=os.getenv('RAG_API_KEY', ''),
            rag_timeout=int(os.getenv('RAG_TIMEOUT', '0')),
            processing_mode=os.getenv('PROCESSING_MODE', ''),
            top_k=int(os.getenv('TOP_K', '0')),
            enable_query_refinement=os.getenv('ENABLE_QUERY_REFINEMENT', 'false').lower() == 'true',

            # Kafka (required)
            kafka_bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', ''),
            kafka_enabled=os.getenv('KAFKA_ENABLED', 'true').lower() == 'true',
            kafka_web_scrape_topic=os.getenv('KAFKA_WEB_SCRAPE_TOPIC', ''),
            kafka_session_complete_topic=os.getenv('KAFKA_SESSION_COMPLETE_TOPIC', ''),

            # Time windows (required)
            default_start_date=os.getenv('DEFAULT_START_DATE', ''),
            default_window_size_days=int(os.getenv('DEFAULT_WINDOW_SIZE_DAYS', '0')),

            # Config cache
            config_cache_dir=os.getenv('CONFIG_CACHE_DIR', ''),

            # Monitoring
            metrics_port=int(os.getenv('METRICS_PORT', '8010')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
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

    def __post_init__(self):
        if self.tags is None:
            self.tags = []


@dataclass
class WindowState:
    """State for a query's time window"""
    query_id: str
    query_template: str
    window_size_days: int
    total_windows_processed: int
    next_window_start: str
    next_window_end: str
    last_window_start: Optional[str] = None
    last_window_end: Optional[str] = None
    last_run_status: Optional[str] = None
    last_run_citations: int = 0
    last_run_timestamp: Optional[str] = None


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

        self.windows_advanced = Counter(
            'cronjob_windows_advanced_total',
            'Windows advanced',
            ['query_id']
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

        self.last_success_timestamp = Gauge(
            'cronjob_last_success_timestamp',
            'Last success timestamp',
            ['query_id']
        )

    def start_server(self):
        """Start metrics HTTP server"""
        try:
            start_http_server(self.port)
        except OSError:
            # Port already in use (e.g., when testing)
            pass


# ============================================================================
# WORKING LOGIC FROM time_series_research_queries.py
# DO NOT MODIFY - TESTED AND WORKING
# ============================================================================

def make_research_query(query: str, config: Config) -> Dict:
    """
    Execute a single research query to the RAG service.
    PRESERVED FROM ORIGINAL SCRIPT - DO NOT MODIFY
    """
    headers = {
        "Content-Type": "application/json",
        "Host": "rag.rapporteur.ai",
        "X-API-Key": config.rag_api_key
    }

    payload = {
        "query": query,
        "processing_mode": config.processing_mode,
        "enable_query_refinement": config.enable_query_refinement,
        "top_k": config.top_k
    }

    try:
        response = requests.post(
            config.rag_api_url,
            headers=headers,
            json=payload,
            timeout=config.rag_timeout,
            verify=False  # Skip SSL verification
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "query": query}


def create_messages_from_rag_response(rag_response: Dict, query_id: str = "default") -> List[Dict]:
    """
    Extract URLs from RAG response grounding citations and create batch messages.
    PRESERVED FROM ORIGINAL SCRIPT - Modified to include query_id
    """
    messages = []

    # Extract session and query info
    session_id = rag_response.get("session_id", "unknown_session")
    query = rag_response.get("query", "")

    # Get citations from grounding metadata
    grounding = rag_response.get("grounding_metadata", {})
    citations = grounding.get("citations", [])

    if not citations:
        return messages

    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Process each citation
    for idx, citation in enumerate(citations):
        # Extract URL - handle both direct URL and grounding redirect
        url = citation.get("uri", "")

        # Skip if no URL
        if not url:
            continue

        # Extract domain for display
        try:
            domain = urlparse(url).netloc
        except:
            domain = "unknown"

        # Create message
        message = {
            "session_id": session_id,
            "request_id": f"req_{timestamp}_{idx:03d}",
            "uri": url,
            "title": citation.get("title", domain),
            "total_requests": len(citations),
            "query_id": query_id,
            "rag_metadata": {
                "query": query,
                "session_id": session_id,
                "request_id": rag_response.get("request_id", ""),
                "citation_index": idx,
                "total_citations": len(citations),
            }
        }

        messages.append(message)

    return messages


def send_batch_to_kafka(rag_response: Dict, query_id: str, config: Config, logger: logging.Logger) -> bool:
    """
    Send RAG response citations to Kafka web scraping pipeline.
    PRESERVED FROM ORIGINAL SCRIPT - Modified to include query_id and rate limiting delay

    Args:
        rag_response: RAG response containing citations
        query_id: Query identifier for multi-index support
        config: Configuration object
        logger: Logger instance

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create messages from RAG response
        messages = create_messages_from_rag_response(rag_response, query_id)

        if not messages:
            logger.warning("No citations to send to Kafka")
            return False

        session_id = rag_response.get("session_id", "unknown")
        query = rag_response.get("query", "")[:80]

        # Get message delay from environment (default 0.1 seconds)
        message_delay = float(os.getenv('KAFKA_MESSAGE_DELAY_SECONDS', '0.1'))

        logger.info(f"Sending {len(messages)} citations to Kafka")
        logger.info(f"Session: {session_id}")
        logger.info(f"Query: {query}...")
        logger.info(f"Message delay: {message_delay}s (reduces throughput to manage rate limits)")

        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': config.kafka_bootstrap_servers,
            'client.id': 'time-series-indexer',
            'acks': 'all',
            'retries': 3,
        }

        producer = Producer(producer_config)

        # Send each message with delay to reduce throughput
        success_count = 0
        for idx, message in enumerate(messages, 1):
            try:
                message_json = json.dumps(message, ensure_ascii=False)

                producer.produce(
                    topic=config.kafka_web_scrape_topic,
                    key=message['request_id'],
                    value=message_json.encode('utf-8'),
                )
                success_count += 1

                # Add delay between messages to reduce system load
                # This helps prevent rate limiting on downstream services (Gemini API)
                if idx < len(messages) and message_delay > 0:
                    import time
                    time.sleep(message_delay)

            except Exception as e:
                logger.error(f"Failed to queue message {idx}: {e}")

        # Flush messages
        outstanding = producer.flush(timeout=30)

        if outstanding > 0:
            logger.warning(f"{outstanding} messages still outstanding")
            return False

        # Send session complete signal
        session_complete_message = {
            "session_id": session_id,
            "signal": "session_complete",
            "total_requests": len(messages),
            "timestamp": datetime.now().isoformat()
        }

        producer.produce(
            topic=config.kafka_session_complete_topic,
            key=session_id,
            value=json.dumps(session_complete_message).encode('utf-8')
        )
        producer.flush(timeout=10)

        logger.info(f"Kafka batch complete: {success_count} messages sent")
        return True

    except Exception as e:
        logger.error(f"Kafka error: {e}")
        return False


# ============================================================================
# CONFIG MANAGER WITH LOCAL CACHE + AZURE SYNC
# ============================================================================

class ConfigManager:
    """Manage query configuration with local cache and Azure sync"""

    def __init__(self, config: Config, blob_client: BlobServiceClient, logger: logging.Logger):
        self.config = config
        self.blob_client = blob_client
        self.logger = logger
        self.cache_dir = Path(config.config_cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "queries.json"

    async def load_queries(self) -> List[QueryConfig]:
        """Load queries with local cache and Azure sync"""
        self.logger.info("Loading query configuration...")

        # Download latest from Azure
        azure_config = await self._download_from_azure()

        # Check local cache
        local_config = self._load_local_cache()

        # Compare and use latest
        if azure_config and local_config:
            azure_hash = self._compute_hash(azure_config)
            local_hash = self._compute_hash(local_config)

            if azure_hash != local_hash:
                self.logger.info("Azure config differs from local cache, using Azure version")
                config_data = azure_config
                self._save_local_cache(azure_config)
            else:
                self.logger.info("Local cache matches Azure, using cached version")
                config_data = local_config
        elif azure_config:
            self.logger.info("Using Azure config (no local cache)")
            config_data = azure_config
            self._save_local_cache(azure_config)
        elif local_config:
            self.logger.warning("Azure config unavailable, using local cache")
            config_data = local_config
        else:
            self.logger.error("No configuration available (Azure or local)")
            return []

        # Parse queries
        queries = self._parse_queries(config_data)
        enabled_queries = [q for q in queries if q.enabled]

        self.logger.info(f"Loaded {len(queries)} queries ({len(enabled_queries)} enabled)")
        return enabled_queries

    async def _download_from_azure(self) -> Optional[Dict]:
        """Download queries.json from Azure Blob Storage"""
        try:
            blob_path = self.config.queries_config_path
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_path
            )

            self.logger.info(f"Downloading config from Azure: {self.config.azure_container}/{blob_path}")
            blob_data = blob_client.download_blob().readall()
            config_data = json.loads(blob_data.decode('utf-8'))

            self.logger.info("Successfully downloaded config from Azure")
            return config_data

        except Exception as e:
            self.logger.error(f"Failed to download config from Azure: {e}")
            return None

    def _load_local_cache(self) -> Optional[Dict]:
        """Load config from local cache"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r') as f:
                    return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load local cache: {e}")
        return None

    def _save_local_cache(self, config_data: Dict):
        """Save config to local cache"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(config_data, f, indent=2)
            self.logger.info(f"Saved config to local cache: {self.cache_file}")
        except Exception as e:
            self.logger.error(f"Failed to save local cache: {e}")

    def _compute_hash(self, config_data: Dict) -> str:
        """Compute hash of config data for comparison"""
        config_str = json.dumps(config_data, sort_keys=True)
        return hashlib.sha256(config_str.encode()).hexdigest()

    def _parse_queries(self, config_data: Dict) -> List[QueryConfig]:
        """Parse query configurations from data"""
        queries = []
        for q in config_data.get('queries', []):
            try:
                query = QueryConfig(
                    query_id=q['query_id'],
                    enabled=q.get('enabled', True),
                    query_template=q['query_template'],
                    window_size_days=q.get('window_size_days', self.config.default_window_size_days),
                    index_prefix=q['index_prefix'],
                    description=q.get('description', ''),
                    tags=q.get('tags', []),
                    priority=q.get('priority', 1)
                )
                queries.append(query)
            except KeyError as e:
                self.logger.error(f"Invalid query config (missing {e}): {q}")

        return queries


# ============================================================================
# STATE MANAGER
# ============================================================================

class StateManager:
    """Manage query window state in Azure Blob Storage"""

    def __init__(self, config: Config, blob_client: BlobServiceClient, logger: logging.Logger):
        self.config = config
        self.blob_client = blob_client
        self.logger = logger

    async def load_state(self, query: QueryConfig) -> WindowState:
        """Load state for a query or create initial state"""
        blob_path = f"{self.config.state_path_prefix}/{query.query_id}.json"

        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_state_container,
                blob=blob_path
            )

            blob_data = blob_client.download_blob().readall()
            state_data = json.loads(blob_data.decode('utf-8'))

            state = WindowState(**state_data)
            self.logger.info(f"Loaded state for {query.query_id}: next window {state.next_window_start} to {state.next_window_end}")
            return state

        except Exception as e:
            self.logger.info(f"No state found for {query.query_id}, creating initial state")
            return self._create_initial_state(query)

    async def save_state(self, state: WindowState):
        """Save state to Azure Blob Storage"""
        blob_path = f"{self.config.state_path_prefix}/{state.query_id}.json"

        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_path
            )

            state_data = asdict(state)
            state_json = json.dumps(state_data, indent=2)

            blob_client.upload_blob(state_json, overwrite=True)
            self.logger.info(f"Saved state for {state.query_id}")

        except Exception as e:
            self.logger.error(f"Failed to save state for {state.query_id}: {e}")
            raise

    def _create_initial_state(self, query: QueryConfig) -> WindowState:
        """Create initial state with default start date"""
        start_date = datetime.strptime(self.config.default_start_date, "%Y-%m-%d")
        end_date = start_date + timedelta(days=query.window_size_days)

        return WindowState(
            query_id=query.query_id,
            query_template=query.query_template,
            window_size_days=query.window_size_days,
            total_windows_processed=0,
            next_window_start=start_date.strftime("%Y-%m-%d"),
            next_window_end=end_date.strftime("%Y-%m-%d"),
            last_run_status=None,
            last_run_citations=0
        )


# ============================================================================
# INDEX STORAGE
# ============================================================================

class IndexStorage:
    """Store index files in Azure Blob Storage"""

    def __init__(self, config: Config, blob_client: BlobServiceClient, logger: logging.Logger):
        self.config = config
        self.blob_client = blob_client
        self.logger = logger

    async def index_exists(
        self,
        query: QueryConfig,
        window_start: str,
        window_end: str
    ) -> bool:
        """Check if index already exists for this query and window"""
        # Generate filename
        start_str = window_start.replace("-", "")
        end_str = window_end.replace("-", "")
        filename = f"{query.index_prefix}_{start_str}_{end_str}.json"

        # Build blob path
        blob_path = f"{self.config.indices_path_prefix}/{query.query_id}/{filename}"

        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_path
            )

            # Check if blob exists
            return blob_client.exists()

        except Exception as e:
            self.logger.debug(f"Error checking if index exists {filename}: {e}")
            return False

    async def load_index(
        self,
        query: QueryConfig,
        window_start: str,
        window_end: str
    ) -> Optional[Dict]:
        """Load existing index data from Azure Blob Storage"""
        # Generate filename
        start_str = window_start.replace("-", "")
        end_str = window_end.replace("-", "")
        filename = f"{query.index_prefix}_{start_str}_{end_str}.json"

        # Build blob path
        blob_path = f"{self.config.indices_path_prefix}/{query.query_id}/{filename}"

        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_path
            )

            blob_data = blob_client.download_blob().readall()
            index_data = json.loads(blob_data.decode('utf-8'))

            self.logger.info(f"Loaded existing index: {filename}")
            return index_data

        except Exception as e:
            self.logger.error(f"Failed to load existing index {filename}: {e}")
            return None

    async def save_index(
        self,
        query: QueryConfig,
        window_start: str,
        window_end: str,
        rag_response: Dict,
        kafka_metadata: Dict
    ) -> str:
        """Save index file to Azure Blob Storage"""
        # Generate filename
        start_str = window_start.replace("-", "")
        end_str = window_end.replace("-", "")
        filename = f"{query.index_prefix}_{start_str}_{end_str}.json"

        # Build blob path
        blob_path = f"{self.config.indices_path_prefix}/{query.query_id}/{filename}"

        # Build index data
        index_data = {
            "metadata": {
                "query_id": query.query_id,
                "query": rag_response.get("query", ""),
                "window_start": window_start,
                "window_end": window_end,
                "window_size_days": query.window_size_days,
                "generated_at": datetime.now().isoformat(),
                "session_id": rag_response.get("session_id", ""),
                "cronjob_run_id": os.getenv("HOSTNAME", "unknown"),
                "index_filename": filename
            },
            "rag_response": rag_response,
            "kafka_metadata": kafka_metadata
        }

        try:
            blob_client = self.blob_client.get_blob_client(
                container=self.config.azure_container,
                blob=blob_path
            )

            index_json = json.dumps(index_data, indent=2, ensure_ascii=False)
            blob_client.upload_blob(index_json, overwrite=True)

            self.logger.info(f"Saved index: {filename}")
            return filename

        except Exception as e:
            self.logger.error(f"Failed to save index {filename}: {e}")
            raise


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

class TimeSeriesIndexer:
    """Main CronJob orchestrator"""

    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger

        # Initialize components
        self.metrics = Metrics(config.metrics_port)
        self.blob_client = BlobServiceClient.from_connection_string(
            config.azure_connection_string
        )

        self.config_manager = ConfigManager(config, self.blob_client, logger)
        self.state_manager = StateManager(config, self.blob_client, logger)
        self.index_storage = IndexStorage(config, self.blob_client, logger)

    async def run(self) -> int:
        """Execute CronJob workflow"""
        try:
            # Start metrics server
            self.metrics.start_server()
            self.logger.info(f"Metrics server started on port {self.config.metrics_port}")

            # Load queries
            queries = await self.config_manager.load_queries()

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

            self.logger.info(f"Execution complete: {successes} succeeded, {failures} failed")

            return 0 if failures == 0 else 1

        except Exception as e:
            self.logger.exception(f"Fatal error: {e}")
            return 1

    async def _process_query(self, query: QueryConfig) -> bool:
        """Process a single query - processes multiple windows until caught up"""
        start_time = datetime.now()
        windows_processed = 0

        try:
            self.logger.info(f"Processing query: {query.query_id}")

            # Load state
            state = await self.state_manager.load_state(query)

            # Process consecutive windows until caught up
            today = datetime.now()

            while True:
                # Check if next window should be processed
                next_start = datetime.strptime(state.next_window_start, "%Y-%m-%d")

                if next_start >= today:
                    self.logger.info(f"Query {query.query_id} is up-to-date (next window starts {state.next_window_start}, today is {today.strftime('%Y-%m-%d')})")
                    break

                # Calculate window to process
                window_start = state.next_window_start
                window_end = state.next_window_end

                self.logger.info(f"Processing window {windows_processed + 1}: {window_start} to {window_end}")

                # Check if index already exists for this window
                index_exists = await self.index_storage.index_exists(query, window_start, window_end)

                rag_response = None
                kafka_metadata = {
                    "batch_sent": False,
                    "citations_published": 0,
                    "reused_existing_index": False
                }

                if index_exists:
                    self.logger.info(f"📥 Index already exists for window {window_start} to {window_end}, loading existing data")

                    # Load existing index and extract RAG response
                    existing_index = await self.index_storage.load_index(query, window_start, window_end)

                    if existing_index and "rag_response" in existing_index:
                        rag_response = existing_index["rag_response"]

                        # Add session_id if not present
                        session_id = f"time_series_{query.query_id}_{window_start.replace('-', '')}_{window_end.replace('-', '')}"
                        if "session_id" not in rag_response:
                            rag_response["session_id"] = session_id

                        citations_count = len(rag_response.get("grounding_metadata", {}).get("citations", []))
                        self.logger.info(f"🔄 Reusing existing RAG data with {citations_count} citations")
                        kafka_metadata["reused_existing_index"] = True
                    else:
                        self.logger.warning(f"⚠️  Existing index found but missing rag_response, will run fresh query")
                        index_exists = False

                if not index_exists:
                    # Format query string
                    query_string = query.query_template.format(
                        start_date=window_start,
                        end_date=window_end
                    )

                    # Execute RAG query (using PRESERVED working logic)
                    session_id = f"time_series_{query.query_id}_{window_start.replace('-', '')}_{window_end.replace('-', '')}"
                    rag_response = make_research_query(query_string, self.config)

                    # Check for errors
                    if "error" in rag_response:
                        self.logger.error(f"RAG query failed: {rag_response['error']}")
                        self.metrics.queries_processed.labels(
                            query_id=query.query_id, status='failure'
                        ).inc()
                        self.metrics.errors_total.labels(
                            query_id=query.query_id, error_type='rag_error'
                        ).inc()
                        return False

                    # Add session_id to response
                    rag_response["session_id"] = session_id

                    citations_count = len(rag_response.get("grounding_metadata", {}).get("citations", []))
                    self.logger.info(f"RAG query successful: {citations_count} citations")

                # Publish to Kafka (using PRESERVED working logic)
                citations_count = len(rag_response.get("grounding_metadata", {}).get("citations", []))

                if self.config.kafka_enabled:
                    kafka_sent = send_batch_to_kafka(rag_response, query.query_id, self.config, self.logger)
                    kafka_metadata["batch_sent"] = kafka_sent
                    if kafka_sent:
                        kafka_metadata["citations_published"] = citations_count
                        self.metrics.citations_published.labels(query_id=query.query_id).inc(citations_count)

                # Save index (only if we ran fresh query, not if reusing)
                if not kafka_metadata["reused_existing_index"]:
                    index_file = await self.index_storage.save_index(
                        query, window_start, window_end, rag_response, kafka_metadata
                    )
                else:
                    self.logger.info(f"📋 Index already saved, skipping save")

                # Update state for next window
                end_date = datetime.strptime(window_end, "%Y-%m-%d")
                next_start_date = end_date
                next_end_date = next_start_date + timedelta(days=query.window_size_days)

                state.last_window_start = window_start
                state.last_window_end = window_end
                state.total_windows_processed += 1
                state.last_run_status = "success"
                state.last_run_citations = citations_count
                state.last_run_timestamp = datetime.now().isoformat()
                state.next_window_start = next_start_date.strftime("%Y-%m-%d")
                state.next_window_end = next_end_date.strftime("%Y-%m-%d")

                await self.state_manager.save_state(state)

                # Update metrics
                self.metrics.windows_advanced.labels(query_id=query.query_id).inc()

                windows_processed += 1
                self.logger.info(f"Window {windows_processed} completed, advancing to next window")

            # Update final metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.metrics.queries_processed.labels(
                query_id=query.query_id, status='success'
            ).inc()
            self.metrics.execution_duration.labels(query_id=query.query_id).observe(duration)
            self.metrics.last_success_timestamp.labels(query_id=query.query_id).set(datetime.now().timestamp())

            self.logger.info(f"Query {query.query_id} completed successfully ({windows_processed} windows processed)")
            return True

        except Exception as e:
            self.logger.exception(f"Error processing query {query.query_id}: {e}")
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
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Suppress some noisy loggers
    logging.getLogger('azure').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Suppress SSL warnings (like in original script)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    """Main entry point"""
    # Load configuration
    config = Config.from_env()

    # Setup logging
    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("Time Series Research Indexer - Kubernetes CronJob")
    logger.info("=" * 80)
    logger.info(f"Azure container: {config.azure_container}")
    logger.info(f"Kafka enabled: {config.kafka_enabled}")
    logger.info(f"Default start date: {config.default_start_date}")
    logger.info(f"Default window size: {config.default_window_size_days} days")
    logger.info("=" * 80)

    # Validate required config
    required_vars = {
        'AZURE_STORAGE_CONNECTION_STRING': config.azure_connection_string,
        'AZURE_CONTAINER': config.azure_container,
        'RAG_API_URL': config.rag_api_url,
        'RAG_API_KEY': config.rag_api_key,
        'KAFKA_BOOTSTRAP_SERVERS': config.kafka_bootstrap_servers,
        'QUERIES_CONFIG_PATH': config.queries_config_path,
        'STATE_PATH_PREFIX': config.state_path_prefix,
        'INDICES_PATH_PREFIX': config.indices_path_prefix,
        'PROCESSING_MODE': config.processing_mode,
        'DEFAULT_START_DATE': config.default_start_date,
        'CONFIG_CACHE_DIR': config.config_cache_dir,
        'KAFKA_WEB_SCRAPE_TOPIC': config.kafka_web_scrape_topic,
        'KAFKA_SESSION_COMPLETE_TOPIC': config.kafka_session_complete_topic,
    }

    missing_vars = [name for name, value in required_vars.items() if not value]

    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    if config.rag_timeout <= 0:
        logger.error("RAG_TIMEOUT must be greater than 0")
        sys.exit(1)

    if config.top_k <= 0:
        logger.error("TOP_K must be greater than 0")
        sys.exit(1)

    if config.default_window_size_days <= 0:
        logger.error("DEFAULT_WINDOW_SIZE_DAYS must be greater than 0")
        sys.exit(1)

    # Create indexer
    indexer = TimeSeriesIndexer(config, logger)

    # Run
    exit_code = asyncio.run(indexer.run())

    logger.info(f"Exiting with code {exit_code}")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
