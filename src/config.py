"""Init for module level config

Define module level config in a module namespace.
Importing the package will result in the config
being accessable within the audio_frontend
namespace: audio_frontend.app_conf, in addition
processing scripts are generated for the
entrypoint.sh file.

TODO expand the config to include worker config
and other config in the process_audio.py file.

Usage:
    >>> from audio_frontend import app_conf as config
"""

import logging
import sys
from typing import Optional

from pyconfs import Configuration

# config dict (expand as needed)
defaults = {
    "logging": {
        "format": "| %(asctime)s | %(levelname)s - %(name)-10s - %(message)s",
        "level": 10,
        "output_full_path": "./logs/logfile.log",
    },
    "gemini": {
        # GCP Gemini configuration (alternative to local LLM)
        "project_id": "another-project-472212",  # Set via PROJECT_ID env var or config
        "location": "us-central1",  # GCP region for Vertex AI
        "model_name": "gemini-2.5-flash",  # Fast model with good performance
        "timeout": 300,  # Request timeout for Gemini API
        "api_key": None,  # Set via GOOGLE_API_KEY env var or config (for direct API, not Vertex AI)
        # Note: Uses llm.* settings for temperature, top_p, max_length, etc.
    },
    "embedding_service": {
        # Embedding service configuration
        "url": "https://embeddings.rapporteur.ai",  # Base URL for embedding service
        "api_key": "H5J1Hd3P7HpJQ2YyGnEDtvz2wOuV5wSbIr4cVV/b/oU=",  # Set via EMBEDDING_SERVICE_API_KEY env var or config
        "timeout": 360.0,  # Request timeout in seconds (6 minutes)
        "max_retries": 3,  # Maximum number of retry attempts
        "retry_delay": 1.0,  # Initial delay between retries in seconds
        "max_connections": 20,  # Maximum number of concurrent connections
        "max_keepalive_connections": 10,  # Maximum number of keepalive connections
        "max_texts_per_request": 64,  # Maximum texts to embed in a single request
        "include_segment_embeddings": False,  # Include per-segment embeddings in response
    },
    "downloader": {
        # Video downloader configuration (for YouTube transcript extraction)
        "output_dir": "./cache/video_transcripts",
        "min_delay": 2,  # Minimum delay between downloads (seconds)
        "max_delay": 5,  # Maximum delay between downloads (seconds)
        "speed_limit": "1M",  # Download speed limit
        "sleep_requests": 1,  # Sleep between requests
        "sleep_interval": 1,  # Base sleep interval
        "max_sleep_interval": 5,  # Max sleep interval
        "sleep_subtitles": 1,  # Sleep after subtitle download
        "socket_timeout": 30,  # Socket timeout
        "retries": 3,  # Number of retries
        "file_access_retries": 3,  # File access retries
        "write_info_json": True,  # Write metadata JSON
        "write_description": True,  # Write video description
        "write_thumbnail": False,  # Write thumbnail (not needed for transcripts)
        "write_subs": True,  # Write subtitles (VTT files)
        "write_auto_subs": True,  # Write auto-generated subtitles
        "skip_download": True,  # Skip video download (use non-existent format trick)
        "force_download": False,  # Force re-download
        "subtitle_languages": ["en", "en-US", "en-GB"],  # Preferred subtitle languages
    },
    "azure_blob": {
        # Azure Blob Storage configuration for search index download
        "connection_string": None,  # Set via AZURE_STORAGE_CONNECTION_STRING env var
        "container_name": "indices",  # Container name for search indices
        "index_blob_url": "https://rapporteurappstorage.blob.core.windows.net/indices/enriched_chunks_with_embeddings_combined.jsonl",  # Full URL to index file (deprecated - use indices)
        "chunks_blob_name": "enriched_chunks_with_embeddings_combined.jsonl",  # Blob name for chunks file (deprecated - use indices)
        "citations_blob_name": "citations.jsonl",  # Blob name for citations file
        "cache_dir": "./cache",  # Local cache directory
        "download_on_startup": True,  # Download indices on service startup
        "check_updates_interval": 3600,  # Check for updates every N seconds (0 to disable)
        # Multi-index configuration: dict of {name: blob_url}
        # Note: URLs can also be local file paths for testing (e.g., "file:///path/to/index.jsonl")
        "indices": {
            "primary": "https://rapporteurappstorage.blob.core.windows.net/indices/enriched_chunks_with_embeddings_combined.jsonl",
            "qa_pairs": "https://rapporteurappstorage.blob.core.windows.net/indices/enriched_qa_pairs_with_embeddings_combined.jsonl",
        },
    },
    "image_downloader": {
        # Output configuration
        "default_output_dir": "downloaded_images",  # Default output directory
        "images_subdir": "images",                  # Subdirectory for image files
        "metadata_subdir": "metadata",              # Subdirectory for metadata files

        # Image filtering settings
        "min_size": 100,              # Minimum image dimension (width or height) in pixels
        "max_size": 5000,             # Maximum image dimension to avoid huge files
        "max_images_per_run": 15,     # Default maximum images to download per run

        # Download behavior
        "request_timeout": 10,        # HTTP request timeout in seconds
        "request_delay": 0.5,         # Delay between downloads in seconds
        "max_file_size": 10485760,    # Maximum file size in bytes (10MB)

        # Filtering keywords for icons/small images
        "icon_keywords": [
            "icon", "logo", "avatar", "close", "menu", "arrow", "button",
            "expand", "collapse", "search", "play", "pause", "stop",
            "social", "facebook", "twitter", "linkedin", "youtube",
            "placeholder", "loading", "spinner"
        ],

        # URL patterns to filter
        "icon_url_patterns": [
            "icon", "logo", "/static/", "/assets/icons/", "favicon"
        ],

        # HTTP headers
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",

        # Content type validation
        "allowed_content_types": [
            "image/jpeg", "image/jpg", "image/png", "image/gif",
            "image/webp", "image/svg+xml", "image/bmp"
        ],

        # Debug and logging
        "debug_downloads": False,     # Enable detailed download logging
        "save_failed_urls": True,     # Save list of failed download URLs
    },
    "kafka": {
        # Kafka consumer configuration
        "bootstrap_servers": None,  # Set via KAFKA_BOOTSTRAP_SERVERS env var (e.g., "localhost:9092")
        "group_id": "web-scraper-consumer",  # Consumer group ID for web scraper
        "auto_offset_reset": "earliest",  # Start from earliest message if no offset
        "enable_auto_commit": True,  # Auto-commit offsets
        "session_timeout_ms": 30000,  # Session timeout (30 seconds)
        "max_poll_records": 10,  # Maximum records to poll at once

        # Topic configuration
        "input_topic": "web-scrape-requests",  # Topic to consume messages from (web scraper input)
        "output_topic": "content-extracted",   # Topic to publish extracted content to (chunker & summarizer input)

        # Security configuration (optional)
        "security_protocol": "PLAINTEXT",  # PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
        "sasl_mechanism": None,  # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
        "sasl_username": None,  # Set via KAFKA_SASL_USERNAME env var
        "sasl_password": None,  # Set via KAFKA_SASL_PASSWORD env var
    },
    "summarizer": {
        # Summarizer consumer configuration
        "consumer_group_id": "summarizer-consumer",  # Consumer group ID for summarizer
        "summary_completed_topic": "summary-completed",  # Topic to publish summary completion messages
        "llm_provider": "gemini",  # LLM provider: 'openai' or 'gemini' (Gemini has native web grounding)
        "summary_word_count": 100,  # Target word count for summaries
    },
    "chunker": {
        # Document chunker consumer configuration
        "consumer_group_id": "document-chunker-consumer",  # Consumer group ID for chunker
        "input_topic": "content-extracted",  # Input topic to consume from (web scraper output)
        "output_topic": "citation-extraction",  # Output topic for chunked documents
        "cache_directory": "cache/web_scrapes",  # Directory for cached chunks
        "max_chunk_words": 350,  # Maximum words per chunk before splitting at sentence boundary
        "process_markdown_chunks": True,  # Apply MarkdownChunkProcessor to filter noise and merge small chunks
        "min_chunk_words": 3,  # Minimum words for a chunk (used by MarkdownChunkProcessor)
        "enable_llm_validation": True,  # Use Gemini LLM to validate chunk content quality (filters nav/UI/off-topic)
    },
    "citation_extractor": {
        # Citation extraction consumer configuration
        "consumer_group_id": "citation-extractor-consumer",  # Consumer group ID for citation extractor
        "input_topic": "citation-extraction",  # Topic to consume chunks from
        "output_topic": "chunk-embedding",  # Send enriched chunks to embedding consumer
        "cache_directory": "cache/citations",  # Directory for cached citations
        "min_citation_length": 2,  # Minimum characters for a valid citation
        "max_citations_per_chunk": 50,  # Maximum citations to extract per chunk
    },
    "chunk_embedder": {
        # Chunk embedding consumer configuration
        "consumer_group_id": "chunk-embedder-consumer",  # Consumer group ID for chunk embedder
        "input_topic": "chunk-embedding",  # Topic to consume enriched chunks from
        "output_topic": "qa-generation",  # Topic to publish embedded chunks to
        "cache_directory": "cache/embeddings",  # Directory for cached embeddings
        "batch_size": 50,  # Batch size for embedding generation
        "hierarchy_separator": " > ",  # Separator for section hierarchy
        "min_chunk_length": 20,  # Minimum chunk length to embed (chars)
        "max_chunk_size": 7500,  # Maximum chunk size before sub-chunking (chars)
    },
    "qa_generator": {
        # QA generation consumer configuration
        "consumer_group_id": "qa-generator-consumer",  # Consumer group ID for QA generator
        "input_topic": "qa-generation",  # Topic to consume embedded chunks from
        "output_topic": "qa-embedding",  # Topic to publish Q&A pairs to embedder
        "cache_directory": "cache/qa_pairs",  # Directory for cached QA pairs
        "pairs_per_chunk": 5,  # Target number of QA pairs per chunk
        "llm_provider": "gemini",  # LLM provider: 'openai' or 'gemini'
        "min_chunk_length": 50,  # Minimum chunk length to generate QA pairs
        "max_chunk_length": 10000,  # Maximum chunk length for QA generation
    },
    "qa_embedder": {
        # Q&A pair embedding consumer configuration
        "consumer_group_id": "qa-embedder-consumer",  # Consumer group ID for Q&A embedder
        "input_topic": "qa-embedding",  # Topic to consume Q&A pairs from
        "output_topic": "qa-indexed",  # Topic to publish enriched Q&A pairs to index aggregator
        "cache_directory": "cache/qa_embeddings",  # Directory for cached embeddings
        "model_name": "all-MiniLM-L6-v2",  # Sentence transformer model
        "batch_size": 32,  # Batch size for encoding
        "separator": " [SEP] ",  # Separator for combining question+answer
        "mode": "all",  # Embedding mode: 'question', 'answer', 'combined', or 'all'
    },
    "summary_indexer": {
        # Summary indexer consumer configuration
        "consumer_group_id": "summary-indexer-consumer",  # Consumer group ID for summary indexer
        "input_topic": "summary-completed",  # Topic to consume summary completion messages from
        "output_topic": "session-complete",  # Topic to publish session completion to index aggregator
        "cache_directory": "cache/summaries",  # Directory for summary index files
        "model_name": "all-MiniLM-L6-v2",  # Sentence transformer model for embeddings
    },
    "index_aggregator": {
        # Index aggregator consumer configuration
        "consumer_group_id": "index-aggregator-consumer",  # Consumer group ID for aggregator
        "input_topic": "qa-indexed",  # Topic to consume indexed Q&A pairs from
        "session_complete_topic": "session-complete",  # Topic for session completion signals
        "session_timeout": 900,  # Session timeout in seconds (2 minutes - aggregate even if incomplete)
        "cache_dir": "./cache/web_scrapes",  # Cache directory for processing files
        "session_backup_dir": "./cache/session_backups",  # Directory for session backups
        "archive_dir": "./cache/archive",  # Directory for archived session files
        "indices_dir": "./cache/indices",  # Directory for aggregated indices
        "cleanup_after_upload": True,  # Clean up local cache after successful upload
        "archive_before_cleanup": True,  # Archive session files before cleanup
        "upload_to_azure": True,  # Enable Azure Blob Storage upload (requires connection string)
        "azure_container_indices": "indices",  # Azure container for research indices
        "azure_container_archive": "research-archive",  # Azure container for archived sessions
        "azure_chunks_blob_name": "research_chunks_with_embeddings_combined.jsonl",  # Blob name for chunks
        "azure_qa_pairs_blob_name": "research_qa_pairs_with_embeddings_combined.jsonl",  # Blob name for Q&A pairs
    },
    "dead_letter": {
        # Dead letter queue consumer configuration
        "consumer_group_id": "dead-letter-consumer",  # Consumer group ID for DLQ
        "input_topic": "dead-letter-queue",  # Topic to consume failed messages from
        "output_directory": "cache/failures",  # Directory for failure logs
    },
    "qa_generation": {
        # Core LLM settings
        "llm_url": "http://localhost:8001",
        "llm_timeout": 300,

        # Batching configuration
        "enable_batching": True,  # Enable batch processing for multiple chunks
        "batch_size": 20,  # Number of chunks to process concurrently (increased from 10)
        "batch_delay": 0.1,  # Delay between batch requests (seconds) - reduced for concurrent processing

        # Text chunking parameters
        "chunk_size": 2000,  # Size of text chunks for processing
        "overlap_size": 200,  # Overlap between consecutive chunks
        "prompt_context_size": 2000,  # Max chars from chunk to include in LLM prompt

        # Web scraping input files
        "default_web_jsonl_files": [
            "merged_content.jsonl"
        ],

        # Retry and error handling
        "max_retries": 3,
        "retry_delays": [0.6, 1.0, 1.4],  # Progressive delays in seconds
        "temperature_progression": [0.6, 0.8, 1.0],  # Temperature escalation for retries

        # LLM generation parameters
        "max_length_base": 1200,
        "top_p": 0.6,
        "max_qa_pairs_per_chunk": 5,

        # Content filtering (keep pipeline flowing, filter downstream if needed)
        "min_words_for_extraction": 10,  # Minimum words for valid web extraction (web scraper)
        "min_words_for_qa": 0,  # Process all chunks for Q&A to keep pipeline flowing (no minimum)

        # Session and file naming
        "session_timestamp_format": "%Y%m%d_%H%M%S",
        "output_file_patterns": {
            "chunks": "chunks_{identifier}_{timestamp}.jsonl",
            "qa_pairs": "qa_pairs_{identifier}_{timestamp}.jsonl",
            "stats": "stats_{identifier}_{timestamp}.json",
            "errors": "errors_{identifier}_{timestamp}.jsonl"
        }
    },
    "citation_research": {
        # Citation research and summary generation
        "summary_word_count": 100,  # Target word count for summaries
        "reasoning_effort": "medium",  # OpenAI reasoning effort: low, medium, high
        "verbosity": 1,  # Verbosity level: 0 (concise), 1 (normal), 2 (detailed)
        "max_urls": 5,  # Maximum URLs to process per citation
        "enable_web_search": True,  # Enable web search grounding for OpenAI
    },
    "openai": {
        # OpenAI API configuration
        "api_key": None,  # Set via OPENAI_API_KEY env var
        "organization": None,  # Set via OPENAI_ORG_ID env var
        "model": "gpt-4o",  # Model to use for generation
        "model_name": "gpt-4o",  # Specific model name for citation research (faster than gpt-5.1)
        "timeout": 180,  # Request timeout in seconds
        "max_tokens": 32000,  # Maximum tokens in response (increased to prevent truncation)
        "temperature": 0.7,  # Temperature for generation
        "reasoning_effort": "medium",  # Reasoning effort: low, medium, high
        "verbosity": "medium",  # Verbosity level: low, medium, high
    },
    "llm": {
        # Generic LLM configuration (used by Gemini client)
        "max_length": 500,  # Maximum output length
        "temperature": 0.7,  # Temperature for generation
        "top_p": 0.9,  # Nucleus sampling parameter
        "max_retries": 3,  # Maximum retry attempts
        "retry_delay": 1.0,  # Delay between retries in seconds
        "max_context_chars": 8000,  # Maximum context characters
    },
    "web_scraper": {
        # Web scraping and URL resolution configuration
        "cache_dir": "./cache/web_scrapes",  # Directory for cached scrapes
        "max_redirects": 10,  # Maximum redirect hops to follow
        "request_timeout": 15,  # Request timeout in seconds
        "max_urls_per_message": 50,  # Maximum URLs to extract per message
        "retry_attempts": 3,  # Number of retry attempts for failed requests
        "retry_delay": 2.0,  # Delay between retries in seconds

        # URL resolution
        "follow_redirects": True,  # Follow HTTP redirects
        "verify_ssl": True,  # Verify SSL certificates

        # Content extraction
        "extract_links": True,  # Extract links from scraped pages
        "extract_images": False,  # Extract images from scraped pages
        "extract_markdown": True,  # Extract markdown from HTML content using docling for structured chunking
        "max_depth": 1,  # Maximum depth for link following (0 = no following)

        # Content validation (aligned with qa_generation.min_words_for_extraction)
        "min_words_for_valid_extraction": 10,  # Minimum words required for valid extraction
    },
    "tree_search": {
        "max_depth": 2,
        "max_queries_per_depth": 5,
        "min_results_per_query": 3,
        "text_chunk_size": 1000,
        "llm_base_url": "http://localhost:8001",
        "use_llm": True,
        "stop_on_no_growth": True,
        "base_storage_dir": "tree_search_data",  # Base directory for all tree searches
        "query_subdir_format": "{query}",         # How to format query subdirectory names
        "normalize_query_name": True,             # Whether to normalize query names for filesystem
        "extract_markdown": True,                 # Whether to extract markdown from HTML content using docling
        "max_links_per_page": 150,                 # Maximum links to extract from each page (increase for more relevant documents)
        "max_images_per_page": 50,                # Maximum images to extract from each page
        "linked_batch_size": 50,                  # Batch size for linked content URL processing
    },
}


# config class for wrapping and accessing the config dictionary
class CustConf(Configuration):
    @staticmethod
    def init_logger(self, name: str) -> logging.Logger:
        """Initialize a logger with the logging configuration defined in the
        configuration dictionary.

        Args:
            name: The name of the logger.
        """

        root_logger = logging.getLogger(name)  # type: ignore[attr]
        root_logger.handlers = []

        root_handler = logging.StreamHandler(stream=sys.stdout)
        root_handler.setFormatter(
            logging.Formatter(fmt=self._app_config.get("logging.format"))
        )
        root_logger.addHandler(root_handler)

        file_handler = logging.FileHandler(
            self._app_config.get("logging.output_full_path")
        )
        file_handler.setFormatter(
            logging.Formatter(fmt=self._app_config.get("logging.format"))
        )
        root_logger.addHandler(file_handler)

        root_logger.setLevel(self._app_config.get("logging.level"))

        return root_logger

    def get(self, key: str) -> Optional[None]:
        """Get the value corresponding to the given key string.

        Args:
            key (str): A string in the form "first_level_key.second_level_key".
        """
        return self.data[key.split(".")[0]][key.split(".")[1]]


# config object for reuse
app_conf = CustConf.from_dict(defaults)
