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
        "project_id": None,  # Set via PROJECT_ID env var or config
        "location": "us-central1",  # GCP region for Vertex AI
        "model_name": "gemini-2.5-pro",  # Stable model with better rate limits (was gemini-2.0-flash-exp)
        "timeout": 300,  # Request timeout for Gemini API
        # Note: Uses llm.* settings for temperature, top_p, max_length, etc.
    },
    "embedding_service": {
        # Embedding service configuration
        "url": "http://localhost:8080",  # Base URL for embedding service
        "api_key": None,  # Set via EMBEDDING_SERVICE_API_KEY env var or config
        "timeout": 360.0,  # Request timeout in seconds (6 minutes)
        "max_retries": 3,  # Maximum number of retry attempts
        "retry_delay": 1.0,  # Initial delay between retries in seconds
        "max_connections": 20,  # Maximum number of concurrent connections
        "max_keepalive_connections": 10,  # Maximum number of keepalive connections
        "max_texts_per_request": 64,  # Maximum texts to embed in a single request
        "include_segment_embeddings": False,  # Include per-segment embeddings in response
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
