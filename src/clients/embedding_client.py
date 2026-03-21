"""
Async HTTP Client for Embedding Service

This module provides an async HTTP client that communicates with the embedding service
via HTTP API. It mirrors the EmbeddingEngine interface for drop-in replacement but
uses async patterns for optimal performance with replicated services.
"""

import asyncio
import logging
import os
from typing import List, Optional, Tuple, Union

import httpx
import numpy as np

# Import base configuration and logging
try:
    # Try relative import first (when used as a package)
    from ...config_handler import Base
except ImportError:
    # Fall back to absolute import (when run directly)
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class EmbeddingServiceClient(Base):
    """
    Async HTTP client for embedding service with connection pooling and retry logic.

    Designed to work with replicated embedding services behind a reverse proxy,
    providing automatic load balancing and fault tolerance.
    """

    def __init__(
        self,
        service_url: str = None,
        api_key: str = None,
        timeout: float = None,
        max_retries: int = None,
        retry_delay: float = None,
        max_connections: int = None,
        max_keepalive_connections: int = None
    ):
        """
        Initialize the embedding service client.

        Args:
            service_url: Base URL of the embedding service (uses config default if None)
            api_key: API key for authentication (uses config default if None)
            timeout: Request timeout in seconds (uses config default if None)
            max_retries: Maximum number of retry attempts (uses config default if None)
            retry_delay: Initial delay between retries in seconds (uses config default if None)
            max_connections: Maximum number of concurrent connections (uses config default if None)
            max_keepalive_connections: Maximum number of keepalive connections (uses config default if None)
        """
        # Initialize base class (gives us _app_config and _logger)
        super().__init__()

        # Get configuration values with fallback defaults
        self.service_url = service_url or os.getenv('EMBEDDING_SERVICE_URL') or self._app_config.get('embedding_service.url') or 'http://localhost:8080'

        # Try to get API key: priority is parameter > env var > config file
        self.api_key = api_key or os.getenv('EMBEDDING_SERVICE_API_KEY')
        if not self.api_key:
            try:
                self.api_key = self._app_config.get('embedding_service.api_key')
            except (KeyError, AttributeError):
                self.api_key = None

        timeout_value = timeout if timeout is not None else (self._app_config.get('embedding_service.timeout') or 300.0)
        self.max_retries = max_retries if max_retries is not None else (self._app_config.get('embedding_service.max_retries') or 3)
        self.retry_delay = retry_delay if retry_delay is not None else (self._app_config.get('embedding_service.retry_delay') or 1.0)
        max_conn = max_connections if max_connections is not None else (self._app_config.get('embedding_service.max_connections') or 20)
        max_keepalive = max_keepalive_connections if max_keepalive_connections is not None else (self._app_config.get('embedding_service.max_keepalive_connections') or 10)

        # Create timeout configuration
        self.timeout = httpx.Timeout(timeout_value, connect=10.0)

        # Create connection pool limits
        self.limits = httpx.Limits(
            max_connections=max_conn,
            max_keepalive_connections=max_keepalive
        )

        # Centralized request chunking limit (texts per request)
        # Keep conservative default to avoid server-side limits
        self.max_texts_per_request: int = (
            self._app_config.get('embedding_service.max_texts_per_request') or 64
        )

        # HTTP client will be initialized on first use (lazy initialization).
        # Because the Kafka consumer calls asyncio.run() per message (creating
        # and destroying event loops), we track which loop the client was
        # created on and discard it when the loop changes.
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

        # Cache embedding dimension from service
        self._embedding_dim: Optional[int] = None

        self._logger.info(f"EmbeddingServiceClient initialized - URL: {self.service_url}, "
                         f"timeout: {timeout_value}s, max_retries: {self.max_retries}")
        self._logger.info(f"max_texts_per_request: {self.max_texts_per_request}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client bound to the *current* event loop.

        If the running loop differs from the one the cached client was
        created on, the stale client is discarded (without ``aclose`` —
        its loop is already dead) and a fresh one is created.
        """
        loop = asyncio.get_running_loop()
        if self._client is not None and self._client_loop is not loop:
            # Old client is bound to a closed loop — drop it.
            self._client = None
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.service_url,
                timeout=self.timeout,
                limits=self.limits,
                verify=False  # Disable SSL verification for self-signed certs
            )
            self._client_loop = loop
            self._logger.info("HTTP client initialized")
        return self._client

    async def _post_with_retry(self, endpoint: str, json_data: dict) -> httpx.Response:
        """
        POST request with exponential backoff retry logic.

        Args:
            endpoint: API endpoint path
            json_data: JSON payload

        Returns:
            HTTP response

        Raises:
            httpx.HTTPError: If all retry attempts fail
        """
        client = await self._get_client()

        # Build headers with API key if available
        headers = {}
        if self.api_key:
            headers['X-API-Key'] = self.api_key

        for attempt in range(self.max_retries):
            try:
                response = await client.post(endpoint, json=json_data, headers=headers)
                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                # Log response body for debugging
                error_body = e.response.text[:500] if e.response.text else "No response body"

                # Don't retry on client errors (4xx)
                if 400 <= e.response.status_code < 500:
                    self._logger.error(f"Client error {e.response.status_code}: {error_body}")
                    raise

                # Retry on server errors (5xx)
                if attempt == self.max_retries - 1:
                    self._logger.error(f"Server error after {self.max_retries} attempts: {e}")
                    self._logger.error(f"Response body: {error_body}")
                    raise

                delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                self._logger.warning(f"Server error (attempt {attempt + 1}/{self.max_retries}), "
                                   f"retrying in {delay:.1f}s... Status: {e.response.status_code}")
                self._logger.debug(f"Error response: {error_body}")
                await asyncio.sleep(delay)

            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt == self.max_retries - 1:
                    self._logger.error(f"Network error after {self.max_retries} attempts: {e}")
                    raise

                delay = self.retry_delay * (2 ** attempt)  # Exponential backoff
                self._logger.warning(f"Network error (attempt {attempt + 1}/{self.max_retries}), "
                                   f"retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)

    async def embed(
        self,
        texts: Union[str, List[str]],
        batch_size: Optional[int] = None,
        show_progress: bool = False,
        use_segmentation: bool = True,
        include_segment_embeddings: Optional[bool] = None,
    ) -> np.ndarray:
        """
        Generate embeddings for input texts via HTTP API.

        Args:
            texts: Single text string or list of text strings
            batch_size: Batch size for processing (passed to service)
            show_progress: Whether to show progress information
            use_segmentation: Whether to use text segmentation for long texts (default: True)

        Returns:
            Numpy array of embeddings with shape (n_texts, embedding_dim)
        """
        # Handle single string input
        if isinstance(texts, str):
            texts = [texts]

        if not texts:
            # Get embedding dimension if not cached
            if self._embedding_dim is None:
                await self._fetch_embedding_dim()
            return np.array([]).reshape(0, self._embedding_dim)

        # Resolve include_segment_embeddings flag from param or config
        include_segments = (
            bool(include_segment_embeddings)
            if include_segment_embeddings is not None
            else bool(self._app_config.get('embedding_service.include_segment_embeddings') or False)
        )

        # Service batch size limit - centralized in config
        MAX_TEXTS_PER_REQUEST = int(self.max_texts_per_request)
        total_texts = len(texts)

        if show_progress:
            self._logger.info(
                f"Embedding {total_texts} texts via HTTP API (segmentation={'ON' if use_segmentation else 'OFF'})"
            )

        # If texts fit in single request, send directly
        if total_texts <= MAX_TEXTS_PER_REQUEST:
            # Prepare request payload
            payload = {
                "texts": texts,
                "use_segmentation": use_segmentation,
            }
            # New flag to request per-segment embeddings in response
            payload["include_segment_embeddings"] = include_segments

            if batch_size is not None:
                payload["batch_size"] = batch_size

            if show_progress:
                payload["show_progress"] = True

            # Debug: Log entire payload (truncate texts for readability)
            payload_debug = payload.copy()
            payload_debug["texts"] = [
                f"{text[:100]}..." if len(text) > 100 else text
                for text in payload_debug["texts"][:3]
            ]
            if len(texts) > 3:
                payload_debug["texts"].append(f"... and {len(texts) - 3} more texts")
            payload_debug["include_segment_embeddings"] = include_segments
            self._logger.debug(f"Single request - Full payload: {payload_debug}")

            # Make request
            response = await self._post_with_retry("/embeddings", payload)
            data = response.json()

            # Extract embeddings and convert to numpy array
            embeddings = np.array(data["embeddings"])

            # Cache embedding dimension
            if self._embedding_dim is None:
                self._embedding_dim = (
                    embeddings.shape[1] if len(embeddings) > 0 else data["shape"][1]
                )

            if show_progress:
                self._logger.info(
                    f"Received {len(embeddings)} embeddings from service (processing_time: {data.get('processing_time', 'N/A')}s)"
                )

            return embeddings

        # Otherwise, split into chunks and process in batches
        num_chunks = (total_texts + MAX_TEXTS_PER_REQUEST - 1) // MAX_TEXTS_PER_REQUEST

        if show_progress:
            self._logger.info(
                f"Splitting into {num_chunks} chunks of max {MAX_TEXTS_PER_REQUEST} texts"
            )

        all_embeddings: List[np.ndarray] = []
        for i in range(0, total_texts, MAX_TEXTS_PER_REQUEST):
            chunk = texts[i : i + MAX_TEXTS_PER_REQUEST]
            chunk_size = len(chunk)

            if show_progress:
                chunk_num = i // MAX_TEXTS_PER_REQUEST + 1
                self._logger.info(f"Processing chunk {chunk_num}/{num_chunks} ({chunk_size} texts)")

            # Prepare request payload
            payload = {
                "texts": chunk,
                "use_segmentation": use_segmentation,
            }
            # New flag to request per-segment embeddings in response
            payload["include_segment_embeddings"] = include_segments

            if batch_size is not None:
                payload["batch_size"] = batch_size

            # Debug: Log entire payload (truncate texts for readability)
            payload_debug = payload.copy()
            payload_debug["texts"] = [
                f"{text[:100]}..." if len(text) > 100 else text
                for text in payload_debug["texts"][:3]
            ]
            if len(chunk) > 3:
                payload_debug["texts"].append(f"... and {len(chunk) - 3} more texts")
            payload_debug["include_segment_embeddings"] = include_segments
            self._logger.debug(
                f"Chunk {i // MAX_TEXTS_PER_REQUEST + 1}/{num_chunks} - Full payload: {payload_debug}"
            )

            # Make request (no progress bar for individual chunks)
            response = await self._post_with_retry("/embeddings", payload)
            data = response.json()

            # Extract embeddings
            chunk_embeddings = np.array(data["embeddings"])
            all_embeddings.append(chunk_embeddings)

            # Cache embedding dimension from first chunk
            if self._embedding_dim is None:
                self._embedding_dim = (
                    chunk_embeddings.shape[1] if len(chunk_embeddings) > 0 else data["shape"][1]
                )

        # Concatenate all embeddings
        embeddings = np.vstack(all_embeddings) if all_embeddings else np.empty((0, self._embedding_dim or 0))

        if show_progress:
            self._logger.info(f"Completed embedding {total_texts} texts (shape: {embeddings.shape})")

        return embeddings

    async def similarity(
        self,
        texts1: Union[str, List[str]],
        texts2: Union[str, List[str]]
    ) -> np.ndarray:
        """
        Compute cosine similarity between two sets of texts via HTTP API.

        Args:
            texts1: First set of texts
            texts2: Second set of texts

        Returns:
            Similarity matrix with shape (len(texts1), len(texts2))
        """
        # Prepare request payload
        payload = {
            "texts1": texts1,
            "texts2": texts2
        }

        # Make request
        response = await self._post_with_retry("/similarity/compute", payload)
        data = response.json()

        # Extract similarity matrix and convert to numpy array
        similarity_matrix = np.array(data["similarity_matrix"])

        return similarity_matrix

    async def find_most_similar(
        self,
        query: str,
        candidates: List[str],
        top_k: int = 5
    ) -> List[Tuple[int, str, float]]:
        """
        Find the most similar texts to a query via HTTP API.

        Args:
            query: Query text
            candidates: List of candidate texts
            top_k: Number of top matches to return

        Returns:
            List of (index, text, similarity_score) tuples
        """
        # Prepare request payload
        payload = {
            "query": query,
            "candidates": candidates,
            "top_k": top_k
        }

        # Make request
        response = await self._post_with_retry("/similarity/search", payload)
        data = response.json()

        # Convert response format to match EmbeddingEngine interface
        results = [
            (r["index"], r["text"], r["similarity_score"])
            for r in data["results"]
        ]

        return results

    async def embed_with_segments(
        self,
        texts: Union[str, List[str]],
        show_progress: bool = False
    ) -> Union[Tuple[np.ndarray, List[str]], Tuple[np.ndarray, List[List[str]]]]:
        """
        Generate embeddings and return the text segments used via HTTP API.

        Note: The HTTP API currently doesn't return segment information,
        so this method falls back to returning embeddings only with empty segment lists.
        Consider enhancing the API if segment information is needed.

        Args:
            texts: Single text string or list of text strings
            show_progress: Whether to show progress information

        Returns:
            If single text: Tuple of (embedding, segments_list)
            If multiple texts: Tuple of (embeddings_array, list_of_segments_lists)
        """
        # Prefer using the detailed API that can return segment data
        embeddings, details = await self.embed_with_details(
            texts,
            show_progress=show_progress,
            include_segment_embeddings=True,
        )
        # Return only segment texts for backward compatibility of this helper
        segment_texts = [d.get("segment_texts", []) for d in details]
        if isinstance(texts, str):
            return embeddings[0], segment_texts[0]
        return embeddings, segment_texts

    async def embed_with_details(
        self,
        texts: Union[str, List[str]],
        show_progress: bool = False,
        include_segment_embeddings: Optional[bool] = True,
        use_segmentation: bool = True,
        batch_size: Optional[int] = None,
    ) -> Tuple[np.ndarray, List[dict]]:
        """
        Generate embeddings and return optional per-text segment details when available.

        Returns:
            (embeddings_array, details_list)
            - embeddings_array: np.ndarray shape (n_texts, dim)
            - details_list: list of dict per input text, possibly containing keys:
                { 'segment_embeddings': List[List[float]],
                  'segment_texts': List[str],
                  'segment_quality_scores': List[float] }
        """
        # Normalize inputs
        single_text = isinstance(texts, str)
        if single_text:
            texts = [texts]
        texts = list(texts)

        # Resolve include flag
        include_segments = (
            bool(include_segment_embeddings)
            if include_segment_embeddings is not None
            else bool(self._app_config.get('embedding_service.include_segment_embeddings') or False)
        )

        if not texts:
            if self._embedding_dim is None:
                await self._fetch_embedding_dim()
            return np.array([]).reshape(0, self._embedding_dim), []

        MAX_TEXTS_PER_REQUEST = int(self.max_texts_per_request)
        total_texts = len(texts)
        if show_progress:
            self._logger.info(
                f"Embedding (details) {total_texts} texts via HTTP API (seg={'ON' if use_segmentation else 'OFF'}, include_segments={include_segments})"
            )

        all_embeddings: List[np.ndarray] = []
        all_details: List[dict] = []

        def _extract_details(data, expected_count: int) -> List[dict]:
            # Try to parse per-text details from various possible response shapes
            per_text: List[dict] = [{} for _ in range(expected_count)]
            # Case 1: unified top-level lists aligned to inputs
            for key in ("segment_embeddings", "segment_texts", "segment_quality_scores"):
                if key in data and isinstance(data[key], list) and len(data[key]) == expected_count:
                    for i in range(expected_count):
                        if data[key][i] is not None:
                            per_text[i][key] = data[key][i]
            # Case 2: results list of dicts
            if not any(per_text) and isinstance(data.get("results"), list) and len(data["results"]) == expected_count:
                for i, item in enumerate(data["results"]):
                    if isinstance(item, dict):
                        for key in ("segment_embeddings", "segment_texts", "segment_quality_scores"):
                            if key in item:
                                per_text[i][key] = item[key]
            return per_text

        if total_texts <= MAX_TEXTS_PER_REQUEST:
            payload = {
                "texts": texts,
                "use_segmentation": use_segmentation,
                "include_segment_embeddings": include_segments,
            }
            if batch_size is not None:
                payload["batch_size"] = batch_size
            payload_debug = {
                **{k: v for k, v in payload.items() if k != "texts"},
                "texts": [
                    (t[:100] + "...") if len(t) > 100 else t for t in texts[:3]
                ] + ([f"... and {len(texts)-3} more texts"] if len(texts) > 3 else []),
            }
            self._logger.debug(f"Single request (details) - Payload: {payload_debug}")
            response = await self._post_with_retry("/embeddings", payload)
            data = response.json()
            embs = np.array(data["embeddings"])
            if self._embedding_dim is None:
                self._embedding_dim = embs.shape[1] if len(embs) > 0 else data.get("shape", [0, self._embedding_dim or 0])[1]
            all_embeddings.append(embs)
            all_details.extend(_extract_details(data, len(texts)))
        else:
            num_chunks = (total_texts + MAX_TEXTS_PER_REQUEST - 1) // MAX_TEXTS_PER_REQUEST
            for i in range(0, total_texts, MAX_TEXTS_PER_REQUEST):
                chunk = texts[i : i + MAX_TEXTS_PER_REQUEST]
                chunk_num = i // MAX_TEXTS_PER_REQUEST + 1
                if show_progress:
                    self._logger.info(f"Processing chunk {chunk_num}/{num_chunks} (details) with {len(chunk)} texts")
                payload = {
                    "texts": chunk,
                    "use_segmentation": use_segmentation,
                    "include_segment_embeddings": include_segments,
                }
                if batch_size is not None:
                    payload["batch_size"] = batch_size
                payload_debug = {
                    **{k: v for k, v in payload.items() if k != "texts"},
                    "texts": [
                        (t[:100] + "...") if len(t) > 100 else t for t in chunk[:3]
                    ] + ([f"... and {len(chunk)-3} more texts"] if len(chunk) > 3 else []),
                }
                self._logger.debug(f"Chunk {chunk_num}/{num_chunks} (details) - Payload: {payload_debug}")
                response = await self._post_with_retry("/embeddings", payload)
                data = response.json()
                embs = np.array(data["embeddings"])
                all_embeddings.append(embs)
                if self._embedding_dim is None:
                    self._embedding_dim = embs.shape[1] if len(embs) > 0 else data.get("shape", [0, self._embedding_dim or 0])[1]
                per_text = _extract_details(data, len(chunk))
                all_details.extend(per_text)

        embeddings = np.vstack(all_embeddings) if all_embeddings else np.empty((0, self._embedding_dim or 0))
        # Ensure details length matches number of inputs
        if len(all_details) != total_texts:
            # pad or trim to match
            if len(all_details) < total_texts:
                all_details.extend({} for _ in range(total_texts - len(all_details)))
            else:
                all_details = all_details[:total_texts]

        if single_text:
            return embeddings, [all_details[0]]
        return embeddings, all_details

    async def get_model_info(self) -> dict:
        """
        Get information about the loaded model from the service via HTTP API.

        Returns:
            Dictionary with model information
        """
        client = await self._get_client()

        try:
            response = await client.get("/models/info")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            self._logger.warning(f"Failed to get model info: {e}")
            # Return basic info if endpoint not available
            return {
                "service_url": self.service_url,
                "embedding_dim": self._embedding_dim,
                "error": str(e)
            }

    async def _fetch_embedding_dim(self):
        """Fetch and cache the embedding dimension from the service."""
        try:
            # Use a test embedding to get dimension
            test_embedding = await self.embed(["test"], show_progress=False)
            self._embedding_dim = test_embedding.shape[1]
        except Exception as e:
            self._logger.warning(f"Failed to fetch embedding dimension: {e}, defaulting to 384")
            self._embedding_dim = 384  # Default for all-MiniLM-L6-v2

    @property
    def embedding_dim(self) -> Optional[int]:
        """Get the embedding dimension (may be None if not yet fetched)."""
        return self._embedding_dim

    async def close(self):
        """Close the HTTP client and cleanup resources."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            self._logger.info("HTTP client closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()

    def __del__(self):
        """Cleanup on deletion."""
        # Note: Can't call async close() in __del__, so we just log
        if self._client is not None:
            self._logger.warning("EmbeddingServiceClient deleted without explicit close() - "
                               "use async context manager or call close() explicitly")


# Convenience function for quick embedding generation via HTTP
async def quick_embed(
    texts: Union[str, List[str]],
    service_url: str = None,
    use_segmentation: bool = True,
    include_segment_embeddings: Optional[bool] = None,
    **kwargs
) -> np.ndarray:
    """
    Quick embedding generation via HTTP service without persistent client.

    Args:
        texts: Text(s) to embed
        service_url: Service URL (uses default if None)
        use_segmentation: Whether to use text segmentation for long texts
        **kwargs: Additional arguments for EmbeddingServiceClient

    Returns:
        Numpy array of embeddings
    """
    async with EmbeddingServiceClient(service_url=service_url, **kwargs) as client:
        return await client.embed(
            texts,
            use_segmentation=use_segmentation,
            include_segment_embeddings=include_segment_embeddings,
        )


if __name__ == "__main__":
    # Example usage
    import time

    async def test_embedding_client():
        """Test the embedding service client."""
        logging.basicConfig(level=logging.INFO)

        # Test texts
        test_texts = [
            "The cat sat on the mat.",
            "A feline rested on the rug.",
            "The dog ran in the park.",
            "Artificial intelligence is transforming technology.",
            "Machine learning algorithms process data efficiently."
        ]

        print("Initializing EmbeddingServiceClient...")
        async with EmbeddingServiceClient() as client:
            print(f"Service URL: {client.service_url}")

            # Test model info
            print("\nFetching model info...")
            try:
                model_info = await client.get_model_info()
                print(f"Model info: {model_info}")
            except Exception as e:
                print(f"Could not fetch model info: {e}")

            # Test embedding
            print(f"\nGenerating embeddings for {len(test_texts)} texts...")
            start_time = time.time()
            embeddings = await client.embed(test_texts, show_progress=True)
            elapsed = time.time() - start_time

            print(f"Generated embeddings shape: {embeddings.shape}")
            print(f"Time: {elapsed:.2f}s ({len(test_texts)/elapsed:.1f} texts/sec)")

            # Test similarity search
            print("\nFinding most similar texts to query...")
            query = "A cat lying on a carpet"
            similar = await client.find_most_similar(query, test_texts, top_k=3)

            print(f"\nQuery: '{query}'")
            print("Most similar texts:")
            for idx, text, score in similar:
                print(f"  {score:.4f}: {text}")

            # Test similarity matrix
            print("\nComputing similarity matrix...")
            sim_matrix = await client.similarity(test_texts[:2], test_texts[2:])
            print(f"Similarity matrix shape: {sim_matrix.shape}")
            print(f"Sample similarities:\n{sim_matrix}")

    # Run the async test
    asyncio.run(test_embedding_client())
