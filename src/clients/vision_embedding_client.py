"""
Async HTTP Client for Vision Embedding Service

This module provides an async HTTP client for DINOv2/v3 vision embeddings,
mirroring the EmbeddingServiceClient architecture for consistency.
"""

import asyncio
import base64
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union

import httpx
import numpy as np
from PIL import Image

from utils.correlation import get_request_id, get_session_id

# Import base configuration and logging
try:
    from ...config_handler import Base
except ImportError:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class VisionEmbeddingClient(Base):
    """
    Async HTTP client for vision embedding service with connection pooling and retry logic.

    Supports DINOv2/v3 models for image embeddings, similarity search, and top-k retrieval.
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
        Initialize the vision embedding service client.

        Args:
            service_url: Base URL of the vision service (uses config default if None)
            api_key: API key for authentication (uses config default if None)
            timeout: Request timeout in seconds (uses config default if None)
            max_retries: Maximum number of retry attempts (uses config default if None)
            retry_delay: Initial delay between retries in seconds (uses config default if None)
            max_connections: Maximum number of concurrent connections (uses config default if None)
            max_keepalive_connections: Maximum number of keepalive connections (uses config default if None)
        """
        # Initialize base class
        super().__init__()

        # Get configuration values with fallback defaults
        self.service_url = (
            service_url or
            os.getenv('VISION_EMBEDDING_SERVICE_URL') or
            self._app_config.get('vision_embedding_service.url') or
            'http://vision.192-168-1-3.nip.io'
        )

        # Try to get API key: priority is parameter > env var > config file
        self.api_key = api_key or os.getenv('VISION_EMBEDDING_SERVICE_API_KEY')
        if not self.api_key:
            try:
                self.api_key = self._app_config.get('vision_embedding_service.api_key')
            except (KeyError, AttributeError):
                self.api_key = None

        timeout_value = (
            timeout if timeout is not None
            else (self._app_config.get('vision_embedding_service.timeout') or 300.0)
        )
        self.max_retries = (
            max_retries if max_retries is not None
            else (self._app_config.get('vision_embedding_service.max_retries') or 3)
        )
        self.retry_delay = (
            retry_delay if retry_delay is not None
            else (self._app_config.get('vision_embedding_service.retry_delay') or 1.0)
        )
        max_conn = (
            max_connections if max_connections is not None
            else (self._app_config.get('vision_embedding_service.max_connections') or 10)
        )
        max_keepalive = (
            max_keepalive_connections if max_keepalive_connections is not None
            else (self._app_config.get('vision_embedding_service.max_keepalive_connections') or 5)
        )

        # Create timeout configuration
        self.timeout = httpx.Timeout(timeout_value, connect=10.0)

        # Create connection pool limits
        self.limits = httpx.Limits(
            max_connections=max_conn,
            max_keepalive_connections=max_keepalive
        )

        # Centralized request chunking limit (images per request)
        self.max_images_per_request: int = (
            self._app_config.get('vision_embedding_service.max_images_per_request') or 16
        )

        # Get preprocessing settings
        self.resize_max_dimension: Optional[int] = (
            self._app_config.get('vision_embedding_service.resize_max_dimension')
        )
        self.max_image_size_mb: float = (
            self._app_config.get('vision_embedding_service.max_image_size_mb') or 10.0
        )
        self.supported_formats: List[str] = (
            self._app_config.get('vision_embedding_service.supported_formats') or
            ['jpeg', 'jpg', 'png', 'webp', 'gif']
        )

        # HTTP client will be initialized on first use.
        # Track the event loop to detect stale clients from prior asyncio.run() calls.
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

        # Cache embedding dimension from service
        self._embedding_dim: Optional[int] = None

        self._logger.info(
            f"VisionEmbeddingClient initialized - URL: {self.service_url}, "
            f"timeout: {timeout_value}s, max_retries: {self.max_retries}"
        )
        self._logger.info(f"max_images_per_request: {self.max_images_per_request}")

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client bound to the *current* event loop."""
        loop = asyncio.get_running_loop()
        if self._client is not None and self._client_loop is not loop:
            self._client = None
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.service_url,
                timeout=self.timeout,
                limits=self.limits,
                verify=False
            )
            self._client_loop = loop
            self._logger.info("HTTP client initialized for vision service")
        return self._client

    def _validate_image_format(self, image_path: Union[str, Path]) -> str:
        """
        Validate image format and return format string.

        Args:
            image_path: Path to image file

        Returns:
            Image format (e.g., 'jpeg', 'png')

        Raises:
            ValueError: If image format not supported
        """
        path = Path(image_path)
        if not path.exists():
            raise FileNotFoundError(f"Image file not found: {image_path}")

        # Try to open with PIL to validate
        try:
            with Image.open(path) as img:
                format_str = img.format.lower() if img.format else None
                if format_str not in self.supported_formats:
                    raise ValueError(
                        f"Unsupported image format '{format_str}' for {image_path}. "
                        f"Supported: {self.supported_formats}"
                    )
                return format_str
        except Exception as e:
            raise ValueError(f"Invalid image file {image_path}: {e}")

    def _resize_image_if_needed(
        self,
        image_path: Union[str, Path],
        max_dimension: Optional[int] = None
    ) -> Tuple[bytes, dict]:
        """
        Resize image if it exceeds max dimension and return bytes + metadata.

        Args:
            image_path: Path to image file
            max_dimension: Maximum width/height dimension (uses config if None)

        Returns:
            Tuple of (image_bytes, metadata_dict)
        """
        path = Path(image_path)
        max_dim = max_dimension or self.resize_max_dimension

        with Image.open(path) as img:
            original_size = img.size
            metadata = {
                'format': img.format.lower() if img.format else 'jpeg',
                'original_size': original_size,
                'preprocessing': 'none'
            }

            # Check if resize needed
            if max_dim and (img.width > max_dim or img.height > max_dim):
                # Calculate new size maintaining aspect ratio
                ratio = min(max_dim / img.width, max_dim / img.height)
                new_size = (int(img.width * ratio), int(img.height * ratio))

                img = img.resize(new_size, Image.Resampling.LANCZOS)
                metadata['resized_size'] = new_size
                metadata['preprocessing'] = f'resize_{max_dim}x{max_dim}'

                self._logger.debug(
                    f"Resized image {path.name} from {original_size} to {new_size}"
                )

            # Convert to RGB if needed (for JPEG compatibility)
            if img.mode not in ('RGB', 'L'):
                img = img.convert('RGB')
                metadata['color_conversion'] = f'{original_size[2] if len(original_size) > 2 else "unknown"}_to_RGB'

            # Save to bytes
            from io import BytesIO
            buffer = BytesIO()
            save_format = metadata['format'].upper()
            if save_format == 'JPG':
                save_format = 'JPEG'
            img.save(buffer, format=save_format)
            image_bytes = buffer.getvalue()
            metadata['size_bytes'] = len(image_bytes)

            return image_bytes, metadata

    def _image_to_base64(
        self,
        image_path: Union[str, Path],
        resize_max: Optional[int] = None
    ) -> Tuple[str, dict]:
        """
        Convert image file to base64 string with optional resizing.

        Args:
            image_path: Path to image file
            resize_max: Maximum dimension for resizing (optional)

        Returns:
            Tuple of (base64_string, metadata_dict)
        """
        # Validate format first
        self._validate_image_format(image_path)

        # Check file size
        path = Path(image_path)
        size_mb = path.stat().st_size / (1024 * 1024)
        if size_mb > self.max_image_size_mb:
            raise ValueError(
                f"Image {path.name} too large ({size_mb:.1f}MB). "
                f"Max: {self.max_image_size_mb}MB"
            )

        # Resize if needed
        image_bytes, metadata = self._resize_image_if_needed(image_path, resize_max)

        # Encode to base64
        b64_string = base64.b64encode(image_bytes).decode('utf-8')

        return b64_string, metadata

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

        # Propagate correlation IDs from the current async context.
        # Sentinel "-" means unbound — omit entirely so the embedding service
        # never sees "X-Session-Id: -". No X-Origin: parity with RAG 0.2.0.
        _sid = get_session_id()
        _rid = get_request_id()
        if _sid and _sid != "-":
            headers['X-Session-Id'] = _sid
        if _rid and _rid != "-":
            headers['X-Request-Id'] = _rid

        for attempt in range(self.max_retries):
            try:
                response = await client.post(endpoint, json=json_data, headers=headers)
                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                error_body = e.response.text[:500] if e.response.text else "No response body"

                # Don't retry on client errors (4xx)
                if 400 <= e.response.status_code < 500:
                    self._logger.error(f"Client error {e.response.status_code}: {error_body}")
                    raise

                # Retry on server errors (5xx)
                if attempt == self.max_retries - 1:
                    self._logger.error(
                        f"Server error after {self.max_retries} attempts: {e}"
                    )
                    self._logger.error(f"Response body: {error_body}")
                    raise

                delay = self.retry_delay * (2 ** attempt)
                self._logger.warning(
                    f"Server error (attempt {attempt + 1}/{self.max_retries}), "
                    f"retrying in {delay:.1f}s... Status: {e.response.status_code}"
                )
                await asyncio.sleep(delay)

            except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt == self.max_retries - 1:
                    self._logger.error(
                        f"Network error after {self.max_retries} attempts: {e}"
                    )
                    raise

                delay = self.retry_delay * (2 ** attempt)
                self._logger.warning(
                    f"Network error (attempt {attempt + 1}/{self.max_retries}), "
                    f"retrying in {delay:.1f}s..."
                )
                await asyncio.sleep(delay)

    async def embed_images(
        self,
        images: Union[str, Path, List[Union[str, Path]]],
        model: str = None,
        normalize: bool = True,
        resize_max: Optional[int] = None,
        show_progress: bool = False
    ) -> Tuple[np.ndarray, List[dict]]:
        """
        Generate embeddings for images via HTTP API.

        Args:
            images: Single image path or list of image paths
            model: Model to use (dinov2-base, dinov2-large, dinov3-base, etc.)
            normalize: Normalize embedding vectors
            resize_max: Maximum dimension for resizing (uses config if None)
            show_progress: Whether to show progress information

        Returns:
            Tuple of (embeddings_array, metadata_list)
            - embeddings_array: np.ndarray shape (n_images, embedding_dim)
            - metadata_list: List[dict] with preprocessing info per image
        """
        # Handle single image input
        if isinstance(images, (str, Path)):
            images = [images]

        if not images:
            if self._embedding_dim is None:
                await self._fetch_embedding_dim(model)
            return np.array([]).reshape(0, self._embedding_dim), []

        # Get model from config if not specified
        if model is None:
            model = self._app_config.get('vision_embedding_service.default_model') or 'dinov2-base'

        # Get resize setting
        resize_dim = resize_max or self.resize_max_dimension

        total_images = len(images)

        if show_progress:
            self._logger.info(
                f"Embedding {total_images} images via HTTP API (model={model})"
            )

        # Convert images to base64
        base64_images = []
        metadata_list = []

        for img_path in images:
            try:
                b64_str, metadata = self._image_to_base64(img_path, resize_dim)
                base64_images.append(b64_str)
                metadata_list.append(metadata)
            except Exception as e:
                self._logger.error(f"Failed to process image {img_path}: {e}")
                # Add empty entry to maintain alignment
                base64_images.append(None)
                metadata_list.append({'error': str(e), 'format': 'unknown', 'size_bytes': 0})

        # Filter out failed images
        valid_indices = [i for i, b64 in enumerate(base64_images) if b64 is not None]
        valid_base64 = [base64_images[i] for i in valid_indices]
        valid_metadata = [metadata_list[i] for i in valid_indices]

        if not valid_base64:
            self._logger.error("No valid images to embed")
            if self._embedding_dim is None:
                await self._fetch_embedding_dim(model)
            return np.array([]).reshape(0, self._embedding_dim), metadata_list

        # If images fit in single request, send directly
        MAX_IMAGES_PER_REQUEST = self.max_images_per_request

        if len(valid_base64) <= MAX_IMAGES_PER_REQUEST:
            # Prepare request payload
            payload = {
                "images": valid_base64,
                "model": model,
                "normalize": normalize
            }

            # Make request
            response = await self._post_with_retry("/vision/embeddings", payload)
            data = response.json()

            # Extract embeddings
            embeddings = np.array(data["embeddings"])

            # Cache embedding dimension
            if self._embedding_dim is None:
                self._embedding_dim = embeddings.shape[1] if len(embeddings) > 0 else 768

            if show_progress:
                self._logger.info(
                    f"Received {len(embeddings)} embeddings from service "
                    f"(dim={embeddings.shape[1] if len(embeddings) > 0 else 0})"
                )

            # Reinsert embeddings for failed images (zeros)
            full_embeddings = np.zeros((len(images), self._embedding_dim))
            for i, idx in enumerate(valid_indices):
                if i < len(embeddings):
                    full_embeddings[idx] = embeddings[i]

            return full_embeddings, metadata_list

        # Otherwise, split into chunks and process in batches
        num_chunks = (len(valid_base64) + MAX_IMAGES_PER_REQUEST - 1) // MAX_IMAGES_PER_REQUEST

        if show_progress:
            self._logger.info(
                f"Splitting into {num_chunks} chunks of max {MAX_IMAGES_PER_REQUEST} images"
            )

        all_embeddings: List[np.ndarray] = []

        for i in range(0, len(valid_base64), MAX_IMAGES_PER_REQUEST):
            chunk = valid_base64[i : i + MAX_IMAGES_PER_REQUEST]
            chunk_size = len(chunk)

            if show_progress:
                chunk_num = i // MAX_IMAGES_PER_REQUEST + 1
                self._logger.info(
                    f"Processing chunk {chunk_num}/{num_chunks} ({chunk_size} images)"
                )

            # Prepare request payload
            payload = {
                "images": chunk,
                "model": model,
                "normalize": normalize
            }

            # Make request
            response = await self._post_with_retry("/vision/embeddings", payload)
            data = response.json()

            # Extract embeddings
            chunk_embeddings = np.array(data["embeddings"])
            all_embeddings.append(chunk_embeddings)

            # Cache embedding dimension from first chunk
            if self._embedding_dim is None:
                self._embedding_dim = (
                    chunk_embeddings.shape[1] if len(chunk_embeddings) > 0 else 768
                )

        # Concatenate all embeddings
        valid_embeddings = (
            np.vstack(all_embeddings) if all_embeddings
            else np.empty((0, self._embedding_dim or 768))
        )

        # Reinsert embeddings for failed images
        full_embeddings = np.zeros((len(images), self._embedding_dim or 768))
        for i, idx in enumerate(valid_indices):
            if i < len(valid_embeddings):
                full_embeddings[idx] = valid_embeddings[i]

        if show_progress:
            self._logger.info(
                f"Completed embedding {total_images} images "
                f"(shape: {full_embeddings.shape}, {len(valid_indices)}/{total_images} valid)"
            )

        return full_embeddings, metadata_list

    async def compute_similarity(
        self,
        images1: Union[str, Path, List[Union[str, Path]]],
        images2: Union[str, Path, List[Union[str, Path]]],
        model: str = None
    ) -> np.ndarray:
        """
        Compute cosine similarity between two sets of images.

        Args:
            images1: First set of image paths
            images2: Second set of image paths
            model: Model to use (default from config)

        Returns:
            Similarity matrix with shape (len(images1), len(images2))
        """
        # Embed both sets
        embeddings1, _ = await self.embed_images(images1, model=model, show_progress=False)
        embeddings2, _ = await self.embed_images(images2, model=model, show_progress=False)

        # Compute cosine similarity
        # Normalize vectors
        norm1 = np.linalg.norm(embeddings1, axis=1, keepdims=True)
        norm2 = np.linalg.norm(embeddings2, axis=1, keepdims=True)

        normalized1 = embeddings1 / (norm1 + 1e-8)
        normalized2 = embeddings2 / (norm2 + 1e-8)

        # Compute similarity matrix
        similarity_matrix = np.dot(normalized1, normalized2.T)

        return similarity_matrix

    async def find_similar_images(
        self,
        query_image: Union[str, Path],
        candidate_images: List[Union[str, Path]],
        top_k: int = 5,
        model: str = None
    ) -> List[Tuple[int, str, float]]:
        """
        Find the most similar images to a query image.

        Args:
            query_image: Query image path
            candidate_images: List of candidate image paths
            top_k: Number of top matches to return
            model: Model to use (default from config)

        Returns:
            List of (index, image_path, similarity_score) tuples
        """
        # Compute similarity matrix
        similarity_matrix = await self.compute_similarity(
            [query_image],
            candidate_images,
            model=model
        )

        # Get similarity scores for query (first row)
        scores = similarity_matrix[0]

        # Get top-k indices
        top_indices = np.argsort(scores)[::-1][:top_k]

        # Build results
        results = [
            (int(idx), str(candidate_images[idx]), float(scores[idx]))
            for idx in top_indices
        ]

        return results

    async def _fetch_embedding_dim(self, model: str = None):
        """Fetch and cache the embedding dimension from the service."""
        try:
            # Use a test embedding to get dimension
            # Create a tiny dummy image
            from io import BytesIO
            dummy_img = Image.new('RGB', (10, 10), color='white')
            buffer = BytesIO()
            dummy_img.save(buffer, format='JPEG')
            dummy_b64 = base64.b64encode(buffer.getvalue()).decode('utf-8')

            model_name = model or self._app_config.get('vision_embedding_service.default_model') or 'dinov2-base'

            payload = {
                "images": [dummy_b64],
                "model": model_name,
                "normalize": True
            }

            response = await self._post_with_retry("/vision/embeddings", payload)
            data = response.json()
            embeddings = np.array(data["embeddings"])
            self._embedding_dim = embeddings.shape[1]

        except Exception as e:
            self._logger.warning(
                f"Failed to fetch embedding dimension: {e}, defaulting to 768"
            )
            self._embedding_dim = 768  # Default for dinov2-base

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
        if self._client is not None:
            self._logger.warning(
                "VisionEmbeddingClient deleted without explicit close() - "
                "use async context manager or call close() explicitly"
            )


# Convenience function for quick embedding generation
async def quick_embed_images(
    images: Union[str, Path, List[Union[str, Path]]],
    service_url: str = None,
    model: str = None,
    **kwargs
) -> np.ndarray:
    """
    Quick image embedding via HTTP service without persistent client.

    Args:
        images: Image path(s) to embed
        service_url: Service URL (uses default if None)
        model: Model to use (default from config)
        **kwargs: Additional arguments for VisionEmbeddingClient

    Returns:
        Numpy array of embeddings
    """
    async with VisionEmbeddingClient(service_url=service_url, **kwargs) as client:
        embeddings, _ = await client.embed_images(images, model=model)
        return embeddings
