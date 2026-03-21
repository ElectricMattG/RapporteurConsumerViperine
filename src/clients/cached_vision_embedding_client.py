#!/usr/bin/env python3
"""
Cached Vision Embedding Client — Redis-backed cache for DINOv2/v3 embeddings.

Wraps ``VisionEmbeddingClient.embed_images()`` at the per-image level:
each image path is hashed individually so hits can be served from cache
without calling the embedding service, and misses are batched and forwarded.

Cache key design
----------------
sha256(model + "|" + sha256(image_bytes).hexdigest())[:32]

The key is based on image *content*, not path, so renaming or moving the
file does not invalidate the cache entry.

Pass-through mode
-----------------
When ``redis_host`` is empty the wrapper is a no-op — ``embed_images()``
is forwarded directly to the inner client unchanged.

Prometheus
----------
Increments ``lamia_redis_cache_hits_total`` / ``lamia_redis_cache_misses_total``
with ``service="vision_embedding"``.

Return contract
---------------
Returns the same ``(np.ndarray, List[dict])`` tuple as the inner client's
``embed_images()``, maintaining full drop-in compatibility.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

try:
    from src.components.image.lamia_metrics import (redis_cache_hits,
                                                    redis_cache_misses)
except ImportError:
    redis_cache_hits = None  # type: ignore[assignment]
    redis_cache_misses = None  # type: ignore[assignment]


class CachedVisionEmbeddingClient:
    """Redis-cached decorator for VisionEmbeddingClient.

    Parameters
    ----------
    inner_client
        The underlying ``VisionEmbeddingClient`` instance.
    redis_host
        Redis hostname.  Empty string disables caching.
    redis_port
        Redis port (default 6379).
    redis_password
        Redis password (default None).
    ttl_seconds
        Cache TTL in seconds (default 24 h = 86 400).
    """

    def __init__(
        self,
        inner_client: Any,
        redis_host: str = "",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        ttl_seconds: int = 86400,
    ) -> None:
        self._inner = inner_client
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_password = redis_password
        self._ttl = ttl_seconds

    @property
    def _cache_enabled(self) -> bool:
        return bool(self._redis_host)

    async def _get_redis(self) -> Any:
        """Return a fresh Redis client bound to the current event loop."""
        import redis.asyncio as aioredis
        return aioredis.Redis(
            host=self._redis_host,
            port=self._redis_port,
            password=self._redis_password if self._redis_password else None,
            decode_responses=False,
        )

    @staticmethod
    def _image_content_hash(image_path: Union[str, Path]) -> Optional[str]:
        """Return sha256 hexdigest of image file contents, or None if unreadable."""
        try:
            return hashlib.sha256(Path(image_path).read_bytes()).hexdigest()
        except Exception:
            return None

    @staticmethod
    def _cache_key(model: str, content_hash: str) -> str:
        """Deterministic cache key: sha256(model + "|" + content_hash)[:32]."""
        raw = f"{model}|{content_hash}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    async def embed_images(
        self,
        images: Union[str, Path, List[Union[str, Path]]],
        model: str = None,
        normalize: bool = True,
        resize_max: Optional[int] = None,
        show_progress: bool = False,
    ) -> Tuple[Any, List[dict]]:
        """Embed images, serving from Redis cache per-image when available.

        When caching is disabled, forwards directly to the inner client.
        """
        import numpy as np

        if isinstance(images, (str, Path)):
            images = [images]

        if not self._cache_enabled:
            return await self._inner.embed_images(
                images=images,
                model=model,
                normalize=normalize,
                resize_max=resize_max,
                show_progress=show_progress,
            )

        effective_model = model or "dinov2-base"
        redis_conn = None

        # Per-image: check cache first, collect misses for batch call
        hit_indices: dict[int, Any] = {}   # index → cached embedding (list)
        miss_indices: list[int] = []       # indices needing actual embedding call
        metadata_list: list[dict] = [{}] * len(images)

        try:
            redis_conn = await self._get_redis()
        except Exception:
            logger.warning("Redis connect failed for vision embedding; falling through")
            return await self._inner.embed_images(
                images=images, model=model, normalize=normalize,
                resize_max=resize_max, show_progress=show_progress,
            )

        for i, img_path in enumerate(images):
            content_hash = self._image_content_hash(img_path)
            if content_hash is None:
                miss_indices.append(i)
                continue

            key = self._cache_key(effective_model, content_hash)
            try:
                cached = await redis_conn.get(key)
            except Exception:
                cached = None

            if cached is not None:
                try:
                    embedding = json.loads(cached)
                    hit_indices[i] = embedding
                    if redis_cache_hits is not None:
                        redis_cache_hits.labels(service="vision_embedding").inc()
                    logger.debug("VisionEmb cache HIT  key=%s…", key[:12])
                except Exception:
                    miss_indices.append(i)
            else:
                miss_indices.append(i)
                if redis_cache_misses is not None:
                    redis_cache_misses.labels(service="vision_embedding").inc()
                logger.debug("VisionEmb cache MISS key=%s…", key[:12])

        # Batch-call inner client for misses
        miss_embeddings = None
        miss_metadata: list[dict] = []
        if miss_indices:
            miss_paths = [images[i] for i in miss_indices]
            miss_embeddings_arr, miss_metadata = await self._inner.embed_images(
                images=miss_paths,
                model=model,
                normalize=normalize,
                resize_max=resize_max,
                show_progress=show_progress,
            )
            miss_embeddings = miss_embeddings_arr

            # Store each miss result in Redis
            for local_idx, orig_idx in enumerate(miss_indices):
                if local_idx >= len(miss_embeddings):
                    continue
                content_hash = self._image_content_hash(images[orig_idx])
                if content_hash is None:
                    continue
                key = self._cache_key(effective_model, content_hash)
                try:
                    await redis_conn.setex(
                        key,
                        self._ttl,
                        json.dumps(miss_embeddings[local_idx].tolist()),
                    )
                except Exception:
                    logger.warning("Redis SETEX failed for VisionEmb key=%s", key[:12])

        # Reconstruct full result array in original order
        embedding_dim = None
        if miss_embeddings is not None and len(miss_embeddings) > 0:
            embedding_dim = miss_embeddings.shape[1]
        if embedding_dim is None and hit_indices:
            embedding_dim = len(next(iter(hit_indices.values())))

        if embedding_dim is None:
            # No embeddings at all; delegate to inner for consistent empty return
            return await self._inner.embed_images(
                images=[], model=model, normalize=normalize,
                resize_max=resize_max, show_progress=show_progress,
            )

        result_embeddings = np.zeros((len(images), embedding_dim), dtype=np.float32)
        result_metadata: list[dict] = [{}] * len(images)

        for i, emb in hit_indices.items():
            result_embeddings[i] = np.array(emb, dtype=np.float32)

        miss_local = 0
        for orig_idx in miss_indices:
            if miss_embeddings is not None and miss_local < len(miss_embeddings):
                result_embeddings[orig_idx] = miss_embeddings[miss_local]
                if miss_local < len(miss_metadata):
                    result_metadata[orig_idx] = miss_metadata[miss_local]
                miss_local += 1

        return result_embeddings, result_metadata

    # ── Forward anything else to the inner client ───────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
