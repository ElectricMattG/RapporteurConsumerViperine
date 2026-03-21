#!/usr/bin/env python3
"""
Cached Vision Client — Redis-backed response cache for VLM analyze_image() calls.

Wraps any object that satisfies the ``VisionClient`` protocol
(``model_name: str`` + ``async analyze_image(image_bytes, mime_type, prompt, ...) -> str``)
and transparently caches responses in Redis.

Cache key design
----------------
sha256(model_name + "|" + sha256(image_bytes).hexdigest() + "|" + prompt)[:32]

Using a two-level hash (sha256 of image bytes rather than raw bytes or base64)
keeps keys short and constant-length regardless of image size, while remaining
collision-resistant for any realistic vision dataset.

Pass-through mode
-----------------
When ``redis_host`` is empty (the default), the wrapper behaves identically to
the underlying client — no Redis import, no extra latency, no code changes needed.

Prometheus
----------
Increments ``lamia_redis_cache_hits_total`` / ``lamia_redis_cache_misses_total``
with ``service="vlm"`` so that hit rates are visible on the Grafana dashboard.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

try:
    from src.components.image.lamia_metrics import (redis_cache_hits,
                                                    redis_cache_misses)
except ImportError:
    redis_cache_hits = None  # type: ignore[assignment]
    redis_cache_misses = None  # type: ignore[assignment]


class CachedVisionClient:
    """Redis-cached decorator for a VisionClient.

    Parameters
    ----------
    inner_client
        The underlying VisionClient (GeminiVisionAdapter, OpenAIVisionAdapter,
        or any object with ``model_name`` + ``async analyze_image()``).
    redis_host
        Redis hostname.  When empty the wrapper is a no-op pass-through.
    redis_port
        Redis port (default 6379).
    redis_password
        Redis password (default None).
    ttl_seconds
        Time-to-live in seconds (default 24 h = 86 400).
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

    # ── Protocol surface ────────────────────────────────────────────

    @property
    def model_name(self) -> str:
        return self._inner.model_name

    # ── Cache enablement ────────────────────────────────────────────

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

    # ── Cache key ───────────────────────────────────────────────────

    @staticmethod
    def _cache_key(model_name: str, image_bytes: bytes, prompt: str) -> str:
        """Deterministic, constant-length key.

        sha256(model + "|" + sha256(image_bytes) + "|" + prompt)[:32]
        """
        image_hash = hashlib.sha256(image_bytes).hexdigest()
        raw = f"{model_name}|{image_hash}|{prompt}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    # ── Core method ─────────────────────────────────────────────────

    async def analyze_image(
        self,
        image_bytes: bytes,
        mime_type: str,
        prompt: str,
        temperature: float = 0.2,
        max_tokens: int = 16000,
    ) -> str:
        """Analyze image, returning cached response when available.

        Falls back to the inner client on any Redis error.
        """
        if not self._cache_enabled:
            return await self._inner.analyze_image(
                image_bytes=image_bytes,
                mime_type=mime_type,
                prompt=prompt,
                temperature=temperature,
                max_tokens=max_tokens,
            )

        key = self._cache_key(self._inner.model_name, image_bytes, prompt)
        redis_conn = None

        try:
            redis_conn = await self._get_redis()
            cached = await redis_conn.get(key)
            if cached is not None:
                if redis_cache_hits is not None:
                    redis_cache_hits.labels(service="vlm").inc()
                logger.debug("VLM cache HIT  key=%s…", key[:12])
                return cached.decode("utf-8") if isinstance(cached, bytes) else cached
        except Exception:
            logger.warning("Redis GET failed for VLM key=%s; falling through", key[:12])

        if redis_cache_misses is not None:
            redis_cache_misses.labels(service="vlm").inc()
        logger.debug("VLM cache MISS key=%s…", key[:12])

        result = await self._inner.analyze_image(
            image_bytes=image_bytes,
            mime_type=mime_type,
            prompt=prompt,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        if isinstance(result, str) and result:
            try:
                if redis_conn is None:
                    redis_conn = await self._get_redis()
                await redis_conn.setex(key, self._ttl, result)
            except Exception:
                logger.warning("Redis SETEX failed for VLM key=%s", key[:12])

        return result

    # ── Forward any other attribute to inner client ─────────────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
