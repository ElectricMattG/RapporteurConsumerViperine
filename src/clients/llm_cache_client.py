#!/usr/bin/env python3
"""
Cached LLM Client — Redis-backed response cache for LLM calls.

Wraps any LLM client that exposes ``async generate_content(prompt, **kw) -> str``
and transparently caches responses in Redis keyed by ``sha256(model + "|" + prompt)``.

When ``redis_host`` is empty the wrapper degrades to a pass-through — no
code changes required to run without Redis (local dev, CI, etc.).

Cache key design:
    ``sha256(model_name + "|" + prompt)[:32]``
    — deterministic across runs for the same prompt+model pair.
    — 32 hex chars = 128 bits of collision resistance; sufficient for
      a dataset of this size.

Prometheus counters ``bboa_llm_cache_hits_total`` and ``bboa_llm_cache_misses_total``
are labelled by ``stage`` so that per-phase hit rates are visible on Grafana.

The Redis connection is created lazily on each ``generate_content()`` call
so that it is bound to the *current* event loop.  This is necessary because
the Kafka consumer calls ``asyncio.run()`` per message, creating a fresh
event loop each time and closing it afterwards.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CachedLLMClient:
    """Redis-cached decorator for an async LLM client.

    Parameters
    ----------
    inner_client
        The underlying LLM client (e.g. ``GeminiLLMClient``).  Must have
        an ``async generate_content(prompt, **kwargs) -> str`` method,
        a ``model_name: str`` attribute, and optionally ``cost_tracker``,
        ``get_usage()``, ``reset_usage()``.
    redis_client
        **(deprecated)** A pre-created ``redis.asyncio.Redis`` instance.
        Prefer passing ``redis_host`` instead to avoid event-loop issues.
    redis_host
        Redis hostname.  When non-empty, a fresh ``redis.asyncio.Redis``
        connection is created per ``generate_content()`` call.
    redis_port
        Redis port (default 6379).
    redis_password
        Redis password (default ``None``).
    ttl_seconds
        Time-to-live for cached responses in seconds (default 24 h).
    """

    def __init__(
        self,
        inner_client: Any,
        redis_client: Any = None,
        ttl_seconds: int = 86400,
        *,
        redis_host: str = "",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
    ) -> None:
        self._inner = inner_client
        self._ttl = ttl_seconds

        # New path: store connection params for lazy creation
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_password = redis_password

        # Legacy path: accept a pre-created client (tests / backwards compat)
        self._redis = redis_client

    @property
    def _cache_enabled(self) -> bool:
        return bool(self._redis_host) or self._redis is not None

    async def _get_redis(self) -> Any:
        """Return a Redis client bound to the current event loop."""
        if self._redis is not None:
            return self._redis
        if self._redis_host:
            import redis.asyncio as aioredis
            return aioredis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                password=self._redis_password if self._redis_password else None,
                decode_responses=False,
            )
        return None

    # ── Proxy attributes from inner client ──────────────────────────

    @property
    def model_name(self) -> str:
        return self._inner.model_name

    @property
    def _stage_models(self) -> Dict[str, str]:
        return getattr(self._inner, '_stage_models', {})

    @property
    def cost_tracker(self) -> Any:
        return self._inner.cost_tracker

    @cost_tracker.setter
    def cost_tracker(self, tracker: Any) -> None:
        self._inner.cost_tracker = tracker

    def get_usage(self) -> Dict[str, int]:
        return self._inner.get_usage()

    def reset_usage(self) -> None:
        self._inner.reset_usage()

    # ── Cache key computation ───────────────────────────────────────

    @staticmethod
    def _cache_key(model_name: str, prompt: str) -> str:
        """Deterministic cache key: first 32 hex chars of sha256(model|prompt)."""
        raw = f"{model_name}|{prompt}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    # ── Core method ─────────────────────────────────────────────────

    async def generate_content(
        self,
        prompt: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        enable_search: bool = False,
        return_grounding_metadata: bool = False,
        response_mime_type: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> Any:
        """Generate content, serving from Redis cache when available.

        Cache is bypassed when:
        - cache is not enabled (no redis_host and no redis_client)
        - ``enable_search`` is True (web-grounded results must be fresh)
        - ``return_grounding_metadata`` is True (non-string return)
        """
        try:
            from src.components.bboa.bboa_metrics import (llm_cache_hits,
                                                          llm_cache_misses)
        except (ImportError, ModuleNotFoundError):
            llm_cache_hits = llm_cache_misses = None

        stage_label = stage or "unknown"

        # Bypass cache for non-cacheable calls or when Redis is absent
        bypass = (
            not self._cache_enabled
            or enable_search
            or return_grounding_metadata
        )

        # Resolve effective model for this stage (V17)
        effective_model = self._stage_models.get(stage, self.model_name) if stage else self.model_name

        redis_conn = None
        if not bypass:
            key = self._cache_key(effective_model, prompt)
            try:
                redis_conn = await self._get_redis()
                cached = await redis_conn.get(key)
                if cached is not None:
                    if llm_cache_hits is not None:
                        llm_cache_hits.labels(stage=stage_label).inc()
                    logger.debug("LLM cache HIT  [%s] key=%s…", stage_label, key[:12])
                    return cached.decode("utf-8") if isinstance(cached, bytes) else cached
            except Exception:
                # Redis failure should never block the pipeline
                logger.warning("Redis GET failed for key=%s; falling through to LLM", key[:12])

            if llm_cache_misses is not None:
                llm_cache_misses.labels(stage=stage_label).inc()
            logger.debug("LLM cache MISS [%s] key=%s…", stage_label, key[:12])

        # Call the real LLM — only forward kwargs the inner client accepts
        import inspect
        _sig = inspect.signature(self._inner.generate_content)
        _params = set(_sig.parameters.keys())
        _kwargs: Dict[str, Any] = {}
        for k, v in {
            "temperature": temperature,
            "max_tokens": max_tokens,
            "enable_search": enable_search,
            "return_grounding_metadata": return_grounding_metadata,
            "response_mime_type": response_mime_type,
        }.items():
            if k in _params:
                _kwargs[k] = v
        result = await self._inner.generate_content(prompt, **_kwargs)

        # Store in cache (only for simple string results, non-bypass)
        if not bypass and isinstance(result, str):
            try:
                if redis_conn is None:
                    redis_conn = await self._get_redis()
                await redis_conn.setex(key, self._ttl, result)
            except Exception:
                logger.warning("Redis SETEX failed for key=%s; result not cached", key[:12])

        return result

    # ── Forward any other attribute access to inner client ──────────

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)
