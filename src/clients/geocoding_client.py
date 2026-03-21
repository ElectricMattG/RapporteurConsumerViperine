"""
Geocoding Client — convert place names to GPS coordinates.

Prioritises Google Maps Geocoding API (low-cost, already in the
Google Cloud ecosystem).  Falls back to Nominatim (OpenStreetMap)
when no Google API key is configured — free, no auth required.

Optionally caches results in Redis to avoid redundant API calls.
When ``redis_host`` is empty the cache layer is a no-op (pass-through).

Usage::

    from src.clients.geocoding_client import GeocodingClient

    geo = GeocodingClient(api_key="...")
    lat, lon = await geo.geocode("Aleppo, Syria")  # (36.2021, 37.1343)

    # With Redis cache
    geo = GeocodingClient(api_key="...", redis_host="redis.svc.cluster.local")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Prometheus metrics — optional import (same pattern as cached_vision_client)
try:
    from src.components.image.lamia_metrics import (geocoding_cache_total,
                                                    geocoding_latency,
                                                    geocoding_requests)
except ImportError:
    geocoding_cache_total = None   # type: ignore[assignment]
    geocoding_latency = None       # type: ignore[assignment]
    geocoding_requests = None      # type: ignore[assignment]

_GOOGLE_GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
_NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_USER_AGENT = "RapporteurConsumers/1.0 (lamia-image-pipeline)"

# Default cache TTLs
_DEFAULT_CACHE_TTL = 2592000       # 30 days for successful lookups
_DEFAULT_NEGATIVE_CACHE_TTL = 86400  # 1 day for failed lookups


class GeocodingClient:
    """Async Google Maps Geocoding API wrapper with optional Redis cache.

    Parameters
    ----------
    api_key : str, optional
        Google Maps API key.  Falls back to the ``GOOGLE_MAPS_API_KEY``
        environment variable.
    timeout : float
        HTTP request timeout in seconds.
    redis_host : str
        Redis hostname.  Empty string disables caching.
    redis_port : int
        Redis port (default 6379).
    redis_password : str, optional
        Redis password.
    cache_ttl_seconds : int
        TTL for successful geocoding results (default 30 days).
    negative_cache_ttl_seconds : int
        TTL for failed/unresolvable lookups (default 1 day).
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: float = 10.0,
        *,
        redis_host: str = "",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        cache_ttl_seconds: int = _DEFAULT_CACHE_TTL,
        negative_cache_ttl_seconds: int = _DEFAULT_NEGATIVE_CACHE_TTL,
    ):
        self.api_key = (
            api_key
            or os.getenv("GOOGLE_MAPS_API_KEY", "")
            or os.getenv("GOOGLE_GEOCODING_API_KEY", "")
            or os.getenv("GOOGLE_API_KEY", "")
        )
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

        # Redis cache params
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_password = redis_password
        self._cache_ttl = cache_ttl_seconds
        self._negative_cache_ttl = negative_cache_ttl_seconds

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        if self._client is not None and self._client_loop is not loop:
            self._client = None
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._timeout)
            self._client_loop = loop
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Redis cache helpers
    # ------------------------------------------------------------------

    @property
    def _cache_enabled(self) -> bool:
        return bool(self._redis_host)

    async def _get_redis(self) -> Any:
        """Return a Redis connection bound to the current event loop."""
        if not self._redis_host:
            return None
        import redis.asyncio as aioredis
        return aioredis.Redis(
            host=self._redis_host,
            port=self._redis_port,
            password=self._redis_password if self._redis_password else None,
            decode_responses=True,
        )

    @staticmethod
    def _cache_key(location_name: str) -> str:
        """Normalised cache key for a location query."""
        return f"geocode:{location_name.strip().lower()}"

    async def _cache_get(self, location_name: str) -> Optional[Tuple[Optional[Tuple[float, float]], bool]]:
        """Check Redis for a cached result.

        Returns
        -------
        None
            Cache miss or cache disabled.
        (result, True)
            Cache hit — ``result`` is ``(lat, lng)`` or ``None`` (negative cache).
        """
        if not self._cache_enabled:
            return None
        try:
            redis_conn = await self._get_redis()
            raw = await redis_conn.get(self._cache_key(location_name))
            if raw is None:
                return None
            data = json.loads(raw)
            if data.get("miss"):
                logger.debug(f"[Cache] Negative hit for '{location_name}'")
                return (None, True)
            logger.debug(
                f"[Cache] Hit for '{location_name}' → "
                f"({data['lat']:.4f}, {data['lng']:.4f})"
            )
            return ((data["lat"], data["lng"]), True)
        except Exception:
            logger.debug(f"Redis GET failed for geocode key; falling through")
            return None

    async def _cache_set(self, location_name: str, result: Optional[Tuple[float, float]]) -> None:
        """Store a geocoding result (or negative miss) in Redis."""
        if not self._cache_enabled:
            return
        try:
            redis_conn = await self._get_redis()
            key = self._cache_key(location_name)
            if result:
                value = json.dumps({"lat": result[0], "lng": result[1]})
                await redis_conn.setex(key, self._cache_ttl, value)
            else:
                value = json.dumps({"miss": True})
                await redis_conn.setex(key, self._negative_cache_ttl, value)
        except Exception:
            logger.debug(f"Redis SETEX failed for geocode key; result not cached")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def geocode(
        self, location_name: str
    ) -> Optional[Tuple[float, float]]:
        """Resolve *location_name* to ``(latitude, longitude)``.

        Checks Redis cache first (when configured).  On miss, tries
        Google Maps (when an API key is configured), then falls back
        to Nominatim (OpenStreetMap).  Stores the result in Redis
        for future calls.

        Returns ``None`` when no service can resolve the location.
        """
        if not location_name or not location_name.strip():
            return None

        import time as _time
        t0 = _time.monotonic()

        # ── Cache lookup ──────────────────────────────────────────
        cached = await self._cache_get(location_name)
        if cached is not None:
            result, _ = cached
            if geocoding_cache_total is not None:
                label = "negative_hit" if result is None else "hit"
                geocoding_cache_total.labels(result=label).inc()
            if geocoding_latency is not None:
                geocoding_latency.labels(source="cache").observe(_time.monotonic() - t0)
            return result

        if geocoding_cache_total is not None:
            geocoding_cache_total.labels(result="miss").inc()

        # ── API lookup chain ──────────────────────────────────────
        result = None

        if self.api_key:
            result = await self._geocode_google(location_name)

        if result is None:
            result = await self._geocode_nominatim(location_name)

        # ── Write-through cache ───────────────────────────────────
        await self._cache_set(location_name, result)

        return result

    # ------------------------------------------------------------------
    # Backend implementations
    # ------------------------------------------------------------------

    async def _geocode_google(
        self, location_name: str
    ) -> Optional[Tuple[float, float]]:
        """Google Maps Geocoding API."""
        import time as _time
        t0 = _time.monotonic()
        try:
            client = await self._ensure_client()
            resp = await client.get(
                _GOOGLE_GEOCODE_URL,
                params={"address": location_name, "key": self.api_key},
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "OK" or not data.get("results"):
                logger.debug(
                    f"Google geocoding '{location_name}' → status={data.get('status')}"
                )
                if geocoding_requests is not None:
                    geocoding_requests.labels(provider="google", status="error").inc()
                return None

            loc = data["results"][0]["geometry"]["location"]
            lat, lng = float(loc["lat"]), float(loc["lng"])
            logger.debug(f"[Google] Geocoded '{location_name}' → ({lat:.4f}, {lng:.4f})")
            if geocoding_requests is not None:
                geocoding_requests.labels(provider="google", status="success").inc()
            if geocoding_latency is not None:
                geocoding_latency.labels(source="google").observe(_time.monotonic() - t0)
            return lat, lng

        except Exception as e:
            logger.warning(f"Google geocoding error for '{location_name}': {e}")
            if geocoding_requests is not None:
                status = "rate_limited" if "429" in str(e) else "error"
                geocoding_requests.labels(provider="google", status=status).inc()
            return None

    async def _geocode_nominatim(
        self, location_name: str
    ) -> Optional[Tuple[float, float]]:
        """Nominatim (OpenStreetMap) fallback — free, no API key required."""
        import time as _time
        t0 = _time.monotonic()
        try:
            client = await self._ensure_client()
            resp = await client.get(
                _NOMINATIM_URL,
                params={"q": location_name, "format": "json", "limit": 1},
                headers={"User-Agent": _NOMINATIM_USER_AGENT},
            )
            resp.raise_for_status()
            results = resp.json()

            if not results:
                logger.debug(f"Nominatim: no results for '{location_name}'")
                if geocoding_requests is not None:
                    geocoding_requests.labels(provider="nominatim", status="error").inc()
                return None

            lat = float(results[0]["lat"])
            lng = float(results[0]["lon"])
            logger.debug(f"[Nominatim] Geocoded '{location_name}' → ({lat:.4f}, {lng:.4f})")
            if geocoding_requests is not None:
                geocoding_requests.labels(provider="nominatim", status="success").inc()
            if geocoding_latency is not None:
                geocoding_latency.labels(source="nominatim").observe(_time.monotonic() - t0)
            return lat, lng

        except Exception as e:
            logger.warning(f"Nominatim geocoding error for '{location_name}': {e}")
            if geocoding_requests is not None:
                status = "rate_limited" if "429" in str(e) else "error"
                geocoding_requests.labels(provider="nominatim", status=status).inc()
            return None
