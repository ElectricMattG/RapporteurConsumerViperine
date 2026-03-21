"""
Weather Client — historical weather data from Open-Meteo.

Open-Meteo is free, requires no API key, and covers historical data
back to 1940.  This makes it ideal for enriching images with climatic
context given a GPS coordinate and a publication date.

Usage::

    from src.clients.weather_client import WeatherClient

    weather = WeatherClient()
    info = await weather.get_weather(lat=36.20, lon=37.13, date="2024-06-15")
    # {'temperature_c': 34.2, 'precipitation_mm': 0.0, ...}
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)

_OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


class WeatherClient:
    """Async Open-Meteo historical weather API wrapper.

    Parameters
    ----------
    timeout : float
        HTTP request timeout in seconds.
    """

    def __init__(self, timeout: float = 10.0):
        self._timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._client_loop: Optional[asyncio.AbstractEventLoop] = None

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
    # Public API
    # ------------------------------------------------------------------

    async def get_weather(
        self,
        latitude: float,
        longitude: float,
        date: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch daily weather for *latitude*/*longitude* on *date*.

        Parameters
        ----------
        latitude, longitude : float
            GPS coordinates (WGS-84).
        date : str
            ISO date ``YYYY-MM-DD``.

        Returns
        -------
        dict or None
            Keys: ``temperature_c``, ``precipitation_mm``,
            ``wind_speed_kmh``, ``humidity_percent``, ``pressure_hpa``,
            ``cloud_cover_percent``, ``shortwave_radiation_wm2``,
            ``sunrise``, ``sunset``, ``weather_description``.
            ``None`` if the request fails or data is unavailable.
        """
        if not date or len(date) < 10:
            return None

        # Extract YYYY-MM-DD from ISO timestamp if needed
        date_only = date[:10]

        if latitude is None or longitude is None:
            logger.debug("Weather skipped — missing latitude/longitude")
            return None

        try:
            client = await self._ensure_client()
            resp = await client.get(
                _OPEN_METEO_ARCHIVE_URL,
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "start_date": date_only,
                    "end_date": date_only,
                    "daily": ",".join([
                        "temperature_2m_mean",
                        "precipitation_sum",
                        "windspeed_10m_max",
                        "winddirection_10m_dominant",
                        "relative_humidity_2m_mean",
                        "surface_pressure_mean",
                        "cloudcover_mean",
                        "shortwave_radiation_sum",
                        "sunrise",
                        "sunset",
                    ]),
                    "timezone": "auto",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            # Fetch altitude (elevation) from Open-Meteo elevation API
            altitude = None
            try:
                elev_resp = await client.get(
                    "https://api.open-meteo.com/v1/elevation",
                    params={"latitude": latitude, "longitude": longitude},
                )
                elev_resp.raise_for_status()
                elev_data = elev_resp.json()
                elev_values = elev_data.get("elevation", [])
                if isinstance(elev_values, list) and elev_values:
                    altitude = float(elev_values[0])
                elif isinstance(elev_values, (int, float)):
                    altitude = float(elev_values)
            except Exception as e:
                logger.debug(f"Altitude lookup failed ({latitude:.2f}, {longitude:.2f}): {e}")
            daily = data.get("daily", {})
            if not daily or not daily.get("time"):
                return None

            temp = _safe_first(daily.get("temperature_2m_mean"))
            precip = _safe_first(daily.get("precipitation_sum"))
            wind = _safe_first(daily.get("windspeed_10m_max"))
            wind_dir = _safe_first(daily.get("winddirection_10m_dominant"))
            humidity = _safe_first(daily.get("relative_humidity_2m_mean"))
            pressure = _safe_first(daily.get("surface_pressure_mean"))
            cloud = _safe_first(daily.get("cloudcover_mean"))
            radiation = _safe_first(daily.get("shortwave_radiation_sum"))
            sunrise = _safe_first_str(daily.get("sunrise"))
            sunset = _safe_first_str(daily.get("sunset"))

            description = _describe_weather(temp, precip, wind)

            result = {
                "temperature_c": temp,
                "precipitation_mm": precip,
                "wind_speed_kmh": wind,
                "wind_direction_deg": wind_dir,
                "humidity_percent": humidity,
                "pressure_hpa": pressure,
                "cloud_cover_percent": cloud,
                "shortwave_radiation_wm2": radiation,
                "altitude_m": altitude,
                "sunrise": sunrise,
                "sunset": sunset,
                "weather_description": description,
            }
            logger.debug(
                f"Weather ({latitude:.2f}, {longitude:.2f}) on {date_only}: "
                f"{temp}°C, {precip}mm rain, {wind} km/h wind, "
                f"{humidity}% humidity, {pressure} hPa, {cloud}% cloud"
            )
            return result

        except Exception as e:
            logger.warning(
                f"Weather lookup failed ({latitude:.2f}, {longitude:.2f}, {date_only}): {e}"
            )
            return None


# ── Helpers ────────────────────────────────────────────────────────────

def _safe_first(values: Any) -> Optional[float]:
    """Extract the first numeric value from a list, or ``None``."""
    if isinstance(values, list) and values:
        v = values[0]
        return float(v) if v is not None else None
    return None


def _safe_first_str(values: Any) -> Optional[str]:
    """Extract the first string value from a list, or ``None``."""
    if isinstance(values, list) and values:
        v = values[0]
        return str(v) if v is not None else None
    return None


def _describe_weather(
    temp: Optional[float],
    precip: Optional[float],
    wind: Optional[float],
) -> str:
    """Build a short human-readable weather description."""
    parts: list[str] = []
    if temp is not None:
        parts.append(f"{temp:.1f}°C")
    if precip is not None:
        if precip > 0:
            parts.append(f"{precip:.1f} mm precipitation")
        else:
            parts.append("dry")
    if wind is not None:
        if wind > 50:
            parts.append(f"strong wind ({wind:.0f} km/h)")
        elif wind > 20:
            parts.append(f"moderate wind ({wind:.0f} km/h)")
    return ", ".join(parts) if parts else "no data"
