"""
URL normalization for deterministic dedup_key generation.

Strips noise (tracking params, fragments, case) while preserving identity.
"""
import re
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

# Query parameters that carry no semantic content (tracking only)
_STRIP_PARAMS = frozenset([
    'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
    'utm_id', 'utm_reader', 'utm_name', 'utm_placement',
    'gclid', 'gclsrc', 'fbclid', 'msclkid', 'dclid',
    'mc_cid', 'mc_eid', '_ga', '_gl',
])

# Default ports per scheme — remove them if present
_DEFAULT_PORTS = {'http': 80, 'https': 443, 'ftp': 21}


def normalize_url(url: str) -> str:
    """
    Normalize a URL to a canonical form for stable md5 hashing.

    Transformations applied:
    - Lowercase scheme and host
    - Remove default ports (80 for http, 443 for https)
    - Remove fragment (#...)
    - Strip tracking query params (utm_*, gclid, fbclid, ...)
    - Sort remaining query params for stability
    - Preserve path as-is (case matters for most web servers)

    Returns the normalized URL string.
    """
    if not url:
        return url

    try:
        parsed = urlparse(url.strip())

        scheme = parsed.scheme.lower()
        netloc = parsed.hostname or ''  # lowercased by urlparse

        # Restore port only when non-default
        port = parsed.port
        if port and port != _DEFAULT_PORTS.get(scheme):
            netloc = f"{netloc}:{port}"

        # Filter query params: remove tracking, sort the rest
        params = [
            (k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)
            if k.lower() not in _STRIP_PARAMS
        ]
        params.sort()
        query = urlencode(params)

        # Fragment is always stripped
        normalized = urlunparse((scheme, netloc, parsed.path, parsed.params, query, ''))
        return normalized

    except Exception:
        # If parsing fails, return original url so dedup still works
        return url
