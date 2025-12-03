#!/usr/bin/env python3
"""
URL Resolver Component

Resolves redirect URLs to their final destinations and extracts all URLs
from web pages. Handles multiple redirect hops and provides detailed
tracking of the redirect chain.

Usage:
    from url_resolver import URLResolver

    resolver = URLResolver()
    result = resolver.resolve_url("https://example.com/redirect")
    print(result['final_url'])
    print(result['redirect_chain'])
"""

import asyncio
import os
import time
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

try:
    from curl_cffi.requests import Session as CurlCffiSession
    CURL_CFFI_AVAILABLE = True
except ImportError:
    import requests
    CurlCffiSession = requests.Session
    CURL_CFFI_AVAILABLE = False

from bs4 import BeautifulSoup

# Base configuration import
try:
    from ..config_handler import Base
except ImportError:
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class URLResolver(Base):
    """Resolves redirect URLs and extracts links from web pages"""

    def __init__(self):
        """Initialize the URL resolver"""
        super().__init__()
        self._logger.info("Initializing URLResolver")

        # Get configuration
        try:
            self.max_redirects = self._app_config.get('web_scraper.max_redirects')
            self.request_timeout = self._app_config.get('web_scraper.request_timeout')
            self.follow_redirects = self._app_config.get('web_scraper.follow_redirects')
            self.verify_ssl = self._app_config.get('web_scraper.verify_ssl')
            self.retry_attempts = self._app_config.get('web_scraper.retry_attempts')
            self.retry_delay = self._app_config.get('web_scraper.retry_delay')
        except:
            # Fallback to defaults
            self.max_redirects = 10
            self.request_timeout = 15
            self.follow_redirects = True
            self.verify_ssl = True
            self.retry_attempts = 3
            self.retry_delay = 2.0

        # Setup session
        self.session = self._setup_session()

        if not CURL_CFFI_AVAILABLE:
            self._logger.warning("⚠️ curl_cffi not available, using standard requests")

    def _setup_session(self):
        """Setup HTTP session with browser impersonation if available"""
        if CURL_CFFI_AVAILABLE:
            try:
                session = CurlCffiSession(
                    impersonate="chrome110",
                    timeout=self.request_timeout
                )
                session.headers.update({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Connection': 'keep-alive',
                    'DNT': '1',
                })
                self._logger.info("✅ Using curl_cffi for URL resolution")
                return session
            except Exception as e:
                self._logger.warning(f"Failed to initialize curl_cffi: {e}, falling back to requests")

        # Fallback to standard requests
        import requests
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
        return session

    async def resolve_url(self, url: str) -> Dict:
        """
        Resolve a URL through all redirects to its final destination.

        Args:
            url: The URL to resolve (may be a redirect)

        Returns:
            Dict containing:
                - original_url: The starting URL
                - final_url: The final destination after all redirects
                - redirect_chain: List of all URLs in the redirect chain
                - redirect_count: Number of redirects followed
                - status_code: Final HTTP status code
                - success: Whether resolution was successful
                - error: Error message if failed
        """
        self._logger.info(f"🔍 Resolving URL: {url[:100]}...")

        result = {
            'original_url': url,
            'final_url': None,
            'redirect_chain': [],
            'redirect_count': 0,
            'status_code': None,
            'success': False,
            'error': None,
            'resolution_time': 0
        }

        start_time = time.time()
        current_url = url
        visited_urls: Set[str] = set()

        try:
            for redirect_num in range(self.max_redirects + 1):
                # Prevent infinite redirect loops
                if current_url in visited_urls:
                    result['error'] = f"Redirect loop detected at {current_url}"
                    self._logger.warning(f"   ⚠️ {result['error']}")
                    break

                visited_urls.add(current_url)
                result['redirect_chain'].append(current_url)

                self._logger.debug(f"   📍 Hop {redirect_num + 1}: {current_url[:80]}...")

                # Make request without following redirects automatically
                response = self.session.get(
                    current_url,
                    allow_redirects=False,
                    timeout=self.request_timeout,
                    verify=self.verify_ssl
                )

                result['status_code'] = response.status_code

                # Check if this is a redirect
                if response.status_code in (301, 302, 303, 307, 308):
                    # Get redirect location
                    redirect_location = response.headers.get('Location')
                    if not redirect_location:
                        result['error'] = f"Redirect status {response.status_code} but no Location header"
                        self._logger.warning(f"   ⚠️ {result['error']}")
                        break

                    # Handle relative URLs
                    if not redirect_location.startswith('http'):
                        redirect_location = urljoin(current_url, redirect_location)

                    self._logger.debug(f"      ↪️  Redirect to: {redirect_location[:80]}...")
                    current_url = redirect_location
                    result['redirect_count'] += 1

                elif response.status_code == 200:
                    # Success - we've reached the final destination
                    result['final_url'] = current_url
                    result['success'] = True
                    self._logger.info(f"   ✅ Resolved to: {current_url[:80]}...")
                    if result['redirect_count'] > 0:
                        self._logger.info(f"   📊 Followed {result['redirect_count']} redirect(s)")
                    break

                else:
                    # Non-redirect, non-success status
                    result['error'] = f"HTTP {response.status_code}"
                    result['final_url'] = current_url  # Still record the final URL we reached
                    self._logger.warning(f"   ⚠️ HTTP {response.status_code} at {current_url[:80]}...")
                    break

            else:
                # Exceeded max redirects
                result['error'] = f"Exceeded maximum redirects ({self.max_redirects})"
                result['final_url'] = current_url
                self._logger.warning(f"   ⚠️ {result['error']}")

        except Exception as e:
            result['error'] = str(e)
            result['final_url'] = current_url if current_url != url else None
            self._logger.error(f"   ❌ Error resolving URL: {e}")

        finally:
            result['resolution_time'] = time.time() - start_time

        return result

    async def extract_links_from_url(self, url: str, max_links: int = 100) -> Dict:
        """
        Extract all links from a web page.

        Args:
            url: The URL to extract links from
            max_links: Maximum number of links to extract

        Returns:
            Dict containing:
                - url: The source URL
                - links: List of extracted links
                - link_count: Number of links found
                - success: Whether extraction was successful
                - error: Error message if failed
        """
        self._logger.info(f"🔗 Extracting links from: {url[:100]}...")

        result = {
            'url': url,
            'links': [],
            'link_count': 0,
            'success': False,
            'error': None
        }

        try:
            response = self.session.get(url, timeout=self.request_timeout)

            if response.status_code != 200:
                result['error'] = f"HTTP {response.status_code}"
                self._logger.warning(f"   ⚠️ {result['error']}")
                return result

            # Parse HTML
            soup = BeautifulSoup(response.content, 'html.parser')

            # Extract all links
            links_found: Set[str] = set()
            for link_tag in soup.find_all('a', href=True):
                href = link_tag.get('href')

                # Convert relative URLs to absolute
                if href and not href.startswith('#'):
                    if not href.startswith('http'):
                        href = urljoin(url, href)

                    # Only include http/https links
                    if href.startswith('http'):
                        links_found.add(href)

                # Stop if we've reached max_links
                if len(links_found) >= max_links:
                    break

            result['links'] = list(links_found)
            result['link_count'] = len(result['links'])
            result['success'] = True

            self._logger.info(f"   ✅ Extracted {result['link_count']} links")

        except Exception as e:
            result['error'] = str(e)
            self._logger.error(f"   ❌ Error extracting links: {e}")

        return result

    async def resolve_and_extract(self, url: str, extract_links: bool = False,
                                  max_links: int = 100) -> Dict:
        """
        Resolve a URL and optionally extract links from the final destination.

        Args:
            url: The URL to resolve
            extract_links: Whether to extract links from the resolved page
            max_links: Maximum number of links to extract

        Returns:
            Dict containing resolution results and optionally extracted links
        """
        # First resolve the URL
        resolution_result = await self.resolve_url(url)

        # If successful and extract_links is True, extract links
        if extract_links and resolution_result['success'] and resolution_result['final_url']:
            link_result = await self.extract_links_from_url(
                resolution_result['final_url'],
                max_links=max_links
            )
            resolution_result['extracted_links'] = link_result['links']
            resolution_result['extracted_link_count'] = link_result['link_count']

        return resolution_result

    async def resolve_batch(self, urls: List[str], extract_links: bool = False) -> List[Dict]:
        """
        Resolve multiple URLs concurrently.

        Args:
            urls: List of URLs to resolve
            extract_links: Whether to extract links from resolved pages

        Returns:
            List of resolution results
        """
        self._logger.info(f"🔄 Resolving batch of {len(urls)} URLs...")

        tasks = [
            self.resolve_and_extract(url, extract_links=extract_links)
            for url in urls
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'original_url': urls[i],
                    'final_url': None,
                    'redirect_chain': [],
                    'redirect_count': 0,
                    'status_code': None,
                    'success': False,
                    'error': str(result)
                })
            else:
                processed_results.append(result)

        successful = sum(1 for r in processed_results if r['success'])
        self._logger.info(f"   ✅ Resolved {successful}/{len(urls)} URLs successfully")

        return processed_results


if __name__ == "__main__":
    """Test the URL resolver"""
    import asyncio

    async def test():
        resolver = URLResolver()

        # Test single URL resolution
        test_url = "https://www.google.com"
        result = await resolver.resolve_url(test_url)
        print(f"\nResolution result:")
        print(f"  Original: {result['original_url']}")
        print(f"  Final: {result['final_url']}")
        print(f"  Redirects: {result['redirect_count']}")
        print(f"  Success: {result['success']}")

        # Test with link extraction
        if result['success']:
            extract_result = await resolver.extract_links_from_url(
                result['final_url'],
                max_links=10
            )
            print(f"\nExtracted {extract_result['link_count']} links:")
            for link in extract_result['links'][:5]:
                print(f"  - {link}")

    asyncio.run(test())
