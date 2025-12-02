#!/usr/bin/env python3
"""
Document Web Searcher Component

Formalized web search module with proper configuration management and logging.
Inherits from Base class for centralized config and logging.

This module provides a clean interface for web searching:
1. First tries Tavily API (if credentials available)
2. Falls back to Google Custom Search API (if credentials available)
3. Final fallback to DuckDuckGo with advanced browser impersonation

Usage:
    from document_websearcher import DocumentWebSearcher

    searcher = DocumentWebSearcher()
    results = searcher.search("python tutorial", num_results=5)

=======================================================================
===                 DEPENDENCY & FIXES APPLIED                    ===
=======================================================================
This module has been updated to use `curl_cffi` instead of `requests`
for all web scraping tasks (DuckDuckGo searching and content fetching).
This is CRITICAL for bypassing TLS/JA3 fingerprinting, which causes
many 403/401 errors.

Changelog:
1.  Replaced `requests.Session` with `curl_cffi.requests.Session`.
2.  `_setup_browser_impersonation` now uses `curl_cffi`'s `impersonate`
    feature, which automatically handles all complex headers (User-Agent,
    Sec-Ch-Ua, etc.) and the TLS signature. This fixes the
    header-inconsistency bug.
3.  Replaced the unofficial `googlesearch` scraping library with the
    official `google-api-python-client` for `search_google`. This
    is far more reliable and avoids IP bans.
4.  Fixed `_decompress_response` to reliably handle byte content.
5.  Fixed minor bugs (temp file paths, logging key errors).

NOTE: While `curl_cffi` defeats passive anti-bot checks, it *cannot*
defeat active JavaScript challenges. Sites like LinkedIn (HTTP 999)
or those with heavy Cloudflare protection will still require a full
browser automation tool like Playwright.
=======================================================================
"""

import gzip
import json
import os
import random
import re
import string
import tempfile
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import brotli
import click
import requests  # Standard requests for fallback
from bs4 import BeautifulSoup

# --- Main Scraping Client ---
try:
    from curl_cffi.requests import Session as CurlCffiSession
    CURL_CFFI_AVAILABLE = True
except ImportError:
    CurlCffiSession = requests.Session # Fallback, scraping will fail
    CURL_CFFI_AVAILABLE = False

# --- HTML to Markdown Converter ---
try:
    from docling.document_converter import DocumentConverter
    DOCLING_AVAILABLE = True
except ImportError:
    DOCLING_AVAILABLE = False
    DocumentConverter = None

# --- Tavily API Client ---
try:
    from tavily import TavilyClient
    TAVILY_AVAILABLE = True
except ImportError:
    TAVILY_AVAILABLE = False
    TavilyClient = None

# --- Google API Client ---
try:
    from googleapiclient.discovery import build
    GOOGLE_API_CLIENT_AVAILABLE = True
except ImportError:
    GOOGLE_API_CLIENT_AVAILABLE = False
    build = None

# --- Base Configuration ---
try:
    # Try relative import first (when used as a package)
    from ...config_handler import Base
except ImportError:
    # Fall back to absolute import (when run directly)
    import os
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class DocumentWebSearcher(Base):
    """Document web searcher with Tavily, Google API and DuckDuckGo fallback"""

    def __init__(self):
        """Initialize the searcher with session and credentials check"""
        super().__init__()
        self._logger.info("Initializing DocumentWebSearcher")

        # Use the robust curl_cffi session for all scraping
        self.session = self._setup_browser_impersonation()

        self.tavily_available = self._check_tavily_credentials()
        self.google_available = self._check_google_credentials()
        self.google_blocked = False  # Track if Google API is blocked

        self._logger.info(f"Search methods available - Tavily: {self.tavily_available}, Google: {self.google_available}, DuckDuckGo: True")
        if not CURL_CFFI_AVAILABLE:
            self._logger.critical("⚠️ `curl_cffi` not found! Scraping will almost certainly fail with 403 errors.")
            self._logger.critical("   Please run: pip install curl-cffi")

    def _check_tavily_credentials(self) -> bool:
        """Check if Tavily API credentials are available"""
        try:
            api_key = self._app_config.get('tavily.api_key')
        except:
            api_key = os.getenv('TAVILY_API_KEY')

        if api_key and TAVILY_AVAILABLE:
            try:
                self.tavily_client = TavilyClient(api_key=api_key)
                self._logger.info("✅ Tavily API credentials found and initialized")
                return True
            except Exception as e:
                self._logger.warning(f"⚠️ Tavily initialization failed: {e}")
                return False

        self._logger.debug("⚠️ Tavily API credentials not found or `tavily-python` not installed")
        return False

    def _check_google_credentials(self) -> bool:
        """Check if Google Custom Search API credentials are available"""
        # Try config first, fallback to environment variables
        try:
            api_key = self._app_config.get('google.api_key')
            search_engine_id = self._app_config.get('google.search_engine_id')
        except:
            api_key = None
            search_engine_id = None

        # Fallback to environment variables if config has None
        if not api_key:
            api_key = os.getenv('GCS_DEVELOPER_KEY')
        if not search_engine_id:
            search_engine_id = os.getenv('GCS_CX')

        if api_key and search_engine_id and GOOGLE_API_CLIENT_AVAILABLE:
            self._logger.info("✅ Google Custom Search API credentials found")
            return True
        else:
            if not GOOGLE_API_CLIENT_AVAILABLE:
                self._logger.debug("⚠️ `google-api-python-client` not installed. Google API search disabled.")
            self._logger.debug("⚠️ Google Custom Search API credentials not found")
            self._logger.debug("   Set GCS_DEVELOPER_KEY and GCS_CX environment variables for Google search")
            return False

    def _setup_browser_impersonation(self) -> Any:
        """Setup curl_cffi to impersonate a real browser's network stack"""

        if not CURL_CFFI_AVAILABLE:
            return requests.Session() # Return a basic session if library is missing

        # List of browser versions curl_cffi supports
        # Full list: from curl_cffi import impersonate; print(impersonate.DEFAULT_BROWSERS)
        # Using more widely supported browser versions
        browsers_to_impersonate = [
            "chrome110",
            "chrome116",
            "chrome99",
            "edge99",
            "safari15_5"
        ]
        chosen_browser = random.choice(browsers_to_impersonate)

        try:
            # This sets ALL headers (User-Agent, Sec-Ch-Ua, etc.)
            # and the TLS/JA3 fingerprint automatically
            session = CurlCffiSession(impersonate=chosen_browser, timeout=15)

            # We only need to add a few simple, non-critical headers
            session.headers.update({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'DNT': '1',
            })

            self._logger.info(f"Browser impersonation configured using curl_cffi (as {chosen_browser})")
            return session

        except Exception as e:
            self._logger.error(f"Failed to initialize curl_cffi session: {e}")
            self._logger.warning("Falling back to standard requests session (SCRAPING WILL LIKELY FAIL)")
            return requests.Session()

    async def search_google(self, query: str, num_results: int = 50) -> List[str]:
        """Search using the *official* Google Custom Search API (async)"""
        self._logger.info(f"🔍 Searching Google API: '{query}'")

        if not GOOGLE_API_CLIENT_AVAILABLE:
            self._logger.debug("   ❌ google-api-python-client not installed. Skipping Google search.")
            return []

        try:
            # Get credentials from config or environment
            try:
                api_key = self._app_config.get('google.api_key')
                search_engine_id = self._app_config.get('google.search_engine_id')
            except:
                api_key = None
                search_engine_id = None

            # Fallback to environment variables if config has None
            if not api_key:
                api_key = os.getenv('GCS_DEVELOPER_KEY')
            if not search_engine_id:
                search_engine_id = os.getenv('GCS_CX')

            if not api_key or not search_engine_id:
                self._logger.warning("   ⚠️ Missing Google API Key or Search Engine ID. Skipping.")
                return []

            # Google's API only allows 10 results at a time.
            # We must make multiple requests to get more.
            service = build("customsearch", "v1", developerKey=api_key)
            all_urls = []

            # API allows max 10 results per page, max 100 results total
            num_to_fetch = min(num_results, 100)

            for start_index in range(1, num_to_fetch + 1, 10):
                remaining = num_to_fetch - start_index + 1
                num_for_this_request = min(10, remaining)

                if num_for_this_request <= 0:
                    break

                self._logger.debug(f"   📞 API call (start_index: {start_index}, num: {num_for_this_request})")

                response = service.cse().list(
                    q=query,
                    cx=search_engine_id,
                    num=num_for_this_request,
                    start=start_index
                ).execute()

                if 'items' in response:
                    call_urls = [item['link'] for item in response['items'] if 'link' in item]
                    all_urls.extend(call_urls)
                else:
                    self._logger.warning("   ⚠️ No 'items' in Google API response. Reached end?")
                    break # No more results

            self._logger.info(f"   ✅ Google API found {len(all_urls)} URLs")
            return list(dict.fromkeys(all_urls))[:num_results] # Remove duplicates and trim

        except Exception as e:
            error_str = str(e)
            # Check if API is blocked (403 forbidden)
            if '403' in error_str and 'blocked' in error_str.lower():
                self._logger.warning(f"   ❌ Google Custom Search API is blocked for this project")
                self._logger.warning(f"   💡 Disabling Google API for this session (will use DuckDuckGo)")
                self.google_blocked = True  # Disable Google for future searches
            else:
                self._logger.error(f"   ❌ Google API search failed: {e}")
            return []

    async def search_tavily(self, query: str, num_results: int = 50) -> List[str]:
        """Search using Tavily API - designed for AI agents (async)"""
        self._logger.info(f"🦉 Searching Tavily API: '{query}'")

        try:
            # Tavily supports up to 20 results per request, so we may need multiple calls
            all_urls = []
            calls_needed = (num_results + 19) // 20  # Round up division
            calls_needed = min(calls_needed, 5)  # Limit to 5 API calls max (100 results)

            for call_num in range(calls_needed):
                remaining_results = min(20, num_results - len(all_urls))
                if remaining_results <= 0:
                    break

                self._logger.debug(f"   📞 API call {call_num + 1}/{calls_needed} (requesting {remaining_results} results)")

                # Use Tavily search with various parameters for comprehensive results
                response = self.tavily_client.search(
                    query=query,
                    search_depth="advanced",  # More comprehensive search
                    max_results=remaining_results,
                    include_answer=False,  # We just want URLs
                    include_raw_content=False  # We'll fetch content separately
                )

                # Extract URLs from Tavily response
                if 'results' in response:
                    call_urls = [result['url'] for result in response['results'] if 'url' in result]
                    # Filter out duplicates
                    new_urls = [url for url in call_urls if url not in all_urls]
                    all_urls.extend(new_urls)
                    self._logger.debug(f"      ✅ Found {len(new_urls)} new URLs (total: {len(all_urls)})")
                else:
                    self._logger.warning(f"      ⚠️  No results in API call {call_num + 1}")
                    break

                # Small delay between API calls to be respectful
                if call_num < calls_needed - 1:
                    time.sleep(0.5)

            self._logger.info(f"   ✅ Tavily API found {len(all_urls)} URLs")
            return all_urls[:num_results]  # Ensure we don't exceed requested amount

        except Exception as e:
            self._logger.error(f"   ❌ Tavily API search failed: {e}")
            return []

    async def search_duckduckgo(self, query: str, num_results: int = 50) -> List[str]:
        """Search DuckDuckGo with advanced browser impersonation and pagination (async)"""
        self._logger.info(f"🦆 Searching DuckDuckGo with impersonation: '{query}'")
        self._logger.info(f"   🎯 Target: {num_results} results (will use pagination if needed)")

        try:
            # Step 1: Establish session with DuckDuckGo homepage
            # This helps set initial cookies
            self._logger.debug("   🌐 Establishing session...")
            homepage_response = self.session.get('https://duckduckgo.com/')

            if homepage_response.status_code != 200:
                self._logger.error(f"   ❌ Failed to load homepage: {homepage_response.status_code}")
                # Don't fail, just log. The html subdomain might still work.

            # Step 2: Wait like a human
            time.sleep(random.uniform(1, 3))

            all_urls = []
            page = 0
            # DDG html results are ~10 per page
            max_pages = min(10, (num_results // 10) + 2) # Add a buffer

            self._logger.debug(f"   📄 Will search up to {max_pages} pages to reach {num_results} results")

            while len(all_urls) < num_results and page < max_pages:
                # Step 3: Search using HTML-only subdomain with pagination
                search_params = {'q': query}
                if page > 0:
                    search_params['s'] = str(page * 10)  # DuckDuckGo uses 's' parameter for offset

                search_url = f"https://html.duckduckgo.com/html/?" + urllib.parse.urlencode(search_params)

                # Note: self.session (CurlCffiSession) handles the complex headers
                # We just add context-specific ones
                search_headers = {
                    'Referer': 'https://duckduckgo.com/' if page == 0 else f'https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}',
                    'Host': 'html.duckduckgo.com',
                }

                self._logger.debug(f"   📖 Fetching page {page + 1} (offset {page * 10})...")

                response = self.session.get(
                    search_url,
                    headers=search_headers,
                    allow_redirects=False
                )

                self._logger.debug(f"      📡 Status: {response.status_code}")

                if response.status_code == 200:
                    # Handle compressed response (Brotli/gzip)
                    content = self._decompress_response(response)

                    if not content:
                        self._logger.error(f"      ❌ Failed to decompress response on page {page + 1}")
                        break

                    # Parse search results for this page
                    page_urls = self._parse_duckduckgo_results(content, num_results - len(all_urls))

                    if not page_urls:
                        self._logger.warning(f"      ⚠️  No more results found on page {page + 1}")
                        break

                    # Add new URLs (avoid duplicates)
                    new_urls = [url for url in page_urls if url not in all_urls]
                    all_urls.extend(new_urls)

                    self._logger.debug(f"      ✅ Found {len(new_urls)} new URLs (total: {len(all_urls)})")

                    # Wait between pages to be respectful
                    if page < max_pages - 1 and len(all_urls) < num_results:
                        time.sleep(random.uniform(2, 4))

                else:
                    self._logger.error(f"      ❌ Page {page + 1} returned status {response.status_code}")
                    break

                page += 1

            # Limit to requested number of results
            final_urls = all_urls[:num_results]

            self._logger.info(f"   ✅ DuckDuckGo found {len(final_urls)} URLs across {page} pages")
            return final_urls

        except Exception as e:
            self._logger.error(f"   ❌ DuckDuckGo search failed: {e}")
            return []

    def _decompress_response(self, response) -> Optional[str]:
        """Handle compressed responses (Brotli/gzip) using raw bytes"""
        content_encoding = response.headers.get('content-encoding', '').lower()
        content = response.content  # ALWAYS use .content (raw bytes)

        try:
            if content_encoding == 'br':
                return brotli.decompress(content).decode('utf-8')
            elif content_encoding == 'gzip':
                return gzip.decompress(content).decode('utf-8')
            else:
                # Fallback: try to decode as-is
                return content.decode('utf-8', errors='ignore')
        except Exception as e:
            self._logger.warning(f"   ⚠️  Decompression failed: {e}")
            # Try to decode with error handling as a last resort
            try:
                return response.text
            except:
                return None

    def _parse_duckduckgo_results(self, content: str, num_results: int) -> List[str]:
        """Parse DuckDuckGo HTML results to extract URLs with enhanced detection"""
        soup = BeautifulSoup(content, 'html.parser')
        urls = []

        self._logger.debug(f"      🔍 Parsing HTML content ({len(content)} chars)...")

        # Method 1: Look for result title links (primary method)
        result_links = soup.find_all('a', {'data-testid': 'result-title-a'})
        if result_links:
            self._logger.debug(f"      📊 Found {len(result_links)} data-testid result links")

        if not result_links:
            result_links = soup.find_all('a', class_=re.compile(r'result.*title'))
            if result_links:
                self._logger.debug(f"      📊 Found {len(result_links)} class-based result links")

        # Method 2: Look for h2/h3 elements with result class, then get their links
        if not result_links:
            result_headers = soup.find_all(['h2', 'h3'], class_=re.compile(r'result'))
            for header in result_headers:
                links = header.find_all('a')
                result_links.extend(links)
            if result_links:
                self._logger.debug(f"      📊 Found {len(result_links)} header-based result links")

        # Extract URLs from found result links
        for link in result_links:
            href = link.get('href', '')
            if href and href.startswith('http') and 'duckduckgo.com' not in href:
                if href not in urls:  # Avoid duplicates
                    urls.append(href)
                    if len(urls) >= num_results:
                        break

        # Method 3: Look for uddg redirect links if no direct links found
        if not urls:
            self._logger.debug("      🔄 Trying uddg redirect pattern...")
            redirect_count = 0
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if '/l/?uddg=' in href or '/l/?kh=' in href:
                    redirect_count += 1
                    try:
                        if 'uddg=' in href:
                            actual_url = urllib.parse.unquote(href.split('uddg=')[1].split('&')[0])
                        else:
                            actual_url = urllib.parse.unquote(href.split('kh=')[1].split('&')[0])

                        if actual_url.startswith('http') and 'duckduckgo.com' not in actual_url:
                            if actual_url not in urls:
                                urls.append(actual_url)
                                if len(urls) >= num_results:
                                    break
                    except:
                        continue
            self._logger.debug(f"      📊 Found {redirect_count} redirect links, extracted {len(urls)} URLs")

        # Method 4: Look for any external links if still no results (broader search)
        if not urls:
            self._logger.debug("      🌐 Trying broad external link search...")
            external_count = 0
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if (href.startswith('http') and
                    'duckduckgo.com' not in href and
                    'javascript:' not in href and
                    len(href) > 10):
                    external_count += 1
                    # Skip common non-result links
                    skip_domains = ['facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com/user', 'instagram.com']
                    if not any(domain in href for domain in skip_domains):
                        if href not in urls:
                            urls.append(href)
                            if len(urls) >= num_results:
                                break
            self._logger.debug(f"      📊 Found {external_count} external links, extracted {len(urls)} URLs")

        # Debug: Show some parsing stats
        all_links = soup.find_all('a', href=True)
        result_divs = soup.find_all('div', class_=re.compile(r'result'))
        self._logger.debug(f"      📈 Page stats: {len(all_links)} total links, {len(result_divs)} result divs")

        # If still no results, save debug info
        if not urls:
            # FIX: Use tempfile.gettempdir() for cross-platform compatibility
            debug_file = Path(tempfile.gettempdir()) / f"ddg_debug_{int(time.time())}.html"
            try:
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                self._logger.error(f"      🚨 No results found! Debug HTML saved to: {debug_file}")
            except Exception as e:
                self._logger.error(f"      🚨 No results found! Failed to save debug HTML: {e}")

            # Show first few link examples for debugging
            self._logger.debug("      🔍 Sample links found:")
            for i, link in enumerate(all_links[:5]):
                href = link.get('href', 'NO_HREF')
                text = link.get_text().strip()[:50]
                self._logger.debug(f"         {i+1}. {href[:80]} | {text}")

        return urls

    async def search(self, query: str, num_results: int = 5, include_content: bool = True) -> Dict:
        """
        Main search method - tries Tavily API first, then Google API, falls back to DuckDuckGo (async)

        Args:
            query: Search query string
            num_results: Maximum number of results to return
            include_content: Whether to fetch page titles and content previews

        Returns:
            Dict with search results and metadata
        """
        self._logger.info(f"🔍 SMART WEB SEARCH: '{query}'")
        self._logger.info("=" * 60)

        results = {
            'query': query,
            'timestamp': datetime.now().isoformat(),
            'method_used': None,
            'urls': [],
            'results': [],  # Will contain {url, title, preview} if include_content=True
            'success': False
        }

        urls = []

        # Try Tavily API first if credentials available
        if self.tavily_available:
            urls = await self.search_tavily(query, num_results)
            if urls:
                results['method_used'] = 'tavily_api'

        # Try Google API if Tavily failed and credentials available and not blocked
        if not urls and self.google_available and not self.google_blocked:
            self._logger.info("\n🔍 Trying Google API...")
            urls = await self.search_google(query, num_results)
            if urls:
                results['method_used'] = 'google_api'

        # Fallback to DuckDuckGo with impersonation
        if not urls:
            self._logger.info("\n🦆 Falling back to DuckDuckGo...")
            urls = await self.search_duckduckgo(query, num_results)
            if urls:
                results['method_used'] = 'duckduckgo_impersonation'

        # Process results if any URLs were found
        if urls:
            results['urls'] = urls
            results['success'] = True

            if include_content:
                results['results'] = self._fetch_page_details(urls)

            self._logger.info(f"\n✅ SUCCESS via {results['method_used']}: {len(urls)} URLs found")

        else:
            results['success'] = False
            self._logger.error(f"\n❌ FAILURE: No results found from any search method")

        return results

    def _fetch_page_details(self, urls: List[str]) -> List[Dict]:
        """Fetch page titles, content previews, and metadata for URLs"""
        self._logger.info("\n📄 Fetching page details and metadata...")
        results = []

        for i, url in enumerate(urls, 1):
            self._logger.debug(f"   📖 Fetching {i}/{len(urls)}: {url[:60]}...")

            try:
                # Use the curl_cffi session with a shorter timeout for content
                # Note: session.timeout is the default, get(timeout=) overrides
                response = self.session.get(url, timeout=10)

                if response.status_code == 200:
                    # Use .content and decode manually for reliability
                    html_content = response.content.decode('utf-8', errors='ignore')
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Extract comprehensive metadata
                    metadata = self._extract_page_metadata(soup)

                    # Extract content preview
                    content_preview = self._extract_content_preview(soup)

                    # Parse domain from URL
                    domain = urllib.parse.urlparse(url).netloc

                    result = {
                        'url': url,
                        'title': metadata['title'],
                        'preview': content_preview,
                        'status': 'success',
                        'domain': domain,
                        'metadata': {
                            'published_date': metadata['published_date'],
                            'modified_date': metadata['modified_date'],
                            'author': metadata['author'],
                            'description': metadata['description'],
                            'keywords': metadata['keywords'],
                            'language': metadata['language'],
                            'article_type': metadata['article_type'],
                            'site_name': metadata['site_name']
                        }
                    }

                    results.append(result)
                    self._logger.debug(f"      ✅ {metadata['title'][:50]}...")
                    if metadata['published_date']:
                        self._logger.debug(f"         📅 Published: {metadata['published_date']}")

                else:
                    results.append({
                        'url': url,
                        'title': f'HTTP {response.status_code}',
                        'preview': 'Content unavailable',
                        'status': 'error',
                        'domain': urllib.parse.urlparse(url).netloc,
                        'metadata': {}
                    })
                    self._logger.warning(f"      ❌ HTTP {response.status_code} for {url[:60]}")

            except Exception as e:
                results.append({
                    'url': url,
                    'title': 'Error fetching',
                    'preview': f'Error: {str(e)[:50]}...',
                    'status': 'error',
                    'domain': urllib.parse.urlparse(url).netloc,
                    'metadata': {}
                })
                self._logger.warning(f"      ❌ Error fetching {url[:60]}: {str(e)[:30]}...")

            # Small delay between requests to be respectful
            time.sleep(random.uniform(0.5, 1.5))

        return results

    def _extract_page_metadata(self, soup: BeautifulSoup) -> Dict:
        """
        Extract comprehensive page metadata from HTML.

        Extraction priority:
        1. JSON-LD structured data (highest priority - most accurate)
        2. Meta tags (name, property attributes)
        3. HTML elements (time tags, class selectors)
        4. Fallback text extraction
        """
        metadata = {
            'title': '',
            'published_date': None,
            'modified_date': None,
            'author': None,
            'description': None,
            'keywords': None,
            'language': None,
            'article_type': None,
            'site_name': None,
            'publisher': None,  # New field
            'section': None,    # New field (article section/category)
        }

        # ========================================
        # STEP 1: Extract from JSON-LD (highest priority)
        # ========================================
        json_ld_data = self._extract_json_ld_metadata(soup)
        if json_ld_data:
            # Merge JSON-LD data into metadata (only if not already set)
            for key in ['title', 'published_date', 'modified_date', 'author',
                       'description', 'keywords', 'publisher', 'article_type']:
                if json_ld_data.get(key) and not metadata.get(key):
                    metadata[key] = json_ld_data[key]

        # ========================================
        # STEP 2: Extract title from <title> tag if not from JSON-LD
        # ========================================
        if not metadata['title']:
            title_tag = soup.find('title')
            metadata['title'] = title_tag.text.strip() if title_tag else 'No title found'

        # ========================================
        # STEP 3: Extract from meta tags
        # ========================================
        meta_tags = soup.find_all('meta')

        for tag in meta_tags:
            name = tag.get('name', '').lower()
            property_attr = tag.get('property', '').lower()
            content = tag.get('content', '')

            if not content:
                continue

            # Publication date patterns (expanded)
            if (name in ['article:published_time', 'publishdate', 'date', 'pubdate',
                        'datepublished', 'last-modified', 'publication_date',
                        'citation_publication_date', 'parsely-pub-date', 'sailthru.date'] or
                property_attr in ['article:published_time', 'article:published', 'dc:date',
                                'article:published_time']):
                if not metadata['published_date']:
                    metadata['published_date'] = self._parse_date(content)

            # Modified date patterns (expanded)
            elif (name in ['article:modified_time', 'lastmod', 'datemodified',
                          'last-modified', 'modified_date', 'updated_time'] or
                  property_attr in ['article:modified_time', 'article:modified',
                                   'article:modified_time']):
                if not metadata['modified_date']:
                    metadata['modified_date'] = self._parse_date(content)

            # Author patterns (expanded)
            elif (name in ['author', 'article:author', 'creator', 'dc:creator',
                          'citation_author', 'parsely-author', 'sailthru.author',
                          'byl', 'twitter:creator'] or
                  property_attr in ['article:author', 'author', 'og:article:author']):
                if not metadata['author']:
                    metadata['author'] = content

            # Publisher patterns (new)
            elif (name in ['publisher', 'citation_publisher', 'dc.publisher'] or
                  property_attr in ['article:publisher', 'og:site_name']):
                if not metadata['publisher']:
                    metadata['publisher'] = content

            # Description patterns
            elif (name in ['description', 'og:description', 'twitter:description',
                          'dc.description', 'citation_abstract'] or
                  property_attr in ['og:description', 'description']):
                if not metadata['description']:
                    metadata['description'] = content

            # Keywords (expanded)
            elif name in ['keywords', 'news_keywords', 'citation_keywords',
                         'article:tag', 'parsely-tags']:
                if not metadata['keywords']:
                    metadata['keywords'] = [k.strip() for k in content.split(',')]

            # Language
            elif name in ['language', 'lang', 'dc.language'] or property_attr in ['og:locale']:
                if not metadata['language']:
                    # Clean language code (e.g., 'en_US' -> 'en')
                    metadata['language'] = content.split('_')[0].split('-')[0]

            # Article type
            elif property_attr in ['og:type']:
                if not metadata['article_type']:
                    metadata['article_type'] = content

            # Site name
            elif property_attr in ['og:site_name']:
                if not metadata['site_name']:
                    metadata['site_name'] = content

            # Section/Category (new)
            elif (name in ['article:section', 'section', 'category'] or
                  property_attr in ['article:section']):
                if not metadata['section']:
                    metadata['section'] = content

        # ========================================
        # STEP 4: Try alternative HTML element methods for missing data
        # ========================================

        # Look for dates in <time> tags
        if not metadata['published_date'] or not metadata['modified_date']:
            time_tags = soup.find_all('time')
            for time_tag in time_tags:
                datetime_attr = time_tag.get('datetime')
                pubdate_attr = time_tag.get('pubdate')

                if datetime_attr:
                    parsed = self._parse_date(datetime_attr)
                    if pubdate_attr and not metadata['published_date']:
                        metadata['published_date'] = parsed
                    elif not metadata['published_date']:
                        metadata['published_date'] = parsed
                    elif not metadata['modified_date']:
                        metadata['modified_date'] = parsed

        # Look for author in common HTML patterns
        if not metadata['author']:
            author_selectors = [
                '.author', '.byline', '[rel="author"]', '.post-author',
                '.article-author', '.writer', '.journalist', '.author-name',
                '.by-author', '[itemprop="author"]', '.contributor'
            ]
            for selector in author_selectors:
                author_elem = soup.select_one(selector)
                if author_elem:
                    author_text = author_elem.get_text().strip()
                    # Clean common prefixes
                    for prefix in ['By ', 'by ', 'BY ', 'Written by ', 'Author: ']:
                        if author_text.startswith(prefix):
                            author_text = author_text[len(prefix):].strip()
                    metadata['author'] = author_text
                    break

        # Look for language in html tag
        if not metadata['language']:
            html_tag = soup.find('html')
            if html_tag:
                lang = html_tag.get('lang') or html_tag.get('xml:lang')
                if lang:
                    metadata['language'] = lang.split('_')[0].split('-')[0]

        return metadata

    def _extract_json_ld_metadata(self, soup: BeautifulSoup) -> Dict:
        """
        Extract metadata from JSON-LD structured data.

        JSON-LD is increasingly common and provides the most reliable metadata.
        This method parses all JSON-LD blocks and extracts relevant fields.
        """
        metadata = {}

        try:
            # Find all JSON-LD script tags
            json_ld_scripts = soup.find_all('script', type='application/ld+json')

            for script in json_ld_scripts:
                if not script.string:
                    continue

                try:
                    data = json.loads(script.string)

                    # Handle @graph arrays (common in WordPress and other CMS)
                    if isinstance(data, dict) and '@graph' in data:
                        data_items = data['@graph']
                    elif isinstance(data, list):
                        data_items = data
                    else:
                        data_items = [data]

                    # Process each item in the JSON-LD
                    for item in data_items:
                        if not isinstance(item, dict):
                            continue

                        item_type = item.get('@type', '').lower()

                        # Skip non-content types
                        if item_type in ['breadcrumblist', 'webpage', 'website', 'faqpage',
                                        'searchaction', 'organization']:
                            continue

                        # Extract title/headline
                        if not metadata.get('title'):
                            metadata['title'] = (item.get('headline') or
                                                item.get('name') or
                                                item.get('title'))

                        # Extract dates
                        if not metadata.get('published_date'):
                            pub_date = (item.get('datePublished') or
                                       item.get('dateCreated') or
                                       item.get('uploadDate'))
                            if pub_date:
                                metadata['published_date'] = self._parse_date(pub_date)

                        if not metadata.get('modified_date'):
                            mod_date = (item.get('dateModified') or
                                       item.get('dateUpdated'))
                            if mod_date:
                                metadata['modified_date'] = self._parse_date(mod_date)

                        # Extract author (can be string, dict, or list)
                        if not metadata.get('author'):
                            author = item.get('author') or item.get('creator')
                            if author:
                                metadata['author'] = self._extract_json_ld_person(author)

                        # Extract publisher
                        if not metadata.get('publisher'):
                            publisher = item.get('publisher')
                            if publisher:
                                metadata['publisher'] = self._extract_json_ld_person(publisher)

                        # Extract description
                        if not metadata.get('description'):
                            metadata['description'] = (item.get('description') or
                                                      item.get('abstract'))

                        # Extract keywords
                        if not metadata.get('keywords'):
                            keywords = item.get('keywords')
                            if keywords:
                                if isinstance(keywords, list):
                                    metadata['keywords'] = keywords
                                elif isinstance(keywords, str):
                                    metadata['keywords'] = [k.strip() for k in keywords.split(',')]

                        # Extract article type
                        if not metadata.get('article_type') and item_type:
                            metadata['article_type'] = item_type

                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    self._logger.debug(f"Error parsing JSON-LD block: {e}")
                    continue

        except Exception as e:
            self._logger.debug(f"Error extracting JSON-LD metadata: {e}")

        return metadata

    def _extract_json_ld_person(self, person_data) -> Optional[str]:
        """
        Extract person name from JSON-LD person object.

        Person can be:
        - String: "John Doe"
        - Dict: {"@type": "Person", "name": "John Doe"}
        - List: [{"name": "Author 1"}, {"name": "Author 2"}]
        """
        if isinstance(person_data, str):
            return person_data
        elif isinstance(person_data, dict):
            return person_data.get('name') or person_data.get('Organization', {}).get('name')
        elif isinstance(person_data, list) and len(person_data) > 0:
            # Return first author for simplicity, or join multiple
            authors = []
            for person in person_data[:3]:  # Limit to first 3 authors
                if isinstance(person, str):
                    authors.append(person)
                elif isinstance(person, dict):
                    name = person.get('name')
                    if name:
                        authors.append(name)
            return ', '.join(authors) if authors else None
        return None

    def _parse_date(self, date_string: str) -> Optional[str]:
        """Parse various date formats into ISO format"""
        if not date_string:
            return None

        # Use dateutil.parser for robust parsing
        try:
            from dateutil.parser import parse
            parsed_date = parse(date_string)
            return parsed_date.isoformat()
        except ImportError:
            # Fallback to regex if dateutil is not available
            date_patterns = [
                r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})',  # ISO format
                r'(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
            ]
            for pattern in date_patterns:
                match = re.search(pattern, date_string)
                if match:
                    try:
                        date_str = match.group(1)
                        if 'T' in date_str:
                            return date_str[:19]  # Return YYYY-MM-DDTHH:MM:SS
                        else:
                            return date_str  # Return YYYY-MM-DD
                    except:
                        continue
        except Exception:
            # Failed to parse
            pass

        return None

    def _extract_content_preview(self, soup: BeautifulSoup) -> str:
        """Extract clean content preview from webpage"""

        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer", "aside"]):
            script.decompose()

        # Try to find main content areas first
        content_selectors = [
            'main', 'article', '.content', '#content',
            '.post-content', '.entry-content', '.article-body'
        ]

        text_content = ""
        for selector in content_selectors:
            element = soup.select_one(selector)
            if element:
                text_content = element.get_text()
                break

        # Fallback to body text if no main content found
        if not text_content:
            body = soup.find('body')
            text_content = body.get_text() if body else soup.get_text()

        # Clean the text
        # Remove extra whitespace and normalize
        lines = (line.strip() for line in text_content.splitlines())
        text_content = ' '.join(line for line in lines if line)

        # Truncate to 300 characters for a better preview
        preview = text_content[:300].strip()

        # Add ellipsis if truncated
        if len(text_content) > 300:
            preview += "..."

        return preview if preview else "No readable content found"

    def create_json_summary(self, results: Dict, save_to_file: bool = True, filename: str = None) -> Dict:
        """Create a structured JSON summary of search results"""

        if not results['success']:
            summary = {
                "search_metadata": {
                    "query": results['query'],
                    "timestamp": results['timestamp'],
                    "success": False,
                    "error": "No results found from any search method"
                },
                "results": []
            }
            return summary

        # Create structured summary
        summary = {
            "search_metadata": {
                "query": results['query'],
                "timestamp": results['timestamp'],
                "method_used": results['method_used'],
                "total_results": len(results['urls']),
                "success": True
            },
            "results": []
        }

        # Add detailed results if available
        if results.get('results'):
            for i, result in enumerate(results['results'], 1):
                result_entry = {
                    "rank": i,
                    "url": result['url'],
                    "title": result['title'],
                    "preview": result['preview'],
                    "status": result['status'],
                    "domain": self._extract_domain(result['url'])
                }

                # Include metadata if available
                if result.get('metadata'):
                    result_entry["metadata"] = result['metadata']

                summary['results'].append(result_entry)
        else:
            # Fallback if only URLs available
            for i, url in enumerate(results['urls'], 1):
                result_entry = {
                    "rank": i,
                    "url": url,
                    "title": "Not fetched",
                    "preview": "Content not retrieved",
                    "status": "url_only",
                    "domain": self._extract_domain(url)
                }
                summary['results'].append(result_entry)

        # Add statistics
        summary["statistics"] = {
            "unique_domains": len(set(result['domain'] for result in summary['results'])),
            "successful_fetches": len([r for r in summary['results'] if r['status'] == 'success']),
            "failed_fetches": len([r for r in summary['results'] if r['status'] == 'error'])
        }

        # Save to file if requested
        if save_to_file:
            if not filename:
                # Create filename from query and timestamp
                clean_query = re.sub(r'[^\w\s-]', '', results['query']).strip()
                clean_query = re.sub(r'[-\s]+', '_', clean_query)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"search_results_{clean_query[:50]}_{timestamp}.json"

            try:
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                self._logger.info(f"📄 JSON summary saved to: {filename}")
            except Exception as e:
                self._logger.error(f"⚠️  Failed to save JSON summary: {e}")

        return summary

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL"""
        try:
            parsed = urllib.parse.urlparse(url)
            return parsed.netloc
        except:
            return "unknown"

    def print_json_summary(self, summary: Dict, pretty: bool = True):
        """Print JSON summary to console"""
        if pretty:
            self._logger.info("\n📊 STRUCTURED JSON SUMMARY")
            self._logger.info("=" * 80)
            print(json.dumps(summary, indent=2, ensure_ascii=False))
        else:
            print(json.dumps(summary, ensure_ascii=False))

    def _convert_html_to_extraction_record(self, url: str, html_content: str, source_query: str = "", storage_dir: str = None, extraction_id: int = None) -> Dict:
        """
        Convert HTML content to extraction record format using docling.

        Args:
            url: Source URL
            html_content: Raw HTML content
            source_query: Original search query
            storage_dir: Directory for saving temporary files
            extraction_id: Optional ID to maintain correspondence with text extractions

        Returns:
            Dict in extraction record format with content, metadata, and links
        """
        if not DOCLING_AVAILABLE:
            self._logger.error("Docling not available for HTML conversion")
            return {}

        try:
            # Structured logging for docling operations
            self._logger.info("======= docling processing =======")
            self._logger.info(f"🔄 Starting HTML-to-markdown conversion for URL: {url[:50]}...")

            # Save HTML to temporary file (docling requires file input)
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as tmp_file:
                tmp_file.write(html_content)
                tmp_path = tmp_file.name

            try:
                # Convert HTML using docling
                converter = DocumentConverter()
                result = converter.convert(tmp_path)
                markdown_content = result.document.export_to_markdown()
                self._logger.debug(f"📝 Exported {len(markdown_content)} characters of markdown content")
                self._logger.info("==================")
            finally:
                # Clean up temporary file
                import os
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

            # Parse HTML for additional metadata
            soup = BeautifulSoup(html_content, 'html.parser')
            title = soup.find('title')
            title_text = title.get_text().strip() if title else "Untitled"

            # Extract links
            links = []
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text(strip=True)
                link_title = link.get('title', '')

                if href and text:
                    # Determine link type and domain
                    if href.startswith('http'):
                        link_type = 'external'
                        domain = urllib.parse.urlparse(href).netloc
                    elif href.startswith('/'):
                        link_type = 'internal'
                        domain = urllib.parse.urlparse(url).netloc
                        href = urllib.parse.urljoin(url, href)
                    else:
                        link_type = 'internal'
                        domain = urllib.parse.urlparse(url).netloc
                        href = urllib.parse.urljoin(url, href)

                    # Get context around the link
                    context = link.get_text(strip=True)
                    parent = link.parent
                    if parent:
                        context = parent.get_text(strip=True)[:100]

                    links.append({
                        'url': href,
                        'text': text[:100],  # Limit text length
                        'title': link_title,
                        'type': link_type,
                        'context': context,
                        'domain': domain
                    })

            # Extract images
            images = []
            for img in soup.find_all('img', src=True):
                src = img.get('src')
                alt = img.get('alt', '')
                title = img.get('title', '')
                width = img.get('width', '')
                height = img.get('height', '')

                if src:
                    # Handle relative URLs
                    if not src.startswith('http'):
                        src = urllib.parse.urljoin(url, src)

                    # Get context around the image
                    context = ""
                    parent = img.parent
                    if parent:
                        context = parent.get_text(strip=True)[:100]

                    images.append({
                        'src': src,
                        'alt': alt,
                        'title': title,
                        'width': width,
                        'height': height,
                        'context': context,
                        'domain': urllib.parse.urlparse(src).netloc
                    })

            # Create chunk sample (first 500 chars with ellipsis)
            chunk_sample = markdown_content[:500].strip()
            if len(markdown_content) > 500:
                chunk_sample += "..."

            # Determine method based on source
            method = 'docling_linked' if source_query else 'docling_web'

            # Return extraction record in cached_extractions.jsonl format
            # Core fields match downstream schema, rich web data preserved in metadata
            record = {
                'source_url': url,
                'method': method,
                'text': markdown_content,
                'text_length': len(markdown_content), # FIX: Was 'full_length'
                'metadata': {
                    'title': title_text,
                    'chunk_sample': chunk_sample,
                    'extracted_links': links[:50],  # Limit to first 50 links
                    'link_count': len(links),
                    'extracted_images': images[:15],  # Limit to first 15 images
                    'image_count': len(images),
                    'has_tables': False,  # Default for web content
                    'file_size_mb': 0.0,  # Not applicable for web content
                    'processing_time': 60  # Default processing time
                },
                'extraction_timestamp': datetime.now().isoformat()
            }

            # Add extraction_id if provided for record correlation
            if extraction_id is not None:
                record['id'] = extraction_id

            return record

        except Exception as e:
            # Structured error logging for docling failures
            self._logger.error("======= docling processing =======")
            self._logger.error(f"❌ HTML-to-markdown conversion failed for URL: {url[:50]}...")
            self._logger.error(f"❌ Error type: {type(e).__name__}")
            self._logger.error(f"❌ Error details: {str(e)}")
            self._logger.error("==================")

            # Save failed extraction if storage directory is provided
            if storage_dir:
                error_type = type(e).__name__
                self.save_failed_markdown_extraction(url, error_type, str(e), storage_dir)

            return {}

    def convert_html_batch_parallel(self, urls_and_html: List[Tuple[str, str, int]], storage_dir: str = None, num_workers: int = None) -> Tuple[List[Dict], List[Dict]]:
        """
        Convert multiple HTML documents to markdown in parallel using worker pool.

        This method uses multiprocessing to parallelize docling conversion, avoiding
        bottlenecks on large/complex HTML pages (e.g., Wikipedia).

        Args:
            urls_and_html: List of (url, html_content, extraction_id) tuples
            storage_dir: Directory for saving temporary files and errors
            num_workers: Number of worker processes (default: CPU count - 1)

        Returns:
            Tuple of (successful_records, error_records)
        """
        try:
            from utils.extraction_worker import ExtractionWorkerPool
        except ImportError:
            self._logger.warning("⚠️ extraction_worker module not available, falling back to serial processing")
            return self._convert_html_batch_serial(urls_and_html, storage_dir)

        if not urls_and_html:
            return [], []

        self._logger.info(f"🔧 Starting parallel HTML-to-markdown conversion for {len(urls_and_html)} documents")
        self._logger.info(f"   Workers: {num_workers or 'auto'}, Storage: {storage_dir or 'temp'}")

        start_time = time.time()

        with ExtractionWorkerPool(num_workers=num_workers) as pool:
            # Submit all tasks
            for url, html_content, extraction_id in urls_and_html:
                metadata = {
                    'method': 'docling_parallel',
                    'processing_time': 0
                }
                pool.submit_task(url, html_content, extraction_id, metadata)

            # Collect results
            successful, errors = pool.get_results(len(urls_and_html))

        elapsed = time.time() - start_time

        self._logger.info(f"✅ Parallel conversion complete: {len(successful)} successful, {len(errors)} failed in {elapsed:.2f}s")

        # Save failed extractions if storage directory provided
        if storage_dir and errors:
            for error in errors:
                self.save_failed_markdown_extraction(
                    error['url'],
                    error['error_type'],
                    error['error_message'],
                    storage_dir
                )

        return successful, errors

    def _convert_html_batch_serial(self, urls_and_html: List[Tuple[str, str, int]], storage_dir: str = None) -> Tuple[List[Dict], List[Dict]]:
        """
        Fallback serial conversion when parallel processing is not available.

        Args:
            urls_and_html: List of (url, html_content, extraction_id) tuples
            storage_dir: Directory for saving temporary files and errors

        Returns:
            Tuple of (successful_records, error_records)
        """
        successful = []
        errors = []

        for url, html_content, extraction_id in urls_and_html:
            record = self._convert_html_to_extraction_record(url, html_content, "", storage_dir, extraction_id)
            if record:
                successful.append(record)
            else:
                errors.append({
                    'url': url,
                    'extraction_id': extraction_id,
                    'error_type': 'conversion_failed',
                    'error_message': 'Serial conversion returned empty record'
                })

        return successful, errors

    def save_web_extractions_to_cache(self, extraction_records: List[Dict], extraction_file: str):
        """
        Save web extraction records to cache file in JSONL format.

        Args:
            extraction_records: List of extraction record dictionaries
            extraction_file: Path to save the JSONL file
        """
        try:
            # Ensure directory exists
            Path(extraction_file).parent.mkdir(parents=True, exist_ok=True)

            # Save as JSONL (one JSON object per line)
            with open(extraction_file, 'w', encoding='utf-8') as f:
                for i, record in enumerate(extraction_records):
                    # Add sequential ID if not present
                    if 'id' not in record:
                        record['id'] = i
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')

            self._logger.info(f"💾 Saved {len(extraction_records)} extraction records to: {extraction_file}")

        except Exception as e:
            self._logger.error(f"Error saving extractions to cache: {e}")

    def save_failed_markdown_extraction(self, url: str, error_type: str, error_message: str, storage_dir: str):
        """
        Save failed markdown extraction attempts for debugging and recovery.

        Args:
            url: Source URL that failed conversion
            error_type: Type of error (e.g., 'docling_error', 'parsing_error')
            error_message: Detailed error message
            storage_dir: Directory to save failed extraction log
        """
        try:
            failed_extraction_file = Path(storage_dir) / "extract_markdown_failed.json"

            # Ensure directory exists
            Path(storage_dir).mkdir(parents=True, exist_ok=True)

            # Create failure record
            failure_record = {
                'url': url,
                'error_type': error_type,
                'error_message': error_message,
                'timestamp': datetime.now().isoformat(),
                'retry_count': 0
            }

            # Load existing failed records or create new list
            failed_records = []
            if failed_extraction_file.exists():
                try:
                    with open(failed_extraction_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        if isinstance(existing_data, list):
                            failed_records = existing_data
                        elif isinstance(existing_data, dict) and 'failed_extractions' in existing_data:
                            failed_records = existing_data['failed_extractions']
                except Exception as load_error:
                    self._logger.warning(f"Could not load existing failed extractions: {load_error}")

            # Check if URL already exists, update retry count if so
            url_exists = False
            for record in failed_records:
                if record.get('url') == url:
                    record['retry_count'] += 1
                    record['last_error'] = error_message
                    record['last_timestamp'] = datetime.now().isoformat()
                    url_exists = True
                    break

            # Add new record if URL doesn't exist
            if not url_exists:
                failed_records.append(failure_record)

            # Save updated failed records
            failed_data = {
                'failed_extractions': failed_records,
                'total_failures': len(failed_records),
                'last_updated': datetime.now().isoformat()
            }

            with open(failed_extraction_file, 'w', encoding='utf-8') as f:
                json.dump(failed_data, f, indent=2, ensure_ascii=False)

            self._logger.debug(f"💾 Saved failed extraction record for {url[:50]}... to {failed_extraction_file}")

        except Exception as e:
            self._logger.error(f"Error saving failed extraction record: {e}")

    def search_with_extractions(self, query: str, num_results: int = 5,
                               save_extractions: bool = False,
                               extraction_file: str = None) -> Dict:
        """
        Enhanced search method that can optionally save extractions to file.

        Args:
            query: Search query string
            num_results: Maximum number of results to return
            save_extractions: Whether to save extraction records to file
            extraction_file: Path to save extractions (optional)

        Returns:
            Dict with search results and metadata
        """
        # First get URLs from search
        search_results = self.search(query, num_results=num_results, include_content=False)

        if save_extractions and search_results.get('success', False) and search_results.get('urls'):
            self._logger.info(f"🔄 Creating extractions for {len(search_results['urls'])} URLs...")
            extraction_records = []

            # Fetch HTML content and convert to extraction records
            for i, url in enumerate(search_results['urls'], 1):
                self._logger.debug(f"   📖 Processing {i}/{len(search_results['urls'])}: {url[:60]}...")

                try:
                    # Fetch the full HTML content
                    response = self.session.get(url, timeout=10)

                    if response.status_code == 200:
                        html_content = response.content.decode('utf-8', errors='ignore')

                        # Convert HTML to extraction record using docling
                        extraction_record = self._convert_html_to_extraction_record(
                            url=url,
                            html_content=html_content,
                            source_query=query
                        )

                        if extraction_record:
                            extraction_record['id'] = i - 1  # Add sequential ID
                            extraction_records.append(extraction_record)
                            self._logger.debug(f"      ✅ {extraction_record['metadata']['title'][:50]}...")
                            # FIX: Use 'text_length' key
                            self._logger.debug(f"         📄 Extracted {extraction_record['text_length']:,} chars")
                        else:
                            self._logger.warning(f"      ⚠️ Failed to create extraction record for {url}")

                    else:
                        self._logger.warning(f"      ❌ HTTP {response.status_code} for {url}")

                except Exception as e:
                    self._logger.warning(f"      ❌ Error processing {url}: {str(e)[:50]}...")

                # Small delay between requests
                time.sleep(random.uniform(0.5, 1.5))

            # Save extractions if we have records
            if extraction_records and extraction_file:
                self.save_web_extractions_to_cache(extraction_records, extraction_file)
                self._logger.info(f"✅ Successfully created {len(extraction_records)} extraction records")
            else:
                self._logger.warning("⚠️ No extraction records were created")

        # Return regular search results with content for compatibility
        # Re-run search but this time with content
        return self.search(query, num_results=num_results, include_content=True)


# CLI Interface using Click
@click.command()
@click.argument('query', nargs=-1)
@click.option('--num-results', '-n', default=5, help='Number of results to fetch')
@click.option('--output', '-o', help='Output filename for JSON results')
@click.option('--no-save', is_flag=True, help='Don\'t save JSON results to file')
@click.option('--compact', is_flag=True, help='Compact JSON output')
@click.option('--include-content/--no-content', default=True, help='Include page content and metadata')
@click.option('--verbose', '-v', is_flag=True, help='Verbose logging')
def main(query, num_results, output, no_save, compact, include_content, verbose):
    """
    Document Web Searcher CLI

    Search the web using multiple search engines with fallback.

    Example:
        python document_websearcher.py python web scraping tutorial
        python document_websearcher.py -n 10 -o results.json machine learning
    """

    # Join query parts
    if query:
        search_query = ' '.join(query)
    else:
        search_query = "python web scraping tutorial"

    # Set logging level
    if verbose:
        import logging
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo(f"🔍 Running search for: '{search_query}'")
    click.echo(f"📊 Requesting {num_results} results")
    if not no_save:
        click.echo(f"📄 Will save JSON summary to file")

    # Create searcher and show available search methods
    searcher = DocumentWebSearcher()

    # Display search method availability
    methods_available = []
    if searcher.tavily_available:
        methods_available.append("Tavily API")
    if searcher.google_available:
        methods_available.append("Google API")
    methods_available.append("DuckDuckGo Impersonation")

    click.echo(f"🔧 Available search methods: {' → '.join(methods_available)}")

    # Perform search (now async)
    import asyncio
    results = asyncio.run(searcher.search(search_query, num_results=num_results, include_content=include_content))

    # Create and display JSON summary
    summary = searcher.create_json_summary(results, save_to_file=not no_save, filename=output)
    searcher.print_json_summary(summary, pretty=not compact)

    # Exit with appropriate code
    if results['success']:
        click.echo(f"\n✅ Search completed successfully - {len(results['urls'])} results found")
        exit(0)
    else:
        click.echo(f"\n❌ Search failed - no results found")
        exit(1)


if __name__ == "__main__":
    # Add dateutil for better parsing
    # pip install python-dateutil
    main()
