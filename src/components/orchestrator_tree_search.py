#!/usr/bin/env python3
"""
Simple Tree Search Implementation - Exact Original Algorithm

This implements the exact original algorithm from the archive without any modifications.
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Set

from bs4 import BeautifulSoup

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..'))

from src._clients._ import GeminiLLMClient
from src._clients.llm_client import SimpleLLMClient
from src.clients.llm_cache_client import CachedLLMClient
from src.components.document_websearcher import DocumentWebSearcher
from src.config import app_conf
from src.config_handler import Base
from src.models.tree_search import SearchNode, TreeSearchStats

# from src.utils.merge_content import _is_valid_content  # TODO: Create this module


class SimpleTreeSearch(Base):
    """Tree search using exact original algorithm from archive."""

    def __init__(self, query: str = "machine_learning", use_llm: bool = None, text_chunk_size: int = None, storage_dir: str = None, extract_markdown: bool = None, llm_provider: str = None):
        """Initialize exactly like the original TreeSearchEngine but with config integration

        Args:
            query: Search query for directory naming
            use_llm: Enable/disable LLM query expansion
            text_chunk_size: Size of text chunks for processing
            storage_dir: Custom storage directory path
            extract_markdown: Enable/disable markdown extraction
            llm_provider: LLM provider to use ('gcp' or 'local'). Defaults to 'gcp'.
        """
        super().__init__()
        self._logger.info("🌳 Initializing SimpleTreeSearch with original algorithm")

        # Use config values as defaults with fallbacks, allow override via parameters
        self.use_llm = use_llm if use_llm is not None else (app_conf.get("tree_search.use_llm") or False)
        self.text_chunk_size = text_chunk_size if text_chunk_size is not None else (app_conf.get("tree_search.text_chunk_size") or 500)
        self.extract_markdown = extract_markdown if extract_markdown is not None else (app_conf.get("tree_search.extract_markdown") if app_conf.get("tree_search.extract_markdown") is not None else True)
        self.llm_provider = llm_provider if llm_provider is not None else 'gcp'  # Default to GCP
        self.linked_batch_size = app_conf.get("tree_search.linked_batch_size") or 10  # Batch size for linked content processing

        # Use DocumentWebSearcher for modern web search capabilities
        self.searcher = DocumentWebSearcher()

        # Set storage directory - use config default relative to workspace root
        if storage_dir is None:
            # Get workspace root (go up from src/components to project root)
            workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_storage_dir = (app_conf.get("tree_search.base_storage_dir") or "working_dir") + "/" + query
            self.storage_dir = os.path.join(workspace_root, config_storage_dir)
        else:
            self.storage_dir = storage_dir

        # Create storage directory if it doesn't exist
        os.makedirs(self.storage_dir, exist_ok=True)

        # Initialize flat file storage paths - EXACT same as original
        self.extracted_text_file = os.path.join(self.storage_dir, "extracted_text.jsonl")
        self.failed_urls_file = os.path.join(self.storage_dir, "failed_urls.jsonl")
        self.pdf_references_file = os.path.join(self.storage_dir, "pdf_references.jsonl")

        if self.use_llm:
            # Initialize LLM client based on provider
            if self.llm_provider == 'gcp':
                _inner = GeminiLLMClient()
                self.llm_client = CachedLLMClient(
                    _inner,
                    redis_host=app_conf.get("redis.host") or "",
                    redis_port=int(app_conf.get("redis.port") or 6379),
                    redis_password=app_conf.get("redis.password") or None,
                    ttl_seconds=int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000),
                )
                self._logger.info(f"✅ Tree search initialized with GCP Gemini LLM expansion (text chunks: {self.text_chunk_size} chars)")
            elif self.llm_provider == 'local':
                self.llm_client = SimpleLLMClient()
                self._logger.info(f"✅ Tree search initialized with local LLM expansion (text chunks: {self.text_chunk_size} chars)")
            else:
                self._logger.warning(f"⚠️  Unknown LLM provider '{self.llm_provider}', defaulting to GCP Gemini")
                _inner = GeminiLLMClient()
                self.llm_client = CachedLLMClient(
                    _inner,
                    redis_host=app_conf.get("redis.host") or "",
                    redis_port=int(app_conf.get("redis.port") or 6379),
                    redis_password=app_conf.get("redis.password") or None,
                    ttl_seconds=int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000),
                )
                self._logger.info(f"✅ Tree search initialized with GCP Gemini LLM expansion (text chunks: {self.text_chunk_size} chars)")
        else:
            self.llm_client = None
            self._logger.info("⚠️  Tree search initialized without LLM expansion (will use keyword variations)")

        # Structured logging for extraction configuration
        if self.extract_markdown:
            self._logger.info("✅ Markdown extraction enabled (using docling HTML-to-markdown conversion)")
        else:
            self._logger.info("⚪ Markdown extraction disabled (text-only mode)")

        self._logger.info(f"📁 Data will be stored in: {self.storage_dir}")

        # Track all discovered URLs to avoid duplicates - exact same as original
        self.discovered_urls: Set[str] = set()
        # Track URLs by source type
        self.search_urls: Set[str] = set()  # URLs found via search engines
        self.extracted_urls: Set[str] = set()  # URLs extracted from page content
        self.search_tree: List[SearchNode] = []
        self.stats = TreeSearchStats()
        self.extraction_counter = 0  # For primary keys

        # Track content hashes to prevent duplicates across all extractions
        self.seen_content_hashes: Set[str] = set()
        self._logger.info("📋 Content deduplication enabled (hash-based)")
        self._logger.info("🔍 Content filtering enabled (media files, binary content)")
        self._logger.info("📄 All PDFs will be logged to pdf_references.jsonl (excluded from markdown conversion)")

    async def true_tree_search(
        self,
        root_query,
        max_depth: int = 2,
        branches_per_node: int = 5,
        urls_per_query: int = 10,
        min_url_growth: int = 1,
        early_stopping_criterion: str = 'none',
        early_stopping_threshold: float = 0.5
    ) -> Dict[str, Any]:
        """
        Execute true tree search algorithm as described:
        1. Root query -> search -> extract chunks -> LLM generates queries
        2. Each query becomes a branch, execute all branches at current depth
        3. Return to leftmost branch, use its chunks for next level expansion
        4. Continue systematically left-to-right, depth-first
        5. Stop when max_depth reached or URL growth <= min_url_growth

        Args:
            root_query: Can be either:
                       - str: Single query string
                       - List[str]: Collection of related queries (first one is primary)
            max_depth: Maximum tree search depth
            branches_per_node: Number of branches to generate per node
            urls_per_query: URLs to fetch per query
            min_url_growth: Minimum URL growth to continue
            early_stopping_criterion: Early stopping criterion ('none', 'avg_growth', 'total_growth')
            early_stopping_threshold: Early stopping threshold value
        """
        start_time = time.time()

        # Handle query collection vs single query
        if isinstance(root_query, list):
            query_collection = root_query
            primary_query = root_query[0]
            self._logger.info(f"🔍 Using query collection with {len(query_collection)} grounding queries")
        else:
            query_collection = root_query
            primary_query = root_query

        self._logger.info("🌳 TRUE TREE SEARCH ALGORITHM")
        self._logger.info("=" * 60)
        self._logger.info(f"Root Query: '{primary_query}'")
        self._logger.info(f"Max Depth: {max_depth}, Branches per node: {branches_per_node}")
        self._logger.info(f"URLs per query: {urls_per_query}, Min URL growth: {min_url_growth}")
        if early_stopping_criterion != 'none':
            self._logger.info(f"🚦 Early stopping: {early_stopping_criterion} <= {early_stopping_threshold}")

        # Initialize tree structure
        tree_nodes = {}  # depth -> [nodes]
        all_discovered_urls = set()

        # Step 1: Execute all queries in collection (if provided) to gather rich context
        if isinstance(root_query, list) and len(root_query) > 1:
            self._logger.info(f"📍 STEP 1: Executing query collection ({len(query_collection)} queries) for rich context")

            # Execute all queries and combine their content
            collection_nodes = []
            combined_text_content = []

            for i, query in enumerate(query_collection, 1):
                self._logger.info(f"   🔍 Query {i}/{len(query_collection)}: '{query}'")
                node = await self._execute_search_node(query, depth=0)
                collection_nodes.append(node)
                all_discovered_urls.update(node.urls)

                if node.text_content:
                    combined_text_content.append(f"=== CONTENT FROM QUERY {i}: {query} ===\n{node.text_content}")
                    self._logger.info(f"      ✅ Found {len(node.urls)} URLs, extracted {len(node.text_content)} chars")
                else:
                    self._logger.warning(f"      ⚠️ No content extracted for query {i}")

            # Use the primary query's node as root, but with combined content
            root_node = collection_nodes[0]
            root_node.text_content = "\n\n".join(combined_text_content)
            tree_nodes[0] = collection_nodes  # Store all nodes at depth 0

            root_urls_count = len(all_discovered_urls)
            self._logger.info(f"✅ Query collection found {root_urls_count} total URLs, combined content: {len(root_node.text_content)} chars")

        else:
            # Single query - original behavior
            self._logger.info("📍 STEP 1: Executing root query")
            root_node = await self._execute_search_node(primary_query, depth=0)
            tree_nodes[0] = [root_node]

            # Update discovered URLs
            all_discovered_urls.update(root_node.urls)
            root_urls_count = len(root_node.urls)

            self._logger.info(f"✅ Root query found {root_urls_count} URLs, extracted {len(root_node.text_content)} chars of content")

        if root_urls_count == 0:
            self._logger.warning("❌ No results from root query - terminating search")
            # Return early with empty growth metrics
            return self._compile_true_tree_results(tree_nodes, all_discovered_urls, time.time() - start_time, [], None)

        # Step 2: Generate expansion queries from root content (with combined context from all queries)
        self._logger.info("📍 STEP 2: Generating expansion queries from combined content")
        expansion_queries = await self._generate_semantic_queries(query_collection, root_node.text_content, branches_per_node)
        root_node.generated_queries = expansion_queries

        self._logger.info(f"🔍 Generated {len(expansion_queries)} expansion queries: {expansion_queries}")

        # Step 3: Main tree traversal loop
        current_depth = 1
        growth_metrics = []  # Track growth for averaging

        while current_depth <= max_depth:
            self._logger.info(f"📍 STEP 3.{current_depth}: Processing depth {current_depth}")

            # Check if we have expansion queries to process
            if not expansion_queries:
                self._logger.warning("⚠️ No expansion queries available - terminating tree traversal")
                break

            # Determine the parent query for this depth
            if current_depth == 1:
                parent_query_for_depth = primary_query
                expansion_query_parents = [primary_query] * len(expansion_queries)
            else:
                # For depth > 1, track which parent generated each expansion query
                expansion_query_parents = getattr(self, 'expansion_query_parents', [primary_query] * len(expansion_queries))

            # Execute all queries at current depth (create all branches)
            current_level_nodes = []
            depth_url_growth = 0
            depth_growth_per_query = []

            for i, query in enumerate(expansion_queries):
                self._logger.info(f"🌿 Branch {i+1}/{len(expansion_queries)}: '{query}'")

                # Use the correct parent for each query
                current_parent = expansion_query_parents[i] if i < len(expansion_query_parents) else primary_query

                # Execute search for this branch with proper parent query tracking
                branch_node = await self._execute_search_node(query, depth=current_depth, parent_query=current_parent)

                # Calculate new URLs (growth)
                new_urls = [url for url in branch_node.urls if url not in all_discovered_urls]
                branch_node.growth = len(new_urls)
                branch_node.urls = new_urls  # Store only new URLs

                # Update discovered URLs
                all_discovered_urls.update(new_urls)
                depth_url_growth += len(new_urls)
                depth_growth_per_query.append(len(new_urls))

                current_level_nodes.append(branch_node)

                self._logger.info(f"📈 Branch {i+1} growth: +{len(new_urls)} new URLs")

            # Store nodes at this depth
            tree_nodes[current_depth] = current_level_nodes

            # Calculate and track growth metrics for this depth
            avg_growth_this_depth = sum(depth_growth_per_query) / len(depth_growth_per_query) if depth_growth_per_query else 0
            growth_metrics.append({
                'depth': current_depth,
                'total_growth': depth_url_growth,
                'avg_growth_per_query': avg_growth_this_depth,
                'queries_count': len(expansion_queries)
            })

            self._logger.info(f"📊 Depth {current_depth} summary: {depth_url_growth} total URLs, avg {avg_growth_this_depth:.1f} URLs/query from {len(expansion_queries)} branches")

            # Check stopping criteria
            if depth_url_growth <= min_url_growth:
                self._logger.info(f"🛑 Stopping: URL growth ({depth_url_growth}) <= minimum ({min_url_growth})")
                break

            # Check early stopping criterion
            if early_stopping_criterion != 'none' and current_depth >= 2:  # Need at least 2 depths for trend
                should_stop = False
                if early_stopping_criterion == 'avg_growth':
                    if avg_growth_this_depth <= early_stopping_threshold:
                        should_stop = True
                        self._logger.info(f"🚦 Early stopping: Average growth ({avg_growth_this_depth:.2f}) <= threshold ({early_stopping_threshold})")
                elif early_stopping_criterion == 'total_growth':
                    if depth_url_growth <= early_stopping_threshold:
                        should_stop = True
                        self._logger.info(f"🚦 Early stopping: Total growth ({depth_url_growth}) <= threshold ({early_stopping_threshold})")

                if should_stop:
                    self._logger.info(f"🎯 Early termination at depth {current_depth} due to growth saturation")
                    break

            if current_depth >= max_depth:
                self._logger.info(f"🛑 Stopping: Reached maximum depth ({max_depth})")
                break

            # Step 4: Generate queries for next level from ALL branches with content
            self._logger.info(f"� STEP 4: Generating next level queries from leftmost branch at depth {current_depth}")

            expansion_queries = []
            expansion_query_parents = []  # Track which parent generated each query
            for node in current_level_nodes:
                if node.text_content and len(node.text_content.strip()) > 100:  # Has substantial content
                    self._logger.info(f"🎯 Generating from branch: '{node.query}' ({len(node.text_content)} chars)")
                    node_expansion_queries = await self._generate_semantic_queries(node.query, node.text_content, branches_per_node)
                    node.generated_queries = node_expansion_queries
                    expansion_queries.extend(node_expansion_queries)
                    # Track parent for each generated query
                    expansion_query_parents.extend([node.query] * len(node_expansion_queries))
                    self._logger.info(f"   📋 Added {len(node_expansion_queries)} queries from this branch")

            # Store parent mapping for next iteration
            self.expansion_query_parents = expansion_query_parents

            if not expansion_queries:
                self._logger.warning("⚠️ No expansion queries generated from current level - terminating")
                break

            self._logger.info(f"🔍 Generated {len(expansion_queries)} total queries for depth {current_depth + 1}: {expansion_queries}")
            current_depth += 1

        # Step 5: Extract linked content from all discovered links
        self._logger.info("📍 STEP 5: Extracting linked content from discovered URLs")
        all_linked_urls = self._collect_all_linked_urls(tree_nodes)

        if all_linked_urls:
            self._logger.info(f"🔗 Found {len(all_linked_urls)} linked URLs to extract content from")
            await self._extract_linked_content(all_linked_urls)
        else:
            self._logger.info("⚠️ No linked URLs found for content extraction")

        # Merge extracted main + linked content files (markdown & text) into combined files
        # This creates deduplicated merged JSONL files for downstream processing.
        try:
            self._merge_extracted_results()
        except Exception as e:
            self._logger.warning(f"⚠️ Failed to merge extracted results: {e}")

        # Compile results
        total_time = time.time() - start_time
        early_stopping_info = {
            'criterion': early_stopping_criterion,
            'threshold': early_stopping_threshold if early_stopping_criterion != 'none' else None,
            'triggered': current_depth < max_depth and early_stopping_criterion != 'none'
        }
        results = self._compile_true_tree_results(tree_nodes, all_discovered_urls, total_time, growth_metrics, early_stopping_info)

        self._logger.info("🎯 TRUE TREE SEARCH COMPLETE")
        self._logger.info(f"Total unique URLs discovered: {len(all_discovered_urls)}")
        self._logger.info(f"Maximum depth reached: {current_depth - 1}")
        self._logger.info(f"Search duration: {total_time:.2f} seconds")

        # Reconcile extraction files
        reconciliation = self._reconcile_extraction_files()
        results['reconciliation'] = reconciliation

        return results

    async def _execute_search_node(self, query: str, depth: int, parent_query: str = None) -> SearchNode:
        """Execute a search for a single node and extract content - now with async search"""
        node = SearchNode(query=query, depth=depth, parent_query=parent_query)

        # Set current query for storage tracking
        self.current_query = query

        # Execute search to get URLs (now async)
        search_results = await self.searcher.search(query, num_results=10, include_content=False)
        node.urls = search_results.get('urls', [])

        self.stats.total_queries_executed += 1

        # Extract text content from pages (this is crucial for LLM query generation)
        if node.urls:
            self._logger.debug(f"📄 Extracting text content from {min(len(node.urls), 5)} pages...")
            # Extract text from all URLs found, not just first 3
            text_content, extracted_links = await self._extract_text_from_urls(node.urls)
            node.text_content = text_content
            node.extracted_links = extracted_links
            self.stats.total_text_extracted += 1

            # Log text extraction success
            if node.text_content:
                total_links = sum(len(links) for links in extracted_links.values())
                self._logger.debug(f"✅ Extracted {len(node.text_content)} characters of text content and {total_links} links")
            else:
                self._logger.warning("⚠️ No text content extracted")

        return node

    async def _fetch_url_content(self, url: str, index: int) -> tuple[int, str, str, int]:
        """
        Fetch a single URL's content asynchronously
        Returns: (index, url, response_text, status_code)
        """
        try:
            # Run blocking call in executor to not block event loop
            import asyncio
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, lambda: self.searcher.session.get(url, timeout=10))
            return (index, url, response.text, response.status_code)
        except Exception as e:
            self._logger.warning(f"❌ URL {index+1}: Error fetching {url[:50]}: {str(e)[:50]}...")
            return (index, url, None, 0)

    async def _extract_text_from_urls(self, urls: List[str]) -> tuple[str, Dict[str, List[Dict[str, str]]]]:
        """
        Extract clean text content from URLs with concurrent fetching for speed

        Returns:
            tuple: (primary_text_chunk, {url: [extracted_links]})
        """
        sampled_text_chunks = []
        successful_extractions = 0
        all_extracted_links = {}  # Store links by source URL
        html_extractions = []  # Store HTML-to-markdown extractions
        html_batch = []  # Batch HTML content for parallel processing

        self._logger.info(f"📡 Processing {len(urls)} URLs for text extraction (chunk size: {self.text_chunk_size} chars)...")

        # Fetch all URLs concurrently (up to 7)
        urls_to_fetch = urls[:10]
        self._logger.debug(f"🚀 Fetching {len(urls_to_fetch)} URLs concurrently...")

        fetch_tasks = [self._fetch_url_content(url, i) for i, url in enumerate(urls_to_fetch)]
        fetch_results = await asyncio.gather(*fetch_tasks)

        # Process fetched content
        for i, url, response_text, status_code in fetch_results:
            if not response_text or status_code != 200:
                self._logger.warning(f"❌ URL {i+1}: HTTP {status_code} for {url[:50]}...")

                # Check if this is a PDF URL (by extension) and log to pdf_references even on failure
                # This ensures we have an audit trail of all PDFs, including blocked ones
                if url.lower().endswith('.pdf'):
                    self._logger.debug(f"📄 Logging failed PDF to pdf_references.jsonl - {url[:60]}...")
                    self._log_pdf_reference(
                        url=url,
                        size_mb=0.0,  # Unknown size for failed download
                        source_article=url,
                        failure_reason=f"HTTP {status_code}"
                    )

                self._log_failed_url(url, f"http_{status_code}", f"Status: {status_code}")
                continue

            try:
                # Check if content should be filtered (media files, binary content)
                # Note: PDFs are NOT filtered here - they're logged during markdown batching
                should_filter, filter_reason = self._should_filter_content(response_text, url)
                if should_filter:
                    self._logger.debug(f"🚫 URL {i+1}: Filtered - {filter_reason} - {url[:50]}...")
                    self._log_failed_url(url, "filtered", filter_reason)
                    continue

                soup = BeautifulSoup(response_text, 'html.parser')

                # Extract comprehensive page metadata (title, dates, author, etc.)
                page_metadata = self.searcher._extract_page_metadata(soup)
                title = page_metadata.get('title', 'No title')

                # BEFORE removing iframes, check for embedded PDF documents
                iframes = soup.find_all('iframe')
                for iframe in iframes:
                    iframe_src = iframe.get('src', '')
                    if iframe_src:
                        # Detect PDF URLs in iframes (common patterns)
                        is_pdf_iframe = (
                            '.pdf' in iframe_src.lower() or
                            'type=pdf' in iframe_src.lower() or
                            't=pdf' in iframe_src.lower() or
                            '/pdf' in iframe_src.lower()
                        )

                        if is_pdf_iframe:
                            self._logger.debug(f"📄 URL {i+1}: Detected iframe PDF: {iframe_src[:80]}...")
                            self._log_pdf_reference(
                                url=iframe_src,
                                size_mb=0.0,  # Size unknown for iframe PDFs
                                source_article=url,
                                failure_reason="iframe_embedded_pdf"
                            )

                # Remove scripts, styles, and navigation elements
                for element in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe"]):
                    element.decompose()

                # Get main content using multiple strategies
                text_content = ""

                # Try main content selectors
                content_selectors = [
                    'main', 'article', '.content', '#content',
                    '.post-content', '.entry-content', '.article-body',
                    '.post-body', '.story-body', '[role="main"]'
                ]

                for selector in content_selectors:
                    element = soup.select_one(selector)
                    if element:
                        text_content = element.get_text()
                        break

                # Fallback to body content
                if not text_content:
                    body = soup.find('body')
                    text_content = body.get_text() if body else soup.get_text()

                # Clean and structure the text
                clean_text = ' '.join(text_content.split())

                # Check for duplicate content
                content_hash = self._get_content_hash(clean_text)
                if self._is_duplicate(content_hash):
                    self._logger.debug(f"🔄 URL {i+1}: Duplicate content detected, skipping - {url[:50]}...")
                    continue

                # Extract links from the page
                extracted_links = self._extract_links_from_page(soup, url)
                all_extracted_links[url] = extracted_links

                # Extract images from the page
                extracted_images = self._extract_images_from_page(soup, url)

                # Create variable-sized text chunk sample
                text_chunk = self._create_text_chunk_sample(clean_text, self.text_chunk_size)

                # Store extracted text in flat file with full content and metadata
                extraction_data = {
                    "id": self.extraction_counter,
                    "url": url,
                    "title": title,
                    "content": clean_text,  # Store FULL content (removed character limit)
                    "chunk_sample": text_chunk,
                    "full_length": len(clean_text),
                    "extracted_links": extracted_links,  # Add extracted links
                    "link_count": len(extracted_links),
                    "extracted_images": extracted_images,  # Add extracted images
                    "image_count": len(extracted_images),
                    "timestamp": datetime.now().isoformat(),
                    "source_query": getattr(self, 'current_query', 'unknown'),
                    # Add comprehensive metadata
                    "published_date": page_metadata.get('published_date'),
                    "modified_date": page_metadata.get('modified_date'),
                    "author": page_metadata.get('author'),
                    "description": page_metadata.get('description'),
                    "keywords": page_metadata.get('keywords'),
                    "language": page_metadata.get('language'),
                    "article_type": page_metadata.get('article_type'),
                    "site_name": page_metadata.get('site_name'),
                    "publisher": page_metadata.get('publisher'),
                    "section": page_metadata.get('section')
                }

                self._store_extracted_text(extraction_data)

                # Collect HTML for batch parallel markdown processing using the same ID as text extraction
                # Skip PDFs to avoid blocking worker pool with pipeline initialization
                # Log ALL PDFs to pdf_references.jsonl for consistency and audit trail
                if self.extract_markdown:
                    is_pdf = response_text.strip().startswith('%PDF')
                    if not is_pdf:
                        html_batch.append((url, response_text, extraction_data["id"]))
                    else:
                        # Log PDF to references file (all PDFs, not just oversized)
                        size_mb = len(response_text.encode('utf-8', errors='ignore')) / (1024 * 1024)
                        self._log_pdf_reference(url, size_mb, source_article=url)
                        self._logger.debug(f"⏭️  Skipping PDF from markdown batch: {url[:60]} ({size_mb:.2f}MB)")

                self.extraction_counter += 1

                # Add the chunk sample to our primary text chunks
                formatted_chunk = f"SOURCE {successful_extractions + 1} - {title[:50]}:\n{text_chunk}"
                sampled_text_chunks.append(formatted_chunk)
                successful_extractions += 1

                self._logger.debug(f"✅ URL {i+1}: Sampled {len(text_chunk)} chars from {len(clean_text)} total chars, {len(extracted_links)} links - {url[:50]}...")

            except Exception as e:
                self._logger.warning(f"❌ URL {i+1}: Error extracting from {url[:50]}: {str(e)[:50]}...")
                self._log_failed_url(url, "extraction_error", str(e)[:200])
                continue

        # Process collected HTML batch in parallel using docling
        if html_batch and self.extract_markdown:
            self._logger.info(f"🔄 Processing {len(html_batch)} HTML documents in parallel with docling...")
            successful_md, failed_md = self.searcher.convert_html_batch_parallel(
                urls_and_html=html_batch,
                storage_dir=self.storage_dir,
                num_workers=None  # Auto-detect CPU count
            )

            # Save successful conversions to JSONL
            if successful_md:
                markdown_output_path = os.path.join(self.storage_dir, "extracted_markdown.jsonl")
                with open(markdown_output_path, 'a', encoding='utf-8') as f:
                    for record in successful_md:
                        f.write(json.dumps(record, ensure_ascii=False) + '\n')
                html_extractions.extend(successful_md)
                successful_extractions += len(successful_md)
                self._logger.info(f"✅ Saved {len(successful_md)} parallel markdown extractions to JSONL")

            # Log failed conversions
            for failure in failed_md:
                error_msg = failure.get('error_message', failure.get('error', 'Unknown error'))
                self._logger.warning(f"⚠️ Parallel extraction failed for {failure['url']}: {error_msg[:100]}")
                self._log_failed_url(failure['url'], "markdown_conversion_error", error_msg[:200])

        # Create optimized primary text chunk from samples
        if sampled_text_chunks:
            primary_text_chunk = "\n\n---SOURCE-BREAK---\n\n".join(sampled_text_chunks)
            self._logger.debug(f"✓ Created optimized text chunk: {len(primary_text_chunk)} chars from {successful_extractions} sources")
        else:
            primary_text_chunk = ""
            self._logger.warning(f"⚠️ No successful text extractions from {len(urls)} URLs")

        # Log summary of HTML-to-markdown extractions (using progressive saving)
        if html_extractions:
            self._logger.info(f"✅ Progressively saved {len(html_extractions)} HTML-to-markdown extractions to: extracted_markdown.jsonl")
        elif self.extract_markdown:
            self._logger.warning("⚠️ No successful HTML-to-markdown conversions to save")

        return primary_text_chunk, all_extracted_links

    def _extract_links_from_page(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, str]]:
        """Extract links from the page and categorize them - EXACT COPY from original"""
        from urllib.parse import urljoin, urlparse

        extracted_links = []
        base_domain = urlparse(source_url).netloc

        # Find all links in the page
        links = soup.find_all('a', href=True)

        for link in links:
            try:
                href = link.get('href', '').strip()
                if not href or href.startswith('#'):  # Skip empty or anchor links
                    continue

                # Convert relative URLs to absolute
                absolute_url = urljoin(source_url, href)
                parsed_url = urlparse(absolute_url)

                # Skip invalid URLs
                if not parsed_url.scheme or parsed_url.scheme not in ['http', 'https']:
                    continue

                # Get link text and clean it
                link_text = link.get_text(strip=True)[:200]  # Limit text length

                # Get title attribute if available
                title_attr = link.get('title', '').strip()[:200]

                # Determine link type
                link_type = self._categorize_link(absolute_url, base_domain)

                # Get surrounding context (parent element text)
                context = ""
                parent = link.parent
                if parent:
                    context = parent.get_text(strip=True)[:300]  # Limit context length

                link_info = {
                    "url": absolute_url,
                    "text": link_text,
                    "title": title_attr,
                    "type": link_type,
                    "context": context,
                    "domain": parsed_url.netloc
                }

                extracted_links.append(link_info)

            except Exception as e:
                # Skip problematic links
                continue

        # Remove duplicates based on URL
        seen_urls = set()
        unique_links = []
        for link in extracted_links:
            if link["url"] not in seen_urls:
                seen_urls.add(link["url"])
                unique_links.append(link)

        # Sort by link type and text relevance
        unique_links.sort(key=lambda x: (x["type"] != "internal", len(x["text"]) == 0))

        # Get max links limit from config
        max_links = self._app_config.get("tree_search.max_links_per_page") or 100

        return unique_links[:max_links]  # Limit to configured max links per page

    def _extract_images_from_page(self, soup: BeautifulSoup, source_url: str) -> List[Dict[str, str]]:
        """Extract images from the page with metadata - EXACT COPY from original"""
        from urllib.parse import urljoin, urlparse

        extracted_images = []
        base_domain = urlparse(source_url).netloc

        # Find all img tags in the page
        images = soup.find_all('img')

        for img in images:
            try:
                src = img.get('src', '').strip()
                if not src:
                    # Check for data-src (lazy loading)
                    src = img.get('data-src', '').strip()

                if not src:
                    continue

                # Convert relative URLs to absolute
                absolute_url = urljoin(source_url, src)
                parsed_url = urlparse(absolute_url)

                # Skip invalid URLs and data URLs
                if not parsed_url.scheme or parsed_url.scheme not in ['http', 'https']:
                    continue

                # Skip very small images (likely icons/tracking pixels)
                width = img.get('width', '')
                height = img.get('height', '')
                if width and height:
                    try:
                        w, h = int(str(width).replace('px', '')), int(str(height).replace('px', ''))
                        if w < 50 and h < 50:
                            continue
                    except (ValueError, TypeError):
                        pass

                # Get image metadata
                alt_text = img.get('alt', '').strip()[:500]
                title_attr = img.get('title', '').strip()[:200]

                # Get surrounding context (parent element text)
                context = ""
                parent = img.parent
                if parent:
                    context = parent.get_text(strip=True)[:300]

                image_info = {
                    "src": absolute_url,
                    "alt": alt_text,
                    "title": title_attr,
                    "width": width,
                    "height": height,
                    "context": context,
                    "domain": parsed_url.netloc
                }

                extracted_images.append(image_info)

            except Exception as e:
                # Skip problematic images
                continue

        # Remove duplicates based on src URL
        seen_urls = set()
        unique_images = []
        for img in extracted_images:
            if img["src"] not in seen_urls:
                seen_urls.add(img["src"])
                unique_images.append(img)

        # Sort by relevance (images with alt text first, then by size)
        unique_images.sort(key=lambda x: (len(x["alt"]) == 0, x["width"] == "", x["height"] == ""))

        # Get max images limit from config
        max_images = self._app_config.get("tree_search.max_images_per_page") or 50

        return unique_images[:max_images]  # Limit to configured max images per page

    def _categorize_link(self, url: str, base_domain: str) -> str:
        """Categorize a link as internal, external, email, or file - EXACT COPY from original"""
        from urllib.parse import urlparse

        parsed = urlparse(url)

        # Email links
        if url.startswith('mailto:'):
            return "email"

        # File links (based on extension)
        file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar']
        if any(url.lower().endswith(ext) for ext in file_extensions):
            return "file"

        # Internal vs external
        if parsed.netloc == base_domain:
            return "internal"
        elif parsed.netloc:
            return "external"
        else:
            return "internal"  # Relative link

    def _create_text_chunk_sample(self, full_text: str, chunk_size: int) -> str:
        """Create a representative sample from the full text - EXACT COPY from original"""
        if len(full_text) <= chunk_size:
            return full_text

        # For longer texts, create a strategic sample
        if len(full_text) <= chunk_size * 2:
            # Take from beginning and end
            half_chunk = chunk_size // 2
            return full_text[:half_chunk] + "..." + full_text[-half_chunk:]

        # For very long texts, sample from beginning, middle, and end
        third_chunk = chunk_size // 3
        middle_start = len(full_text) // 2 - third_chunk // 2

        beginning = full_text[:third_chunk]
        middle = full_text[middle_start:middle_start + third_chunk]
        end = full_text[-third_chunk:]

        return f"{beginning}...{middle}...{end}"

    def _get_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication"""
        import hashlib
        return hashlib.md5(content.encode('utf-8', errors='ignore')).hexdigest()

    def _is_duplicate(self, content_hash: str) -> bool:
        """Check if content hash already exists"""
        if content_hash in self.seen_content_hashes:
            return True
        self.seen_content_hashes.add(content_hash)
        return False

    def _should_filter_content(self, content: str, url: str) -> tuple[bool, str]:
        """
        Check if content should be filtered (binary files, media).
        Uses shared filtering logic from utils.merge_content for consistency.

        Note: Binary PDFs are detected and filtered here - they're logged to pdf_references.jsonl
        instead of storing the binary content in text extraction files.

        Returns: (should_filter: bool, reason: str)
        """
        # Check for binary PDF content first (before other checks)
        # Binary PDFs start with %PDF header - detect both string and bytes versions
        if content:
            content_start = content[:4] if isinstance(content, str) else content[:4].decode('utf-8', errors='ignore')
            if content_start.startswith('%PDF'):
                # Calculate size for logging
                size_mb = len(content) / (1024 * 1024) if content else 0.0
                self._log_pdf_reference(
                    url=url,
                    size_mb=size_mb,
                    source_article=url,
                    failure_reason="binary_pdf_content"
                )
                return (True, "binary_pdf_filtered")

        # Media file extensions to filter
        media_extensions = (
            '.mp3', '.mp4', '.wav', '.avi', '.mov', '.wmv', '.flv', '.m4a', '.aac',
            '.ogg', '.webm', '.mkv', '.flac', '.wma', '.m4v', '.3gp', '.mpg', '.mpeg',
            '.zip', '.rar', '.tar', '.gz', '.7z', '.exe', '.dmg', '.iso', '.bin'
        )

        # Check for media files by URL extension
        url_lower = url.lower()
        if any(url_lower.endswith(ext) for ext in media_extensions):
            return (True, f"media_file_{url_lower.split('.')[-1]}")

        # Simple binary content validation
        # Check if content looks like text (has printable characters)
        if content:
            try:
                # Try to count printable vs non-printable characters
                printable_count = sum(1 for c in content[:1000] if c.isprintable() or c.isspace())
                if printable_count < 500:  # Less than 50% printable in first 1000 chars
                    return (True, "binary_content_detected")
            except:
                return (True, "content_validation_error")

        return (False, "")

    def _store_extracted_text(self, extraction_data: Dict[str, Any]) -> None:
        """Store extracted text data to flat file - EXACT COPY from original"""
        try:
            with open(self.extracted_text_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(extraction_data, ensure_ascii=False) + '\n')
        except Exception as e:
            self._logger.error(f"⚠️ Failed to store extraction data: {e}")

    def _log_failed_url(self, url: str, error_type: str, details: str) -> None:
        """Store failed URL to flat file - EXACT COPY from original"""
        try:
            failed_data = {
                "url": url,
                "error_type": error_type,
                "details": details,
                "timestamp": datetime.now().isoformat(),
                "source_query": getattr(self, 'current_query', 'unknown')
            }

            with open(self.failed_urls_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(failed_data, ensure_ascii=False) + '\n')
        except Exception as e:
            self._logger.error(f"⚠️ Failed to log failed URL: {e}")

    def _log_pdf_reference(self, url: str, size_mb: float, source_article: str = None, failure_reason: str = None) -> None:
        """Store PDF reference for later processing or audit trail.

        Args:
            url: PDF URL
            size_mb: PDF size in megabytes (0.0 if download failed)
            source_article: Optional source article URL that contained this PDF link
            failure_reason: Optional failure reason (e.g., "HTTP 403") for blocked PDFs
        """
        try:
            pdf_entry = {
                "pdf_url": url,
                "size_mb": round(size_mb, 2),
                "source_article": source_article,
                "status": "failed_download" if failure_reason else "deferred_large_pdf",
                "failure_reason": failure_reason,
                "source_query": getattr(self, 'current_query', 'unknown'),
                "timestamp": datetime.now().isoformat()
            }

            with open(self.pdf_references_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(pdf_entry, ensure_ascii=False) + '\n')

            status_msg = f"({failure_reason})" if failure_reason else f"({size_mb:.2f}MB)"
            self._logger.debug(f"📄 PDF reference logged: {url} {status_msg}")
        except Exception as e:
            self._logger.error(f"⚠️ Failed to log PDF reference {url}: {e}")


    def _reconcile_extraction_files(self) -> Dict[str, Any]:
        """
        Reconcile extraction files to verify no information loss.

        Validates: text_records = markdown_records + pdf_references + markdown_failures

        Returns:
            Dict with reconciliation status and details
        """
        try:
            from pathlib import Path

            def load_urls_from_file(filepath: Path, field_names: List[str]) -> set:
                """Load URLs from JSONL file, trying multiple field names."""
                urls = set()
                if not filepath.exists():
                    return urls

                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            rec = json.loads(line)
                            for field in field_names:
                                url = rec.get(field)
                                if url:
                                    urls.add(url)
                                    break
                        except json.JSONDecodeError:
                            continue
                return urls

            def count_records(filepath: Path) -> int:
                """Count records in JSONL file."""
                if not filepath.exists():
                    return 0
                with open(filepath, 'r', encoding='utf-8') as f:
                    return sum(1 for _ in f)

            # Compute file paths directly (same as used during extraction)
            extracted_markdown_file = os.path.join(self.storage_dir, "extracted_markdown.jsonl")
            extracted_linked_text_file = os.path.join(self.storage_dir, "extracted_linked_text.jsonl")
            extracted_linked_markdown_file = os.path.join(self.storage_dir, "extracted_linked_markdown.jsonl")

            # Count records
            text_count = count_records(Path(self.extracted_text_file))
            md_count = count_records(Path(extracted_markdown_file))
            pdf_count = count_records(Path(self.pdf_references_file))
            linked_text_count = count_records(Path(extracted_linked_text_file))
            linked_md_count = count_records(Path(extracted_linked_markdown_file))
            failed_count = count_records(Path(self.failed_urls_file))

            # Load URLs
            text_urls = load_urls_from_file(Path(self.extracted_text_file), ['url'])
            md_urls = load_urls_from_file(Path(extracted_markdown_file), ['source_url', 'url'])
            pdf_urls = load_urls_from_file(Path(self.pdf_references_file), ['pdf_url', 'url'])
            failed_urls = load_urls_from_file(Path(self.failed_urls_file), ['url'])

            linked_text_urls = load_urls_from_file(Path(extracted_linked_text_file), ['url'])
            linked_md_urls = load_urls_from_file(Path(extracted_linked_markdown_file), ['source_url', 'url'])

            # Calculate accounting
            main_in_md = md_urls & text_urls
            main_in_pdf = pdf_urls & text_urls
            main_unaccounted = text_urls - md_urls - pdf_urls
            main_unaccounted_in_failed = main_unaccounted & failed_urls

            linked_in_md = linked_md_urls & linked_text_urls
            linked_in_pdf = pdf_urls & linked_text_urls
            linked_unaccounted = linked_text_urls - linked_md_urls - pdf_urls
            linked_unaccounted_in_failed = linked_unaccounted & failed_urls

            # Check consistency
            main_accounted = len(main_in_md) + len(main_in_pdf) + len(main_unaccounted_in_failed)
            main_consistent = main_accounted == len(text_urls)

            linked_accounted = len(linked_in_md) + len(linked_in_pdf) + len(linked_unaccounted_in_failed)
            linked_consistent = linked_accounted == len(linked_text_urls)

            overall_consistent = main_consistent and linked_consistent

            # Build report
            report = {
                'consistent': overall_consistent,
                'main': {
                    'text_count': text_count,
                    'markdown_count': md_count,
                    'pdf_count': pdf_count,
                    'in_markdown': len(main_in_md),
                    'in_pdf': len(main_in_pdf),
                    'in_failed': len(main_unaccounted_in_failed),
                    'unaccounted': len(main_unaccounted - main_unaccounted_in_failed),
                    'consistent': main_consistent
                },
                'linked': {
                    'text_count': linked_text_count,
                    'markdown_count': linked_md_count,
                    'pdf_count': pdf_count,  # Shared
                    'in_markdown': len(linked_in_md),
                    'in_pdf': len(linked_in_pdf),
                    'in_failed': len(linked_unaccounted_in_failed),
                    'unaccounted': len(linked_unaccounted - linked_unaccounted_in_failed),
                    'consistent': linked_consistent
                },
                'failed_count': failed_count
            }

            # Log results
            self._logger.info("=" * 80)
            self._logger.info("📊 EXTRACTION RECONCILIATION")
            self._logger.info("=" * 80)

            self._logger.info(f"MAIN EXTRACTION:")
            self._logger.info(f"  Text records:           {text_count}")
            self._logger.info(f"  Markdown records:       {md_count}")
            self._logger.info(f"  PDF references:         {pdf_count}")
            self._logger.info(f"  Accounted in markdown:  {len(main_in_md)}")
            self._logger.info(f"  Accounted in PDFs:      {len(main_in_pdf)}")
            self._logger.info(f"  Accounted in failed:    {len(main_unaccounted_in_failed)}")

            if main_consistent:
                self._logger.info(f"  Status:                 ✅ CONSISTENT ({main_accounted}/{len(text_urls)})")
            else:
                self._logger.warning(f"  Status:                 ❌ INCONSISTENT ({main_accounted}/{len(text_urls)})")
                if report['main']['unaccounted'] > 0:
                    self._logger.warning(f"  Unaccounted URLs:       {report['main']['unaccounted']}")

            self._logger.info("")
            self._logger.info(f"LINKED EXTRACTION:")
            self._logger.info(f"  Text records:           {linked_text_count}")
            self._logger.info(f"  Markdown records:       {linked_md_count}")
            self._logger.info(f"  PDF references:         {pdf_count} (shared)")
            self._logger.info(f"  Accounted in markdown:  {len(linked_in_md)}")
            self._logger.info(f"  Accounted in PDFs:      {len(linked_in_pdf)}")
            self._logger.info(f"  Accounted in failed:    {len(linked_unaccounted_in_failed)}")

            if linked_consistent:
                self._logger.info(f"  Status:                 ✅ CONSISTENT ({linked_accounted}/{len(linked_text_urls)})")
            else:
                self._logger.warning(f"  Status:                 ❌ INCONSISTENT ({linked_accounted}/{len(linked_text_urls)})")
                if report['linked']['unaccounted'] > 0:
                    self._logger.warning(f"  Unaccounted URLs:       {report['linked']['unaccounted']}")

            self._logger.info("")
            self._logger.info(f"SUMMARY:")
            self._logger.info(f"  Failed URLs:            {failed_count}")
            self._logger.info(f"  Formula: text = markdown + pdfs + failures")

            if overall_consistent:
                self._logger.info(f"  ✅ ALL RECORDS RECONCILED - No information loss!")
            else:
                self._logger.warning(f"  ❌ DISCREPANCIES FOUND - Please investigate")

            self._logger.info("=" * 80)

            return report

        except Exception as e:
            self._logger.error(f"⚠️ Failed to reconcile extraction files: {e}")
            return {'consistent': False, 'error': str(e)}

    async def _generate_semantic_queries(self, root_query: str, text_content: str, max_queries: int) -> List[str]:
        """Generate semantic query variations using LLM - EXACT COPY from original"""
        if not self.use_llm or not text_content:
            return self._generate_keyword_queries(root_query, max_queries)

        return await self._generate_llm_queries(root_query, text_content, max_queries)

    async def _generate_llm_queries(self, root_query: str, text_content: str, max_queries: int) -> List[str]:
        """Generate queries using LLM - EXACT COPY from original"""
        if not self.llm_client:
            return []

        try:
            self.stats.llm_calls_made += 1
            queries = await self.llm_client.generate_queries(root_query, text_content, max_queries)
            return queries[:max_queries]  # Ensure we don't exceed the limit
        except Exception as e:
            self._logger.warning(f"⚠️ LLM query generation failed: {e}")
            # Fallback to keyword-based generation
            return self._generate_keyword_queries(root_query, max_queries)

    def _generate_keyword_queries(self, root_query: str, max_queries: int) -> List[str]:
        """Generate query variations using keyword manipulation - EXACT COPY from original"""
        variations = []

        # Split the root query into words
        words = root_query.lower().split()

        if len(words) == 1:
            # Single word - generate related searches
            base_word = words[0]
            variations = [
                f"{base_word} overview",
                f"{base_word} definition",
                f"{base_word} examples",
                f"{base_word} tutorial",
                f"what is {base_word}",
                f"{base_word} guide",
                f"{base_word} introduction",
                f"{base_word} explained"
            ]
        else:
            # Multi-word - rearrange and modify
            variations = [
                " ".join(words[1:] + [words[0]]),  # Rotate words
                " ".join(words[::-1]),  # Reverse order
                f"what is {root_query}",
                f"{root_query} guide",
                f"{root_query} tutorial",
                f"{root_query} overview",
                f"{root_query} examples",
                f"introduction to {root_query}"
            ]

        return variations[:max_queries]

    def _log_growth(self, stage: str, query: str, total_results: int, growth: int):
        """Log growth metrics for analysis - EXACT COPY from original"""
        growth_entry = {
            "stage": stage,
            "query": query,
            "total_results": total_results,
            "growth": growth,
            "timestamp": datetime.now().isoformat()
        }
        self.stats.growth_per_query.append(growth_entry)

    def _compile_true_tree_results(self, tree_nodes: Dict[int, List], all_urls: Set[str], duration: float, growth_metrics: List[Dict[str, Any]], early_stopping_info: Dict[str, Any] = None) -> Dict[str, Any]:
        """Compile results for true tree search algorithm with growth metrics and early stopping info"""

        # Flatten tree nodes for statistics
        all_nodes = []
        for depth, nodes in tree_nodes.items():
            all_nodes.extend(nodes)

        # Calculate overall growth statistics
        total_growth = sum(metric['total_growth'] for metric in growth_metrics)
        total_queries = sum(metric['queries_count'] for metric in growth_metrics)
        overall_avg_growth = total_growth / total_queries if total_queries > 0 else 0

        # Update global statistics
        self.stats.total_unique_urls = len(all_urls)
        self.stats.search_duration = duration
        self.stats.total_queries_executed = len(all_nodes)
        self.stats.max_depth_reached = max(tree_nodes.keys()) if tree_nodes else 0

        return {
            "search_metadata": {
                "algorithm": "true_tree_search",
                "total_unique_urls": len(all_urls),
                "search_duration": duration,
                "timestamp": datetime.now().isoformat(),
                "llm_enabled": self.use_llm
            },
            "discovered_urls": list(all_urls),
            "tree_structure": {
                f"depth_{depth}": [
                    {
                        "query": node.query,
                        "depth": node.depth,
                        "parent_query": node.parent_query,
                        "urls_found": len(node.urls),
                        "growth": getattr(node, 'growth', 0),
                        "generated_queries": getattr(node, 'generated_queries', []),
                        "content_length": len(node.text_content) if node.text_content else 0,
                        "timestamp": node.timestamp
                    }
                    for node in nodes
                ]
                for depth, nodes in tree_nodes.items()
            },
            "growth_analysis": {
                "total_growth": total_growth,
                "overall_avg_growth_per_query": overall_avg_growth,
                "depth_metrics": growth_metrics,
                "growth_trend": [metric['avg_growth_per_query'] for metric in growth_metrics],
                "early_stopping": early_stopping_info or {
                    'criterion': 'none',
                    'threshold': None,
                    'triggered': False
                }
            },
            "statistics": {
                "total_queries_executed": self.stats.total_queries_executed,
                "total_unique_urls": self.stats.total_unique_urls,
                "max_depth_reached": self.stats.max_depth_reached,
                "search_duration": self.stats.search_duration,
                "llm_calls_made": self.stats.llm_calls_made,
                "total_growth": total_growth,
                "overall_avg_growth_per_query": overall_avg_growth
            }
        }

    def _collect_all_linked_urls(self, tree_nodes: Dict[int, List]) -> List[str]:
        """Collect all linked URLs from all nodes in the tree structure"""
        all_linked_urls = set()

        for depth, nodes in tree_nodes.items():
            for node in nodes:
                if hasattr(node, 'extracted_links') and node.extracted_links:
                    # Extract URLs from the extracted_links structure
                    for source_url, links in node.extracted_links.items():
                        for link in links:
                            if 'url' in link:
                                all_linked_urls.add(link['url'])

        self._logger.info(f"🔗 Collected {len(all_linked_urls)} unique linked URLs from tree structure")
        return list(all_linked_urls)

    def _compile_results(self, duration: float) -> Dict[str, Any]:
        """Compile final results with comprehensive statistics - EXACT COPY from original"""

        self.stats.total_unique_urls = len(self.discovered_urls)
        self.stats.search_duration = duration

        # Update calculated markdown success rate
        self.stats.update_markdown_success_rate()

        return {
            "search_metadata": {
                "algorithm": "tree_search",
                "total_unique_urls": len(self.discovered_urls),
                "search_urls_count": len(self.search_urls),
                "extracted_urls_count": len(self.extracted_urls),
                "search_duration": duration,
                "timestamp": datetime.now().isoformat(),
                "llm_enabled": self.use_llm
            },
            "search_urls": list(self.search_urls),
            "extracted_urls": list(self.extracted_urls),
            "discovered_urls": list(self.discovered_urls),
            "search_tree": [
                {
                    "query": node.query,
                    "depth": node.depth,
                    "parent_query": node.parent_query,
                    "urls_found": len(node.urls),
                    "growth": node.growth,
                    "generated_queries": node.generated_queries,
                    "extracted_links_count": sum(len(links) for links in node.extracted_links.values()),
                    "extracted_links_by_source": {url: len(links) for url, links in node.extracted_links.items()},
                    "timestamp": node.timestamp
                }
                for node in self.search_tree
            ],
            "statistics": {
                "total_queries_executed": self.stats.total_queries_executed,
                "total_unique_urls": self.stats.total_unique_urls,
                "max_depth_reached": self.stats.max_depth_reached,
                "total_text_extracted": self.stats.total_text_extracted,
                "llm_calls_made": self.stats.llm_calls_made,
                "search_duration": self.stats.search_duration,
                "growth_per_query": self.stats.growth_per_query,
                "search_urls_count": len(self.search_urls),
                "extracted_urls_count": len(self.extracted_urls),
                "markdown_extraction": {
                    "total_attempts": self.stats.total_markdown_attempts + self.stats.total_markdown_linked_attempts,
                    "total_successes": self.stats.total_markdown_successes + self.stats.total_markdown_linked_successes,
                    "total_failures": self.stats.total_markdown_failures,
                    "success_rate": self.stats.markdown_success_rate,
                    "processing_time": self.stats.markdown_processing_time,
                    "content_size_mb": round(self.stats.markdown_content_size / (1024 * 1024), 2),
                    "search_results": {
                        "attempts": self.stats.total_markdown_attempts,
                        "successes": self.stats.total_markdown_successes
                    },
                    "linked_content": {
                        "attempts": self.stats.total_markdown_linked_attempts,
                        "successes": self.stats.total_markdown_linked_successes
                    }
                }
            }
        }

    def _merge_extracted_results(self) -> None:
        """Merge main and linked extracted JSONL files for markdown and text.

        Uses the reusable merge_content utility to create:
        - merged_content.jsonl (text files, deduped by URL)
        - merged_markdown.jsonl (markdown files, deduped by URL)
        """
        try:
            # Import the merge utility
            import logging

            from utils.merge_content import (merge_markdown_files,
                                             merge_text_files)

            # Temporarily suppress detailed logging from merge utility
            merge_logger = logging.getLogger('__main__')
            original_level = merge_logger.level
            merge_logger.setLevel(logging.WARNING)

            self._logger.info("🔀 Merging extracted files...")

            # Merge text files
            text_stats = merge_text_files(
                self.storage_dir,
                output_filename="merged_content.jsonl",
                input_files=["extracted_text.jsonl", "extracted_linked_text.jsonl"],
                deduplicate_by_url=True
            )

            self._logger.info(
                f"✅ Text: {text_stats['total_records']} records "
                f"({text_stats.get('duplicates_skipped', 0)} duplicates skipped) "
                f"→ merged_content.jsonl"
            )

            # Merge markdown files (if markdown extraction was enabled)
            if self.extract_markdown:
                md_stats = merge_markdown_files(
                    self.storage_dir,
                    output_filename="merged_markdown.jsonl",
                    input_files=["extracted_markdown.jsonl", "extracted_linked_markdown.jsonl"],
                    deduplicate_by_url=True
                )

                self._logger.info(
                    f"✅ Markdown: {md_stats['total_records']} records "
                    f"({md_stats.get('duplicates_skipped', 0)} duplicates skipped) "
                    f"→ merged_markdown.jsonl"
                )
            else:
                self._logger.info("⚪ Markdown merge skipped (extraction was disabled)")

            # Restore logger level
            merge_logger.setLevel(original_level)

        except Exception as e:
            # Non-fatal - log and continue
            self._logger.warning(f"⚠️ Error during merge_extracted_results: {e}")

    async def _extract_linked_content(self, linked_urls: List[str]) -> None:
        """
        Final pipeline step: Extract content from all discovered linked URLs - EXACT COPY from original
        and save to extracted_linked_text.jsonl
        """
        if not linked_urls:
            return

        linked_file = os.path.join(self.storage_dir, "extracted_linked_text.jsonl")
        successful_extractions = 0
        failed_extractions = 0
        html_extractions = []  # Store HTML-to-markdown extractions
        html_batch = []  # Batch HTML content for parallel processing

        # Structured logging for linked content processing
        self._logger.info(f"� Processing {len(linked_urls)} linked URLs for content extraction...")
        self._logger.info(f"💾 Saving text content to: {linked_file}")
        if self.extract_markdown:
            self._logger.info("📝 HTML-to-markdown conversion enabled for linked content")

        # Process URLs in batches to avoid overwhelming servers
        batch_size = self.linked_batch_size
        total_batches = (len(linked_urls) + batch_size - 1) // batch_size
        self._logger.info(f"📦 Processing in {total_batches} batches of {batch_size} URLs each")
        for i in range(0, len(linked_urls), batch_size):
            batch = linked_urls[i:i + batch_size]
            current_batch = i // batch_size + 1
            batch_prefix = f"Batch ({current_batch}/{total_batches})"

            self._logger.info(f"{batch_prefix} 🔄 Processing {len(batch)} linked URLs...")

            # Fetch all URLs in batch concurrently
            self._logger.debug(f"{batch_prefix} 🚀 Fetching {len(batch)} URLs concurrently...")
            fetch_tasks = [self._fetch_url_content(url, j) for j, url in enumerate(batch)]
            fetch_results = await asyncio.gather(*fetch_tasks)

            # Process fetched content
            for j, url, response_text, status_code in fetch_results:
                # Skip if already processed in main extraction
                if any(url in node.urls for node in self.search_tree):
                    continue

                if not response_text or status_code != 200:
                    failed_extractions += 1
                    self._logger.debug(f"{batch_prefix} ❌ HTTP {status_code} - {url[:50]}...")

                    # Check if this is a PDF URL (by extension) and log to pdf_references even on failure
                    # This ensures we have an audit trail of all PDFs, including blocked ones
                    if url.lower().endswith('.pdf'):
                        self._logger.debug(f"{batch_prefix} 📄 Logging failed PDF to pdf_references.jsonl - {url[:60]}...")
                        self._log_pdf_reference(
                            url=url,
                            size_mb=0.0,  # Unknown size for failed download
                            source_article=url,
                            failure_reason=f"HTTP {status_code}"
                        )

                    self._log_failed_url(url, f"http_{status_code}", f"Status: {status_code}")
                    continue

                try:
                    # Check if content should be filtered (media files, binary content)
                    # Note: PDFs are NOT filtered here - they're logged during markdown batching
                    should_filter, filter_reason = self._should_filter_content(response_text, url)
                    if should_filter:
                        self._logger.debug(f"{batch_prefix} 🚫 Filtered - {filter_reason} - {url[:50]}...")
                        self._log_failed_url(url, "filtered", filter_reason)
                        failed_extractions += 1
                        continue

                    soup = BeautifulSoup(response_text, 'html.parser')

                    # Extract comprehensive page metadata (title, dates, author, etc.)
                    page_metadata = self.searcher._extract_page_metadata(soup)
                    title = page_metadata.get('title', 'No title')

                    # BEFORE removing iframes, check for embedded PDF documents
                    iframes = soup.find_all('iframe')
                    for iframe in iframes:
                        iframe_src = iframe.get('src', '')
                        if iframe_src:
                            # Detect PDF URLs in iframes (common patterns)
                            is_pdf_iframe = (
                                '.pdf' in iframe_src.lower() or
                                'type=pdf' in iframe_src.lower() or
                                't=pdf' in iframe_src.lower() or
                                '/pdf' in iframe_src.lower()
                            )

                            if is_pdf_iframe:
                                self._logger.debug(f"{batch_prefix} 📄 Detected iframe PDF: {iframe_src[:80]}...")
                                self._log_pdf_reference(
                                    url=iframe_src,
                                    size_mb=0.0,  # Size unknown for iframe PDFs
                                    source_article=url,
                                    failure_reason="iframe_embedded_pdf"
                                )

                    # Remove scripts, styles, and navigation elements
                    for element in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe"]):
                        element.decompose()

                    # Get main content using multiple strategies
                    text_content = ""
                    content_selectors = [
                        'main', 'article', '.content', '#content',
                        '.post-content', '.entry-content', '.article-body',
                        '.post-body', '.story-body', '[role="main"]'
                    ]

                    for selector in content_selectors:
                        element = soup.select_one(selector)
                        if element:
                            text_content = element.get_text()
                            break

                    # Fallback to body content
                    if not text_content:
                        body = soup.find('body')
                        text_content = body.get_text() if body else soup.get_text()

                    # Clean and structure the text
                    clean_text = ' '.join(text_content.split())

                    # Check for duplicate content
                    content_hash = self._get_content_hash(clean_text)
                    if self._is_duplicate(content_hash):
                        self._logger.debug(f"{batch_prefix} 🔄 Duplicate content detected, skipping - {url[:50]}...")
                        continue

                    # Extract links from the page
                    extracted_links = self._extract_links_from_page(soup, url)

                    # Extract images from the page
                    extracted_images = self._extract_images_from_page(soup, url)

                    # Create text chunk sample
                    text_chunk = self._create_text_chunk_sample(clean_text, self.text_chunk_size)

                    # Store linked content data with full content and metadata
                    linked_data = {
                        "id": self.extraction_counter,
                        "url": url,
                        "title": title,
                        "content": clean_text,  # Store FULL content (no character limit)
                        "chunk_sample": text_chunk,
                        "full_length": len(clean_text),
                        "extracted_links": extracted_links,
                        "link_count": len(extracted_links),
                        "extracted_images": extracted_images,
                        "image_count": len(extracted_images),
                        "timestamp": datetime.now().isoformat(),
                        "source_type": "linked_url",
                        "page_type": "general",
                        # Add comprehensive metadata
                        "published_date": page_metadata.get('published_date'),
                        "modified_date": page_metadata.get('modified_date'),
                        "author": page_metadata.get('author'),
                        "description": page_metadata.get('description'),
                        "keywords": page_metadata.get('keywords'),
                        "language": page_metadata.get('language'),
                        "article_type": page_metadata.get('article_type'),
                        "site_name": page_metadata.get('site_name'),
                        "publisher": page_metadata.get('publisher'),
                        "section": page_metadata.get('section')
                    }

                    # Save to linked content file
                    with open(linked_file, 'a', encoding='utf-8') as f:
                        f.write(json.dumps(linked_data, ensure_ascii=False) + '\n')

                    # Collect HTML for batch parallel markdown conversion using the same ID as text extraction
                    # Skip PDFs to avoid blocking worker pool with pipeline initialization
                    # Log ALL PDFs to pdf_references.jsonl for consistency and audit trail
                    if self.extract_markdown:
                        is_pdf = response_text.strip().startswith('%PDF')
                        if not is_pdf:
                            html_batch.append((url, response_text, linked_data["id"]))
                        else:
                            # Log PDF to references file (all PDFs, not just oversized)
                            size_mb = len(response_text.encode('utf-8', errors='ignore')) / (1024 * 1024)
                            self._log_pdf_reference(url, size_mb, source_article=url)
                            self._logger.debug(f"⏭️  Skipping PDF from markdown batch: {url[:60]} ({size_mb:.2f}MB)")

                    self.extraction_counter += 1
                    successful_extractions += 1

                except Exception as e:
                    failed_extractions += 1
                    self._log_failed_url(url, "extraction_error", str(e)[:200])
                    continue

            # Process HTML batch for this batch in parallel using docling
            if html_batch and self.extract_markdown:
                import time
                batch_html_count = len(html_batch)
                self._logger.info(f"{batch_prefix} 🔄 Converting {batch_html_count} documents to markdown...")
                batch_start_time = time.time()

                successful_md, failed_md = self.searcher.convert_html_batch_parallel(
                    urls_and_html=html_batch,
                    storage_dir=self.storage_dir,
                    num_workers=None  # Auto-detect CPU count
                )

                # Save successful conversions to JSONL progressively
                if successful_md:
                    linked_markdown_file = os.path.join(self.storage_dir, "extracted_linked_markdown.jsonl")
                    with open(linked_markdown_file, 'a', encoding='utf-8') as f:
                        for record in successful_md:
                            f.write(json.dumps(record, ensure_ascii=False) + '\n')
                    html_extractions.extend(successful_md)

                    # Update stats
                    self.stats.total_markdown_linked_attempts += batch_html_count
                    self.stats.total_markdown_linked_successes += len(successful_md)
                    for record in successful_md:
                        if 'text' in record:
                            self.stats.markdown_content_size += len(record['text'])

                    elapsed = time.time() - batch_start_time
                    self._logger.info(f"{batch_prefix} ✅ Markdown: {len(successful_md)} successful, {len(failed_md)} failed in {elapsed:.2f}s")

                # Log failed conversions
                if failed_md:
                    self.stats.total_markdown_failures += len(failed_md)
                    for failure in failed_md:
                        error_msg = failure.get('error_message', failure.get('error', 'Unknown error'))
                        self._logger.warning(f"{batch_prefix} ⚠️ Markdown conversion failed for {failure['url'][:60]}: {error_msg[:80]}")
                        self._log_failed_url(failure['url'], "markdown_conversion_error", error_msg[:200])

                # Track processing time
                self.stats.markdown_processing_time += time.time() - batch_start_time

                # Clear html_batch for next batch
                html_batch = []

            # Batch summary
            batch_successful = successful_extractions - (i // batch_size * 10)  # Approximate
            self._logger.info(f"{batch_prefix} ✅ Complete - Extracted: {successful_extractions} total, Failed: {failed_extractions} total")

            # Longer delay between batches
            if i + batch_size < len(linked_urls):
                self._logger.debug(f"{batch_prefix} ⏳ Waiting 3 seconds before next batch...")
                await asyncio.sleep(3)

        # Final summary - no more batch processing needed since we did it progressively
        if self.extract_markdown and html_extractions:
            self._logger.info(f"📝 Total linked markdown extractions saved progressively: {len(html_extractions)}")

        self._logger.info("🎯 LINKED CONTENT EXTRACTION COMPLETE:")
        self._logger.info(f"✅ Successfully extracted: {successful_extractions} pages")
        self._logger.info(f"❌ Failed extractions: {failed_extractions} pages")
        self._logger.info(f"📁 Content saved to: {linked_file}")
        if html_extractions:
            self._logger.info(f"📝 Markdown saved to: extracted_linked_markdown.jsonl ({len(html_extractions)} documents)")

def parse_query_tree_from_log(log_file_path: str = None):
    """Parse tree search log and display hierarchical query structure"""
    import re
    from collections import defaultdict

    def count_words(text):
        """Count words in a text string"""
        return len(text.split())

    # Default to logs.logfile if not specified
    if log_file_path is None:
        workspace_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_file_path = os.path.join(workspace_root, "logs", "logs.logfile")

    # Read the log file
    with open(log_file_path, 'r') as f:
        log_content = f.read()

    # Extract key information
    queries_by_depth = defaultdict(list)

    # Parse initial query collection (Depth 0)
    root_match = re.search(r"Root Query: '([^']+)'", log_content)
    if root_match:
        root_query = root_match.group(1)
        print("=" * 80)
        print("TREE SEARCH STRUCTURE ANALYSIS")
        print("=" * 80)
        print(f"\n📌 ROOT QUERY: '{root_query}' (words = {count_words(root_query)})")
        print("\n" + "=" * 80)

    # Extract query collection (treated as depth 0)
    collection_queries = re.findall(r"🔍 Query (\d+)/(\d+): '([^']+)'", log_content)
    if collection_queries:
        print("\n🌱 DEPTH 0 - Query Collection (Initial Grounding):")
        print("-" * 80)
        for idx, total, query in collection_queries:
            word_count = count_words(query)
            print(f"  Query {idx}/{total} (words = {word_count}): {query}")
        print(f"\n  Total: {len(collection_queries)} grounding queries")

    # Extract depth 1 expansion
    depth1_match = re.search(r"🔍 Generated 2 expansion queries: \[([^\]]+)\]", log_content)
    if depth1_match:
        queries_str = depth1_match.group(1)
        queries = [q.strip().strip("'\"") for q in queries_str.split("', '")]
        print("\n" + "=" * 80)
        print("🌿 DEPTH 1 - First LLM Expansion:")
        print("-" * 80)
        for i, q in enumerate(queries, 1):
            word_count = count_words(q)
            print(f"  Branch {i}/2 (words = {word_count}): {q}")
        queries_by_depth[1] = queries

    # Get depth 2 queries
    depth2_section = re.search(
        r"STEP 4: Generating next level queries from leftmost branch at depth 1(.*?)"
        r"STEP 4: Generating next level queries from leftmost branch at depth 2",
        log_content,
        re.DOTALL
    )
    if depth2_section:
        depth2_queries = re.findall(r"🌿 Branch (\d+)/(\d+): '([^']+)'", depth2_section.group(1))
        if depth2_queries:
            print("\n" + "=" * 80)
            print("🌳 DEPTH 2 - Second Level Expansion:")
            print("-" * 80)
            for branch_num, total, query in depth2_queries:
                word_count = count_words(query)
                print(f"  Branch {branch_num}/{total} (words = {word_count}): {query}")

    # Get depth 3 queries
    depth3_section = re.search(
        r"STEP 4: Generating next level queries from leftmost branch at depth 2(.*?)"
        r"STEP 4: Generating next level queries from leftmost branch at depth 3",
        log_content,
        re.DOTALL
    )
    if depth3_section:
        depth3_queries = re.findall(r"🌿 Branch (\d+)/(\d+): '([^']+)'", depth3_section.group(1))
        if depth3_queries:
            print("\n" + "=" * 80)
            print("🌲 DEPTH 3 - Third Level Expansion:")
            print("-" * 80)
            for branch_num, total, query in depth3_queries:
                word_count = count_words(query)
                print(f"  Branch {branch_num}/{total} (words = {word_count}): {query}")

    # Get depth 4 summary
    depth4_match = re.search(r"Generated (\d+) total queries for depth 4: \[([^\]]+)\]", log_content)
    if depth4_match:
        num_queries = depth4_match.group(1)
        queries_str = depth4_match.group(2)
        # Split by ', ' but respect quotes
        queries = re.findall(r"'([^']+)'", queries_str)
        print("\n" + "=" * 80)
        print(f"🌴 DEPTH 4 - Fourth Level Expansion ({num_queries} queries):")
        print("-" * 80)
        for i, q in enumerate(queries, 1):
            word_count = count_words(q)
            print(f"  Branch {i}/{num_queries} (words = {word_count}): {q}")

    # Summary statistics
    print("\n" + "=" * 80)
    print("📊 EXECUTION SUMMARY:")
    print("-" * 80)

    # Extract statistics
    stats = {
        'Depth 1': re.search(r"Depth 1 summary: (\d+) total URLs, avg ([\d.]+) URLs/query from (\d+) branches", log_content),
        'Depth 2': re.search(r"Depth 2 summary: (\d+) total URLs, avg ([\d.]+) URLs/query from (\d+) branches", log_content),
        'Depth 3': re.search(r"Depth 3 summary: (\d+) total URLs, avg ([\d.]+) URLs/query from (\d+) branches", log_content),
    }

    for depth_name, match in stats.items():
        if match:
            total_urls, avg_urls, num_branches = match.groups()
            print(f"  {depth_name}: {num_branches} branches → {total_urls} URLs (avg {avg_urls} URLs/branch)")

    # Extract extraction progress
    extraction_lines = re.findall(r"✅ Extracted (\d+) pages so far", log_content)
    if extraction_lines:
        final_count = extraction_lines[-1]
        print(f"\n  Content Extraction: {final_count} pages extracted")

    # Check for failures
    failures = len(re.findall(r"Failed to generate novel queries", log_content))
    jaccard_rejections = len(re.findall(r"Query too similar \(Jaccard=", log_content))
    timeouts = len(re.findall(r"Operation timed out", log_content))

    print(f"\n  LLM Generation Failures: {failures} attempts")
    print(f"  Jaccard Similarity Rejections: {jaccard_rejections} queries")
    print(f"  URL Timeouts: {timeouts} URLs")

    print("\n" + "=" * 80)


async def main():
    """CLI interface for tree search - updated for true tree search."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Tree Search - True Tree Search Algorithm",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single query with GCP Gemini (default)
  %(prog)s "Calvo Mawela Multichoice" --max-depth 2 --branches 2

  # Use local LLM instead of GCP
  %(prog)s "Calvo Mawela Multichoice" --max-depth 2 --branches 2 --llm-provider local

  # Query collection (for richer semantic grounding)
  %(prog)s "Calvo Mawela Multichoice" --related "Canal Plus merger" "African streaming market" --max-depth 2 --branches 2

  # Parse query tree from log
  %(prog)s --parse-query-tree [log_file_path]
        """
    )
    parser.add_argument("query", nargs="?", help="Primary search query to expand")
    parser.add_argument("--related", nargs="*", help="Related queries for semantic grounding context (optional)")
    parser.add_argument("--max-depth", type=int, default=2, help="Maximum tree search depth")
    parser.add_argument("--branches", type=int, default=5, help="Number of branches per node")
    parser.add_argument("--urls-per-query", type=int, default=10, help="URLs to fetch per query")
    parser.add_argument("--min-growth", type=int, default=1, help="Minimum URL growth to continue")
    parser.add_argument("--early-stopping-crit", choices=['avg_growth', 'total_growth', 'none'], default='none', help="Early stopping criterion")
    parser.add_argument("--early-stopping-thresh", type=float, default=0.5, help="Early stopping threshold")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM expansion")
    parser.add_argument("--no-markdown", action="store_true", help="Disable markdown extraction")
    parser.add_argument("--llm-provider", choices=['gcp', 'local'], default='gcp', help="LLM provider to use: 'gcp' (Gemini) or 'local' (SimpleLLM). Default: gcp")
    parser.add_argument("--classic", action="store_true", help="Use classic tree search algorithm")
    parser.add_argument("--parse-query-tree", nargs="?", const=True, help="Parse and display query tree from log file (optional: specify log file path)")

    args = parser.parse_args()

    # Handle log parsing mode
    if args.parse_query_tree:
        try:
            log_path = args.parse_query_tree if isinstance(args.parse_query_tree, str) else None
            parse_query_tree_from_log(log_path)
            return
        except FileNotFoundError as e:
            print(f"❌ Error: Log file not found")
            print(f"   {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error parsing log file: {e}")
            sys.exit(1)

    # Validate that query is provided for search mode
    if not args.query:
        parser.error("the following arguments are required: query (unless using --parse-query-tree)")
        return

    try:
        # Build query collection if related queries provided
        if args.related:
            query_collection = [args.query] + args.related
            print(f"🔍 Using query collection with {len(query_collection)} queries:")
            for i, q in enumerate(query_collection, 1):
                print(f"  {i}. {q}")
        else:
            query_collection = args.query  # Single query (backward compatible)

        # Initialize tree search (use primary query for storage dir naming)
        tree_engine = SimpleTreeSearch(
            args.query,
            use_llm=not args.no_llm,
            extract_markdown=not args.no_markdown,
            llm_provider=args.llm_provider
        )

        if args.classic:
            # Execute classic tree search algorithm (doesn't support query collections yet)
            results = await tree_engine.tree_search(
                root_query=args.query,
                max_depth=args.max_depth,
                max_queries_per_depth=args.branches,
                min_results_per_query=args.min_growth,
                stop_on_no_growth=True
            )
            algorithm_name = "classic_tree_search"
        else:
            # Execute true tree search algorithm (supports query collections)
            results = await tree_engine.true_tree_search(
                root_query=query_collection,  # Pass collection or single query
                max_depth=args.max_depth,
                branches_per_node=args.branches,
                urls_per_query=args.urls_per_query,
                min_url_growth=args.min_growth,
                early_stopping_criterion=args.early_stopping_crit,
                early_stopping_threshold=args.early_stopping_thresh
            )
            algorithm_name = "true_tree_search"

        # Display results
        print(f"\n📊 FINAL RESULTS ({algorithm_name.upper()}):")
        print(f"Total URLs discovered: {results['search_metadata']['total_unique_urls']}")
        print(f"Search duration: {results['search_metadata']['search_duration']:.2f}s")
        print(f"Max depth reached: {results['statistics']['max_depth_reached']}")
        print(f"Total queries executed: {results['statistics']['total_queries_executed']}")

        # Display growth analysis
        if 'growth_analysis' in results:
            growth_analysis = results['growth_analysis']
            print(f"Overall avg growth per query: {growth_analysis['overall_avg_growth_per_query']:.2f}")
            print(f"Growth trend: {' → '.join([f'{x:.1f}' for x in growth_analysis['growth_trend']])}")

            # Display early stopping info
            early_stopping = growth_analysis.get('early_stopping', {})
            if early_stopping.get('triggered'):
                print(f"🚦 Early stopping triggered: {early_stopping['criterion']} <= {early_stopping['threshold']}")
            elif early_stopping.get('criterion') != 'none':
                print(f"🔄 Early stopping not triggered (criterion: {early_stopping['criterion']} > {early_stopping['threshold']})")

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{algorithm_name}_results_{timestamp}.json"
        full_path = os.path.join(tree_engine.storage_dir, filename)

        with open(full_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"📄 Results saved to: {full_path}")

    except KeyboardInterrupt:
        print("\n⚠️  Search interrupted by user")
    except Exception as e:
        print(f"\n❌ Search failed: {e}")


if __name__ == "__main__":
    asyncio.run(main())
