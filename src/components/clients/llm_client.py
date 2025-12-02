#!/usr/bin/env python3
"""
Simple LLM Client for Query Generation

This module provides a client for interacting with an LLM service to generate semantic search queries.
"""
import json
import os
import re
import sys
from typing import List, Optional

import requests

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

try:
    from config_handler import Base
except ImportError:
    # Fallback import path
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from src.config_handler import Base

class SimpleLLMClient(Base):
    """Simple LLM client for semantic query generation and expansion with Base inheritance"""

    def __init__(self, base_url: Optional[str] = None):
        """Initialize LLM client with Base inheritance for config and logging.

        Args:
            base_url: Optional base URL override. If None, uses config value.
        """
        # GIVEN: Base class initialization for centralized config and logging
        super().__init__()
        self._logger.info("🧠 Initializing SimpleLLMClient")

        # WHEN: We set up the LLM client configuration
        self.base_url = base_url or self._app_config.get('llm.base_url')
        self.timeout = self._app_config.get('llm.timeout')
        self.max_length = self._app_config.get('llm.max_length')
        self.temperature = self._app_config.get('llm.temperature')
        self.top_p = self._app_config.get('llm.top_p')
        self.max_retries = self._app_config.get('llm.max_retries')
        self.retry_delay = self._app_config.get('llm.retry_delay')
        self.max_context_chars = self._app_config.get('llm.max_context_chars')

        self.session = requests.Session()

        # THEN: Log the initialization status
        self._logger.info(f"   🌐 LLM service endpoint: {self.base_url}")
        self._logger.debug(f"   ⚙️  Config - timeout: {self.timeout}s, temp: {self.temperature}, top_p: {self.top_p}")
        self._logger.debug(f"   📏 Max context chars: {self.max_context_chars}")
        self._logger.debug("   📡 HTTP session initialized")

        # FAIL FAST: Verify LLM service is reachable on initialization
        self._verify_service_availability()

    def _verify_service_availability(self) -> None:
        """Verify that the LLM service is available and responsive.

        Raises:
            ConnectionError: If the service is not reachable
            RuntimeError: If the service returns an unexpected response
        """
        self._logger.debug(f"   🔍 Verifying LLM service availability at {self.base_url}")

        try:
            # Try a simple health check (root endpoint or /health if available)
            response = self.session.get(
                self.base_url,
                timeout=5  # Short timeout for health check
            )

            if response.status_code in [200, 404]:  # 404 is OK if root doesn't exist but server responds
                self._logger.info(f"   ✅ LLM service is reachable (HTTP {response.status_code})")
            else:
                self._logger.warning(f"   ⚠️  LLM service returned unexpected status: {response.status_code}")

        except requests.exceptions.ConnectionError as e:
            error_msg = (
                f"❌ FAIL FAST: Cannot connect to LLM service at {self.base_url}\n"
                f"   Error: {str(e)}\n\n"
                f"   💡 To fix this issue:\n"
                f"   1. Start the local LLM service:\n"
                f"      make tree_search_dependencies\n"
                f"   2. Or use GCP Gemini instead:\n"
                f"      python your_script.py --llm-provider gcp\n"
                f"   3. Or disable LLM expansion:\n"
                f"      python your_script.py --no-llm\n"
            )
            self._logger.error(error_msg)
            raise ConnectionError(error_msg) from e

        except requests.exceptions.Timeout as e:
            error_msg = (
                f"❌ FAIL FAST: LLM service at {self.base_url} did not respond within 5 seconds\n"
                f"   Error: {str(e)}\n\n"
                f"   💡 The service may be starting up or overloaded. Try:\n"
                f"   1. Check service status: ps aux | grep uvicorn\n"
                f"   2. Check service logs: tail -f /tmp/llm_service.log\n"
                f"   3. Restart service: make tree_search_dependencies\n"
            )
            self._logger.error(error_msg)
            raise ConnectionError(error_msg) from e

        except Exception as e:
            error_msg = (
                f"❌ FAIL FAST: Unexpected error connecting to LLM service at {self.base_url}\n"
                f"   Error: {type(e).__name__}: {str(e)}\n"
            )
            self._logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _jaccard_similarity(self, query1: str, query2: str) -> float:
        """Calculate Jaccard similarity between two queries (word-level).

        Args:
            query1: First query string
            query2: Second query string

        Returns:
            Jaccard similarity score (0.0 to 1.0)
        """
        # Tokenize queries into word sets (lowercase, remove punctuation)
        words1 = set(query1.lower().replace("'", "").split())
        words2 = set(query2.lower().replace("'", "").split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def _check_query_novelty(self, root_query: str, generated_queries: List[str], similarity_threshold: float = 0.7) -> bool:
        """Check if generated queries are sufficiently novel compared to root query.

        Args:
            root_query: Original query to expand
            generated_queries: List of generated queries to check
            similarity_threshold: Maximum allowed Jaccard similarity (default 0.7)

        Returns:
            True if queries are novel enough, False if too similar (need retry)
        """
        if not generated_queries:
            return False

        similar_count = 0
        for query in generated_queries:
            similarity = self._jaccard_similarity(root_query, query)
            if similarity >= similarity_threshold:
                similar_count += 1
                self._logger.debug(f"      ⚠️  Query too similar (Jaccard={similarity:.2f}): '{query[:60]}...'")

        # If more than half the queries are too similar, reject the batch
        if similar_count > len(generated_queries) / 2:
            self._logger.debug(f"      ❌ {similar_count}/{len(generated_queries)} queries too similar to root query (threshold={similarity_threshold})")
            return False

        return True

    async def generate_queries(self, root_query, text_content: str, num_queries: int = 5) -> List[str]:
        """Generate semantic query variations using LLM with retry logic.

        Args:
            root_query: Original search query to expand. Can be:
                       - str: Single query string
                       - List[str]: Collection of related queries (first one is primary for expansion)
            text_content: Context text for semantic grounding
            num_queries: Number of query variations to generate

        Returns:
            List of generated query strings
        """
        # Handle both string and list inputs
        if isinstance(root_query, list):
            if not root_query:
                self._logger.warning("⚠️  Empty query collection provided")
                return []
            query_collection = root_query
            primary_query = root_query[0]  # First query is the primary one for expansion
            self._logger.debug(f"🧠 Generating {num_queries} semantic queries for: '{primary_query}' (with {len(query_collection)} grounding queries)")
        else:
            query_collection = [root_query]
            primary_query = root_query
            self._logger.debug(f"🧠 Generating {num_queries} semantic queries for: '{primary_query}'")

        # Retry logic with temperature adjustment and novelty checking
        max_retries = 3
        original_temp = self.temperature

        for attempt in range(max_retries):
            # Adjust temperature for each retry (increase by 0.2 to get more diverse outputs)
            current_temp = min(1.0, original_temp - (attempt * 0.2))
            if attempt > 0:
                self._logger.debug(f"🔄 Retry attempt {attempt + 1}/{max_retries} with temperature {current_temp}")

            queries = await self._attempt_query_generation(primary_query, query_collection, text_content, num_queries, current_temp)

            # Check if we got queries AND they're sufficiently novel
            if queries:
                if self._check_query_novelty(primary_query, queries):
                    self._logger.debug(f"      ✅ Generated {len(queries)} novel queries")
                    # Reset temperature back to original after successful generation
                    self.temperature = original_temp
                    return queries
                else:
                    # Queries too similar, continue to retry with higher temperature
                    self._logger.debug(f"      🔄 Queries too similar to root, retrying with higher temperature")
                    continue

        # If all retries failed, log warning and return empty list
        self._logger.warning(f"⚠️  Failed to generate novel queries after {max_retries} attempts")
        self.temperature = original_temp  # Reset temperature
        return []

    async def _attempt_query_generation(self, primary_query: str, query_collection: List[str], text_content: str, num_queries: int, temperature: float) -> List[str]:
        """Attempt to generate queries with specified temperature.

        Args:
            primary_query: Primary search query to expand (first in collection)
            query_collection: Full collection of related queries for grounding context
            text_content: Context text for semantic grounding
            num_queries: Number of query variations to generate
            temperature: Temperature for LLM generation

        Returns:
            List of generated query strings or empty list if failed
        """
        # Build grounding context from query collection if provided
        if len(query_collection) > 1:
            grounding_section = f"""
Related Query Context (use these to understand semantic space):
{chr(10).join(f"{i+1}. {q}" for i, q in enumerate(query_collection))}

Focus on expanding query #1 ("{primary_query}") but use the related queries above to understand the broader semantic context and discover complementary angles.
"""
        else:
            grounding_section = ""

        prompt = f"""Generate {num_queries} semantic search queries for: "{primary_query}"
{grounding_section}
Context: {text_content[:self.max_context_chars]}

CRITICAL REQUIREMENT: Do NOT simply rephrase or reorder words from the root query. You must introduce NEW concepts and themes from the context that are NOT present in the root query.

Create NEW queries that expand on "{primary_query}" using the context above. Read through the Context above and consider the themes, concepts and ideas associated with the query provided. Then expand on the query provided using NEW ideas, concepts and themes extracted from the context. Each expanded query should be semantically distinct and relevant, and provide variety in expected results - grounded in the context.

Expanding the query leads to a longer query in terms of char count, so expand and then consider a concise representation that maintains and encapsulates the semantics but would be effective with lexical matching BM-25 and modern vector search engines. It's about finding a balance between novel semantics grounded in the context provided and the root query, yet being concise to work effectively with lexical based search engines.

NOVELTY REQUIREMENTS (these are mandatory):
- DO NOT just rearrange words from the root query
- DO NOT just remove or add small words like "the", "of", "and"
- MUST introduce at least 2-3 NEW concepts from the context that are absent from root query
- MUST explore different angles, facets, or related topics mentioned in the context
- Each query should lead to discovering DIFFERENT documents than the root query would find

Concise means that the length of the query and syntactic structure should be simple:
- avoid multiphrase statements, use concatenated keywords
- capture semantics not necessary sentence structure
- provide queries that speak to concepts not conversational sentences
- avoid connectives: and, but, however, although
- avoid preposition: on, under, beside
- use: concepts, idea, constructs to expand
- always introduce novel concepts and ideas from the context provided
- highlight context and ideas that are not present in the root query
- do not repeat the root query or rephrase it expand it with new concepts
- explore different angles or facets


For example, here is an initial expansion of a query using root: "Tom Fletcher UN Under-Secretary-General for Humanitarian Affairs"

- "Tom Fletcher UN Under-Secretary-General for Humanitarian Affairs and Emergency Relief Coordinator with a background in diplomacy and humanitarian work"

a concise rendering according to our principles would be:

- "Tom Fletcher UN Under-Secretary-General for Humanitarian Affairs Emergency Relief Coordinator Diplomacy Humanitarian work"

Even better as this focuses on the new concepts introduced

- "Emergency Relief Coordinator Diplomacy Humanitarian work"


Output ONLY valid JSON in this format. The query must always be wrapped in ```json{{}}```, never generate without wrapping in ```json{{}}```:
```json
{{"root_query": "{primary_query}", "expanded_queries": ["query1", "query2"]}}
```

```json
{{"root_query": "colours", "expanded_queries": ["red", "green"]}}
```

```json
{{"root_query": "days of the week", "expanded_queries": ["monday", "tuesday"]}}
```


"""
        try:
            # WHEN: We send request to LLM service with specified temperature
            request_data = {
                "batch": [prompt],
                "max_length": self.max_length,
                "temperature": temperature,  # Use passed temperature parameter
                "top_p": self.top_p
            }
            response = self.session.post(
                f"{self.base_url}/generate",
                json=request_data,
                timeout=self.timeout
            )
            if response.status_code == 200:
                # THEN: Parse and extract generated queries
                data = response.json()
                generated_text = data.get('response', [''])[0]  # Fixed: 'response' not 'responses'

                # DEBUG: Print full input and output
                print(f"\n🔍 DEBUG LLM CALL:")
                print(f"📤 INPUT PROMPT:")
                print(f"{prompt}")
                print(f"\n📥 FULL LLM RESPONSE:")
                print(f"{generated_text}")
                print(f"\n" + "="*80)

                self._logger.debug(f"      📝 LLM Raw Response: {generated_text[:200]}...")

                # Use more flexible JSON extraction (matching original implementation)
                json_pattern = r'```json\s*(.*?)\s*```'
                json_matches = re.findall(json_pattern, generated_text, re.DOTALL | re.IGNORECASE)

                print(f"🔍 DEBUG JSON PARSING:")
                print(f"Found {len(json_matches)} JSON blocks")
                for i, match in enumerate(json_matches):
                    print(f"JSON Block {i+1}:")
                    print(f"{match}")
                    print(f"-" * 40)

                if json_matches:
                    self._logger.debug(f"      📦 Found {len(json_matches)} JSON code blocks")
                    # Try each JSON block until we find a valid one
                    for json_block in json_matches:
                        try:
                            print(f"🔍 Trying to parse JSON block: {json_block[:100]}...")
                            parsed_data = json.loads(json_block.strip())
                            print(f"✅ Successfully parsed JSON: {parsed_data}")
                            if ('primary_queries' in parsed_data or 'expanded_queries' in parsed_data) and 'root_query' in parsed_data:
                                queries = parsed_data.get('primary_queries', []) or parsed_data.get('expanded_queries', [])
                                print(f"📋 Extracted queries: {queries}")
                                # Filter out template/example queries
                                example_queries = {"red", "green", "blue", "US", "China", "Russia", "Ukraine", "Paris", "ChatGPT", "Large language models", "Sam Altman", "query1", "query2", "query3", "query4", "query5"}
                                filtered_queries = [q for q in queries if q and q.strip() and q not in example_queries]
                                print(f"🎯 Filtered queries: {filtered_queries}")
                                if filtered_queries:
                                    self._logger.debug(f"      ✅ Generated {len(filtered_queries)} semantic queries")
                                    return filtered_queries[:num_queries]
                        except json.JSONDecodeError as e:
                            print(f"❌ JSON parse error: {e}")
                            self._logger.debug(f"      ❌ JSON parse error (temp={temperature}): {e}")
                            continue

                    self._logger.debug(f"      ⚠️  No valid JSON found in response blocks (temp={temperature})")
                    return []
                else:
                    self._logger.debug(f"      ⚠️  No JSON block found in response (temp={temperature})")
                    return []
            else:
                self._logger.error(f"      ❌ LLM service error: {response.status_code} (temp={temperature})")
                return []
        except Exception as e:
            self._logger.error(f"      ❌ LLM request failed (temp={temperature}): {e}")
            return []

    def assess_qa_clusters(self, prompt: str, temperature: float = 0.3, max_tokens: int = 2000) -> str:
        """
        Assess Q&A clusters for deduplication (synchronous method for local LLM).

        This is a synchronous wrapper that will be called via asyncio.to_thread()
        in the QADeduplicator for consistent async interface.

        Args:
            prompt: Assessment prompt with clusters and context
            temperature: Temperature for generation (lower = more consistent)
            max_tokens: Maximum tokens to generate

        Returns:
            LLM response text with JSON decisions
        """
        try:
            self._logger.debug(f"🎯 Calling local LLM for Q&A assessment (temp={temperature})")

            url = f"{self.base_url}/v1/completions"
            payload = {
                "prompt": prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": self.top_p,
                "stop": None
            }

            response = self.session.post(url, json=payload, timeout=self.timeout)

            if response.status_code == 200:
                result = response.json()
                text = result.get('choices', [{}])[0].get('text', '').strip()
                self._logger.debug(f"   ✅ Received response ({len(text)} chars)")
                return text
            else:
                self._logger.error(f"   ❌ LLM service error: {response.status_code}")
                return ""

        except Exception as e:
            self._logger.error(f"   ❌ LLM assessment request failed: {e}")
            return ""
