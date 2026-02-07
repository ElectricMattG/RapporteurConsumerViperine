#!/usr/bin/env python3
"""
GCP Gemini LLM Client

This module provides a client for interacting with Google Gemini via Vertex AI
using the google-genai SDK. Compatible interface with SimpleLLMClient.
"""
import asyncio
import json
import os
import random
import re
import sys
import time
from typing import List, Optional

from google import genai
from google.genai import types

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

try:
    from config_handler import Base
except ImportError:
    # Fallback import path
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class GeminiLLMClient(Base):
    """GCP Gemini LLM client for semantic query generation and Q&A deduplication assessment.

    Compatible interface with SimpleLLMClient but uses Google Gemini via Vertex AI.
    Supports large context windows (up to 1M tokens for Gemini 1.5 Pro).
    """

    def __init__(self, project_id: Optional[str] = None, location: Optional[str] = None, model_name: Optional[str] = None):
        """Initialize Gemini client with Base inheritance for config and logging.

        Args:
            project_id: GCP project ID. If None, uses PROJECT_ID env var or config.
            location: GCP region. If None, uses LOCATION env var or config (default: us-central1).
            model_name: Gemini model name. If None, uses config (default: gemini-2.0-flash-exp).
        """
        # GIVEN: Base class initialization for centralized config and logging
        super().__init__()
        self._logger.info("🧠 Initializing GeminiLLMClient")

        # WHEN: We set up the Gemini client configuration
        self.project_id = project_id or os.getenv('PROJECT_ID') or self._app_config.get('gemini.project_id')
        self.location = location or os.getenv('LOCATION') or self._app_config.get('gemini.location') or 'us-central1'
        self.model_name = model_name or self._app_config.get('gemini.model_name') or 'gemini-2.5-pro'

        # LLM generation parameters from config (compatible with SimpleLLMClient)
        self.max_length = self._app_config.get('llm.max_length') or 500
        self.temperature = self._app_config.get('llm.temperature') or 0.7
        self.top_p = self._app_config.get('llm.top_p') or 0.9
        self.max_retries = self._app_config.get('llm.max_retries') or 3
        self.retry_delay = self._app_config.get('llm.retry_delay') or 1.0
        self.max_context_chars = self._app_config.get('llm.max_context_chars') or 8000

        # Gemini-specific settings
        self.timeout = self._app_config.get('gemini.timeout') or 60

        # Initialize Gemini client
        try:
            self.client = genai.Client(
                vertexai=True,
                project=self.project_id,
                location=self.location
            )
            self._logger.info(f"   🌐 GCP Project: {self.project_id}")
            self._logger.info(f"   📍 Location: {self.location}")
            self._logger.info(f"   🤖 Model: {self.model_name}")
            self._logger.debug(f"   ⚙️  Config - timeout: {self.timeout}s, temp: {self.temperature}, top_p: {self.top_p}")
            self._logger.debug(f"   📏 Max context chars: {self.max_context_chars}")
        except Exception as e:
            self._logger.error(f"   ❌ Failed to initialize Gemini client: {e}")
            raise

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

    async def generate_content(self, prompt: str, temperature: Optional[float] = None,
                        max_tokens: Optional[int] = None, enable_search: bool = False,
                        return_grounding_metadata: bool = False, response_mime_type: Optional[str] = None) -> str:
        """Generate content using Gemini with specified parameters (async).

        Args:
            prompt: Input prompt for generation
            temperature: Generation temperature (0.0-1.0). If None, uses default.
            max_tokens: Maximum tokens to generate. If None, uses default max_length.
            enable_search: Enable Google Search grounding for real-time web search
            return_grounding_metadata: Return dict with text and grounding metadata instead of just text
            response_mime_type: Force response format (e.g., 'application/json')

        Returns:
            Generated text response (or dict if return_grounding_metadata=True)

        Raises:
            Exception: If generation fails after all retries
        """
        config_params = {
            'temperature': temperature if temperature is not None else self.temperature,
            'top_p': self.top_p,
            'max_output_tokens': max_tokens if max_tokens is not None else self.max_length,
        }

        # Add JSON mode if requested
        if response_mime_type:
            config_params['response_mime_type'] = response_mime_type

        # Add Google Search grounding tool if enabled
        tools = []
        if enable_search:
            self._logger.debug(f"      🔍 Enabling Google Search grounding")
            # Google Search tool for real-time web grounding
            # This allows the model to search the web and cite sources
            tools.append(types.Tool(google_search=types.GoogleSearch()))

        config = types.GenerateContentConfig(
            tools=tools if tools else None,
            **config_params
        )

        # Exponential backoff for rate limiting (429 errors)
        max_retries = 5
        base_delay = 2.0  # Start with 2 seconds
        max_delay = 60.0  # Cap at 60 seconds

        for attempt in range(max_retries):
            try:
                # Run blocking Gemini API call in thread pool to avoid blocking event loop
                # This enables true concurrent processing of multiple requests
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,  # Use default ThreadPoolExecutor
                    lambda: self.client.models.generate_content(
                        model=self.model_name,
                        contents=prompt,
                        config=config
                    )
                )

                # Extract text and optional grounding metadata
                text = response.text

                # Log finish reason to diagnose truncation
                if hasattr(response, 'candidates') and response.candidates:
                    candidate = response.candidates[0]
                    finish_reason = candidate.finish_reason if hasattr(candidate, 'finish_reason') else 'UNKNOWN'
                    self._logger.info(f"   🏁 Gemini finish_reason: {finish_reason}")

                # If search was enabled and metadata requested, return full response
                if return_grounding_metadata and enable_search:
                    grounding_metadata = {}

                    # Extract grounding metadata if present
                    if hasattr(response, 'candidates') and response.candidates:
                        candidate = response.candidates[0]
                        if hasattr(candidate, 'grounding_metadata'):
                            grounding_meta = candidate.grounding_metadata

                            # Extract search queries used
                            if hasattr(grounding_meta, 'search_entry_point'):
                                search_queries = grounding_meta.search_entry_point.rendered_content if hasattr(grounding_meta.search_entry_point, 'rendered_content') else None
                                grounding_metadata['search_queries'] = search_queries

                            # Extract grounding chunks (citations)
                            if hasattr(grounding_meta, 'grounding_chunks') and grounding_meta.grounding_chunks:
                                chunks = []
                                for chunk in grounding_meta.grounding_chunks:
                                    chunk_data = {}
                                    if hasattr(chunk, 'web'):
                                        chunk_data['uri'] = chunk.web.uri if hasattr(chunk.web, 'uri') else None
                                        chunk_data['title'] = chunk.web.title if hasattr(chunk.web, 'title') else None
                                    chunks.append(chunk_data)
                                grounding_metadata['citations'] = chunks

                            # Extract grounding support scores
                            if hasattr(grounding_meta, 'grounding_supports') and grounding_meta.grounding_supports:
                                supports = []
                                for support in grounding_meta.grounding_supports:
                                    support_data = {
                                        'segment_text': support.segment.text if hasattr(support, 'segment') and hasattr(support.segment, 'text') else None,
                                        'grounding_chunk_indices': list(support.grounding_chunk_indices) if hasattr(support, 'grounding_chunk_indices') else [],
                                        'confidence_score': support.confidence_score if hasattr(support, 'confidence_score') else None
                                    }
                                    supports.append(support_data)
                                grounding_metadata['grounding_supports'] = supports

                    return {
                        'text': text,
                        'grounding_metadata': grounding_metadata
                    }

                # Success - return text
                return text

            except Exception as e:
                error_str = str(e)

                # Check if it's a rate limit error (429)
                if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                    if attempt < max_retries - 1:
                        # Calculate exponential backoff with jitter
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        # Add jitter: random value between 0 and 25% of delay
                        jitter = random.uniform(0, delay * 0.25)
                        total_delay = delay + jitter

                        self._logger.warning(
                            f"⏳ Rate limit hit (429), retrying in {total_delay:.1f}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(total_delay)
                        continue
                    else:
                        self._logger.error(
                            f"❌ Rate limit exhausted after {max_retries} attempts: {e}"
                        )
                        raise
                else:
                    # Non-rate-limit error, fail immediately
                    self._logger.error(f"❌ Gemini generation failed: {e}")
                    raise
            raise

    async def generate_queries(self, root_query, text_content: str, num_queries: int = 5,
                              enable_search: bool = False) -> List[str]:
        """Generate semantic query variations using Gemini with retry logic.

        Compatible interface with SimpleLLMClient.generate_queries().

        Args:
            root_query: Original search query to expand. Can be:
                       - str: Single query string
                       - List[str]: Collection of related queries (first one is primary for expansion)
            text_content: Context text for semantic grounding
            num_queries: Number of query variations to generate
            enable_search: Enable Google Search grounding to find real-time context for query expansion

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

        if enable_search:
            self._logger.debug(f"   🔍 Web search grounding enabled for query expansion")

        # Retry logic with temperature adjustment and novelty checking
        max_retries = 3
        original_temp = self.temperature

        for attempt in range(max_retries):
            # Adjust temperature for each retry (increase by 0.2 to get more diverse outputs)
            current_temp = min(1.0, original_temp + (attempt * 0.2))
            if attempt > 0:
                self._logger.debug(f"🔄 Retry attempt {attempt + 1}/{max_retries} with temperature {current_temp}")

            queries = await self._attempt_query_generation(primary_query, query_collection, text_content,
                                                          num_queries, current_temp, enable_search)

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

    async def _attempt_query_generation(self, primary_query: str, query_collection: List[str],
                                       text_content: str, num_queries: int, temperature: float,
                                       enable_search: bool = False) -> List[str]:
        """Attempt to generate queries with specified temperature using Gemini.

        Args:
            primary_query: Primary search query to expand (first in collection)
            query_collection: Full collection of related queries for grounding context
            text_content: Context text for semantic grounding
            num_queries: Number of query variations to generate
            temperature: Temperature for LLM generation
            enable_search: Enable Google Search grounding for real-time context

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

        search_note = "\nNOTE: You have access to Google Search. Use it to find current, relevant context about this topic to inform your query expansion." if enable_search else ""

        prompt = f"""Generate {num_queries} semantic search queries for: "{primary_query}"
{grounding_section}
Context: {text_content[:self.max_context_chars]}
{search_note}

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

Output ONLY valid JSON in this format. The query must always be wrapped in ```json{{}}```:
```json
{{"root_query": "{primary_query}", "expanded_queries": ["query1", "query2", "query3"]}}
```
"""
        try:
            # Call Gemini with specified temperature and optional search (async)
            generated_text = await self.generate_content(prompt, temperature=temperature, enable_search=enable_search)

            self._logger.debug(f"      📝 Gemini Raw Response: {generated_text[:200]}...")

            # Use more flexible JSON extraction (matching SimpleLLMClient implementation)
            json_pattern = r'```json\s*(.*?)\s*```'
            json_matches = re.findall(json_pattern, generated_text, re.DOTALL | re.IGNORECASE)

            if json_matches:
                self._logger.debug(f"      📦 Found {len(json_matches)} JSON code blocks")
                # Try each JSON block until we find a valid one
                for json_block in json_matches:
                    try:
                        parsed_data = json.loads(json_block.strip())
                        if ('primary_queries' in parsed_data or 'expanded_queries' in parsed_data) and 'root_query' in parsed_data:
                            queries = parsed_data.get('primary_queries', []) or parsed_data.get('expanded_queries', [])
                            # Filter out template/example queries
                            example_queries = {"query1", "query2", "query3", "query4", "query5"}
                            filtered_queries = [q for q in queries if q and q.strip() and q not in example_queries]
                            if filtered_queries:
                                self._logger.debug(f"      ✅ Generated {len(filtered_queries)} semantic queries")
                                return filtered_queries[:num_queries]
                    except json.JSONDecodeError as e:
                        self._logger.debug(f"      ❌ JSON parse error (temp={temperature}): {e}")
                        continue

                self._logger.debug(f"      ⚠️  No valid JSON found in response blocks (temp={temperature})")
                return []
            else:
                self._logger.debug(f"      ⚠️  No JSON block found in response (temp={temperature})")
                return []
        except Exception as e:
            self._logger.error(f"      ❌ Gemini request failed (temp={temperature}): {e}")
            return []

    async def assess_qa_clusters(self, clusters_prompt: str, temperature: Optional[float] = None) -> str:
        """Assess Q&A clusters for deduplication using Gemini (async).

        This method is specifically for Q&A deduplication assessment with context.

        Args:
            clusters_prompt: Formatted prompt with clusters and source contexts
            temperature: Generation temperature. If None, uses lower default (0.3) for consistency.

        Returns:
            Gemini response with assessment decisions in JSON format
        """
        # Use lower temperature for assessment (more consistent/deterministic)
        assess_temp = temperature if temperature is not None else 0.3

        self._logger.debug(f"🎯 Assessing Q&A clusters with Gemini (temp={assess_temp})")

        try:
            # Generate with larger max tokens for detailed assessments (async)
            response = await self.generate_content(
                prompt=clusters_prompt,
                temperature=assess_temp,
                max_tokens=2000  # Larger for cluster assessments
            )

            self._logger.debug(f"   ✅ Received assessment response ({len(response)} chars)")
            return response

        except Exception as e:
            self._logger.error(f"   ❌ Cluster assessment failed: {e}")
            raise

    def get_model_info(self) -> dict:
        """Get information about the current Gemini model.

        Returns:
            Dictionary with model information (name, limits, etc.)
        """
        try:
            model_info = self.client.models.get(model=self.model_name)
            info = {
                'name': self.model_name,
                'display_name': model_info.display_name if hasattr(model_info, 'display_name') else 'N/A',
                'description': model_info.description if hasattr(model_info, 'description') else 'N/A',
                'input_token_limit': getattr(model_info, 'input_token_limit', 'Unknown'),
                'output_token_limit': getattr(model_info, 'output_token_limit', 'Unknown'),
            }
            return info
        except Exception as e:
            self._logger.warning(f"Could not fetch model info: {e}")
            return {
                'name': self.model_name,
                'display_name': 'N/A',
                'description': 'N/A',
                'input_token_limit': 'Unknown',
                'output_token_limit': 'Unknown',
            }
