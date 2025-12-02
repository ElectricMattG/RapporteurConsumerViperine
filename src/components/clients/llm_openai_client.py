#!/usr/bin/env python3
"""
OpenAI LLM Client

This module provides a client for interacting with OpenAI's models
using the OpenAI SDK. Compatible interface with SimpleLLMClient and GeminiLLMClient.

Supports:
- GPT-5 family (gpt-5.1, gpt-5, gpt-5-mini, gpt-5-nano) via Responses API
  - Reasoning effort control (none, low, medium, high)
  - Verbosity control (low, medium, high)
  - No temperature/top_p parameters (reasoning models)
- GPT-4 family (gpt-4o, gpt-4-turbo, gpt-4) via Chat Completions API
  - Traditional temperature/top_p parameters

Configuration (app_config.yaml or env vars):
  openai:
    api_key: your-api-key  # or OPENAI_API_KEY env var
    organization: your-org-id  # optional, or OPENAI_ORG_ID env var
    model_name: gpt-5.1  # default model
    reasoning_effort: none  # for GPT-5 models: none, low, medium, high
    verbosity: medium  # for GPT-5 models: low, medium, high
    timeout: 60  # request timeout in seconds
"""
import json
import os
import re
import sys
from typing import List, Optional

from openai import AsyncOpenAI

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

try:
    from config_handler import Base
except ImportError:
    # Fallback import path
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from config_handler import Base


class OpenAILLMClient(Base):
    """OpenAI LLM client for semantic query generation and Q&A deduplication assessment.

    Compatible interface with SimpleLLMClient and GeminiLLMClient.
    Supports GPT-4, GPT-3.5-turbo, and other OpenAI models.
    """

    def __init__(self, api_key: Optional[str] = None, model_name: Optional[str] = None, organization: Optional[str] = None):
        """Initialize OpenAI client with Base inheritance for config and logging.

        Args:
            api_key: OpenAI API key. If None, uses OPENAI_API_KEY env var or config.
            model_name: OpenAI model name. If None, uses config (default: gpt-4o-mini).
            organization: OpenAI organization ID. If None, uses OPENAI_ORG_ID env var or config.
        """
        # GIVEN: Base class initialization for centralized config and logging
        super().__init__()
        self._logger.info("🧠 Initializing OpenAILLMClient")

        # WHEN: We set up the OpenAI client configuration
        self.api_key = api_key or os.getenv('OPENAI_API_KEY') or self._app_config.get('openai.api_key')
        self.organization = organization or os.getenv('OPENAI_ORG_ID') or self._app_config.get('openai.organization')
        self.model_name = model_name or self._app_config.get('openai.model_name') or 'gpt-5.1'

        if not self.api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY env var or provide api_key parameter.")

        # LLM generation parameters from config (compatible with SimpleLLMClient)
        self.max_length = self._app_config.get('llm.max_length') or 500
        self.max_retries = self._app_config.get('llm.max_retries') or 3
        self.retry_delay = self._app_config.get('llm.retry_delay') or 1.0
        self.max_context_chars = self._app_config.get('llm.max_context_chars') or 8000

        # GPT-5 specific settings (reasoning models don't use temperature/top_p)
        # Use reasoning effort and verbosity instead
        self.reasoning_effort = self._app_config.get('openai.reasoning_effort') or 'none'  # none, low, medium, high
        self.verbosity = self._app_config.get('openai.verbosity') or 'medium'  # low, medium, high
        self.timeout = self._app_config.get('openai.timeout') or 60

        # Check if using GPT-5 model family (reasoning models)
        self.is_reasoning_model = 'gpt-5' in self.model_name.lower()

        # Initialize OpenAI client
        try:
            client_kwargs = {
                'api_key': self.api_key,
                'timeout': self.timeout,
            }
            if self.organization:
                client_kwargs['organization'] = self.organization

            self.client = AsyncOpenAI(**client_kwargs)

            self._logger.info(f"   🤖 Model: {self.model_name}")
            if self.organization:
                self._logger.info(f"   🏢 Organization: {self.organization}")
            if self.is_reasoning_model:
                self._logger.debug(f"   🧠 Reasoning model - effort: {self.reasoning_effort}, verbosity: {self.verbosity}")
            self._logger.debug(f"   ⚙️  Config - timeout: {self.timeout}s")
            self._logger.debug(f"   📏 Max context chars: {self.max_context_chars}")
        except Exception as e:
            self._logger.error(f"   ❌ Failed to initialize OpenAI client: {e}")
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

    async def generate_content(self, prompt: str, reasoning_effort: Optional[str] = None,
                        max_tokens: Optional[int] = None, verbosity: Optional[str] = None) -> str:
        """Generate content using OpenAI with specified parameters (async).

        Args:
            prompt: Input prompt for generation
            reasoning_effort: Reasoning effort level for GPT-5 models (none, low, medium, high).
                            For non-reasoning models, this is ignored and temperature is used.
            max_tokens: Maximum tokens to generate. If None, uses default max_length.
            verbosity: Output verbosity for GPT-5 models (low, medium, high).

        Returns:
            Generated text response

        Raises:
            Exception: If generation fails
        """
        try:
            if self.is_reasoning_model:
                # Use Responses API for GPT-5 models
                request_data = {
                    'model': self.model_name,
                    'input': prompt,
                }

                # Add reasoning configuration
                reasoning_config = {
                    'effort': reasoning_effort if reasoning_effort is not None else self.reasoning_effort
                }
                request_data['reasoning'] = reasoning_config

                # Add text configuration
                text_config = {
                    'verbosity': verbosity if verbosity is not None else self.verbosity
                }
                request_data['text'] = text_config

                # Add max_output_tokens if specified
                if max_tokens:
                    request_data['max_output_tokens'] = max_tokens

                # Call Responses API
                response = await self.client.responses.create(**request_data)
                return response.output_text
            else:
                # Use Chat Completions API for non-reasoning models (GPT-4, etc.)
                response = await self.client.chat.completions.create(
                    model=self.model_name,
                    messages=[
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.7,  # Default for non-reasoning models
                    top_p=0.9,
                    max_tokens=max_tokens if max_tokens is not None else self.max_length,
                )
                return response.choices[0].message.content
        except Exception as e:
            self._logger.error(f"❌ OpenAI generation failed: {e}")
            raise

    async def generate_queries(self, root_query, text_content: str, num_queries: int = 5) -> List[str]:
        """Generate semantic query variations using OpenAI with retry logic.

        Compatible interface with SimpleLLMClient.generate_queries().

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

        # Retry logic with reasoning effort adjustment and novelty checking
        max_retries = 3
        original_effort = self.reasoning_effort if self.is_reasoning_model else None

        for attempt in range(max_retries):
            # For GPT-5 models, adjust reasoning effort for each retry
            # For non-reasoning models, this will be ignored
            if self.is_reasoning_model:
                # Progression: none → low → medium
                effort_levels = ['none', 'low', 'medium']
                current_effort = effort_levels[min(attempt, len(effort_levels) - 1)]
                if attempt > 0:
                    self._logger.debug(f"🔄 Retry attempt {attempt + 1}/{max_retries} with reasoning effort: {current_effort}")
            else:
                current_effort = None
                if attempt > 0:
                    self._logger.debug(f"🔄 Retry attempt {attempt + 1}/{max_retries}")

            queries = await self._attempt_query_generation(primary_query, query_collection, text_content, num_queries, current_effort)

            # Check if we got queries AND they're sufficiently novel
            if queries:
                if self._check_query_novelty(primary_query, queries):
                    self._logger.debug(f"      ✅ Generated {len(queries)} novel queries")
                    # Reset reasoning effort back to original after successful generation
                    if self.is_reasoning_model:
                        self.reasoning_effort = original_effort
                    return queries
                else:
                    # Queries too similar, continue to retry with higher reasoning effort
                    self._logger.debug(f"      🔄 Queries too similar to root, retrying with higher reasoning effort")
                    continue

        # If all retries failed, log warning and return empty list
        self._logger.warning(f"⚠️  Failed to generate novel queries after {max_retries} attempts")
        if self.is_reasoning_model:
            self.reasoning_effort = original_effort  # Reset reasoning effort
        return []

    async def _attempt_query_generation(self, primary_query: str, query_collection: List[str],
                                       text_content: str, num_queries: int, reasoning_effort: Optional[str]) -> List[str]:
        """Attempt to generate queries with specified reasoning effort using OpenAI.

        Args:
            primary_query: Primary search query to expand (first in collection)
            query_collection: Full collection of related queries for grounding context
            text_content: Context text for semantic grounding
            num_queries: Number of query variations to generate
            reasoning_effort: Reasoning effort level for GPT-5 models (none, low, medium, high).
                            For non-reasoning models, this is ignored.

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

Output ONLY valid JSON in this format. The query must always be wrapped in ```json{{}}```:
```json
{{"root_query": "{primary_query}", "expanded_queries": ["query1", "query2", "query3"]}}
```
"""
        try:
            # Call OpenAI with specified reasoning effort (async)
            generated_text = await self.generate_content(prompt, reasoning_effort=reasoning_effort)

            self._logger.debug(f"      📝 OpenAI Raw Response: {generated_text[:200]}...")

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
                        self._logger.debug(f"      ❌ JSON parse error (effort={reasoning_effort}): {e}")
                        continue

                self._logger.debug(f"      ⚠️  No valid JSON found in response blocks (effort={reasoning_effort})")
                return []
            else:
                self._logger.debug(f"      ⚠️  No JSON block found in response (effort={reasoning_effort})")
                return []
        except Exception as e:
            self._logger.error(f"      ❌ OpenAI request failed (effort={reasoning_effort}): {e}")
            return []

    async def assess_qa_clusters(self, clusters_prompt: str, reasoning_effort: Optional[str] = None) -> str:
        """Assess Q&A clusters for deduplication using OpenAI (async).

        This method is specifically for Q&A deduplication assessment with context.

        Args:
            clusters_prompt: Formatted prompt with clusters and source contexts
            reasoning_effort: Reasoning effort level. If None, uses 'low' for consistency.

        Returns:
            OpenAI response with assessment decisions in JSON format
        """
        # Use low reasoning effort for assessment (more consistent/deterministic)
        assess_effort = reasoning_effort if reasoning_effort is not None else 'low'

        self._logger.debug(f"🎯 Assessing Q&A clusters with OpenAI (effort={assess_effort})")

        try:
            # Generate with larger max tokens for detailed assessments (async)
            response = await self.generate_content(
                prompt=clusters_prompt,
                reasoning_effort=assess_effort,
                max_tokens=2000  # Larger for cluster assessments
            )

            self._logger.debug(f"   ✅ Received assessment response ({len(response)} chars)")
            return response

        except Exception as e:
            self._logger.error(f"   ❌ Cluster assessment failed: {e}")
            raise

    def get_model_info(self) -> dict:
        """Get information about the current OpenAI model.

        Returns:
            Dictionary with model information (name, limits, etc.)
        """
        # OpenAI model information (static since API doesn't provide model.get)
        model_limits = {
            # GPT-5 family (reasoning models)
            'gpt-5.1': {'input': 200000, 'output': 100000, 'description': 'Latest flagship reasoning model with none/low/medium/high reasoning'},
            'gpt-5': {'input': 200000, 'output': 100000, 'description': 'Previous flagship reasoning model'},
            'gpt-5-mini': {'input': 200000, 'output': 100000, 'description': 'Cost-optimized reasoning model'},
            'gpt-5-nano': {'input': 200000, 'output': 100000, 'description': 'High-throughput reasoning model'},
            # GPT-4 family
            'gpt-4o': {'input': 128000, 'output': 16384, 'description': 'GPT-4 Optimized'},
            'gpt-4o-mini': {'input': 128000, 'output': 16384, 'description': 'GPT-4 Optimized Mini'},
            'gpt-4-turbo': {'input': 128000, 'output': 4096, 'description': 'GPT-4 Turbo'},
            'gpt-4': {'input': 8192, 'output': 8192, 'description': 'GPT-4 Base'},
            'gpt-3.5-turbo': {'input': 16385, 'output': 4096, 'description': 'GPT-3.5 Turbo'},
        }

        limits = model_limits.get(self.model_name, {'input': 'Unknown', 'output': 'Unknown', 'description': 'Unknown model'})

        return {
            'name': self.model_name,
            'display_name': self.model_name,
            'description': limits['description'],
            'input_token_limit': limits['input'],
            'output_token_limit': limits['output'],
            'is_reasoning_model': self.is_reasoning_model,
        }
