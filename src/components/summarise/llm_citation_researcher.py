#!/usr/bin/env python3
"""
Citation Research and Summary Client

This module provides a client for researching citations (books, journal articles)
by searching the web and generating comprehensive markdown summaries using OpenAI GPT-5.1.
"""
import json
import os
import re
import sys
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from openai import AsyncOpenAI

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../src'))

try:
    from config_handler import Base
except ImportError:
    # Fallback import path
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../../src'))
    from config_handler import Base


class CitationResearchClient(Base):
    """OpenAI-powered client for citation research and markdown summary generation.

    This client:
    1. Extracts citations from text or processes citation lists
    2. Searches the web for each citation
    3. Generates 500-word markdown summaries using GPT-5.1
    4. Returns structured results with URLs, summaries, and markdown
    """

    def __init__(self, llm_provider: str = 'gemini', api_key: Optional[str] = None,
                 model_name: Optional[str] = None, organization: Optional[str] = None,
                 web_searcher=None):
        """Initialize Citation Research Client with support for multiple LLM providers.

        Args:
            llm_provider: LLM provider ('openai' or 'gemini'). Default: 'gemini'.
            api_key: API key. If None, uses env var or config.
            model_name: Model name. If None, uses provider-specific defaults.
            organization: OpenAI organization ID (OpenAI only). If None, uses env var or config.
            web_searcher: Optional web search client (e.g., DocumentWebSearcher).
                         If None, will need to be set before calling research_citations().
        """
        # GIVEN: Base class initialization for centralized config and logging
        super().__init__()
        self._logger.info("📚 Initializing CitationResearchClient")

        # WHEN: We set up the LLM client configuration
        self.llm_provider = llm_provider.lower()

        # Citation research settings (with fallback defaults) - set these BEFORE initializing clients
        self.summary_word_count = self._app_config.get('citation_research.summary_word_count') if self._app_config.get('citation_research.summary_word_count') else 100
        self.timeout = self._app_config.get('openai.timeout') if self._app_config.get('openai.timeout') else 180

        if self.llm_provider == 'openai':
            self._init_openai(api_key, model_name, organization)
        elif self.llm_provider == 'gemini':
            self._init_gemini(api_key, model_name)
        else:
            raise ValueError(f"Unsupported LLM provider: {llm_provider}. Choose 'openai' or 'gemini'.")
        self.max_urls_per_citation = self._app_config.get('citation_research.max_urls') if self._app_config.get('citation_research.max_urls') else 5

        # Check if web search is enabled (default to True if not specified)
        enable_web_search_config = self._app_config.get('citation_research.enable_web_search')
        self.enable_web_search = enable_web_search_config if enable_web_search_config is not None else True

        # Web searcher (will search for citation sources)
        self.web_searcher = web_searcher

        self._logger.info(f"   ✅ {self.llm_provider.upper()} client initialized")
        self._logger.debug(f"   📝 Summary length: {self.summary_word_count} words")

    def _init_openai(self, api_key: Optional[str], model_name: Optional[str], organization: Optional[str]):
        """Initialize OpenAI client."""
        self.api_key = api_key or os.getenv('OPENAI_API_KEY') or self._app_config.get('openai.api_key')
        self.organization = organization or os.getenv('OPENAI_ORG_ID') or self._app_config.get('openai.organization')
        self.model_name = model_name or self._app_config.get('openai.model_name') or 'gpt-5.1'

        if not self.api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY env var or provide api_key parameter.")

        # OpenAI-specific settings
        self.reasoning_effort = self._app_config.get('openai.reasoning_effort') if self._app_config.get('openai.reasoning_effort') else 'low'
        self.verbosity = self._app_config.get('openai.verbosity') if self._app_config.get('openai.verbosity') else 'high'

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
                self._logger.debug(f"   🧠 Reasoning: {self.reasoning_effort}, Verbosity: {self.verbosity}")
        except Exception as e:
            self._logger.error(f"   ❌ Failed to initialize OpenAI client: {e}")
            raise

    def _init_gemini(self, api_key: Optional[str], model_name: Optional[str]):
        """Initialize Gemini client."""
        # Import Gemini client
        try:
            from src.clients.llm_gcp_client import GeminiLLMClient
        except ImportError as e:
            raise ImportError(f"Failed to import GeminiLLMClient: {e}")

        self.model_name = model_name or self._app_config.get('gemini.model_name') or 'gemini-2.5-pro'

        # Initialize Gemini client
        try:
            from clients.llm_cache_client import CachedLLMClient
            from config import app_conf
            _inner = GeminiLLMClient(model_name=self.model_name)
            self.client = CachedLLMClient(
                _inner,
                redis_host=app_conf.get("redis.host") or "",
                redis_port=int(app_conf.get("redis.port") or 6379),
                redis_password=app_conf.get("redis.password") or None,
                ttl_seconds=int(app_conf.get("redis.llm_cache_ttl_seconds") or 432000),
            )
            self.is_reasoning_model = False  # Gemini doesn't have separate reasoning API

            self._logger.info(f"   🤖 Model: {self.model_name}")
        except Exception as e:
            self._logger.error(f"   ❌ Failed to initialize Gemini client: {e}")
            raise

    def extract_citations(self, text: str) -> List[str]:
        """Extract citations embedded in text.

        Uses pattern matching to find common citation formats:
        - Author, Year. Title. Publisher.
        - Author (Year). Title. Journal.
        - Title, Author, Year
        - etc.

        Args:
            text: Text containing embedded citations

        Returns:
            List of extracted citation strings
        """
        citations = []

        # Pattern 1: Author (Year) format
        pattern1 = r'([A-Z][a-z]+(?:,?\s+[A-Z][a-z]+)*)\s*\((\d{4})\)\.?\s*([^\.]+\.)'
        matches1 = re.findall(pattern1, text)
        for author, year, title in matches1:
            citations.append(f"{author} ({year}). {title}")

        # Pattern 2: Title in quotes or italics
        pattern2 = r'(?:"([^"]+)"|_([^_]+)_)\s*(?:by\s+)?([A-Z][a-z]+(?:,?\s+[A-Z][a-z]+)*)'
        matches2 = re.findall(pattern2, text)
        for title1, title2, author in matches2:
            title = title1 or title2
            citations.append(f"{author}. {title}")

        # Pattern 3: Simple comma-separated (Author, Title, Year, Publisher)
        pattern3 = r'([A-Z][a-z]+(?:,?\s+[A-Z][a-z]+)*),\s*([^,]+),\s*(\d{4})'
        matches3 = re.findall(pattern3, text)
        for author, title, year in matches3:
            citations.append(f"{author}, {title}, {year}")

        if not citations:
            # If no patterns match, treat each line as a potential citation
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            citations = [line for line in lines if len(line) > 20]  # Filter very short lines

        self._logger.debug(f"📖 Extracted {len(citations)} citations from text")
        return citations

    async def search_citation(self, citation: str) -> List[str]:
        """Search the web for URLs related to a citation.

        Args:
            citation: Citation string to search for

        Returns:
            List of URLs (up to max_urls_per_citation)
        """
        if not self.web_searcher:
            self._logger.warning("⚠️  No web searcher configured, returning empty URL list")
            return []

        try:
            self._logger.debug(f"🔍 Searching web for: {citation[:100]}...")

            # Use web searcher to find URLs
            results = await self.web_searcher.search(citation, max_results=self.max_urls_per_citation)

            urls = []
            if hasattr(results, 'organic_results'):
                urls = [r.get('link') for r in results.organic_results if r.get('link')]
            elif isinstance(results, list):
                urls = [r.get('link') or r.get('url') for r in results if r.get('link') or r.get('url')]

            urls = urls[:self.max_urls_per_citation]
            self._logger.debug(f"   ✅ Found {len(urls)} URLs")
            return urls

        except Exception as e:
            self._logger.error(f"   ❌ Web search failed for citation: {e}")
            return []

    async def _generate_with_openai(self, prompt: str) -> str:
        """Generate summary using OpenAI API.

        Args:
            prompt: The prompt to send to OpenAI

        Returns:
            Generated text response
        """
        if self.is_reasoning_model:
            # Use Responses API for GPT-5
            self._logger.info(f"   🤖 Using Responses API with model: {self.model_name}")

            # Build API call kwargs
            max_tokens = self._app_config.get('openai.max_tokens') or 16000
            api_kwargs = {
                'model': self.model_name,
                'input': prompt,
                'reasoning': {'effort': self.reasoning_effort},
                'text': {'verbosity': self.verbosity},
                'max_output_tokens': max_tokens,
            }

            # Web search is only supported on Responses API (GPT-5 models)
            if self.enable_web_search:
                self._logger.info(f"   🔍 Enabling web search tool for grounding")
                api_kwargs['tools'] = [{"type": "web_search_preview"}]
            else:
                self._logger.info(f"   ℹ️  Web search disabled")

            response = await self.client.responses.create(**api_kwargs)
            return response.output_text
        else:
            # Use Chat Completions API for GPT-4
            # Note: web_search_preview is NOT supported on Chat Completions API
            self._logger.info(f"   🤖 Using Chat Completions API with model: {self.model_name}")

            if self.enable_web_search:
                self._logger.warning(f"   ⚠️  Web search not supported on Chat Completions API (GPT-4), ignoring")

            # Build API call kwargs (no tools support for web search)
            max_tokens = self._app_config.get('openai.max_tokens') or 16000
            temperature = self._app_config.get('openai.temperature') or 0.5
            api_kwargs = {
                'model': self.model_name,
                'messages': [{"role": "user", "content": prompt}],
                'temperature': temperature,
                'max_tokens': max_tokens,
            }

            response = await self.client.chat.completions.create(**api_kwargs)
            return response.choices[0].message.content

    async def _generate_with_gemini(self, prompt: str, enable_search: bool = False) -> str:
        """Generate summary using Gemini API.

        Args:
            prompt: The prompt to send to Gemini
            enable_search: Enable Google Search grounding for web access

        Returns:
            Generated text response
        """
        self._logger.info(f"   🤖 Using Gemini API with model: {self.model_name}")
        if enable_search:
            self._logger.info(f"   🔍 Google Search grounding ENABLED for web access")

        # Gemini uses the async generate_content method with optional search grounding
        temperature = self._app_config.get('openai.temperature') or 0.5
        max_tokens = self._app_config.get('openai.max_tokens') or 16000
        response = await self.client.generate_content(
            prompt=prompt,
            temperature=temperature,
            max_tokens=max_tokens,
            enable_search=enable_search  # Enable Google Search tool for web grounding
        )

        return response

    async def generate_summary(self, citation: str, urls: List[str], context: Optional[str] = None, use_web_grounding: bool = False) -> Dict[str, Any]:
        """Generate a structured citation summary with metadata and markdown.

        Args:
            citation: Citation string
            urls: List of relevant URLs found for this citation
            context: Optional additional context (e.g., abstract, excerpt)
            use_web_grounding: If True, tells LLM to access URL directly (Gemini with web grounding)

        Returns:
            Dictionary with structured metadata and markdown summary:
            {
                'citation_text': str,
                'metadata': {
                    'title': str,
                    'authors': List[str],
                    'publication_date': str,
                    'publisher': str,
                    'publication_venue': str,  # Journal name, conference, etc.
                    'pages': str,
                    'volume': str,
                    'issue': str,
                    'doi': str,
                    'isbn': str,
                    'type': str,  # 'book', 'journal_article', 'report', 'chapter', etc.
                },
                'urls': List[str],
                'markdown_summary': str
            }
        """
        # Build prompt for summary generation with structured metadata extraction
        url_section = "\n".join([f"- {url}" for url in urls]) if urls else "No URLs found."

        # For Gemini, ALWAYS use web grounding with the URL (web scraping is often partial/incomplete)
        if self.llm_provider == 'gemini' and urls:
            # Always use web grounding for Gemini since web scraping may be partial
            context_section = f"\n\nPartial Extracted Context (may be incomplete):\n{context[:500]}..." if context else ""
            web_instruction = f"\n\n**CRITICAL INSTRUCTION**: You MUST use Google Search grounding to access the article at the URL below. The extracted content may be partial or incomplete, so retrieve the FULL article directly from the source.\n\n**Primary URL to access**: {urls[0]}\n\nUse Google Search to:\n1. Access the article at this exact URL\n2. Read the complete article content (ignore any partial extracted text above)\n3. Extract all metadata (title, authors, date, publisher, etc.)\n4. Generate a comprehensive summary based on the full article from the source\n"
            # Enable search for Gemini when we have URLs
            use_web_grounding = True
        else:
            context_section = f"\n\nAdditional Context:\n{context}" if context else ""
            web_instruction = ""

        prompt = f"""Analyze this article and generate a comprehensive response with structured metadata and a flowing review-style summary.

You have access to Google Search - use it to find accurate information about this work, including publication details, author information, and full article content.
{web_instruction}
Citation: {citation}

Relevant URLs:
{url_section}
{context_section}

METADATA INSTRUCTIONS:
- Use web search to verify and extract ALL metadata fields accurately
- Extract metadata from the citation string and supplement with web search results
- Use null for fields not available
- Determine the type of publication accurately

SUMMARY INSTRUCTIONS (approximately {self.summary_word_count} words):

Write a REVIEW-STYLE summary with free-flowing narrative paragraphs. Use web search to gather accurate information about the work's content, arguments, and impact. Structure it as follows:

1. MAIN NARRATIVE (3-5 flowing paragraphs, max 500 words):
   - First paragraph: Introduce the work, its main argument, and scholarly context
   - Middle paragraphs: Explain key concepts, arguments, evidence, methodology, and contributions in flowing prose
   - Final paragraph: Discuss significance, impact, and relevance to the field

2. CITATION SECTION (at the very end):
   - Add "## Citation" heading
   - Include the full formatted citation
   - Add DOI, ISBN, or URLs if available

STYLE REQUIREMENTS:
- Write in flowing, readable prose like a book review or scholarly abstract
- NO bullet points, numbered lists, or subsection headings in the main narrative
- Use smooth transitions between paragraphs
- Focus on explaining the content, arguments, and significance
- Keep text cohesive and easy to index
- Only heading allowed: "## Citation" at the end

CRITICAL JSON FORMATTING REQUIREMENTS:
- Return ONLY valid JSON - the entire response must be parseable by a JSON parser
- The "markdown_summary" field MUST be a single-line JSON string with ALL special characters escaped:
  * Newlines: Use \\n (backslash followed by 'n'), NOT actual line breaks
  * Double quotes: Use \\" (backslash followed by quote)
  * Backslashes: Use \\\\ (double backslash)
  * Example (correct): {{"markdown_summary": "First para.\\n\\nSecond para with \\"quotes\\".\\n\\n## Citation\\n..."}}
  * Example (WRONG): {{"markdown_summary": "First para.\n\nSecond para..."}}  ← This breaks JSON parsing!
- Test that your JSON is valid before returning it
- The entire output must be ONE JSON object on ONE logical line (strings can be long but must be escaped)

OUTPUT FORMAT (must be valid JSON). Always return JSON as the output, never free text. JSON should follow the following structure:
```json
{{
  "citation_text": "{citation}",
  "metadata": {{
    "title": "Full title of the work",
    "authors": ["Author 1", "Author 2"],
    "publication_date": "YYYY or YYYY-MM-DD",
    "publisher": "Publisher name or null",
    "publication_venue": "Journal/Conference name or null",
    "pages": "page range or null",
    "volume": "volume number or null",
    "issue": "issue number or null",
    "doi": "DOI if available or null",
    "isbn": "ISBN if available or null",
    "type": "book|journal_article|book_chapter|report|conference_paper|other"
  }},
  "urls": {json.dumps(urls)},
  "markdown_summary": "Review-style summary in flowing paragraphs in mardown format including markdown tags..."
}}
```
"""

        try:
            self._logger.debug(f"📝 Generating structured summary for: {citation[:60]}...")

            # For Gemini, always enable Google Search when we have URLs
            # (web scraping is often partial or fails, so let Gemini fetch directly)
            enable_search = self.llm_provider == 'gemini' and bool(urls)

            if self.llm_provider == 'openai':
                generated_text = await self._generate_with_openai(prompt)
            elif self.llm_provider == 'gemini':
                generated_text = await self._generate_with_gemini(prompt, enable_search=enable_search)
            else:
                raise ValueError(f"Unsupported LLM provider: {self.llm_provider}")

            # Log the raw response for debugging
            self._logger.info(f"   📄 Raw API response ({len(generated_text)} chars):")
            self._logger.info(f"   ─────────── START OF RESPONSE ───────────")
            self._logger.info(generated_text)
            self._logger.info(f"   ─────────── END OF RESPONSE ───────────")

            # Try to extract JSON from response
            # First, try markdown code block (lenient pattern - closing backticks optional)
            json_pattern = r'```json\s*(.*?)(?:\s*```|$)'
            json_matches = re.findall(json_pattern, generated_text, re.DOTALL | re.IGNORECASE)

            self._logger.info(f"   🔍 Looking for JSON blocks in response...")
            self._logger.info(f"   📊 Found {len(json_matches)} JSON markdown block(s)")

            json_text = None

            if json_matches:
                self._logger.info(f"   📦 Using JSON from markdown block ({len(json_matches[0])} chars)")
                json_text = json_matches[0].strip()
            else:
                # Fallback: Try to parse entire response as JSON
                self._logger.warning("   ⚠️  No markdown JSON block found, trying to parse entire response as JSON")
                json_text = generated_text.strip()

            # Check for truncation (incomplete JSON after extraction)
            if not json_text.strip().endswith('}'):
                self._logger.warning(f"   ⚠️  JSON appears truncated (doesn't end with }})")
                self._logger.warning(f"   💡 Last 100 chars: ...{json_text[-100:]}")
                raise ValueError("API response was truncated - increase max_output_tokens or reduce summary length")

            # Log what we're trying to parse
            self._logger.info(f"   ─────────── JSON TO PARSE ───────────")
            self._logger.info(json_text[:1000])  # First 1000 chars
            self._logger.info(f"   ─────────── END JSON (truncated) ───────────")

            try:
                # Try standard JSON parsing first
                result = json.loads(json_text)
                self._logger.info(f"   ✅ Standard JSON parsing succeeded")

            except json.JSONDecodeError as e:
                self._logger.warning(f"   ⚠️  Standard JSON parsing failed: {e}")
                self._logger.info(f"   🔧 Attempting lenient JSON repair...")

                # Lenient parsing: fix common issues with model-generated JSON
                try:
                    # Strategy: Find markdown_summary field and escape its content
                    # The field starts with: "markdown_summary": "
                    # The field ends with: "\n} (last closing brace of the JSON object)

                    # Find where markdown_summary value starts
                    summary_start_pattern = r'"markdown_summary"\s*:\s*"'
                    summary_match = re.search(summary_start_pattern, json_text, re.DOTALL)

                    if not summary_match:
                        raise ValueError("Could not find markdown_summary field")

                    # Everything before the summary value
                    before_summary = json_text[:summary_match.end()]

                    # Everything after the summary field starts
                    after_summary_start = json_text[summary_match.end():]

                    # Find the end of the JSON object (last })
                    # The summary ends just before the closing }
                    # Look for the pattern: "\n} or "} at the very end
                    # We need to find the LAST unescaped quote before the final }

                    # Find last closing brace
                    if not after_summary_start.rstrip().endswith('}'):
                        raise ValueError("JSON doesn't end with }")

                    # Work backwards from end to find where summary ends
                    # Pattern: any amount of whitespace, then }, optionally more whitespace
                    end_pattern = r'"\s*\}?\s*$'

                    # Find all quote positions that could be the end
                    quote_positions = [m.start() for m in re.finditer(r'"', after_summary_start)]

                    if not quote_positions:
                        raise ValueError("Could not find closing quote for markdown_summary")

                    # Try quotes from last to first (most likely to be the closing quote)
                    repaired_json = None
                    for quote_pos in reversed(quote_positions):
                        # Extract summary content up to this quote
                        summary_content = after_summary_start[:quote_pos]
                        after_summary = after_summary_start[quote_pos:]

                        # Escape the summary content
                        # 1. Replace literal newlines with \n
                        summary_escaped = summary_content.replace('\n', '\\n')
                        # 2. Replace literal tabs with \t
                        summary_escaped = summary_escaped.replace('\t', '\\t')
                        # 3. Escape unescaped quotes (but not already escaped)
                        summary_escaped = re.sub(r'(?<!\\)"', r'\\"', summary_escaped)
                        # 4. Fix any double-escaping we might have introduced
                        summary_escaped = summary_escaped.replace('\\\\n', '\\n')
                        summary_escaped = summary_escaped.replace('\\\\t', '\\t')

                        # Reconstruct
                        candidate_json = before_summary + summary_escaped + after_summary

                        # Try parsing
                        try:
                            result = json.loads(candidate_json)
                            repaired_json = candidate_json
                            self._logger.info(f"   ✅ Lenient JSON repair succeeded!")
                            break
                        except json.JSONDecodeError:
                            continue  # Try next quote position

                    if repaired_json is None:
                        raise ValueError("Could not repair JSON by escaping summary field")

                except Exception as repair_error:
                    self._logger.error(f"   ❌ Lenient JSON repair also failed: {repair_error}")
                    # Log original error details
                    self._logger.error(f"   📍 Original error location: line {e.lineno}, column {e.colno}")
                    self._logger.error(f"   🔍 Problematic text around error:")
                    lines = json_text.split('\n')
                    if e.lineno and e.lineno <= len(lines):
                        start_line = max(0, e.lineno - 3)
                        end_line = min(len(lines), e.lineno + 2)
                        for i in range(start_line, end_line):
                            marker = " >>> " if i == e.lineno - 1 else "     "
                            self._logger.error(f"{marker}Line {i+1}: {lines[i]}")
                    raise e

            # Validate required fields
            if 'metadata' in result and 'markdown_summary' in result:
                self._logger.info(f"   ✅ Successfully parsed JSON with all required fields")
                self._logger.info(f"   � Metadata keys: {list(result.get('metadata', {}).keys())}")
                summary_length = len(result.get('markdown_summary', '').split())
                self._logger.info(f"   📝 Summary length: {summary_length} words")
                return result
            else:
                self._logger.warning("   ⚠️  Missing required fields in parsed JSON")
                self._logger.warning(f"   🔑 Found keys: {list(result.keys())}")
                raise ValueError("Missing required fields")

        except Exception as e:
            self._logger.error(f"   ❌ Summary generation failed: {e}")
            self._logger.error(f"   🔧 Exception type: {type(e).__name__}")
            import traceback
            self._logger.error(f"   📚 Full traceback:")
            self._logger.error(traceback.format_exc())

            # Return minimal error structure
            self._logger.warning(f"   ⚠️  Returning error structure for citation")
            return {
                'citation_text': citation,
                'metadata': {
                    'title': 'Error extracting metadata',
                    'authors': [],
                    'publication_date': None,
                    'publisher': None,
                    'publication_venue': None,
                    'pages': None,
                    'volume': None,
                    'issue': None,
                    'doi': None,
                    'isbn': None,
                    'type': 'unknown'
                },
                'urls': urls,
                'markdown_summary': f"# Error\n\nFailed to generate summary for citation.\n\n**Citation:** {citation}\n\n**Error:** {str(e)}"
            }

    async def research_citations(self, citations: List[str], extract_embedded: bool = False) -> Dict[str, Any]:
        """Research a list of citations and generate structured summaries with metadata.

        Args:
            citations: List of citation strings or text containing embedded citations
            extract_embedded: If True, extract citations from text in the list

        Returns:
            Dictionary with consolidated structure:
            {
                'request_uuid': str,
                'timestamp': str,
                'total_citations': int,
                'citations': [
                    {
                        'citation_text': str,
                        'metadata': {
                            'title': str,
                            'authors': List[str],
                            'publication_date': str,
                            'publisher': str,
                            'publication_venue': str,
                            'pages': str,
                            'volume': str,
                            'issue': str,
                            'doi': str,
                            'isbn': str,
                            'type': str
                        },
                        'urls': List[str],
                        'markdown_summary': str
                    },
                    ...
                ]
            }
        """
        request_uuid = str(uuid.uuid4())
        timestamp = datetime.utcnow().isoformat() + 'Z'

        self._logger.info(f"🔬 Research request {request_uuid} - Processing {len(citations)} citations")

        # Extract embedded citations if requested
        if extract_embedded:
            all_extracted = []
            for item in citations:
                all_extracted.extend(self.extract_citations(item))
            citations = all_extracted
            self._logger.info(f"   📖 Extracted {len(citations)} citations from provided text")

        citation_results = []

        # Process each citation
        for i, citation in enumerate(citations, 1):
            self._logger.info(f"   [{i}/{len(citations)}] Processing: {citation[:80]}...")

            # Search for URLs
            urls = await self.search_citation(citation)

            # Generate structured summary with metadata
            result = await self.generate_summary(citation, urls)
            citation_results.append(result)

        # Build consolidated response
        response = {
            'request_uuid': request_uuid,
            'timestamp': timestamp,
            'total_citations': len(citation_results),
            'citations': citation_results
        }

        self._logger.info(f"✅ Completed research request {request_uuid}")
        self._logger.info(f"   📊 Processed {len(citation_results)} citations")

        total_urls = sum(len(c.get('urls', [])) for c in citation_results)
        self._logger.info(f"   🔗 Found {total_urls} total URLs")

        return response

    def set_web_searcher(self, web_searcher):
        """Set or update the web searcher client.

        Args:
            web_searcher: Web search client (e.g., DocumentWebSearcher)
        """
        self.web_searcher = web_searcher
        self._logger.debug("🔍 Web searcher configured")
