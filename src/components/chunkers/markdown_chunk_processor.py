"""
Markdown Chunk Post-Processor

Filters noisy chunks and merges small adjacent chunks into larger,
more meaningful text segments.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class ChunkMergerConfig:
    """Configuration for chunk filtering and merging."""
    # Noise filtering
    min_words: int = 3  # More permissive - keep phrases with 3+ words
    max_chunk_words: int = 350

    # Navigation/noise keywords
    noise_keywords: List[str] = None

    # Patterns to filter
    filter_standalone_links: bool = True
    filter_html_comments: bool = True
    filter_date_only: bool = True

    # YouTube specific
    filter_youtube_headers: bool = True
    youtube_header_patterns: List[str] = None

    def __post_init__(self):
        if self.noise_keywords is None:
            self.noise_keywords = [
                'cookies', 'cookie', 'preferences', 'accept', 'decline',
                'privacy', 'terms',
                'subscribe', 'follow', 'share', 'tweet', 'like',
                'newsletter', 'sign up', 'log in', 'register',
                'advertisement', 'homepage'
            ]

        # Navigation and footer patterns (will match headers and content)
        if not hasattr(self, 'navigation_patterns'):
            self.navigation_patterns = [
                'quick links', 'footer links', 'my account', 'connect with',
                'contact us', 'contact cbc', 'about us', 'about cbc', 'social sharing',
                'related stories', 'services', 'accessibility',
                'doing business with', 'audience relations',
                'journalistic standards', 'corrections and clarifications',
                'being black in canada', '/news/', '/sports/', '/radio/', '/music/',
                'youtube video transcript', 'share', 'recent', 'keep in touch',
                'support', 'privacy statement', 'cookie', 'technical storage',
                'your voice at the united nations', 'copyright ©', 'un watch in the news',
                'tags', 'resources', 'related', 'main navigation',
                # Common navigation/sidebar sections
                'most viewed', 'most popular', 'most read', 'trending now',
                'top stories', 'latest news', 'more from', 'more on',
                'follow us on', 'us politics', 'uk politics', 'climate crisis',
                'middle east', 'global development',
                # Citation/metadata headers that shouldn't be content
                'citation', 'unknown', 'jpost.com', 'theguardian.com',
                # Wikipedia UI elements
                'table of contents', 'contents', 'move to sidebar', 'toggle',
                'view source', 'view history', 'what links here', 'upload file',
                'permanent link', 'page information', 'cite this page',
                'shortened url', 'download qr', 'printable version', 'wikimedia commons',
                'wikidata item', 'coordinates', 'from wikipedia', 'bahasa indonesia',
                'bahasa melayu', 'tiếng việt'
            ]

        if self.youtube_header_patterns is None:
            self.youtube_header_patterns = [
                r'^#\s*YouTube Video Transcript',
                r'^\*\*Title:\*\*',
                r'^\*\*Channel:\*\*',
                r'^\*\*Upload Date:\*\*',
                r'^\*\*Duration:\*\*',
                r'^\*\*Video ID:\*\*',
                r'^\*\*Source:\*\*'
            ]


class MarkdownChunkProcessor:
    """Post-process markdown chunks: filter noise and merge small chunks."""

    def __init__(self, config: Optional[ChunkMergerConfig] = None, llm_client=None, enable_llm_validation: bool = True):
        self.config = config or ChunkMergerConfig()
        self.llm_client = llm_client

        # Check environment variable first, then fall back to parameter
        import os
        env_llm_validation = os.getenv('ENABLE_LLM_VALIDATION', '').lower()
        if env_llm_validation in ('true', '1', 'yes'):
            self.enable_llm_validation = True
        elif env_llm_validation in ('false', '0', 'no'):
            self.enable_llm_validation = False
        else:
            self.enable_llm_validation = enable_llm_validation

        # Initialize logger
        import logging
        self.logger = logging.getLogger(__name__)

        if not self.enable_llm_validation:
            self.logger.info("⚠️ LLM validation is DISABLED")
        else:
            self.logger.info("✅ LLM validation is ENABLED")

        # Compile patterns
        self.standalone_link_pattern = re.compile(r'^\s*\[([^\]]*)\]\([^)]+\)\s*$')
        self.html_comment_pattern = re.compile(r'<!--.*?-->', re.DOTALL)
        self.date_pattern = re.compile(
            r'^\s*\d{1,2}\s+(January|February|March|April|May|June|July|'
            r'August|September|October|November|December)\s+\d{4}\s*$'
        )
        self.sentence_end_pattern = re.compile(r'[.!?]\s+')
        self.markdown_link_pattern = re.compile(r'\[([^\]]+)\]\([^\)]+\)')

    @staticmethod
    def _strip_markdown_links(text: str) -> str:
        """
        Strip markdown links from text, extracting just the anchor text.

        Converts [anchor text](url) to just "anchor text"

        Args:
            text: Text potentially containing markdown links

        Returns:
            Text with markdown links replaced by their anchor text
        """
        if not text:
            return text

        # Pattern: [anchor text](url)
        # Replace with just the anchor text
        pattern = re.compile(r'\[([^\]]+)\]\([^\)]+\)')
        cleaned = pattern.sub(r'\1', text)

        return cleaned

    @staticmethod
    def _strip_leading_headers(text: str) -> str:
        """
        Strip leading markdown headers from content.

        Headers like "# Title" or "## Section" are already in section_hierarchy
        and parent_section, so they shouldn't be duplicated in content.

        Args:
            text: Content text that may start with headers

        Returns:
            Text with leading headers removed
        """
        if not text:
            return text

        lines = text.split('\n')
        # Skip leading header lines (lines starting with #)
        content_start = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                content_start = i
                break

        # Return content without leading headers
        return '\n'.join(lines[content_start:]).strip()

    async def process_chunks(self, chunks: List[Dict], source_url: str = "") -> List[Dict]:
        """Filter and merge chunks with optional LLM validation."""
        # Step 1: Filter noise
        filtered_chunks = self._filter_noise(chunks)

        # Step 2: Merge small chunks
        merged_chunks = self._merge_chunks(filtered_chunks)

        # Step 3: LLM validation on merged chunks (optional)
        if self.llm_client and self.enable_llm_validation and merged_chunks:
            try:
                validated_chunks = []
                for i in range(0, len(merged_chunks), 2):
                    batch = merged_chunks[i:i+2]
                    validation_results = await self._validate_content_batch(batch, source_url)
                    if validation_results:  # Only apply if we got results
                        batch_validated = self._apply_validation_results(batch, validation_results)
                        validated_chunks.extend(batch_validated)
                    else:
                        # Validation failed - keep originals unvalidated
                        self.logger.warning(f"Batch validation returned no results - keeping {len(batch)} chunks unvalidated")
                        validated_chunks.extend(batch)
                merged_chunks = validated_chunks
            except Exception as e:
                self.logger.error(f"LLM validation failed: {str(e)[:100]}")
                # Continue with unvalidated chunks on error

        # Step 4: Update metadata
        self._update_chunk_metadata(merged_chunks)

        return merged_chunks

    async def _validate_content_batch(self, chunks: List[Dict], source_url: str = "") -> List[Dict]:
        """Validate chunk content using LLM."""
        if not self.llm_client or not self.enable_llm_validation:
            return [{"chunk_id": c.get('chunk_metadata', {}).get('chunk_id', f'chunk_{i}'),
                    "classification": "valid", "extracted_content": None, "reason": "validation_disabled"}
                    for i, c in enumerate(chunks)]

        chunks = chunks[:5]  # Batch size limit - smaller batches for reliable parsing

        # Build chunks data
        chunks_data = []
        for i, chunk in enumerate(chunks):
            content = chunk.get('content', '').strip()
            content = content[:1000].replace('\n', ' ').replace('\r', '')
            chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id', f'chunk_{i}')
            chunks_data.append({"chunk_id": chunk_id, "content": content})

        # Build prompt
        prompt = f"""Classify these content chunks as VALID (substantive news/legal content), MIXED (has content but also UI noise or other lists of content headers not directly related to the articles it;s), or INVALID (pure navigation/UI or lists of other articles with NO substantive content).

CLASSIFICATION RULES:
1. VALID: Coherent paragraph(s) about ONE topic with depth and context, minimal UI noise
2. MIXED: Contains ANY news/article content (even 1 sentence) mixed with UI elements - ALWAYS extract the content. Or perhaps intermediate elements with recommended content, or sign-up and other linked activity for a web-site.
3. INVALID: ONLY for pure UI with ZERO substantive information - navigation bars, login forms, footers. Sign-up pages and or lists or related content, but not the articles main body. Contains membership notification, photo caption, and multiple article teasers. Multiple article teasers and photo captions are present. Contains multiple article teasers and photo captions.

CRITICAL DECISION RULE:
- Does the chunk contain ANY sentences about news events, facts, or article information? → MIXED (extract it)
- Is it ONLY navigation links, buttons, or form fields with NO article content? → INVALID (discard)
- When in doubt, classify as MIXED and extract what you can

When extracting content for MIXED chunks, include ALL substantive information:
- Article text (keep it)
- Article teasers/headlines (keep them)
- Photo captions with context (keep them)
- Related article titles (keep them as they are content)
But exclude:
- Navigation menus
- "Sign in" / "Subscribe" UI
- Social media buttons
- Cookie notices

INVALID EXAMPLES (ONLY pure UI with NO content):

Example 1 - Pure navigation:
"Home | News | Politics | Business | Subscribe | Sign In | Facebook Twitter Instagram"
Classification: INVALID
Reason: Pure navigation menu, zero article content

Example 2 - Pure login form:
"Enter your email: _____ Password: _____ Remember me [ ] Forgot password? Sign Up"
Classification: INVALID
Reason: Pure form UI, zero article content

MIXED EXAMPLES (ANY content + UI):

Example 1 - News + navigation:
"Netanyahu announces ground offensive in Gaza. Related: Iran tensions | Syria updates | Subscribe to newsletter"
Classification: MIXED
Extracted: "Netanyahu announces ground offensive in Gaza. Related: Iran tensions | Syria updates"
Reason: Has news content and related topics - extract both, discard subscribe button

Example 2 - Photo caption + article + UI:
"Prime Minister Benjamin Netanyahu at press conference, Oct 28. Israeli forces launched strikes in Gaza targeting Hamas. Click here to share: Facebook Twitter"
Classification: MIXED
Extracted: "Prime Minister Benjamin Netanyahu at press conference, Oct 28. Israeli forces launched strikes in Gaza targeting Hamas."
Reason: Has photo caption and news - extract both, discard social buttons

Example 3 - Multiple article teasers:
"5:18pm Gaza strikes continue 5:01pm Syrian conflict 4:50pm Lebanon tensions Sign in | Subscribe"
Classification: MIXED
Extracted: "5:18pm Gaza strikes continue 5:01pm Syrian conflict 4:50pm Lebanon tensions"
Reason: Multiple article teasers ARE content - extract all, discard sign in UI

OUTPUT FORMAT - wrap your JSON array in markdown code block:
```json
[{{"chunk_id":"id","classification":"valid|mixed|invalid","extracted_content":"text or null","reason":"one simple sentence"}}]
```

EXAMPLES:

```json
[{{"chunk_id":"abc123","classification":"valid","extracted_content":null,"reason":"Coherent news story."}}]
```

```json
[{{"chunk_id":"def456","classification":"invalid","extracted_content":null,"reason":"Pure navigation menu."}}]
```

```json
[{{"chunk_id":"ghi789","classification":"mixed","extracted_content":"Netanyahu announced the war has entered a second phase.","reason":"News content with trailing UI links."}}]
```

CRITICAL RULES:
- Wrap JSON in ```json code block
- Reason: ONE simple sentence, under 10 words
- Use null (not "null") for extracted_content when not extracting
- Keep all strings on single lines
- If chunk lists 4+ unrelated topics = INVALID

Now classify these chunks:
"""
        for i, chunk_data in enumerate(chunks_data):
            prompt += f"\n{i+1}. ID: {chunk_data['chunk_id']}\nContent: {chunk_data['content'][:800]}\n"

        try:
            response = await self.llm_client.generate_content(
                prompt=prompt,
                temperature=0.0,
                max_tokens=16000  # Increased to handle batches with multiple long extracted_content strings
            )

            # Log full response for debugging
            self.logger.info(f"LLM raw response (length: {len(response)} chars): {response}")

            # Pre-process response - extract JSON from markdown code blocks or pure JSON
            import re
            cleaned_response = response.strip()

            # Try multiple patterns to extract JSON
            # Pattern 1: ```json ... ```
            markdown_match = re.search(r'```json\s*\n(.*?)```', cleaned_response, re.DOTALL)
            if markdown_match:
                cleaned_response = markdown_match.group(1).strip()
                self.logger.info(f"✅ Extracted JSON from ```json block (length: {len(cleaned_response)} chars)")
            else:
                # Pattern 2: ``` ... ``` (without json keyword)
                markdown_match = re.search(r'```\s*\n(.*?)```', cleaned_response, re.DOTALL)
                if markdown_match:
                    cleaned_response = markdown_match.group(1).strip()
                    self.logger.info(f"✅ Extracted JSON from ``` block (length: {len(cleaned_response)} chars)")
                else:
                    # Pattern 3: Try to find JSON array directly (starts with [ and ends with ])
                    # This handles pure JSON responses without markdown
                    if cleaned_response.startswith('[') and cleaned_response.endswith(']'):
                        self.logger.info(f"✅ Using response as pure JSON array (length: {len(cleaned_response)} chars)")
                    else:
                        self.logger.warning("⚠️ No markdown code block found, trying to extract JSON array")
                        # Fallback: try to find JSON array within the response
                        array_match = re.search(r'(\[\s*\{.*?\}\s*\])', cleaned_response, re.DOTALL)
                        if array_match:
                            cleaned_response = array_match.group(1).strip()
                            self.logger.info(f"✅ Extracted JSON array from response (length: {len(cleaned_response)} chars)")
                        else:
                            self.logger.error(f"❌ Could not extract JSON. Full response: {cleaned_response}")

            if not cleaned_response:
                self.logger.error("❌ Cleaned response is empty after extraction!")
                return []

            # Try to parse JSON - handle malformed responses
            try:
                results = json.loads(cleaned_response)
            except json.JSONDecodeError as je:
                # Try multiple repair strategies
                self.logger.warning(f"Initial JSON parse failed, attempting repair: {str(je)[:100]}")

                # Strategy 1: Aggressively remove ALL newlines and whitespace between tokens
                try:
                    # Remove newlines everywhere
                    fixed_response = cleaned_response.replace('\n', ' ').replace('\r', ' ').replace('\\n', ' ').replace('\\r', ' ').replace('\t', ' ')
                    # Collapse multiple spaces
                    fixed_response = re.sub(r'\s+', ' ', fixed_response).strip()
                    results = json.loads(fixed_response)
                    self.logger.info("✅ JSON repair successful (newline removal)")
                except json.JSONDecodeError:
                    # Strategy 2: Extract array and try again
                    try:
                        match = re.search(r'\[.*\]', cleaned_response, re.DOTALL)
                        if match:
                            json_str = match.group(0)
                            # Remove newlines
                            json_str = json_str.replace('\n', ' ').replace('\r', ' ').replace('\\n', ' ').replace('\\r', ' ').replace('\t', ' ')
                            json_str = re.sub(r'\s+', ' ', json_str).strip()
                            results = json.loads(json_str)
                            self.logger.info("✅ JSON repair successful (array extraction)")
                        else:
                            raise ValueError("No JSON array found")
                    except Exception:
                        # Strategy 3: Try to fix common patterns - replace literal newlines in quoted strings
                        try:
                            # This regex finds string values and replaces newlines within them
                            def fix_string(match):
                                content = match.group(1)
                                # Replace newlines with spaces inside the captured string
                                return '"' + content.replace('\n', ' ').replace('\r', ' ') + '"'

                            fixed_response = re.sub(r'"([^"]*(?:\n[^"]*)*)"', fix_string, cleaned_response)
                            # Now collapse remaining whitespace
                            fixed_response = re.sub(r'\s+', ' ', fixed_response).strip()
                            results = json.loads(fixed_response)
                            self.logger.info("✅ JSON repair successful (string newline fix)")
                        except Exception as final_error:
                            self.logger.error(f"All JSON repair strategies failed: {final_error}")
                            return []

            if not isinstance(results, list):
                self.logger.warning(f"LLM returned non-list: {type(results)}")
                return [{"chunk_id": cd['chunk_id'], "classification": "valid",
                        "extracted_content": None, "reason": "format_error"} for cd in chunks_data]

            # Match by chunk_id
            results_by_id = {r.get('chunk_id'): r for r in results if isinstance(r, dict) and 'chunk_id' in r}
            matched_results = []
            for chunk_data in chunks_data:
                result = results_by_id.get(chunk_data['chunk_id'])
                if result:
                    matched_results.append(result)
                else:
                    matched_results.append({
                        "chunk_id": chunk_data['chunk_id'],
                        "classification": "valid",
                        "extracted_content": None,
                        "reason": "missing_from_llm"
                    })

            self.logger.info(f"✅ LLM validated {len(matched_results)} chunks")
            return matched_results

        except json.JSONDecodeError as e:
            self.logger.error(f"JSON parse failed: {e}\nResponse: {response[:500]}")
            # Return empty list to trigger fallback - don't assume valid!
            return []
        except asyncio.TimeoutError:
            self.logger.warning(f"LLM timeout for {len(chunks_data)} chunks")
            return [{"chunk_id": cd['chunk_id'], "classification": "valid",
                    "extracted_content": None, "reason": "timeout"} for cd in chunks_data]
        except Exception as e:
            self.logger.error(f"LLM error: {str(e)[:100]}")
            return [{"chunk_id": cd['chunk_id'], "classification": "valid",
                    "extracted_content": None, "reason": "error"} for cd in chunks_data]

    def _apply_validation_results(self, chunks: List[Dict], validation_results: List[Dict]) -> List[Dict]:
        """Apply validation results to chunks."""
        validation_by_id = {v.get('chunk_id'): v for v in validation_results if v.get('chunk_id')}

        filtered_chunks = []
        invalid_count = 0

        for chunk in chunks:
            chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
            validation = validation_by_id.get(chunk_id, {})
            classification = validation.get('classification', 'valid').lower()

            if classification == 'invalid':
                invalid_count += 1
                self.logger.debug(f"Filtered invalid chunk: {validation.get('reason', '')}")
                continue
            elif classification == 'mixed':
                extracted = validation.get('extracted_content')
                # Always keep mixed chunks and save metadata
                chunk['chunk_metadata']['validation_status'] = 'mixed'
                chunk['chunk_metadata']['validation_reason'] = validation.get('reason', '')
                chunk['chunk_metadata']['extracted_content'] = extracted

                # Replace content with cleaned extraction if available
                if extracted and extracted.strip():
                    # Check for truncation (ends mid-word without punctuation)
                    if extracted and len(extracted) > 20:
                        last_chars = extracted[-20:].strip()
                        ends_with_word = last_chars and last_chars[-1].isalnum()
                        if ends_with_word:
                            self.logger.warning(f"⚠️ Extracted content may be truncated (ends with '{extracted[-30:]}') for chunk {chunk_id}")

                    chunk['content'] = extracted.strip()
                    self.logger.debug(f"Replaced content with extraction: {validation.get('reason', '')}")
                else:
                    self.logger.warning(f"Mixed chunk kept with original content (no extraction): {validation.get('reason', '')}")

                filtered_chunks.append(chunk)
            else:  # valid
                chunk['chunk_metadata']['validation_status'] = 'valid'
                chunk['chunk_metadata']['validation_reason'] = validation.get('reason', '')
                filtered_chunks.append(chunk)

        # Warn if excessive filtering
        if invalid_count > 0 and len(chunks) > 0:
            filter_rate = (invalid_count / len(chunks)) * 100
            if filter_rate > 80:
                self.logger.warning(f"⚠️ High filter rate: {invalid_count}/{len(chunks)} ({filter_rate:.1f}%) marked INVALID - verify not discarding article content")

        return filtered_chunks

    def _filter_noise(self, chunks: List[Dict]) -> List[Dict]:
        """Remove noisy chunks."""
        filtered = []

        for chunk in chunks:
            content = chunk.get('content', '')

            if self._is_noise(content):
                continue

            filtered.append(chunk)

        return filtered

    def _is_noise(self, content: str) -> bool:
        """Check if content is noise."""
        content_stripped = content.strip()

        # Empty
        if not content_stripped:
            return True

        content_lower = content_stripped.lower()

        # Filter table of contents patterns
        # TOC usually has many consecutive markdown links with anchor patterns
        toc_patterns = [
            r'\- \[\d+\.?\d*\s+[^\]]+\]\(#[^\)]+\)',  # - [1 Title](#anchor) or - [1.1 Title](#anchor)
            r'\- \[\d+\s+[^\]]+\]\(#[^\)]+\).*\- \[\d+',  # Multiple consecutive TOC entries
        ]
        import re
        for pattern in toc_patterns:
            if re.search(pattern, content_stripped):
                # Check if content has 3+ TOC-style links (strong TOC signal)
                toc_link_count = len(re.findall(r'\- \[\d+\.?\d*\s+[^\]]+\]\(#', content_stripped))
                if toc_link_count >= 3:
                    return True

        # Keep numbered bold headers like **1. Title** or **2.  Title** FIRST (structural elements)
        # Check this before navigation patterns since some headers contain navigation words
        import re
        if re.match(r'^\*\*\d+\.\s+.+\*\*', content_stripped):
            return False

        # Check for navigation/footer patterns (applies to all content including headers)
        # Extract header text for comparison
        if content_stripped.startswith('#'):
            header_text = content_stripped.lstrip('#').strip().lower()

            # Filter out EMPTY headers (just "#", "##", "###" with no text)
            if not header_text:
                return True

            # Check if header matches navigation patterns
            for pattern in self.config.navigation_patterns:
                if pattern in header_text:
                    return True
        else:
            # For non-headers, only filter if it's a SHORT chunk that's mostly navigation
            # Don't filter long content just because it contains navigation words
            if len(content_stripped.split()) <= 20:  # Only check short chunks
                for pattern in self.config.navigation_patterns:
                    if pattern in content_lower:
                        return True

        # Keep markdown headers (they provide structure) if not navigation/footer and not empty
        if content_stripped.startswith('#'):
            # Only keep if it has actual text after the #
            header_text = content_stripped.lstrip('#').strip()
            if not header_text or len(header_text) < 2:  # No text or single character
                return True
            return False

        # Word count check
        word_count = len(content_stripped.split())
        if word_count < self.config.min_words:
            # Exception: if it has meaningful surrounding text with a link
            if not self._has_contextual_link(content_stripped):
                return True

        # HTML comments
        if self.config.filter_html_comments:
            if self.html_comment_pattern.search(content_stripped):
                return True

        # Standalone links (no surrounding text)
        if self.config.filter_standalone_links:
            if self._is_standalone_link(content_stripped):
                return True

        # Date-only lines
        if self.config.filter_date_only:
            if self.date_pattern.match(content_stripped):
                return True

        # YouTube headers
        if self.config.filter_youtube_headers:
            for pattern in self.config.youtube_header_patterns:
                if re.match(pattern, content_stripped, re.IGNORECASE):
                    return True

        # Navigation keywords (only for very short chunks without other content)
        if word_count <= 5:  # Only filter if 5 words or less
            # Check if it's ONLY the noise keyword (not part of a sentence)
            if any(content_lower == keyword or content_lower.startswith(keyword + ' ') for keyword in self.config.noise_keywords):
                return True

        return False

    def _is_standalone_link(self, content: str) -> bool:
        """Check if content is just a link with no context."""
        # Match: [text](url) with optional whitespace
        match = self.standalone_link_pattern.match(content)
        if not match:
            return False

        # If link text is just underscore or very short, it's standalone
        link_text = match.group(1).strip()
        if not link_text or link_text == '_' or len(link_text) <= 2:
            return True

        return False

    def _has_contextual_link(self, content: str) -> bool:
        """Check if link has meaningful surrounding text."""
        # Remove the link to check surrounding text
        text_without_link = re.sub(r'\[([^\]]*)\]\([^)]+\)', '', content)
        remaining_text = text_without_link.strip()

        # If there's meaningful text around the link (5+ words), keep it
        if len(remaining_text.split()) >= 5:
            return True

        return False

    def _merge_chunks(self, chunks: List[Dict]) -> List[Dict]:
        """Merge adjacent small chunks up to max_chunk_words, respecting section boundaries."""
        if not chunks:
            return []

        import re
        merged = []
        current_merge = None

        for chunk in chunks:
            content = chunk.get('content', '')
            word_count = len(content.split())
            is_header = content.strip().startswith('#')
            # Allow one or more spaces after period in numbered headers: **2.  Title**
            is_numbered_header = bool(re.match(r'^\*\*\d+\.\s+.+\*\*', content.strip()))

            if current_merge is None:
                # Start new merge
                current_merge = {
                    'content': content,
                    'chunks_merged': [chunk],
                    'word_count': word_count,
                    'is_header': is_header or is_numbered_header
                }
            else:
                # If this chunk is a header (markdown or numbered bold), save current merge and start new
                if is_header or is_numbered_header:
                    merged.append(self._create_merged_chunk(current_merge))
                    current_merge = {
                        'content': content,
                        'chunks_merged': [chunk],
                        'word_count': word_count,
                        'is_header': is_header or is_numbered_header
                    }
                    continue

                # If current merge IS a header, don't merge content into it - headers stand alone
                if current_merge.get('is_header'):
                    merged.append(self._create_merged_chunk(current_merge))
                    current_merge = {
                        'content': content,
                        'chunks_merged': [chunk],
                        'word_count': word_count,
                        'is_header': False
                    }
                    continue

                # Check if we can add to current merge
                potential_words = current_merge['word_count'] + word_count

                if potential_words <= self.config.max_chunk_words:
                    # Add to merge
                    current_merge['content'] += '\n\n' + content
                    current_merge['chunks_merged'].append(chunk)
                    current_merge['word_count'] = potential_words
                else:
                    # Check if we should stop at sentence boundary
                    if self._can_add_partial(current_merge['content'], content):
                        partial = self._extract_until_sentence_boundary(
                            content,
                            self.config.max_chunk_words - current_merge['word_count']
                        )
                        if partial:
                            current_merge['content'] += '\n\n' + partial
                            current_merge['word_count'] += len(partial.split())

                    # Save current merge and start new
                    merged.append(self._create_merged_chunk(current_merge))
                    current_merge = {
                        'content': content,
                        'chunks_merged': [chunk],
                        'word_count': word_count,
                        'is_header': is_header
                    }

        # Add final merge
        if current_merge:
            merged.append(self._create_merged_chunk(current_merge))

        return merged

    def _can_add_partial(self, current_content: str, new_content: str) -> bool:
        """Check if we should try to add partial content."""
        # Only add partial if current content ends with incomplete sentence
        if current_content.rstrip().endswith(('.', '!', '?')):
            return False
        return True

    def _extract_until_sentence_boundary(self, content: str, max_words: int) -> Optional[str]:
        """Extract text up to a sentence boundary within word limit."""
        words = content.split()
        if len(words) <= max_words:
            return content

        # Find sentence boundaries in first max_words
        partial_text = ' '.join(words[:max_words])

        # Find last sentence end
        sentences = self.sentence_end_pattern.split(partial_text)
        if len(sentences) > 1:
            # Return up to last complete sentence
            complete_sentences = sentences[:-1]
            return self.sentence_end_pattern.split(partial_text)[0] + '.'

        return None

    def _create_merged_chunk(self, merge_data: Dict) -> Dict:
        """Create a new chunk from merged data."""
        content = merge_data['content']
        word_count = merge_data['word_count']

        # Strip leading headers from content (they're already in section_hierarchy)
        content = self._strip_leading_headers(content)

        # Flatten content - remove newlines to create single-line chunks
        content = content.replace('\n\n', ' ').replace('\n', ' ')
        # Normalize whitespace
        content = ' '.join(content.split())

        # Use first chunk as base
        base_chunk = merge_data['chunks_merged'][0]

        # Generate new chunk ID
        chunk_id = f"chunk_{hashlib.md5(content.encode()).hexdigest()[:12]}"

        # Get section hierarchy and parent section, clean markdown links
        section_hierarchy = base_chunk.get('chunk_metadata', {}).get('section_hierarchy', [])
        parent_section = base_chunk.get('chunk_metadata', {}).get('parent_section')

        # Strip markdown links from section hierarchy to get clean text
        if section_hierarchy:
            section_hierarchy = [self._strip_markdown_links(s) for s in section_hierarchy]
        if parent_section:
            parent_section = self._strip_markdown_links(parent_section)

        # Build merged chunk
        merged_chunk = {
            'content': content,
            'document_metadata': base_chunk.get('document_metadata', {}),
            'chunk_metadata': {
                'chunk_id': chunk_id,
                'chunk_index': base_chunk.get('chunk_metadata', {}).get('chunk_index', 0),
                'chunk_type': 'merged_paragraph',
                'word_count': word_count,
                'char_count': len(content),
                'line_count': content.count('\n') + 1,
                'section_hierarchy': section_hierarchy,
                'parent_section': parent_section,
                'prev_chunk_id': None,
                'next_chunk_id': None,
                'merged_from_count': len(merge_data['chunks_merged']),
                'start_position': base_chunk.get('chunk_metadata', {}).get('start_position', 0),
                'end_position': base_chunk.get('chunk_metadata', {}).get('end_position', 0),
                'page_number': None,
                'is_first_in_section': False,
                'is_last_in_section': False,
                'contains_urls': '[' in content and '](' in content,
                'contains_references': False,
                'table_rows': None,
                'table_cols': None,
                'image_alt_text': None,
                'list_item_count': None,
                'citation_number': None,
                'citation_type': None,
                'references_citations': [],
                'cited_by_chunks': []
            },
            'processing_timestamp': base_chunk.get('processing_timestamp')
        }

        return merged_chunk

    def _update_chunk_metadata(self, chunks: List[Dict]):
        """Update chunk indices and linking."""
        import time
        timestamp = time.time()

        for i, chunk in enumerate(chunks):
            meta = chunk['chunk_metadata']

            # Update index
            meta['chunk_index'] = i

            # Update links
            if i > 0:
                meta['prev_chunk_id'] = chunks[i-1]['chunk_metadata']['chunk_id']
            else:
                meta['prev_chunk_id'] = None

            if i < len(chunks) - 1:
                meta['next_chunk_id'] = chunks[i+1]['chunk_metadata']['chunk_id']
            else:
                meta['next_chunk_id'] = None

            # Deduplicate section_hierarchy and add numbered headers
            section_hierarchy = meta.get('section_hierarchy', [])[:]
            if section_hierarchy:
                import html
                import re

                # Check if this chunk starts with a numbered header and add it to hierarchy
                content = chunks[i].get('content', '').strip()
                numbered_match = re.match(r'^\*\*\d+\.\s+(.+?)\*\*', content)
                if numbered_match:
                    # Extract the full header text with number
                    header_text = numbered_match.group(0).strip('*').strip()
                    # Add to section hierarchy if not already there
                    if not section_hierarchy or section_hierarchy[-1] != header_text:
                        section_hierarchy.append(header_text)

                # Normalize for comparison: unescape HTML + normalize unicode apostrophes/quotes
                def normalize(s):
                    s = html.unescape(s)
                    s = s.replace('\u2019', "'").replace('\u2018', "'")
                    s = s.replace('\u201b', "'").replace('\u201a', "'")
                    s = s.replace('\u201c', '"').replace('\u201d', '"')
                    s = s.replace('\u201e', '"').replace('\u201f', '"')
                    return s

                normalized = [normalize(s) for s in section_hierarchy]

                # Find indices to keep (remove consecutive duplicates)
                indices_to_keep = [0]
                for j in range(1, len(normalized)):
                    if normalized[j] != normalized[indices_to_keep[-1]]:
                        indices_to_keep.append(j)

                # Apply deduplication
                meta['section_hierarchy'] = [section_hierarchy[j] for j in indices_to_keep]

            # Update timestamp
            chunk['processing_timestamp'] = timestamp

        # Mark first/last in sections
        if chunks:
            chunks[0]['chunk_metadata']['is_first_in_section'] = True
            chunks[-1]['chunk_metadata']['is_last_in_section'] = True
