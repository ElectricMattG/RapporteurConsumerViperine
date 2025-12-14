"""
Markdown Chunk Post-Processor

Filters noisy chunks and merges small adjacent chunks into larger,
more meaningful text segments.
"""

import hashlib
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
                'newsletter', 'sign up', 'log in', 'register'
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
                'tags', 'resources', 'related', 'main navigation'
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

    def __init__(self, config: Optional[ChunkMergerConfig] = None):
        self.config = config or ChunkMergerConfig()

        # Compile patterns
        self.standalone_link_pattern = re.compile(r'^\s*\[([^\]]*)\]\([^)]+\)\s*$')
        self.html_comment_pattern = re.compile(r'<!--.*?-->', re.DOTALL)
        self.date_pattern = re.compile(
            r'^\s*\d{1,2}\s+(January|February|March|April|May|June|July|'
            r'August|September|October|November|December)\s+\d{4}\s*$'
        )
        self.sentence_end_pattern = re.compile(r'[.!?]\s+')

    def process_chunks(self, chunks: List[Dict]) -> List[Dict]:
        """Filter and merge chunks."""
        # Step 1: Filter noise
        filtered_chunks = self._filter_noise(chunks)

        # Step 2: Merge small chunks
        merged_chunks = self._merge_chunks(filtered_chunks)

        # Step 3: Update metadata
        self._update_chunk_metadata(merged_chunks)

        return merged_chunks

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

        # Keep numbered bold headers like **1. Title** or **2.  Title** FIRST (structural elements)
        # Check this before navigation patterns since some headers contain navigation words
        import re
        if re.match(r'^\*\*\d+\.\s+.+\*\*', content_stripped):
            return False

        # Check for navigation/footer patterns (applies to all content including headers)
        # Extract header text for comparison
        if content_stripped.startswith('#'):
            header_text = content_stripped.lstrip('#').strip().lower()
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

        # Keep markdown headers (they provide structure) if not navigation/footer
        if content_stripped.startswith('#'):
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

        # Use first chunk as base
        base_chunk = merge_data['chunks_merged'][0]

        # Generate new chunk ID
        chunk_id = f"chunk_{hashlib.md5(content.encode()).hexdigest()[:12]}"

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
                'section_hierarchy': base_chunk.get('chunk_metadata', {}).get('section_hierarchy', []),
                'parent_section': base_chunk.get('chunk_metadata', {}).get('parent_section'),
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
