"""
Simplified Markdown Processor

Walks through markdown, aggregates paragraphs between headers,
filters out short content (< 5 words) and lists.
"""

import hashlib
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class MarkdownProcessorConfig:
    """Configuration for markdown processing."""
    min_paragraph_words: int = 5  # Skip paragraphs with fewer words
    max_chunk_words: int = 500  # Max words per aggregated chunk


class SimpleMarkdownProcessor:
    """Simplified markdown processor - walks and aggregates paragraphs."""

    def __init__(self, config: MarkdownProcessorConfig):
        self.config = config
        self.header_pattern = re.compile(r'^(#{1,6})\s+(.+)$')
        self.list_pattern = re.compile(r'^\s*[-*+]\s+|^\s*\d+\.\s+')

    def process(self, markdown_text: str, document_metadata: Dict) -> List[Dict]:
        """Process markdown and return aggregated chunks."""
        lines = markdown_text.split('\n')
        chunks = []

        # Track current header context
        header_stack = []  # Stack of (level, text) tuples
        current_section_paragraphs = []
        current_section_words = 0
        chunk_index = 0

        for line in lines:
            line_stripped = line.strip()

            # Skip empty lines
            if not line_stripped:
                continue

            # Check for header
            header_match = self.header_pattern.match(line_stripped)
            if header_match:
                # Save accumulated paragraphs before starting new section
                if current_section_paragraphs:
                    chunk = self._create_chunk(
                        paragraphs=current_section_paragraphs,
                        header_stack=header_stack,
                        chunk_index=chunk_index,
                        document_metadata=document_metadata
                    )
                    if chunk:
                        chunks.append(chunk)
                        chunk_index += 1
                    current_section_paragraphs = []
                    current_section_words = 0

                # Update header stack
                level = len(header_match.group(1))
                text = header_match.group(2).strip()

                # Pop headers of same or higher level
                while header_stack and header_stack[-1][0] >= level:
                    header_stack.pop()

                # Add new header
                header_stack.append((level, text))
                continue

            # Skip list items
            if self.list_pattern.match(line_stripped):
                continue

            # Check if it's a valid paragraph (5+ words)
            word_count = len(line_stripped.split())
            if word_count >= self.config.min_paragraph_words:
                current_section_paragraphs.append(line_stripped)
                current_section_words += word_count

        # Save final accumulated content
        if current_section_paragraphs:
            chunk = self._create_chunk(
                paragraphs=current_section_paragraphs,
                header_stack=header_stack,
                chunk_index=chunk_index,
                document_metadata=document_metadata
            )
            if chunk:
                chunks.append(chunk)

        # Link chunks together and add timestamps
        import time
        timestamp = time.time()

        for i in range(len(chunks)):
            meta = chunks[i]['chunk_metadata']
            if i > 0:
                meta['prev_chunk_id'] = chunks[i-1]['chunk_metadata']['chunk_id']
            if i < len(chunks) - 1:
                meta['next_chunk_id'] = chunks[i+1]['chunk_metadata']['chunk_id']
            chunks[i]['processing_timestamp'] = timestamp

        return chunks

    def _create_chunk(self, paragraphs: List[str], header_stack: List[tuple],
                      chunk_index: int, document_metadata: Dict) -> Optional[Dict]:
        """Create chunk from accumulated paragraphs - matches existing schema."""
        if not paragraphs:
            return None

        # Aggregate paragraphs
        text = "\n\n".join(paragraphs)
        word_count = sum(len(p.split()) for p in paragraphs)
        char_count = len(text)

        chunk_id = f"chunk_{hashlib.md5(f'{text}_{chunk_index}'.encode()).hexdigest()[:12]}"

        # Build section hierarchy - prepend document title
        title = document_metadata.get('title', '')
        section_hierarchy = [title] if title else []
        if header_stack:
            section_hierarchy.extend([h[1] for h in header_stack])

        # Get current section info
        section_title = header_stack[-1][1] if header_stack else title

        # Parent is the last header
        parent_section = section_title

        return {
            'content': text,
            'document_metadata': document_metadata,
            'chunk_metadata': {
                'chunk_id': chunk_id,
                'chunk_index': chunk_index,
                'chunk_type': 'aggregated_paragraph',
                'start_position': 0,
                'end_position': char_count,
                'word_count': word_count,
                'char_count': char_count,
                'line_count': len(paragraphs),
                'page_number': None,
                'section_hierarchy': section_hierarchy,
                'prev_chunk_id': None,
                'next_chunk_id': None,
                'parent_section': parent_section,
                'is_first_in_section': False,
                'is_last_in_section': False,
                'contains_urls': False,
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
            'processing_timestamp': None
        }


class MarkdownDocumentProcessor:
    """Main processor - simplified to walk and aggregate."""

    def __init__(self, config: Optional[MarkdownProcessorConfig] = None):
        self.config = config or MarkdownProcessorConfig()
        self.processor = SimpleMarkdownProcessor(self.config)

    def process(self, markdown_text: str, document_metadata: Dict) -> List[Dict]:
        """Process markdown document into aggregated chunks."""
        return self.processor.process(markdown_text, document_metadata)
