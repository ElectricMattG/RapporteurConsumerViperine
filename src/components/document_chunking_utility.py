#!/usr/bin/env python3
"""
Document Chunking Utility

Advanced document segmentation system for processing docling-extracted markdown into
meaningful chunks with comprehensive metadata. Supports flat structure with navigation
links for efficient storage and retrieval.

Key Features:
- Comprehensive segment type detection (headers, tables, images, lists, etc.)
- Two-level metadata inheritance (document + chunk level)
- Flat structure with document ID references
- Navigation linking via chunk IDs
- Batch processing with progress tracking
- Integration with existing PDF extraction pipeline
"""

import importlib.util
import json
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Add the src directory to the path
src_dir = Path(__file__).parent.parent.parent  # Go up to src/
sys.path.insert(0, str(src_dir))

from config_handler import Base

# Import PDFTextExtractor using importlib to handle folder name with number
pdf_extractor_path = src_dir / "components" / "4_pdf_extractor" / "pdf_text_extractor.py"
spec = importlib.util.spec_from_file_location(
    "pdf_text_extractor",
    str(pdf_extractor_path)
)
pdf_extractor_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pdf_extractor_module)
PDFTextExtractor = pdf_extractor_module.PDFTextExtractor


class SegmentType(Enum):
    """Enumeration of all supported document segment types."""
    HEADER_1 = "header_1"
    HEADER_2 = "header_2"
    HEADER_3 = "header_3"
    HEADER_4 = "header_4"
    HEADER_5 = "header_5"
    HEADER_6 = "header_6"
    PARAGRAPH = "paragraph"
    TABLE = "table"
    IMAGE = "image"
    LIST_ORDERED = "list_ordered"
    LIST_UNORDERED = "list_unordered"
    CODE_BLOCK = "code_block"
    BLOCKQUOTE = "blockquote"
    FOOTNOTE = "footnote"
    ENDNOTE = "endnote"
    CITATION = "citation"
    BIBLIOGRAPHY_ENTRY = "bibliography_entry"
    REFERENCE = "reference"
    CAPTION = "caption"
    PAGE_BREAK = "page_break"
    METADATA_SECTION = "metadata_section"
    HORIZONTAL_RULE = "horizontal_rule"
    UNKNOWN = "unknown"


@dataclass
class DocumentMetadata:
    """Document-level metadata inherited by all chunks."""
    document_id: str
    source_url: str
    extraction_method: str
    file_size_bytes: Optional[int] = None
    total_pages: Optional[int] = None
    extraction_timestamp: Optional[float] = None
    language: Optional[str] = None
    title: Optional[str] = None
    author: Optional[str] = None
    creation_date: Optional[str] = None
    modification_date: Optional[str] = None
    pdf_version: Optional[str] = None
    has_images: Optional[bool] = None
    has_tables: Optional[bool] = None
    total_chars: Optional[int] = None
    # Enrichment metadata (from content_metadata.jsonl)
    metadata_id: Optional[int] = None
    content_type: Optional[str] = None
    document_word_count: Optional[int] = None
    source_type: Optional[str] = None
    classification_confidence: Optional[float] = None
    classification_reasoning: Optional[str] = None
    content_hash: Optional[str] = None


@dataclass
class ChunkMetadata:
    """Chunk-level metadata for individual segments."""
    chunk_id: str
    chunk_index: int
    chunk_type: SegmentType
    start_position: int
    end_position: int
    word_count: int
    char_count: int
    line_count: int
    page_number: Optional[int] = None
    section_hierarchy: Optional[List[str]] = None
    prev_chunk_id: Optional[str] = None
    next_chunk_id: Optional[str] = None
    parent_section: Optional[str] = None
    is_first_in_section: bool = False
    is_last_in_section: bool = False
    contains_urls: bool = False
    contains_references: bool = False
    table_rows: Optional[int] = None
    table_cols: Optional[int] = None
    image_alt_text: Optional[str] = None
    list_item_count: Optional[int] = None
    # Citation-specific metadata
    citation_number: Optional[int] = None
    citation_type: Optional[str] = None  # "legal_case", "academic_ref", "official_doc", etc.
    references_citations: Optional[List[str]] = None  # List of citation chunk IDs this chunk references
    cited_by_chunks: Optional[List[str]] = None  # List of chunk IDs that cite this citation


@dataclass
class DocumentChunk:
    """Complete chunk record with content and metadata."""
    content: str
    document_metadata: DocumentMetadata
    chunk_metadata: ChunkMetadata
    processing_timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        # Convert chunk metadata and handle SegmentType enum
        chunk_meta_dict = asdict(self.chunk_metadata)
        if 'chunk_type' in chunk_meta_dict and hasattr(chunk_meta_dict['chunk_type'], 'value'):
            chunk_meta_dict['chunk_type'] = chunk_meta_dict['chunk_type'].value

        # Handle citation_type which may be a tuple containing SegmentType
        if 'citation_type' in chunk_meta_dict and chunk_meta_dict['citation_type']:
            citation_type = chunk_meta_dict['citation_type']
            if isinstance(citation_type, tuple) and len(citation_type) >= 2:
                # Extract just the string classification from the tuple
                chunk_meta_dict['citation_type'] = citation_type[1]  # Second element is the string type
            elif hasattr(citation_type, 'value'):
                chunk_meta_dict['citation_type'] = citation_type.value

        return {
            'content': self.content,
            'document_metadata': asdict(self.document_metadata),
            'chunk_metadata': chunk_meta_dict,
            'processing_timestamp': self.processing_timestamp
        }


class MarkdownParser:
    """Parser for processing docling-extracted markdown."""

    def __init__(self):
        # Regex patterns for different markdown elements
        self.patterns = {
            'header': re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE),
            'table': re.compile(r'^\|(.+)\|$', re.MULTILINE),
            'image': re.compile(r'!\[([^\]]*)\]\(([^)]+)\)'),
            'code_block': re.compile(r'```(\w+)?\n(.*?)\n```', re.DOTALL),
            'blockquote': re.compile(r'^>\s+(.+)$', re.MULTILINE),
            'list_ordered': re.compile(r'^\d+\.\s+(.+)$', re.MULTILINE),
            'list_unordered': re.compile(r'^[-*+]\s+(.+)$', re.MULTILINE),
            'horizontal_rule': re.compile(r'^---+$', re.MULTILINE),
            'page_break': re.compile(r'<!-- page-break -->|<div class="page-break">'),
            'footnote': re.compile(r'\[\^([^\]]+)\]:\s*(.+)'),
            'reference': re.compile(r'^\[\d+\]\s+(.+)$', re.MULTILINE),
            'url': re.compile(r'https?://[^\s\]]+')
        }

    def parse_document(self, markdown_text: str) -> List[Dict[str, Any]]:
        """Parse markdown document into segments with position tracking."""
        # Pre-process to fix ligature splitting from OCR errors
        markdown_text = self._fix_ligature_splitting(markdown_text)

        # Pre-process to split mixed citation+paragraph content
        markdown_text = self._split_mixed_citations(markdown_text)

        segments = []
        lines = markdown_text.split('\n')
        current_pos = 0

        # Track document structure
        section_hierarchy = []
        current_section = None

        i = 0
        while i < len(lines):
            line = lines[i]
            line_start_pos = current_pos
            line_end_pos = current_pos + len(line) + 1  # +1 for newline

            # Skip empty lines but track position
            if not line.strip():
                current_pos = line_end_pos
                i += 1
                continue

            # Detect segment type and process
            segment_type, content, consumed_lines = self._detect_segment_type(lines[i:])

            if segment_type != SegmentType.UNKNOWN:
                # Calculate positions for multi-line segments
                segment_start = line_start_pos
                segment_end = current_pos
                for j in range(consumed_lines):
                    if i + j < len(lines):
                        segment_end += len(lines[i + j]) + 1

                # Update section hierarchy for headers
                if segment_type.value.startswith('header_'):
                    level = int(segment_type.value.split('_')[1])
                    header_text = content.strip('#').strip()
                    self._update_section_hierarchy(section_hierarchy, level, header_text)
                    current_section = header_text

                segment = {
                    'type': segment_type,
                    'content': content,
                    'start_position': segment_start,
                    'end_position': segment_end,
                    'line_start': i,
                    'line_end': i + consumed_lines,
                    'section_hierarchy': section_hierarchy.copy(),
                    'parent_section': current_section
                }

                segments.append(segment)

                # Advance position and line counter
                i += consumed_lines
                current_pos = segment_end
            else:
                # Treat as paragraph if not recognized
                paragraph_lines = [line]
                j = i + 1

                # Collect consecutive non-empty lines for paragraph
                while j < len(lines) and lines[j].strip() and not self._is_special_line(lines[j]):
                    paragraph_lines.append(lines[j])
                    j += 1

                if paragraph_lines:
                    paragraph_content = '\n'.join(paragraph_lines)
                    paragraph_end = current_pos
                    for para_line in paragraph_lines:
                        paragraph_end += len(para_line) + 1

                    segment = {
                        'type': SegmentType.PARAGRAPH,
                        'content': paragraph_content,
                        'start_position': line_start_pos,
                        'end_position': paragraph_end,
                        'line_start': i,
                        'line_end': j,
                        'section_hierarchy': section_hierarchy.copy(),
                        'parent_section': current_section
                    }
                    segments.append(segment)

                    i = j
                    current_pos = paragraph_end
                else:
                    i += 1
                    current_pos = line_end_pos

        return segments

    def _detect_segment_type(self, lines: List[str]) -> Tuple[SegmentType, str, int]:
        """Detect the type of segment starting at the given lines."""
        if not lines:
            return SegmentType.UNKNOWN, "", 0

        first_line = lines[0]

        # Headers
        if first_line.startswith('#'):
            level = len(first_line) - len(first_line.lstrip('#'))
            if 1 <= level <= 6:
                segment_type = getattr(SegmentType, f'HEADER_{level}')
                return segment_type, first_line, 1

        # Tables - check if first line OR second line contains table pipes
        # This handles tables with captions/titles before them
        table_start_line = 0
        if '|' in first_line and first_line.strip().startswith('|'):
            table_start_line = 0
        elif len(lines) > 1 and '|' in lines[1] and lines[1].strip().startswith('|'):
            # Check if second line is a table - include first line as caption
            table_start_line = 0
        elif len(lines) > 2 and '|' in lines[2] and lines[2].strip().startswith('|'):
            # Check if third line is a table - include first two lines as caption
            table_start_line = 0

        if table_start_line == 0 and ('|' in first_line or (len(lines) > 1 and '|' in lines[1])):
            table_lines = []
            i = 0
            # Include any caption/title lines before the table
            while i < len(lines):
                if '|' in lines[i]:
                    # Found table content - check if it's actually a table row
                    line = lines[i].strip()

                    # Handle case where table and text are on the same line (all one giant line)
                    # Look for document section markers that indicate end of table
                    if line.count('|') > 20:  # Suspiciously long line - might be mixed content
                        # Try to split at document section markers (but NOT "Sources" which is table metadata)
                        for marker in [' TD/B/', ' A/HRC/', ' A/RES/', ' E/CN.', '## ', '\n\n']:
                            if marker in line:
                                # Split here - everything before is table, after is paragraph
                                table_part = line[:line.index(marker)]
                                remaining_part = line[line.index(marker):]

                                if table_part.count('|') >= 2:
                                    # Add the table part
                                    table_lines.append(table_part.rstrip())

                                    # Create a new "line" for the remaining text to be processed later
                                    # We'll return the table and let the remaining part be processed as next segment
                                    # Store remaining in lines for next iteration (won't work in current design)
                                    # For now, just return the clean table
                                    return SegmentType.TABLE, '\n'.join(table_lines), len(table_lines)

                    # A table row should have at least 2 pipes, OR start/end with pipes
                    if line.count('|') >= 2 or line.startswith('|') or line.endswith('|'):
                        table_lines.append(lines[i])
                        i += 1
                        # Continue collecting table rows
                        while i < len(lines):
                            if not lines[i].strip():
                                # Blank line - could be end of table or spacing within table
                                # Peek ahead to see if next non-blank line is a table row
                                if i + 1 < len(lines):
                                    next_line = lines[i + 1].strip()
                                    if next_line and '|' in next_line and (next_line.count('|') >= 2 or next_line.startswith('|') or next_line.endswith('|')):
                                        table_lines.append(lines[i])  # Include blank line
                                        i += 1
                                        continue
                                break  # End of table
                            elif '|' in lines[i]:
                                line = lines[i].strip()
                                # Check if it's a proper table row (not just text with a single pipe)
                                if line.count('|') >= 2 or line.startswith('|') or line.endswith('|'):
                                    table_lines.append(lines[i])
                                    i += 1
                                else:
                                    # Line has a pipe but doesn't look like a table row - end table
                                    break
                            else:
                                # No pipe - end of table
                                break
                        return SegmentType.TABLE, '\n'.join(table_lines), len(table_lines)
                    else:
                        # Has a pipe but not a table row - skip this detection
                        break
                elif i < 3 and not lines[i].strip().startswith('#'):
                    # Include up to 3 lines of caption (but not headers)
                    table_lines.append(lines[i])
                    i += 1
                else:
                    break

        # Code blocks
        if first_line.strip().startswith('```'):
            code_lines = [first_line]
            i = 1
            while i < len(lines):
                code_lines.append(lines[i])
                if lines[i].strip() == '```':
                    break
                i += 1
            return SegmentType.CODE_BLOCK, '\n'.join(code_lines), len(code_lines)

        # Lists - ordered lists (collect continuation lines)
        if re.match(r'^\d+\.\s+', first_line):
            list_lines = [first_line]
            i = 1
            # Collect continuation lines until we hit:
            # - Empty line
            # - Next list item (starts with number.)
            # - Special markdown element
            while i < len(lines):
                next_line = lines[i]
                # Empty line ends the list item
                if not next_line.strip():
                    break
                # Next numbered item ends current item
                if re.match(r'^\d+\.\s+', next_line):
                    break
                # Special elements end the list item
                if self._is_special_line(next_line):
                    break
                # This is a continuation line
                list_lines.append(next_line)
                i += 1
            return SegmentType.LIST_ORDERED, '\n'.join(list_lines), len(list_lines)

        # Lists - unordered lists (collect continuation lines)
        if re.match(r'^[-*+]\s+', first_line):
            list_lines = [first_line]
            i = 1
            # Collect continuation lines until we hit:
            # - Empty line
            # - Next list item
            # - Special markdown element
            while i < len(lines):
                next_line = lines[i]
                # Empty line ends the list item
                if not next_line.strip():
                    break
                # Next bullet point ends current item
                if re.match(r'^[-*+]\s+', next_line):
                    break
                # Special elements end the list item
                if self._is_special_line(next_line):
                    break
                # This is a continuation line
                list_lines.append(next_line)
                i += 1
            return SegmentType.LIST_UNORDERED, '\n'.join(list_lines), len(list_lines)

        # Blockquotes
        if first_line.strip().startswith('>'):
            return SegmentType.BLOCKQUOTE, first_line, 1

        # Horizontal rules
        if re.match(r'^---+$', first_line.strip()):
            return SegmentType.HORIZONTAL_RULE, first_line, 1

        # Images
        if re.match(r'!\[.*\]\(.*\)', first_line):
            return SegmentType.IMAGE, first_line, 1

        # Footnotes
        if re.match(r'\[\^.*\]:', first_line):
            return SegmentType.FOOTNOTE, first_line, 1

        # References
        if re.match(r'^\[\d+\]\s+', first_line):
            return SegmentType.REFERENCE, first_line, 1

        # Citations and bibliographic entries - multiline detection
        citation_result = self._detect_citation_type_multiline(lines)
        if citation_result[0] != SegmentType.UNKNOWN:
            return citation_result[0], citation_result[2], citation_result[3]

        return SegmentType.UNKNOWN, "", 0

    def _is_special_line(self, line: str) -> bool:
        """Check if line is a special markdown element that shouldn't be part of paragraph."""
        line = line.strip()

        # Headers
        if line.startswith('#'):
            return True

        # Lists
        if re.match(r'^\d+\.\s+', line) or re.match(r'^[-*+]\s+', line):
            return True

        # Tables
        if line.startswith('|') and line.endswith('|'):
            return True

        # Code blocks
        if line.startswith('```'):
            return True

        # Blockquotes
        if line.startswith('>'):
            return True

        # Horizontal rules
        if re.match(r'^---+$', line):
            return True

        return False

    def _update_section_hierarchy(self, hierarchy: List[str], level: int, header_text: str):
        """Update section hierarchy based on header level."""
        # Trim hierarchy to current level
        while len(hierarchy) >= level:
            hierarchy.pop()

        # Add current header
        hierarchy.append(header_text)

    def _fix_ligature_splitting(self, text: str) -> str:
        """Fix OCR-induced ligature splitting where 'fi', 'fl', 'ffi' characters were split.

        Docling sometimes splits ligatures across lines:
        - "of fi" + "cials" → "officials"
        - "to fi" + "x" → "fix"
        - "fi" + "nancial" → "financial"
        - "suf fi" + "fi lled" → "sufficient" + "filled" (double split)

        Strategy: Remove standalone " fi" and " fl" fragments at line ends,
        then merge continuation lines. Apply multiple passes to handle nested splits.
        """
        # Apply fixes iteratively to handle nested/chained ligature splits
        max_iterations = 3
        for _ in range(max_iterations):
            before = text

            # Pattern 1: " fi\n\n" followed by lowercase letter (e.g., "of fi\n\ncials")
            text = re.sub(r' fi\n\n([a-z])', r'fi\1', text)

            # Pattern 2: " fl\n\n" followed by lowercase letter
            text = re.sub(r' fl\n\n([a-z])', r'fl\1', text)

            # Pattern 3: " fi\n\n" followed by 'x' (for "fix")
            text = re.sub(r' fi\n\nx', r'fix', text)

            # Pattern 4: Handle standalone "fi " or "fl " at line starts (like "\n\nfi lled")
            text = re.sub(r'\n\nfi ([a-z]{2,})', r'fi\1', text)
            text = re.sub(r'\n\nfl ([a-z]{2,})', r'fl\1', text)

            # Pattern 5: Remove spaces after ligature fragments (fixes "suffifi lled" → "suffifilled")
            # Only remove space if followed by lowercase letter (to avoid breaking real spaces)
            text = re.sub(r'fi\s+([a-z])', r'fi\1', text)
            text = re.sub(r'fl\s+([a-z])', r'fl\1', text)

            # If no changes made, we're done
            if text == before:
                break

        return text

    def _detect_citation_type_multiline(self, lines: List[str]) -> Tuple[SegmentType, str, str, int]:
        """
        Detect citation type with CONSERVATIVE multiline collection.

        Strategy:
        - Collect continuation lines for academic/legal citations that span multiple lines
        - STRICT stopping conditions to prevent over-collection
        - Max 100 words per citation (academic citations rarely exceed this)

        Stopping conditions (any of these stops collection):
        1. Empty line (paragraph break)
        2. New numbered citation starts (e.g., "23 Author Name...")
        3. Header pattern detected
        4. Bullet/list item detected
        5. Citation ends with period AND next line starts with capital letter (new sentence/citation)
        6. Word count exceeds 100 words

        Returns:
            (SegmentType, pattern_matched, full_content, consumed_lines)
        """
        if not lines or not lines[0].strip():
            return SegmentType.UNKNOWN, "", "", 0

        first_line = lines[0].strip()
        MAX_CITATION_WORDS = 100

        # Check if first line looks like a citation
        citation_result = self._detect_citation_type(first_line)
        if citation_result[0] == SegmentType.UNKNOWN:
            return SegmentType.UNKNOWN, "", "", 0

        # Start collecting - first line is a citation
        collected = [first_line]
        word_count = len(first_line.split())
        i = 1

        while i < len(lines) and word_count < MAX_CITATION_WORDS:
            line = lines[i].strip()

            # STOP 1: Empty line - but allow ONE blank line within citation (PDF line wrapping)
            # Stop at second consecutive blank line (real paragraph break)
            if not line:
                # Check if next line is also empty (double blank = paragraph break)
                if i + 1 < len(lines) and not lines[i + 1].strip():
                    break
                # Check if next line starts a new numbered item (citation break)
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if re.match(r'^\d+\s{2,}[A-Z]', next_line):
                        break
                # Single blank line - skip it but continue collecting
                i += 1
                continue

            # STOP 2: New numbered citation starts
            # Pattern: "23 Author Name" or "23   See also" (number + 2+ spaces + capital letter)
            if re.match(r'^\d+\s{2,}[A-Z]', line):
                break

            # STOP 3: Header patterns
            if re.match(r'^#{1,6}\s+', line) or re.match(r'^[IVX]+\.\s+[A-Z]', line):
                break

            # STOP 4: Bullet/list patterns
            if re.match(r'^[-*+]\s+', line) or re.match(r'^\d+\.\s+', line):
                break

            # STOP 5: Previous line ended cleanly AND current line starts new sentence
            # This prevents collecting multiple citations run together
            if collected:
                prev_line = collected[-1]
                # Previous ends with period (citation complete)
                if re.search(r'\.\s*$|\."\s*$|\.\)\s*$', prev_line):
                    # Current starts with capital letter or number (new citation/sentence)
                    if re.match(r'^[A-Z0-9]', line):
                        # BUT: Allow continuation if current line has citation markers
                        # (e.g., "vol. 2" or "pp. 43-50" or year in parens)
                        has_continuation_markers = bool(re.search(
                            r'^vol\.|^pp\.|^No\.|^para\.|^\(\d{4}\)',
                            line,
                            re.IGNORECASE
                        ))
                        if not has_continuation_markers:
                            break

            # Add this line to collection
            line_words = len(line.split())
            word_count += line_words

            # Check if adding this line would exceed limit
            if word_count > MAX_CITATION_WORDS:
                break

            collected.append(line)
            i += 1

        # Join collected lines
        full_content = '\n'.join(collected)

        return citation_result[0], citation_result[2], full_content, len(collected)

    def _split_mixed_citations(self, text: str) -> str:
        """
        Split paragraphs that have citations at the beginning followed by regular text.

        Example input:
        "256 Goldstone Report, supra n. 1, at 1969. 257 See G.A. Res 64/10... Israel's initial establishment..."

        Output:
        "256 Goldstone Report, supra n. 1, at 1969.
        257 See G.A. Res 64/10...
        Israel's initial establishment..."

        This ensures citations are properly separated into their own segments.
        """
        lines = text.split('\n')
        processed_lines = []

        for line in lines:
            line_stripped = line.strip()

            # Skip empty lines or lines that don't start with a number
            if not line_stripped or not re.match(r'^\d+([\.\s\t])', line_stripped):
                processed_lines.append(line)
                continue

            # Check if this looks like a citation at the start
            # Pattern: "256 Something... 257 Something else..." or "256. Text that goes on..."
            # We want to split when we see a sentence end followed by more numbered content or regular text

            # Find all potential citation boundaries
            # A citation ends at: period + space + (number OR capital letter for new sentence)
            parts = []
            current_part = []
            words = line_stripped.split()

            for i, word in enumerate(words):
                current_part.append(word)

                # Check if this word ends with a period
                if word.endswith('.'):
                    # Look ahead to see if next word starts a new citation or regular text
                    if i + 1 < len(words):
                        next_word = words[i + 1]

                        # If next word is a number (possible new citation) or starts with capital (new sentence)
                        # AND we've accumulated at least 3 words (minimum citation length)
                        if len(current_part) >= 3:
                            # Check if current part looks like a citation (has citation indicators)
                            current_text = ' '.join(current_part)
                            if self._looks_like_citation(current_text):
                                # Check if we should split here
                                # Split if: next is a number, OR next is capital AND current is short enough to be citation
                                if (re.match(r'^\d+', next_word) or
                                    (next_word[0].isupper() and len(current_part) <= 30)):  # 30 words max for citation
                                    parts.append(' '.join(current_part))
                                    current_part = []

            # Add remaining part
            if current_part:
                parts.append(' '.join(current_part))

            # Add all parts as separate lines
            if len(parts) > 1:
                processed_lines.extend(parts)
            else:
                processed_lines.append(line)

        return '\n'.join(processed_lines)

    def _looks_like_citation(self, text: str) -> bool:
        """Quick check if text looks like a citation.

        Returns False for:
        - Financial/tabular data (multiple numbers, percentages, repeated patterns)
        - Lists with consecutive numbering patterns
        - Content with heavy numeric density (but allows citation doc codes)
        """
        text_lower = text.lower()

        # FIRST: Check for data patterns that indicate NOT a citation

        # Pattern 1: Multiple comma-separated numbers (financial data)
        # Example: "30,078 3,721,364 0.01%" or "252,983 11,929,047"
        comma_numbers = re.findall(r'\d{1,3}(?:,\d{3})+', text)
        if len(comma_numbers) >= 3:  # 3+ formatted numbers = likely financial data
            return False

        # Pattern 2: Multiple percentages (data/statistics)
        percentages = re.findall(r'\d+\.?\d*%', text)
        if len(percentages) >= 2:  # 2+ percentages = likely data
            return False

        # Pattern 3: Repeated company suffixes (financial holdings)
        # "AG" appears multiple times = German stock holdings
        ag_count = len(re.findall(r'\b[A-Z][a-z]+\s+AG\b', text))
        if ag_count >= 2:  # 2+ "Company AG" = stock list
            return False

        # Pattern 4: Multiple decimal numbers in sequence
        # Example: "0.01% ... 0.03% ... 0.05%"
        decimal_numbers = re.findall(r'\d+\.\d+', text)
        if len(decimal_numbers) >= 4:  # 4+ decimals = likely data
            return False

        # Pattern 5: High numeric density ONLY if also has comma-separated numbers
        # This avoids rejecting citations like "A/RES/64/10" which have slashes
        # But catches pure numeric data like "30,078 3,721,364"
        if len(comma_numbers) >= 1:  # Has formatted numbers
            total_chars = len(text.replace(' ', ''))
            if total_chars > 0:
                digit_chars = sum(1 for c in text if c.isdigit())
                numeric_density = digit_chars / total_chars
                if numeric_density > 0.35:  # >35% digits + formatted numbers = financial data
                    return False

        # SECOND: Check for citation indicators
        citation_indicators = [
            'supra', 'infra', 'ibid', 'see', 'v.', 'vs.',
            'report', 'resolution', 'g.a.', 'u.n.', 'art.',
            'para.', 'p.', 'pp.', 'n.', 'note', 'fn.',
            'http://', 'https://', 'www.',
            'a/hrc/', 'a/res/', 's/res/',  # UN doc codes
        ]
        return any(indicator in text_lower for indicator in citation_indicators)

    def _detect_citation_type(self, line: str) -> Tuple[SegmentType, str, str]:
        """Detect if line is a citation and classify its type.

        STRICT CITATION RULES:
        1. MUST start with a number (e.g., "24." or "24 " or "24\t")
        2. MUST be SHORT (typically under 50 words, max 100 with indicators)
        3. MUST contain citation indicators (URLs, doc codes like A/HRC, titles, "see", "supra", etc.)
        4. WITHOUT citation indicators = numbered paragraph, NOT a citation
        5. Long descriptive content = numbered paragraph, NOT a citation
        """
        content = line.strip()

        # Return early if too short
        if len(content) < 5:
            return SegmentType.UNKNOWN, "", ""

        # CRITICAL: Must start with a number followed by period, space, or tab
        start_match = re.match(r'^(\d+)([\.\s\t])', content)
        if not start_match:
            return SegmentType.UNKNOWN, "", ""

        citation_number = start_match.group(1)

        # CRITICAL: Check word count - citations are SHORT
        word_count = len(content.split())

        # Normalize for case-insensitive matching
        content_lower = content.lower()

        # CRITICAL: Check for citation indicators
        # Citations have specific markers that distinguish them from numbered paragraphs
        citation_indicators = [
            # URLs
            'http://', 'https://', 'www.',
            # UN document codes
            'a/hrc/', 'a/res/', 's/res/', 'e/cn.', 'a/ac.',
            # Legal references
            'v.', 'vs.', 'prosecutor', 'case no.',
            # Citation keywords
            'see ', 'supra', 'infra', 'ibid', 'id.', 'cf.',
            # Document references
            'report', 'resolution', 'available at', 'accessed',
            # Organizational acronyms
            'u.n.', 'icrc', 'icc', 'nato',
            # Article/paragraph references
            'art.', 'para.', 'paras.', 'note ',
        ]

        has_citation_indicator = any(indicator in content_lower for indicator in citation_indicators)

        # Apply strict word limits based on whether citation indicators are present
        if not has_citation_indicator:
            # NO citation indicators = numbered paragraph, regardless of length
            return SegmentType.UNKNOWN, "", ""

        if word_count > 100:
            # Too long to be a citation, even with indicators
            return SegmentType.UNKNOWN, "", ""

        # Prefer shorter citations - if over 50 words, must have strong indicators
        if word_count > 50:
            strong_indicators = ['http://', 'https://', 'a/hrc/', 'a/res/', 's/res/',
                                'v.', 'vs.', 'see ', 'supra', 'available at']
            if not any(indicator in content_lower for indicator in strong_indicators):
                # Long text without strong indicators = numbered paragraph
                return SegmentType.UNKNOWN, "", ""

        # CRITICAL: Ensure ONLY ONE citation number
        # Reject content with consecutive citation numbers like "4 13." or "24. 25."
        consecutive_numbers_pattern = r'\b(\d+)([\.\s\t])\s*(\d+)([\.\s\t])'
        if re.search(consecutive_numbers_pattern, content):
            # Found consecutive citation numbers - this is multiple citations or a list
            return SegmentType.UNKNOWN, "", ""

        # Additional check: Count total number groups in content
        # Pattern: digit(s) followed by period/space/tab
        number_groups = re.findall(r'\b(\d+)([\.\s\t])', content)
        if len(number_groups) > 1:
            # Multiple citation numbers found - likely multiple citations merged
            # or a numbered list, not a single citation
            return SegmentType.UNKNOWN, "", ""

        # NOTE: Testimony patterns are intentionally removed from citation detection
        # Testimonies are substantive content that should be treated as paragraphs
        # for embedding and search purposes, not as bibliographic metadata

        # Legal case patterns (numbered citations)
        legal_patterns = [
            r'^\d+[\.\s\t]+.*v\..*',  # "11 Prosecutor v. Smith" or "11. Prosecutor v. Smith" or "11\tProc..."
            r'^\d+[\.\s\t]+.*vs\..*',  # "11 State vs. Defendant"
            r'^\d+[\.\s\t]+.*prosecutor.*v\..*',  # Case variations
            r'^\d+[\.\s\t]+.*in\s+re:.*',  # "In re:" cases
            r'^\d+[\.\s\t]+.*ex\s+parte.*',  # "Ex parte" cases
            r'^\d+[\.\s\t]+.*it-\d+-\d+-t.*',  # International tribunal cases
            r'^\d+[\.\s\t]+.*\d+\s+f\.\d+d\s+\d+.*',  # Federal reporter citations
            r'^\d+[\.\s\t]+.*no\.\s+\d+-\d+.*',  # Case numbers
        ]

        # Academic/official document patterns (numbered citations)
        academic_patterns = [
            r'^\d+[\.\s\t]+.*see\s+.*',  # "11 See Report of..." or "11.\tSee Report..."
            r'^\d+[\.\s\t]+.*cf\..*',  # "11 Cf. similar case"
            r'^\d+[\.\s\t]+.*supra.*note.*',  # "11 supra note 5"
            r'^\d+[\.\s\t]+.*infra.*',  # "11 infra section 3"
            r'^\d+[\.\s\t]+.*available\s+at.*',  # "11 Document available at..."
            r'^\d+[\.\s\t]+.*retrieved\s+from.*',  # "11 Retrieved from..."
            r'^\d+[\.\s\t]+.*accessed.*',  # "11 Accessed on..."
            r'^\d+[\.\s\t]+.*u\.?n\..*',  # UN documents
            r'^\d+[\.\s\t]+.*united\s+nations.*',  # UN full name
            r'^\d+[\.\s\t]+.*icrc.*rule.*',  # ICRC rules
            r'^\d+[\.\s\t]+.*art\..*',  # Article references
            r'^\d+[\.\s\t]+.*para\..*',  # Paragraph references
            r'^\d+[\.\s\t]+.*e/cn\.\d+.*',  # UN Economic and Social Council docs
            r'^\d+[\.\s\t]+.*a/res/.*',  # UN General Assembly resolutions
            r'^\d+[\.\s\t]+.*s/res/.*',  # UN Security Council resolutions
            r'^\d+[\.\s\t]+.*http[s]?://.*',  # Contains URL
        ]

        # Organizational acronym patterns (numbered citations)
        org_patterns = [
            r'^\d+[\.\s\t]+.*\bu\.?n\.?\b.*',  # U.N.
            r'^\d+[\.\s\t]+.*\bicrc\b.*',  # ICRC
            r'^\d+[\.\s\t]+.*\bnato\b.*',  # NATO
            r'^\d+[\.\s\t]+.*\beu\b.*',  # EU (careful with this one)
            r'^\d+[\.\s\t]+.*\bwho\b.*',  # WHO
            r'^\d+[\.\s\t]+.*\bicc\b.*',  # ICC
            r'^\d+[\.\s\t]+.*\bechr\b.*',  # ECHR
        ]

        # Check legal case patterns
        for pattern in legal_patterns:
            if re.match(pattern, content_lower):
                return SegmentType.CITATION, "legal_case", content

        # Check academic/official document patterns
        for pattern in academic_patterns:
            if re.match(pattern, content_lower):
                return SegmentType.CITATION, "academic_ref", content

        # Check organizational patterns
        for pattern in org_patterns:
            if re.match(pattern, content_lower):
                return SegmentType.CITATION, "official_doc", content

        # Simple numbered reference pattern (fallback) - but only if contains citation keywords
        if re.match(r'^\d+[\.\s\t]+[A-Z].*[.!?]$', content):
            # Starts with number, has capital letter, ends with punctuation
            # Additional checks to ensure it's actually a citation
            if any(keyword in content_lower for keyword in ['ibid', 'id.', 'supra', 'infra', 'see', 'cf.', 'note']):
                return SegmentType.CITATION, "bibliography_entry", content

        # If it starts with a number but doesn't match any pattern, it's likely a numbered paragraph
        return SegmentType.UNKNOWN, "", ""


class DocumentChunkingUtility(Base):
    """Main utility class for document chunking operations."""

    def __init__(self, output_dir: str = None, cache_dir: str = None, max_chunk_words: int = 350):
        """Initialize the document chunking utility.

        Args:
            output_dir: Directory for output files
            cache_dir: Directory for cached extractions
            max_chunk_words: Maximum words per chunk before splitting at sentence boundary (default: 350)
        """
        super().__init__()

        # Use centralized config for paths
        if output_dir is None:
            output_dir = self._app_config.get_legacy_pdf_path("output_dir")
        if cache_dir is None:
            cache_dir = self._app_config.get_legacy_pdf_path("cache_dir")

        self.output_dir = Path(output_dir)
        self.cache_dir = Path(cache_dir)
        self.max_chunk_words = max_chunk_words
        self.parser = MarkdownParser()

        # Initialize PDF extractor for cache access
        self.pdf_extractor = PDFTextExtractor(
            download_dir=str(self.cache_dir.parent / "downloads" if self.cache_dir.parent else "downloads"),
            cache_dir=str(self.cache_dir)
        )

        # Processing state
        self.results: List[Dict[str, Any]] = []
        self.failed_chunking: List[Dict[str, Any]] = []
        self.skipped_documents: List[Dict[str, Any]] = []

        # Statistics
        self.chunk_stats = {
            'total_documents': 0,
            'total_chunks': 0,
            'chunks_by_type': {},
            'processing_times': [],
            'avg_chunks_per_document': 0
        }

        self._logger.info(f"DocumentChunkingUtility initialized:")
        self._logger.info(f"  📁 Output dir: {self.output_dir}")
        self._logger.info(f"  💾 Cache dir: {self.cache_dir}")
        self._logger.info(f"  📏 Max chunk words: {self.max_chunk_words}")

    def split_at_sentence_boundary(self, text: str, max_words: int) -> List[str]:
        """Split text at sentence boundaries when it exceeds max_words.

        Args:
            text: Text to split
            max_words: Maximum words per chunk

        Returns:
            List of text chunks split at sentence boundaries
        """
        words = text.split()
        if len(words) <= max_words:
            return [text]

        # Sentence boundary patterns (periods, exclamation, question marks followed by space or end)
        sentence_pattern = re.compile(r'([.!?]+)(\s+|$)')

        chunks = []
        current_chunk = []
        current_word_count = 0

        # Split text into sentences
        sentences = []
        current_sentence = []

        for word in words:
            current_sentence.append(word)
            # Check if this word ends with sentence boundary
            if sentence_pattern.search(word):
                sentences.append(' '.join(current_sentence))
                current_sentence = []

        # Add any remaining words as final sentence
        if current_sentence:
            sentences.append(' '.join(current_sentence))

        # Group sentences into chunks respecting max_words
        for sentence in sentences:
            sentence_words = sentence.split()
            sentence_word_count = len(sentence_words)

            # If single sentence exceeds max, we have to include it anyway
            if sentence_word_count > max_words:
                if current_chunk:
                    chunks.append(' '.join(current_chunk))
                    current_chunk = []
                    current_word_count = 0
                chunks.append(sentence)
                continue

            # If adding this sentence would exceed max, start new chunk
            if current_word_count + sentence_word_count > max_words and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [sentence]
                current_word_count = sentence_word_count
            else:
                current_chunk.append(sentence)
                current_word_count += sentence_word_count

        # Add final chunk
        if current_chunk:
            chunks.append(' '.join(current_chunk))

        return chunks if chunks else [text]

    def generate_chunk_id(self) -> str:
        """Generate unique chunk ID."""
        return f"chunk_{uuid.uuid4().hex[:12]}"

    def generate_document_id(self, source_url: str) -> str:
        """Generate consistent document ID from source URL."""
        # Use hash of URL for consistent IDs
        import hashlib
        url_hash = hashlib.md5(source_url.encode()).hexdigest()[:12]
        return f"doc_{url_hash}"

    def extract_document_metadata(self, cached_data: Dict[str, Any], source_url: str) -> DocumentMetadata:
        """Extract document-level metadata from cached extraction data."""
        # Initialize enrichment fields
        metadata_id = None
        content_type = None
        document_word_count = None
        source_type = None
        classification_confidence = None
        classification_reasoning = None
        content_hash = None

        # Check if we have original_metadata (from citation summaries with publication metadata)
        if 'original_metadata' in cached_data:
            orig_meta = cached_data['original_metadata']
            pub_meta = orig_meta.get('publication_metadata', {})

            # Use publication metadata fields
            doc_id = str(cached_data.get('id', ''))
            actual_url = cached_data.get('url', source_url)
            title = cached_data.get('title') or pub_meta.get('title')

            # Extract authors - join if list, use as-is if string
            authors = pub_meta.get('authors', [])
            if isinstance(authors, list):
                author = ', '.join(authors) if authors else None
            else:
                author = authors

            # Use publication metadata
            language = None  # Not in publication_metadata
            file_size = None
            total_pages = pub_meta.get('pages')
            creation_date = pub_meta.get('publication_date')
            modification_date = None
            pdf_version = None

            # Extract enrichment-specific fields
            metadata_id = cached_data.get('id')
            content_type = orig_meta.get('document_type')
            document_word_count = cached_data.get('full_length')
            source_type = cached_data.get('source_type')
            classification_confidence = None
            classification_reasoning = None
            content_hash = cached_data.get('content_hash')

        # Check if we have enriched document_metadata (from pre-chunk enrichment)
        elif 'document_metadata' in cached_data:
            doc_meta = cached_data['document_metadata']
            # Use enriched metadata fields
            doc_id = str(doc_meta.get('metadata_id', '')) if doc_meta.get('metadata_id') is not None else self.generate_document_id(source_url)
            actual_url = doc_meta.get('source_url', source_url)
            title = doc_meta.get('title')
            author = doc_meta.get('author')
            language = doc_meta.get('language')
            file_size = doc_meta.get('file_size_bytes')
            total_pages = doc_meta.get('total_pages')
            creation_date = doc_meta.get('creation_date')
            modification_date = doc_meta.get('modification_date')
            pdf_version = doc_meta.get('pdf_version')
            # Extract enrichment-specific fields
            metadata_id = doc_meta.get('metadata_id')
            content_type = doc_meta.get('content_type')
            document_word_count = doc_meta.get('document_word_count')
            source_type = doc_meta.get('source_type')
            classification_confidence = doc_meta.get('classification_confidence')
            classification_reasoning = doc_meta.get('classification_reasoning')
            content_hash = doc_meta.get('content_hash')
        # Check if we have enriched metadata at top level (id, url, title, author from content_metadata)
        elif 'id' in cached_data and 'url' in cached_data:
            # Use enriched metadata fields
            doc_id = str(cached_data.get('id', ''))
            actual_url = cached_data.get('url', source_url)
            title = cached_data.get('title')
            author = cached_data.get('author')
            # Get nested metadata if available
            nested_metadata = cached_data.get('metadata', {})
            language = nested_metadata.get('language')
            file_size = nested_metadata.get('file_size')
            total_pages = nested_metadata.get('pages')
            creation_date = nested_metadata.get('creation_date')
            modification_date = nested_metadata.get('modification_date')
            pdf_version = nested_metadata.get('pdf_version')
            # Extract enrichment-specific fields from top level
            metadata_id = cached_data.get('id')
            content_type = cached_data.get('content_type')
            document_word_count = cached_data.get('word_count')
            classification_confidence = cached_data.get('classification_confidence')
            classification_reasoning = cached_data.get('classification_reasoning')
            content_hash = cached_data.get('content_hash')
        else:
            # Fall back to nested metadata structure
            metadata = cached_data.get('metadata', {})
            doc_id = self.generate_document_id(source_url)
            actual_url = source_url
            title = metadata.get('title')
            author = metadata.get('author')
            language = metadata.get('language')
            file_size = metadata.get('file_size')
            total_pages = metadata.get('pages')
            creation_date = metadata.get('creation_date')
            modification_date = metadata.get('modification_date')
            pdf_version = metadata.get('pdf_version')

        return DocumentMetadata(
            document_id=doc_id,
            source_url=actual_url,
            extraction_method=cached_data.get('method', 'unknown'),
            file_size_bytes=file_size,
            total_pages=total_pages,
            extraction_timestamp=cached_data.get('timestamp'),
            language=language,
            title=title,
            author=author,
            creation_date=creation_date,
            modification_date=modification_date,
            pdf_version=pdf_version,
            total_chars=len(cached_data.get('text', '')),
            metadata_id=metadata_id,
            content_type=content_type,
            document_word_count=document_word_count,
            source_type=source_type,
            classification_confidence=classification_confidence,
            classification_reasoning=classification_reasoning,
            content_hash=content_hash
        )

    def create_chunk_metadata(self,
                            segment: Dict[str, Any],
                            chunk_index: int,
                            prev_chunk_id: Optional[str] = None,
                            next_chunk_id: Optional[str] = None) -> ChunkMetadata:
        """Create chunk-level metadata from segment information."""
        content = segment['content']

        # Count basic stats
        word_count = len(content.split())
        char_count = len(content)
        line_count = len(content.split('\n'))

        # Detect content features
        contains_urls = bool(re.search(r'https?://[^\s\]]+', content))
        contains_references = bool(re.search(r'\[\d+\]|\[\^[^\]]+\]', content))

        # Citation-specific metadata
        citation_number = None
        citation_type = None
        references_citations = []
        cited_by_chunks = []

        # Detect citation information if segment is citation-related
        if segment['type'] in [SegmentType.CITATION, SegmentType.ENDNOTE, SegmentType.BIBLIOGRAPHY_ENTRY]:
            citation_type = self.parser._detect_citation_type(content)

            # Extract citation number if present
            number_match = re.search(r'\[(\d+)\]|^(\d+)\.|\((\d+)\)', content)
            if number_match:
                citation_number = next((g for g in number_match.groups() if g), None)

            # Find references to other citations in the content
            ref_matches = re.findall(r'\[(\d+)\]|see.*?(\d+)|supra.*?note\s+(\d+)', content, re.IGNORECASE)
            for match in ref_matches:
                ref_num = next((g for g in match if g), None)
                if ref_num and ref_num != citation_number:
                    references_citations.append(ref_num)

        # Type-specific metadata
        table_rows = None
        table_cols = None
        if segment['type'] == SegmentType.TABLE:
            lines = content.strip().split('\n')
            table_rows = len(lines)
            if lines:
                table_cols = lines[0].count('|') - 1

        image_alt_text = None
        if segment['type'] == SegmentType.IMAGE:
            match = re.search(r'!\[([^\]]*)\]', content)
            if match:
                image_alt_text = match.group(1)

        list_item_count = None
        if segment['type'] in [SegmentType.LIST_ORDERED, SegmentType.LIST_UNORDERED]:
            list_item_count = len(content.split('\n'))

        return ChunkMetadata(
            chunk_id=self.generate_chunk_id(),
            chunk_index=chunk_index,
            chunk_type=segment['type'],
            start_position=segment['start_position'],
            end_position=segment['end_position'],
            word_count=word_count,
            char_count=char_count,
            line_count=line_count,
            section_hierarchy=segment.get('section_hierarchy', []),
            prev_chunk_id=prev_chunk_id,
            next_chunk_id=next_chunk_id,
            parent_section=segment.get('parent_section'),
            contains_urls=contains_urls,
            contains_references=contains_references,
            table_rows=table_rows,
            table_cols=table_cols,
            image_alt_text=image_alt_text,
            list_item_count=list_item_count,
            citation_number=citation_number,
            citation_type=citation_type,
            references_citations=references_citations,
            cited_by_chunks=cited_by_chunks
        )

    def process_document(self, cached_data: Dict[str, Any], source_url: str) -> List[DocumentChunk]:
        """Process a single document into chunks."""
        # Extract document metadata
        doc_metadata = self.extract_document_metadata(cached_data, source_url)

        # Get markdown text - support both 'text' (from PDF extraction) and 'content' (from citation summaries)
        markdown_text = cached_data.get('text') or cached_data.get('content', '')
        if not markdown_text:
            raise ValueError("No text content found in cached data")

        # Parse into segments
        segments = self.parser.parse_document(markdown_text)
        if not segments:
            raise ValueError("No segments detected in document")

        # Prepend document title to section hierarchy for all segments
        if doc_metadata.title:
            for segment in segments:
                current_hierarchy = segment.get('section_hierarchy', [])
                # Filter out None values from current hierarchy
                current_hierarchy = [h for h in current_hierarchy if h is not None]
                if current_hierarchy:
                    # Prepend title to existing hierarchy
                    segment['section_hierarchy'] = [doc_metadata.title] + current_hierarchy
                else:
                    # Set title as the only hierarchy level
                    segment['section_hierarchy'] = [doc_metadata.title]

        # Split oversized segments at sentence boundaries
        split_segments = []
        for segment in segments:
            content = segment['content']
            word_count = len(content.split())

            # Split any text-based segment that exceeds max_chunk_words
            # Skip only structural elements like tables, images, code blocks
            skip_types = [
                SegmentType.TABLE,
                SegmentType.IMAGE,
                SegmentType.CODE_BLOCK,
                SegmentType.HORIZONTAL_RULE,
                SegmentType.PAGE_BREAK
            ]

            if (segment['type'] not in skip_types and word_count > self.max_chunk_words):

                # Split at sentence boundaries
                text_chunks = self.split_at_sentence_boundary(content, self.max_chunk_words)

                self._logger.info(f"Split {segment['type'].value} ({word_count} words) into {len(text_chunks)} chunks")

                # Create separate segments for each chunk
                for idx, chunk_text in enumerate(text_chunks):
                    split_segment = segment.copy()
                    split_segment['content'] = chunk_text
                    # Adjust positions proportionally (approximate)
                    chunk_ratio = len(chunk_text) / len(content) if len(content) > 0 else 0
                    segment_length = segment['end_position'] - segment['start_position']

                    # Calculate start and end positions for this chunk
                    if idx == 0:
                        split_segment['start_position'] = segment['start_position']
                    else:
                        split_segment['start_position'] = split_segments[-1]['end_position']

                    split_segment['end_position'] = split_segment['start_position'] + int(segment_length * chunk_ratio)

                    split_segments.append(split_segment)
            else:
                # Keep segment as-is
                split_segments.append(segment)

        segments = split_segments

        # Create chunks with linking
        chunks = []
        chunk_ids = []

        # First pass: create all chunk IDs
        for i in range(len(segments)):
            chunk_ids.append(self.generate_chunk_id())

        # Second pass: create chunks with proper linking
        for i, segment in enumerate(segments):
            prev_id = chunk_ids[i - 1] if i > 0 else None
            next_id = chunk_ids[i + 1] if i < len(segments) - 1 else None

            chunk_metadata = self.create_chunk_metadata(segment, i, prev_id, next_id)
            chunk_metadata.chunk_id = chunk_ids[i]  # Use pre-generated ID

            chunk = DocumentChunk(
                content=segment['content'],
                document_metadata=doc_metadata,
                chunk_metadata=chunk_metadata,
                processing_timestamp=time.time()
            )
            chunks.append(chunk)

        # Post-process to merge citation fragments
        chunks = self.merge_citation_fragments(chunks)

        # Validate chunks
        validation_results = self.validate_document_chunks(chunks)

        # Log validation results
        if validation_results['invalid_chunks'] > 0:
            self._logger.info(f"⚠️  Validation: {validation_results['invalid_chunks']} invalid chunks out of {validation_results['total_chunks']}")
            for chunk_val in validation_results['chunk_validations']:
                if not chunk_val['is_valid']:
                    self._logger.info(f"    Chunk {chunk_val['chunk_index']} ({chunk_val['chunk_type']}): {', '.join(chunk_val['issues'])}")

        if validation_results['document_issues']:
            self._logger.info(f"⚠️  Document issues: {'; '.join(validation_results['document_issues'])}")

        # Update statistics
        self.chunk_stats['chunks_by_type'] = self.chunk_stats.get('chunks_by_type', {})
        for chunk in chunks:
            chunk_type = chunk.chunk_metadata.chunk_type.value
            self.chunk_stats['chunks_by_type'][chunk_type] = self.chunk_stats['chunks_by_type'].get(chunk_type, 0) + 1

        return chunks

    def validate_chunk(self, chunk: DocumentChunk) -> Tuple[bool, List[str]]:
        """Validate chunk quality and completeness."""
        issues = []

        # More nuanced minimum content length checking
        content = chunk.content.strip()
        char_count = chunk.chunk_metadata.char_count
        chunk_type = chunk.chunk_metadata.chunk_type

        # Different minimum lengths based on chunk type and content
        if char_count == 0:
            issues.append("Empty chunk")
        elif char_count == 1:
            # Allow single characters for specific cases
            if not (content.isdigit() or  # Numbers/roman numerals
                   content in ['I', 'V', 'X', 'L', 'C', 'D', 'M'] or  # Roman numerals
                   content in ['…', '•', '·', '-', '–', '—'] or  # Punctuation/bullets
                   len(content.encode('utf-8')) > 1):  # Unicode characters (Hebrew, Arabic, etc.)
                issues.append("Single character chunk without clear meaning")
        elif char_count < 3:
            # Very short chunks - be more permissive for headers and special content
            if chunk_type not in [SegmentType.HEADER_1, SegmentType.HEADER_2, SegmentType.HEADER_3,
                                SegmentType.HEADER_4, SegmentType.HEADER_5, SegmentType.HEADER_6]:
                if not (content.isdigit() or content in ['II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX']):
                    issues.append("Very short chunk (< 3 characters) without clear purpose")
        elif char_count < 5:
            # Short chunks - allow headers, references, and meaningful abbreviations
            if chunk_type not in [SegmentType.HEADER_1, SegmentType.HEADER_2, SegmentType.HEADER_3,
                                SegmentType.HEADER_4, SegmentType.HEADER_5, SegmentType.HEADER_6]:
                # Allow common patterns: references, abbreviations, locations
                if not (content.upper().startswith(('CRS', 'DOC', 'REF', 'FIG', 'TAB')) or
                       'Ibid' in content or
                       content.isupper() or  # Acronyms
                       content.isdigit() or  # Page numbers, etc.
                       any(c.isdigit() for c in content)):  # Contains numbers
                    issues.append("Short chunk (< 5 characters) without clear academic/reference purpose")

        # Check maximum content length (avoid extremely large chunks)
        if chunk.chunk_metadata.char_count > 50000:
            issues.append("Chunk too long (> 50,000 characters)")

        # Check for empty content
        if not chunk.content.strip():
            issues.append("Empty content")

        # Check metadata consistency
        if chunk.chunk_metadata.word_count != len(chunk.content.split()):
            issues.append("Word count inconsistency")

        if chunk.chunk_metadata.char_count != len(chunk.content):
            issues.append("Character count inconsistency")

        # Check for required metadata fields
        if not chunk.document_metadata.document_id:
            issues.append("Missing document ID")

        if not chunk.chunk_metadata.chunk_id:
            issues.append("Missing chunk ID")

        # Check chunk type specific validations
        if chunk.chunk_metadata.chunk_type == SegmentType.TABLE:
            if not chunk.content.count('|'):
                issues.append("Table chunk missing table formatting")

        elif chunk.chunk_metadata.chunk_type == SegmentType.IMAGE:
            if '![' not in chunk.content and '](' not in chunk.content:
                issues.append("Image chunk missing markdown image syntax")

        elif chunk.chunk_metadata.chunk_type.value.startswith('header_'):
            if not chunk.content.strip().startswith('#'):
                issues.append("Header chunk missing markdown header syntax")

        # Check for valid section hierarchy
        if chunk.chunk_metadata.section_hierarchy:
            for section in chunk.chunk_metadata.section_hierarchy:
                if not section or not section.strip():
                    issues.append("Invalid section in hierarchy")

        return len(issues) == 0, issues

    def merge_citation_fragments(self, chunks: List[DocumentChunk]) -> List[DocumentChunk]:
        """Merge citation fragments with their appropriate parent chunks."""
        if not chunks:
            return chunks

        merged_chunks = []
        i = 0

        while i < len(chunks):
            current_chunk = chunks[i]
            content = current_chunk.content.strip()

            # Identify citation fragments that should be merged
            is_citation_fragment = self._is_citation_fragment(content, current_chunk.chunk_metadata.char_count)

            if is_citation_fragment and merged_chunks:
                # Merge with previous chunk if it makes sense
                prev_chunk = merged_chunks[-1]
                if self._should_merge_with_previous(content, prev_chunk):
                    # Merge into previous chunk
                    merged_content = prev_chunk.content + " " + content

                    # Update metadata
                    prev_chunk.content = merged_content
                    prev_chunk.chunk_metadata.char_count = len(merged_content)
                    prev_chunk.chunk_metadata.word_count = len(merged_content.split())
                    prev_chunk.chunk_metadata.end_position = current_chunk.chunk_metadata.end_position

                    # Update linking to skip the merged chunk
                    if i + 1 < len(chunks):
                        prev_chunk.chunk_metadata.next_chunk_id = current_chunk.chunk_metadata.next_chunk_id
                        if current_chunk.chunk_metadata.next_chunk_id:
                            # Find next chunk and update its prev_chunk_id
                            next_chunk_idx = i + 1
                            if next_chunk_idx < len(chunks):
                                chunks[next_chunk_idx].chunk_metadata.prev_chunk_id = prev_chunk.chunk_metadata.chunk_id
                    else:
                        prev_chunk.chunk_metadata.next_chunk_id = None

                    i += 1  # Skip the merged chunk
                    continue

            # Keep the chunk as-is
            merged_chunks.append(current_chunk)
            i += 1

        return merged_chunks

    def _is_citation_fragment(self, content: str, char_count: int) -> bool:
        """Determine if content is likely a citation fragment."""
        if char_count > 50:  # Too long to be a fragment
            return False

        content = content.strip()

        # Common citation patterns
        citation_patterns = [
            # Ibid references
            lambda s: 'ibid' in s.lower() and len(s) < 20,
            # Numbered references
            lambda s: re.match(r'^\d+\s+Ibid\.?$', s, re.IGNORECASE),
            # Footnote numbers in brackets
            lambda s: re.match(r'^\[\d+\]$', s),
            # Document reference numbers
            lambda s: re.match(r'^CRS-\d+$', s),
            # Simple numeric references
            lambda s: re.match(r'^\d{1,3}$', s) and len(s) <= 3,
            # Page markers
            lambda s: re.match(r'^-\s*\d+\s*-$', s),
            # Single punctuation or symbols that are reference artifacts
            lambda s: s in ['…', '•', '·', '--', '___', '***'],
            # Very short academic references
            lambda s: len(s) <= 10 and any(term in s.lower() for term in ['id.', 'op.', 'cit.', 'cf.', 'loc.']),
        ]

        return any(pattern(content) for pattern in citation_patterns)

    def _should_merge_with_previous(self, fragment: str, prev_chunk: DocumentChunk) -> bool:
        """Determine if fragment should merge with previous chunk."""
        prev_content = prev_chunk.content.strip()

        # Don't merge if previous chunk is already very long
        if len(prev_content) > 2000:
            return False

        # Don't merge across different chunk types unless specific cases
        fragment_looks_like_reference = any([
            'ibid' in fragment.lower(),
            re.match(r'^\d+\s', fragment),
            fragment.startswith('[') and fragment.endswith(']'),
            'CRS-' in fragment
        ])

        # Merge reference fragments with paragraphs
        if (prev_chunk.chunk_metadata.chunk_type == SegmentType.PARAGRAPH and
            fragment_looks_like_reference):
            return True

        # Don't merge different structural elements
        if prev_chunk.chunk_metadata.chunk_type in [
            SegmentType.HEADER_1, SegmentType.HEADER_2, SegmentType.HEADER_3,
            SegmentType.HEADER_4, SegmentType.HEADER_5, SegmentType.HEADER_6,
            SegmentType.TABLE
        ]:
            return False

        # Default: merge if both are paragraphs and fragment is citation-like
        return (prev_chunk.chunk_metadata.chunk_type == SegmentType.PARAGRAPH and
                fragment_looks_like_reference)

    def validate_document_chunks(self, chunks: List[DocumentChunk]) -> Dict[str, Any]:
        """Validate all chunks in a document."""
        validation_results = {
            'total_chunks': len(chunks),
            'valid_chunks': 0,
            'invalid_chunks': 0,
            'chunk_validations': [],
            'document_issues': []
        }

        if not chunks:
            validation_results['document_issues'].append("No chunks generated")
            return validation_results

        # Check individual chunks
        for i, chunk in enumerate(chunks):
            is_valid, issues = self.validate_chunk(chunk)

            chunk_validation = {
                'chunk_index': i,
                'chunk_id': chunk.chunk_metadata.chunk_id,
                'chunk_type': chunk.chunk_metadata.chunk_type.value,
                'is_valid': is_valid,
                'issues': issues
            }
            validation_results['chunk_validations'].append(chunk_validation)

            if is_valid:
                validation_results['valid_chunks'] += 1
            else:
                validation_results['invalid_chunks'] += 1

        # Check document-level consistency
        document_ids = set(chunk.document_metadata.document_id for chunk in chunks)
        if len(document_ids) > 1:
            validation_results['document_issues'].append(f"Multiple document IDs: {document_ids}")

        # Check chunk ordering
        chunk_indices = [chunk.chunk_metadata.chunk_index for chunk in chunks]
        if chunk_indices != list(range(len(chunks))):
            validation_results['document_issues'].append("Chunk indices not sequential")

        # Check linking consistency
        for i, chunk in enumerate(chunks):
            expected_prev = chunks[i-1].chunk_metadata.chunk_id if i > 0 else None
            expected_next = chunks[i+1].chunk_metadata.chunk_id if i < len(chunks)-1 else None

            if chunk.chunk_metadata.prev_chunk_id != expected_prev:
                validation_results['document_issues'].append(f"Chunk {i} prev_chunk_id inconsistency")

            if chunk.chunk_metadata.next_chunk_id != expected_next:
                validation_results['document_issues'].append(f"Chunk {i} next_chunk_id inconsistency")

        return validation_results

    def load_from_cache(self, source_url: str) -> Optional[Dict[str, Any]]:
        """Load cached extraction data for a URL."""
        try:
            cached_data = self.pdf_extractor.get_cached_extraction(source_url)
            if cached_data and cached_data.get('success'):
                return cached_data
            else:
                self._logger.info(f"⚠️  No successful cached extraction found for: {source_url}")
                return None
        except Exception as e:
            self._logger.info(f"❌ Error loading from cache for {source_url}: {e}")
            return None

    def chunk_document_from_url(self, source_url: str) -> Dict[str, Any]:
        """Chunk a document by loading it from cache first."""
        # Try to load from cache
        cached_data = self.load_from_cache(source_url)
        if not cached_data:
            return {
                'source_url': source_url,
                'status': 'failed',
                'error': 'No cached extraction data found',
                'timestamp': time.time()
            }

        # Process the cached data
        return self.chunk_single_document(source_url, cached_data)

    def batch_chunk_from_urls(self, urls: List[str], output_filename: str, start_index: int = 0) -> Dict[str, Any]:
        """Process multiple documents by URL, loading from cache."""
        total_urls = len(urls)
        successful_chunking = []

        self._logger.info(f"🚀 Starting batch chunking of {total_urls} URLs from cache...")
        if start_index > 0:
            self._logger.info(f"📍 Resuming from index {start_index}")

        # Prepare output file
        output_file = self.output_dir / f"{output_filename}_chunks.jsonl"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        start_time = time.time()

        # Open output file for appending
        with open(output_file, 'a', encoding='utf-8') as f:
            for i, url in enumerate(urls[start_index:], start=start_index + 1):
                self._logger.info(f"\n📊 Progress: {i}/{total_urls}")

                # Check if should skip
                should_skip, skip_reason = self.should_skip_document(url, output_file)
                if should_skip:
                    self._logger.info(f"⏭️  Skipping: {skip_reason}")
                    self.skipped_documents.append({
                        'source_url': url,
                        'reason': skip_reason,
                        'index': i
                    })
                    continue

                # Process document from URL
                result = self.chunk_document_from_url(url)
                self.results.append(result)

                # Write chunks to file immediately
                if result['status'] == 'success':
                    successful_chunking.append(result)
                    for chunk in result['chunks']:
                        chunk_record = chunk.to_dict()
                        f.write(json.dumps(chunk_record, ensure_ascii=False) + '\n')
                    f.flush()  # Ensure data is written

                # Add small delay to be respectful
                if i < total_urls:
                    time.sleep(0.1)

        end_time = time.time()
        processing_time = end_time - start_time

        # Calculate statistics
        total_chunks = sum(r.get('chunk_count', 0) for r in successful_chunking)
        self.chunk_stats.update({
            'total_documents': len(successful_chunking),
            'total_chunks': total_chunks,
            'avg_chunks_per_document': total_chunks / len(successful_chunking) if successful_chunking else 0
        })

        # Generate summary
        summary = {
            'total_urls': total_urls,
            'successful_chunking': len(successful_chunking),
            'failed_chunking': len(self.failed_chunking),
            'skipped_documents': len(self.skipped_documents),
            'success_rate': len(successful_chunking) / total_urls * 100,
            'total_chunks_generated': total_chunks,
            'avg_chunks_per_doc': self.chunk_stats['avg_chunks_per_document'],
            'processing_time_seconds': processing_time,
            'processing_time_minutes': processing_time / 60,
            'chunk_statistics': self.chunk_stats,
            'results': self.results,
            'failures': self.failed_chunking,
            'skipped': self.skipped_documents
        }

        return summary

    def load_urls_from_summary(self, summary_file: str) -> List[str]:
        """Load PDF URLs from summary file (same format as batch extractor)."""
        with open(summary_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        pdf_urls = []

        # Handle both old format (pages) and new format (sources)
        if 'pages' in data:
            # Old format
            for page_data in data.get('pages', []):
                for pdf in page_data.get('pdfs', []):
                    pdf_urls.append(pdf['url'])
        elif 'sources' in data:
            # New format
            for source in data.get('sources', []):
                for pdf in source.get('verified_pdfs', []):
                    pdf_urls.append(pdf['url'])

        self._logger.info(f"📊 Found {len(pdf_urls)} verified PDFs from summary file")
        return pdf_urls

    def chunk_single_document(self, source_url: str, cached_data: Dict[str, Any]) -> Dict[str, Any]:
        """Chunk a single document with error handling."""
        self._logger.info(f"📄 Processing document: {source_url}")

        start_time = time.time()

        try:
            chunks = self.process_document(cached_data, source_url)

            processing_time = time.time() - start_time
            self.chunk_stats['processing_times'].append(processing_time)

            self._logger.info(f"✅ Success: Generated {len(chunks)} chunks in {processing_time:.2f}s")

            return {
                'source_url': source_url,
                'status': 'success',
                'chunk_count': len(chunks),
                'chunks': chunks,
                'processing_time': processing_time,
                'timestamp': time.time()
            }

        except Exception as e:
            self._logger.info(f"❌ Failed: {str(e)}")
            self.failed_chunking.append({
                'source_url': source_url,
                'error': str(e),
                'timestamp': time.time()
            })

            return {
                'source_url': source_url,
                'status': 'failed',
                'error': str(e),
                'timestamp': time.time()
            }

    def should_skip_document(self, source_url: str, output_file: Path) -> Tuple[bool, str]:
        """Check if document should be skipped (already processed)."""
        if not output_file.exists():
            return False, ""

        # Check if document already processed by looking for its document ID in output
        document_id = self.generate_document_id(source_url)

        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if document_id in line:
                        return True, "Document already processed"
        except Exception as e:
            self._logger.info(f"⚠️  Warning: Could not check existing output: {e}")

        return False, ""

    def batch_chunk_documents(self,
                            extraction_data: List[Dict[str, Any]],
                            output_filename: str,
                            start_index: int = 0) -> Dict[str, Any]:
        """Process multiple documents in batch."""
        total_docs = len(extraction_data)
        successful_chunking = []

        self._logger.info(f"🚀 Starting batch chunking of {total_docs} documents...")
        if start_index > 0:
            self._logger.info(f"📍 Resuming from index {start_index}")

        # Prepare output file
        output_file = self.output_dir / f"{output_filename}_chunks.jsonl"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        start_time = time.time()

        # Open output file for appending
        with open(output_file, 'a', encoding='utf-8') as f:
            for i, extraction in enumerate(extraction_data[start_index:], start=start_index + 1):
                # Support both 'url' (from manual extraction) and 'source_url' (from standard extraction)
                source_url = extraction.get('url') or extraction.get('source_url', '')

                self._logger.info(f"\n📊 Progress: {i}/{total_docs}")

                # Check if should skip
                should_skip, skip_reason = self.should_skip_document(source_url, output_file)
                if should_skip:
                    self._logger.info(f"⏭️  Skipping: {skip_reason}")
                    self.skipped_documents.append({
                        'source_url': source_url,
                        'reason': skip_reason,
                        'index': i
                    })
                    continue

                # Process document
                result = self.chunk_single_document(source_url, extraction)
                self.results.append(result)

                # Write chunks to file immediately
                if result['status'] == 'success':
                    successful_chunking.append(result)
                    for chunk in result['chunks']:
                        chunk_record = chunk.to_dict()
                        f.write(json.dumps(chunk_record, ensure_ascii=False) + '\n')
                    f.flush()  # Ensure data is written

                # Add small delay to be respectful
                if i < total_docs:
                    time.sleep(0.1)

        end_time = time.time()
        processing_time = end_time - start_time

        # Calculate statistics
        total_chunks = sum(r.get('chunk_count', 0) for r in successful_chunking)
        self.chunk_stats.update({
            'total_documents': len(successful_chunking),
            'total_chunks': total_chunks,
            'avg_chunks_per_document': total_chunks / len(successful_chunking) if successful_chunking else 0
        })

        # Generate summary
        summary = {
            'total_documents': total_docs,
            'successful_chunking': len(successful_chunking),
            'failed_chunking': len(self.failed_chunking),
            'skipped_documents': len(self.skipped_documents),
            'success_rate': len(successful_chunking) / total_docs * 100,
            'total_chunks_generated': total_chunks,
            'avg_chunks_per_doc': self.chunk_stats['avg_chunks_per_document'],
            'processing_time_seconds': processing_time,
            'processing_time_minutes': processing_time / 60,
            'chunk_statistics': self.chunk_stats,
            'results': self.results,
            'failures': self.failed_chunking,
            'skipped': self.skipped_documents
        }

        return summary

    def load_extraction_data(self, input_file: str) -> List[Dict[str, Any]]:
        """Load extraction data from JSONL file."""
        extraction_data = []

        with open(input_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    extraction_data.append(data)
                except json.JSONDecodeError as e:
                    self._logger.info(f"⚠️  Warning: Invalid JSON on line {line_num}: {e}")
                    continue

        self._logger.info(f"📊 Loaded {len(extraction_data)} extraction records")
        return extraction_data

    def save_results(self, summary: Dict[str, Any], base_filename: str):
        """Save chunking results and statistics."""
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Save complete summary
        summary_file = self.output_dir / f"{base_filename}_chunking_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            # Convert non-serializable objects to dict
            serializable_summary = self._make_serializable(summary)
            json.dump(serializable_summary, f, indent=2, ensure_ascii=False)
        self._logger.info(f"💾 Chunking summary saved to: {summary_file}")

        # Save chunk type statistics
        if summary.get('chunk_statistics', {}).get('chunks_by_type'):
            stats_file = self.output_dir / f"{base_filename}_chunk_statistics.json"
            chunk_stats = {
                'chunk_type_distribution': summary['chunk_statistics']['chunks_by_type'],
                'total_chunks': summary.get('total_chunks_generated', 0),
                'total_documents': summary.get('successful_chunking', 0),
                'avg_processing_time': sum(self.chunk_stats['processing_times']) / len(self.chunk_stats['processing_times']) if self.chunk_stats['processing_times'] else 0,
                'processing_times': self.chunk_stats['processing_times']
            }

            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(chunk_stats, f, indent=2, ensure_ascii=False)
            self._logger.info(f"📊 Chunk statistics saved to: {stats_file}")

        # Save error report if there are failures
        if summary.get('failures') or summary.get('skipped'):
            error_file = self.output_dir / f"{base_filename}_chunking_errors.json"
            error_report = {
                'failed_chunking': summary.get('failures', []),
                'skipped_documents': summary.get('skipped', []),
                'error_summary': {
                    'total_errors': len(summary.get('failures', [])) + len(summary.get('skipped', [])),
                    'failure_count': len(summary.get('failures', [])),
                    'skipped_count': len(summary.get('skipped', []))
                }
            }

            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(error_report, f, indent=2, ensure_ascii=False)
            self._logger.info(f"🚨 Error report saved to: {error_file}")

    def _make_serializable(self, obj: Any) -> Any:
        """Convert objects to JSON-serializable format."""
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                result[k] = self._make_serializable(v)
            return result
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, DocumentChunk):
            return obj.to_dict()
        elif isinstance(obj, (DocumentMetadata, ChunkMetadata)):
            # Handle dataclasses with SegmentType
            result = asdict(obj)
            return self._make_serializable(result)
        elif isinstance(obj, SegmentType):
            return obj.value
        elif hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        else:
            return obj


def main():
    """Command-line interface for document chunking."""
    if len(sys.argv) < 3:
        print("Usage: python document_chunking_utility.py <input_file> <output_directory> [--mode=<mode>] [--start-index=<index>] [--max-words=<words>]")
        print("  input_file: Either extraction JSONL file or PDF summary JSON file")
        print("  output_directory: Directory for chunked output files")
        print("  --mode: 'jsonl' for extraction data, 'urls' for PDF summary (auto-detected if not specified)")
        print("  --start-index: Optional starting index for resumption")
        print("  --max-words: Maximum words per chunk before splitting at sentence boundary (default: 350)")
        print("\nExamples:")
        print("  python document_chunking_utility.py extractions.jsonl output/ --mode=jsonl")
        print("  python document_chunking_utility.py pdf_summary.json output/ --mode=urls")
        print("  python document_chunking_utility.py extractions.jsonl output/ --start-index=50")
        print("  python document_chunking_utility.py extractions.jsonl output/ --max-words=500")
        sys.exit(1)

    input_file = sys.argv[1]
    output_directory = sys.argv[2]

    # Parse optional arguments
    mode = None
    start_index = 0
    max_chunk_words = 350  # Default value

    for arg in sys.argv[3:]:
        if arg.startswith('--mode='):
            mode = arg.split('=')[1]
        elif arg.startswith('--start-index='):
            try:
                start_index = int(arg.split('=')[1])
            except ValueError:
                print("⚠️  Invalid start index format, using 0")
        elif arg.startswith('--max-words='):
            try:
                max_chunk_words = int(arg.split('=')[1])
            except ValueError:
                print("⚠️  Invalid max-words format, using default 350")
        elif arg.isdigit():
            # Legacy support for positional start index
            start_index = int(arg)

    # Check if input file exists
    if not Path(input_file).exists():
        print(f"❌ Error: Input file '{input_file}' not found")
        sys.exit(1)

    # Auto-detect mode if not specified
    if mode is None:
        if input_file.lower().endswith('.jsonl'):
            mode = 'jsonl'
        elif input_file.lower().endswith('.json'):
            mode = 'urls'
        else:
            print("❌ Error: Cannot auto-detect file type. Please specify --mode=jsonl or --mode=urls")
            sys.exit(1)

    print("🔧 Initializing Document Chunking Utility...")
    chunker = DocumentChunkingUtility(output_directory, max_chunk_words=max_chunk_words)

    base_filename = Path(input_file).stem

    if start_index > 0:
        print(f"📍 Starting from index: {start_index}")

    print(f"📏 Max chunk words: {max_chunk_words}")

    try:
        if mode == 'jsonl' or mode == 'extraction':
            # Process from extraction JSONL file
            print(f"📊 Loading extraction data from: {input_file}")
            extraction_data = chunker.load_extraction_data(input_file)

            if not extraction_data:
                print("❌ No extraction data found in input file")
                sys.exit(1)

            summary = chunker.batch_chunk_documents(extraction_data, base_filename, start_index)

        elif mode == 'urls':
            # Process from URL summary file using cache
            print(f"� Loading URLs from summary file: {input_file}")
            urls = chunker.load_urls_from_summary(input_file)

            if not urls:
                print("❌ No URLs found in summary file")
                sys.exit(1)

            summary = chunker.batch_chunk_from_urls(urls, base_filename, start_index)

        else:
            print(f"❌ Error: Unknown mode '{mode}'. Use 'jsonl' or 'urls'")
            sys.exit(1)

        # Save results
        chunker.save_results(summary, base_filename)

        # Print final summary
        print(f"\n🎉 Batch chunking completed!")
        total_key = 'total_documents' if mode == 'jsonl' else 'total_urls'
        print(f"📊 Total {mode}: {summary.get(total_key, 0)}")
        print(f"✅ Successful: {summary['successful_chunking']} ({summary['success_rate']:.1f}%)")
        print(f"❌ Failed: {summary['failed_chunking']}")
        print(f"⏭️  Skipped: {summary['skipped_documents']}")
        print(f"📄 Total chunks generated: {summary['total_chunks_generated']}")
        print(f"📈 Average chunks per document: {summary['avg_chunks_per_doc']:.1f}")
        print(f"⏱️  Processing time: {summary['processing_time_minutes']:.1f} minutes")

        # Print chunk type distribution
        if summary.get('chunk_statistics', {}).get('chunks_by_type'):
            print(f"\n📊 Chunk Type Distribution:")
            for chunk_type, count in summary['chunk_statistics']['chunks_by_type'].items():
                print(f"  {chunk_type}: {count}")

    except KeyboardInterrupt:
        print(f"\n🛑 Process interrupted. Processed {len(chunker.results)} documents.")
        if chunker.results:
            # Save partial results
            total_items = len(chunker.results) + len(chunker.failed_chunking) + len(chunker.skipped_documents)
            partial_summary = {
                'total_processed': total_items,
                'processed_documents': len(chunker.results),
                'successful_chunking': len([r for r in chunker.results if r['status'] == 'success']),
                'failed_chunking': len(chunker.failed_chunking),
                'skipped_documents': len(chunker.skipped_documents),
                'results': chunker.results,
                'failures': chunker.failed_chunking,
                'skipped': chunker.skipped_documents,
                'chunk_statistics': chunker.chunk_stats,
                'status': 'interrupted'
            }
            partial_filename = base_filename + "_partial"
            chunker.save_results(partial_summary, partial_filename)
            print(f"💾 Partial results saved")


if __name__ == "__main__":
    main()
