#!/usr/bin/env python3
"""
Citation Extractor Utility

This utility processes the chunked documents and extracts all citation chunks
into a separate JSONL file for specialized citation analysis and linking.
Maintains the same schema but organizes citations separately from content chunks.
"""

import argparse
import hashlib
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse


class CitationExtractor:
    """Extract and organize citation chunks from document chunking output."""

    def __init__(self, input_jsonl: str, output_dir: str,
                 mode: str = "pdf",
                 base_url: Optional[str] = None,
                 normalize_urls: bool = True,
                 filter_paragraphs: bool = True,
                 min_word_count: int = 5,
                 min_char_count: int = 50,
                 link_citations: bool = False):
        """
        Initialize the citation extractor.

        Args:
            input_jsonl: Path to the chunked documents JSONL file
            output_dir: Directory to save citation outputs
            mode: Processing mode ('pdf', 'inline', 'combined')
            base_url: Base URL for resolving relative URLs (default: https://en.wikipedia.org)
            normalize_urls: Whether to normalize URLs (remove tracking params, etc.)
            filter_paragraphs: Whether to filter and refine paragraph content
            min_word_count: Minimum word count for content filtering
            min_char_count: Minimum character count for content filtering
            link_citations: Whether to add bidirectional citation links (PDF mode only)
        """
        self.input_jsonl = Path(input_jsonl)
        self.output_dir = Path(output_dir)
        self.mode = mode.lower()
        self.base_url = base_url or "https://en.wikipedia.org"
        self.normalize_urls = normalize_urls
        self.filter_paragraphs = filter_paragraphs
        self.min_word_count = min_word_count
        self.min_char_count = min_char_count
        self.link_citations = link_citations

        # Validate mode
        valid_modes = ['pdf', 'inline', 'combined']
        if self.mode not in valid_modes:
            raise ValueError(f"Invalid mode '{mode}'. Must be one of: {valid_modes}")

        # Setup citation-ready output structure
        self._setup_citation_ready_structure()

        # Inline citation processing state
        self.url_to_citation_number = {}
        self.citation_chunks = []
        self.global_citation_counter = 1  # Global counter for consistent numbering
        self.global_citation_metadata = {}  # Store comprehensive metadata for each citation number
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='| %(asctime)s | %(levelname)s - %(name)s - %(message)s'
        )
        self.logger = logging.getLogger('CitationExtractor')

        # Statistics tracking (enhanced for inline processing)
        self.stats = {
            'total_chunks_processed': 0,
            'citations_extracted': 0,
            'content_chunks_retained': 0,
            'documents_processed': set(),
            'citations_by_document': {},
            'citations_by_type': {},
            'processing_start': datetime.now(),
            # Inline-specific stats
            'inline_citations_found': 0,
            'chunks_enriched': 0,
            'unique_urls': 0,
            'relative_urls_resolved': 0,
            'duplicate_urls_merged': 0,
            # Filtering stats
            'chunks_before_filtering': 0,
            'chunks_after_filtering': 0,
            'paragraphs_filtered': 0,
            'navigation_elements_removed': 0,
            'mode': self.mode
        }

        self.logger.info(f"🔗 Citation Extractor initialized")
        self.logger.info(f"  📁 Input: {self.input_jsonl}")
        self.logger.info(f"  📁 Output dir: {self.output_dir}")
        self.logger.info(f"  🔧 Mode: {self.mode}")
        if self.mode in ['inline', 'combined']:
            self.logger.info(f"  🌐 Base URL: {self.base_url}")
            self.logger.info(f"  🔗 Normalize URLs: {self.normalize_urls}")
            self.logger.info(f"  📝 Filter paragraphs: {self.filter_paragraphs}")
            if self.filter_paragraphs:
                self.logger.info(f"  📊 Min words: {self.min_word_count}, Min chars: {self.min_char_count}")

    def _setup_citation_ready_structure(self):
        """Setup citation-ready output directory structure."""
        # Create citation_ready subfolder
        self.citation_ready_dir = self.output_dir / "citation_ready"
        self.citation_ready_dir.mkdir(parents=True, exist_ok=True)

        # Set the actual output directory based on mode
        if self.mode in ['inline', 'combined']:
            # For inline/combined modes, use citation_ready subfolder
            self.actual_output_dir = self.citation_ready_dir
        else:
            # For PDF mode, use original output directory (backward compatibility)
            self.actual_output_dir = self.output_dir
            self.actual_output_dir.mkdir(parents=True, exist_ok=True)

    def _filter_paragraph_chunks(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter to keep only paragraph chunks (exclude headers)."""
        paragraph_chunks = []

        for chunk in chunks:
            chunk_metadata = chunk.get('chunk_metadata', {})
            if chunk_metadata.get('chunk_type') == 'paragraph':
                paragraph_chunks.append(chunk)

        self.stats['paragraphs_filtered'] = len(paragraph_chunks)
        return paragraph_chunks

    def _is_hyperlink_element(self, content: str) -> bool:
        """Check if content is primarily a hyperlink element [text](url)."""
        # Strip whitespace
        content = content.strip()

        # Pattern for markdown links: [text](url)
        markdown_link_pattern = r'^\s*\[([^\]]+)\]\(([^)]+)\)\s*$'

        # Check if entire content is just a markdown link
        if re.match(markdown_link_pattern, content):
            return True

        # Check if content is mostly markdown links (>80% of content)
        markdown_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content)
        if markdown_links:
            # Calculate what percentage of content is links
            total_link_chars = sum(len(f"[{text}]({url})") for text, url in markdown_links)
            link_percentage = total_link_chars / len(content)
            if link_percentage > 0.8:  # More than 80% is links
                return True

        return False

    def _filter_refined_content(self, content_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter content chunks to remove navigation elements and low-quality content."""
        refined_chunks = []
        navigation_removed = 0

        for chunk in content_chunks:
            content = chunk.get('content', '').strip()
            char_count = chunk['chunk_metadata']['char_count']
            word_count = chunk['chunk_metadata']['word_count']

            # Skip if too short
            if char_count < self.min_char_count or word_count < self.min_word_count:
                continue

            # Skip navigation/link elements
            if self._is_hyperlink_element(content):
                navigation_removed += 1
                continue

            # Skip if starts with common navigation patterns
            if content.startswith(('http', 'www.', 'mailto:', 'tel:')):
                navigation_removed += 1
                continue

            # Skip single words or very short phrases
            if len(content.split()) < self.min_word_count:
                continue

            # Skip content that's mostly punctuation or symbols
            alpha_chars = sum(1 for c in content if c.isalpha())
            if len(content) > 0 and alpha_chars / len(content) < 0.5:  # Less than 50% alphabetic
                continue

            refined_chunks.append(chunk)

        self.stats['navigation_elements_removed'] = navigation_removed
        return refined_chunks

    def _normalize_url(self, url: str) -> str:
        """Normalize URL for consistent processing."""
        if not self.normalize_urls:
            return url

        try:
            # Parse the URL
            parsed = urlparse(url)

            # Remove fragment (anchor) and common tracking parameters
            query_params = parse_qs(parsed.query)
            # Remove common tracking parameters
            tracking_params = {'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                             'fbclid', 'gclid', 'ref', 'source'}
            filtered_params = {k: v for k, v in query_params.items() if k not in tracking_params}

            # Rebuild query string
            new_query = urlencode(filtered_params, doseq=True)

            # Rebuild URL without fragment
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                ''  # No fragment
            ))

            return normalized
        except Exception:
            # Return original URL if parsing fails
            return url

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            return parsed.netloc
        except Exception:
            return "unknown"

    def _is_wikipedia_internal_link(self, url: str, anchor_text: str) -> bool:
        """Check if this is a Wikipedia UI/internal link that should be filtered."""
        # Filter Wikipedia action links (edit, history, etc.)
        wiki_action_patterns = [
            '/w/index.php?',  # Edit, history, etc.
            'action=edit',
            'action=history',
            'action=info',
            'Special:',  # Special pages
            'File_Upload_Wizard',
            'WhatLinksHere',
            'CiteThisPage',
            'UrlShortener',
            'QrCode',
            'DownloadAsPdf',
        ]

        url_lower = url.lower()
        for pattern in wiki_action_patterns:
            if pattern.lower() in url_lower:
                return True

        # Filter language/interwiki links by anchor text
        wiki_ui_anchors = [
            'view source', 'view history', 'what links here', 'upload file',
            'permanent link', 'page information', 'cite this page',
            'get shortened url', 'download qr', 'printable version',
            'wikimedia commons', 'wikidata item', 'bahasa indonesia',
            'bahasa melayu', 'oʻzbekcha', 'tiếng việt',
        ]

        anchor_lower = anchor_text.lower()
        for ui_text in wiki_ui_anchors:
            if ui_text in anchor_lower:
                return True

        # Filter geographic coordinates
        if 'geohack.toolforge.org' in url_lower or 'geographic coordinate' in anchor_lower:
            return True

        return False

    def _extract_inline_citations(self, content: str, source_chunk_id: str = None, doc_metadata: Dict = None) -> tuple[str, Dict[int, Dict[str, str]]]:
        """
        Extract citations from content and replace with numbered references.
        Handles punctuation: [text](url). becomes text [N].
        """
        # Pattern to match markdown links with optional trailing punctuation: [text](url)[.,;:!?]?
        pattern = r'\[([^\]]+)\]\(([^)]+)\)([.,;:!?]?)'

        metadata = {}

        def replace_citation(match):
            anchor_text = match.group(1)
            url = match.group(2)
            trailing_punct = match.group(3)  # Capture trailing punctuation

            # Resolve relative URLs to absolute URLs first
            if self.base_url and not url.startswith(('http://', 'https://', 'mailto:', 'tel:', '#')):
                url = urljoin(self.base_url, url)

            # Skip Wikipedia internal/UI links
            if self._is_wikipedia_internal_link(url, anchor_text):
                # Return just the anchor text without citation number
                return anchor_text + trailing_punct if trailing_punct else anchor_text

            # Normalize URL if enabled (remove tracking params, etc.)
            normalized_url = self._normalize_url(url) if self.normalize_urls else url

            # Get or assign global citation number
            if normalized_url not in self.url_to_citation_number:
                self.url_to_citation_number[normalized_url] = self.global_citation_counter
                # Store the first encounter of this URL in global metadata
                self.global_citation_metadata[self.global_citation_counter] = {
                    'url': normalized_url,
                    'anchor_text': anchor_text,
                    'domain': self._extract_domain(normalized_url),
                    'source_chunk_id': source_chunk_id,
                    'original_url': url,
                    'document_metadata': doc_metadata,  # Include document metadata
                    'all_anchor_texts': [anchor_text]  # Track all anchor texts for this URL
                }
                self.global_citation_counter += 1
            else:
                # URL already exists, add this anchor text if it's new
                citation_num = self.url_to_citation_number[normalized_url]
                existing_anchors = self.global_citation_metadata[citation_num]['all_anchor_texts']
                if anchor_text not in existing_anchors:
                    existing_anchors.append(anchor_text)

            citation_num = self.url_to_citation_number[normalized_url]

            # Store rich metadata for this citation
            metadata[citation_num] = {
                'url': normalized_url,
                'anchor_text': anchor_text,
                'domain': self._extract_domain(normalized_url),
                'source_chunk_id': source_chunk_id,
                'original_url': url
            }

            # Replace with: anchor_text [citation_num] + preserved punctuation
            # If there's trailing punctuation, put it after the citation number
            if trailing_punct:
                return f"{anchor_text} [{citation_num}]{trailing_punct}"
            else:
                return f"{anchor_text} [{citation_num}]"

        # Replace all citations in content
        processed_content = re.sub(pattern, replace_citation, content)

        return processed_content, metadata

    def _generate_citation_chunks(self, all_metadata: Dict[int, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate synthetic citation chunks from collected metadata."""
        citation_chunks = []

        for citation_num in sorted(all_metadata.keys()):
            metadata = all_metadata[citation_num]
            url = metadata['url']
            anchor_text = metadata.get('anchor_text', '')
            source_chunk_id = metadata.get('source_chunk_id', '')
            domain = metadata.get('domain', '')
            original_url = metadata.get('original_url', url)
            doc_metadata = metadata.get('document_metadata', {})

            # Create content in the correct format: "Citation: anchor_text -> url"
            content = f"Citation: {anchor_text} -> {url}"

            # Extract document info - use actual document_id from metadata
            doc_id = doc_metadata.get('document_id', 'unknown') if doc_metadata else 'unknown'
            if source_chunk_id and doc_id != 'unknown':
                citation_id = [doc_id, source_chunk_id, citation_num]
                chunk_id = f"citation_{doc_id}_{source_chunk_id}_{citation_num}"
            else:
                citation_id = ["unknown", "unknown", citation_num]
                chunk_id = f"citation_{citation_num}"

            # Generate citation chunk with rich metadata structure
            citation_chunk = {
                'content': content,
                'document_metadata': doc_metadata,  # Include document metadata
                'chunk_metadata': {
                    'chunk_id': chunk_id,
                    'chunk_index': -1,
                    'chunk_type': 'citation',
                    'citation_id': citation_id,
                    'citation_number': citation_num,
                    'citation_type': 'inline_link',
                    'cited_url': url,
                    'original_url': original_url,
                    'anchor_text': anchor_text,
                    'source_chunk_id': source_chunk_id,
                    'domain': domain,
                    'word_count': len(anchor_text.split()) if anchor_text else 0,
                    'char_count': len(content),
                    'contains_urls': True,
                    'is_citation': True
                },
                'processing_timestamp': datetime.now().timestamp()
            }
            citation_chunks.append(citation_chunk)

        return citation_chunks

    def _enrich_chunk_metadata(self, chunk: Dict[str, Any], metadata: Dict[int, str]) -> Dict[str, Any]:
        """Enrich chunk metadata with citation information."""
        enriched_chunk = chunk.copy()

        # Add citation metadata if any citations were found
        if metadata:
            enriched_chunk['chunk_metadata']['citations'] = metadata
            enriched_chunk['chunk_metadata']['citation_count'] = len(metadata)
            enriched_chunk['chunk_metadata']['has_citations'] = True
        else:
            enriched_chunk['chunk_metadata']['has_citations'] = False
            enriched_chunk['chunk_metadata']['citation_count'] = 0

        return enriched_chunk

    def extract_citations(self) -> Tuple[str, str, str]:
        """
        Extract citations from chunked documents into separate files.
        Mode-aware processing for PDF and inline citations.

        Returns:
            Tuple of (citations_file, content_file, summary_file) paths
        """
        self.logger.info(f"🚀 Starting citation extraction from {self.input_jsonl}")
        self.logger.info(f"🔧 Processing mode: {self.mode}")

        # Generate output filenames based on actual output directory
        base_name = self.input_jsonl.stem
        citations_file = self.actual_output_dir / f"{base_name}_citations.jsonl"
        content_file = self.actual_output_dir / f"{base_name}_content.jsonl"
        summary_file = self.actual_output_dir / f"{base_name}_citation_summary.json"

        citations = []
        content_chunks = []
        all_citation_metadata = {}  # For inline mode

        # Load all chunks first
        raw_chunks = []
        with open(self.input_jsonl, 'r', encoding='utf-8') as infile:
            for line_num, line in enumerate(infile, 1):
                try:
                    chunk = json.loads(line.strip())
                    raw_chunks.append(chunk)
                except json.JSONDecodeError as e:
                    self.logger.warning(f"⚠️  JSON decode error on line {line_num}: {e}")
                except Exception as e:
                    self.logger.error(f"❌ Error processing line {line_num}: {e}")

        self.stats['chunks_before_filtering'] = len(raw_chunks)

        # Apply filtering if enabled
        chunks_to_process = raw_chunks
        if self.mode in ['inline', 'combined'] and self.filter_paragraphs:
            self.logger.info(f"🔍 Filtering paragraph chunks")
            paragraph_chunks = self._filter_paragraph_chunks(raw_chunks)
            refined_chunks = self._filter_refined_content(paragraph_chunks)
            chunks_to_process = refined_chunks
            self.logger.info(f"📊 Filtered {len(raw_chunks)} → {len(chunks_to_process)} chunks")

        self.stats['chunks_after_filtering'] = len(chunks_to_process)

        # Process chunks based on mode
        if self.mode == 'pdf':
            # Original PDF processing logic
            citations, content_chunks = self._process_pdf_mode(chunks_to_process)
        elif self.mode == 'inline':
            # Inline citation processing
            content_chunks, all_citation_metadata = self._process_inline_mode(chunks_to_process)
            citations = self._generate_citation_chunks(all_citation_metadata)
        elif self.mode == 'combined':
            # Combined processing
            pdf_citations, pdf_content = self._process_pdf_mode(chunks_to_process)
            inline_content, all_citation_metadata = self._process_inline_mode(pdf_content)
            inline_citations = self._generate_citation_chunks(all_citation_metadata)

            # Combine results
            citations = pdf_citations + inline_citations
            content_chunks = inline_content

        # Update final statistics
        self.stats['total_chunks_processed'] = len(raw_chunks)
        self.stats['citations_extracted'] = len(citations)
        self.stats['content_chunks_retained'] = len(content_chunks)

        # Write output files
        self._write_output_files(citations, content_chunks, citations_file, content_file, summary_file)

        self.logger.info(f"✅ Citation extraction completed successfully")

        return str(citations_file), str(content_file), str(summary_file)

    def _is_standalone_citation(self, chunk: Dict[str, Any]) -> bool:
        """
        Check if a chunk is actually a standalone citation that was misclassified as a paragraph or list.
        Patterns:
        - Direct numbered: "4   Document reference..." (3+ spaces to distinguish from footnotes)
        - List-formatted: "- 4 Document reference..."
        """
        content = chunk.get('content', '').strip()
        chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')

        # Handle list-formatted citations (e.g., "- 4 A/77/356...")
        # These are often formatted as list_unordered or list_ordered
        if chunk_type in ['list_unordered', 'list_ordered']:
            # Check for list item citations: "- 4 ..." or "4. ..."
            if re.match(r'^[-*]\s*\d+\s+', content):  # "- 4 Document..."
                return True
            if re.match(r'^\d+\.\s+\d+\s+', content):  # "4. Document..." (list numbering + citation number)
                return True

        # Must start with a number followed by whitespace OR be just a number (split citation)
        if not re.match(r'^\d+(\s+|$)', content):
            return False

        # If it's ONLY a number (1-3 digits), it's a split citation fragment
        if re.match(r'^\d{1,3}$', content):
            return True

        # URLs without number prefix are split citations too
        if re.match(r'^https?://', content) and not re.match(r'^\d+\s+', content):
            return True

        # "Ibid." patterns without number prefix
        if re.match(r'^Ibid\.', content, re.IGNORECASE) and not re.match(r'^\d+\s+', content):
            return True

        # Should be relatively short (citations are typically concise references)
        if len(content) > 500:
            return False

        # CRITICAL: Primary citation pattern - number followed by 2+ spaces (or 3+ for stricter PDFs)
        # This distinguishes citations like "1   Consider Philip Stern" from footnote markers "1 text"
        if re.match(r'^\d+\s{2,}', content):
            return True

        # Common citation indicators (fallback patterns for edge cases)
        citation_indicators = [
            r'^\d+\s+https?://',  # URL citations: "23   https://..."
            r'^\d+\s+.*https?://',  # URL citations with text: "27   Document; and https://..."
            r'^\d+\s+[A-Z][a-z]+.*,.*\d{4}',  # Academic: "30   Author Name, Title (2020)"
            r'^\d+\s+[A-Z]/\d',  # UN docs: "109 A/79/588" or "23 A/77/35"
            r'^\d+\s+Ibid\.',  # Ibid references
            r'^\d+\s+See\s+',  # See references
            r'^\d+\s+.*,\s+para',  # Paragraph refs: "25   Document, para. 80"
            r'^\d+\s+.*\d{4}\)',  # Year in parens: "Document (2020)"
            r'^\d+\s+.*v\.',  # Legal cases: "Case v. Case"
            r'^\d+\s+www\.',  # Web URLs: "30   www.example.com"
            r'^\d+\s+.*www\.',  # Web URLs with text: "30   Common Article...; and www.example.com"
            r'^\d+\s+.*(?:Article|Convention|Treaty|Agreement|Protocol)',  # Legal documents: "28   Arms Trade Treaty..."
        ]

        return any(re.search(pattern, content, re.IGNORECASE) for pattern in citation_indicators)

    def _split_multi_citations(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Split chunks that contain multiple citations on the same line.
        Handles two patterns:
        1. Consecutive numbers at start: "36 37 38 Convention..."
        2. Embedded citations: "28    Arms Trade Treaty... 29 Ibid..."
        """
        split_chunks = []
        split_count = 0

        for chunk in chunks:
            chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')
            content = chunk.get('content', '').strip()

            # Only process citation-type chunks
            if chunk_type != 'citation':
                split_chunks.append(chunk)
                continue

            # Pattern 1: Consecutive numbers at start: "36 37 38 Convention..."
            consecutive_match = re.match(r'^(\d+(?:\s+\d+)+)\s+(.+)$', content)

            if consecutive_match:
                numbers_str = consecutive_match.group(1)  # "36 37 38"
                citation_text = consecutive_match.group(2)  # The actual citation content
                numbers = re.findall(r'\d+', numbers_str)

                if len(numbers) > 1:
                    # Split into separate citations
                    for i, num in enumerate(numbers):
                        new_chunk = chunk.copy()
                        new_chunk['chunk_metadata'] = chunk['chunk_metadata'].copy()

                        if i == 0:
                            new_content = f"{num}    {citation_text}"
                        else:
                            new_content = f"{num}    Ibid."

                        new_chunk['content'] = new_content
                        new_chunk['chunk_metadata']['char_count'] = len(new_content)
                        new_chunk['chunk_metadata']['word_count'] = len(new_content.split())
                        new_chunk['chunk_metadata']['chunk_id'] = chunk['chunk_metadata']['chunk_id'] + f"_split_{i}"

                        split_chunks.append(new_chunk)

                    split_count += 1
                    continue

            # Pattern 2a: Embedded citations with URLs: "242    https://... 243 https://..."
            # URL citations don't have period/semicolon between them
            url_embedded_match = re.match(r'^(\d+)\s+(.+?)\s+(\d+)\s+(https?://.+)$', content)

            if url_embedded_match:
                num1 = url_embedded_match.group(1)
                text1 = url_embedded_match.group(2)
                num2 = url_embedded_match.group(3)
                text2 = url_embedded_match.group(4)

                # Create two separate citations
                chunk1 = chunk.copy()
                chunk1['chunk_metadata'] = chunk['chunk_metadata'].copy()
                chunk1['content'] = f"{num1}    {text1}"
                chunk1['chunk_metadata']['char_count'] = len(chunk1['content'])
                chunk1['chunk_metadata']['word_count'] = len(chunk1['content'].split())
                chunk1['chunk_metadata']['chunk_id'] = chunk['chunk_metadata']['chunk_id'] + "_split_0"

                chunk2 = chunk.copy()
                chunk2['chunk_metadata'] = chunk['chunk_metadata'].copy()
                chunk2['content'] = f"{num2}    {text2}"
                chunk2['chunk_metadata']['char_count'] = len(chunk2['content'])
                chunk2['chunk_metadata']['word_count'] = len(chunk2['content'].split())
                chunk2['chunk_metadata']['chunk_id'] = chunk['chunk_metadata']['chunk_id'] + "_split_1"

                split_chunks.extend([chunk1, chunk2])
                split_count += 1
                continue

            # Pattern 2b: Embedded citations with punctuation: "28    Arms Trade Treaty... 29 Ibid..."
            # Look for pattern: number + text + period/semicolon + space + number + citation-like text
            # Avoid matching article numbers like "Articles 2(4), and 51"
            # Only split when second number starts a new citation (capital letter or citation keyword)
            embedded_match = re.match(r'^(\d+)\s+(.+?[.;])\s+(\d+)\s+((?:Ibid|See|Article|Convention|Treaty|Agreement|Protocol|[A-Z][a-z]+).+)$', content)

            if embedded_match:
                num1 = embedded_match.group(1)
                text1 = embedded_match.group(2)
                num2 = embedded_match.group(3)
                text2 = embedded_match.group(4)

                # Additional validation: text2 should look like a citation start, not a continuation
                # Reject if text2 is very short or starts with lowercase/connector words
                if len(text2) < 5 or re.match(r'^(?:and|or|the|of|in)\s', text2, re.IGNORECASE):
                    split_chunks.append(chunk)
                    continue

                # Create two separate citations
                chunk1 = chunk.copy()
                chunk1['chunk_metadata'] = chunk['chunk_metadata'].copy()
                chunk1['content'] = f"{num1}    {text1}"
                chunk1['chunk_metadata']['char_count'] = len(chunk1['content'])
                chunk1['chunk_metadata']['word_count'] = len(chunk1['content'].split())
                chunk1['chunk_metadata']['chunk_id'] = chunk['chunk_metadata']['chunk_id'] + "_split_0"

                chunk2 = chunk.copy()
                chunk2['chunk_metadata'] = chunk['chunk_metadata'].copy()
                chunk2['content'] = f"{num2}    {text2}"
                chunk2['chunk_metadata']['char_count'] = len(chunk2['content'])
                chunk2['chunk_metadata']['word_count'] = len(chunk2['content'].split())
                chunk2['chunk_metadata']['chunk_id'] = chunk['chunk_metadata']['chunk_id'] + "_split_1"

                split_chunks.extend([chunk1, chunk2])
                split_count += 1
                continue

            # No split needed
            split_chunks.append(chunk)

        if split_count > 0:
            self.logger.info(f"  ✂️  Split {split_count} multi-citation chunks into separate citations")

        return split_chunks

    def _merge_split_citations(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge citation chunks that were split across multiple lines.
        Pattern: "23" on one line, followed by citation content on next line(s).
        Handles edge case where number and text are separate chunks.
        """
        merged_chunks = []
        i = 0
        merge_count = 0

        while i < len(chunks):
            chunk = chunks[i]
            content = chunk.get('content', '').strip()
            chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')

            # Check if this is a citation number fragment (just a number)
            if chunk_type == 'citation' and re.match(r'^\d{1,3}$', content):
                # Look ahead to merge subsequent citation fragments
                citation_number = content
                merged_content = citation_number
                merged_parts = [chunk]
                j = i + 1

                # Merge up to 10 subsequent chunks that are part of THIS citation
                max_lookahead = 10
                while j < len(chunks) and (j - i) < max_lookahead:
                    next_chunk = chunks[j]
                    next_content = next_chunk.get('content', '').strip()
                    next_type = next_chunk.get('chunk_metadata', {}).get('chunk_type')

                    # STOP CONDITION 1: Next citation starts (numbered pattern with 2+ spaces)
                    # Pattern: "2   Consider" or "2  Text" indicates citation 2 starts
                    if re.match(r'^\d+\s{2,}', next_content):
                        break

                    # STOP CONDITION 2: Bare citation number (split citation for NEXT entry)
                    # Pattern: "2" alone indicates the start of citation 2
                    if re.match(r'^\d{1,3}$', next_content):
                        break

                    # STOP CONDITION 3: Paragraph chunk (not a citation fragment)
                    # This indicates we've moved into body text, not citation content
                    if next_type == 'paragraph' and not self._looks_like_citation_content(next_content):
                        break

                    # Determine if we should merge this chunk (it's part of the current citation)
                    should_merge = False

                    if next_type == 'citation':
                        # Always merge citation-type chunks (unless it's a new citation number, handled above)
                        should_merge = True
                    elif next_type == 'paragraph':
                        # Merge paragraphs that look like citation content
                        should_merge = self._looks_like_citation_content(next_content)

                    if should_merge:
                        merged_content += "   " + next_content
                        merged_parts.append(next_chunk)
                        j += 1
                    else:
                        # Stop merging
                        break

                # Create merged chunk if we found parts to merge
                if len(merged_parts) > 1:
                    merged_chunk = chunk.copy()
                    merged_chunk['content'] = merged_content
                    merged_chunk['chunk_metadata'] = chunk['chunk_metadata'].copy()
                    merged_chunk['chunk_metadata']['char_count'] = len(merged_content)
                    merged_chunk['chunk_metadata']['word_count'] = len(merged_content.split())
                    merged_chunk['chunk_metadata']['end_position'] = merged_parts[-1]['chunk_metadata']['end_position']
                    merged_chunks.append(merged_chunk)
                    merge_count += 1
                    i = j  # Skip the merged parts
                else:
                    # Standalone number without continuation - keep it
                    merged_chunks.append(chunk)
                    i += 1
            else:
                merged_chunks.append(chunk)
                i += 1

        if merge_count > 0:
            self.logger.info(f"  🔗 Merged {merge_count} split citation fragments")

        return merged_chunks

    def _looks_like_citation_content(self, content: str) -> bool:
        """
        Check if text looks like citation content (vs body paragraph).
        Used to determine merge boundaries for split citations.
        """
        # Key patterns: URLs, legal references, academic citations, Ibid
        return bool(
            re.match(r'^https?://', content) or
            re.match(r'^Ibid\.', content, re.IGNORECASE) or
            re.match(r'^See\s+', content, re.IGNORECASE) or
            re.search(r'\bpara\.?\s*\d+', content, re.IGNORECASE) or
            re.search(r'\bI\.C\.J\.\s+Reports', content) or
            re.search(r'Application of the Convention', content) or
            re.search(r'Advisory Opinion', content) or
            re.search(r'Legal Consequences', content) or
            re.search(r'\d{4}\)', content) or  # Year in parens
            re.search(r',\s*vol\.\s*\d+', content, re.IGNORECASE) or
            re.search(r',\s*No\.\s*\d+', content, re.IGNORECASE) or
            (re.match(r'^[A-Z]', content) and len(content) < 300 and
             any(indicator in content.lower() for indicator in
                 ['para', 'page', 'vol', 'ed.', 'op.', 'cit.', 'available at',
                  'report', 'order', 'judgment', 'opinion', 'gazette']))
        )

    def _reclassify_citations(self, chunks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Reclassify paragraph chunks that are actually citations.

        Args:
            chunks: List of all chunks

        Returns:
            Tuple of (content_chunks, citation_chunks)
        """
        # First pass: reclassify chunks in place (modify the actual chunks)
        reclassified_count = 0

        for chunk in chunks:
            chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')
            content = chunk.get('content', '').strip()

            # Check if paragraph/list is actually a citation
            if chunk_type in ['paragraph', 'list_ordered', 'list_unordered']:
                # Special case: bare citation numbers (e.g., "104" on its own line)
                # These are citation fragments that will be merged later
                if re.match(r'^\d{1,3}$', content):
                    chunk['chunk_metadata']['chunk_type'] = 'citation'
                    chunk['chunk_metadata']['citation_type'] = 'bibliography_entry'
                    reclassified_count += 1
                elif self._is_standalone_citation(chunk):
                    # Reclassify as citation IN PLACE
                    chunk['chunk_metadata']['chunk_type'] = 'citation'

                    # Normalize content: remove list markers from citations
                    content = chunk.get('content', '').strip()
                    # Remove leading list markers (- or * followed by space)
                    normalized_content = re.sub(r'^[-*]\s+', '', content)
                    chunk['content'] = normalized_content

                    # Update character and word counts after normalization
                    chunk['chunk_metadata']['char_count'] = len(normalized_content)
                    chunk['chunk_metadata']['word_count'] = len(normalized_content.split())

                    # Try to detect citation type
                    if re.search(r'https?://', normalized_content):
                        chunk['chunk_metadata']['citation_type'] = 'official_doc'
                    elif re.search(r'v\.|vs\.', normalized_content, re.IGNORECASE):
                        chunk['chunk_metadata']['citation_type'] = 'legal_case'
                    elif re.search(r'[A-Z][a-z]+.*,.*\d{4}', normalized_content):
                        chunk['chunk_metadata']['citation_type'] = 'academic_ref'
                    else:
                        chunk['chunk_metadata']['citation_type'] = 'bibliography_entry'

                    reclassified_count += 1

        if reclassified_count > 0:
            self.logger.info(f"  📝 Reclassified {reclassified_count} paragraph chunks as citations")

        # Split multi-citation chunks (like "28 29 Arms Trade Treaty...")
        # Do this BEFORE merging to handle cases where multiple citations are on one line
        chunks = self._split_multi_citations(chunks)

        # Merge split citation fragments (like "2" on one line, citation content on next)
        # Important: Work with reclassified chunks sorted by index
        chunks_sorted = sorted(chunks, key=lambda c: c.get('chunk_metadata', {}).get('chunk_index', 0))
        merged_chunks = self._merge_split_citations(chunks_sorted)

        # Final separation after merging
        citation_chunks = [c for c in merged_chunks if c.get('chunk_metadata', {}).get('chunk_type') == 'citation']
        content_chunks = [c for c in merged_chunks if c.get('chunk_metadata', {}).get('chunk_type') != 'citation']

        return content_chunks, citation_chunks

    def _process_pdf_mode(self, chunks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Process chunks in PDF mode (original logic)."""

        # Reclassify misidentified citations if linking is enabled
        if self.link_citations:
            self.logger.info(f"🔍 Checking for misclassified citations...")
            # Reclassify before processing - this already separates content and citations
            content_chunks, citations = self._reclassify_citations(chunks)

            # Track document statistics for all chunks
            for chunk in content_chunks + citations:
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                source_url = chunk.get('document_metadata', {}).get('source_url', 'unknown')
                if doc_id:
                    self.stats['documents_processed'].add(doc_id)
                    if doc_id not in self.stats['citations_by_document']:
                        self.stats['citations_by_document'][doc_id] = {
                            'source_url': source_url,
                            'citation_count': 0,
                            'content_count': 0
                        }

                chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')
                if chunk_type == 'citation':
                    # Track citation type
                    citation_type = chunk.get('chunk_metadata', {}).get('citation_type')
                    if citation_type:
                        if isinstance(citation_type, (list, tuple)) and len(citation_type) > 0:
                            citation_type = citation_type[0]  # Extract first element if tuple/list

                        if citation_type not in self.stats['citations_by_type']:
                            self.stats['citations_by_type'][citation_type] = 0
                        self.stats['citations_by_type'][citation_type] += 1

                    # Update document stats
                    if doc_id:
                        self.stats['citations_by_document'][doc_id]['citation_count'] += 1
                else:
                    # Update document stats
                    if doc_id:
                        self.stats['citations_by_document'][doc_id]['content_count'] += 1

            # Add bidirectional links
            content_chunks, citations = self._link_citations_bidirectional(citations, content_chunks)

        else:
            # Original non-linking path
            citations = []
            content_chunks = []

            for chunk in chunks:
                # Track document
                doc_id = chunk.get('document_metadata', {}).get('document_id')
                source_url = chunk.get('document_metadata', {}).get('source_url', 'unknown')
                if doc_id:
                    self.stats['documents_processed'].add(doc_id)
                    if doc_id not in self.stats['citations_by_document']:
                        self.stats['citations_by_document'][doc_id] = {
                            'source_url': source_url,
                            'citation_count': 0,
                            'content_count': 0
                        }

                # Check if this is a citation chunk
                chunk_type = chunk.get('chunk_metadata', {}).get('chunk_type')
                if chunk_type == 'citation':
                    citations.append(chunk)

                    # Track citation type
                    citation_type = chunk.get('chunk_metadata', {}).get('citation_type')
                    if citation_type:
                        if isinstance(citation_type, (list, tuple)) and len(citation_type) > 0:
                            citation_type = citation_type[0]  # Extract first element if tuple/list

                        if citation_type not in self.stats['citations_by_type']:
                            self.stats['citations_by_type'][citation_type] = 0
                        self.stats['citations_by_type'][citation_type] += 1

                    # Update document stats
                    if doc_id:
                        self.stats['citations_by_document'][doc_id]['citation_count'] += 1
                else:
                    content_chunks.append(chunk)

                    # Update document stats
                    if doc_id:
                        self.stats['citations_by_document'][doc_id]['content_count'] += 1

        return citations, content_chunks

    def _process_inline_mode(self, chunks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
        """Process chunks in inline mode."""
        processed_chunks = []
        all_citation_metadata = {}

        for chunk in chunks:
            content = chunk.get('content', '')
            source_chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id')
            doc_metadata = chunk.get('document_metadata', {})

            # Extract and replace inline citations
            processed_content, chunk_metadata = self._extract_inline_citations(
                content, source_chunk_id, doc_metadata
            )

            # Update chunk with processed content
            updated_chunk = chunk.copy()
            updated_chunk['content'] = processed_content

            # Enrich metadata
            enriched_chunk = self._enrich_chunk_metadata(updated_chunk, chunk_metadata)
            processed_chunks.append(enriched_chunk)

            # Update statistics (don't merge metadata - use global tracking)
            if chunk_metadata:
                self.stats['chunks_enriched'] += 1
                self.stats['inline_citations_found'] += len(chunk_metadata)

            # Track document
            doc_id = chunk.get('document_metadata', {}).get('document_id')
            source_url = chunk.get('document_metadata', {}).get('source_url', 'unknown')
            if doc_id:
                self.stats['documents_processed'].add(doc_id)
                if doc_id not in self.stats['citations_by_document']:
                    self.stats['citations_by_document'][doc_id] = {
                        'source_url': source_url,
                        'citation_count': 0,
                        'content_count': 0
                    }

                # Update content count
                self.stats['citations_by_document'][doc_id]['content_count'] += 1

                # Add inline citations to citation count
                if chunk_metadata:
                    self.stats['citations_by_document'][doc_id]['citation_count'] += len(chunk_metadata)

        # Update unique URLs count
        self.stats['unique_urls'] = len(self.global_citation_metadata)

        return processed_chunks, self.global_citation_metadata

    def _write_output_files(self, citations: List[Dict[str, Any]], content_chunks: List[Dict[str, Any]],
                           citations_file: Path, content_file: Path, summary_file: Path):
        """Write output files with citations, content, and summary."""
        # Write citations file
        self.logger.info(f"💾 Writing {len(citations)} citations to {citations_file}")
        with open(citations_file, 'w', encoding='utf-8') as outfile:
            for citation in citations:
                outfile.write(json.dumps(citation, ensure_ascii=False) + '\n')

        # Write content file (without citations)
        self.logger.info(f"💾 Writing {len(content_chunks)} content chunks to {content_file}")
        with open(content_file, 'w', encoding='utf-8') as outfile:
            for chunk in content_chunks:
                outfile.write(json.dumps(chunk, ensure_ascii=False) + '\n')

        # Generate and write summary
        summary = self._generate_summary()
        self.logger.info(f"📊 Writing citation summary to {summary_file}")
        with open(summary_file, 'w', encoding='utf-8') as outfile:
            json.dump(summary, outfile, indent=2, ensure_ascii=False, default=str)

        # Automatically split citations by document
        if citations:
            self.logger.info(f"📂 Splitting citations by document...")
            self._split_citations_by_document(citations, citations_file)

    def _split_citations_by_document(self, citations: List[Dict[str, Any]], citations_file: Path):
        """
        Split citations into separate files per document.

        Args:
            citations: List of citation chunks
            citations_file: Path to main citations file
        """
        from collections import defaultdict

        # Create by_document subdirectory
        by_doc_dir = citations_file.parent / "by_document"
        by_doc_dir.mkdir(exist_ok=True)

        # Group citations by document
        citations_by_doc = defaultdict(list)
        for citation in citations:
            source_url = citation.get('document_metadata', {}).get('source_url', 'unknown')
            citations_by_doc[source_url].append(citation)

        # Write separate files
        doc_summary = {}
        for source_url, doc_citations in sorted(citations_by_doc.items()):
            # Extract filename from URL
            filename = source_url.split('://')[-1] if '://' in source_url else source_url
            # Remove .pdf extension and sanitize
            filename = filename.replace('.pdf', '').replace('/', '_').replace(' ', '_')

            # Write citations for this document
            output_file = by_doc_dir / f"{filename}_citations.jsonl"
            with open(output_file, 'w', encoding='utf-8') as f:
                for citation in doc_citations:
                    f.write(json.dumps(citation, ensure_ascii=False) + '\n')

            # Calculate statistics
            with_usage = sum(1 for c in doc_citations
                           if c.get('chunk_metadata', {}).get('citation_usage_count', 0) > 0)
            usage_pct = (with_usage / len(doc_citations) * 100) if doc_citations else 0

            doc_summary[filename] = {
                'source_url': source_url,
                'citation_count': len(doc_citations),
                'citations_with_usage': with_usage,
                'usage_percentage': round(usage_pct, 1),
                'output_file': str(output_file.name)
            }

            self.logger.info(f"  ✅ {filename}: {len(doc_citations)} citations ({with_usage} with usage, {usage_pct:.1f}%)")

        # Write summary
        summary_file = by_doc_dir / "citations_by_document_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump({
                'total_documents': len(doc_summary),
                'total_citations': len(citations),
                'documents': doc_summary,
                'generated_at': datetime.now().isoformat()
            }, f, indent=2, ensure_ascii=False)

        self.logger.info(f"📊 Summary: {summary_file}")
        self.logger.info(f"📁 Output: {by_doc_dir}")
        self.logger.info(f"✅ Split {len(doc_summary)} documents")

    def _extract_citation_numbers(self, content: str) -> List[int]:
        """
        Extract citation numbers from content (superscript-style).
        Looks for patterns:
        - After punctuation: '. 3 ' or "'. 3 " or ', 4 ' or '. 5 After'
        - After word boundaries: 'capitalism'. 3 The'
        - Standalone with spacing: 'sector 1  Commercial' (2+ spaces after number)

        Args:
            content: Text content to search

        Returns:
            List of citation numbers found
        """
        citation_numbers = set()

        # Pattern 1: After punctuation (period, comma, quote, apostrophe, paren)
        # Handles: ". 3 ", "'. 3 ", ", 4 ", ". 5 After", "capitalism'. 3 The"
        pattern1 = r'[.,;:!?\'\"\)]\.?\s*(\d+)(?=\s|[,;.!?]|$)'
        matches1 = re.findall(pattern1, content)
        citation_numbers.update(int(num) for num in matches1)

        # Pattern 2: Number followed by 2+ spaces (typical superscript spacing)
        # Handles: 'sector 1  Commercial', 'lands 2  -'
        pattern2 = r'\b(\d{1,3})\s{2,}'
        matches2 = re.findall(pattern2, content)
        citation_numbers.update(int(num) for num in matches2)

        # Return sorted list (maintains order from smaller to larger)
        return sorted(list(citation_numbers))

    def _get_citation_number_from_chunk(self, citation_chunk: Dict[str, Any]) -> Optional[int]:
        """
        Extract citation number from a citation chunk's content.
        Citation chunks start with the number: "1   https://..."

        Args:
            citation_chunk: Citation chunk dictionary

        Returns:
            Citation number or None
        """
        content = citation_chunk.get('content', '').strip()
        match = re.match(r'^(\d+)\s+', content)
        if match:
            return int(match.group(1))
        return None

    def _link_citations_bidirectional(self, citations: List[Dict[str, Any]],
                                     content_chunks: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Add bidirectional citation links between content and citation chunks.

        Args:
            citations: List of citation chunks
            content_chunks: List of content chunks

        Returns:
            Tuple of (linked_content, linked_citations)
        """
        self.logger.info(f"🔗 Adding bidirectional citation links...")

        # Build citation index by (document_id, citation_number) -> list of citations
        # Store ALL citations with same number, then pick closest by position
        citation_index = {}  # (document_id, citation_number) -> list of chunks
        for citation in citations:
            citation_num = self._get_citation_number_from_chunk(citation)
            doc_id = citation.get('document_metadata', {}).get('document_id')
            if citation_num and doc_id:
                key = (doc_id, citation_num)
                if key not in citation_index:
                    citation_index[key] = []
                citation_index[key].append(citation)

        # Track citation usage
        citation_usage = defaultdict(list)  # (document_id, citation_number) -> list of chunk_ids

        # Process content chunks and add forward links
        linked_content = []
        for chunk in content_chunks:
            content = chunk.get('content', '')
            chunk_id = chunk.get('chunk_metadata', {}).get('chunk_id', 'unknown')
            doc_id = chunk.get('document_metadata', {}).get('document_id')
            chunk_pos = chunk.get('chunk_metadata', {}).get('start_position', 0)

            # Extract citation numbers from content
            citation_numbers = self._extract_citation_numbers(content)

            if citation_numbers:
                # Remove duplicates while preserving order
                unique_citations = list(dict.fromkeys(citation_numbers))

                # Add citation metadata to chunk
                chunk['chunk_metadata']['has_citations'] = True
                chunk['chunk_metadata']['citation_count'] = len(unique_citations)
                chunk['chunk_metadata']['citation_numbers'] = unique_citations

                # Build list of citation chunk IDs that exist (from same document)
                # Pick closest citation by position if multiple exist with same number
                citation_chunk_ids = []
                for cit_num in unique_citations:
                    citation_key = (doc_id, cit_num)
                    if citation_key in citation_index:
                        # Get all citations with this number
                        candidate_citations = citation_index[citation_key]

                        # Pick the closest citation by position
                        # Citations AFTER content chunk (typical pattern: content -> citations at end)
                        # OR citations BEFORE but closest to content chunk
                        closest_citation = None
                        min_distance = float('inf')

                        for cit_chunk in candidate_citations:
                            cit_pos = cit_chunk.get('chunk_metadata', {}).get('start_position', 0)
                            # Prefer citations that come after content (typical footnote pattern)
                            # But if none exist after, use the closest one before
                            distance = abs(cit_pos - chunk_pos)

                            # Bonus for citations that come after (typical pattern)
                            if cit_pos > chunk_pos:
                                distance = distance * 0.5  # Make "after" citations closer

                            if distance < min_distance:
                                min_distance = distance
                                closest_citation = cit_chunk

                        if closest_citation:
                            cit_chunk_id = closest_citation.get('chunk_metadata', {}).get('chunk_id')
                            if cit_chunk_id:
                                citation_chunk_ids.append(cit_chunk_id)
                                # Track usage with the specific citation chunk
                                cit_internal_key = (doc_id, cit_num, cit_chunk_id)
                                citation_usage[cit_internal_key].append(chunk_id)

                chunk['chunk_metadata']['references_citations'] = citation_chunk_ids
            else:
                chunk['chunk_metadata']['has_citations'] = False
                chunk['chunk_metadata']['citation_count'] = 0
                chunk['chunk_metadata']['citation_numbers'] = []
                chunk['chunk_metadata']['references_citations'] = []

            linked_content.append(chunk)

        # Add backlinks to citation chunks
        linked_citations = []
        for citation in citations:
            citation_num = self._get_citation_number_from_chunk(citation)
            citation_id = citation.get('chunk_metadata', {}).get('chunk_id')
            doc_id = citation.get('document_metadata', {}).get('document_id')
            if citation_num and doc_id and citation_id:
                # Look up usage by (doc_id, citation_num, citation_chunk_id)
                citation_key = (doc_id, citation_num, citation_id)
                citing_chunks = citation_usage.get(citation_key, [])
                citation['chunk_metadata']['cited_by_chunks'] = citing_chunks
                citation['chunk_metadata']['citation_usage_count'] = len(citing_chunks)
            else:
                citation['chunk_metadata']['cited_by_chunks'] = []
                citation['chunk_metadata']['citation_usage_count'] = 0

            linked_citations.append(citation)

        # Log linking statistics
        content_with_cites = sum(1 for c in linked_content if c['chunk_metadata']['has_citations'])
        cites_with_backlinks = sum(1 for c in linked_citations if c['chunk_metadata']['citation_usage_count'] > 0)

        self.logger.info(f"  ✅ Linked {content_with_cites}/{len(content_chunks)} content chunks to citations")
        self.logger.info(f"  ✅ {cites_with_backlinks}/{len(citations)} citations have backlinks")

        return linked_content, linked_citations

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate comprehensive citation extraction summary."""
        processing_time = datetime.now() - self.stats['processing_start']

        # Calculate percentages
        total_chunks = self.stats['total_chunks_processed']
        citation_percentage = (self.stats['citations_extracted'] / total_chunks * 100) if total_chunks > 0 else 0

        # Document statistics
        documents_with_citations = sum(1 for doc_stats in self.stats['citations_by_document'].values()
                                     if doc_stats['citation_count'] > 0)

        # Top cited documents
        top_cited_docs = sorted(
            [(doc_id, stats) for doc_id, stats in self.stats['citations_by_document'].items()],
            key=lambda x: x[1]['citation_count'],
            reverse=True
        )[:10]

        # Base summary structure
        summary = {
            'extraction_metadata': {
                'extraction_timestamp': datetime.now().isoformat(),
                'input_file': str(self.input_jsonl),
                'processing_time_seconds': processing_time.total_seconds(),
                'processing_time_minutes': processing_time.total_seconds() / 60,
                'mode': self.mode
            },
            'extraction_statistics': {
                'total_chunks_processed': self.stats['total_chunks_processed'],
                'citations_extracted': self.stats['citations_extracted'],
                'content_chunks_retained': self.stats['content_chunks_retained'],
                'citation_percentage': round(citation_percentage, 2),
                'total_documents': len(self.stats['documents_processed']),
                'documents_with_citations': documents_with_citations,
                'documents_without_citations': len(self.stats['documents_processed']) - documents_with_citations
            },
            'citation_type_distribution': self.stats['citations_by_type'],
            'top_cited_documents': [
                {
                    'document_id': doc_id,
                    'source_url': stats['source_url'],
                    'citation_count': stats['citation_count'],
                    'content_count': stats['content_count'],
                    'citation_density': round(stats['citation_count'] / (stats['citation_count'] + stats['content_count']) * 100, 2) if (stats['citation_count'] + stats['content_count']) > 0 else 0
                }
                for doc_id, stats in top_cited_docs
            ],
            'document_statistics': {
                doc_id: {
                    'source_url': stats['source_url'],
                    'citation_count': stats['citation_count'],
                    'content_count': stats['content_count'],
                    'total_chunks': stats['citation_count'] + stats['content_count'],
                    'citation_density_percent': round(stats['citation_count'] / (stats['citation_count'] + stats['content_count']) * 100, 2) if (stats['citation_count'] + stats['content_count']) > 0 else 0
                }
                for doc_id, stats in self.stats['citations_by_document'].items()
            }
        }

        # Add mode-specific statistics
        if self.mode in ['inline', 'combined']:
            summary['inline_processing_statistics'] = {
                'inline_citations_found': self.stats.get('inline_citations_found', 0),
                'chunks_enriched': self.stats.get('chunks_enriched', 0),
                'unique_urls': self.stats.get('unique_urls', 0),
                'relative_urls_resolved': self.stats.get('relative_urls_resolved', 0),
                'duplicate_urls_merged': self.stats.get('duplicate_urls_merged', 0)
            }

            # Add filtering statistics if enabled
            if self.filter_paragraphs:
                summary['filtering_statistics'] = {
                    'chunks_before_filtering': self.stats.get('chunks_before_filtering', 0),
                    'chunks_after_filtering': self.stats.get('chunks_after_filtering', 0),
                    'paragraphs_filtered': self.stats.get('paragraphs_filtered', 0),
                    'navigation_elements_removed': self.stats.get('navigation_elements_removed', 0),
                    'filtering_efficiency': round((self.stats.get('chunks_before_filtering', 0) - self.stats.get('chunks_after_filtering', 0)) / max(self.stats.get('chunks_before_filtering', 1), 1) * 100, 2)
                }

        return summary

    def _print_summary_report(self, summary: Dict[str, Any]):
        """Print a formatted summary report to console."""
        print("\n" + "="*80)
        print("🔗 CITATION EXTRACTION SUMMARY")
        print("="*80)

        stats = summary['extraction_statistics']
        print(f"📊 Total chunks processed: {stats['total_chunks_processed']:,}")
        print(f"🔗 Citations extracted: {stats['citations_extracted']:,} ({stats['citation_percentage']}%)")
        print(f"📄 Content chunks retained: {stats['content_chunks_retained']:,}")
        print(f"📚 Total documents: {stats['total_documents']}")
        print(f"📖 Documents with citations: {stats['documents_with_citations']}")
        print(f"📑 Documents without citations: {stats['documents_without_citations']}")

        if summary['citation_type_distribution']:
            print(f"\n📈 Citation Type Distribution:")
            for citation_type, count in sorted(summary['citation_type_distribution'].items(),
                                             key=lambda x: x[1], reverse=True):
                print(f"  {citation_type}: {count}")

        if summary['top_cited_documents']:
            print(f"\n🏆 Top 5 Documents by Citation Count:")
            for i, doc in enumerate(summary['top_cited_documents'][:5], 1):
                url_short = doc['source_url'][:60] + "..." if len(doc['source_url']) > 60 else doc['source_url']
                print(f"  {i}. {doc['citation_count']} citations ({doc['citation_density']}% density)")
                print(f"     {url_short}")

        processing_time = summary['extraction_metadata']['processing_time_minutes']
        print(f"\n⏱️  Processing time: {processing_time:.2f} minutes")
        print("="*80)

def main():
    """Main entry point for the citation extractor."""
    parser = argparse.ArgumentParser(description="Extract citations from chunked documents",
                                   formatter_class=argparse.RawDescriptionHelpFormatter,
                                   epilog="""
Processing Modes:
  pdf       Process PDF-extracted citations only (default, backward compatible)
  inline    Process inline markdown citations [text](url) only
  combined  Process both PDF and inline citations

Examples:
  # Basic PDF citation extraction (original behavior)
  python citation_extractor.py input.jsonl output_dir/

  # Inline citation processing with filtering
  python citation_extractor.py input.jsonl output_dir/ --mode inline --base-url https://example.com --filter-paragraphs

  # Combined processing with custom filtering thresholds
  python citation_extractor.py input.jsonl output_dir/ --mode combined --min-words 10 --min-chars 100
""")

    # Required arguments
    parser.add_argument('input_jsonl', help='Input JSONL file with chunked documents')
    parser.add_argument('output_dir', help='Output directory for citation files')

    # Processing mode
    parser.add_argument('--mode', choices=['pdf', 'inline', 'combined'], default='pdf',
                       help='Citation processing mode (default: pdf)')

    # Inline citation processing options
    parser.add_argument('--base-url', help='Base URL for resolving relative URLs in inline mode')
    parser.add_argument('--no-normalize-urls', action='store_true',
                       help='Disable URL normalization (remove tracking params, etc.)')

    # Filtering options
    parser.add_argument('--no-filter-paragraphs', action='store_true',
                       help='Disable paragraph filtering (process all chunk types)')
    parser.add_argument('--min-words', type=int, default=5,
                       help='Minimum word count for filtered chunks (default: 5)')
    parser.add_argument('--min-chars', type=int, default=50,
                       help='Minimum character count for filtered chunks (default: 50)')

    # Citation linking options
    parser.add_argument('--link', action='store_true',
                       help='Add bidirectional citation links (PDF mode only)')

    # Output options
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress summary output')

    args = parser.parse_args()

    # Validate input file
    if not Path(args.input_jsonl).exists():
        print(f"❌ Error: Input file not found: {args.input_jsonl}")
        return 1

    # Validate mode-specific arguments
    if args.mode in ['inline', 'combined'] and args.base_url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(args.base_url)
            if not parsed.scheme or not parsed.netloc:
                print(f"❌ Error: Invalid base URL: {args.base_url}")
                return 1
        except Exception:
            print(f"❌ Error: Invalid base URL: {args.base_url}")
            return 1

    try:
        # Validate link flag usage
        if args.link and args.mode != 'pdf':
            print(f"⚠️  Warning: --link flag only works with PDF mode. Ignoring for {args.mode} mode.")
            args.link = False

        # Initialize extractor with all parameters
        extractor = CitationExtractor(
            input_jsonl=args.input_jsonl,
            output_dir=args.output_dir,
            mode=args.mode,
            base_url=args.base_url,
            normalize_urls=not args.no_normalize_urls,
            filter_paragraphs=not args.no_filter_paragraphs,
            min_word_count=args.min_words,
            min_char_count=args.min_chars,
            link_citations=args.link
        )

        citations_file, content_file, summary_file = extractor.extract_citations()

        # Load and display summary
        if not args.quiet:
            with open(summary_file, 'r', encoding='utf-8') as f:
                summary = json.load(f)
            extractor._print_summary_report(summary)

        print(f"\n🎉 Citation extraction completed!")
        print(f"📁 Citations: {citations_file}")
        print(f"📁 Content: {content_file}")
        print(f"📁 Summary: {summary_file}")

        return 0

    except Exception as e:
        print(f"❌ Error during citation extraction: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
