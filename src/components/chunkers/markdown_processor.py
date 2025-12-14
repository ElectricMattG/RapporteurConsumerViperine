"""
Hierarchical Markdown Document Processor

Parses markdown into a structured tree, filters noise content,
and generates chunks with proper section hierarchy metadata.
"""

import hashlib
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class LinkType(Enum):
    """Classification of link types"""
    NAVIGATION = "navigation"
    CITATION = "citation"
    INTERNAL_REF = "internal_ref"
    UNKNOWN = "unknown"


@dataclass
class MarkdownProcessorConfig:
    """Configuration for markdown processing"""
    # Filtering
    remove_navigation_sections: bool = True
    remove_footer_sections: bool = True
    remove_consent_dialogs: bool = True
    header_blocklist: List[str] = field(default_factory=lambda: [
        "Share", "Recent", "Keep in touch", "Support", "Copyright",
        "Manage Consent", "Privacy Statement", "Manage consent",
        "Cookie", "Newsletter", "Subscribe"
    ])

    # Chunking
    min_chunk_words: int = 10
    max_chunk_words: int = 500
    merge_short_sections: bool = True
    min_section_words: int = 20  # Merge sections shorter than this

    # Content preservation
    keep_inline_links: bool = True
    track_image_positions: bool = True
    keep_list_items: bool = True

    # Metadata
    max_hierarchy_depth: int = 6


@dataclass
class LinkInfo:
    """Information about a link"""
    text: str
    url: str
    link_type: LinkType
    position: int


@dataclass
class ContentBlock:
    """Base class for content elements"""
    block_type: str  # "paragraph", "header", "list", "image"
    text: str
    start_pos: int
    end_pos: int

    def word_count(self) -> int:
        return len(self.text.split())


@dataclass
class ParagraphBlock(ContentBlock):
    """Paragraph with potential inline links"""
    inline_links: List[LinkInfo] = field(default_factory=list)

    def __post_init__(self):
        self.block_type = "paragraph"


@dataclass
class ImageBlock(ContentBlock):
    """Image placeholder"""
    alt_text: Optional[str] = None
    caption: Optional[str] = None

    def __post_init__(self):
        self.block_type = "image"


@dataclass
class HeaderNode:
    """Node in the header hierarchy tree"""
    level: int  # 1-6 for H1-H6
    text: str
    start_pos: int
    end_pos: int = -1  # Will be set when next header found
    parent: Optional['HeaderNode'] = None
    children: List['HeaderNode'] = field(default_factory=list)
    content_blocks: List[ContentBlock] = field(default_factory=list)
    is_noise: bool = False  # Marked as noise section

    def get_section_path(self) -> List[str]:
        """Get breadcrumb path from root to this node"""
        path = []
        node = self
        while node is not None:
            if node.text:  # Skip root
                path.insert(0, node.text)
            node = node.parent
        return path

    def get_all_text(self) -> str:
        """Get all text content in this section"""
        texts = []
        for block in self.content_blocks:
            if block.block_type != "image":
                texts.append(block.text)
        return "\n".join(texts)

    def total_word_count(self) -> int:
        """Count words in all content blocks"""
        return sum(block.word_count() for block in self.content_blocks if block.block_type != "image")


class HeaderParser:
    """Parse markdown headers and build hierarchy tree"""

    HEADER_PATTERN = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)

    def parse(self, markdown_text: str) -> HeaderNode:
        """
        Parse markdown and return root of header tree

        Args:
            markdown_text: Markdown document text

        Returns:
            Root HeaderNode with complete tree structure
        """
        root = HeaderNode(level=0, text="", start_pos=0)
        current_node = root
        last_pos = 0

        # Find all headers
        headers = []
        for match in self.HEADER_PATTERN.finditer(markdown_text):
            level = len(match.group(1))
            text = match.group(2).strip()
            start_pos = match.start()
            headers.append((level, text, start_pos))

        # Build tree
        for i, (level, text, start_pos) in enumerate(headers):
            # Set end position of previous section
            if current_node != root:
                current_node.end_pos = start_pos

            # Create new node
            node = HeaderNode(level=level, text=text, start_pos=start_pos)

            # Find parent (traverse up until we find lower level)
            parent = current_node
            while parent.level >= level and parent.parent is not None:
                parent = parent.parent

            node.parent = parent
            parent.children.append(node)
            current_node = node
            last_pos = start_pos

        # Set end position of last section
        if current_node != root:
            current_node.end_pos = len(markdown_text)

        # Extract content blocks for each section
        self._extract_content_blocks(root, markdown_text)

        return root

    def _extract_content_blocks(self, root: HeaderNode, markdown_text: str):
        """Extract paragraphs, lists, images for each header section"""

        def process_node(node: HeaderNode):
            if node.start_pos >= 0 and node.end_pos > node.start_pos:
                # Get section text (excluding the header line itself)
                section_start = node.start_pos
                section_end = node.end_pos

                # Skip the header line
                header_end = markdown_text.find('\n', section_start)
                if header_end == -1:
                    header_end = section_start
                content_start = header_end + 1

                # Get content up to first child or section end
                if node.children:
                    content_end = node.children[0].start_pos
                else:
                    content_end = section_end

                if content_start < content_end:
                    section_text = markdown_text[content_start:content_end]
                    node.content_blocks = self._parse_content_blocks(section_text, content_start)

            # Process children
            for child in node.children:
                process_node(child)

        process_node(root)

    def _parse_content_blocks(self, text: str, offset: int) -> List[ContentBlock]:
        """Parse text into content blocks (paragraphs, images, lists)"""
        blocks = []

        # Split into lines and group into paragraphs
        lines = text.split('\n')
        current_para = []
        current_start = offset

        for line in lines:
            line = line.strip()

            # Empty line ends paragraph
            if not line:
                if current_para:
                    para_text = '\n'.join(current_para)
                    blocks.append(ParagraphBlock(
                        block_type="paragraph",
                        text=para_text,
                        start_pos=current_start,
                        end_pos=current_start + len(para_text)
                    ))
                    current_para = []
                current_start = offset
                continue

            # Image marker
            if line == "<!-- image -->":
                if current_para:
                    para_text = '\n'.join(current_para)
                    blocks.append(ParagraphBlock(
                        block_type="paragraph",
                        text=para_text,
                        start_pos=current_start,
                        end_pos=current_start + len(para_text)
                    ))
                    current_para = []

                blocks.append(ImageBlock(
                    block_type="image",
                    text="",
                    start_pos=current_start,
                    end_pos=current_start + len(line)
                ))
                current_start = offset
                continue

            # Regular content line
            current_para.append(line)

        # Final paragraph
        if current_para:
            para_text = '\n'.join(current_para)
            blocks.append(ParagraphBlock(
                block_type="paragraph",
                text=para_text,
                start_pos=current_start,
                end_pos=current_start + len(para_text)
            ))

        return blocks


class ContentFilter:
    """Filter noise sections and classify content"""

    def __init__(self, config: MarkdownProcessorConfig):
        self.config = config

        # Compile patterns for efficiency
        self.noise_patterns = [
            re.compile(r'\bmanage\s+consent\b', re.IGNORECASE),
            re.compile(r'\bcookie\b', re.IGNORECASE),
            re.compile(r'\bprivacy\s+statement\b', re.IGNORECASE),
            re.compile(r'\bnewsletter\s+sign-?up\b', re.IGNORECASE),
            re.compile(r'\bfollow\s+us\b', re.IGNORECASE),
            re.compile(r'copyright\s+©', re.IGNORECASE),
        ]

    def filter_tree(self, root: HeaderNode):
        """Mark noise sections in the tree"""

        def mark_noise(node: HeaderNode):
            # Check if header is in blocklist
            if any(blocked.lower() in node.text.lower() for blocked in self.config.header_blocklist):
                node.is_noise = True

            # Check for noise patterns in content
            elif self._contains_noise_patterns(node):
                node.is_noise = True

            # Mark children
            for child in node.children:
                mark_noise(child)

        mark_noise(root)

    def _contains_noise_patterns(self, node: HeaderNode) -> bool:
        """Check if section contains noise patterns"""
        text = node.get_all_text()

        # Check against patterns
        for pattern in self.noise_patterns:
            if pattern.search(text):
                return True

        return False

    def should_keep_paragraph(self, text: str, word_count: int) -> bool:
        """Determine if paragraph should be kept"""
        # Too short
        if word_count < 3:
            return False

        # Noise patterns
        for pattern in self.noise_patterns:
            if pattern.search(text):
                return False

        return True


class HierarchicalChunkBuilder:
    """Build chunks from filtered header tree"""

    def __init__(self, config: MarkdownProcessorConfig):
        self.config = config

    def build_chunks(self, root: HeaderNode, document_metadata: dict) -> List[dict]:
        """
        Build chunks from header tree

        Args:
            root: Root of header tree
            document_metadata: Document-level metadata

        Returns:
            List of chunk dictionaries
        """
        chunks = []
        chunk_index = 0
        prev_chunk_id = None

        def process_node(node: HeaderNode, parent_sections: List[str]):
            nonlocal chunk_index, prev_chunk_id

            # Skip root and noise sections
            if node.level == 0:
                for child in node.children:
                    process_node(child, [])
                return

            if node.is_noise:
                return

            # Build section hierarchy
            section_hierarchy = parent_sections + [node.text]

            # Create chunk for header
            header_chunk = self._create_chunk(
                content=f"{'#' * node.level} {node.text}",
                chunk_type=f"header_{node.level}",
                start_pos=node.start_pos,
                end_pos=node.start_pos + len(node.text),
                chunk_index=chunk_index,
                section_hierarchy=section_hierarchy,
                parent_section=node.text,
                document_metadata=document_metadata,
                prev_chunk_id=prev_chunk_id
            )

            prev_chunk_id = header_chunk["chunk_metadata"]["chunk_id"]
            chunks.append(header_chunk)
            chunk_index += 1

            # Create chunks for content blocks
            for block in node.content_blocks:
                if block.block_type == "image":
                    continue  # Skip images

                if block.block_type == "paragraph":
                    # Filter paragraph
                    if not self._should_keep_block(block):
                        continue

                    content_chunk = self._create_chunk(
                        content=block.text,
                        chunk_type="paragraph",
                        start_pos=block.start_pos,
                        end_pos=block.end_pos,
                        chunk_index=chunk_index,
                        section_hierarchy=section_hierarchy,
                        parent_section=node.text,
                        document_metadata=document_metadata,
                        prev_chunk_id=prev_chunk_id
                    )

                    prev_chunk_id = content_chunk["chunk_metadata"]["chunk_id"]
                    chunks.append(content_chunk)
                    chunk_index += 1

            # Process children
            for child in node.children:
                process_node(child, section_hierarchy)

        process_node(root, [])

        # Link chunks (set next_chunk_id)
        for i in range(len(chunks) - 1):
            chunks[i]["chunk_metadata"]["next_chunk_id"] = chunks[i + 1]["chunk_metadata"]["chunk_id"]

        if chunks:
            chunks[-1]["chunk_metadata"]["next_chunk_id"] = None

        return chunks

    def _should_keep_block(self, block: ContentBlock) -> bool:
        """Determine if content block should be included"""
        word_count = block.word_count()

        # Too short
        if word_count < 3:
            return False

        # Check for standalone links (likely navigation)
        text = block.text.strip()
        if text.startswith('[') and text.endswith(')') and word_count < 5:
            return False

        return True

    def _create_chunk(
        self,
        content: str,
        chunk_type: str,
        start_pos: int,
        end_pos: int,
        chunk_index: int,
        section_hierarchy: List[str],
        parent_section: str,
        document_metadata: dict,
        prev_chunk_id: Optional[str]
    ) -> dict:
        """Create a chunk dictionary"""

        # Generate chunk ID
        chunk_id = self._generate_chunk_id(content, chunk_index)

        # Count metrics
        words = content.split()
        word_count = len(words)
        char_count = len(content)
        line_count = content.count('\n') + 1

        # Check for URLs
        contains_urls = 'http://' in content or 'https://' in content

        chunk = {
            "content": content,
            "document_metadata": document_metadata,
            "chunk_metadata": {
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "chunk_type": chunk_type,
                "start_position": start_pos,
                "end_position": end_pos,
                "word_count": word_count,
                "char_count": char_count,
                "line_count": line_count,
                "page_number": None,
                "section_hierarchy": section_hierarchy,
                "prev_chunk_id": prev_chunk_id,
                "next_chunk_id": None,  # Will be set later
                "parent_section": parent_section,
                "is_first_in_section": False,
                "is_last_in_section": False,
                "contains_urls": contains_urls,
                "contains_references": False,
                "table_rows": None,
                "table_cols": None,
                "image_alt_text": None,
                "list_item_count": None,
                "citation_number": None,
                "citation_type": None,
                "references_citations": [],
                "cited_by_chunks": []
            },
            "processing_timestamp": None  # Will be set by caller
        }

        return chunk

    def _generate_chunk_id(self, content: str, index: int) -> str:
        """Generate unique chunk ID"""
        hash_input = f"{content}_{index}".encode('utf-8')
        hash_digest = hashlib.md5(hash_input).hexdigest()[:12]
        return f"chunk_{hash_digest}"


class MarkdownDocumentProcessor:
    """Main orchestrator for markdown document processing"""

    def __init__(
        self,
        header_parser: Optional[HeaderParser] = None,
        content_filter: Optional[ContentFilter] = None,
        chunk_builder: Optional[HierarchicalChunkBuilder] = None,
        config: Optional[MarkdownProcessorConfig] = None
    ):
        self.config = config or MarkdownProcessorConfig()
        self.header_parser = header_parser or HeaderParser()
        self.content_filter = content_filter or ContentFilter(self.config)
        self.chunk_builder = chunk_builder or HierarchicalChunkBuilder(self.config)

    def process(self, markdown_text: str, document_metadata: dict) -> List[dict]:
        """
        Process markdown document into filtered chunks

        Args:
            markdown_text: Raw markdown text
            document_metadata: Document-level metadata

        Returns:
            List of chunk dictionaries with proper hierarchy
        """
        # 1. Parse headers and build tree
        root = self.header_parser.parse(markdown_text)

        # 2. Filter noise sections
        self.content_filter.filter_tree(root)

        # 3. Build chunks with metadata
        chunks = self.chunk_builder.build_chunks(root, document_metadata)

        # 4. Add processing timestamp
        import time
        timestamp = time.time()
        for chunk in chunks:
            chunk["processing_timestamp"] = timestamp

        return chunks
