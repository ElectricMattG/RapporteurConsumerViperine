"""
Image Extractor - Discovers and enriches images from documents.

Extracts images from markdown/HTML, gathers context, and optionally
uses Gemini Vision for description generation.
"""

import hashlib
import logging
import re
from dataclasses import dataclass, field
from io import BytesIO
from typing import Dict, List, Optional, Tuple

import requests
from PIL import Image

logger = logging.getLogger(__name__)


@dataclass
class ImageMetadata:
    """Metadata for a single image."""
    image_id: str
    url: str
    alt_text: str = ""
    description: str = ""
    description_source: str = "none"  # alt_text|caption|surrounding_text|gemini_vision

    # Position in document
    position_in_doc: int = 0
    markdown_position: int = 0

    # Image properties
    width: Optional[int] = None
    height: Optional[int] = None
    file_size: Optional[int] = None
    format: Optional[str] = None

    # Context
    text_before: str = ""
    text_after: str = ""
    parent_section: str = ""
    section_hierarchy: List[str] = field(default_factory=list)

    # Classification
    image_type: str = "inline"  # featured|inline|thumbnail|icon|chart|diagram
    is_decorative: bool = False

    # Optional local storage
    local_path: Optional[str] = None

    # Gemini analysis
    gemini_description: Optional[str] = None
    gemini_confidence: Optional[float] = None


@dataclass
class ImageExtractorConfig:
    """Configuration for image extraction."""
    # Size filtering (pixels)
    min_width: int = 200
    min_height: int = 200
    min_file_size: int = 5000  # bytes (5KB)

    # Context extraction
    context_words_before: int = 50
    context_words_after: int = 50

    # Gemini Vision
    use_gemini_vision: bool = False
    gemini_for_featured_only: bool = True
    gemini_batch_size: int = 5

    # Download options
    download_images: bool = False
    download_path: str = "cache/images"
    timeout_seconds: int = 10

    # Filtering
    skip_decorative: bool = True
    decorative_alt_patterns: List[str] = field(default_factory=lambda: [
        r'^(image|photo|picture|icon|logo|banner)$',
        r'^untitled',
        r'^\s*$'
    ])


class ImageExtractor:
    """Extract and enrich images from documents."""

    def __init__(self, config: Optional[ImageExtractorConfig] = None,
                 gemini_client=None):
        self.config = config or ImageExtractorConfig()
        self.gemini_client = gemini_client

        # Regex patterns
        self.markdown_image_pattern = re.compile(
            r'!\[([^\]]*)\]\(([^)]+)\)'
        )
        self.html_image_pattern = re.compile(
            r'<img[^>]+src=["\']([^"\']+)["\'][^>]*alt=["\']([^"\']*)["\'][^>]*>|'
            r'<img[^>]+alt=["\']([^"\']*)["\'][^>]+src=["\']([^"\']+)["\'][^>]*>',
            re.IGNORECASE
        )
        self.header_pattern = re.compile(r'^(#{1,6})\s+(.+)$', re.MULTILINE)

    def extract_images(self, text: str, document_metadata: Dict) -> List[ImageMetadata]:
        """Extract all images from text and enrich with metadata."""
        images = []

        # Extract from markdown
        markdown_images = self._extract_markdown_images(text)
        images.extend(markdown_images)

        # Extract from HTML (if present)
        html_images = self._extract_html_images(text)
        images.extend(html_images)

        # Remove duplicates by URL
        seen_urls = set()
        unique_images = []
        for img in images:
            if img.url not in seen_urls:
                seen_urls.add(img.url)
                unique_images.append(img)

        # Enrich each image
        enriched_images = []
        for idx, img in enumerate(unique_images):
            img.position_in_doc = idx

            # Get context
            self._extract_context(img, text, document_metadata)

            # Get image properties
            if self.config.download_images:
                self._get_image_properties(img)

            # Filter by size (only if we have dimensions)
            if not self._passes_size_filter(img):
                logger.debug(f"Filtered out small image: {img.url}")
                continue

            # Classify image type
            self._classify_image_type(img, idx, len(unique_images))

            # Check if decorative
            if self.config.skip_decorative and self._is_decorative(img):
                logger.debug(f"Filtered out decorative image: {img.url}")
                continue

            # Build description
            self._build_description(img, document_metadata)

            enriched_images.append(img)

        # Apply Gemini Vision if enabled
        if self.config.use_gemini_vision and self.gemini_client:
            self._apply_gemini_vision(enriched_images, document_metadata)

        return enriched_images

    def _extract_markdown_images(self, text: str) -> List[ImageMetadata]:
        """Extract images from markdown syntax."""
        images = []
        for match in self.markdown_image_pattern.finditer(text):
            alt_text = match.group(1)
            url = match.group(2)

            img = ImageMetadata(
                image_id=self._generate_image_id(url),
                url=url,
                alt_text=alt_text,
                markdown_position=match.start()
            )
            images.append(img)

        return images

    def _extract_html_images(self, text: str) -> List[ImageMetadata]:
        """Extract images from HTML img tags."""
        images = []
        for match in self.html_image_pattern.finditer(text):
            # Handle both src-alt and alt-src orders
            if match.group(1):  # src-alt order
                url = match.group(1)
                alt_text = match.group(2)
            else:  # alt-src order
                alt_text = match.group(3)
                url = match.group(4)

            img = ImageMetadata(
                image_id=self._generate_image_id(url),
                url=url,
                alt_text=alt_text,
                markdown_position=match.start()
            )
            images.append(img)

        return images

    def _extract_context(self, img: ImageMetadata, text: str,
                        document_metadata: Dict):
        """Extract surrounding text and section hierarchy."""
        pos = img.markdown_position

        # Get text before and after
        text_before_pos = max(0, pos - 500)  # ~100 words context
        text_after_pos = min(len(text), pos + 500)

        before_text = text[text_before_pos:pos]
        after_text = text[pos:text_after_pos]

        # Extract N words
        before_words = before_text.split()[-self.config.context_words_before:]
        after_words = after_text.split()[:self.config.context_words_after]

        img.text_before = ' '.join(before_words)
        img.text_after = ' '.join(after_words)

        # Find closest header
        headers_before = list(self.header_pattern.finditer(text[:pos]))
        if headers_before:
            last_header = headers_before[-1]
            img.parent_section = last_header.group(2).strip()

            # Build hierarchy
            hierarchy = [document_metadata.get('title', '')]
            # Add headers in order of nesting
            for header_match in headers_before:
                header_text = header_match.group(2).strip()
                hierarchy.append(header_text)

            img.section_hierarchy = hierarchy

    def _get_image_properties(self, img: ImageMetadata) -> bool:
        """Fetch image and extract properties."""
        try:
            response = requests.get(
                img.url,
                timeout=self.config.timeout_seconds,
                headers={'User-Agent': 'Mozilla/5.0'}
            )
            response.raise_for_status()

            img.file_size = len(response.content)

            # Get dimensions
            try:
                image = Image.open(BytesIO(response.content))
                img.width, img.height = image.size
                img.format = image.format.lower() if image.format else None
            except Exception as e:
                logger.warning(f"Failed to parse image {img.url}: {e}")
                return False

            return True

        except Exception as e:
            logger.warning(f"Failed to fetch image {img.url}: {e}")
            return False

    def _should_check_size(self, img: ImageMetadata) -> bool:
        """Determine if we should fetch image to check size."""
        # Always check if we don't have dimensions
        if img.width is None or img.height is None:
            return True
        return False

    def _passes_size_filter(self, img: ImageMetadata) -> bool:
        """Check if image meets minimum size requirements."""
        if img.width is None or img.height is None:
            # If we couldn't get dimensions, assume it passes
            return True

        if img.width < self.config.min_width or img.height < self.config.min_height:
            return False

        if img.file_size and img.file_size < self.config.min_file_size:
            return False

        return True

    def _classify_image_type(self, img: ImageMetadata, position: int, total: int):
        """Classify image type based on position and properties."""
        # First large image is likely featured
        if position == 0 and img.width and img.width > 600:
            img.image_type = "featured"
        # Very small images are likely icons
        elif img.width and img.height and (img.width < 100 or img.height < 100):
            img.image_type = "icon"
        # Square images might be thumbnails
        elif img.width and img.height and abs(img.width - img.height) < 50:
            if img.width < 300:
                img.image_type = "thumbnail"
        # Wide images might be charts
        elif img.width and img.height and img.width > img.height * 1.5:
            img.image_type = "chart"
        else:
            img.image_type = "inline"

    def _is_decorative(self, img: ImageMetadata) -> bool:
        """Check if image is decorative (should be filtered)."""
        if img.image_type in ["icon", "thumbnail"]:
            return True

        # Check alt text patterns
        for pattern in self.config.decorative_alt_patterns:
            if re.match(pattern, img.alt_text.lower()):
                return True

        return False

    def _build_description(self, img: ImageMetadata, document_metadata: Dict):
        """Build description from available sources."""
        # Priority: alt_text > surrounding context > section

        # Check if alt text is meaningful
        if img.alt_text and not self._is_generic_alt(img.alt_text):
            img.description = img.alt_text
            img.description_source = "alt_text"
        # Use surrounding text
        elif img.text_before or img.text_after:
            context = f"{img.text_before} [IMAGE] {img.text_after}".strip()
            img.description = context[:200]  # Limit length
            img.description_source = "surrounding_text"
        # Fall back to section
        elif img.parent_section:
            img.description = f"Image in section: {img.parent_section}"
            img.description_source = "section_header"

    def _is_generic_alt(self, alt_text: str) -> bool:
        """Check if alt text is generic/not useful."""
        alt_lower = alt_text.lower().strip()
        generic_terms = ['image', 'photo', 'picture', 'img', 'untitled', '']
        return alt_lower in generic_terms

    def _apply_gemini_vision(self, images: List[ImageMetadata],
                            document_metadata: Dict):
        """Use Gemini Vision to describe images."""
        if not self.gemini_client:
            return

        # Filter images for Gemini
        images_to_analyze = []
        for img in images:
            # Only analyze if no good description
            if self.config.gemini_for_featured_only and img.image_type != "featured":
                continue
            if img.description_source in ["alt_text", "caption"]:
                continue
            images_to_analyze.append(img)

        # Batch process
        for i in range(0, len(images_to_analyze), self.config.gemini_batch_size):
            batch = images_to_analyze[i:i + self.config.gemini_batch_size]

            for img in batch:
                description = self._get_gemini_description(img, document_metadata)
                if description:
                    img.gemini_description = description
                    img.description = description
                    img.description_source = "gemini_vision"
                    img.gemini_confidence = 0.95  # Placeholder

    def _get_gemini_description(self, img: ImageMetadata,
                                document_metadata: Dict) -> Optional[str]:
        """Get description from Gemini Vision."""
        try:
            title = document_metadata.get('title', 'Unknown')
            url = document_metadata.get('source_url', '')
            section = ' > '.join(img.section_hierarchy) if img.section_hierarchy else 'Unknown'
            context = f"{img.text_before} {img.text_after}".strip()

            prompt = f"""You are analyzing an image from a news article.

Article Title: {title}
Article URL: {url}
Section: {section}
Surrounding Text: {context[:300]}

Describe the image contents in 2-3 sentences focusing on:
- What is shown (people, objects, scenes)
- Relevant context (if chart/data: what it shows)
- Connection to the article topic

Description:"""

            # Call Gemini Vision API
            response = self.gemini_client.analyze_image(img.url, prompt)
            return response.strip() if response else None

        except Exception as e:
            logger.error(f"Gemini Vision failed for {img.url}: {e}")
            return None

    def _generate_image_id(self, url: str) -> str:
        """Generate unique image ID from URL."""
        return f"img_{hashlib.md5(url.encode()).hexdigest()[:12]}"

    def to_document_metadata(self, images: List[ImageMetadata]) -> Dict:
        """Convert images to document metadata format."""
        featured = next((img for img in images if img.image_type == "featured"), None)

        return {
            "images": [
                {
                    "image_id": img.image_id,
                    "url": img.url,
                    "alt_text": img.alt_text,
                    "description": img.description,
                    "description_source": img.description_source,
                    "type": img.image_type,
                    "position_in_doc": img.position_in_doc,
                    "width": img.width,
                    "height": img.height,
                    "file_size": img.file_size,
                    "format": img.format,
                    "parent_section": img.parent_section,
                    "section_hierarchy": img.section_hierarchy,
                    "local_path": img.local_path
                }
                for img in images
            ],
            "featured_image_id": featured.image_id if featured else None,
            "total_images": len(images)
        }
