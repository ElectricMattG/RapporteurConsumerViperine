"""
Simple markdown content filter - filters short chunks and lists.
"""

import re
from typing import Dict, List


class MarkdownFilter:
    """Simple filter for markdown chunks."""

    def __init__(self, min_words: int = 5):
        self.min_words = min_words
        self.list_pattern = re.compile(r'^\s*[-*+]\s+|^\s*\d+\.\s+')

    def filter_chunks(self, chunks: List[Dict]) -> List[Dict]:
        """Filter out short chunks and list items."""
        filtered = []

        for chunk in chunks:
            content = chunk.get("content", "")

            # Skip if content is a list item
            if self.list_pattern.match(content.strip()):
                continue

            # Skip if content has fewer than min_words
            word_count = len(content.split())
            if word_count < self.min_words:
                continue

            filtered.append(chunk)

        return filtered

    def extract_contextual_links(self, chunks: List[Dict]) -> List[Dict]:
        """Extract markdown links from paragraph chunks."""
        results = []

        for chunk in chunks:
            content = chunk.get("content", "")
            metadata = chunk.get("chunk_metadata", {})

            # Filter for paragraphs in main body
            is_main_content = metadata.get("chunk_type") == "paragraph"
            section_hierarchy = metadata.get("section_hierarchy", [])
            is_not_nav = "Quick Links" not in section_hierarchy

            # Extract Markdown links: [Text](URL)
            links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', content)

            if is_main_content and is_not_nav and links:
                results.append({
                    "chunk_id": metadata.get("chunk_id"),
                    "text_context": content,
                    "links": links,
                    "section": section_hierarchy[-1] if section_hierarchy else None
                })

        return results
