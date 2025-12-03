#!/usr/bin/env python3
"""
Tree Search Data Models - Exact Original Structure

This module provides Pydantic v2 data models that exactly match the original dataclass
structures from the archive tree search implementation.

Models:
    - SearchNode: Exact copy of original dataclass structure
    - TreeSearchStats: Exact copy of original dataclass structure
    - ExtractedTextData: Structure for extracted_text.jsonl files
    - FailedUrlData: Structure for failed_urls.jsonl files
    - LinkedContentData: Structure for extracted_linked_text.jsonl files
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class SearchNode(BaseModel):
    """Represents a node in the search tree - EXACT copy of original dataclass structure."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    query: str = Field(..., description="Search query for this node")
    depth: int = Field(..., description="Depth level in the search tree")
    parent_query: Optional[str] = Field(None, description="Parent query that generated this node")
    urls: List[str] = Field(default_factory=list, description="URLs discovered for this query")
    text_content: str = Field(default="", description="Extracted text content for LLM processing")
    generated_queries: List[str] = Field(default_factory=list, description="LLM-generated semantic queries")
    extracted_links: Dict[str, List[Dict[str, str]]] = Field(default_factory=dict, description="Store extracted links by URL")
    growth: int = Field(default=0, description="Number of new URLs this node contributed")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat(), description="Node creation timestamp")


class TreeSearchStats(BaseModel):
    """Statistics for the tree search process - EXACT copy of original dataclass structure."""
    model_config = ConfigDict(
        validate_assignment=True
    )

    total_queries_executed: int = Field(default=0, description="Total number of queries executed")
    total_unique_urls: int = Field(default=0, description="Total unique URLs discovered")
    max_depth_reached: int = Field(default=0, description="Maximum depth level reached")
    total_text_extracted: int = Field(default=0, description="Total successful text extractions")
    llm_calls_made: int = Field(default=0, description="Total LLM API calls made")
    search_duration: float = Field(default=0.0, description="Total search duration in seconds")
    growth_per_query: List[Dict] = Field(default_factory=list, description="Growth metrics per query")

    # Extraction statistics
    total_markdown_attempts: int = Field(default=0, description="Total markdown conversion attempts")
    total_markdown_successes: int = Field(default=0, description="Successful markdown conversions")
    total_markdown_failures: int = Field(default=0, description="Failed markdown conversions")
    total_markdown_linked_attempts: int = Field(default=0, description="Markdown conversion attempts for linked content")
    total_markdown_linked_successes: int = Field(default=0, description="Successful markdown conversions for linked content")
    markdown_processing_time: float = Field(default=0.0, description="Total time spent on markdown processing (seconds)")
    markdown_content_size: int = Field(default=0, description="Total size of markdown content extracted (bytes)")
    markdown_success_rate: float = Field(default=0.0, description="Calculated markdown success rate percentage")

    def update_markdown_success_rate(self) -> None:
        """Calculate and update the markdown success rate percentage."""
        total_attempts = self.total_markdown_attempts + self.total_markdown_linked_attempts
        total_successes = self.total_markdown_successes + self.total_markdown_linked_successes

        if total_attempts > 0:
            self.markdown_success_rate = round((total_successes / total_attempts) * 100, 2)
        else:
            self.markdown_success_rate = 0.0


class ExtractedTextData(BaseModel):
    """Structure for extracted_text.jsonl - EXACT copy of original format."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    id: int = Field(..., description="Unique extraction ID")
    url: str = Field(..., description="Source URL")
    title: str = Field(..., description="Page title")
    content: str = Field(..., description="Extracted text content (up to 5000 chars)")
    chunk_sample: str = Field(..., description="Representative text chunk sample")
    full_length: int = Field(..., description="Full length of extracted content")
    extracted_links: List[Dict[str, str]] = Field(..., description="Links extracted from page")
    link_count: int = Field(..., description="Number of extracted links")
    extracted_images: List[Dict[str, str]] = Field(..., description="Images extracted from page")
    image_count: int = Field(..., description="Number of extracted images")
    timestamp: str = Field(..., description="Extraction timestamp")
    source_query: str = Field(..., description="Query that led to this extraction")


class FailedUrlData(BaseModel):
    """Structure for failed_urls.jsonl - EXACT copy of original format."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    url: str = Field(..., description="Failed URL")
    error_type: str = Field(..., description="Type of error (http_XXX or extraction_error)")
    details: str = Field(..., description="Error details and status information")
    timestamp: str = Field(..., description="Failure timestamp")
    source_query: str = Field(..., description="Query that led to this URL")


class LinkedContentData(BaseModel):
    """Structure for extracted_linked_text.jsonl - EXACT copy of original format."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    id: int = Field(..., description="Unique extraction ID")
    url: str = Field(..., description="Source URL")
    title: str = Field(..., description="Page title")
    content: str = Field(..., description="Extracted text content")
    chunk_sample: str = Field(..., description="Representative text chunk sample")
    full_length: int = Field(..., description="Full length of extracted content")
    extracted_links: List[Dict[str, str]] = Field(..., description="Links extracted from page")
    link_count: int = Field(..., description="Number of extracted links")
    extracted_images: List[Dict[str, str]] = Field(..., description="Images extracted from page")
    image_count: int = Field(..., description="Number of extracted images")
    timestamp: str = Field(..., description="Extraction timestamp")
    source_type: str = Field(..., description="Source type (linked_url)")
    page_type: str = Field(..., description="Page type (general or about)")


class LinkData(BaseModel):
    """Structure for individual link data - EXACT copy of original format."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    url: str = Field(..., description="Absolute URL")
    text: str = Field(..., description="Link text/title")
    title: str = Field(default="", description="Link title attribute")
    type: str = Field(..., description="Link type: internal/external/email/file")
    context: str = Field(default="", description="Surrounding text context")
    domain: str = Field(..., description="Link domain")


class ImageData(BaseModel):
    """Structure for individual image data - EXACT copy of original format."""
    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True
    )

    src: str = Field(..., description="Absolute image URL")
    alt: str = Field(default="", description="Alt text")
    title: str = Field(default="", description="Title attribute")
    width: str = Field(default="", description="Width attribute")
    height: str = Field(default="", description="Height attribute")
    context: str = Field(default="", description="Surrounding text context")
    domain: str = Field(..., description="Image domain")


# =============================================================================
# UTILITY FUNCTIONS - For creating models from original data
# =============================================================================

def create_search_node(
    query: str,
    depth: int,
    parent_query: Optional[str] = None,
    urls: Optional[List[str]] = None
) -> SearchNode:
    """Factory function to create a SearchNode matching original structure.

    Args:
        query: Search query string
        depth: Depth level in the search tree
        parent_query: Optional parent query
        urls: Optional list of URLs

    Returns:
        Validated SearchNode instance
    """
    return SearchNode(
        query=query,
        depth=depth,
        parent_query=parent_query,
        urls=urls or []
    )


def create_tree_search_stats() -> TreeSearchStats:
    """Factory function to create TreeSearchStats matching original structure.

    Returns:
        Validated TreeSearchStats instance
    """
    return TreeSearchStats()


def create_extracted_text_data(
    extraction_id: int,
    url: str,
    title: str,
    content: str,
    chunk_sample: str,
    extracted_links: List[Dict[str, str]],
    extracted_images: List[Dict[str, str]],
    source_query: str
) -> ExtractedTextData:
    """Factory function to create ExtractedTextData matching original structure.

    Args:
        extraction_id: Unique extraction ID
        url: Source URL
        title: Page title
        content: Extracted text content
        chunk_sample: Representative text chunk
        extracted_links: Links extracted from page
        extracted_images: Images extracted from page
        source_query: Query that led to this extraction

    Returns:
        Validated ExtractedTextData instance
    """
    return ExtractedTextData(
        id=extraction_id,
        url=url,
        title=title,
        content=content[:5000],  # Match original 5000 char limit
        chunk_sample=chunk_sample,
        full_length=len(content),
        extracted_links=extracted_links,
        link_count=len(extracted_links),
        extracted_images=extracted_images,
        image_count=len(extracted_images),
        timestamp=datetime.now().isoformat(),
        source_query=source_query
    )


def create_failed_url_data(
    url: str,
    error_type: str,
    details: str,
    source_query: str
) -> FailedUrlData:
    """Factory function to create FailedUrlData matching original structure.

    Args:
        url: Failed URL
        error_type: Type of error
        details: Error details
        source_query: Query that led to this URL

    Returns:
        Validated FailedUrlData instance
    """
    return FailedUrlData(
        url=url,
        error_type=error_type,
        details=details,
        timestamp=datetime.now().isoformat(),
        source_query=source_query
    )


def create_linked_content_data(
    extraction_id: int,
    url: str,
    title: str,
    content: str,
    chunk_sample: str,
    extracted_links: List[Dict[str, str]],
    extracted_images: List[Dict[str, str]],
    page_type: str = "general"
) -> LinkedContentData:
    """Factory function to create LinkedContentData matching original structure.

    Args:
        extraction_id: Unique extraction ID
        url: Source URL
        title: Page title
        content: Extracted text content
        chunk_sample: Representative text chunk
        extracted_links: Links extracted from page
        extracted_images: Images extracted from page
        page_type: Page type (general or about)

    Returns:
        Validated LinkedContentData instance
    """
    return LinkedContentData(
        id=extraction_id,
        url=url,
        title=title,
        content=content,
        chunk_sample=chunk_sample,
        full_length=len(content),
        extracted_links=extracted_links,
        link_count=len(extracted_links),
        extracted_images=extracted_images,
        image_count=len(extracted_images),
        timestamp=datetime.now().isoformat(),
        source_type="linked_url",
        page_type=page_type
    )
