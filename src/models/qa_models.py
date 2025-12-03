"""
Q&A Data Models

This module contains the data models for Q&A processing,
including embeddings, segmentation info, and analysis results.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np


@dataclass
class SegmentationInfo:
    """Information about text segmentation."""
    segment_count: int
    segments: List[str]
    segment_lengths: List[int]
    total_characters: int
    total_tokens_estimated: int

    @classmethod
    def from_segments(cls, segments: List[str]) -> 'SegmentationInfo':
        """Create SegmentationInfo from a list of segments."""
        segment_lengths = [len(seg) for seg in segments]
        total_chars = sum(segment_lengths)
        # Rough token estimation (1 token ≈ 4 characters for English)
        total_tokens = total_chars // 4

        return cls(
            segment_count=len(segments),
            segments=segments,
            segment_lengths=segment_lengths,
            total_characters=total_chars,
            total_tokens_estimated=total_tokens
        )


@dataclass
class QAPairEmbedding:
    """Represents a Q&A pair with its combined embedding."""
    pair_id: str
    question: str
    answer: str
    combined_text: str
    pair_embedding: List[float]

    # Original record metadata
    chunk_id: str
    source_url: str
    timestamp: str

    # Embedding metadata
    embedding_dimension: int
    segmentation_info: Optional[SegmentationInfo] = None

    # Additional metadata
    additional_metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format, preserving original schema."""
        result = {
            'question': self.question,
            'answer': self.answer,
            'chunk_id': self.chunk_id,
            'source_url': self.source_url,
            'timestamp': self.timestamp,
        }

        # Merge additional_metadata at top level to preserve original schema
        if self.additional_metadata:
            result.update(self.additional_metadata)

        # Add embedding-specific fields
        result.update({
            'pair_id': self.pair_id,
            'combined_text': self.combined_text,
            'pair_embedding': self.pair_embedding,
            'embedding_dimension': self.embedding_dimension
        })

        if self.segmentation_info:
            result['segmentation_info'] = self.segmentation_info.__dict__

        return result


@dataclass
class PairSimilarityAnalysis:
    """Analysis of Q&A pair similarities for filtering."""
    total_pairs: int
    similarity_matrix: List[List[float]]
    similarity_statistics: Dict[str, float]

    # Duplicate detection
    high_similarity_pairs: List[Dict[str, Any]]
    potential_duplicates: List[Dict[str, Any]]
    recommended_removals: List[str]  # pair_ids to remove

    # Filtering recommendations
    similarity_threshold: float
    pairs_to_keep: List[str]
    pairs_to_remove: List[str]

    # Analysis metadata
    analysis_timestamp: str
    similarity_metric: str = "cosine"

    def __post_init__(self):
        if not self.analysis_timestamp:
            self.analysis_timestamp = datetime.now().isoformat()
