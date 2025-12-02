"""
Q&A Pair Embedding Processor

This module focuses on Q&A pair analysis by:
1. Creating combined embeddings for each Q&A pair (concatenated question + answer)
2. Computing pair-to-pair similarities for duplicate detection
3. Providing distance-based filtering for data ingestion
4. Analyzing pair relationships rather than individual Q or A components

The goal is to identify redundant/duplicate Q&A pairs and filter them out
before ingestion into downstream systems.
"""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

# Handle imports with try/except for relative vs absolute
try:
    from ..embedding_engine import EmbeddingEngine
    from ..models.qa_models import (PairSimilarityAnalysis, QAPairEmbedding,
                                    SegmentationInfo)
    from ..utils.qa_utils import (compute_similarity_matrix,
                                  compute_similarity_statistics,
                                  find_top_similar_pairs)
except ImportError:
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
    from embedding_engine import EmbeddingEngine

    from models.qa_models import (PairSimilarityAnalysis, QAPairEmbedding,
                                  SegmentationInfo)
    from utils.qa_utils import (compute_similarity_matrix,
                                compute_similarity_statistics,
                                find_top_similar_pairs)


class QAPairProcessor:
    """
    Processor focused on Q&A pair analysis and filtering.
    """

    def __init__(
        self,
        model_name: str = 'all-mini',
        max_length: int = 512,
        segment_overlap: int = 50,
        batch_size: int = 32,
        pair_separator: str = " [SEP] "
    ):
        """
        Initialize Q&A pair processor.

        Args:
            model_name: Embedding model to use
            max_length: Maximum segment length for long texts
            segment_overlap: Overlap between segments
            batch_size: Batch size for processing
            pair_separator: Separator between question and answer in combined text
        """
        self.embedding_engine = EmbeddingEngine(
            model_name=model_name,
            max_length=max_length,
            segment_overlap=segment_overlap,
            batch_size=batch_size,
            normalize_embeddings=True
        )

        self.pair_separator = pair_separator
        self.model_name = model_name
        self.max_length = max_length

        # Get embedding dimension
        sample_embedding = self.embedding_engine.embed(["test"], use_segmentation=False)
        self.embedding_dimension = sample_embedding.shape[1]

    def create_combined_text(self, question: str, answer: str) -> str:
        """
        Create combined text from question and answer.

        Args:
            question: Question text
            answer: Answer text

        Returns:
            Combined text string
        """
        return f"{question.strip()}{self.pair_separator}{answer.strip()}"

    def process_qa_pairs(
        self,
        records: List[Dict[str, Any]],
        include_segmentation: bool = True
    ) -> List[QAPairEmbedding]:
        """
        Process Q&A records into pair embeddings.

        Args:
            records: List of Q&A records
            include_segmentation: Whether to include segmentation info

        Returns:
            List of Q&A pair embeddings
        """
        print(f"🔄 Processing {len(records)} Q&A pairs...")

        # Create combined texts
        combined_texts = []
        pair_embeddings = []

        for i, record in enumerate(records):
            question = record['question']
            answer = record['answer']
            combined_text = self.create_combined_text(question, answer)
            combined_texts.append(combined_text)

            # Create pair ID
            pair_id = f"pair_{i}_{record.get('chunk_id', i)}"

            # Process individual pair
            if include_segmentation:
                embedding, segments = self.embedding_engine.embed_with_segments(combined_text)
                segmentation_info = SegmentationInfo.from_segments(segments)
            else:
                embedding = self.embedding_engine.embed([combined_text], use_segmentation=True)[0]
                segmentation_info = None

            # Extract additional metadata
            additional_metadata = {}
            for key, value in record.items():
                if key not in ['question', 'answer', 'chunk_id', 'source_url', 'timestamp']:
                    additional_metadata[key] = value

            pair_embedding = QAPairEmbedding(
                pair_id=pair_id,
                question=question,
                answer=answer,
                combined_text=combined_text,
                pair_embedding=embedding.tolist(),
                chunk_id=str(record.get('chunk_id', i)),
                source_url=record.get('source_url', ''),
                timestamp=record.get('timestamp', datetime.now().isoformat()),
                embedding_dimension=self.embedding_dimension,
                segmentation_info=segmentation_info,
                additional_metadata=additional_metadata if additional_metadata else None
            )

            pair_embeddings.append(pair_embedding)

        print(f"✓ Created {len(pair_embeddings)} Q&A pair embeddings")
        return pair_embeddings

    def analyze_pair_similarities(
        self,
        pair_embeddings: List[QAPairEmbedding],
        similarity_threshold: float = 0.85,
        duplicate_threshold: float = 0.95
    ) -> PairSimilarityAnalysis:
        """
        Analyze similarities between Q&A pairs for filtering.

        Args:
            pair_embeddings: List of Q&A pair embeddings
            similarity_threshold: Threshold for considering pairs similar
            duplicate_threshold: Threshold for considering pairs duplicates

        Returns:
            Similarity analysis with filtering recommendations
        """
        print(f"🔍 Analyzing similarities between {len(pair_embeddings)} pairs...")

        # Extract embeddings matrix
        embeddings_matrix = np.array([pair.pair_embedding for pair in pair_embeddings])

        # Compute similarity matrix
        similarity_matrix = compute_similarity_matrix(embeddings_matrix, metric="cosine")

        # Compute statistics (excluding diagonal)
        mask = ~np.eye(similarity_matrix.shape[0], dtype=bool)
        off_diagonal_values = similarity_matrix[mask]

        similarity_stats = {
            'min': float(np.min(off_diagonal_values)),
            'max': float(np.max(off_diagonal_values)),
            'mean': float(np.mean(off_diagonal_values)),
            'std': float(np.std(off_diagonal_values)),
            'median': float(np.median(off_diagonal_values))
        }

        # Find high similarity pairs
        high_similarity_pairs = []
        potential_duplicates = []

        for i in range(len(pair_embeddings)):
            for j in range(i + 1, len(pair_embeddings)):
                similarity = similarity_matrix[i, j]

                if similarity >= similarity_threshold:
                    pair_info = {
                        'pair_id_1': pair_embeddings[i].pair_id,
                        'pair_id_2': pair_embeddings[j].pair_id,
                        'similarity_score': float(similarity),
                        'question_1': pair_embeddings[i].question,
                        'question_2': pair_embeddings[j].question,
                        'answer_1': pair_embeddings[i].answer,
                        'answer_2': pair_embeddings[j].answer,
                        'source_1': pair_embeddings[i].source_url,
                        'source_2': pair_embeddings[j].source_url
                    }

                    high_similarity_pairs.append(pair_info)

                    if similarity >= duplicate_threshold:
                        potential_duplicates.append(pair_info)

        # Sort by similarity score
        high_similarity_pairs.sort(key=lambda x: x['similarity_score'], reverse=True)
        potential_duplicates.sort(key=lambda x: x['similarity_score'], reverse=True)

        # Recommend removals (keep first occurrence, remove subsequent duplicates)
        recommended_removals = []
        pairs_to_remove = set()

        for duplicate in potential_duplicates:
            pair_id_1 = duplicate['pair_id_1']
            pair_id_2 = duplicate['pair_id_2']

            # Keep the first one encountered, remove the second
            if pair_id_1 not in pairs_to_remove:
                pairs_to_remove.add(pair_id_2)
                recommended_removals.append(pair_id_2)

        pairs_to_keep = [pair.pair_id for pair in pair_embeddings if pair.pair_id not in pairs_to_remove]

        print(f"✓ Found {len(high_similarity_pairs)} high similarity pairs (≥{similarity_threshold})")
        print(f"✓ Found {len(potential_duplicates)} potential duplicates (≥{duplicate_threshold})")
        print(f"✓ Recommended removing {len(recommended_removals)} duplicate pairs")
        print(f"✓ Keeping {len(pairs_to_keep)} unique pairs")

        return PairSimilarityAnalysis(
            total_pairs=len(pair_embeddings),
            similarity_matrix=similarity_matrix.tolist(),
            similarity_statistics=similarity_stats,
            high_similarity_pairs=high_similarity_pairs,
            potential_duplicates=potential_duplicates,
            recommended_removals=recommended_removals,
            similarity_threshold=similarity_threshold,
            pairs_to_keep=pairs_to_keep,
            pairs_to_remove=list(pairs_to_remove),
            analysis_timestamp=datetime.now().isoformat()
        )

    def filter_pairs(
        self,
        pair_embeddings: List[QAPairEmbedding],
        analysis: PairSimilarityAnalysis
    ) -> Tuple[List[QAPairEmbedding], List[QAPairEmbedding]]:
        """
        Filter Q&A pairs based on similarity analysis.

        Args:
            pair_embeddings: Original list of pair embeddings
            analysis: Similarity analysis with filtering recommendations

        Returns:
            Tuple of (kept_pairs, removed_pairs)
        """
        kept_pairs = []
        removed_pairs = []

        for pair in pair_embeddings:
            if pair.pair_id in analysis.pairs_to_keep:
                kept_pairs.append(pair)
            else:
                removed_pairs.append(pair)

        return kept_pairs, removed_pairs

    def process_and_filter(
        self,
        records: List[Dict[str, Any]],
        similarity_threshold: float = 0.85,
        duplicate_threshold: float = 0.95,
        include_segmentation: bool = False
    ) -> Tuple[List[QAPairEmbedding], PairSimilarityAnalysis]:
        """
        Complete pipeline: process pairs and filter duplicates.

        Args:
            records: Q&A records
            similarity_threshold: Threshold for high similarity
            duplicate_threshold: Threshold for duplicates
            include_segmentation: Whether to include segmentation info

        Returns:
            Tuple of (filtered_pairs, analysis)
        """
        # Process into pair embeddings
        pair_embeddings = self.process_qa_pairs(records, include_segmentation)

        # Analyze similarities
        analysis = self.analyze_pair_similarities(
            pair_embeddings,
            similarity_threshold,
            duplicate_threshold
        )

        # Filter pairs
        kept_pairs, removed_pairs = self.filter_pairs(pair_embeddings, analysis)

        print(f"\n📊 Filtering Summary:")
        print(f"  Original pairs: {len(pair_embeddings)}")
        print(f"  Pairs kept: {len(kept_pairs)}")
        print(f"  Pairs removed: {len(removed_pairs)}")
        print(f"  Deduplication rate: {len(removed_pairs)/len(pair_embeddings)*100:.1f}%")

        return kept_pairs, analysis


def save_pair_analysis(
    pair_embeddings: List[QAPairEmbedding],
    analysis: PairSimilarityAnalysis,
    output_file: str
):
    """Save pair analysis results to JSON file."""
    output_data = {
        'pair_embeddings': [pair.to_dict() for pair in pair_embeddings],
        'similarity_analysis': {
            'total_pairs': analysis.total_pairs,
            'similarity_statistics': analysis.similarity_statistics,
            'high_similarity_pairs': analysis.high_similarity_pairs,
            'potential_duplicates': analysis.potential_duplicates,
            'recommended_removals': analysis.recommended_removals,
            'similarity_threshold': analysis.similarity_threshold,
            'pairs_to_keep': analysis.pairs_to_keep,
            'pairs_to_remove': analysis.pairs_to_remove,
            'analysis_timestamp': analysis.analysis_timestamp,
            'similarity_metric': analysis.similarity_metric
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)


def save_pair_analysis(
    pair_embeddings: List[QAPairEmbedding],
    analysis: PairSimilarityAnalysis,
    output_file: str
):
    """Save pair analysis results to JSON file."""
    output_data = {
        'pair_embeddings': [pair.to_dict() for pair in pair_embeddings],
        'similarity_analysis': {
            'total_pairs': analysis.total_pairs,
            'similarity_statistics': analysis.similarity_statistics,
            'high_similarity_pairs': analysis.high_similarity_pairs,
            'potential_duplicates': analysis.potential_duplicates,
            'recommended_removals': analysis.recommended_removals,
            'similarity_threshold': analysis.similarity_threshold,
            'pairs_to_keep': analysis.pairs_to_keep,
            'pairs_to_remove': analysis.pairs_to_remove,
            'analysis_timestamp': analysis.analysis_timestamp,
            'similarity_metric': analysis.similarity_metric
        }
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Process and deduplicate Q&A pairs")
    parser.add_argument("input_file", help="Input JSONL file with Q&A pairs")
    parser.add_argument("output_dir", help="Output directory for results")
    parser.add_argument("--similarity-threshold", type=float, default=0.90, help="Similarity threshold")
    parser.add_argument("--duplicate-threshold", type=float, default=0.97, help="Duplicate threshold")
    parser.add_argument("--model", default="all-mini", help="Embedding model")

    args = parser.parse_args()

    print("🔧 Q&A Pair Processor for Duplicate Filtering")
    print("="*50)

    # Load Q&A pairs
    qa_pairs = []
    with open(args.input_file) as f:
        for line in f:
            if line.strip():
                qa_pairs.append(json.loads(line))

    print(f"Loaded {len(qa_pairs)} Q&A pairs")

    # Process
    processor = QAPairProcessor(model_name=args.model)
    filtered, analysis = processor.process_and_filter(
        qa_pairs,
        similarity_threshold=args.similarity_threshold,
        duplicate_threshold=args.duplicate_threshold,
        include_segmentation=False
    )

    # Save
    import os
    os.makedirs(args.output_dir, exist_ok=True)

    output_file = os.path.join(args.output_dir, 'qa_pairs_filtered.jsonl')
    with open(output_file, 'w') as f:
        for p in filtered:
            f.write(json.dumps(p.to_dict()) + '\n')

    analysis_file = os.path.join(args.output_dir, 'similarity_analysis.json')
    with open(analysis_file, 'w') as f:
        json.dump({
            'total_pairs': analysis.total_pairs,
            'similarity_statistics': analysis.similarity_statistics,
            'high_similarity_count': len(analysis.high_similarity_pairs),
            'duplicates_removed': len(analysis.recommended_removals),
            'pairs_kept': len(analysis.pairs_to_keep)
        }, f, indent=2)

    print(f"\n✅ Saved {len(filtered)} filtered pairs to {output_file}")
    print(f"✅ Saved analysis to {analysis_file}")
