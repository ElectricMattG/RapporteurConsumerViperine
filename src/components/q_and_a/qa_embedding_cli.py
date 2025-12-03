#!/usr/bin/env python3
"""
Q&A Pair Embedding Generator

This module generates embeddings for Q&A pairs with three modes:
1. question-only: Embed questions separately for question-based search
2. answer-only: Embed answers separately (useful for answer similarity)
3. combined: Embed question+answer together (captures full pair meaning)

The enriched Q&A pairs include all embedding modes for flexible search strategies.
"""

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
from sentence_transformers import SentenceTransformer


@dataclass
class QAEmbeddingEnrichment:
    """Container for Q&A pair with embeddings."""
    # Original Q&A data
    question: str
    answer: str
    context: str
    chunk_id: str
    source_url: str
    document_id: str
    section_hierarchy: List[str]

    # Embedding data
    question_embedding: Optional[List[float]] = None
    answer_embedding: Optional[List[float]] = None
    combined_embedding: Optional[List[float]] = None

    # Metadata
    embedding_model: str = ""
    embedding_dimension: int = 0
    enrichment_timestamp: str = ""

    # Original metadata
    chunk_type: Optional[str] = None
    processing_type: Optional[str] = None
    parent_section: Optional[str] = None
    word_count_question: Optional[int] = None
    word_count_answer: Optional[int] = None
    qa_index: Optional[int] = None
    qa_total_for_source: Optional[int] = None
    generation_timestamp: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, excluding None values."""
        result = {}
        for key, value in asdict(self).items():
            if value is not None:
                result[key] = value
        return result


class QAEmbeddingGenerator:
    """
    Generate embeddings for Q&A pairs with multiple modes.
    """

    def __init__(
        self,
        model_name: str = 'all-MiniLM-L6-v2',
        batch_size: int = 32,
        separator: str = " [SEP] "
    ):
        """
        Initialize embedding generator.

        Args:
            model_name: Sentence transformer model name
            batch_size: Batch size for encoding
            separator: Separator for combining question+answer
        """
        self.model_name = model_name
        self.batch_size = batch_size
        self.separator = separator

        print(f"🤖 Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.embedding_dimension = self.model.get_sentence_embedding_dimension()
        print(f"✓ Model loaded (dimension: {self.embedding_dimension})")

    def load_qa_pairs(self, qa_file: str) -> List[Dict[str, Any]]:
        """
        Load Q&A pairs from JSONL file.

        Args:
            qa_file: Path to Q&A pairs JSONL

        Returns:
            List of Q&A pair dictionaries
        """
        qa_pairs = []
        qa_path = Path(qa_file)

        if not qa_path.exists():
            raise FileNotFoundError(f"Q&A file not found: {qa_file}")

        print(f"📂 Loading Q&A pairs from: {qa_file}")

        with open(qa_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    qa_pair = json.loads(line.strip())
                    qa_pairs.append(qa_pair)
                except json.JSONDecodeError as e:
                    print(f"⚠️  Line {line_num}: JSON decode error: {e}")
                    continue

        print(f"✓ Loaded {len(qa_pairs)} Q&A pairs")
        return qa_pairs

    def create_combined_text(self, question: str, answer: str) -> str:
        """
        Create combined text from question and answer.

        Args:
            question: Question text
            answer: Answer text

        Returns:
            Combined text with separator
        """
        return f"{question.strip()}{self.separator}{answer.strip()}"

    def generate_embeddings(
        self,
        qa_pairs: List[Dict[str, Any]],
        mode: str = 'all',
        verbose: bool = True
    ) -> List[QAEmbeddingEnrichment]:
        """
        Generate embeddings for Q&A pairs.

        Args:
            qa_pairs: List of Q&A pair dictionaries
            mode: Embedding mode - 'question', 'answer', 'combined', or 'all'
            verbose: Print progress

        Returns:
            List of enriched Q&A pairs with embeddings
        """
        if verbose:
            print(f"\n🔢 Generating embeddings (mode: {mode})")
            print(f"   Processing {len(qa_pairs)} Q&A pairs...")

        # Prepare texts for each mode
        questions = []
        answers = []
        combined_texts = []

        for qa in qa_pairs:
            question = qa.get('question', '')
            answer = qa.get('answer', '')

            questions.append(question)
            answers.append(answer)
            combined_texts.append(self.create_combined_text(question, answer))

        # Generate embeddings based on mode
        question_embeddings = None
        answer_embeddings = None
        combined_embeddings = None

        if mode in ['question', 'all']:
            if verbose:
                print(f"   → Encoding {len(questions)} questions...")
            question_embeddings = self.model.encode(
                questions,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=verbose
            )
            if verbose:
                print(f"   ✓ Question embeddings: {question_embeddings.shape}")

        if mode in ['answer', 'all']:
            if verbose:
                print(f"   → Encoding {len(answers)} answers...")
            answer_embeddings = self.model.encode(
                answers,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=verbose
            )
            if verbose:
                print(f"   ✓ Answer embeddings: {answer_embeddings.shape}")

        if mode in ['combined', 'all']:
            if verbose:
                print(f"   → Encoding {len(combined_texts)} combined texts...")
            combined_embeddings = self.model.encode(
                combined_texts,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                show_progress_bar=verbose
            )
            if verbose:
                print(f"   ✓ Combined embeddings: {combined_embeddings.shape}")

        # Create enriched Q&A pairs
        enriched_pairs = []
        timestamp = datetime.now().isoformat()

        for i, qa in enumerate(qa_pairs):
            enriched = QAEmbeddingEnrichment(
                # Core Q&A data
                question=qa.get('question', ''),
                answer=qa.get('answer', ''),
                context=qa.get('context', ''),
                chunk_id=qa.get('chunk_id', ''),
                source_url=qa.get('source_url', ''),
                document_id=qa.get('document_id', ''),
                section_hierarchy=qa.get('section_hierarchy', []),

                # Embeddings
                question_embedding=question_embeddings[i].tolist() if question_embeddings is not None else None,
                answer_embedding=answer_embeddings[i].tolist() if answer_embeddings is not None else None,
                combined_embedding=combined_embeddings[i].tolist() if combined_embeddings is not None else None,

                # Embedding metadata
                embedding_model=self.model_name,
                embedding_dimension=self.embedding_dimension,
                enrichment_timestamp=timestamp,

                # Original metadata
                chunk_type=qa.get('chunk_type'),
                processing_type=qa.get('processing_type'),
                parent_section=qa.get('parent_section'),
                word_count_question=len(qa.get('question', '').split()),
                word_count_answer=len(qa.get('answer', '').split()),
                qa_index=qa.get('qa_index'),
                qa_total_for_source=qa.get('qa_total_for_source'),
                generation_timestamp=qa.get('generation_timestamp')
            )

            enriched_pairs.append(enriched)

        if verbose:
            print(f"✓ Created {len(enriched_pairs)} enriched Q&A pairs")

        return enriched_pairs

    def save_enriched_pairs(
        self,
        enriched_pairs: List[QAEmbeddingEnrichment],
        output_file: str,
        verbose: bool = True
    ):
        """
        Save enriched Q&A pairs to JSONL file.

        Args:
            enriched_pairs: List of enriched Q&A pairs
            output_file: Output JSONL file path
            verbose: Print progress
        """
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if verbose:
            print(f"\n💾 Saving enriched Q&A pairs to: {output_file}")

        with open(output_path, 'w', encoding='utf-8') as f:
            for enriched in enriched_pairs:
                json.dump(enriched.to_dict(), f, ensure_ascii=False)
                f.write('\n')

        if verbose:
            file_size = output_path.stat().st_size / (1024 * 1024)  # MB
            print(f"✓ Saved {len(enriched_pairs)} pairs ({file_size:.2f} MB)")

    def generate_summary(
        self,
        enriched_pairs: List[QAEmbeddingEnrichment],
        output_file: str,
        verbose: bool = True
    ):
        """
        Generate processing summary.

        Args:
            enriched_pairs: List of enriched Q&A pairs
            output_file: Output JSON file path
            verbose: Print summary
        """
        # Count embeddings
        has_question = sum(1 for p in enriched_pairs if p.question_embedding is not None)
        has_answer = sum(1 for p in enriched_pairs if p.answer_embedding is not None)
        has_combined = sum(1 for p in enriched_pairs if p.combined_embedding is not None)

        # Word count statistics
        word_counts_q = [p.word_count_question for p in enriched_pairs if p.word_count_question]
        word_counts_a = [p.word_count_answer for p in enriched_pairs if p.word_count_answer]

        summary = {
            'processing_metadata': {
                'total_qa_pairs': len(enriched_pairs),
                'embedding_model': self.model_name,
                'embedding_dimension': self.embedding_dimension,
                'enrichment_timestamp': datetime.now().isoformat()
            },
            'embedding_statistics': {
                'pairs_with_question_embedding': has_question,
                'pairs_with_answer_embedding': has_answer,
                'pairs_with_combined_embedding': has_combined
            },
            'content_statistics': {
                'question_word_count': {
                    'min': min(word_counts_q) if word_counts_q else 0,
                    'max': max(word_counts_q) if word_counts_q else 0,
                    'mean': sum(word_counts_q) / len(word_counts_q) if word_counts_q else 0
                },
                'answer_word_count': {
                    'min': min(word_counts_a) if word_counts_a else 0,
                    'max': max(word_counts_a) if word_counts_a else 0,
                    'mean': sum(word_counts_a) / len(word_counts_a) if word_counts_a else 0
                }
            },
            'document_coverage': {
                'unique_documents': len(set(p.document_id for p in enriched_pairs)),
                'unique_chunks': len(set(p.chunk_id for p in enriched_pairs)),
                'unique_sources': len(set(p.source_url for p in enriched_pairs))
            }
        }

        # Save summary
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        if verbose:
            print(f"\n📊 Processing Summary:")
            print(f"   Total Q&A pairs: {summary['processing_metadata']['total_qa_pairs']}")
            print(f"   Embedding model: {summary['processing_metadata']['embedding_model']}")
            print(f"   Embedding dimension: {summary['processing_metadata']['embedding_dimension']}")
            print(f"\n   Embedding coverage:")
            print(f"     Question embeddings: {has_question}")
            print(f"     Answer embeddings: {has_answer}")
            print(f"     Combined embeddings: {has_combined}")
            print(f"\n   Content statistics:")
            print(f"     Questions: {summary['content_statistics']['question_word_count']['mean']:.1f} words (avg)")
            print(f"     Answers: {summary['content_statistics']['answer_word_count']['mean']:.1f} words (avg)")
            print(f"\n   Coverage:")
            print(f"     Unique documents: {summary['document_coverage']['unique_documents']}")
            print(f"     Unique chunks: {summary['document_coverage']['unique_chunks']}")
            print(f"\n💾 Summary saved to: {output_file}")


def validate_enriched_pairs(enriched_file: str, sample_size: int = 5):
    """
    Validate enriched Q&A pairs file.

    Args:
        enriched_file: Path to enriched Q&A pairs JSONL
        sample_size: Number of samples to display
    """
    print(f"\n🔍 Validating: {enriched_file}")
    print("="*80)

    enriched_path = Path(enriched_file)
    if not enriched_path.exists():
        print(f"❌ File not found: {enriched_file}")
        return

    # Load and validate
    pairs = []
    with open(enriched_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                pair = json.loads(line.strip())
                pairs.append(pair)
            except json.JSONDecodeError as e:
                print(f"⚠️  Line {line_num}: JSON decode error: {e}")

    print(f"✓ Loaded {len(pairs)} enriched Q&A pairs")

    if not pairs:
        print("❌ No valid pairs found")
        return

    # Check first pair structure
    first_pair = pairs[0]

    print(f"\n📋 Structure validation:")
    print(f"   Required fields:")
    required_fields = ['question', 'answer', 'chunk_id', 'document_id']
    for field in required_fields:
        present = field in first_pair
        print(f"     {field}: {'✓' if present else '❌'}")

    print(f"\n   Embedding fields:")
    embedding_fields = ['question_embedding', 'answer_embedding', 'combined_embedding']
    for field in embedding_fields:
        if field in first_pair and first_pair[field]:
            dim = len(first_pair[field])
            print(f"     {field}: ✓ (dimension: {dim})")
        else:
            print(f"     {field}: ❌")

    # Display samples
    print(f"\n📄 Sample Q&A pairs (first {min(sample_size, len(pairs))}):")
    print("="*80)

    for i, pair in enumerate(pairs[:sample_size], 1):
        question = pair.get('question', '')[:100]
        answer = pair.get('answer', '')[:100]

        has_q_emb = '✓' if pair.get('question_embedding') else '❌'
        has_a_emb = '✓' if pair.get('answer_embedding') else '❌'
        has_c_emb = '✓' if pair.get('combined_embedding') else '❌'

        print(f"\n[{i}] Chunk: {pair.get('chunk_id', 'N/A')}")
        print(f"    Question: {question}...")
        print(f"    Answer: {answer}...")
        print(f"    Embeddings: Q={has_q_emb} A={has_a_emb} C={has_c_emb}")
        print(f"    Section: {' > '.join(pair.get('section_hierarchy', [])[-2:])}")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Generate embeddings for Q&A pairs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all embeddings (question, answer, combined)
  python qa_embedding_cli.py \\
    --qa-pairs qa_results/un_charter_qa_progress.jsonl \\
    --output qa_embeddings/ \\
    --mode all

  # Generate only question embeddings
  python qa_embedding_cli.py \\
    --qa-pairs qa_results/un_charter_qa_progress.jsonl \\
    --output qa_embeddings/ \\
    --mode question

  # Validate existing enriched file
  python qa_embedding_cli.py \\
    --validate qa_embeddings/enriched_qa_pairs.jsonl
        """
    )

    parser.add_argument('--qa-pairs', help='Path to Q&A pairs JSONL file')
    parser.add_argument('--output', help='Output directory for enriched pairs')
    parser.add_argument('--mode', choices=['question', 'answer', 'combined', 'all'],
                       default='all', help='Embedding mode (default: all)')
    parser.add_argument('--model', default='all-MiniLM-L6-v2',
                       help='Sentence transformer model (default: all-MiniLM-L6-v2)')
    parser.add_argument('--batch-size', type=int, default=32,
                       help='Batch size for encoding (default: 32)')
    parser.add_argument('--validate', help='Validate existing enriched Q&A pairs file')
    parser.add_argument('--sample-size', type=int, default=5,
                       help='Number of samples to display during validation (default: 5)')

    args = parser.parse_args()

    # Validation mode
    if args.validate:
        validate_enriched_pairs(args.validate, args.sample_size)
        return

    # Generation mode
    if not args.qa_pairs or not args.output:
        parser.error("--qa-pairs and --output are required for generation mode")

    print("="*80)
    print("Q&A PAIR EMBEDDING GENERATOR")
    print("="*80)

    # Initialize generator
    generator = QAEmbeddingGenerator(
        model_name=args.model,
        batch_size=args.batch_size
    )

    # Load Q&A pairs
    qa_pairs = generator.load_qa_pairs(args.qa_pairs)

    # Generate embeddings
    enriched_pairs = generator.generate_embeddings(
        qa_pairs=qa_pairs,
        mode=args.mode,
        verbose=True
    )

    # Prepare output paths
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    enriched_file = output_dir / 'enriched_qa_pairs.jsonl'
    summary_file = output_dir / 'embedding_summary.json'

    # Save results
    generator.save_enriched_pairs(enriched_pairs, str(enriched_file), verbose=True)
    generator.generate_summary(enriched_pairs, str(summary_file), verbose=True)

    print(f"\n{'='*80}")
    print("✅ EMBEDDING GENERATION COMPLETE")
    print(f"{'='*80}")
    print(f"\nOutput files:")
    print(f"  Enriched Q&A pairs: {enriched_file}")
    print(f"  Summary: {summary_file}")
    print(f"\nNext steps:")
    print(f"  1. Validate: python qa_embedding_cli.py --validate {enriched_file}")
    print(f"  2. Search: python qa_semantic_search.py --qa-pairs {enriched_file} --query '...'")


if __name__ == "__main__":
    main()
