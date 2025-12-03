#!/usr/bin/env python3
"""
PDF Q&A Generator for Enriched Chunks

This module generates question-answer pairs from PDF chunks that have been:
1. Extracted from PDFs
2. Chunked into semantic units
3. Enriched with embeddings and hierarchy context

Strategy:
- Prioritizes legal clauses and articles (with sub-clauses)
- Intelligently groups lists by section boundaries
- Uses enriched_text for hierarchy context
- Dynamic Q&A count scaling based on content type and length
- Metadata-driven grouping using section_hierarchy
"""

import asyncio
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../..'))

# Import LLM client
from clients.llm_gcp_client import GeminiLLMClient
# Import Base class for configuration and logging
from config_handler import Base


class PDFQAGenerator(Base):
    """Generate Q&A pairs from enriched PDF chunks."""

    def __init__(self, output_dir: str = None, incremental_output: str = None):
        """Initialize PDF Q&A generator."""
        super().__init__()

        # Setup output directory
        if output_dir is None:
            output_dir = "./qa_results/pdf_qa"
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Incremental output file for monitoring progress
        self.incremental_output = Path(incremental_output) if incremental_output else None
        if self.incremental_output:
            self.incremental_output.parent.mkdir(parents=True, exist_ok=True)
            # Clear the file if it exists
            if self.incremental_output.exists():
                self.incremental_output.unlink()

        # LLM configuration
        self.llm_client = GeminiLLMClient()
        self.max_retries = self._app_config.get("qa_generation.max_retries")
        self.batch_size = self._app_config.get("qa_generation.batch_size")
        self.batch_delay = self._app_config.get("qa_generation.batch_delay")

        # Session timestamp
        timestamp_format = self._app_config.get("qa_generation.session_timestamp_format")
        self.session_timestamp = datetime.now().strftime(timestamp_format)

        # Statistics
        self.stats = {
            'total_chunks_loaded': 0,
            'chunks_processed': 0,
            'chunks_skipped': 0,
            'articles_processed': 0,
            'list_groups_processed': 0,
            'paragraphs_processed': 0,
            'tables_processed': 0,
            'total_qa_pairs': 0,
            'processing_errors': 0,
            'start_time': None,
            'end_time': None
        }

        self._logger.info("📚 PDF Q&A Generator initialized")
        self._logger.info(f"💾 Output directory: {self.output_dir}")

    # ==================== Input Text Selection ====================

    def get_qa_input_text(self, chunk: Dict[str, Any]) -> str:
        """
        Get the best text for Q&A generation.

        Priority:
        1. enriched_text (includes hierarchy context)
        2. content field
        3. Reconstruct from metadata
        """
        # Priority 1: enriched_text
        if 'embedding_enrichment' in chunk:
            enriched_text = chunk['embedding_enrichment'].get('enriched_text')
            if enriched_text:
                return enriched_text

        # Priority 2: content
        content = chunk.get('content', '')
        if content:
            # Add hierarchy if available
            hierarchy = chunk['chunk_metadata'].get('section_hierarchy', [])
            if hierarchy:
                hierarchy_str = ' > '.join(hierarchy)
                return f"{hierarchy_str}: {content}"
            return content

        # Priority 3: Reconstruct
        hierarchy = chunk['chunk_metadata'].get('section_hierarchy', [])
        hierarchy_str = ' > '.join(hierarchy) if hierarchy else 'Unknown'
        return f"{hierarchy_str}: [No content available]"

    # ==================== Chunk Type Detection ====================

    def get_chunk_type_category(self, chunk: Dict[str, Any]) -> str:
        """
        Categorize chunk types for grouping logic.

        Returns: 'list', 'paragraph', 'header', or 'other'
        """
        chunk_type = chunk['chunk_metadata']['chunk_type']

        if chunk_type in ['list_ordered', 'list_unordered']:
            return 'list'
        elif chunk_type == 'paragraph':
            return 'paragraph'
        elif chunk_type.startswith('header_'):
            return 'header'
        else:
            return 'other'

    def is_legal_clause(self, chunk: Dict[str, Any]) -> bool:
        """
        Detect if chunk is a legal clause/sub-clause.

        Patterns:
        - (a), (i), (1) - sub-clauses
        - 1., 2., Article 1. - numbered clauses
        - a., b. - lettered clauses
        """
        content = chunk.get('content', '').strip()

        # Sub-clause patterns
        if re.match(r'^\([a-z0-9ivxlcdm]+\)', content, re.IGNORECASE):
            return True

        # Numbered clauses
        if re.match(r'^(\d+\.|Article\s+\d+)', content, re.IGNORECASE):
            return True

        # Lettered clauses
        if re.match(r'^[a-z]\.\s+', content):
            return True

        return False

    def should_skip_chunk(self, chunk: Dict[str, Any]) -> bool:
        """
        Determine if chunk should be skipped for Q&A generation.

        Skip:
        - Headers (already in hierarchy)
        - Citations (separate processing)
        - Metadata markers
        - Very short chunks (unless they're clauses)
        """
        chunk_type = chunk['chunk_metadata']['chunk_type']
        content = chunk.get('content', '').strip()
        word_count = chunk['chunk_metadata'].get('word_count', 0)

        # Skip headers
        if chunk_type.startswith('header_'):
            return True

        # Skip citations
        if chunk_type == 'citation':
            return True

        # Skip metadata markers
        if re.match(r'^[\*†‡§¶]', content):
            return True

        # Skip very short non-clause content
        if word_count < 7 and not self.is_legal_clause(chunk):
            return True

        return False

    # ==================== Grouping & Organization ====================

    def identify_section_groups(self, chunks: List[Dict[str, Any]]) -> Dict[tuple, List[Dict[str, Any]]]:
        """
        Group chunks by section_hierarchy.

        Returns: Dict mapping section_hierarchy tuple to list of chunks
        """
        section_groups = defaultdict(list)

        for chunk in chunks:
            hierarchy = tuple(chunk['chunk_metadata'].get('section_hierarchy', []))
            section_groups[hierarchy].append(chunk)

        # Sort each group by chunk_index
        for hierarchy in section_groups:
            section_groups[hierarchy].sort(
                key=lambda c: c['chunk_metadata']['chunk_index']
            )

        return dict(section_groups)

    def is_article_with_subclauses(self, chunks: List[Dict[str, Any]]) -> bool:
        """
        Check if group contains an article intro + subclauses.

        Pattern: Paragraph followed by (a), (b), (c) subclauses
        """
        has_intro = any(c['chunk_metadata']['chunk_type'] == 'paragraph'
                       for c in chunks)
        has_subclauses = any(
            re.match(r'^\([a-z0-9]\)', c.get('content', '').strip(), re.IGNORECASE)
            for c in chunks
        )
        return has_intro and has_subclauses

    def has_lists(self, chunks: List[Dict[str, Any]]) -> bool:
        """Check if group contains list items."""
        return any(
            c['chunk_metadata']['chunk_type'] in ['list_ordered', 'list_unordered']
            for c in chunks
        )

    # ==================== Article Processing ====================

    def process_article_with_subclauses(self, article_chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process an Article with sub-clauses.

        Returns: List of qa_tasks
        """
        qa_tasks = []

        # Identify intro and subclauses
        intro_chunk = None
        subclause_chunks = []

        for chunk in article_chunks:
            content = chunk.get('content', '').strip()

            if re.match(r'^\([a-z0-9]\)', content, re.IGNORECASE):
                subclause_chunks.append(chunk)
            elif chunk['chunk_metadata']['chunk_type'] == 'paragraph':
                if intro_chunk is None:
                    intro_chunk = chunk

        # Task 1: Complete article (intro + all subclauses)
        if intro_chunk or subclause_chunks:
            complete_text = self._construct_complete_article_text(intro_chunk, subclause_chunks)
            qa_count = self._calculate_article_qa_count(intro_chunk, subclause_chunks)

            source_chunks = ([intro_chunk] if intro_chunk else []) + subclause_chunks

            qa_tasks.append({
                'input_text': complete_text,
                'qa_count': qa_count,
                'source_chunks': source_chunks,
                'type': 'article_complete',
                'section_hierarchy': article_chunks[0]['chunk_metadata'].get('section_hierarchy', [])
            })

            self._logger.debug(f"Created complete article task: {qa_count} Q&A pairs")

        # Task 2-N: Individual subclauses
        for subclause_chunk in subclause_chunks:
            subclause_text = self.get_qa_input_text(subclause_chunk)

            qa_tasks.append({
                'input_text': subclause_text,
                'qa_count': 2,
                'source_chunks': [subclause_chunk],
                'type': 'subclause_focused',
                'section_hierarchy': subclause_chunk['chunk_metadata'].get('section_hierarchy', [])
            })

        self.stats['articles_processed'] += 1
        return qa_tasks

    def _construct_complete_article_text(self, intro_chunk: Optional[Dict],
                                         subclause_chunks: List[Dict]) -> str:
        """Construct complete article text with hierarchy context."""
        parts = []

        # Get hierarchy
        first_chunk = intro_chunk if intro_chunk else subclause_chunks[0]
        hierarchy = first_chunk['chunk_metadata'].get('section_hierarchy', [])
        if hierarchy:
            parts.append(f"Article Context: {' > '.join(hierarchy)}")

        # Add intro
        if intro_chunk:
            intro_text = self.get_qa_input_text(intro_chunk)
            # Remove hierarchy prefix if present (already added above)
            if ': ' in intro_text and ' > ' in intro_text:
                intro_text = intro_text.split(': ', 1)[1]
            parts.append(f"Article Definition:\n{intro_text}")

        # Add subclauses
        if subclause_chunks:
            parts.append("\nSub-clauses:")
            for subclause in subclause_chunks:
                content = subclause.get('content', '').strip()
                parts.append(content)

        return '\n\n'.join(parts)

    def _calculate_article_qa_count(self, intro_chunk: Optional[Dict],
                                    subclause_chunks: List[Dict]) -> int:
        """Calculate Q&A count for complete article."""
        num_subclauses = len(subclause_chunks)

        if num_subclauses == 0:
            return 2  # Just intro
        elif num_subclauses <= 3:
            return 5  # Moderate complexity
        else:
            return min(8, 4 + num_subclauses)  # Scale with subclauses

    # ==================== List Processing ====================

    def process_list_sections(self, chunks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process lists using section boundaries.

        Returns: List of qa_tasks
        """
        qa_tasks = []

        i = 0
        while i < len(chunks):
            chunk = chunks[i]
            chunk_type = chunk['chunk_metadata']['chunk_type']

            # Only process list chunks
            if chunk_type not in ['list_ordered', 'list_unordered']:
                i += 1
                continue

            # Collect contiguous list items
            list_group = [chunk]
            hierarchy = tuple(chunk['chunk_metadata'].get('section_hierarchy', []))
            parent = chunk['chunk_metadata'].get('parent_section', '')

            j = i + 1
            while j < len(chunks):
                next_chunk = chunks[j]
                next_type = next_chunk['chunk_metadata']['chunk_type']
                next_hierarchy = tuple(next_chunk['chunk_metadata'].get('section_hierarchy', []))
                next_parent = next_chunk['chunk_metadata'].get('parent_section', '')

                # Stop if boundaries change
                if next_type not in ['list_ordered', 'list_unordered']:
                    break
                if next_hierarchy != hierarchy:
                    break
                if next_parent != parent:
                    break

                list_group.append(next_chunk)
                j += 1

            # Process the list group
            if len(list_group) > 1:
                # Multiple items - process as group
                combined_text = self._construct_list_text(list_group)
                qa_count = min(6, 2 + len(list_group) // 2)

                qa_tasks.append({
                    'input_text': combined_text,
                    'qa_count': qa_count,
                    'source_chunks': list_group,
                    'type': 'list_group',
                    'section_hierarchy': list(hierarchy)
                })

                self.stats['list_groups_processed'] += 1
            else:
                # Single item - process if substantial
                if chunk['chunk_metadata'].get('word_count', 0) >= 10:
                    item_text = self.get_qa_input_text(chunk)
                    qa_tasks.append({
                        'input_text': item_text,
                        'qa_count': 2,
                        'source_chunks': [chunk],
                        'type': 'list_item_single',
                        'section_hierarchy': list(hierarchy)
                    })

            i = j

        return qa_tasks

    def _construct_list_text(self, list_chunks: List[Dict[str, Any]]) -> str:
        """Construct text for list group with hierarchy context."""
        parts = []

        # Add hierarchy
        hierarchy = list_chunks[0]['chunk_metadata'].get('section_hierarchy', [])
        if hierarchy:
            parts.append(f"Section: {' > '.join(hierarchy)}")

        # Add list items
        parts.append("\nList items:")
        for chunk in list_chunks:
            content = chunk.get('content', '').strip()
            parts.append(content)

        return '\n'.join(parts)

    # ==================== Individual Chunk Processing ====================

    def should_process_individual(self, chunk: Dict[str, Any]) -> bool:
        """Determine if individual chunk should generate Q&A."""
        if self.should_skip_chunk(chunk):
            return False

        chunk_type = chunk['chunk_metadata']['chunk_type']
        word_count = chunk['chunk_metadata'].get('word_count', 0)

        # Process substantial paragraphs
        if chunk_type == 'paragraph' and word_count >= 20:
            return True

        # Process tables
        if chunk_type == 'table':
            return True

        # Process legal clauses even if isolated
        if self.is_legal_clause(chunk):
            return True

        return False

    def create_individual_task(self, chunk: Dict[str, Any]) -> Dict[str, Any]:
        """Create Q&A task for individual chunk."""
        chunk_type = chunk['chunk_metadata']['chunk_type']
        input_text = self.get_qa_input_text(chunk)

        # Calculate Q&A count based on type
        if chunk_type == 'paragraph':
            qa_count = self._calculate_paragraph_qa_count(chunk)
            self.stats['paragraphs_processed'] += 1
        elif chunk_type == 'table':
            qa_count = self._calculate_table_qa_count(chunk)
            self.stats['tables_processed'] += 1
        else:
            qa_count = 2

        return {
            'input_text': input_text,
            'qa_count': qa_count,
            'source_chunks': [chunk],
            'type': chunk_type,
            'section_hierarchy': chunk['chunk_metadata'].get('section_hierarchy', [])
        }

    def _calculate_paragraph_qa_count(self, chunk: Dict[str, Any]) -> int:
        """Calculate Q&A count for paragraphs."""
        word_count = chunk['chunk_metadata'].get('word_count', 0)

        if word_count < 50:
            return 1
        elif word_count < 150:
            return 2
        elif word_count < 300:
            return 3
        elif word_count < 500:
            return 5
        else:
            return 7

    def _calculate_table_qa_count(self, chunk: Dict[str, Any]) -> int:
        """Calculate Q&A count for tables."""
        rows = chunk['chunk_metadata'].get('table_rows', 5)

        if rows < 5:
            return 2
        elif rows < 15:
            return 3
        else:
            return 5

    # ==================== Main Processing Pipeline ====================

    def process_enriched_chunks(self, chunks_file: str) -> Dict[str, Any]:
        """
        Main entry point for processing enriched chunks.

        Args:
            chunks_file: Path to enriched_chunks.jsonl

        Returns:
            Processing results with Q&A pairs and statistics
        """
        self.stats['start_time'] = time.time()

        self._logger.info(f"📖 Loading enriched chunks from: {chunks_file}")
        chunks = self._load_chunks(chunks_file)
        self.stats['total_chunks_loaded'] = len(chunks)

        self._logger.info(f"📊 Loaded {len(chunks)} chunks")

        # Group by section
        section_groups = self.identify_section_groups(chunks)
        self._logger.info(f"📑 Identified {len(section_groups)} section groups")

        # Process each section group
        all_qa_tasks = []

        for hierarchy, group_chunks in section_groups.items():
            hierarchy_str = ' > '.join(hierarchy) if hierarchy else 'Root'
            self._logger.debug(f"Processing section: {hierarchy_str} ({len(group_chunks)} chunks)")

            # Route to appropriate processor
            if self.is_article_with_subclauses(group_chunks):
                tasks = self.process_article_with_subclauses(group_chunks)
                all_qa_tasks.extend(tasks)

            elif self.has_lists(group_chunks):
                tasks = self.process_list_sections(group_chunks)
                all_qa_tasks.extend(tasks)

            else:
                # Process individual chunks
                for chunk in group_chunks:
                    if self.should_process_individual(chunk):
                        task = self.create_individual_task(chunk)
                        all_qa_tasks.append(task)
                        self.stats['chunks_processed'] += 1
                    else:
                        self.stats['chunks_skipped'] += 1

        self._logger.info(f"✅ Created {len(all_qa_tasks)} Q&A generation tasks")
        self._logger.info(f"📊 Task breakdown:")
        self._logger.info(f"   Articles: {self.stats['articles_processed']}")
        self._logger.info(f"   List groups: {self.stats['list_groups_processed']}")
        self._logger.info(f"   Paragraphs: {self.stats['paragraphs_processed']}")
        self._logger.info(f"   Tables: {self.stats['tables_processed']}")
        self._logger.info(f"   Skipped: {self.stats['chunks_skipped']}")

        # Generate Q&A pairs
        qa_pairs = self._batch_process_qa_tasks(all_qa_tasks)

        self.stats['end_time'] = time.time()
        self.stats['duration_minutes'] = (self.stats['end_time'] - self.stats['start_time']) / 60

        # Save results
        results = self._save_results(qa_pairs, chunks_file)

        return results

    def _load_chunks(self, chunks_file: str) -> List[Dict[str, Any]]:
        """Load chunks from JSONL file."""
        chunks = []

        with open(chunks_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    chunk = json.loads(line)
                    chunks.append(chunk)

        return chunks

    # ==================== LLM Processing ====================

    def _batch_process_qa_tasks(self, qa_tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process Q&A tasks in batches (wraps async processing)."""
        all_qa_pairs = []

        total_batches = (len(qa_tasks) + self.batch_size - 1) // self.batch_size

        self._logger.info(f"🤖 Processing {len(qa_tasks)} tasks in {total_batches} batches")
        if self.incremental_output:
            self._logger.info(f"📝 Writing incremental output to: {self.incremental_output}")

        for i in range(0, len(qa_tasks), self.batch_size):
            batch = qa_tasks[i:i+self.batch_size]
            batch_num = i // self.batch_size + 1

            self._logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tasks)...")

            # Process batch (run async function in event loop)
            batch_results = asyncio.run(self._generate_qa_batch(batch))

            # Validate and enrich
            for task, qa_list in zip(batch, batch_results):
                enriched_qa = self._enrich_qa_pairs(qa_list, task)
                all_qa_pairs.extend(enriched_qa)

                # Write to incremental output immediately
                if self.incremental_output and enriched_qa:
                    self._write_incremental(enriched_qa)
                    self._logger.debug(f"   💾 Wrote {len(enriched_qa)} Q&A pairs to incremental file")

            # Log progress
            batch_qa_count = sum(len(qa_list) for qa_list in batch_results)
            self._logger.info(f"✓ Batch {batch_num}/{total_batches} complete. Generated {batch_qa_count} Q&A pairs. Total so far: {len(all_qa_pairs)}")

            # Show incremental file status
            if self.incremental_output and os.path.exists(self.incremental_output):
                file_size = os.path.getsize(self.incremental_output)
                self._logger.info(f"   📄 Incremental file size: {file_size:,} bytes")


            # Delay between batches
            if i + self.batch_size < len(qa_tasks):
                time.sleep(self.batch_delay)

        self.stats['total_qa_pairs'] = len(all_qa_pairs)
        self._logger.info(f"✅ Generated {len(all_qa_pairs)} Q&A pairs total")

        return all_qa_pairs

    async def _generate_qa_batch(self, batch: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Generate Q&A pairs for a batch of tasks using batched LLM calls (async)."""
        # Build all prompts for the batch
        prompts = []
        for task in batch:
            task_type = task['type']
            input_text = task['input_text']
            qa_count = task['qa_count']

            # Select appropriate prompt
            if task_type == 'article_complete':
                prompt = self._create_article_prompt(input_text, qa_count)
            elif task_type == 'subclause_focused':
                prompt = self._create_subclause_prompt(input_text)
            elif task_type in ['list_group', 'list_item_single']:
                prompt = self._create_list_prompt(input_text, qa_count)
            else:
                prompt = self._create_paragraph_prompt(input_text, qa_count)

            prompts.append(prompt)

        # Call LLM with all prompts in one batch (async)
        self._logger.debug(f"🚀 Sending batch of {len(prompts)} prompts to Gemini")
        responses = await self._call_llm_batch(prompts)

        # Parse all responses
        results = []
        for i, (task, response) in enumerate(zip(batch, responses)):
            try:
                qa_pairs = self._parse_qa_response(response)
                results.append(qa_pairs)

                if qa_pairs:
                    self._logger.info(f"   ✅ Task {i+1}/{len(batch)}: Generated {len(qa_pairs)} Q&A pairs")

                    # Log each Q&A pair for visibility
                    for idx, qa in enumerate(qa_pairs, 1):
                        q = qa.get('question', '...')[:100]
                        a = qa.get('answer', '...')[:100]
                        self._logger.info(f"      Q{idx}: {q}")
                        self._logger.info(f"      A{idx}: {a}")
                else:
                    self._logger.warning(f"   ⚠️  Task {i+1}/{len(batch)}: No Q&A pairs extracted")

            except Exception as e:
                self._logger.error(f"   ❌ Task {i+1}/{len(batch)}: Error parsing Q&A - {e}")
                self.stats['processing_errors'] += 1
                results.append([])

        return results

    async def _call_llm_batch(self, prompts: List[str], retry_count: int = 0) -> List[str]:
        """Call LLM with a batch of prompts using Gemini (async).

        Args:
            prompts: List of prompts to send
            retry_count: Current retry attempt

        Returns:
            List of response strings (one per prompt)
        """
        try:
            self._logger.debug(f"🔵 LLM Batch Call with Gemini (batch size: {len(prompts)})")

            # Process prompts sequentially with delays to avoid rate limiting
            responses = []
            for i, prompt in enumerate(prompts):
                try:
                    response = await self.llm_client.generate_content(
                        prompt=prompt,
                        temperature=0.7,
                        max_tokens=2000
                    )
                    responses.append(response)

                    # Add delay between requests (except after last one)
                    if i < len(prompts) - 1:
                        await asyncio.sleep(0.5)  # 500ms delay between individual calls

                except Exception as e:
                    self._logger.warning(f"⚠️  Prompt {i+1} failed: {e}")
                    responses.append(e)

            # Handle any exceptions in responses
            processed_responses = []
            for i, response in enumerate(responses):
                if isinstance(response, Exception):
                    self._logger.warning(f"⚠️  Prompt {i+1} failed: {response}")
                    processed_responses.append("")
                else:
                    processed_responses.append(response)

            self._logger.debug(f"   ✅ Received {len(processed_responses)} responses ({sum(1 for r in processed_responses if r)} successful)")

            return processed_responses

        except Exception as e:
            if retry_count < self.max_retries - 1:
                # Exponential backoff: 2, 4, 8 seconds
                delay = 2 ** (retry_count + 1)
                self._logger.warning(f"LLM batch call failed, retrying in {delay}s ({retry_count + 1}/{self.max_retries})...")
                await asyncio.sleep(delay)
                return await self._call_llm_batch(prompts, retry_count + 1)
            else:
                self._logger.error(f"❌ LLM batch call failed after {self.max_retries} retries: {e}")
                return [""] * len(prompts)  # Return empty responses for all prompts

    def _create_article_prompt(self, text: str, qa_count: int) -> str:
        """Create prompt for complete article."""
        return f"""Generate {qa_count} question-answer pairs about this legal article. If the chunks language is not english generate the q and a pair in english.

{text}

Questions should cover:
1. What the article defines or establishes
2. The complete structure (enumerate all sub-clauses)
3. The scope and conditions

IMPORTANT: Only use information directly stated in the text.
Do NOT infer case law or external applications.

Format: JSON array [{{"question": "...", "answer": "..."}}]
"""

    def _create_subclause_prompt(self, text: str) -> str:
        """Create prompt for individual subclause."""
        return f"""Generate 2 question-answer pairs about this specific sub-clause.

{text}

Questions:
1. What does this sub-clause specifically define or prohibit?
2. What are the key terms or conditions?

Only use the text provided.

Format: JSON array [{{"question": "...", "answer": "..."}}]
"""

    def _create_list_prompt(self, text: str, qa_count: int) -> str:
        """Create prompt for list group."""
        return f"""Generate {qa_count} question-answer pairs about this list.

{text}

Questions should:
1. Identify the purpose/category of these items
2. Describe specific items
3. Compare or identify patterns

Format: JSON array [{{"question": "...", "answer": "..."}}]
"""

    def _create_paragraph_prompt(self, text: str, qa_count: int) -> str:
        """Create prompt for paragraph."""
        return f"""Generate {qa_count} question-answer pairs about this content.

{text}

Focus on key facts, arguments, and statements in the text.

Format: JSON array [{{"question": "...", "answer": "..."}}]
"""

    # Legacy single-call method - no longer used (using batch processing instead)
    # def _call_llm(self, prompt: str, retry_count: int = 0) -> str:
    #     """Call LLM API with retry logic."""
    #     pass

    def _parse_qa_response(self, response: str) -> List[Dict[str, Any]]:
        """Parse LLM response to extract Q&A pairs.

        Handles multiple formats:
        1. JSON wrapped in markdown code blocks: ```json [...]```
        2. Raw JSON array: [...]
        3. Multiple JSON objects on separate lines
        """
        try:
            self._logger.debug("🟢 Parsing Q&A Response:")
            self._logger.debug(f"   Raw response length: {len(response)} chars")
            self._logger.debug(f"   First 500 chars: {response[:500]}")
            self._logger.debug(f"   Last 200 chars: {response[-200:]}")

            # Strategy 1: Look for JSON in markdown code blocks (most reliable)
            json_pattern = r'```(?:json)?\s*(.*?)\s*```'
            json_matches = re.findall(json_pattern, response, re.DOTALL | re.IGNORECASE)

            if json_matches:
                self._logger.debug(f"   Found {len(json_matches)} markdown code blocks")

                # Try each code block
                for i, json_str in enumerate(json_matches):
                    json_str = json_str.strip()
                    self._logger.debug(f"   Trying code block {i+1}: {json_str[:200]}...")

                    try:
                        parsed = json.loads(json_str)

                        # Check if it's a valid Q&A array
                        if isinstance(parsed, list):
                            # Validate that all items are dictionaries with question/answer keys
                            valid_qa = []
                            for item in parsed:
                                if isinstance(item, dict) and 'question' in item and 'answer' in item:
                                    valid_qa.append(item)
                                else:
                                    self._logger.warning(f"   ⚠️  Skipping invalid Q&A item: {item}")

                            if valid_qa:
                                self._logger.debug(f"   ✅ Successfully parsed {len(valid_qa)} Q&A pairs from code block {i+1}")
                                return valid_qa
                            else:
                                self._logger.debug(f"   ⚠️  Code block {i+1} has no valid Q&A pairs, skipping")
                        else:
                            self._logger.debug(f"   ⚠️  Code block {i+1} is not a list, skipping")

                    except json.JSONDecodeError as e:
                        self._logger.debug(f"   ❌ Code block {i+1} JSON error: {e}")
                        continue

            # Strategy 2: Look for raw JSON array (fallback)
            self._logger.debug("   No valid code blocks found, trying raw JSON array search")
            json_match = re.search(r'\[.*?\]', response, re.DOTALL)

            if json_match:
                json_str = json_match.group(0)
                self._logger.debug(f"   Found JSON array: {json_str[:300]}...")

                try:
                    parsed = json.loads(json_str)

                    # Validate that it's a list of valid Q&A dictionaries
                    if isinstance(parsed, list):
                        valid_qa = []
                        for item in parsed:
                            if isinstance(item, dict) and 'question' in item and 'answer' in item:
                                valid_qa.append(item)
                            else:
                                self._logger.warning(f"   ⚠️  Skipping invalid Q&A item in raw JSON: {item}")

                        if valid_qa:
                            self._logger.debug(f"   ✅ Successfully parsed {len(valid_qa)} Q&A pairs from raw JSON")
                            return valid_qa
                        else:
                            self._logger.warning("   ⚠️  Raw JSON array has no valid Q&A pairs")

                except json.JSONDecodeError as e:
                    self._logger.error(f"   ❌ Raw JSON parsing error: {e}")
                    self._logger.error(f"   Error at position: {e.pos}")
                    self._logger.error(f"   Attempted to parse: {json_str[:500]}...")

            # No valid JSON found
            self._logger.warning("⚠️  No valid JSON found in response")
            self._logger.debug(f"   Full response: {response}")
            return []

        except Exception as e:
            self._logger.error(f"❌ Error parsing Q&A response: {e}")
            self._logger.error(f"   Exception type: {type(e).__name__}")
            return []

    def _enrich_qa_pairs(self, qa_pairs: List[Dict[str, Any]],
                        task: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enrich Q&A pairs with metadata and aggregated context."""
        enriched = []

        source_chunks = task['source_chunks']
        primary_chunk = source_chunks[0]

        # Get the aggregated context that was used for Q&A generation
        aggregated_context = task.get('input_text', '')

        for i, qa in enumerate(qa_pairs, 1):
            # Validate that qa is a dictionary
            if not isinstance(qa, dict):
                self._logger.warning(f"   ⚠️  Skipping invalid Q&A pair (not a dict): {qa}")
                continue

            # Ensure required fields exist
            if 'question' not in qa or 'answer' not in qa:
                self._logger.warning(f"   ⚠️  Skipping Q&A pair missing question/answer: {qa}")
                continue

            # Preserve all document_metadata from source chunk
            doc_metadata = primary_chunk.get('document_metadata', {})

            enriched_qa = {
                'question': qa.get('question', ''),
                'answer': qa.get('answer', ''),
                'context': aggregated_context,  # Add the full context used for generation
                'chunk_id': primary_chunk['chunk_metadata']['chunk_id'],
                'related_chunk_ids': [c['chunk_metadata']['chunk_id'] for c in source_chunks],
                'source_url': doc_metadata.get('source_url', ''),
                'document_id': doc_metadata.get('document_id', ''),
                'chunk_type': task['type'],
                'processing_type': task['type'],
                'section_hierarchy': task['section_hierarchy'],
                'parent_section': primary_chunk['chunk_metadata'].get('parent_section', ''),
                'word_count_source': sum(c['chunk_metadata'].get('word_count', 0) for c in source_chunks),
                'word_count_context': len(aggregated_context.split()),
                'qa_index': i,
                'qa_total_for_source': len(qa_pairs),
                'used_enriched_text': 'embedding_enrichment' in primary_chunk,
                'generation_timestamp': datetime.now().isoformat()
            }

            # Add document metadata fields only if present
            if doc_metadata.get('title'):
                enriched_qa['title'] = doc_metadata['title']
            if doc_metadata.get('author'):
                enriched_qa['author'] = doc_metadata['author']
            if doc_metadata.get('creation_date'):
                enriched_qa['creation_date'] = doc_metadata['creation_date']
            if doc_metadata.get('content_type'):
                enriched_qa['content_type'] = doc_metadata['content_type']
            if doc_metadata.get('source_type'):
                enriched_qa['source_type'] = doc_metadata['source_type']
            if doc_metadata.get('document_word_count'):
                enriched_qa['document_word_count'] = doc_metadata['document_word_count']
            if doc_metadata.get('metadata_id') is not None:
                enriched_qa['metadata_id'] = doc_metadata['metadata_id']

            enriched.append(enriched_qa)

        return enriched

    # ==================== Incremental Output ====================

    def _write_incremental(self, qa_pairs: List[Dict[str, Any]]):
        """Write Q&A pairs incrementally to JSONL file."""
        if not self.incremental_output:
            return

        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.incremental_output), exist_ok=True)

            with open(self.incremental_output, 'a', encoding='utf-8') as f:
                for qa in qa_pairs:
                    f.write(json.dumps(qa, ensure_ascii=False) + '\n')

            self._logger.debug(f"   ✅ Wrote {len(qa_pairs)} Q&A pairs to {self.incremental_output}")

        except Exception as e:
            self._logger.error(f"   ❌ Error writing incremental output: {e}")
            self._logger.error(f"      Output path: {self.incremental_output}")

    # ==================== Output & Results ====================

    def _save_results(self, qa_pairs: List[Dict[str, Any]],
                     chunks_file: str) -> Dict[str, Any]:
        """Save Q&A pairs and statistics."""
        # Generate output filenames
        chunks_path = Path(chunks_file)
        base_name = chunks_path.stem

        qa_file = self.output_dir / f"{base_name}_qa_pairs_{self.session_timestamp}.jsonl"
        stats_file = self.output_dir / f"{base_name}_qa_stats_{self.session_timestamp}.json"

        # Save Q&A pairs
        self._logger.info(f"💾 Saving {len(qa_pairs)} Q&A pairs to: {qa_file}")
        with open(qa_file, 'w', encoding='utf-8') as f:
            for qa in qa_pairs:
                f.write(json.dumps(qa, ensure_ascii=False) + '\n')

        # Save statistics
        self._logger.info(f"📊 Saving statistics to: {stats_file}")
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2, ensure_ascii=False)

        # Print summary
        self._print_summary(qa_file, stats_file)

        return {
            'qa_pairs': qa_pairs,
            'stats': self.stats,
            'qa_file': str(qa_file),
            'stats_file': str(stats_file)
        }

    def _print_summary(self, qa_file: Path, stats_file: Path):
        """Print processing summary."""
        print("\n" + "="*80)
        print("🎉 PDF Q&A GENERATION COMPLETE!")
        print("="*80)
        print(f"\n📊 Processing Statistics:")
        print(f"   Total chunks loaded: {self.stats['total_chunks_loaded']}")
        print(f"   Chunks processed: {self.stats['chunks_processed']}")
        print(f"   Chunks skipped: {self.stats['chunks_skipped']}")
        print(f"\n📚 Content Breakdown:")
        print(f"   Articles: {self.stats['articles_processed']}")
        print(f"   List groups: {self.stats['list_groups_processed']}")
        print(f"   Paragraphs: {self.stats['paragraphs_processed']}")
        print(f"   Tables: {self.stats['tables_processed']}")
        print(f"\n✨ Results:")
        print(f"   Total Q&A pairs: {self.stats['total_qa_pairs']}")
        print(f"   Processing errors: {self.stats['processing_errors']}")
        print(f"   Duration: {self.stats['duration_minutes']:.1f} minutes")
        print(f"\n📁 Output Files:")
        print(f"   Q&A pairs: {qa_file}")
        print(f"   Statistics: {stats_file}")
        print("="*80 + "\n")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Generate Q&A pairs from enriched PDF chunks')
    parser.add_argument('chunks_file', help='Path to enriched_chunks.jsonl')
    parser.add_argument('--output-dir', default=None, help='Output directory for Q&A results')
    parser.add_argument('--incremental-output', default=None,
                       help='Path for incremental JSONL output (for monitoring progress)')

    args = parser.parse_args()

    # Auto-set incremental output if not specified
    if args.incremental_output is None:
        chunks_path = Path(args.chunks_file)
        incremental_file = chunks_path.parent / f"{chunks_path.stem}_qa_pairs_incremental.jsonl"
        args.incremental_output = str(incremental_file)

    # Initialize generator
    generator = PDFQAGenerator(
        output_dir=args.output_dir,
        incremental_output=args.incremental_output
    )

    # Process chunks
    results = generator.process_enriched_chunks(args.chunks_file)

    print(f"\n✅ Generated {len(results['qa_pairs'])} Q&A pairs")
    if args.incremental_output:
        print(f"📝 Incremental output: {args.incremental_output}")


if __name__ == "__main__":
    main()
