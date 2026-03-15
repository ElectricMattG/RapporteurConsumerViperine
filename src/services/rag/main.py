#!/usr/bin/env python3
"""
FastAPI service for querying summary index and generating LLM responses.

Features:
- Loads index into memory on startup
- Search endpoint: Query index and return matching documents
- Query LLM endpoint: Query index + generate LLM response with grounding
- Health and readiness checks
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional
from urllib.parse import unquote

import numpy as np
from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.clients.embedding_client import EmbeddingServiceClient
from src.clients.llm_gcp_client import GeminiLLMClient
from src.prompts.q_a import build_qa_refinement_prompt
from src.prompts.summaries import build_summary_prompt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
app = FastAPI(
    title="Summary Query Service",
    description="Query summary index with vector similarity and optional LLM grounding",
    version="1.0.0"
)

class ServiceState:
    """Service state container."""
    def __init__(self):
        self.kafka_producer: Optional[Producer] = None
        self.kafka_bootstrap_servers: Optional[str] = None
        self.index_loaded = False
        self.summaries: List[Dict] = []
        self.index_path: Optional[str] = None
        self.qa_items: List[Dict] = []
        self.qa_index_path: Optional[str] = None
        self.qa_index_loaded = False
        self.embedding_client: Optional[EmbeddingServiceClient] = None
        self.llm_client: Optional[GeminiLLMClient] = None
        self.startup_time: Optional[datetime] = None
        # Enrichment indices: keyed by normalised URL → list of matching records
        self.linked_enrichments: Dict[str, List[Dict]] = {}
        self.unlinked_enrichments: Dict[str, List[Dict]] = {}
        self.linked_index_path: Optional[str] = None
        self.unlinked_index_path: Optional[str] = None
        self.enrichment_retriever = None  # EnrichmentRetriever, instantiated at startup

state = ServiceState()


# ============================================================================
# Pydantic Models
# ============================================================================

class SearchRequest(BaseModel):
    """Request model for search endpoint."""
    query: str = Field(..., description="Natural language search query")
    top_k: int = Field(default=10, description="Number of top results to return", ge=1, le=100)
    start_date: Optional[str] = Field(None, description="Filter start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Filter end date (YYYY-MM-DD)")
    post_filter_top_k: Optional[int] = Field(None, description="Post-filter top-k by date")
    enrich: bool = Field(default=False, description="Include BBOA media enrichment in results")
    enrich_types: List[str] = Field(default=["all"], description="Enrichment type filter: events, executive_orders, media, risk, all")
    get_linked: bool = Field(default=False, description="Include event-linked records and expand to all articles for matched events")

    @field_validator("enrich_types")
    @classmethod
    def validate_enrich_types(cls, v: List[str]) -> List[str]:
        valid = {"events", "executive_orders", "media", "risk", "all"}
        invalid = [t for t in v if t not in valid]
        if invalid:
            raise ValueError(
                f"Unknown enrich_types: {invalid!r}. "
                f"Valid values: {sorted(valid)}"
            )
        return v

class QueryLLMRequest(SearchRequest):
    """Request model for query_llm endpoint (extends SearchRequest)."""
    mode: Literal['ask', 'search'] = Field(default='ask', description="Query mode: 'ask' uses index only, 'search' uses web search grounding")
    temperature: float = Field(default=0.5, description="LLM temperature", ge=0.0, le=2.0)
    max_tokens: int = Field(default=24000, description="Maximum tokens for LLM response", ge=100, le=32768)
    use_qa: bool = Field(default=False, description="Enable QA grounding in addition to summaries")
    qa_top_k: int = Field(default=5, description="Top-K QA pairs to retrieve", ge=1, le=150)

class DocumentResult(BaseModel):
    """Single document result with metadata."""
    document_id: str
    source_url: str
    source_url_display: Optional[str] = None  # Decoded URL for display
    publication_date: Optional[str]
    similarity_score: float
    summary_text: str
    doc_metadata: Dict

class SearchResponse(BaseModel):
    """Response model for search endpoint."""
    request_id: str
    session_id: str
    query: str
    index_queried: str
    top_k: int
    date_filters: Dict[str, Optional[str]]
    retrieved_docs: List[DocumentResult]
    total_retrieved: int
    execution_time_ms: float
    enrichment_results: Optional[List[Dict]] = None

class QueryLLMResponse(SearchResponse):
    """Response model for query_llm endpoint (extends SearchResponse)."""
    mode: str
    llm_response: str
    llm_model: str
    llm_params: Dict[str, float]
    prompt: str
    retrieved_qa: Optional[List[Dict]] = None
    citation_map: Optional[Dict[str, Dict]] = None  # Map doc_id -> {author, date, card_type, card_index}

class SearchModeResponse(QueryLLMResponse):
    """Response model for search mode with grounding metadata."""
    grounding_metadata: Optional[Dict] = None


class GroundingPreviewRequest(BaseModel):
    """Request model for grounding preview (no LLM)."""
    query: str = Field(..., description="Natural language search query")
    top_k: int = Field(default=10, description="Top summaries to retrieve", ge=1, le=100)
    start_date: Optional[str] = Field(None, description="Filter start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Filter end date (YYYY-MM-DD)")
    post_filter_top_k: Optional[int] = Field(None, description="Post-filter top-k by date")
    use_qa: bool = Field(default=False, description="Enable QA grounding in addition to summaries")
    qa_top_k: int = Field(default=5, description="Top-K QA pairs to retrieve", ge=1, le=50)
    enrich: bool = Field(default=False, description="Include BBOA media enrichment in preview")
    enrich_types: List[str] = Field(default=["all"], description="Enrichment type filter: events, executive_orders, media, risk, all")
    get_linked: bool = Field(default=False, description="Include event-linked records and expand to all articles for matched events")

    @field_validator("enrich_types")
    @classmethod
    def validate_enrich_types(cls, v: List[str]) -> List[str]:
        valid = {"events", "executive_orders", "media", "risk", "all"}
        invalid = [t for t in v if t not in valid]
        if invalid:
            raise ValueError(
                f"Unknown enrich_types: {invalid!r}. "
                f"Valid values: {sorted(valid)}"
            )
        return v


class GroundingPreviewResponse(BaseModel):
    """Response model for grounding preview (no LLM)."""
    request_id: str
    session_id: str
    query: str
    index_queried: str
    retrieved_docs: List[DocumentResult]
    retrieved_qa: Optional[List[Dict]] = None
    total_retrieved: int
    execution_time_ms: float
    enrichment_results: Optional[List[Dict]] = None
    enrichment_preview: Optional[str] = None


# ============================================================================
# Utility Functions (from query_summaries.py)
# ============================================================================

def load_summaries_index(index_path: str) -> List[Dict]:
    """Load summaries from JSONL index file."""
    summaries = []
    with open(index_path, 'r') as f:
        for line in f:
            if line.strip():
                summaries.append(json.loads(line))
    return summaries


def _extract_qa_fields(qa_obj: Dict) -> Optional[Dict]:
    """Normalize QA record into question/answer/context with metadata and embedding."""
    import re

    question = qa_obj.get('qa_metadata', {}).get('question') or qa_obj.get('question')
    answer = qa_obj.get('qa_metadata', {}).get('answer') or qa_obj.get('answer')
    context = qa_obj.get('context') or qa_obj.get('qa_metadata', {}).get('context')

    content_block = qa_obj.get('content') or qa_obj.get('embedding_enrichment', {}).get('enriched_text')
    if not context and content_block:
        pattern = re.compile(r"QUESTION:\s*(.*?)\n+ANSWER:\s*(.*?)\n+SOURCE CONTEXT:\s*(.*)", re.IGNORECASE | re.DOTALL)
        match = pattern.search(content_block)
        if match:
            extracted_q, extracted_a, extracted_ctx = match.groups()
            question = question or extracted_q.strip()
            answer = answer or extracted_a.strip()
            context = extracted_ctx.strip()

    doc_metadata = qa_obj.get('document_metadata') or qa_obj.get('doc_metadata') or {}
    source_url = doc_metadata.get('source_url') or qa_obj.get('source_url')
    if source_url and not doc_metadata.get('source_url'):
        doc_metadata['source_url'] = source_url

    embedding = qa_obj.get('embedding_enrichment', {}).get('embedding_vector')

    if not (question and answer and context and doc_metadata):
        return None

    return {
        'question': question.strip(),
        'answer': answer.strip(),
        'context': context.strip(),
        'source_url': source_url or 'N/A',
        'doc_metadata': doc_metadata,
        'embedding': embedding
    }


def load_qa_index(index_path: str) -> List[Dict]:
    """Load QA pairs from JSONL index file and normalize fields."""
    qa_items = []
    skipped_malformed = 0
    skipped_incomplete = 0
    with open(index_path, 'r') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as e:
                skipped_malformed += 1
                logger.warning(f"Skipping malformed QA line: {e}")
                continue

            normalized = _extract_qa_fields(raw)
            if normalized:
                qa_items.append(normalized)
            else:
                skipped_incomplete += 1

    if skipped_malformed:
        logger.warning(f"QA index load: skipped {skipped_malformed} malformed lines")
    if skipped_incomplete:
        logger.warning(f"QA index load: skipped {skipped_incomplete} incomplete lines (missing question/answer/context/metadata)")

    return qa_items


def load_enrichment_index(index_path: str) -> Dict[str, List[Dict]]:
    """
    Load an enrichment JSONL file (linked or unlinked) into a URL-keyed lookup map.

    Each record is stored under its normalised ``link`` value.  Multiple records
    that share the same normalised URL (e.g. same article appearing in different
    time windows) are collected into a list so no data is silently dropped.

    Returns an empty dict and logs a warning on any IO or parse error so the
    caller can continue with graceful degradation.
    """
    lookup: Dict[str, List[Dict]] = {}
    if not index_path:
        return lookup

    path = Path(index_path)
    if not path.exists():
        logger.warning(f"⚠️  Enrichment index not found: {index_path}; skipping")
        return lookup

    malformed = 0
    loaded = 0
    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    malformed += 1
                    logger.debug(f"Skipping malformed enrichment line: {e}")
                    continue

                raw_url = record.get('link', '')
                key = normalise_url(raw_url)
                if not key:
                    continue

                lookup.setdefault(key, []).append(record)
                loaded += 1

    except OSError as e:
        logger.warning(f"⚠️  Could not read enrichment index {index_path}: {e}; skipping")
        return {}

    if malformed:
        logger.warning(f"Enrichment index {path.name}: skipped {malformed} malformed lines")

    logger.info(f"✅ Loaded enrichment index {path.name}: {loaded} records → {len(lookup)} unique URLs")
    return lookup


def decode_url_for_display(url: str) -> str:
    """
    Decode percent-encoded URLs for human-readable display.
    Handles Arabic/Hebrew characters and other international characters.

    Args:
        url: The percent-encoded URL string

    Returns:
        Human-readable decoded URL string
    """
    try:
        return unquote(url)
    except Exception as e:
        logger.warning(f"Failed to decode URL: {url}, error: {e}")
        return url  # Return original if decoding fails


def normalise_url(url: str) -> str:
    """
    Produce a stable foreign-key string from a URL for enrichment index lookups.

    Strips scheme (http/https), lowercases the host, removes trailing slash,
    and drops fragment/query to tolerate minor URL variations between indices.
    """
    if not url:
        return ""
    # Remove scheme
    normalised = re.sub(r'^https?://', '', url.strip().lower())
    # Remove fragment
    normalised = normalised.split('#')[0]
    # Remove query string
    normalised = normalised.split('?')[0]
    # Strip trailing slash
    normalised = normalised.rstrip('/')
    return normalised


def _clean_author_name(name: str) -> str:
    """Clean author name: truncate long names, title-case ALL CAPS, preserve acronyms."""
    if not name or not name.strip():
        return 'Unknown'
    # Decode URL-encoded characters (e.g. %D8%A7 → Arabic)
    author = unquote(name.strip()).rstrip(',')
    words = author.split()
    # Long names (>6 words) — likely includes scraped junk
    if len(words) > 6:
        has_initial = (len(words[0]) == 2 and words[0].endswith('.')) or \
                      (len(words) > 1 and len(words[1]) == 2 and words[1].endswith('.'))
        author = ' '.join(words[:3]) if has_initial else ' '.join(words[:2])
        words = author.split()
    # Multi-word ALL CAPS → Title Case (but single-word caps kept for acronyms)
    if len(words) > 1:
        all_caps = all(w.replace('.', '').isupper() for w in words if w.replace('.', '').isalpha())
        if all_caps:
            author = ' '.join(w.title() for w in words)
    return author


def _format_citation_date(pub_date: Optional[str]) -> str:
    """Parse a date string into 'Month Year' format."""
    if not pub_date or str(pub_date).lower() in ('null', 'none', 'nan', ''):
        return 'N.D.'
    pub_date = str(pub_date).strip()
    try:
        date_obj = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
        return date_obj.strftime('%B %Y')
    except Exception:
        pass
    # Handle year-only dates like "2026" or "2024"
    if pub_date.isdigit() and len(pub_date) == 4:
        return pub_date
    # Handle partial dates like "2024-05"
    try:
        parts = pub_date.split('-')
        if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
            year = parts[0]
            month_num = int(parts[1])
            month_name = datetime(2000, month_num, 1).strftime('%B')
            return f'{month_name} {year}'
    except Exception:
        pass
    return pub_date


def build_citation_map(retrieved_docs: List, retrieved_qa: Optional[List] = None) -> Dict[str, Dict]:
    """
    Build a citation map for converting (doc_id, qa-N) placeholders to (Author, Date) format.

    Keys:
        - Summaries: keyed by document_id  (e.g. "req_20260108_025143_005")
        - QA items:  keyed by "doc_id__qa-N" composite  (e.g. "doc_2432eb5092b5__qa-3")
          where N is 1-indexed matching the grounding text numbering.

    Returns:
        { key: { author, date, card_type, card_index } }
    """
    logger.info(f"🗺️ build_citation_map called with {len(retrieved_docs)} docs, {len(retrieved_qa) if retrieved_qa else 0} QA items")
    citation_map = {}

    # Process summary documents
    logger.info("📄 Processing summary documents for citation map...")
    for index, doc in enumerate(retrieved_docs):
        doc_id = doc.document_id if hasattr(doc, 'document_id') else doc.get('document_id')
        doc_metadata = doc.doc_metadata if hasattr(doc, 'doc_metadata') else doc.get('doc_metadata', {})

        logger.info(f"   Summary {index}: doc_id='{doc_id}'")

        authors_list = doc_metadata.get('authors', [])
        if isinstance(authors_list, list) and authors_list:
            raw_author = authors_list[0]
        else:
            raw_author = doc_metadata.get('author') or doc_metadata.get('publisher') or 'Unknown'
        author = _clean_author_name(raw_author).strip()
        formatted_date = _format_citation_date(
            doc_metadata.get('publication_date') or doc_metadata.get('creation_date')
        ).strip()

        # Get source URL for PDF hyperlinks
        source_url = ''
        if hasattr(doc, 'source_url'):
            source_url = doc.source_url or ''
        elif isinstance(doc, dict):
            source_url = doc.get('source_url', '')

        citation_info = {
            "author": author,
            "date": formatted_date,
            "card_type": "sum",
            "card_index": index,
            "url": source_url
        }

        # Add with both doc_id key and doc_id__summary key for consistent resolution
        citation_map[doc_id] = citation_info
        citation_map[f"{doc_id}__summary"] = citation_info
        logger.info(f"      ✅ Added citation_map['{doc_id}'] and ['{doc_id}__summary'] = {author}, {formatted_date}")

    # Process QA items — use composite key "doc_id__qa-N" (1-indexed)
    if retrieved_qa:
        logger.info("❓ Processing QA items for citation map...")
        for index, qa in enumerate(retrieved_qa):
            qa_metadata = qa.get('doc_metadata', {})
            doc_id = qa_metadata.get('document_id', qa.get('document_id', 'unknown'))

            logger.info(f"   QA {index+1}: doc_id='{doc_id}'")

            author = _clean_author_name(
                qa_metadata.get('author') or qa_metadata.get('publisher') or 'Unknown'
            ).strip()
            formatted_date = _format_citation_date(
                qa_metadata.get('publication_date') or qa_metadata.get('creation_date')
            ).strip()

            qa_num = index + 1  # 1-indexed to match grounding text numbering
            composite_key = f"{doc_id}__qa-{qa_num}"

            # Get source URL for PDF hyperlinks
            qa_source_url = qa.get('source_url') or qa_metadata.get('source_url', '')

            citation_map[composite_key] = {
                "author": author,
                "date": formatted_date,
                "card_type": "qa",
                "card_index": index,
                "url": qa_source_url
            }
            logger.info(f"      ✅ Added citation_map['{composite_key}'] = {author}, {formatted_date}")

            # Fallback: also key by plain doc_id if no summary entry exists for this doc
            if doc_id not in citation_map:
                citation_map[doc_id] = {
                    "author": author,
                    "date": formatted_date,
                    "card_type": "qa",
                    "card_index": index,
                    "url": qa_source_url
                }
                logger.info(f"      ➕ Added QA fallback citation_map['{doc_id}'] (no summary entry)")

    logger.info(f"✅ Citation map built with {len(citation_map)} total keys")
    logger.info(f"   Sample keys: {list(citation_map.keys())[:10]}")
    return citation_map


def dot_product_similarity(query_embedding: List[float], doc_embedding: List[float]) -> float:
    """Calculate dot product similarity between query and document embeddings."""
    result = np.dot(query_embedding, doc_embedding)
    return float(result) if isinstance(result, np.ndarray) else result

def filter_by_date(summaries: List[Dict], start_date: Optional[str], end_date: Optional[str]) -> List[Dict]:
    """Filter summaries by date range using doc_metadata.publication_date."""
    if not start_date and not end_date:
        return summaries

    filtered = []
    for summary in summaries:
        doc_metadata = summary.get('doc_metadata', {})
        pub_date_str = doc_metadata.get('publication_date')

        if not pub_date_str:
            continue

        try:
            doc_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
            start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
            end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None

            if (not start or doc_date >= start) and (not end or doc_date <= end):
                filtered.append(summary)
        except (ValueError, AttributeError):
            continue

    return filtered


async def search_index(
    query: str,
    top_k: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    post_filter_top_k: Optional[int] = None
) -> List[Dict]:
    """
    Search the index using vector similarity.

    Returns list of dicts with 'summary' and 'similarity' keys.
    """
    summaries = state.summaries.copy()

    # Pre-filter by date if not using post-filter
    if (start_date or end_date) and not post_filter_top_k:
        summaries = filter_by_date(summaries, start_date, end_date)
        logger.info(f"Pre-filtered to {len(summaries)} summaries by date")

    # Generate query embedding
    logger.info(f"Generating embedding for query: '{query}'")
    query_embedding = await state.embedding_client.embed(query)

    # Flatten if batch (2D array)
    if isinstance(query_embedding, np.ndarray) and query_embedding.ndim > 1:
        query_embedding = query_embedding[0]

    # Calculate similarities
    results = []
    for summary in summaries:
        doc_embedding = summary.get('summary_embedding')
        if doc_embedding is None:
            continue

        similarity = dot_product_similarity(query_embedding, doc_embedding)
        results.append({
            'summary': summary,
            'similarity': similarity
        })

    # Sort by similarity and get top k
    results.sort(key=lambda x: x['similarity'], reverse=True)
    top_results = results[:top_k]

    # Apply post-filtering if requested
    if post_filter_top_k and (start_date or end_date):
        logger.info(f"Post-filtering top {top_k} results by date...")
        filtered_results = []
        for result in top_results:
            summary = result['summary']
            doc_metadata = summary.get('doc_metadata', {})
            pub_date_str = doc_metadata.get('publication_date')

            if pub_date_str:
                try:
                    doc_date = datetime.strptime(pub_date_str, '%Y-%m-%d').date()
                    start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
                    end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None

                    if (not start or doc_date >= start) and (not end or doc_date <= end):
                        filtered_results.append(result)
                except (ValueError, AttributeError):
                    pass

        top_results = filtered_results[:post_filter_top_k]
        logger.info(f"After date filtering: {len(top_results)} results")

    logger.info(f"Returning {len(top_results)} results")
    return top_results


async def search_qa_index(
    query: str,
    top_k: int = 5
) -> Dict[str, List[Dict]]:
    """Search QA index via vector similarity; returns deduped structured QA items with contexts."""
    if not state.qa_index_loaded:
        return {"qa_items": [], "total": 0}

    qa_items = state.qa_items.copy()

    logger.info(f"Generating embedding for query (QA index): '{query}'")
    query_embedding = await state.embedding_client.embed(query)

    if isinstance(query_embedding, np.ndarray) and query_embedding.ndim > 1:
        query_embedding = query_embedding[0]

    scored = []
    for qa in qa_items:
        embedding = qa.get('embedding')
        if embedding is None:
            continue

        similarity = dot_product_similarity(query_embedding, embedding)
        scored.append({
            'question': qa.get('question'),
            'answer': qa.get('answer'),
            'context': qa.get('context'),
            'source_url': qa.get('source_url'),
            'doc_metadata': qa.get('doc_metadata'),
            'similarity': similarity
        })

    scored.sort(key=lambda x: x['similarity'], reverse=True)

    deduped = {}
    for item in scored:
        key = item.get('doc_metadata', {}).get('document_id') or item.get('source_url') or (item.get('question'), item.get('answer'))
        if key not in deduped:
            deduped[key] = item

    qa_results = list(deduped.values())[:top_k]
    logger.info(f"Returning {len(qa_results)} QA results (deduped from {len(scored)})")

    return {
        "qa_items": qa_results,
        "total": len(qa_results)
    }


async def generate_llm_response(query: str, top_summaries: List[Dict], temperature: float, max_tokens: int, enrichment_text: str = "") -> tuple[str, str]:
    """
    Generate LLM response using retrieved summaries as grounding.

    Args:
        enrichment_text: Optional BBOA enrichment context block appended after
            primary grounding to give the LLM additional event/media/risk context.

    Returns: (llm_response, full_prompt)
    """
    prompt = build_summary_prompt(query, top_summaries, clean_author_fn=_clean_author_name)
    if enrichment_text:
        # Insert enrichment context BEFORE the generation trigger ("Overview:")
        # so the LLM reads it as grounding input, not as content to complete.
        generation_trigger = "Overview:"
        if generation_trigger in prompt:
            prompt = prompt.replace(generation_trigger, f"{enrichment_text}\n\n{generation_trigger}", 1)
        else:
            prompt = enrichment_text + "\n\n" + prompt

    logger.info(f"Generating LLM response with {len(top_summaries)} summaries as grounding")

    # Generate response with specified parameters
    llm_response = await state.llm_client.generate_content(
        prompt,
        max_tokens=max_tokens,
        temperature=temperature
    )

    return llm_response, prompt


def build_qa_grounding_text(qa_items: List[Dict]) -> str:
    """Format QA items with contexts for refinement prompt."""
    if not qa_items:
        return ""

    lines = []
    for i, item in enumerate(qa_items, 1):
        q = item.get('question', '')
        a = item.get('answer', '')
        c = item.get('context', '')

        # Extract metadata for citation formatting
        doc_metadata = item.get('doc_metadata', {})
        doc_id = doc_metadata.get('document_id', 'unknown')
        author = _clean_author_name(doc_metadata.get('author') or doc_metadata.get('publisher', 'Unknown'))

        # Format date
        date_str = 'N.D.'
        creation_date = doc_metadata.get('creation_date') or doc_metadata.get('publication_date')
        if creation_date:
            try:
                date_obj = datetime.strptime(creation_date[:10], '%Y-%m-%d')
                date_str = date_obj.strftime('%b %Y')
            except:
                date_str = creation_date[:10] if len(creation_date) >= 10 else creation_date

        # Create citation reference with document_id for traceability
        citation = f"{author} [qa-{i}], {date_str}"

        lines.append(f"QA Pair {i}:\nDocument ID: {doc_id}\nCitation: ({citation})\nCite as: ({doc_id}, qa-{i})\nQ: {q}\nA: {a}\nContext: {c}")

    return "\n\n".join(lines)


async def refine_with_qa(draft_response: str, qa_items: List[Dict], query: str, temperature: float, max_tokens: int) -> tuple[str, str]:
    """Refine draft response using QA grounding."""
    grounding_text = build_qa_grounding_text(qa_items)
    prompt = build_qa_refinement_prompt(query=query, draft_response=draft_response, grounding_text=grounding_text)

    refined_response = await state.llm_client.generate_content(
        prompt,
        max_tokens=max_tokens,
        temperature=temperature
    )

    return refined_response, prompt

async def generate_web_search_response(query: str, temperature: float = 0.5, max_tokens: int = 4000) -> tuple:
    """
    Generate LLM response using web search grounding.

    Returns:
        tuple: (response_dict, query_string) where response_dict contains 'text' and 'grounding_metadata'
    """
    logger.info(f"Generating web search response for query: {query}")

    # Generate response with web search grounding
    response = await state.llm_client.generate_content(
        query,
        max_tokens=max_tokens,
        temperature=temperature,
        enable_search=True,
        return_grounding_metadata=True
    )

    return response, query

def publish_webscraper_messages(request_id: str, session_id: str, query: str, response_data: Dict):
    """
    Extract URLs from grounding metadata and publish web-scraper messages.

    Extracts citation URLs from Vertex AI grounding metadata and creates
    web-scraper messages for downstream processing.
    """
    if not state.kafka_producer:
        logger.warning("Kafka producer not initialized, skipping web-scraper message publication")
        return

    try:
        grounding_metadata = response_data.get('grounding_metadata', {})
        citations = grounding_metadata.get('citations', [])

        if not citations:
            logger.info("No citations found in grounding metadata, skipping web-scraper messages")
            return

        # Extract unique URLs from citations
        urls = set()
        for citation in citations:
            if 'uri' in citation:
                urls.add(citation['uri'])

        if not urls:
            logger.info("No URIs found in citations")
            return

        urls_list = sorted(list(urls))
        total_urls = len(urls_list)

        logger.info(f"Publishing {total_urls} web-scraper messages from search grounding")

        # Create and publish web-scraper messages
        for idx, url in enumerate(urls_list, 1):
            # Parse URL for title
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc or "unknown"

            message = {
                "uri": url,
                "title": f"Document from {domain}",
                "context": [f"URL {idx} of {total_urls} from search grounding: {query}"],
                "session_id": session_id,
                "request_id": f"{request_id}_url_{idx:03d}",
                "total_requests": total_urls,
                "source": "search_mode_grounding"
            }

            state.kafka_producer.produce(
                'web-scrape-requests',
                key=message['request_id'].encode('utf-8'),
                value=json.dumps(message).encode('utf-8'),
                callback=lambda err, msg, i=idx: logger.error(f"Kafka produce error for URL {i}: {err}") if err else logger.info(f"✅ Published web-scraper message {i}/{total_urls}")
            )

        state.kafka_producer.flush()
        logger.info(f"📤 Published {total_urls} web-scraper messages to web-scrape-requests topic")

    except Exception as e:
        logger.error(f"Error publishing web-scraper messages: {e}", exc_info=True)


# ============================================================================
# Startup & Shutdown
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Load index and initialize clients on startup."""
    logger.info("🚀 Starting Summary Query Service...")

    # Index path - env var override for dev/demo; falls back to project root combined file
    project_root = Path(__file__).parent.parent.parent.parent
    index_env = os.environ.get('INDEX_PATH')
    index_path = Path(index_env) if index_env else project_root / "research_summaries_with_embeddings_combined.jsonl"

    # Check if index exists
    if not index_path.exists():
        logger.error(f"❌ Index file not found: {index_path}")
        raise RuntimeError(f"Index file not found: {index_path}")

    # Load index
    logger.info(f"📚 Loading index from: {index_path}")
    state.summaries = load_summaries_index(str(index_path))
    state.index_path = str(index_path)
    logger.info(f"✅ Loaded {len(state.summaries)} summaries")

    # Load QA index (optional; keeps legacy summary-only when missing)
    qa_index_env = os.environ.get('QA_INDEX_PATH')
    qa_index_path = Path(qa_index_env) if qa_index_env else project_root / "research_qa_pairs_with_embeddings_combined.jsonl"

    if qa_index_path.exists():
        state.qa_items = load_qa_index(str(qa_index_path))
        state.qa_index_path = str(qa_index_path)
        state.qa_index_loaded = True
        logger.info(f"✅ Loaded {len(state.qa_items)} QA pairs")
    else:
        state.qa_index_loaded = False
        state.qa_index_path = str(qa_index_path)
        logger.warning(f"⚠️  QA index not found at {qa_index_path}; continuing with summaries only")

    # Load enrichment indices (linked and unlinked BBOA media)
    linked_env = os.environ.get('LINKED_INDEX_PATH')
    unlinked_env = os.environ.get('UNLINKED_INDEX_PATH')

    # Default: find the most-recent timestamped linked/unlinked file in project root
    def _find_enrichment_file(prefix: str) -> Optional[Path]:
        candidates = sorted(project_root.glob(f"{prefix}_*.jsonl"), reverse=True)
        return candidates[0] if candidates else None

    linked_path = Path(linked_env) if linked_env else _find_enrichment_file("linked_media_bboa_full")
    unlinked_path = Path(unlinked_env) if unlinked_env else _find_enrichment_file("unlinked_media_bboa_full")

    state.linked_index_path = str(linked_path) if linked_path else None
    state.unlinked_index_path = str(unlinked_path) if unlinked_path else None

    state.linked_enrichments = load_enrichment_index(str(linked_path)) if linked_path else {}
    state.unlinked_enrichments = load_enrichment_index(str(unlinked_path)) if unlinked_path else {}

    logger.info(
        f"📎 Enrichment indices: linked={len(state.linked_enrichments)} URLs, "
        f"unlinked={len(state.unlinked_enrichments)} URLs"
    )

    from src.services.rag.enrichment_retriever import \
        EnrichmentRetriever  # noqa: E402
    state.enrichment_retriever = EnrichmentRetriever(
        linked_map=state.linked_enrichments,
        unlinked_map=state.unlinked_enrichments,
    )
    logger.info("✅ EnrichmentRetriever ready")

    # Initialize clients
    logger.info("🔧 Initializing embedding client...")
    state.embedding_client = EmbeddingServiceClient()

    logger.info("🔧 Initializing LLM client...")
    state.llm_client = GeminiLLMClient()

    state.index_loaded = True
    state.startup_time = datetime.now()

    # Initialize Kafka producer for research results
    kafka_bootstrap = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    state.kafka_bootstrap_servers = kafka_bootstrap
    try:
        producer_config = {
            'bootstrap.servers': kafka_bootstrap,
            'client.id': 'rag-service-producer'
        }
        state.kafka_producer = Producer(producer_config)
        logger.info(f"✅ Kafka producer initialized: {kafka_bootstrap}")
    except Exception as e:
        logger.warning(f"⚠️  Kafka producer not available: {e}")

    # Start Kafka consumer for index reload (optional - only if Kafka available)
    try:
        from src.services.rag.index_reload_consumer import IndexReloadConsumer

        async def reload_index_callback():
            """Callback to reload index when message received"""
            try:
                logger.info("🔄 Reloading index...")
                new_summaries = load_summaries_index(state.index_path)
                old_count = len(state.summaries)
                new_count = len(new_summaries)
                state.summaries = new_summaries
                logger.info(f"✅ Index reloaded: {old_count} -> {new_count} summaries (+{new_count - old_count})")
            except Exception as e:
                logger.error(f"❌ Error reloading index: {e}")

        reload_consumer = IndexReloadConsumer(reload_index_callback, kafka_bootstrap)
        reload_consumer.start()
        logger.info("✅ Index reload consumer started")
    except Exception as e:
        logger.warning(f"⚠️  Kafka reload consumer not available: {e}")

    logger.info("✅ Service ready!")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    logger.info("🛑 Shutting down Summary Query Service...")
    state.index_loaded = False


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "uptime_seconds": (datetime.now() - state.startup_time).total_seconds() if state.startup_time else 0
    }

@app.get("/ready")
async def readiness():
    """Readiness check endpoint."""
    if not state.index_loaded:
        raise HTTPException(status_code=503, detail="Service not ready - index not loaded")

    return {
        "status": "ready",
        "index_loaded": state.index_loaded,
        "total_summaries": len(state.summaries),
        "index_path": state.index_path,
        "enrichment": {
            "linked_urls": len(state.linked_enrichments),
            "linked_index_path": state.linked_index_path,
            "unlinked_urls": len(state.unlinked_enrichments),
            "unlinked_index_path": state.unlinked_index_path,
        },
        "timestamp": datetime.now().isoformat()
    }

@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """
    Search the summary index using vector similarity.

    Returns matching documents without LLM processing.
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    session_id = f"search_{int(time.time())}"

    logger.info(f"[{request_id}] Search request: query='{request.query}', top_k={request.top_k}")

    if not state.index_loaded:
        raise HTTPException(status_code=503, detail="Index not loaded")

    try:
        # Search index
        results = await search_index(
            query=request.query,
            top_k=request.top_k,
            start_date=request.start_date,
            end_date=request.end_date,
            post_filter_top_k=request.post_filter_top_k
        )

        # Format results
        retrieved_docs = []
        for result in results:
            summary = result['summary']
            retrieved_docs.append(DocumentResult(
                document_id=summary.get('document_id', 'unknown'),
                source_url=summary.get('source_url', 'N/A'),
                publication_date=summary.get('doc_metadata', {}).get('publication_date'),
                similarity_score=result['similarity'],
                summary_text=summary.get('summary_text', ''),
                doc_metadata=summary.get('doc_metadata', {})
            ))

        # Enrichment lookup (optional)
        enrichment_results = None
        if request.enrich and state.enrichment_retriever and retrieved_docs:
            urls = [doc.source_url for doc in retrieved_docs]
            enrichment_results = state.enrichment_retriever.lookup(
                urls, enrich_types=request.enrich_types, get_linked=request.get_linked
            )
            logger.info(f"[{request_id}] Enrichment: {len(enrichment_results)} records")

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(f"[{request_id}] Search completed: {len(retrieved_docs)} docs, {execution_time_ms:.2f}ms")

        return SearchResponse(
            request_id=request_id,
            session_id=session_id,
            query=request.query,
            index_queried=state.index_path,
            top_k=request.top_k,
            date_filters={
                "start_date": request.start_date,
                "end_date": request.end_date
            },
            retrieved_docs=retrieved_docs,
            total_retrieved=len(retrieved_docs),
            execution_time_ms=execution_time_ms,
            enrichment_results=enrichment_results,
        )

    except Exception as e:
        logger.error(f"[{request_id}] Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query_llm", response_model=QueryLLMResponse)
async def query_llm(request: QueryLLMRequest):
    """
    Query the summary index and generate LLM response with grounding.

    Performs vector similarity search, then uses retrieved documents
    as grounding data for LLM response generation.
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())
    session_id = f"query_{int(time.time())}"

    logger.info(f"[{request_id}] Query LLM request: query='{request.query}', top_k={request.top_k}")

    if not state.index_loaded and request.mode == 'ask':
        raise HTTPException(status_code=503, detail="Index not loaded")

    try:
        # Step 1: Search index (only for ask mode)
        if request.mode == 'search':
            # Skip index search for search mode - use web grounding only
            logger.info(f"[{request_id}] Step 1: Skipping index search (using web grounding)")
            results = []
            retrieved_docs = []
            qa_grounding = {"qa_items": [], "total": 0}
        else:
            # Search index for ask mode
            logger.info(f"[{request_id}] Step 1: Searching index...")
            results = await search_index(
                query=request.query,
                top_k=request.top_k,
                start_date=request.start_date,
                end_date=request.end_date,
                post_filter_top_k=request.post_filter_top_k
            )

            if not results:
                raise HTTPException(status_code=404, detail="No matching documents found")

            # Format results for response
            retrieved_docs = []
            for result in results:
                summary = result['summary']
                source_url = summary.get('source_url', 'N/A')
                retrieved_docs.append(DocumentResult(
                    document_id=summary.get('document_id', 'unknown'),
                    source_url=source_url,
                    source_url_display=decode_url_for_display(source_url),  # Add decoded URL for display
                    publication_date=summary.get('doc_metadata', {}).get('publication_date'),
                    similarity_score=result['similarity'],
                    summary_text=summary.get('summary_text', ''),
                    doc_metadata=summary.get('doc_metadata', {})
                ))

            # Optionally search QA index
            qa_grounding = {"qa_items": [], "total": 0}
            if request.use_qa:
                logger.info(f"[{request_id}] Searching QA index (top_k={request.qa_top_k})...")
                qa_grounding = await search_qa_index(query=request.query, top_k=request.qa_top_k)

                # Add source_url_display to QA items
                for qa_item in qa_grounding.get("qa_items", []):
                    if 'source_url' in qa_item:
                        qa_item['source_url_display'] = decode_url_for_display(qa_item['source_url'])

        # Enrichment lookup (optional; used in ask mode to augment LLM grounding)
        enrichment_results = None
        enrichment_text = ""
        if request.enrich and state.enrichment_retriever and retrieved_docs:
            urls = [doc.source_url for doc in retrieved_docs]
            enrichment_results = state.enrichment_retriever.lookup(
                urls, enrich_types=request.enrich_types, get_linked=request.get_linked
            )
            if enrichment_results:
                enrichment_text = state.enrichment_retriever.format_for_grounding(enrichment_results)
                logger.info(
                    f"[{request_id}] Enrichment: {len(enrichment_results)} records "
                    f"for {len(urls)} primary docs"
                )

        # Step 2: Generate LLM response based on mode
        logger.info(f"[{request_id}] Step 2: Generating LLM response (mode={request.mode})...")

        grounding_metadata = None

        if request.mode == 'search':
            # Use web search grounding
            logger.info(f"[{request_id}] Using web search mode")
            response_data, full_prompt = await generate_web_search_response(
                query=request.query,
                temperature=request.temperature,
                max_tokens=request.max_tokens
            )
            llm_response = response_data.get('text', '')
            grounding_metadata = response_data.get('grounding_metadata', {})

            # Publish web-scraper messages for citation URLs
            publish_webscraper_messages(request_id, session_id, request.query, response_data)
        else:
            # Use index-only mode
            logger.info(f"[{request_id}] Using ask mode (index-only)")
            llm_response, full_prompt = await generate_llm_response(
                query=request.query,
                top_summaries=results,
                temperature=request.temperature,
                max_tokens=request.max_tokens,
                enrichment_text=enrichment_text,
            )

            # Optional QA refinement (second-stage prompt)
            if request.use_qa and qa_grounding.get("qa_items"):
                logger.info(f"[{request_id}] Refining with QA grounding ({len(qa_grounding.get('qa_items', []))} items)")
                # Use the user's requested max_tokens for refinement to support large grounding contexts
                llm_response, full_prompt = await refine_with_qa(
                    draft_response=llm_response,
                    qa_items=qa_grounding.get("qa_items", []),
                    query=request.query,
                    temperature=request.temperature,
                    max_tokens=request.max_tokens  # Use user's requested max_tokens for large grounding
                )

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(f"[{request_id}] Query LLM completed: {len(retrieved_docs)} docs, {execution_time_ms:.2f}ms")

        # Build citation map for converting (doc_id, qa-N) -> (Author, Date) in frontend
        logger.info(f"[{request_id}] Building citation map...")
        logger.info(f"[{request_id}]   Retrieved docs: {len(retrieved_docs)}")
        logger.info(f"[{request_id}]   QA items: {len(qa_grounding.get('qa_items', []))}")

        citation_map = build_citation_map(retrieved_docs, qa_grounding.get("qa_items"))

        logger.info(f"[{request_id}] Citation map built with {len(citation_map)} entries")
        logger.info(f"[{request_id}] Citation map keys: {list(citation_map.keys())[:5]}...")  # Show first 5

        # Return appropriate response based on mode
        if request.mode == 'search':
            return SearchModeResponse(
                request_id=request_id,
                session_id=session_id,
                query=request.query,
                index_queried=state.index_path,
                top_k=request.top_k,
                date_filters={
                    "start_date": request.start_date,
                    "end_date": request.end_date
                },
                retrieved_docs=[],  # No index docs in search mode
                total_retrieved=0,  # No index docs in search mode
                execution_time_ms=execution_time_ms,
                mode=request.mode,
                llm_response=llm_response,
                llm_model="gemini-2.5-pro",
                llm_params={
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens
                },
                prompt=full_prompt,
                grounding_metadata=grounding_metadata
            )
        else:
            return QueryLLMResponse(
                request_id=request_id,
                session_id=session_id,
                query=request.query,
                index_queried=state.index_path,
                top_k=request.top_k,
                date_filters={
                    "start_date": request.start_date,
                    "end_date": request.end_date
                },
                retrieved_docs=retrieved_docs,
                total_retrieved=len(retrieved_docs),
                execution_time_ms=execution_time_ms,
                mode=request.mode,
                llm_response=llm_response,
                llm_model="gemini-2.5-pro",
                llm_params={
                    "temperature": request.temperature,
                    "max_tokens": request.max_tokens
                },
                prompt=full_prompt,
                retrieved_qa=qa_grounding.get("qa_items"),
                citation_map=citation_map,
                enrichment_results=enrichment_results,
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Query LLM error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/grounding_preview", response_model=GroundingPreviewResponse)
async def grounding_preview(request: GroundingPreviewRequest):
    """Return summaries and optional QA grounding without invoking LLM."""
    start_time = time.time()
    request_id = str(uuid.uuid4())
    session_id = f"preview_{int(time.time())}"

    logger.info(f"[{request_id}] Grounding preview request: query='{request.query}', top_k={request.top_k}, use_qa={request.use_qa}")

    if not state.index_loaded:
        raise HTTPException(status_code=503, detail="Index not loaded")

    try:
        # Summaries
        results = await search_index(
            query=request.query,
            top_k=request.top_k,
            start_date=request.start_date,
            end_date=request.end_date,
            post_filter_top_k=request.post_filter_top_k
        )

        retrieved_docs = []
        for result in results:
            summary = result['summary']
            retrieved_docs.append(DocumentResult(
                document_id=summary.get('document_id', 'unknown'),
                source_url=summary.get('source_url', 'N/A'),
                publication_date=summary.get('doc_metadata', {}).get('publication_date'),
                similarity_score=result['similarity'],
                summary_text=summary.get('summary_text', ''),
                doc_metadata=summary.get('doc_metadata', {})
            ))

        qa_grounding = {"qa_items": [], "total": 0}
        if request.use_qa:
            qa_grounding = await search_qa_index(query=request.query, top_k=request.qa_top_k)

        # Enrichment lookup (optional)
        enrichment_results = None
        enrichment_preview = None
        if request.enrich and state.enrichment_retriever and retrieved_docs:
            urls = [doc.source_url for doc in retrieved_docs]
            enrichment_results = state.enrichment_retriever.lookup(
                urls, enrich_types=request.enrich_types, get_linked=request.get_linked
            )
            if enrichment_results:
                enrichment_preview = state.enrichment_retriever.format_for_grounding(enrichment_results)
                logger.info(f"[{request_id}] Enrichment preview: {len(enrichment_results)} records")

        execution_time_ms = (time.time() - start_time) * 1000

        return GroundingPreviewResponse(
            request_id=request_id,
            session_id=session_id,
            query=request.query,
            index_queried=state.index_path,
            retrieved_docs=retrieved_docs,
            retrieved_qa=qa_grounding.get("qa_items"),
            total_retrieved=len(retrieved_docs),
            execution_time_ms=execution_time_ms,
            enrichment_results=enrichment_results,
            enrichment_preview=enrichment_preview,
        )

    except Exception as e:
        logger.error(f"[{request_id}] Grounding preview error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Metadata Query Route
# ============================================================================

class MetadataQueryRequest(BaseModel):
    """Request model for metadata-based index lookup (no embedding search)."""
    source_url: Optional[str] = Field(None, description="Filter by exact source URL")
    document_id: Optional[str] = Field(None, description="Filter by document_id (summary ID or QA doc_id)")
    start_date: Optional[str] = Field(None, description="Filter start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="Filter end date (YYYY-MM-DD)")
    include_summaries: bool = Field(default=True, description="Search the summary index")
    include_qa: bool = Field(default=True, description="Search the QA index")
    max_results: int = Field(default=100, description="Max results per index", ge=1, le=500)


class MetadataQueryResponse(BaseModel):
    """Response model for metadata query."""
    request_id: str
    filters_applied: Dict[str, Optional[str]]
    retrieved_docs: List[DocumentResult]
    retrieved_qa: List[Dict]
    total_summaries: int
    total_qa: int
    execution_time_ms: float


def _strip_hierarchy_prefix(text: str) -> str:
    """Strip section hierarchy prefix from chunk context.

    QA contexts are stored as ``a > b > c: actual content``.
    This returns just the content after the colon.
    """
    if ' > ' not in text:
        return text
    last_gt = text.rfind(' > ')
    colon_pos = text.find(': ', last_gt)
    if colon_pos != -1:
        stripped = text[colon_pos + 2:]
        return stripped if stripped else text
    return text


def _match_date_range(pub_date_str: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> bool:
    """Check whether a publication date falls within the given range."""
    if not pub_date_str:
        return False
    try:
        doc_date = datetime.strptime(pub_date_str[:10], '%Y-%m-%d').date()
        start = datetime.strptime(start_date, '%Y-%m-%d').date() if start_date else None
        end = datetime.strptime(end_date, '%Y-%m-%d').date() if end_date else None
        return (not start or doc_date >= start) and (not end or doc_date <= end)
    except (ValueError, AttributeError):
        return False


@app.post("/query_metadata", response_model=MetadataQueryResponse)
async def query_metadata(request: MetadataQueryRequest):
    """
    Query the summary and QA indices by metadata filters instead of semantic search.

    Supports filtering by source_url, document_id, and date range.
    Returns matching summaries and QA pairs without embedding computation or LLM calls.
    """
    start_time = time.time()
    request_id = str(uuid.uuid4())

    has_url = bool(request.source_url)
    has_doc_id = bool(request.document_id)
    has_date = bool(request.start_date or request.end_date)

    if not (has_url or has_doc_id or has_date):
        raise HTTPException(
            status_code=422,
            detail="At least one filter required: source_url, document_id, or date range"
        )

    logger.info(
        f"[{request_id}] Metadata query: source_url={request.source_url}, "
        f"document_id={request.document_id}, dates={request.start_date}..{request.end_date}"
    )

    retrieved_docs: List[DocumentResult] = []
    retrieved_qa: List[Dict] = []

    # --- Search summaries ---
    if request.include_summaries and state.index_loaded:
        for summary in state.summaries:
            # URL filter
            if has_url and summary.get('source_url') != request.source_url:
                continue
            # Document ID filter
            if has_doc_id and summary.get('document_id') != request.document_id:
                continue
            # Date filter
            if has_date:
                pub_date = summary.get('doc_metadata', {}).get('publication_date')
                if not _match_date_range(pub_date, request.start_date, request.end_date):
                    continue

            retrieved_docs.append(DocumentResult(
                document_id=summary.get('document_id', 'unknown'),
                source_url=summary.get('source_url', 'N/A'),
                source_url_display=decode_url_for_display(summary.get('source_url', '')),
                publication_date=summary.get('doc_metadata', {}).get('publication_date'),
                similarity_score=1.0,  # exact match, not similarity-based
                summary_text=summary.get('summary_text', ''),
                doc_metadata=summary.get('doc_metadata', {})
            ))

            if len(retrieved_docs) >= request.max_results:
                break

    # --- Search QA index ---
    if request.include_qa and state.qa_index_loaded:
        for qa in state.qa_items:
            qa_meta = qa.get('doc_metadata', {})
            qa_source_url = qa.get('source_url') or qa_meta.get('source_url')
            qa_doc_id = qa_meta.get('document_id')

            # URL filter
            if has_url and qa_source_url != request.source_url:
                continue
            # Document ID filter
            if has_doc_id and qa_doc_id != request.document_id:
                continue
            # Date filter
            if has_date:
                pub_date = qa_meta.get('creation_date') or qa_meta.get('publication_date')
                if not _match_date_range(pub_date, request.start_date, request.end_date):
                    continue

            raw_context = qa.get('context') or ''
            retrieved_qa.append({
                'question': qa.get('question'),
                'answer': qa.get('answer'),
                'context': _strip_hierarchy_prefix(raw_context),
                'source_url': qa_source_url,
                'source_url_display': decode_url_for_display(qa_source_url or ''),
                'doc_metadata': qa_meta,
            })

            if len(retrieved_qa) >= request.max_results:
                break

    execution_time_ms = (time.time() - start_time) * 1000

    logger.info(
        f"[{request_id}] Metadata query completed: "
        f"{len(retrieved_docs)} summaries, {len(retrieved_qa)} QA pairs, {execution_time_ms:.2f}ms"
    )

    return MetadataQueryResponse(
        request_id=request_id,
        filters_applied={
            "source_url": request.source_url,
            "document_id": request.document_id,
            "start_date": request.start_date,
            "end_date": request.end_date,
        },
        retrieved_docs=retrieved_docs,
        retrieved_qa=retrieved_qa,
        total_summaries=len(retrieved_docs),
        total_qa=len(retrieved_qa),
        execution_time_ms=execution_time_ms,
    )


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
