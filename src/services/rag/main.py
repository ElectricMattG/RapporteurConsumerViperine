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
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.clients.embedding_client import EmbeddingServiceClient
from src.clients.llm_gcp_client import GeminiLLMClient

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
        self.index_loaded = False
        self.summaries: List[Dict] = []
        self.index_path: Optional[str] = None
        self.embedding_client: Optional[EmbeddingServiceClient] = None
        self.llm_client: Optional[GeminiLLMClient] = None
        self.startup_time: Optional[datetime] = None

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

class QueryLLMRequest(SearchRequest):
    """Request model for query_llm endpoint (extends SearchRequest)."""
    temperature: float = Field(default=0.7, description="LLM temperature", ge=0.0, le=2.0)
    max_tokens: int = Field(default=4000, description="Maximum tokens for LLM response", ge=100, le=8000)

class DocumentResult(BaseModel):
    """Single document result with metadata."""
    document_id: str
    source_url: str
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

class QueryLLMResponse(SearchResponse):
    """Response model for query_llm endpoint (extends SearchResponse)."""
    llm_response: str
    llm_model: str
    llm_params: Dict[str, float]
    prompt: str


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


async def generate_llm_response(query: str, top_summaries: List[Dict], temperature: float, max_tokens: int) -> tuple[str, str]:
    """
    Generate LLM response using retrieved summaries as grounding.

    Returns: (llm_response, full_prompt)
    """
    # Format summaries for context
    summaries_text = []
    for i, result in enumerate(top_summaries, 1):
        summary = result['summary']
        score = result['similarity']
        doc_id = summary.get('document_id', 'unknown')
        source_url = summary.get('source_url', 'N/A')
        doc_metadata = summary.get('doc_metadata', {})
        pub_date = doc_metadata.get('publication_date', 'N/A')
        summary_text = summary.get('summary_text', '')

        # Extract author/publisher
        author = doc_metadata.get('authors', [])
        if isinstance(author, list) and author:
            author = author[0]
        elif not author:
            author = doc_metadata.get('author') or doc_metadata.get('publisher', 'Unknown')

        # Create URL stub
        url_stub = 'N/A'
        if source_url != 'N/A':
            try:
                from urllib.parse import urlparse
                parsed = urlparse(source_url)
                url_stub = parsed.netloc.replace('www.', '')
                # Shorten YouTube URLs
                if 'youtube.com' in url_stub or 'youtu.be' in url_stub:
                    url_stub = 'youtube.com'
            except:
                url_stub = source_url[:30] + '...'

        # Format date
        date_str = pub_date
        if pub_date != 'N/A':
            try:
                date_obj = datetime.strptime(pub_date, '%Y-%m-%d')
                date_str = date_obj.strftime('%b %Y')
            except:
                pass

        # Create citation reference
        citation = f"{author}, {date_str}, {url_stub}"

        summaries_text.append(
            f"Summary {i} (similarity: {score:.4f}):\n"
            f"Citation: {citation}\n"
            f"Full URL: {source_url}\n"
            f"Content: {summary_text}\n"
        )

    context = "\n\n".join(summaries_text)

    prompt = f"""Given these summaries, that contain key information from reputable news sources and sometimes international agencies, synthesise the information to communicate the key facts, events and outcomes, with reference to dates if available. All events are real events they are not hypothetical scenarios so treat them as such.

    The synthesis of should incorporate the available context to provide a new document that yeilds insights and understanding, not just repeating facts from the different documents. The document should speak to legal and policy researchers highlight policy and legal aspects of the documents. Cite the documents as below. When you cite summaries, don't refer to them as 'summaries', rather refer to them as 'artilces' and/or 'documents.'

IMPORTANT: When citing sources, use this format: (Author/Publisher, Date) - for example: (Al-Jazeera, Nov 2023) and then hyperlink to the actual url, but don't display the url use the text as an anchor.
Use the Citation information provided for each summary to create your references.

Original Query: {query}

Top {len(top_summaries)} Summaries:

{context}

Please provide:
1. A high-level overview of the main themes across these summaries
2. Key findings or insights that emerge
3. Any patterns or connections between the summaries
4. Notable differences or contrasting viewpoints if any

Overview:"""

    logger.info(f"Generating LLM response with {len(top_summaries)} summaries as grounding")

    # Generate response with specified parameters
    llm_response = await state.llm_client.generate_content(
        prompt,
        max_tokens=max_tokens,
        temperature=temperature
    )

    return llm_response, prompt


# ============================================================================
# Startup & Shutdown
# ============================================================================

@app.on_event("startup")
async def startup_event():
    """Load index and initialize clients on startup."""
    logger.info("🚀 Starting Summary Query Service...")

    # Index path - use root level index with 131 summaries
    # Path calculation: main.py is in src/services/rag/, so go up 3 levels to project root
    project_root = Path(__file__).parent.parent.parent.parent
    index_path = project_root / "research_summaries_with_embeddings_combined.jsonl"

    # Check if index exists
    if not index_path.exists():
        logger.error(f"❌ Index file not found: {index_path}")
        raise RuntimeError(f"Index file not found: {index_path}")


    # Load index
    logger.info(f"📚 Loading index from: {index_path}")
    state.summaries = load_summaries_index(str(index_path))
    state.index_path = str(index_path)
    logger.info(f"✅ Loaded {len(state.summaries)} summaries")

    # Initialize clients
    logger.info("🔧 Initializing embedding client...")
    state.embedding_client = EmbeddingServiceClient()

    logger.info("🔧 Initializing LLM client...")
    state.llm_client = GeminiLLMClient()

    state.index_loaded = True
    state.startup_time = datetime.now()

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
            execution_time_ms=execution_time_ms
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

    if not state.index_loaded:
        raise HTTPException(status_code=503, detail="Index not loaded")

    try:
        # Step 1: Search index
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
            retrieved_docs.append(DocumentResult(
                document_id=summary.get('document_id', 'unknown'),
                source_url=summary.get('source_url', 'N/A'),
                publication_date=summary.get('doc_metadata', {}).get('publication_date'),
                similarity_score=result['similarity'],
                summary_text=summary.get('summary_text', ''),
                doc_metadata=summary.get('doc_metadata', {})
            ))

        # Step 2: Generate LLM response
        logger.info(f"[{request_id}] Step 2: Generating LLM response...")
        llm_response, full_prompt = await generate_llm_response(
            query=request.query,
            top_summaries=results,
            temperature=request.temperature,
            max_tokens=request.max_tokens
        )

        execution_time_ms = (time.time() - start_time) * 1000

        logger.info(f"[{request_id}] Query LLM completed: {len(retrieved_docs)} docs, {execution_time_ms:.2f}ms")

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
            llm_response=llm_response,
            llm_model="gemini-2.5-flash",
            llm_params={
                "temperature": request.temperature,
                "max_tokens": request.max_tokens
            },
            prompt=full_prompt
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Query LLM error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
