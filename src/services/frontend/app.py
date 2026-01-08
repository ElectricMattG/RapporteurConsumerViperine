#!/usr/bin/env python3
"""
Simple Flask server for RAG Frontend UI.

Serves static HTML/JS/CSS and proxies requests to the RAG service.
"""

import json
import logging
from datetime import datetime

import requests
from flask import Flask, jsonify, render_template, request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# RAG service configuration
RAG_SERVICE_URL = "http://localhost:8000"

# In-memory conversation storage (persists during runtime)
conversations = []
current_conversation_id = 0


@app.route('/')
def index():
    """Serve the main UI page."""
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
def query_rag():
    """
    Proxy query to RAG service with context.

    Expects JSON:
    {
        "query": "user query text",
        "context_depth": 5,
        "top_k": 3,
        "start_date": "2025-01-01",
        "end_date": "2026-01-07",
        "temperature": 0.7,
        "max_tokens": 4000
    }
    """
    global current_conversation_id

    try:
        data = request.json
        user_query = data.get('query', '')
        context_depth = data.get('context_depth', 3)
        top_k = data.get('top_k', 3)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        temperature = data.get('temperature', 0.7)
        max_tokens = data.get('max_tokens', 16000)

        logger.info(f"Received query: {user_query}")

        # Build context from previous conversations
        context = build_context(context_depth)

        # Prepend context to query
        full_query = user_query
        if context:
            full_query = f"CONTEXT:\n{context}\n\nCurrent Query: {user_query}"

        # Call RAG service
        rag_payload = {
            "query": full_query,
            "top_k": top_k,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        if start_date:
            rag_payload["start_date"] = start_date
        if end_date:
            rag_payload["end_date"] = end_date

        logger.info(f"Calling RAG service: {RAG_SERVICE_URL}/query_llm")
        logger.info(f"RAG Request Payload: {json.dumps(rag_payload, indent=2)}")
        response = requests.post(
            f"{RAG_SERVICE_URL}/query_llm",
            json=rag_payload,
            timeout=60
        )
        response.raise_for_status()

        rag_response = response.json()

        # Log response summary
        logger.info(f"RAG Response received:")
        logger.info(f"  - Request ID: {rag_response.get('request_id')}")
        logger.info(f"  - Session ID: {rag_response.get('session_id')}")
        logger.info(f"  - Execution time: {rag_response.get('execution_time_ms')}ms")
        logger.info(f"  - Documents retrieved: {rag_response.get('total_retrieved')}")
        logger.info(f"  - LLM model: {rag_response.get('llm_model')}")
        logger.info(f"  - LLM response length: {len(rag_response.get('llm_response', ''))} chars")
        logger.info(f"Full RAG Response: {json.dumps(rag_response, indent=2, ensure_ascii=False)}")

        # Create conversation entry
        conversation_entry = {
            "id": current_conversation_id,
            "timestamp": datetime.now().isoformat(),
            "user_query": user_query,
            "assistant_response": rag_response.get('llm_response', ''),
            "retrieved_docs": rag_response.get('retrieved_docs', []),
            "metadata": {
                "request_id": rag_response.get('request_id'),
                "session_id": rag_response.get('session_id'),
                "execution_time_ms": rag_response.get('execution_time_ms'),
                "total_retrieved": rag_response.get('total_retrieved'),
                "llm_model": rag_response.get('llm_model'),
                "llm_params": rag_response.get('llm_params'),
                "date_filters": data.get('start_date') or data.get('end_date')
            }
        }

        conversations.append(conversation_entry)
        current_conversation_id += 1

        logger.info(f"Query completed: {len(rag_response.get('retrieved_docs', []))} docs retrieved")

        return jsonify(conversation_entry)

    except requests.exceptions.RequestException as e:
        logger.error(f"RAG service error: {e}")
        return jsonify({
            "error": "Failed to connect to RAG service",
            "details": str(e)
        }), 503
    except Exception as e:
        logger.error(f"Query error: {e}", exc_info=True)
        return jsonify({
            "error": "Internal server error",
            "details": str(e)
        }), 500


@app.route('/api/conversations', methods=['GET'])
def get_conversations():
    """Get all conversations."""
    return jsonify(conversations)


@app.route('/api/conversation/<int:conversation_id>', methods=['GET'])
def get_conversation(conversation_id):
    """Get a specific conversation by ID."""
    for conv in conversations:
        if conv['id'] == conversation_id:
            return jsonify(conv)
    return jsonify({"error": "Conversation not found"}), 404


def build_context(depth: int) -> str:
    """
    Build context string from last N conversations.

    Format:
    user -> query text
    assistant -> response text
    """
    if not conversations:
        return ""

    # Get last N conversations
    recent = conversations[-depth:] if len(conversations) > depth else conversations

    context_parts = []
    for conv in recent:
        context_parts.append(f"user -> {conv['user_query']}")
        # Truncate assistant response to first 200 chars for context
        response_snippet = conv['assistant_response'][:200]
        if len(conv['assistant_response']) > 200:
            response_snippet += "..."
        context_parts.append(f"assistant -> {response_snippet}")

    return "\n".join(context_parts)


@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "conversations_count": len(conversations),
        "rag_service": RAG_SERVICE_URL
    })


if __name__ == '__main__':
    logger.info("🚀 Starting RAG Frontend Service...")
    logger.info(f"📡 RAG Service: {RAG_SERVICE_URL}")
    app.run(host='0.0.0.0', port=5000, debug=True)
