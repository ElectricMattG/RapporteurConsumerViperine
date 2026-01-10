#!/usr/bin/env python3
"""
Simple Flask server for RAG Frontend UI.

Serves static HTML/JS/CSS and proxies requests to the RAG service.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime
from queue import Empty, Queue

import requests
from confluent_kafka import Consumer, KafkaError
from flask import Flask, Response, jsonify, render_template, request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# RAG service configuration
RAG_SERVICE_URL = os.environ.get("RAG_SERVICE_URL", "http://localhost:8000")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# In-memory conversation storage (persists during runtime)
conversations = []
current_conversation_id = 0

# List of SSE client queues for broadcasting notifications
sse_clients = []


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
        mode = data.get('mode', 'ask')
        context_depth = data.get('context_depth', 1)
        top_k = data.get('top_k', 3)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        temperature = data.get('temperature', 0.7)
        max_tokens = data.get('max_tokens', 8000)

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
            "mode": mode,
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
            "assistant_response": rag_response.get('llm_response', '') or rag_response.get('answer', ''),
            "retrieved_docs": rag_response.get('retrieved_docs', []),
            "metadata": {
                "request_id": rag_response.get('request_id'),
                "session_id": rag_response.get('session_id'),
                "mode": mode,
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


@app.route('/api/notifications')
def notifications():
    """Server-Sent Events endpoint for index update notifications."""
    def event_stream():
        # Create a queue for this client
        client_queue = Queue()
        sse_clients.append(client_queue)
        logger.info(f"📡 SSE client connected. Total clients: {len(sse_clients)}")

        try:
            # Send initial connection confirmation
            yield f"data: {json.dumps({'type': 'connected', 'timestamp': datetime.now().isoformat()})}\n\n"

            # Stream notifications from this client's queue
            while True:
                try:
                    # Block until a notification is available (with timeout for heartbeat)
                    message = client_queue.get(timeout=15)
                    yield f"data: {json.dumps(message)}\n\n"
                except Empty:
                    # Send heartbeat as data event to keep connection alive
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now().isoformat()})}\n\n"
        except GeneratorExit:
            logger.info("🔌 GeneratorExit: Client closed connection")
        except Exception as e:
            logger.error(f"❌ SSE stream error: {e}", exc_info=True)
        finally:
            # Remove this client's queue when connection closes
            if client_queue in sse_clients:
                sse_clients.remove(client_queue)
            logger.info(f"📡 SSE client disconnected. Total clients: {len(sse_clients)}")

    response = Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache, no-transform',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive',
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Access-Control-Allow-Origin': '*'
        }
    )
    response.direct_passthrough = False
    return response


def consume_kafka_notifications():
    """Background thread to consume frontend-message Kafka topic and push to SSE queue."""
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'frontend-notification-consumer',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe(['frontend-message'])

    logger.info(f"🔔 Kafka notification consumer started: {KAFKA_BOOTSTRAP_SERVERS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

            try:
                # Parse the message
                message_data = json.loads(msg.value().decode('utf-8'))
                logger.info(f"📬 Received notification: {message_data}")

                # Broadcast to all connected SSE clients
                notification_event = {
                    'type': 'index_update',
                    'new_summaries': message_data.get('new_summaries_count', 0),
                    'total_summaries': message_data.get('total_summaries_count', 0),
                    'preview': message_data.get('new_summaries', []),
                    'timestamp': message_data.get('timestamp', datetime.now().isoformat())
                }

                # Send to all connected clients
                logger.info(f"📡 Broadcasting to {len(sse_clients)} SSE client(s)")
                for client_queue in sse_clients:
                    try:
                        client_queue.put_nowait(notification_event)
                        logger.info(f"✅ Queued notification for client")
                    except Exception as e:
                        logger.warning(f"Failed to queue for client: {e}")

            except Exception as e:
                logger.error(f"Error processing notification: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)
    finally:
        consumer.close()


if __name__ == '__main__':
    logger.info("🚀 Starting RAG Frontend Service...")
    logger.info(f"📡 RAG Service: {RAG_SERVICE_URL}")
    logger.info(f"📨 Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")

    # Start Kafka notification consumer in background thread
    try:
        kafka_thread = threading.Thread(target=consume_kafka_notifications, daemon=True)
        kafka_thread.start()
        logger.info("✅ Kafka notification consumer thread started")
    except Exception as e:
        logger.warning(f"⚠️ Could not start Kafka consumer: {e}")

    # SSE works with Flask's built-in server using data event heartbeats
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
