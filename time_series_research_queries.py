#!/usr/bin/env python3
"""
Time Series Research Query Script with Kafka Producer Integration
Runs RAG research queries across multiple time windows starting from Oct 7, 2023.
Each query covers a 2-week period, advancing chronologically.
After each successful query, sends citation URLs to Kafka for web scraping.
"""

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse

import requests
from confluent_kafka import Producer

# Configuration
RAG_API_URL = "https://4.207.24.92/query"
API_KEY = "32d160f623efa11ebd88d34ca0b9f4093eb1f866a640d4b70b0fbafb6c22e398"
OUTPUT_DIR = Path("research_results")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka.rapporteur.ai:9094"
KAFKA_ENABLED = True  # Set to False to disable Kafka publishing

# Time window configuration
START_DATE = datetime(2023, 10, 7)  # October 7, 2023
END_DATE = datetime(2025, 12, 17)   # December 17, 2025
WINDOW_DAYS = 14  # 2 weeks

# Query template
QUERY_TEMPLATE = "Get the latest news from the Israeli Defense Force from {start_date} to {end_date}"

# Request configuration
PROCESSING_MODE = "research"
TOP_K = 3
ENABLE_QUERY_REFINEMENT = False
REQUEST_TIMEOUT = 180  # 3 minutes
DELAY_BETWEEN_WINDOWS = 90  # 90 seconds between each window/batch


def generate_time_windows(start_date, end_date, window_days):
    """Generate list of (start, end) date tuples for each time window."""
    windows = []
    current_start = start_date

    while current_start < end_date:
        current_end = current_start + timedelta(days=window_days)
        if current_end > end_date:
            current_end = end_date
        windows.append((current_start, current_end))
        current_start = current_end

    return windows


def format_query(start_date, end_date):
    """Format the query with specific date range."""
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")
    return QUERY_TEMPLATE.format(start_date=start_str, end_date=end_str)


def make_research_query(query):
    """Execute a single research query to the RAG service."""
    headers = {
        "Content-Type": "application/json",
        "Host": "rag.rapporteur.ai",
        "X-API-Key": API_KEY
    }

    payload = {
        "query": query,
        "processing_mode": PROCESSING_MODE,
        "enable_query_refinement": ENABLE_QUERY_REFINEMENT,
        "top_k": TOP_K
    }

    try:
        response = requests.post(
            RAG_API_URL,
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
            verify=False  # Skip SSL verification
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e), "query": query}


def save_result(result, start_date, end_date):
    """Save query result to a JSON file."""
    OUTPUT_DIR.mkdir(exist_ok=True)

    filename = f"idf_news_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.json"
    filepath = OUTPUT_DIR / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    return filepath


def summarize_result(result):
    """Generate a brief summary of the query result."""
    if "error" in result:
        return f"❌ Error: {result['error']}"

    answer_preview = result.get("answer", "")[:200].replace("\n", " ")
    processing_time = result.get("processing_time_ms", 0) / 1000
    citations_count = len(result.get("grounding_metadata", {}).get("citations", []))

    return f"✓ {processing_time:.1f}s | {citations_count} citations | {answer_preview}..."


def create_messages_from_rag_response(rag_response: Dict) -> List[Dict]:
    """
    Extract URLs from RAG response grounding citations and create batch messages.

    Args:
        rag_response: RAG service response with grounding_metadata

    Returns:
        List of message dictionaries ready to send to Kafka
    """
    messages = []

    # Extract session and query info
    session_id = rag_response.get("session_id", "unknown_session")
    query = rag_response.get("query", "")

    # Get citations from grounding metadata
    grounding = rag_response.get("grounding_metadata", {})
    citations = grounding.get("citations", [])

    if not citations:
        return messages

    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Process each citation
    for idx, citation in enumerate(citations):
        # Extract URL - handle both direct URL and grounding redirect
        url = citation.get("uri", "")

        # Skip if no URL
        if not url:
            continue

        # Extract domain for display
        try:
            domain = urlparse(url).netloc
        except:
            domain = "unknown"

        # Create message
        message = {
            "session_id": session_id,
            "request_id": f"req_{timestamp}_{idx:03d}",
            "uri": url,
            "title": citation.get("title", domain),
            "total_requests": len(citations),
            "rag_metadata": {
                "query": query,
                "session_id": session_id,
                "request_id": rag_response.get("request_id", ""),
                "citation_index": idx,
                "total_citations": len(citations),
            }
        }

        messages.append(message)

    return messages


def send_batch_to_kafka(rag_response: Dict, bootstrap_servers: str) -> bool:
    """
    Send RAG response citations to Kafka web scraping pipeline.

    Args:
        rag_response: RAG service response with grounding_metadata
        bootstrap_servers: Kafka bootstrap servers

    Returns:
        True if successful, False otherwise
    """
    try:
        # Create messages from RAG response
        messages = create_messages_from_rag_response(rag_response)

        if not messages:
            print("    ⚠️  No citations to send to Kafka")
            return False

        session_id = rag_response.get("session_id", "unknown")
        query = rag_response.get("query", "")[:80]

        print(f"    📤 Sending {len(messages)} citations to Kafka")
        print(f"       Session: {session_id}")
        print(f"       Query: {query}...")

        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'rag-time-series-producer',
            'acks': 'all',
            'retries': 3,
        }

        producer = Producer(producer_config)

        # Send each message
        success_count = 0
        for idx, message in enumerate(messages, 1):
            try:
                domain = urlparse(message['uri']).netloc or "unknown"
                message_json = json.dumps(message, ensure_ascii=False)

                producer.produce(
                    topic='web-scrape-requests',
                    key=message['request_id'],
                    value=message_json.encode('utf-8'),
                )
                success_count += 1

            except Exception as e:
                print(f"       ❌ Failed to queue message {idx}: {e}")

        # Flush messages
        outstanding = producer.flush(timeout=30)

        if outstanding > 0:
            print(f"       ⚠️  {outstanding} messages still outstanding")
            return False

        # Send session complete signal
        session_complete_message = {
            "session_id": session_id,
            "signal": "session_complete",
            "total_requests": len(messages),
            "timestamp": datetime.now().isoformat()
        }

        producer.produce(
            topic='session-complete',
            key=session_id,
            value=json.dumps(session_complete_message).encode('utf-8')
        )
        producer.flush(timeout=10)

        print(f"    ✅ Kafka batch complete: {success_count} messages sent")
        return True

    except Exception as e:
        print(f"    ❌ Kafka error: {e}")
        return False


def main():
    """Main execution function."""
    print("=" * 80)
    print("RAG Time Series Research Query Script with Kafka Integration")
    print("=" * 80)
    print(f"Start Date: {START_DATE.strftime('%Y-%m-%d')}")
    print(f"End Date: {END_DATE.strftime('%Y-%m-%d')}")
    print(f"Window Size: {WINDOW_DAYS} days")
    print(f"Window Delay: {DELAY_BETWEEN_WINDOWS} seconds")
    print(f"Output Directory: {OUTPUT_DIR}")
    print(f"Kafka Enabled: {KAFKA_ENABLED}")
    if KAFKA_ENABLED:
        print(f"Kafka Endpoint: {KAFKA_BOOTSTRAP_SERVERS}")
    print("=" * 80)

    # Generate time windows
    windows = generate_time_windows(START_DATE, END_DATE, WINDOW_DAYS)
    print(f"\nGenerated {len(windows)} time windows\n")

    # Estimate total time
    estimated_hours = (len(windows) * DELAY_BETWEEN_WINDOWS) / 3600
    print(f"⏱️  Estimated total runtime: {estimated_hours:.1f} hours")
    print(f"   (with {DELAY_BETWEEN_WINDOWS}s delay between windows)\n")

    # Process each window
    results_summary = []
    kafka_summary = {"sent": 0, "failed": 0, "total_citations": 0}

    for idx, (start_date, end_date) in enumerate(windows, 1):
        date_range = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        print(f"[{idx}/{len(windows)}] Processing: {date_range}")

        # Format and execute query
        query = format_query(start_date, end_date)
        result = make_research_query(query)

        # Add session_id for Kafka tracking
        if "error" not in result:
            result["session_id"] = f"time_series_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"

        # Save result
        filepath = save_result(result, start_date, end_date)

        # Display summary
        summary = summarize_result(result)
        print(f"    {summary}")
        print(f"    Saved to: {filepath.name}")

        # Send to Kafka if enabled and query was successful
        kafka_sent = False
        if KAFKA_ENABLED and "error" not in result:
            kafka_sent = send_batch_to_kafka(result, KAFKA_BOOTSTRAP_SERVERS)
            if kafka_sent:
                citations_count = len(result.get("grounding_metadata", {}).get("citations", []))
                kafka_summary["sent"] += 1
                kafka_summary["total_citations"] += citations_count
            else:
                kafka_summary["failed"] += 1

        results_summary.append({
            "window": date_range,
            "filepath": str(filepath),
            "success": "error" not in result,
            "kafka_sent": kafka_sent
        })

        # Delay between windows (except for last one)
        if idx < len(windows):
            print(f"    ⏳ Waiting {DELAY_BETWEEN_WINDOWS} seconds before next window...\n")
            time.sleep(DELAY_BETWEEN_WINDOWS)
        else:
            print()

    # Final summary
    print("=" * 80)
    print("EXECUTION SUMMARY")
    print("=" * 80)
    successful = sum(1 for r in results_summary if r["success"])
    failed = len(results_summary) - successful
    print(f"Total Windows: {len(results_summary)}")
    print(f"Successful RAG Queries: {successful}")
    print(f"Failed RAG Queries: {failed}")

    if KAFKA_ENABLED:
        print(f"\n📤 Kafka Publishing:")
        print(f"   Windows sent to Kafka: {kafka_summary['sent']}")
        print(f"   Failed to send: {kafka_summary['failed']}")
        print(f"   Total citations published: {kafka_summary['total_citations']}")

    print(f"\n💾 All results saved to: {OUTPUT_DIR.absolute()}")
    print("=" * 80)


if __name__ == "__main__":
    # Suppress InsecureRequestWarning for self-signed certificates
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    main()
