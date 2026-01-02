#!/usr/bin/env python3
"""
Test batch producer from RAG Service response.
Creates batch from grounding citations for October 21-28, 2023.
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List

from confluent_kafka import Producer


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

    print(f"\n📋 Processing RAG response:")
    print(f"   Session ID: {session_id}")
    print(f"   Query: {query[:100]}...")
    print(f"   Citations found: {len(citations)}")

    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Process each citation
    for idx, citation in enumerate(citations):
        # Extract URL - handle both direct URL and grounding redirect
        url = citation.get("uri", "")

        # Skip if no URL
        if not url:
            print(f"   ⚠️  Skipping citation {idx+1}: no URI found")
            continue

        # Extract domain for display
        from urllib.parse import urlparse
        try:
            domain = urlparse(url).netloc
        except:
            domain = "unknown"

        # Create message
        message = {
            "session_id": session_id,
            "request_id": f"req_{timestamp}_{idx:03d}",
            "uri": url,  # Changed from "url" to "uri" to match web scraper expectations
            "title": citation.get("title", domain),
            "total_requests": len(citations),  # Total number of requests in this session
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


def send_batch_messages(messages: List[Dict], bootstrap_servers: str = "localhost:9092"):
    """
    Send batch of messages to Kafka web-scrape-requests topic.

    Args:
        messages: List of message dictionaries
        bootstrap_servers: Kafka bootstrap servers
    """
    if not messages:
        print("❌ No messages to send")
        return

    # Create Kafka producer
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'rag-batch-producer',
        'acks': 'all',
        'retries': 3,
    }

    producer = Producer(producer_config)

    session_id = messages[0]['session_id']

    print(f"\n📤 Sending batch of {len(messages)} messages to Kafka")
    print(f"Session ID: {session_id}")

    # Get query from first message's rag_metadata
    query = messages[0].get('rag_metadata', {}).get('query', '')
    if query:
        print(f"RAG Query: {query[:100]}...")

    # Send each message
    success_count = 0
    failed_count = 0

    for idx, message in enumerate(messages, 1):
        try:
            # Extract domain for logging
            from urllib.parse import urlparse
            domain = urlparse(message['uri']).netloc or "unknown"

            # Serialize message
            message_json = json.dumps(message, ensure_ascii=False)

            # Send to Kafka
            producer.produce(
                topic='web-scrape-requests',
                key=message['request_id'],
                value=message_json.encode('utf-8'),
            )

            print(f"✅ Message {idx}/{len(messages)} queued: {domain}")
            success_count += 1

        except Exception as e:
            print(f"❌ Failed to send message {idx}: {e}")
            failed_count += 1

    # Flush to ensure all messages are sent
    print("\n⏳ Flushing messages...")
    producer.flush(timeout=30)

    # Send session complete signal
    print(f"🏁 Session complete signal sent for: {session_id}")
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

    print(f"\n📊 Batch Summary: {success_count} sent successfully, {failed_count} failed")


def main():
    """Main entry point."""
    # RAG response from user (October 21-28, 2023)
    rag_response = {
        "session_id": f"sess_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "request_id": "65f2862308fa59afc0e56d37053fdd69",
        "query": "Get the news from the Israeli PM office 21 October 2023 - 28 October 2023 include any YouTube video from Israeli PM",
        "answer_preview": "## News from the Israeli Prime Minister's Office (October 21-28, 2023)\n\nDuring the week of October 21 to October 28, 2023, the primary focus of the Israeli Prime Minister's Office was the ongoing war with Hamas, which had begun on October 7. Key activities included public addresses by Prime Minister Benjamin Netanyahu, meetings with the families of hostages, and decisions regarding the expansion of military operations.\n\nOn Wednesday, October 25, Prime Minister Netanyahu delivered a public statement affirming the two main goals of the war: the elimination of Hamas's military and governing capabilities, and the return of all captives. [www.gov.il](https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEq6Tzk2qEVtfIO6bdBz71cuMTtgAdgpDEAdk4vCsMRX4H82bMgJHi03kJ_K7JFMtbrtTP0IxUrtCm",
        "total_results": 3,
        "processing_time_ms": 14856.473207473755,
        "grounding_metadata": {
            "search_queries": None,
            "citations_count": 8,
            "citations": [
                {
                    "title": "www.gov.il",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEq6Tzk2qEVtfIO6bdBz71cuMTtgAdgpDEAdk4vCsMRX4H82bMgJHi03kJ_K7JFMtbrtTP0IxUrtCmYRYfW6Vdaj3e_0bOCmZx9dtdVHb6mb8vS7ryOG1aHox7cz8GXrH_h_y4JGaVQQywijV4=",
                    "text_preview": "N/A"
                },
                {
                    "title": "www.gov.il",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQG6mwgF1PTLAyS3DxEis6ShPNPodgSoxi4miq7z4fsqBKc6DKo-zBrwr59tUmFhRYmrtxETK5whfdnnDIqZqj9ffXW1Z1-yk-x5_vCgL7tXHHyXEMAF_9VIge1esH1riflPXORlSBFDWc0PG36bmTaWdHqrAzRY1uKhCBwn",
                    "text_preview": "N/A"
                },
                {
                    "title": "timesofisrael.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEfbyLCzt72j8Qg9N_dMyFYAc9SDF9pmPwlLhcdpENTHTv2ca1A8-W1z7HvDA3jUyJubPhqO9TeKRzmkkLVP7GGxwmTBoVJcTggUOPgTyl5uiZhufXnbKKkeLcwUfSpQ3COo1psR_AWon61weMHz3QkUNpLxDIE37La5-ese22KaKYLP9Sfhob0nqooBlj9KUtjX43T1usRWsejAiN_rxROnTk1ivFoYw==",
                    "text_preview": "N/A"
                },
                {
                    "title": "aljazeera.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGcdes0DXbJpHwt_KEKqNV3SveSGF-mK-jiUQbh5TTnZW8GFk_gMilMB1k-ZrfvKKedmYcqpDohZME7bewdFd_F1a9kX5T6REs6XRo5vYz0WpigpRN2J8xodiAdf-zmpkFF88DvoPoRwlOzEAML1DeCxL3PWONWSD1mVsUVSxYzDpchm7kXQHhyOgsDFwTaIg594rg2oSLaVsBBDtytd5K0D0YFx__m",
                    "text_preview": "N/A"
                },
                {
                    "title": "theguardian.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHwS5z7pLMiA3xL74Nr2YssbktSCxBT3hRkSWwIxsrRJ8I1yYNaPM5LlO8bzv9223dLm6oSLm-rvwcUhQjyAyOyFWn9QIzmYoWC9yNwJk7s9Dom3iLdZJ0UPs9x1_wVgi5Upk27a-tpD3WqcPPbze4qFQeuiwtpOKyT3Dxy1pr0VBk6fICG2eSh0ISDb-5fRwB0lVdMxNLTP3ed_E5BXw-PoHuCo0TkK6n9IEOvA4s6TMk=",
                    "text_preview": "N/A"
                },
                {
                    "title": "timesofisrael.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFyO3ZKC3V7gEsScKa6jB_w6gac6ntwOkxKrSI27wWg6Dq9K9XcsMQTIb8L4QdGcqyyXiRlKqFeasG0Akt4oroL0_jmkBPEBh2eeqr_Fbv_kv18srKhBZd_6ryEZ6Zi1zM3jnx8K3jaauUsF9GBLAoqegPBa1MHLTNVvJt5HfbpvOKr6gzKsOIUkeyvfm1Vw0kVL4_6c9lr1NE2Lf2TrW7hY3Br4Q==",
                    "text_preview": "N/A"
                },
                {
                    "title": "jpost.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHcbkTxj_dlMn-DI_FwmkLWzEIAY0z6rgto8RuZQ9tG6jHPI4pU55g2gcnhw1Sv0sQ_di4KA2fPzmwx3cdPSTQGhCxhlQk9oChPZDaJr1ANL0XKf6gL1aMKXg--lGdIWeuNzslv9ab03vdG-QRRwlswhBAZTNQF9v6yTwWlpbY=",
                    "text_preview": "N/A"
                },
                {
                    "title": "timesofisrael.com",
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFmGGTYxS2SSYeHOgXWIWqGpjFQy3L6IGyoK2wI_d66noPr-09ATUc2VFxOxn92JNDJQGtw4klEow7DVdYujt0-XhFbGpz5MZnTvXDjIOvsWwDepAYUhbzH1XdsLx5T8O5sPDO1q0yXx_kdPvBOF05GRhQ=",
                    "text_preview": "N/A"
                }
            ]
        }
    }

    # Create messages from RAG response
    messages = create_messages_from_rag_response(rag_response)

    if not messages:
        print("\n❌ No valid messages created from RAG response")
        sys.exit(1)

    # Get Kafka bootstrap servers from environment or use default
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')

    # Send messages to Kafka
    send_batch_messages(messages, bootstrap_servers)

    print("\n✅ Batch sent successfully!")
    print(f"   Session ID: {rag_response['session_id']}")
    print(f"   Total messages: {len(messages)}")


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
