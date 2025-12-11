#!/usr/bin/env python3
"""
Batch Message Producer from RAG Service Response

Sends messages to Kafka based on grounding metadata citations from RAG service.
"""

import json
import os
from datetime import datetime

from confluent_kafka import Producer


def create_messages_from_rag_response(rag_response, session_id=None):
    """
    Create Kafka messages from RAG service grounding metadata.

    Args:
        rag_response: Dict containing grounding_metadata with citations and grounding_supports
        session_id: Optional session ID to group related messages

    Returns:
        List of message dictionaries
    """
    if session_id is None:
        # Use RAG session_id if available, otherwise generate
        session_id = rag_response.get('session_id', f"rag-batch-{datetime.now().strftime('%Y%m%d_%H%M%S')}")

    grounding_metadata = rag_response.get('grounding_metadata', {})
    citations = grounding_metadata.get('citations', [])
    grounding_supports = grounding_metadata.get('grounding_supports', [])

    messages = []

    # Create a message for each citation
    for idx, citation in enumerate(citations):
        # Find all grounding supports that reference this citation
        citation_context = []
        for support in grounding_supports:
            chunk_indices = support.get('grounding_chunk_indices', [])
            if idx in chunk_indices:
                citation_context.append(support.get('segment_text', ''))

        # Create message
        message = {
            "uri": citation['uri'],
            "title": citation['title'],
            "context": citation_context if citation_context else ["No context available"],
            "session_id": session_id,
            "request_id": f"req_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{idx:03d}",
            "total_requests": len(citations),  # Total requests in this session
            "rag_metadata": {
                "query": rag_response.get('query', ''),
                "rag_session_id": rag_response.get('session_id', ''),
                "rag_request_id": rag_response.get('request_id', '')
            }
        }
        messages.append(message)

    return messages


def send_batch_messages(messages, bootstrap_servers='localhost:9092', topic='web-scrape-requests'):
    """Send batch of messages to Kafka"""

    # Configure producer
    config = {
        'bootstrap.servers': bootstrap_servers
    }

    producer = Producer(config)

    print("=" * 80)
    print(f"📤 Sending batch of {len(messages)} messages to Kafka")
    print("=" * 80)
    print(f"Topic: {topic}")
    print(f"Bootstrap servers: {bootstrap_servers}")
    print(f"Session ID: {messages[0]['session_id'] if messages else 'N/A'}")
    if messages and 'rag_metadata' in messages[0]:
        print(f"RAG Query: {messages[0]['rag_metadata']['query'][:80]}...")
    print("=" * 80)

    sent_count = 0
    failed_count = 0

    for idx, message in enumerate(messages, 1):
        try:
            # Serialize and send
            message_bytes = json.dumps(message).encode('utf-8')
            producer.produce(topic, message_bytes)

            print(f"\n✅ Message {idx}/{len(messages)} queued:")
            print(f"   Request ID: {message['request_id']}")
            print(f"   Title: {message['title']}")
            print(f"   URI: {message['uri'][:80]}...")
            print(f"   Context snippets: {len(message['context'])}")

            sent_count += 1

        except Exception as e:
            print(f"\n❌ Message {idx}/{len(messages)} failed: {e}")
            failed_count += 1

    # Flush all messages
    producer.flush()

    # Send session complete signal to trigger immediate aggregation
    if sent_count > 0 and messages:
        session_id = messages[0]['session_id']
        complete_signal = {
            "session_id": session_id,
            "session_complete": True,
            "total_requests": len(messages),
            "request_id": f"{session_id}_complete"
        }
        producer.produce('session-complete', json.dumps(complete_signal).encode('utf-8'))
        producer.flush()
        print(f"\n🏁 Session complete signal sent for: {session_id}")

    print("\n" + "=" * 80)
    print(f"📊 Batch Summary:")
    print(f"   Total messages: {len(messages)}")
    print(f"   Sent successfully: {sent_count}")
    print(f"   Failed: {failed_count}")
    print("=" * 80)

    return sent_count, failed_count


if __name__ == "__main__":
    # RAG Service Response - Israeli PM office news Oct 7-14, 2023
    rag_response = {
        "session_id": "sess_4211f750a1d5",
        "request_id": "d3e3ffc489a9d275898e9f12eac7adf5",
        "query": "Get the news from the Israeli PM office 07 October 2023 - 14 October 2023 include any YouTube video from Israeli PM",
        "grounding_metadata": {
            "citations": [
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGfmtF_MMCkJHBUe0hqAwN3poQ8SS9cWCoC85upzIt9Nywy1GUA-r972W2GzuD5rGP3-dq2gYDu1_RxkvwYp6DHqNTmnoEjcLxYBlb_VGApp0TUT2xUJBLfKt89uVNi2yWnj-BSGag17eGSG45VZLD2rLSLsYA_PP7KCLBglfjDBWof4yYq3r9P0EL29GGFymprIdDtvFv5pxupARCgsPKlXuB9WJzX6ASjg3r6graJLzDmaQPeB112Rs4CTJajowIqnvGC",
                    "title": "indiatoday.in"
                },
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFjplWMAzL52WRqvV6JGN__c5u8EqsNOIr1eU73eqRJy0DcK3dB_gsabcSPlQq437zCzVwLDkckXBE3mvm5VMoAImQx0uHeunP5k9fpd33HuKFU32NwNRDWmMo3zN6px0KHfDHWoIwBEOSa595qiq_Imx4tpwEMn_zxZQKpy9UkiFFxDuB2l9oa8H0BU1pjZHF9C109C64nNIdVFUFmngl2d6yPLhW0Bg==",
                    "title": "timesofisrael.com"
                },
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH1dfGA0CFsnJMCkNQ1BhgzPciaJth07OcDBj9RISilTg5lfb06TOFj5FzUkRIRVwQXJxXDigrOj4uEUaXNudLi8hwXZ7v6FMIcaPJ9b2YKOt8MTFKwSlCN3CAjlT-jL7cyET19HfupjVXNdmPMg7x_JA79rLjW3w==",
                    "title": "www.gov.il"
                },
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGiRyTWyaRJILqXBnsdkjtVV1-MMC5jmzj4PtKpOEEzFOPpWiiLjRHSeemj4pwLdFahQe_fqZpPGFOre28FPlMWv6t9QO2BqbG3V9esNr477YdFniS3XSLeiZAA2TD8N5F42LOVGQhl5RWeazYhEI9woZ_3xK_3POhXhOinAR2ovZGFV9K4iNnd9mlMSXHfrsWANQ3J5hWghxHsmt-Sth2UL0ns3d_tB7VyMwJdIeyVLn0ATs_OnTo=",
                    "title": "indiatoday.in"
                },
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHgXhaAWuAjfFx7z-JxatluCaOM-bcUcNZFCE4VAzo0pfYYElQEkohlpdj6P8QGO6CA-8PhBPtA20wHtfg23YdrLjfKxcDZccPFp1HuBO0wOyl3AZMQbMxbjFw-KrkxsvTGxRFZeVdJWFnUnsfvP0U5CidLhRLUNRvxuH6zCcPdRbg=",
                    "title": "washingtonpost.com"
                },
                {
                    "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHuyCz0EeOMLcHEHhe3t32kDxYRgHibrG7AKTONo6woDzkOzFRkeuXbLVf9lUuYF0yWG77kCvExRiJ1_5XQ385rtqC-FPY3D2Wi62ZEy2ygVkmv2ke_cQIOvD_GeGS5W_k2JEaao8mIztBcjRkqJBdQsUhv3I_yH3vhuYtKvPHm23uRhHn2cKwygCa-3YjcaKXONzShh16QjmOOUOpJkZsZG82HKKZWFanzGGbvQA-pRlTikevvw0b-vBBYmQ==",
                    "title": "alarabiya.net"
                }
            ],
            "grounding_supports": [
                {
                    "segment_text": "On the morning of October 7, Prime Minister Netanyahu announced, \"Citizens of Israel, We are at war, not in an operation or in rounds, but at war.\"",
                    "grounding_chunk_indices": [0, 1],
                    "confidence_score": None
                },
                {
                    "segment_text": "He stated that he had convened the heads of the security establishment and ordered the clearing of communities infiltrated by terrorists, an extensive mobilization of reserves, and a military response of a magnitude the enemy had not known.",
                    "grounding_chunk_indices": [0, 2],
                    "confidence_score": None
                },
                {
                    "segment_text": "Later that evening, Netanyahu delivered another statement, describing the attack as a \"brutal and evil war\" and vowing victory despite the \"unbearable price.\"",
                    "grounding_chunk_indices": [3],
                    "confidence_score": None
                },
                {
                    "segment_text": "He declared that the IDF would use all its strength to destroy Hamas's capabilities and turn their locations into \"rubble,\" urging Gaza residents to leave.",
                    "grounding_chunk_indices": [3],
                    "confidence_score": None
                },
                {
                    "segment_text": "The Prime Minister also emphasized that Hamas was responsible for the well-being of the hostages and that Israel would settle accounts with anyone who harmed them.",
                    "grounding_chunk_indices": [3],
                    "confidence_score": None
                },
                {
                    "segment_text": "On October 9, in an English-language statement, Prime Minister Netanyahu asserted that while Israel did not start the war, it would finish it.",
                    "grounding_chunk_indices": [4],
                    "confidence_score": None
                },
                {
                    "segment_text": "He described the attacks as \"savage\" and \"mindboggling,\" comparing Hamas to ISIS and calling on the \"forces of civilization\" to support Israel in defeating them.",
                    "grounding_chunk_indices": [4],
                    "confidence_score": None
                },
                {
                    "segment_text": "He expressed gratitude for the support from U.S. President Joe Biden and other world leaders.",
                    "grounding_chunk_indices": [4],
                    "confidence_score": None
                },
                {
                    "segment_text": "Towards the end of the week, on October 14, Prime Minister Netanyahu stated in a televised address that Israel would fight its enemies \"without limit\" and that the military's response had \"only just started paying the price.\"",
                    "grounding_chunk_indices": [3, 0],
                    "confidence_score": None
                },
                {
                    "segment_text": "He added, \"I cannot reveal what will happen, but I am telling you this is just the beginning.\"",
                    "grounding_chunk_indices": [3, 0],
                    "confidence_score": None
                },
                {
                    "segment_text": "On the same day, Israel's National Security Chief, Tzachi Hanegbi, admitted to making a mistake in assessing Hamas's intentions and stated that \"complete victory will be the only possible outcome of this battle.\"",
                    "grounding_chunk_indices": [4],
                    "confidence_score": None
                },
                {
                    "segment_text": "He also confirmed that the Israeli military was preparing for a wide range of offensive plans.",
                    "grounding_chunk_indices": [4],
                    "confidence_score": None
                },
                {
                    "segment_text": "Prime Minister Netanyahu also spoke with U.S. President Joe Biden on October 14, telling him that \"unity and determination\" were needed to achieve Israel's combat goals.",
                    "grounding_chunk_indices": [5],
                    "confidence_score": None
                },
                {
                    "segment_text": "During this period, no official videos were released on the Israeli Prime Minister's official YouTube channel. News outlets, however, broadcasted clips of his statements.",
                    "grounding_chunk_indices": [3],
                    "confidence_score": None
                },
                {
                    "segment_text": "Opposition parties in the Knesset, along with protest organizers, announced their full support for the IDF and the government's response, calling for national unity in the face of the attacks.",
                    "grounding_chunk_indices": [1],
                    "confidence_score": None
                }
            ]
        }
    }

    # Get Kafka config from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'web-scrape-requests')

    # Create messages
    messages = create_messages_from_rag_response(rag_response)

    # Send to Kafka
    send_batch_messages(messages, bootstrap_servers, topic)
