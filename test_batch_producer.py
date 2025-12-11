#!/usr/bin/env python3
"""
Batch Message Producer for Web Scraper Consumer

Sends multiple messages to Kafka based on grounding metadata citations.
Creates one message per citation with associated grounding support text.
"""

import json
import os
from datetime import datetime

from confluent_kafka import Producer


def create_messages_from_grounding(grounding_data, session_id=None):
    """
    Create Kafka messages from grounding metadata.

    Args:
        grounding_data: Dict containing citations and grounding_supports
        session_id: Optional session ID to group related messages

    Returns:
        List of message dictionaries
    """
    if session_id is None:
        session_id = f"grounding-batch-{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    citations = grounding_data.get('citations', [])
    grounding_supports = grounding_data.get('grounding_supports', [])

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
            "total_requests": len(citations)  # Total requests in this session
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
    # Sample grounding data from user
    grounding_data = {
        "citations": [
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQG24TbAB1-GI1JUMEOaLEXAJh4WTUKnqzR9XP7fuq4XOmTmVylJW04cW2yj_Ycj-ROphy07pWrqk-YAG0qKTGwDmS8R2LLJI1hf5DQz3cxibXXWwYXfAXtc5F8Pt9ef2wwUGC1Xdrw=",
                "title": "youtube.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFzEmmEW1AaQU3NehJyplXzv8TcgE30lR9Io03b9sUbOiq0sB7akk63AqplG6pf5h0-yEPdGoG3IBxXbqZPs7e26IlHNJVv3YoLV_8BjkfaPEpCaFrJ-uUezG8xpObscXLhBX0c5uk=",
                "title": "youtube.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHDmmh9xEgeIjBrHVS_ZxrmhBKaLPo3kZMnOMFzONnkZySybKC1wx4V7OreUFIHjoGeaT91pMWNoe3H30J9DPl5IAw6eYctg1rJ82iJJEbBpsQanQshr590KlBYjJrg9bY7OK5ZwLFIThaA_hi_4IYtHGw=",
                "title": "timesofisrael.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQG_anz25OEYLdoyE-DVwGEfUY731mbxrUhzfMosizSGdfrr1Q0R6nMpknYjEQ1guuTvSrlWjZCJKfYVo146gDBe2-fVqKmFGtPDKa_C9lKyl6GqW8fzxcWtSqyEjOI1ITGsE9cPGQq9tzfTBOk4BRpxd_Y1n8ZGZ-_VmxvmxrLv8rsNf4T27TgpHQ8I1uwXayVELeLWMVuNQAMNwqJYFT4q8hoQEtLfmioGmBGNDdqN6lE4pDOIkP-t0jF1tSRmtZ10PxTBSnCyjL015i5YCd1hEhw-ke9gOI6Kg8qW1ubhyvIQV35aL6Qjpk5Mig==",
                "title": "longwarjournal.org"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGYIZXIM0IPNM9DHLLH513hRuBgTMxslg0Lvt0zGPOMRsNDD4XiwwCd5kYMv9Lcy3vNSUQdqv66AwFFfld8tV604W47fJw6DwtHIFesRxq_06IIvM7Jh8-E4hV1nU2VpfZR8rctXsMfmxMpnv2YHx5K-WGclYnUhOzeT61Syd3YiKfRHIqcVFhpbLDLoQsT14rlN4BQVG-yvP3auXcMSCbUfQ==",
                "title": "timesofisrael.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH9UT0cJWxRvVP5K5agQTcr8mItimW5EZP8yjgx6vSGJ9ukEoXm9zzN--888NV-IUQfOLSoEhzkDz_d8gGpo2AKDHrYlleZjbgYFmuiSVm3OBML-vZxH-DIk2QFVfdY9xyDDP1T97cZAIFSEtF826AjhxOOPgSNO0M=",
                "title": "jpost.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFUFK2-z9dNVdvyp-x3t5WPOtczFjcwNOajDTE-GtrxvYUOw1JGzkEDHiHewCB_A0dVc3X1qGrHlakI6EDkap5xqGlQuQksd5TiCJE96gwcRbumWnbUL0tpMcRPP3DHgRGU2R4JeI0YHzOZHoeSCVVbQPg=",
                "title": "timesofisrael.com"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEPQt-BHiChvlmYohgUq2_wGUE2VxaHde5SkXJIeEDwMoTLM95EtWOHGW20fzpsLrR6idd2-B4b9-0gLzJC4X2LA03zJbK7maB3-yNjOspN4dex7LApjqLY39hmbvTphvWchYbNZWk7XLz2ellYugZGpZtYzwZcSbXAo64Yh65TXGEVxksJwxuK8sG92_wa2_AcusYVgJ7E2Fo9JQ==",
                "title": "fdd.org"
            },
            {
                "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQF0GMYtnZnpdnP5j-ws7Z1VG9yYDkTmMpIXo-vsSDNgLGsYpUFmkJy6yNUKLPlD1JXBommFQKr9kiR1jUe4BvgbG6PVn4ZKRv3_mkP-hUyc-uwJ8MasAXlnGc7XV69ekspkkVnIPmRIJl7DzfixfSZxRsbhC8CltwU0ljtoiIMVcviWpJssOxm159a-b031lV41y4XePkH0n8Eg4cge8ulfiGKwkxIp0H02ZE2qY2IBsNz1DtOp",
                "title": "timesofisrael.com"
            }
        ],
        "grounding_supports": [
            {
                "segment_text": "On December 2nd, the IDF announced it had eliminated a senior Palestinian Islamic Jihad commander, Allah Alin Abd al-Naser Hassan Kudari, in a targeted strike in Gaza.",
                "grounding_chunk_indices": [0, 1],
                "confidence_score": None
            },
            {
                "segment_text": "The IDF stated this operation was in retaliation for ceasefire violations by Hamas in November and noted that Kudari had participated in the October 7th massacre and continued to direct attacks.",
                "grounding_chunk_indices": [0, 1],
                "confidence_score": None
            },
            {
                "segment_text": "On the same day, IDF troops killed three Palestinian terror operatives in separate incidents in the Gaza Strip who had approached forces in a threatening manner.",
                "grounding_chunk_indices": [2],
                "confidence_score": None
            },
            {
                "segment_text": "Also on December 2nd, there were reports of a stabbing attack near Jerusalem and a car-ramming attempt near Hebron, where the assailant was shot by troops after lightly wounding a soldier.",
                "grounding_chunk_indices": [0, 1],
                "confidence_score": None
            },
            {
                "segment_text": "A major ceasefire violation occurred on December 3rd, when Palestinian terrorists ambushed IDF troops in eastern Rafah after emerging from a tunnel, wounding several soldiers.",
                "grounding_chunk_indices": [3],
                "confidence_score": None
            },
            {
                "segment_text": "Between November 27 and December 3, the IDF reported a total of seven ceasefire violations by Palestinian terror groups.",
                "grounding_chunk_indices": [3],
                "confidence_score": None
            },
            {
                "segment_text": "Eyal Zamir visited the Gaza Strip and referred to this line as \"a new border line\" that serves as a forward defensive position.",
                "grounding_chunk_indices": [4],
                "confidence_score": None
            },
            {
                "segment_text": "During his visit, Zamir emphasized that the mission was not complete until the last hostage's body was returned.",
                "grounding_chunk_indices": [4],
                "confidence_score": None
            },
            {
                "segment_text": "The following day, the IDF struck and killed a Hamas terrorist in central Gaza who was reportedly planning an imminent attack, with the military stating the action was necessary to remove a threat to soldiers and did not violate the ceasefire.",
                "grounding_chunk_indices": [5],
                "confidence_score": None
            },
            {
                "segment_text": "On the northern border, the IDF confirmed carrying out a wave of airstrikes in southern Lebanon on December 9th, targeting Hezbollah sites.",
                "grounding_chunk_indices": [6],
                "confidence_score": None
            },
            {
                "segment_text": "The targets included a training facility for the elite Radwan Force, buildings used by the terror group, and a rocket-launching site.",
                "grounding_chunk_indices": [6],
                "confidence_score": None
            },
            {
                "segment_text": "The IDF stated that these sites constituted a violation of understandings between Israel and Lebanon.",
                "grounding_chunk_indices": [6],
                "confidence_score": None
            },
            {
                "segment_text": "Also during this period, the IDF and Shin Bet revealed intelligence on December 8th about a significant Hamas financial network operating in Turkey, allegedly under Iranian supervision, which funneled hundreds of millions of dollars to the terror group.",
                "grounding_chunk_indices": [7],
                "confidence_score": None
            },
            {
                "segment_text": "This intelligence pointed to Hamas using Turkish banking and commercial infrastructure to fund its activities.",
                "grounding_chunk_indices": [7],
                "confidence_score": None
            },
            {
                "segment_text": "Prime Minister Benjamin Netanyahu expressed that phase two, involving the gradual withdrawal of the IDF and the disarmament of Hamas, would begin once the last hostage's body was returned.",
                "grounding_chunk_indices": [4],
                "confidence_score": None
            },
            {
                "segment_text": "However, he also voiced skepticism about whether an international stabilization force would be capable of disarming the terror group.",
                "grounding_chunk_indices": [8],
                "confidence_score": None
            }
        ]
    }

    # Get Kafka config from environment
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.getenv('KAFKA_TOPIC', 'web-scrape-requests')

    # Create messages
    messages = create_messages_from_grounding(grounding_data)

    # Send to Kafka
    send_batch_messages(messages, bootstrap_servers, topic)
