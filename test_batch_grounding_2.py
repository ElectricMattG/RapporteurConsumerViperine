#!/usr/bin/env python3
"""
Test batch producer for additional grounding data.
"""

import json
import os
import sys
from datetime import datetime
from typing import List, Dict
from confluent_kafka import Producer


def create_messages_from_citations(citations: List[Dict], session_id: str = None) -> List[Dict]:
    """
    Extract URLs from citations and create batch messages.
    
    Args:
        citations: List of citation dictionaries with uri and title
        session_id: Optional session ID to group related messages
        
    Returns:
        List of message dictionaries ready to send to Kafka
    """
    if session_id is None:
        session_id = f"grounding-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    messages = []
    
    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n📋 Processing grounding citations:")
    print(f"   Session ID: {session_id}")
    print(f"   Citations found: {len(citations)}")
    
    # Process each citation
    for idx, citation in enumerate(citations):
        url = citation.get("uri", "")
        
        if not url:
            print(f"   ⚠️  Skipping citation {idx+1}: no URI found")
            continue
        
        # Create message
        message = {
            "session_id": session_id,
            "request_id": f"req_{timestamp}_{idx:03d}",
            "uri": url,
            "title": citation.get("title", "unknown"),
            "total_requests": len(citations),
        }
        
        messages.append(message)
        print(f"   ✅ Message {idx+1}: {citation.get('title', 'unknown')}")
    
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
        'client.id': 'grounding-batch-producer',
        'acks': 'all',
        'retries': 3,
    }
    
    producer = Producer(producer_config)
    
    session_id = messages[0]['session_id']
    
    print(f"\n📤 Sending batch of {len(messages)} messages to Kafka")
    print(f"Session ID: {session_id}")
    
    # Send each message
    success_count = 0
    failed_count = 0
    
    for idx, message in enumerate(messages, 1):
        try:
            # Serialize message
            message_json = json.dumps(message, ensure_ascii=False)
            
            # Send to Kafka
            producer.produce(
                topic='web-scrape-requests',
                key=message['request_id'],
                value=message_json.encode('utf-8'),
            )
            
            print(f"✅ Message {idx}/{len(messages)} queued: {message['title']}")
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed to send message {idx}: {e}")
            failed_count += 1
    
    # Flush to ensure all messages are sent
    print("\n⏳ Flushing messages...")
    producer.flush(timeout=30)
    
    # Send session complete signal
    print(f"🏁 Sending session complete signal for: {session_id}")
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
    # Citations from user - additional grounding data
    citations = [
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEjPzvAvX1CB_SOtjuuRGmHnpJvl8bc-637fPOpeEEO3mPLMHq2_Hu-L1ZCKurtr0bxs44cryBGR_Jd9rjMrsRfWcRLbOrxo3tZPyNhnFLrk5V91rhlRtfBjKlB8OG9Gdx3NwdX3uDCKQBzHaiSH5NVwKKdcHcqzLOFSodeFdreCHLxOTq3DzoT3XoCQu917momWlLumD3GaMAAAGe-RGsCzPC49EtIyWYQz7oNG0yOYDV7",
            "title": "ohchr.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFhyF-u0InM5PAvAMw82YikBvOHBxTPZqmD7Qg59dg9M1Zo8R0qly2kJFM1NBKt__BYRctPbXTHMD7hFErGtp6jJAqJcIb7ZpNhAEMCipFED0qjL3Ij5O4ShUXNfFLcjTAwqRAcAnIHux0KDBhgBNhNTH6IEHkiC9L3cHex_MobC5FWJguUS3zslmI46KY8_RI06hWDA-NvKfvjXxof",
            "title": "unwatch.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEJ6TuDjWv8HWtd1QfEz2uipnUir4aFtePlYNZ502dsLEsReB-qOsWt5IlPrvP--oaS_eDuw_Ydz_DpJZYFJV0BPUeyrpli6YX2fqwoapeRwgsc1lhZiM-4zAYF--C6-KKkKWqPlPpwYF7BhBVvoCFz-vF9GlhKtvaCFX3g8aCXbNmXrmOFubNvqRvhJuARncgWPPfW6wKlnagLxHS8UFwQNpuNnYJ3i-9ERoJOIzhntsxhAqyqlB5NXscX2kvpEmMf6taRYHWpzLd3PCKMhLLQ9gEV_TT2ZM6xyg==",
            "title": "un.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFi4LbEgmhoY6Ck5CE8SmxbfnuJagvAbOVVLnJYK2PzfjN9QTvrWdVAZ3qTxpg3VQN4iPYdmjTLQNyGZe79u1yd5c710ixDoTDUj_DZhScadh_hDwXw3ytyBDjcwbYdxBYEXkZ-UZw=",
            "title": "youtube.com"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH204RpDfVH7B5JbovQSKYM_MrF-jrsL6ush4M7CFjeSQ6KQ4F9D6XwG61IQlPU1ApaRhSm4ik73ZnZr0N3VbctqQfGoWbAV5bYfvrodJfFfjXx5oMuLEonIDzzKTjoMrEsm_BD_Fs14VlQW1QgoITNQA-hH70oUz1ZHM-N2hY_T7nUzaviJebTtHX1l4MAEG434Vu1lheBfOtJuzHRjUDKovJ8Ozoqpj1mzCrMxoM=",
            "title": "ohchr.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHVpncxi1mqcsxoet7SmyfSvIuX4hegJ2Rmolt0tZXjCSXZT12VhwyHqcyNCGKRUFB0y3UxgUluv4QmcG1WMFxRAUA69kCEWXyuLExK78k7X-Ck5T-P-PYDSMPJJYFzopjYAhxsWUuzAzNJdelH2MUu2yGXztGYxKWLGfkXHOkMLTd8RL4ijcxgHe-EDkxkceqRVUbBX-RNVq1DA0ERJwwc9wl11Js=",
            "title": "wrp.org.uk"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEKzgFKnGGq_1CFouDoMRXxxFoldqK6d54zkXorQyn-cYUadshVubvJXOv9GPqFAA_sZyTkIfqkQqXcJXGCr3LOoye7sGO2igLaRG2tmqMFBnp85iIDHiEyIt0EY0buxt1_zXhAhKxQf3R7Av5NC3OU0IcRSkiIfGGTu0cHG2Bjy5h39IlaWJBb4rEnuIlVLV1IeOuN91w4G6vI8Xo4QXyH",
            "title": "yenisafak.com"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEpaorLW2s_Fz8w4caqxwNScDy6z884xvflRLy0G1cWXabR-_EEeXRVvVLu-YesfPf3KqJIvooynCoXxvSrXhKePXcjLNuob-fXpYIkm7NBUw7r2GQKh7Gq7tVjpL9D2o2j2eP8vgPndK46R6OSoSmV5pjBjybHtuWDExoe-VAH8DTK_tBrwNfX3Z8EFucqlpmRHku96r4H3VV-6zru_hxjE7M9hMSl4m7V8n0vnnb-ukPiPqCO",
            "title": "aljazeera.com"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGeD-Pre_GvFWy89nRmzv01SugVnwpTV3DMMEocjnscZeXsn5b-07AImY7nCoEY5ae_tmVynKWrpuTsEZYEkzQcTZM_pcslr1AuVEXRe9JYQFawxWga_ZWhp7RWkQh4y9MtEz-TH7o=",
            "title": "youtube.com"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFVHtp-Xdnq2boJpPZxLxzcMFdPDXpbcOWg6psF-SHBn-1xLeqtRRoiTifCIYJAS3Zs-I0xCsn7Tq9OMa2-QYdE-Sgl06_6AmtsIdd1PV0kzJ2lN80SfapRfmaZjUZ7j1qcS402YyD8l92Oty5R6EbU1kNk9WKNyJf0frp5DVgRE6HvyVmqdEUumGA8cITXTmu_99I=",
            "title": "unwatch.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFAN0DTOY1DSOwZtc5AC0-qTaNiB1TzRdTvlOlGugyOkOOKH4Jz42MdxrfIzyHLl1EDMyXJ-qfISOwYxCv38Fr2KSjqCVYWbW4RhqNW_DLm9zYISCC2Kx4-Fv1SikZUy3I-o41MHue2lu5p8-_s-KMwdd_dt3wYheO2Tz8T2VmRXKPSXk4=",
            "title": "cbc.ca"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEUOdd904337C1tmIwWvLdRNdOwwFekNOcFtB7I8AtgUmv0tLpocCILMGaWaAZN1wmHOSLlVMdz3s89SGLuQdVjZmEK2a8b2Yrftif6MGGogEaqNAv6gEgLGb2MHqSH0OZqRVUBhKc9Yu-2iS_H7lk0WqPYaqSc4Q==",
            "title": "youtube.com"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHpXeB_N90UEiUykpQ_FYpinCF3QC5z_LIbMt7BaC5me1MHop3Jm0ah_ierryfKYC7okfs3JxqJaYYNq1f0j4Oc46-JIzkCu1vn6cxT9WkffRs6n5RZI3jFA4RWtaDS_Y_FtzyyTMk=",
            "title": "youtube.com"
        }
    ]
    
    # Create messages from citations
    messages = create_messages_from_citations(citations)
    
    if not messages:
        print("\n❌ No valid messages created")
        sys.exit(1)
    
    # Get Kafka bootstrap servers from environment or use default
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Send messages to Kafka
    send_batch_messages(messages, bootstrap_servers)
    
    print("\n✅ Batch sent successfully!")
    print(f"   Session ID: {messages[0]['session_id']}")
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
