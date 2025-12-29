#!/usr/bin/env python3
"""
Test batch producer for UN Guiding Principles sources.
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
        session_id = f"ungp-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    messages = []
    
    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n📋 Processing UN Guiding Principles citations:")
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
        'client.id': 'ungp-batch-producer',
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
    # Citations from user - UN Guiding Principles sources
    citations = [
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQG-9n5exoEv--ByC4yBPYVBC_OPMyqWOPd3gblpzCdJyNsHy-WmOjlM_tl-g2FasnQQ4H9RyjYagFT5WINZByxXNVdMgffykS9DCalNzXJtTzQa7-sMENqz4PLgWir2ayqzkfKUfaIDZS8mCbFxfE2tnuUfVlno6jeblFxNfrQ_dpJ0XkKutybqjbUyjnn8aPH5P5u146w=",
            "title": "shiftproject.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH_J67QVxrjpnt2GamrP41ti-N2_YdEeF8QPBLsw7ZL_cOjBpcpn3EKSxfK8JSKPiWOj0qYW6dJIpiOih3O51-g9slzhgCbXIsnrN27chTeXofywhCibwOX4s4_BT5JLqc4kC4mDd1G2WUeMEUF5hwUMlct-t0MLTcI9997Auvme-DbQruG_uXJAKprVXzKpbRwsPx9Dr4hFODrB22ZANAeAf_-zw==",
            "title": "business-humanrights.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEg6o2HVOZnTQCn_ncbNbQde_rFQ-3JbTuC2psvLINUhy55LEMee96gK0Yq_76QlXetrPLivY4gdbshlG377qaHUQoBMW7Xepn7mkl8eRaGTdKPbwP8oFOFny0hcY8ixsgLMF8dLTTXGTugvPBjqoZ7r3f7mZU9kzdrD3bmDGeNb4lmQmeoO4TGyPWuyRSl-xCRBM5I9sAM0XIqyIltVyEVLfaWfbVJng5OvoM=",
            "title": "respect.international"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFPLzEd2VKvKz2zA0RBLZ36rV2rltE9u5wX33QWM6-KStKJ1lFAdJ8YHv9t9h8_mkkS459eGUVHr4JpnSE3c99KrIEaF9Z4Kzg4VQZMs7xOiA0xU3yNZz8CISCWY-C9ISyognifHqa7qtYc8iXz1EXSfwp0htAAmqKCWWgxbrDy5iTU5DDFoThuDfCg4ejAI6qbsrTOyc5MhBGbYWnMMxfCpTs9P-RtVrhPYSEYf03DhCkACvoeQf9pzAy3acwXXA==",
            "title": "europa.eu"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFtjhs151InWtuGaRjAGlP8Bhr8pD7IzBJJ8YDHZFziQYyIRcZx9KbZhkxEfXsALSimmVYyx_6nbbR7GLhNDMLNEf968CbJ28SUOKymDbC08JP-J4uJaNF8O0W0er1bjeotgXfjxTCYn8UbJoGg6iNQqCPgk0RpQLWsf2Z-U46ihf-YYwE6pN_7fO3AS211_QDtWq0zD0RG",
            "title": "wikipedia.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQE6BynRxuQvVSaL2RTWfJ9IjDZHd-a5FU2rOLNcyEr3zDskYXAlsBxxHm_wOoBHI5J0FsQ34osHOPpPDB1MzT-cp9G6-yoVmtd8ez_BWi02_wG4IgOLXAA8Qms7EKU8gKLM22-9-Eyx_cXTqyUkOdJmNfwyq4lfpBvjmJjGxfIzaa4WX1IbPcu2zgZOON4qVkMOmB9Mtl7uPTtH897JugT16AsSTxgo7aB9ruulNtPqoQ==",
            "title": "ethicaltrade.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHGJudelxzap0gyGqC03ubiqKSVNazijifSsIrq3Kh7lWdwkv9KAVGq8RcpOdLpdEpDLgRrumaZihM2YsGJdcpS3_0JmpGQmk1UjFVmYAXaO2_TQpqE-n2bXdfzJ0ud9HCeHEHjGRMSArrLOzr4",
            "title": "ungpreporting.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEYTpVEcO-CSplMMKTAiIh9edNGCk_lzo3teFTh-Md1SAXMSmWkhTeAWxngYlb-Ae-AMz0FR0PxjAfjDocAxXynzEJZ6YHbAaJm_J96j06ZEiv9RKULVPPbPxPoiEO1x4uVYqnyEMfBwIZdhV9jGmsklMsO21V2iZS4-tvTIvZlM0oqzdy8eNR0syep1RGntLiZA_K7Tya5Oip3-WeGb1EkM-dCKSSDS550HWHk6R_f1-qpTSlipThu3A==",
            "title": "business-humanrights.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQHjjoG_dw1aEPPTXvzsKpr0gLbAhoWfCXGdIYd2kmNjPMm2ap2smA7p9KTNO7ytuRjsY7B_XE13vzR7mVXmgEo4-QOgA4_Cu2A3OJUv8sHQb_T_fhT79zREhVspd1lPoVs6JnTH4lW4HS3q9bagRBklbelhppjGs_sDfzZ-BSaKaI3JI4YNd2d7n3w=",
            "title": "ohchr.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEjd7pPrBwlmePsyy7MKuyC2fuTMwmGXFb8l6lSsbYlQy-Rg1bcoNr8hPPBsFl8esml9Rv_UKSqzzYc71CzcwyodsSrBgj9CGe9A8GN_vdlikic3k6OnmkkO_OYnNY-y8s8hYcZ4Q6gAEH9hE0qkfHoYfq3xp4_SMNI87-oZw==",
            "title": "ituc-csi.org"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEWxNu2ClC1LKLI-oaD5jAldumQeOlXnNlRpNhxD2ix6c1eNrqxaqBZCc14HyKSdk1QirP8Vm79wKU99tltjzoELlV9S3wzNgVhDLcjlQxNweg0dfG9pG6JeMookrQ6OQ8M4X_DYlEA9UYy3QwlgBgv-WdAZocVyBxQ-t7hjMZRCcu4y0lATGY0z8Ae9ZLKfFZw3xpLFBEhWaFFSisukfuJaai4Dhz-5i03MasuqEf-Ms3KExegOHJVGsHmGumW42WfJo_G",
            "title": "unl.pt"
        },
        {
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFp-soGIOVvafLlGkWSeOgXl0XshUiuzqcxdQuUcWXDRDF5P8B-yvIOtK0uD-bfusxJyfdEMiznoWTtejnPAUYDfETJ0V787LT-7lR2KmBaU_hAcLr5jhecjTmRvhkIC0taPqAfVgiW846UO-juNeI8n49twRTYNjr7_59lYpeGlUoHhCiLUhwciTHVNOyeKXMXeVE20Zt6Agg36C_p0KzJgn7q7Vnf",
            "title": "business-humanrights.org"
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
