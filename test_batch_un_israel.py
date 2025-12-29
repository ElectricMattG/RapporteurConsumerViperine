#!/usr/bin/env python3
"""
Test batch producer for UN and Israel sources.
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
        session_id = f"un-israel-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    messages = []
    
    # Create timestamp for request IDs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    print(f"\n📋 Processing citations:")
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
        'client.id': 'un-israel-batch-producer',
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
    # Citations from user
    citations = [
        {
            "title": "un.org",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQEhu419fHKP49HFp8OHmarTRPbUFBSKRHbvNGl6ZM6mI1ALUvVBmhEINTuUAr_mCYtIMWN5TywguM7AnL118lLT62P61abXPPrvEkowC5MUhs4UGkzw4cA4TeMvRZtv_lr5Mlm5BR5n9znlNt6tWD4tQ4Hf8CHBBozibdzBrSXrR7fFXoSCzvMjyE8wACN8olP-jWU6ccyXpqerWU0SCw==",
            "text_preview": "No text"
        },
        {
            "title": "ohchr.org",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGJGg52qFkR2v3HdCkJTejJD760T5419NG36YlfK5MW4p6O_lvR7OyMN1Y8PstQRcogvpBNgZ0GZeWKluB2gj1Q2SuceX44s5mSYdl1uCmr0ORRz4FfExQM51fUTqOhOP7dDGvXUKAtUkO9abNUdyr_3a3QLOr6F4NoI1gDbxM_d-tCStgtFTg-opZBMRE7dIJ3oKiYl5mO8tdZ0LsqJOzPu3c=",
            "text_preview": "No text"
        },
        {
            "title": "timesofisrael.com",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQH1bLFSp7IgIyeNlClhgKzC6CSdcuRGKad0iryc50w8zRz1GPrT2YoeM3IX-apRUXrLQ_JdKKbIctyAd89N7BUl1QCv8qzGRlaG7EWnpx8LalJWupIVdY1paosMP24g_6Z4nTbl_eSy0AMIwIKtYvFimqMsXb6H1EKDoU7nlOxFhwqWlnU9nNmrB1o57sAxPIf5CTgfv52xT92MOSpHfkXop9-WaBTpBeeFe84=",
            "text_preview": "No text"
        },
        {
            "title": "www.gov.il",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQGdkwhtekUKGpE_-_sCI73IJ5sScCZ2eWPQ3DJQRzvAUgjnRco_0faaYVMXOar6XufB5adDbsIAjQCsHnRmlfzrLruIWlCjkysvKaDN7Wqln7H1bIXhAZYdpDVb3CxBUTe0GNJtVEnQ32nNFdm9XjROQlgc-aKexi0ITEg=",
            "text_preview": "No text"
        },
        {
            "title": "timesofisrael.com",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFNnJ_JFUXZGpac2PycyU2lGVRf7ERCFwIjWoAxZKJBd6u4RtY_J5PQv0keWPxw8vG9PXJc4TlhxXrNvpVcIKQsiKxPmvocmyG7-tkofvCUYFAlYsWGja3C72JGDu1vNbo0SdfLNCAh6zdJlyylQkuyVhj0jBBbZPtWTsn-hPPWLa7LwehehD27ssIuA9e9mJGoyJXUj3e-FqX0b-yvQw2W1KtqGkbw4A==",
            "text_preview": "No text"
        },
        {
            "title": "youtube.com",
            "uri": "https://vertexaisearch.cloud.google.com/grounding-api-redirect/AUZIYQFwG5Rffq7j2Vi_ggiBShqvaHgpAT3qkUaj3gmFJgpJXB9tuaUApucUgPJLZyyQY3Gxc3iUIzNHsaqtpqAvGaa9EPdIP98bHJOYKRaMQ6U7xczJa22fV-DkLtKUFj8R-xyvMDfGVeU=",
            "text_preview": "No text"
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
