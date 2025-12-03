#!/usr/bin/env python3
"""
Runner script for Kafka Summarizer Consumer

Starts the summarizer consumer that processes extracted content
and generates LLM-powered summaries.
"""
import os
import sys

# Calculate project root (parent of src/)
current_file = os.path.abspath(__file__)
src_dir = os.path.dirname(os.path.dirname(current_file))
project_root = os.path.dirname(src_dir)

# Add to Python path
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.consumers.kafka_summarizer import KafkaSummarizerConsumer


def main():
    """Main entry point"""
    print("=" * 80)
    print("🤖 Kafka Summarizer Consumer")
    print("=" * 80)
    print()
    print("⏳ Initializing consumer...")

    try:
        consumer = KafkaSummarizerConsumer()
        print()
        print("✅ Consumer initialized successfully!")
        print("👂 Waiting for messages... (Press Ctrl+C to stop)")
        print()

        consumer.start()

    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
