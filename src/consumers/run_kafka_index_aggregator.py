#!/usr/bin/env python3
"""
Run Kafka Index Aggregator Consumer

Starts the consumer that aggregates research session documents
into unified research indices in Azure Blob Storage.
"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from consumers.kafka_index_aggregator import main

if __name__ == "__main__":
    print("=" * 80)
    print("🚀 Kafka Index Aggregator Consumer")
    print("=" * 80)
    print()
    print("⏳ Initializing consumer...")

    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
