#!/usr/bin/env python3
"""
Runner script for Kafka Q&A Embedder Consumer

Usage:
    python run_kafka_qa_embedder.py

    # Or run in background
    python run_kafka_qa_embedder.py > /tmp/qa_embedder.log 2>&1 &
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from consumers.kafka_qa_embedder import main

if __name__ == "__main__":
    main()
