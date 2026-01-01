#!/usr/bin/env python3
"""
Runner script for Kafka Summary Indexer Consumer

Usage:
    python run_kafka_summary_indexer.py
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from consumers.kafka_summary_indexer import main

if __name__ == "__main__":
    main()
