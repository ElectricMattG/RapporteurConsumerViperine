#!/usr/bin/env python3
"""
Run Kafka Chunk Embedder Consumer

Entry point for the chunk embedding consumer service.
"""

import sys

from kafka_chunk_embedder import main

if __name__ == "__main__":
    sys.exit(main())
