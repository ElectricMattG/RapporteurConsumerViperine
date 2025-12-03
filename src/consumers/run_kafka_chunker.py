#!/usr/bin/env python3
"""
Run Kafka Document Chunker Consumer

Entry point for starting the document chunking Kafka consumer.
"""

import sys
from pathlib import Path

# Add the src directory to the path
src_dir = Path(__file__).parent.parent
sys.path.insert(0, str(src_dir))

from consumers.kafka_document_chunker import main

if __name__ == '__main__':
    main()
