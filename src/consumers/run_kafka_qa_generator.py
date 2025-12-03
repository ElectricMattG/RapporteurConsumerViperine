#!/usr/bin/env python3
"""
Run Kafka Q&A Pair Generator Consumer

Entry point for the Q&A pair generation consumer service.
"""

import sys

from kafka_qa_generator import main

if __name__ == "__main__":
    sys.exit(main())
