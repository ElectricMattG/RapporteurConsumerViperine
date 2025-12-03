#!/usr/bin/env python3
"""
Run Kafka Citation Extractor Consumer

Entry point for the citation extraction consumer service.
"""

import sys

from kafka_citation_extractor import main

if __name__ == "__main__":
    sys.exit(main())
