#!/usr/bin/env python3
"""
Dead Letter Queue Consumer Runner

Starts the dead letter queue consumer that monitors pipeline failures.
"""

from kafka_dead_letter_consumer import main

if __name__ == '__main__':
    main()
