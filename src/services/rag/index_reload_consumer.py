#!/usr/bin/env python3
"""
Index Reload Consumer for RAG Service

Listens to reload-index Kafka topic and triggers index reload without restarting the service.
"""

import asyncio
import json
import logging
import os
import threading
from typing import Callable

from confluent_kafka import Consumer, KafkaError

logger = logging.getLogger(__name__)


class IndexReloadConsumer:
    """Kafka consumer for index reload messages"""

    def __init__(self, reload_callback: Callable, bootstrap_servers: str = "localhost:9092"):
        """
        Initialize the reload consumer.

        Args:
            reload_callback: Async function to call when reload message received
            bootstrap_servers: Kafka bootstrap servers
        """
        self.reload_callback = reload_callback
        self.bootstrap_servers = bootstrap_servers
        self.running = False
        self.consumer = None
        self.thread = None

    def start(self):
        """Start the consumer in a background thread"""
        if self.running:
            logger.warning("Index reload consumer already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.thread.start()
        logger.info("🔄 Index reload consumer started")

    def stop(self):
        """Stop the consumer"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("🛑 Index reload consumer stopped")

    def _consume_loop(self):
        """Main consumer loop (runs in background thread)"""
        try:
            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': 'rag-service-reload',
                'auto.offset.reset': 'latest',  # Only process new messages
                'enable.auto.commit': True
            }

            self.consumer = Consumer(config)
            self.consumer.subscribe(['reload-index'])

            logger.info(f"✅ Subscribed to reload-index topic")

            while self.running:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    action = message_data.get('action')

                    if action == 'reload_index':
                        new_count = message_data.get('new_summaries', 0)
                        total_count = message_data.get('total_summaries', 0)

                        logger.info(f"📥 Received reload-index message: +{new_count} summaries (total: {total_count})")

                        # Call the reload callback in a thread-safe way
                        # Use asyncio to run the async callback from sync thread
                        asyncio.run(self.reload_callback())

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing reload message: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"Fatal error in reload consumer: {e}", exc_info=True)
        finally:
            if self.consumer:
                self.consumer.close()
