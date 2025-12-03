#!/usr/bin/env python3
"""
Run Kafka Web Scraper Consumer

CLI tool to start the Kafka consumer for web scraping.

Usage:
    python run_kafka_consumer.py

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker addresses (default: localhost:9092)
    KAFKA_SASL_USERNAME: SASL username for authentication
    KAFKA_SASL_PASSWORD: SASL password for authentication
"""

import os
import sys

import click

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.consumers.kafka_web_scraper import KafkaWebScraperConsumer


@click.command()
@click.option('--bootstrap-servers', '-b',
              default=None,
              help='Kafka bootstrap servers (comma-separated)')
@click.option('--topic', '-t',
              default=None,
              help='Kafka topic to consume from')
@click.option('--group-id', '-g',
              default=None,
              help='Consumer group ID')
@click.option('--cache-dir', '-c',
              default=None,
              help='Directory for cached scrapes')
def main(bootstrap_servers, topic, group_id, cache_dir):
    """
    Start the Kafka Web Scraper Consumer

    This consumer will:
    1. Listen for messages on the configured Kafka topic
    2. Resolve redirect URLs to their final destinations
    3. Extract links from the resolved pages
    4. Scrape content from all discovered URLs
    5. Convert HTML to markdown and save to cache

    Press Ctrl+C to stop the consumer gracefully.
    """

    # Set environment variables if provided
    if bootstrap_servers:
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = bootstrap_servers

    # Display startup information
    click.echo("=" * 80)
    click.echo("🚀 Kafka Web Scraper Consumer")
    click.echo("=" * 80)

    if bootstrap_servers:
        click.echo(f"📡 Kafka Servers: {bootstrap_servers}")
    if topic:
        click.echo(f"📬 Topic: {topic}")
    if group_id:
        click.echo(f"👥 Consumer Group: {group_id}")
    if cache_dir:
        click.echo(f"💾 Cache Directory: {cache_dir}")

    click.echo("\n⏳ Initializing consumer...")

    try:
        consumer = KafkaWebScraperConsumer()

        # Override config if command line args provided
        if topic:
            consumer.input_topic = topic
        if group_id:
            consumer.kafka_config['group.id'] = group_id
        if cache_dir:
            from pathlib import Path
            consumer.cache_dir = Path(cache_dir)
            consumer.cache_dir.mkdir(parents=True, exist_ok=True)

        click.echo("\n✅ Consumer initialized successfully!")
        click.echo("👂 Waiting for messages... (Press Ctrl+C to stop)\n")

        consumer.start()

    except KeyboardInterrupt:
        click.echo("\n\n🛑 Received interrupt, shutting down...")
    except Exception as e:
        click.echo(f"\n\n❌ Error: {e}", err=True)
        sys.exit(1)
    finally:
        click.echo("\n✅ Consumer stopped\n")


if __name__ == "__main__":
    main()
