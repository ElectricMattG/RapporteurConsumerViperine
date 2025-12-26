#!/usr/bin/env python3
"""Upload initial config and state to Azure Blob Storage"""
import json
import os
import sys
from pathlib import Path

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

# Load .env file if it exists
env_file = Path(__file__).parent / '.env'
if env_file.exists():
    print(f"Loading environment from {env_file}")
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value.strip('"')

# Azure config
AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING', 'DefaultEndpointsProtocol=https;AccountName=ragserviceneblob;AccountKey=8j4dykfcCr06fE+xoL6SujpNFsj+aAxvtO+q1vx2I2+aNq1SrOjZK5AzAVNcMXC3K/yZs9FbhqZN+AStlHhJRQ==;EndpointSuffix=core.windows.net')
CONFIG_CONTAINER = 'config'

if not AZURE_CONNECTION_STRING:
    print("❌ AZURE_CONNECTION_STRING not set")
    sys.exit(1)

# Initialize client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

print("=" * 80)
print("Uploading Config to Azure Blob Storage")
print("=" * 80)

# Check and create container if it doesn't exist
print(f"\nChecking container: {CONFIG_CONTAINER}")
try:
    container_client = blob_service_client.get_container_client(CONFIG_CONTAINER)
    # Try to get properties to check if container exists
    container_client.get_container_properties()
    print(f"✓ Container exists: {CONFIG_CONTAINER}")
except ResourceNotFoundError:
    print(f"Creating container: {CONFIG_CONTAINER}...")
    try:
        blob_service_client.create_container(CONFIG_CONTAINER)
        print(f"✅ Created container: {CONFIG_CONTAINER}")
    except Exception as e:
        print(f"❌ Failed to create container: {e}")
        sys.exit(1)
except Exception as e:
    print(f"❌ Error accessing container: {e}")
    sys.exit(1)

# 1. Upload queries.json
print("\n1. Uploading queries.json to config container...")
with open('cronjobs/config/queries.json.example', 'r') as f:
    queries_data = f.read()

blob_client = blob_service_client.get_blob_client(
    container=CONFIG_CONTAINER,
    blob='queries.json'
)
blob_client.upload_blob(queries_data, overwrite=True)
print(f"✅ Uploaded queries.json to {CONFIG_CONTAINER} container")

# 2. Parse queries and create initial state for each
print("\n2. Creating initial state files for each query...")
queries = json.loads(queries_data)

for query in queries['queries']:
    if not query['enabled']:
        print(f"⏭️  Skipping disabled query: {query['query_id']}")
        continue

    # Create initial state (starting from 2023-09-01)
    from datetime import datetime, timedelta

    start_date = datetime.strptime("2023-09-01", "%Y-%m-%d")
    window_size = query.get('window_size_days', 14)
    end_date = start_date + timedelta(days=window_size)

    state = {
        "query_id": query['query_id'],
        "query_template": query['query_template'],
        "window_size_days": window_size,
        "total_windows_processed": 0,
        "next_window_start": start_date.strftime("%Y-%m-%d"),
        "next_window_end": end_date.strftime("%Y-%m-%d"),
        "last_window_start": None,
        "last_window_end": None,
        "last_run_status": None,
        "last_run_citations": 0,
        "last_run_timestamp": None
    }

    # Upload state to config container
    state_blob = f"state/{query['query_id']}.json"
    blob_client = blob_service_client.get_blob_client(
        container=CONFIG_CONTAINER,
        blob=state_blob
    )

    blob_client.upload_blob(
        json.dumps(state, indent=2),
        overwrite=True
    )
    print(f"✅ Created state/{query['query_id']}.json (start: {state['next_window_start']})")

print("\n" + "=" * 80)
print("✅ All config and state files uploaded successfully")
print("=" * 80)
print(f"\nUploaded to:")
print(f"  - {CONFIG_CONTAINER}/queries.json")
print(f"  - {CONFIG_CONTAINER}/state/*.json")
print("\nYou can now run the CronJob - it will:")
print("  1. Download config from 'config' container")
print("  2. Load state from 'config' container")
print("  3. Process the next time window")
print("  4. Save indices to 'research-indices' container")
print("  5. Update state in 'config' container")

print("\n" + "=" * 80)
print("✅ All config files uploaded successfully to 'config' container")
print("=" * 80)
print("\nFiles in config container:")
print("  - queries.json (query configurations)")
print("  - state_<query_id>.json (initial state for each enabled query)")
print("\nYou can now run the CronJob - it will:")
print("  1. Download queries.json from config container")
print("  2. Load state for each enabled query")
print("  3. Process the next time window")
print("  4. Advance the window and save updated state")
