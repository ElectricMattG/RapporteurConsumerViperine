#!/usr/bin/env python3
"""List contents of Azure Storage containers."""

import os

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connection_string:
    print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
    exit(1)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# List all containers
print("\n📦 Azure Storage Containers:\n")
containers = list(blob_service_client.list_containers())
for container in containers:
    print(f"Container: {container['name']}")

    # List blobs in container
    container_client = blob_service_client.get_container_client(container['name'])
    blobs = list(container_client.list_blobs())

    if blobs:
        print(f"  Blobs ({len(blobs)}):")
        for blob in blobs:
            size_mb = blob.size / (1024 * 1024)
            last_modified = blob.last_modified.strftime('%Y-%m-%d %H:%M:%S')
            print(f"    - {blob.name}")
            print(f"      Size: {size_mb:.2f} MB | Modified: {last_modified}")
    else:
        print(f"  (empty)")
    print()
