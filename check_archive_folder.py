#!/usr/bin/env python3
"""List archive folder contents in Azure Storage."""

import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connection_string:
    print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
    exit(1)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Check all containers for "archive" folder/prefix
print("\n🔍 Searching for 'archive' folders in all containers:\n")

containers = list(blob_service_client.list_containers())
for container in containers:
    print(f"📦 Container: {container['name']}")
    
    container_client = blob_service_client.get_container_client(container['name'])
    
    # List all blobs to see structure
    blobs = list(container_client.list_blobs())
    
    # Filter for archive-related paths
    archive_blobs = [b for b in blobs if 'archive' in b.name.lower()]
    
    if archive_blobs:
        print(f"  ✅ Found {len(archive_blobs)} archive-related blobs:")
        for blob in archive_blobs[:20]:  # Show first 20
            size_mb = blob.size / (1024 * 1024)
            print(f"    📄 {blob.name}")
            print(f"       Size: {size_mb:.2f} MB")
    else:
        # Show all blob paths to see structure
        if blobs:
            print(f"  📋 All blobs ({len(blobs)}):")
            for blob in blobs[:10]:  # Show first 10
                print(f"    - {blob.name}")
        else:
            print(f"  (empty)")
    print()
