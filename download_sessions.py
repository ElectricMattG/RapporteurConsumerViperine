#!/usr/bin/env python3
"""Download session backup files from Azure Storage for comparison."""

import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connection_string:
    print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
    exit(1)

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client("session-backups")

sessions = [
    "sess_e0fc67c87f0lc",
    "external_test_20251218_171758"
]

files = [
    "chunks_aggregated.jsonl",
    "qa_pairs_aggregated.jsonl",
    "session_metadata.json"
]

print("📥 Downloading session backup files...\n")

for session in sessions:
    print(f"Session: {session}")
    session_dir = f"/tmp/session_compare/{session}"
    
    for filename in files:
        blob_name = f"{session}/{filename}"
        local_path = f"{session_dir}/{filename}"
        
        try:
            blob_client = container_client.get_blob_client(blob_name)
            
            # Download blob
            with open(local_path, "wb") as f:
                download_stream = blob_client.download_blob()
                f.write(download_stream.readall())
            
            # Get file size
            size_mb = os.path.getsize(local_path) / (1024 * 1024)
            print(f"  ✅ {filename}: {size_mb:.2f} MB")
            
        except Exception as e:
            print(f"  ❌ {filename}: {e}")
    
    print()

print("✅ Download complete!")
print(f"\nFiles saved to: /tmp/session_compare/")
