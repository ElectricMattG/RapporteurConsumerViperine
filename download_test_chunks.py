#!/usr/bin/env python3
"""Download test chunked documents from Azure for citation extraction testing."""

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client('archive')

# Download first 2 chunked documents
blobs_to_download = [
    'sess_israel_pm_oct23_test_001/req_20251226_115245_000/chunked_document.jsonl',
    'sess_israel_pm_oct23_test_001/req_20251226_115245_005/chunked_document.jsonl'
]

Path("cache/test_citation").mkdir(parents=True, exist_ok=True)

for blob_name in blobs_to_download:
    blob_client = container_client.get_blob_client(blob_name)
    local_file = Path(f"cache/test_citation/{blob_name.split('/')[-2]}_chunked_document.jsonl")
    
    with open(local_file, 'wb') as f:
        f.write(blob_client.download_blob().readall())
    
    print(f"✅ Downloaded: {local_file}")
    
    # Count chunks
    with open(local_file, 'r') as f:
        chunk_count = sum(1 for line in f if line.strip())
    print(f"   Contains {chunk_count} chunks")

print("\n✅ Download complete")
