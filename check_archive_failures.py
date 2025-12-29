#!/usr/bin/env python3
"""Check archive files for extraction failures."""

import json
import os
from azure.storage.blob import BlobServiceClient

# Get connection string
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connection_string:
    # Try loading from .env
    from dotenv import load_dotenv
    load_dotenv()
    connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def check_session_archive(session_id):
    """Check archive for extraction failures."""
    print(f"\n{'='*80}")
    print(f"📦 Checking archive for: {session_id}")
    print('='*80)
    
    container_client = blob_service_client.get_container_client("research-archive")
    
    # List blobs in session archive
    prefix = f"archive/{session_id}/"
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    
    print(f"\nTotal files in archive: {len(blobs)}")
    
    # Check for extraction failures
    failures = []
    for blob in blobs:
        if 'extraction_failure' in blob.name:
            failures.append(blob.name)
    
    if failures:
        print(f"\n⚠️  Found {len(failures)} extraction failures:")
        for failure in failures:
            # Extract request ID from filename
            parts = failure.split('/')
            filename = parts[-1]
            print(f"  - {filename}")
            
            # Download and show failure details
            blob_client = container_client.get_blob_client(failure)
            data = blob_client.download_blob().readall()
            try:
                failure_data = json.loads(data)
                print(f"    URL: {failure_data.get('url', 'N/A')}")
                print(f"    Error: {failure_data.get('error', 'N/A')}")
                print(f"    Status: {failure_data.get('status_code', 'N/A')}")
            except:
                print(f"    Raw: {data[:200].decode('utf-8', errors='ignore')}")
    else:
        print("\n✅ No extraction failures found")
    
    # Show all archive files
    print(f"\n📄 Archive files:")
    gov_il_files = []
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        if 'gov.il' in filename.lower() or 'gov_il' in filename.lower():
            gov_il_files.append(filename)
            print(f"  🔍 {filename}")
    
    if not gov_il_files:
        print("  (No gov.il files found - checking all files)")
        for blob in blobs[:20]:  # Show first 20
            filename = blob.name.split('/')[-1]
            print(f"  - {filename}")

# Check both sessions
check_session_archive("sess_e0fc67c87f0lc")
check_session_archive("external_test_20251218_171758")

print("\n" + "="*80)
print("🔍 SUMMARY")
print("="*80)
print("\nIf www.gov.il has different chunk counts, check if extraction_failure files exist.")
print("Different chunk counts could be due to:")
print("  1. 403 Forbidden errors (rate limiting)")
print("  2. Timeout errors")
print("  3. Different response content (dynamic pages)")
print("  4. Connection failures")
