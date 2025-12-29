#!/usr/bin/env python3
"""Verify summary files are present in Azure archive"""
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client('archive')

session_id = 'external_test_updated_ii_20251218_233143'
print(f'\n🔍 Double-checking summary files in {session_id}:\n')

blobs = list(container_client.list_blobs(name_starts_with=f'{session_id}/'))

if not blobs:
    print('❌ No files found - session may still be processing')
else:
    print(f'📊 Total files in archive: {len(blobs)}\n')
    
    # Group by request
    requests = {}
    for blob in blobs:
        parts = blob.name.split('/')
        if len(parts) >= 2:
            request_id = parts[1]
            filename = parts[-1]
            if request_id not in requests:
                requests[request_id] = []
            requests[request_id].append((filename, blob.size / 1024))
    
    print(f'📁 Requests processed: {len(requests)}\n')
    
    # Check each request for summary files
    summary_json_count = 0
    summary_md_count = 0
    for request_id in sorted(requests.keys())[:3]:  # Show first 3 requests
        files = requests[request_id]
        summary_files = [f for f in files if 'summary.' in f[0].lower()]
        
        print(f'Request: {request_id}')
        if summary_files:
            for filename, size_kb in summary_files:
                print(f'  ✅ {filename} ({size_kb:.2f} KB)')
                if filename == 'summary.json':
                    summary_json_count += 1
                elif filename == 'summary.md':
                    summary_md_count += 1
        else:
            print(f'  ❌ No summary files')
        print()
    
    if len(requests) > 3:
        # Count remaining
        for request_id in sorted(requests.keys())[3:]:
            files = requests[request_id]
            for filename, _ in files:
                if filename == 'summary.json':
                    summary_json_count += 1
                elif filename == 'summary.md':
                    summary_md_count += 1
        print(f'... and {len(requests) - 3} more requests\n')
    
    print(f'📊 Summary:')
    print(f'   ✅ {summary_json_count} summary.json files')
    print(f'   ✅ {summary_md_count} summary.md files')
    print(f'   📁 {len(requests)} total requests')
    
    if summary_json_count > 0 and summary_md_count > 0:
        print(f'\n✅ SUCCESS: Summary files are being uploaded to Azure archive!')
    else:
        print(f'\n⚠️  WARNING: Some or all summary files are missing')
