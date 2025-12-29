#!/usr/bin/env python3
import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service = BlobServiceClient.from_connection_string(connection_string)
container = blob_service.get_container_client('config')

# Check a few files
files_to_check = [
    'indices/israel_prime_minister/israel_pm_20230901_20230915.json',
    'indices/israel_prime_minister/israel_pm_20251219_20260102.json',
]

for blob_name in files_to_check:
    print(f'\n{"="*80}')
    print(f'File: {blob_name}')
    print("="*80)
    
    blob = container.get_blob_client(blob_name)
    raw_data = blob.download_blob().readall()
    
    print(f'Size: {len(raw_data)} bytes')
    print(f'\nRaw content (first 1000 chars):')
    print(raw_data[:1000].decode('utf-8'))
    
    # Try to parse as JSON
    try:
        data = json.loads(raw_data)
        print(f'\nJSON keys: {list(data.keys())}')
        print(f'JSON structure preview:')
        print(json.dumps(data, indent=2)[:500])
    except Exception as e:
        print(f'\nError parsing JSON: {e}')
