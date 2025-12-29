#!/usr/bin/env python3
import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service = BlobServiceClient.from_connection_string(connection_string)
container = blob_service.get_container_client('config')

# Download first, middle, and last few files
files_to_check = [
    'indices/israel_prime_minister/israel_pm_20230901_20230915.json',  # First
    'indices/israel_prime_minister/israel_pm_20240105_20240119.json',  # Early
    'indices/israel_prime_minister/israel_pm_20240510_20240524.json',  # Middle
    'indices/israel_prime_minister/israel_pm_20251219_20260102.json',  # Last
]

for blob_name in files_to_check:
    print(f'\n{"="*80}')
    print(f'File: {blob_name}')
    print("="*80)
    
    blob = container.get_blob_client(blob_name)
    data = json.loads(blob.download_blob().readall())
    
    print(f'Query: {data.get("query", "N/A")}')
    print(f'Start Date: {data.get("start_date", "N/A")}')
    print(f'End Date: {data.get("end_date", "N/A")}')
    print(f'Citations: {len(data.get("citations", []))}')
    print(f'Sources: {len(data.get("sources", []))}')
    
    # Show first citation if available
    if data.get('citations'):
        print(f'\nFirst citation:')
        citation = data['citations'][0]
        print(f'  - ID: {citation.get("id", "N/A")}')
        print(f'  - Text: {citation.get("text", "N/A")[:150]}...')
        print(f'  - Source: {citation.get("source_url", "N/A")[:80]}...')
        print(f'  - Relevance: {citation.get("relevance_score", "N/A")}')
