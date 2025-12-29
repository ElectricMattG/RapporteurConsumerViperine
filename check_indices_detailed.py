#!/usr/bin/env python3
import os
import json
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service = BlobServiceClient.from_connection_string(connection_string)
container = blob_service.get_container_client('config')

# Check a few files across the timeline
files_to_check = [
    'indices/israel_prime_minister/israel_pm_20230901_20230915.json',  # First
    'indices/israel_prime_minister/israel_pm_20240510_20240524.json',  # Middle
    'indices/israel_prime_minister/israel_pm_20251219_20260102.json',  # Last
]

for blob_name in files_to_check:
    print(f'\n{"="*80}')
    print(f'File: {blob_name.split("/")[-1]}')
    print("="*80)
    
    blob = container.get_blob_client(blob_name)
    data = json.loads(blob.download_blob().readall())
    
    # Extract metadata
    metadata = data.get('metadata', {})
    print(f'Query: {metadata.get("query", "N/A")}')
    print(f'Window: {metadata.get("window_start")} to {metadata.get("window_end")}')
    print(f'Generated: {metadata.get("generated_at")}')
    
    # Extract RAG response info
    rag = data.get('rag_response', {})
    citations = rag.get('citations', [])
    sources = rag.get('sources', [])
    
    print(f'Citations: {len(citations)}')
    print(f'Sources: {len(sources)}')
    print(f'\nAnswer preview:')
    answer = rag.get('answer', 'N/A')
    print(answer[:300] + '...' if len(answer) > 300 else answer)
    
    # Show first citation if available
    if citations:
        print(f'\nFirst citation:')
        citation = citations[0]
        print(f'  - Text: {citation.get("text", "N/A")[:150]}...')
        print(f'  - Source: {citation.get("source_url", "N/A")[:80]}')
        print(f'  - Relevance: {citation.get("relevance_score", "N/A")}')
