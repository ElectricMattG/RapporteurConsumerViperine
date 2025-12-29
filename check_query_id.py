#!/usr/bin/env python3
from azure.storage.blob import BlobServiceClient
import json

conn_str = 'DefaultEndpointsProtocol=https;AccountName=ragservicene;AccountKey=+6hU+KaLn4r2YTL/xGvH0X0YhVuJZwg2cNx3lp8zg5k2vhPq8JfpRZ9FhOPdgqywcwVREaKfTc5c+AStL1qOYA==;EndpointSuffix=core.windows.net'
blob_client = BlobServiceClient.from_connection_string(conn_str)
container = blob_client.get_container_client('archive')

# Get first chunk file
blob_name = 'sess_e0fc67c87f0lc/req_20251225_231034_000/chunked_document_enriched_embedded.jsonl'
blob = container.get_blob_client(blob_name)
content = blob.download_blob().readall().decode('utf-8')

# Parse first line
first_line = content.split('\n')[0]
chunk = json.loads(first_line)

# Check for query_id
doc_metadata = chunk.get('document_metadata', {})
print('document_metadata keys:', list(doc_metadata.keys()))
print('query_id:', doc_metadata.get('query_id', 'NOT FOUND'))
