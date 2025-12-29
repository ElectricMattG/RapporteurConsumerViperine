#!/usr/bin/env python3
"""
Check Azure archive for summary uploads
"""
import os
from azure.storage.blob import BlobServiceClient
from collections import defaultdict

def check_summaries(session_id=None, request_id=None):
    """Check for summaries in Azure archive"""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        print("❌ AZURE_STORAGE_CONNECTION_STRING not set")
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "research-archive"
    
    print(f"\n🔍 Checking summaries in archive container: {container_name}\n")
    
    # Build prefix based on what we're searching for
    if request_id:
        prefix = f"{request_id}/"
        print(f"📂 Searching for request: {request_id}\n")
    elif session_id:
        # Search for any request in this session
        # Request IDs typically start with req_
        prefix = ""
        print(f"📂 Searching for session: {session_id}\n")
    else:
        prefix = ""
        print(f"📂 Searching all requests\n")
    
    container_client = blob_service_client.get_container_client(container_name)
    
    # Collect files by request
    requests = defaultdict(lambda: {
        'scraped': [],
        'chunked': [],
        'summarized': [],
        'qa_pairs': [],
        'embedded_chunks': [],
        'embedded_qa': [],
        'other': []
    })
    
    blob_count = 0
    for blob in container_client.list_blobs(name_starts_with=prefix):
        blob_count += 1
        
        # Filter by session if specified
        if session_id and session_id not in blob.name:
            continue
            
        parts = blob.name.split('/')
        if len(parts) >= 2:
            request = parts[0]
            filename = parts[-1]
            
            # Categorize the file
            if 'scraped_content' in filename:
                requests[request]['scraped'].append((filename, blob.size, blob.last_modified))
            elif 'chunked_documents' in filename:
                requests[request]['chunked'].append((filename, blob.size, blob.last_modified))
            elif 'summarized_chunks' in filename:
                requests[request]['summarized'].append((filename, blob.size, blob.last_modified))
            elif 'qa_pairs' in filename:
                requests[request]['qa_pairs'].append((filename, blob.size, blob.last_modified))
            elif 'embedded_chunks' in filename:
                requests[request]['embedded_chunks'].append((filename, blob.size, blob.last_modified))
            elif 'embedded_qa' in filename:
                requests[request]['embedded_qa'].append((filename, blob.size, blob.last_modified))
            else:
                requests[request]['other'].append((filename, blob.size, blob.last_modified))
    
    print(f"📊 Total blobs scanned: {blob_count}\n")
    print(f"📊 Unique requests found: {len(requests)}\n")
    
    if not requests:
        print("❌ No matching requests found in archive\n")
        return
    
    # Display results
    for request, files in sorted(requests.items()):
        print(f"\n{'='*80}")
        print(f"📁 Request: {request}")
        print(f"{'='*80}")
        
        # Check for session ID in request
        if session_id:
            # Extract session from filename if present
            for category in files.values():
                for filename, _, _ in category:
                    if session_id in filename:
                        print(f"✅ Session match found: {session_id}")
                        break
        
        categories = [
            ('🌐 Scraped Content', files['scraped']),
            ('📄 Chunked Documents', files['chunked']),
            ('📝 Summarized Chunks', files['summarized']),
            ('❓ Q&A Pairs', files['qa_pairs']),
            ('🔢 Embedded Chunks', files['embedded_chunks']),
            ('🔢 Embedded Q&A', files['embedded_qa']),
            ('📎 Other Files', files['other'])
        ]
        
        for category_name, category_files in categories:
            if category_files:
                print(f"\n{category_name}: {len(category_files)} file(s)")
                for filename, size, modified in category_files:
                    size_kb = size / 1024
                    print(f"  ✅ {filename}")
                    print(f"     Size: {size_kb:.2f} KB | Modified: {modified}")
            else:
                print(f"\n{category_name}: ❌ None")
    
    # Summary statistics
    print(f"\n\n{'='*80}")
    print("📊 SUMMARY STATISTICS")
    print(f"{'='*80}")
    
    total_scraped = sum(len(r['scraped']) for r in requests.values())
    total_chunked = sum(len(r['chunked']) for r in requests.values())
    total_summarized = sum(len(r['summarized']) for r in requests.values())
    total_qa = sum(len(r['qa_pairs']) for r in requests.values())
    total_embedded_chunks = sum(len(r['embedded_chunks']) for r in requests.values())
    total_embedded_qa = sum(len(r['embedded_qa']) for r in requests.values())
    
    print(f"Requests processed: {len(requests)}")
    print(f"Scraped content files: {total_scraped}")
    print(f"Chunked document files: {total_chunked}")
    print(f"📝 Summarized chunks files: {total_summarized} {'✅' if total_summarized > 0 else '❌'}")
    print(f"Q&A pairs files: {total_qa}")
    print(f"Embedded chunks files: {total_embedded_chunks}")
    print(f"Embedded Q&A files: {total_embedded_qa}")
    
    # Check pipeline health
    print(f"\n{'='*80}")
    print("🏥 PIPELINE HEALTH CHECK")
    print(f"{'='*80}")
    
    for request, files in sorted(requests.items()):
        has_scraped = len(files['scraped']) > 0
        has_chunked = len(files['chunked']) > 0
        has_summarized = len(files['summarized']) > 0
        has_qa = len(files['qa_pairs']) > 0
        
        status = "✅ Complete" if (has_scraped and has_chunked and has_summarized and has_qa) else "⚠️ Incomplete"
        print(f"{status} {request}")
        
        if not has_summarized and has_chunked:
            print(f"  ⚠️ Missing summaries (has chunks)")

if __name__ == "__main__":
    import sys
    
    session_id = None
    request_id = None
    
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.startswith("req_"):
            request_id = arg
        else:
            session_id = arg
    
    check_summaries(session_id=session_id, request_id=request_id)
