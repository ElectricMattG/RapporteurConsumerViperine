"""
Test the markdown filter on existing chunks
"""

import json
from src.components.chunkers.markdown_filter import MarkdownFilter

# Load chunks
input_file = "/Users/home/workspace/ScrapeLinkedWebSources/cache/web_scrapes/req_20251212_175343_010/chunked_document.jsonl"
output_file = "/Users/home/workspace/ScrapeLinkedWebSources/cache/web_scrapes/req_20251212_175343_010/chunked_document_filtered.jsonl"

chunks = []
with open(input_file, 'r') as f:
    for line in f:
        chunks.append(json.loads(line))

print(f"📥 Loaded {len(chunks)} chunks")

# Apply filter
filter = MarkdownFilter(min_words=5)
filtered_chunks = filter.filter_chunks(chunks)

print(f"✅ Filtered to {len(filtered_chunks)} chunks ({len(chunks) - len(filtered_chunks)} removed)")

# Save filtered chunks
with open(output_file, 'w') as f:
    for chunk in filtered_chunks:
        f.write(json.dumps(chunk, ensure_ascii=False) + '\n')

print(f"💾 Saved to: {output_file}")

# Show some stats
short_removed = sum(1 for c in chunks if len(c.get("content", "").split()) < 5)
print(f"\n📊 Stats:")
print(f"  - Short chunks removed (< 5 words): {short_removed}")
print(f"  - List items removed: {len(chunks) - len(filtered_chunks) - short_removed}")

# Extract contextual links
links_data = filter.extract_contextual_links(filtered_chunks)
print(f"\n🔗 Found {len(links_data)} chunks with contextual links")

if links_data:
    print("\nSample links:")
    for item in links_data[:3]:
        print(f"  Section: {item['section']}")
        print(f"  Links: {item['links'][:2]}")
        print()
