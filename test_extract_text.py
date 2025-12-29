import json
import re
import sys

def extract_article_text(data):
    """Extract clean article text from JSON data."""
    text_content = data.get("text", "")

    # Split text into lines for processing
    lines = text_content.split('\n')
    
    filtered_lines = []
    is_article_body = False
    
    # Regex to identify the start of the article (Date or Location usually starts the body)
    # Adjust this pattern based on common article starts in your data
    start_pattern = re.compile(r"^\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}|^\*\*([A-Z\s]+)\*\*\s-")

    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue

        # Detect start of article body (e.g., "28 October 2025" or "**CAPE TOWN** - ...")
        if start_pattern.search(line):
            is_article_body = True

        # Stop if we hit footer sections
        if line.startswith("## Tags") or line.startswith("Back to top") or "For more information" in line:
            is_article_body = False
        
        # Logic: We only want text if we are inside the body
        # OR if the line looks like a headline (starts with #) but isn't a nav menu
        if is_article_body:
            # Remove Markdown links syntax [text](url) -> text
            clean_line = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', line)
            
            # Remove bold/italic markdown (** or *)
            clean_line = re.sub(r'\*\*|__', '', clean_line)
            
            # Skip lines that look like menu items or breadcrumbs
            if "Press releases" in clean_line and len(clean_line) < 20:
                continue
                
            filtered_lines.append(clean_line)

    return "\n\n".join(filtered_lines)


def process_file(input_file, output_file=None):
    """Process JSONL file and extract clean article text."""
    results = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                data = json.loads(line.strip())
                url = data.get("url", "")
                
                # Extract clean text
                clean_text = extract_article_text(data)
                
                # Store result
                result = {
                    "url": url,
                    "original_url": data.get("original_url", url),
                    "clean_text": clean_text,
                    "word_count": len(clean_text.split()),
                    "metadata": data.get("metadata", {})
                }
                results.append(result)
                
                print(f"✅ Processed line {line_num}: {url[:60]}... ({result['word_count']} words)")
                
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing line {line_num}: {e}")
                continue
    
    # Save results
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            for result in results:
                f.write(json.dumps(result, ensure_ascii=False) + '\n')
        print(f"\n💾 Saved {len(results)} results to: {output_file}")
    
    return results


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_extract_text.py <input_file.jsonl> [output_file.jsonl]")
        print("\nExample:")
        print("  python test_extract_text.py cache/web_scrapes/req_*/extracted_linked_text.jsonl")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file.replace('.jsonl', '_clean.jsonl')
    
    print(f"📄 Processing: {input_file}")
    print(f"💾 Output to: {output_file}\n")
    
    results = process_file(input_file, output_file)
    
    print(f"\n📊 Summary:")
    print(f"  Total documents: {len(results)}")
    if results:
        total_words = sum(r['word_count'] for r in results)
        avg_words = total_words / len(results)
        print(f"  Total words extracted: {total_words:,}")
        print(f"  Average words per doc: {avg_words:.0f}")