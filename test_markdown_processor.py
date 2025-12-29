"""
Test script for Simplified MarkdownDocumentProcessor

Tests the simplified markdown processing that aggregates paragraphs between headers
"""

import json
from src.components.chunkers.markdown_processor_simple import (
    MarkdownDocumentProcessor,
    MarkdownProcessorConfig
)


# Sample markdown (similar to what we extract)
SAMPLE_MARKDOWN = """# Debunking Lies in Francesca Albanese's October 2025 UNGA Report

- [October 27, 2025](https://unwatch.org/2025/10/27/)
- [Antisemitism](https://unwatch.org/category/a/antisemitism-en/) , [Israel](https://unwatch.org/category/a/israel-en/)

## **14 Ways Francesca Albanese Weaponizes Human Rights Against Israel**

*Following is UN Watch's legal analysis of the October 2025 report of Special Rapporteur on Palestine Francesca Albanese titled "An International Genocide."*

*By Dina Rovner, Legal Advisor at UN Watch*

### **1. A Blueprint for Israel's Isolation and Delegitimization**

In her October 2025 report to the UN General Assembly, titled *"An International Genocide,"* Francesca Albanese advances a sustained campaign of delegitimization against Israel - the world's only Jewish state - charging it with "full-fledged genocide" (Para. 1).

Her report brands Israel a threat to "international peace and security" (Para. 19) and declares that support for it constitutes "an offence to justice" and "the idea of common humanity" (Para. 69).

### **2. Debases the Term "Genocide"**

As the report's title, *"An International Genocide,"* makes clear, Albanese's justification for turning Israel into a pariah state rests on the claim that it is guilty of the gravest crime against humanity - genocide.

The word *"genocide"* appears 78 times in her 24-page report.

## Share

[Twitter](https://twitter.com/UNWatch)

[Facebook-f](https://www.facebook.com/unwatch/)

[Youtube](https://youtube.com/HumanRightsUN)

## Recent

### [Hillel Neuer Exposes the UN's Alena Douhan](https://unwatch.org/hillel-neuer/)

December 1, 2025

### [NEW: Refuting UNRWA Facts](https://unwatch.org/new-refuting-unrwa/)

December 1, 2025

## Your Voice at the United Nations

UN Watch is a non-profit organization dedicated to holding the United Nations accountable.

## Keep in touch

[newsletter SIGN-uP](https://civicrm.unwatch.org/Sign_up)

[Twitter](https://twitter.com/UNWatch)

## Support

[make a donation](https://unwatch.org/donate)

### Copyright © 2014-2026. UN Watch | Your voice at the UN.

### [Privacy Statement](https://unwatch.org/privacy-statement/)

Manage Consent

To provide the best experiences, we use technologies like cookies to store and/or access device information.

Functional Functional Always active

The technical storage or access is strictly necessary for the legitimate purpose of enabling the use of a specific service.

Accept

Deny
"""


def test_markdown_processor():
    """Test the markdown processor with sample document"""
    
    print("=" * 80)
    print("Testing Simplified MarkdownDocumentProcessor")
    print("=" * 80)
    
    # Create processor with default config
    config = MarkdownProcessorConfig(
        min_paragraph_words=5,  # Filter out paragraphs < 5 words
        max_chunk_words=500
    )
    
    processor = MarkdownDocumentProcessor(config=config)
    
    # Sample document metadata
    doc_metadata = {
        "document_id": "doc_test_001",
        "source_url": "https://unwatch.org/test-article/",
        "extraction_method": "web_extraction",
        "title": "Test Article",
        "author": "Test Author",
        "language": "en",
        "extraction_timestamp": "2025-12-12T18:00:00",
        "total_chars": len(SAMPLE_MARKDOWN)
    }
    
    # Process markdown
    print("\n📄 Processing markdown document...")
    chunks = processor.process(SAMPLE_MARKDOWN, doc_metadata)
    
    print(f"\n✅ Generated {len(chunks)} chunks\n")
    
    # Display results
    print("-" * 80)
    print("CHUNKS GENERATED:")
    print("-" * 80)
    
    for i, chunk in enumerate(chunks, 1):
        meta = chunk["chunk_metadata"]
        content_preview = chunk["content"][:100].replace('\n', ' ')
        
        section_path = ' > '.join(meta['section_hierarchy']) if meta['section_hierarchy'] else "(no headers)"
        
        print(f"\nChunk {i}:")
        print(f"  ID: {meta['chunk_id'][:16]}...")
        print(f"  Type: {meta['chunk_type']}")
        print(f"  Word Count: {meta['word_count']}")
        print(f"  Section: {meta['section_title']}")
        print(f"  Hierarchy: {section_path}")
        print(f"  Parent: {meta['parent_section']}")
        print(f"  Content: {content_preview}...")
    
    # Verify filtering worked
    print("\n" + "=" * 80)
    print("FILTERING EFFECTIVENESS:")
    print("=" * 80)
    
    # Check filtering effectiveness
    chunk_contents = [chunk["content"] for chunk in chunks]
    all_content = " ".join(chunk_contents)
    
    # Count list items (should be zero)
    list_indicators = all_content.count('- [') + all_content.count('* ')
    print(f"\n📋 List items in chunks: {list_indicators}")
    
    # Check word counts
    short_chunks = [c for c in chunks if c["chunk_metadata"]["word_count"] < 5]
    print(f"📏 Chunks with < 5 words: {len(short_chunks)}")
    
    if list_indicators == 0 and len(short_chunks) == 0:
        print("✅ Filtering successful - no lists or short chunks!")
    
    # Check substantive content is preserved
    substantive_terms = ["Albanese", "genocide", "Israel", "UN General Assembly"]
    found_substantive = []
    
    for term in substantive_terms:
        if term in all_content:
            found_substantive.append(term)
    
    print(f"\n✅ Substantive content preserved: {len(found_substantive)}/{len(substantive_terms)} key terms found")
    
    # Section hierarchy check
    print("\n" + "=" * 80)
    print("SECTION HIERARCHY CHECK:")
    print("=" * 80)
    
    hierarchies = set()
    for chunk in chunks:
        hierarchy = chunk["chunk_metadata"]["section_hierarchy"]
        if hierarchy:
            hierarchy_str = " > ".join(hierarchy)
            hierarchies.add(hierarchy_str)
    
    print(f"\nUnique section paths ({len(hierarchies)}):")
    for hierarchy in sorted(hierarchies):
        print(f"  • {hierarchy}")
    
    # Save output for inspection
    output_file = "test_markdown_chunks_output.jsonl"
    with open(output_file, 'w') as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + '\n')
    
    print(f"\n💾 Chunks saved to: {output_file}")
    
    return chunks


if __name__ == "__main__":
    test_markdown_processor()
