"""
Test ImageExtractor on scraped documents
"""

import json
import sys
from pathlib import Path
from src.components.image.image_extractor import ImageExtractor, ImageExtractorConfig


def test_image_extraction(document_path: str, download_images: bool = False):
    """Test image extraction on a document."""
    
    # Load document
    print(f"📄 Loading document: {document_path}")
    with open(document_path, 'r', encoding='utf-8') as f:
        data = json.loads(f.read())
    
    text = data.get("text", "")
    metadata = data.get("metadata", {})
    
    # Add required fields to metadata
    doc_metadata = {
        "document_id": f"doc_{hash(data.get('url', ''))}"[:16],
        "source_url": data.get("url", ""),
        "title": metadata.get("title", "Unknown"),
        **metadata
    }
    
    print(f"\n📰 Document: {doc_metadata['title']}")
    print(f"🔗 URL: {doc_metadata['source_url']}")
    print(f"📝 Text length: {len(text):,} characters\n")
    
    # Configure extractor
    config = ImageExtractorConfig(
        min_width=200,
        min_height=200,
        min_file_size=5000,
        context_words_before=50,
        context_words_after=50,
        download_images=download_images,
        download_path="cache/images",
        skip_decorative=True,
        use_gemini_vision=False  # Disable for testing
    )
    
    # Extract images
    print("🔍 Extracting images...")
    extractor = ImageExtractor(config=config)
    images = extractor.extract_images(text, doc_metadata)
    
    print(f"✅ Found {len(images)} images (after filtering)\n")
    
    # Display results
    print("=" * 80)
    print("IMAGE DETAILS:")
    print("=" * 80)
    
    for idx, img in enumerate(images, 1):
        print(f"\n📷 Image {idx}:")
        print(f"  ID: {img.image_id}")
        print(f"  URL: {img.url[:80]}...")
        print(f"  Type: {img.image_type}")
        print(f"  Alt Text: {img.alt_text or '(none)'}")
        
        if img.width and img.height:
            print(f"  Dimensions: {img.width}x{img.height}px")
        if img.file_size:
            print(f"  File Size: {img.file_size:,} bytes")
        if img.format:
            print(f"  Format: {img.format}")
        
        print(f"  Section: {img.parent_section or '(root)'}")
        
        if img.section_hierarchy:
            hierarchy = ' > '.join(img.section_hierarchy[:3])  # First 3 levels
            print(f"  Hierarchy: {hierarchy}")
        
        print(f"  Description Source: {img.description_source}")
        if img.description:
            desc_preview = img.description[:150]
            print(f"  Description: {desc_preview}...")
        
        if img.text_before:
            print(f"  Context Before: {img.text_before[:80]}...")
        if img.text_after:
            print(f"  Context After: {img.text_after[:80]}...")
    
    # Summary stats
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    
    types = {}
    for img in images:
        types[img.image_type] = types.get(img.image_type, 0) + 1
    
    print(f"\nTotal images: {len(images)}")
    print("\nBy type:")
    for img_type, count in sorted(types.items()):
        print(f"  {img_type}: {count}")
    
    sources = {}
    for img in images:
        sources[img.description_source] = sources.get(img.description_source, 0) + 1
    
    print("\nDescription sources:")
    for source, count in sorted(sources.items()):
        print(f"  {source}: {count}")
    
    # Generate metadata output
    metadata_output = extractor.to_document_metadata(images)
    
    # Save to file
    output_dir = Path(document_path).parent
    output_file = output_dir / "images_metadata.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metadata_output, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved metadata to: {output_file}")
    
    return images


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_image_extractor.py <path_to_extracted_linked_text.jsonl> [--download]")
        print("\nExamples:")
        print("  python test_image_extractor.py cache/web_scrapes/req_20251212_175343_004/extracted_linked_text.jsonl")
        print("  python test_image_extractor.py cache/web_scrapes/req_20251212_175343_010/extracted_linked_text.jsonl --download")
        sys.exit(1)
    
    document_path = sys.argv[1]
    download = "--download" in sys.argv
    
    if download:
        print("⬇️  Download mode enabled - will fetch images to check dimensions\n")
    
    test_image_extraction(document_path, download_images=download)
