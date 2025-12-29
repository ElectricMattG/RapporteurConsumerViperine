"""
Test ImageExtractor with sample markdown containing images
"""

import json

from src.components.image.image_extractor import (ImageExtractor,
                                                  ImageExtractorConfig)

# Sample markdown with images
SAMPLE_MARKDOWN = """
# UN Report on Gaza Situation

![Featured image of UN building](https://example.com/un-building.jpg)

The United Nations released a comprehensive report today detailing the humanitarian situation.

## Key Findings

According to experts, the situation remains critical. The following chart shows the data:

![Chart showing humanitarian statistics](https://example.com/statistics-chart.png)

The report states that immediate action is required. Multiple stakeholders have been consulted.

### Expert Statements

![Portrait of UN expert Francesca Albanese](https://example.com/albanese-portrait.jpg)

Francesca Albanese, UN Special Rapporteur, emphasized the urgency of the situation. She stated that "the international community must act now."

![Small icon](https://example.com/small-icon.png)

## Additional Resources

For more information, visit our website.

![Large infographic about human rights](https://example.com/human-rights-infographic.jpg)

This comprehensive infographic details the various aspects of human rights violations documented in the report.
"""

def test_with_sample():
    """Test with sample markdown."""

    doc_metadata = {
        "document_id": "doc_sample_001",
        "source_url": "https://example.com/article",
        "title": "UN Report on Gaza Situation",
        "language": "en"
    }

    print("=" * 80)
    print("Testing ImageExtractor with Sample Markdown")
    print("=" * 80)
    print(f"\n📝 Sample text length: {len(SAMPLE_MARKDOWN)} characters")
    print(f"📰 Title: {doc_metadata['title']}\n")

    # Configure (without downloading to avoid errors)
    config = ImageExtractorConfig(
        min_width=200,
        min_height=200,
        min_file_size=5000,
        download_images=False,  # Don't fetch real images
        skip_decorative=False,  # Keep all for testing
    )

    extractor = ImageExtractor(config=config)

    print("🔍 Extracting images...")
    images = extractor.extract_images(SAMPLE_MARKDOWN, doc_metadata)

    print(f"✅ Found {len(images)} images\n")

    # Display details
    print("-" * 80)
    for idx, img in enumerate(images, 1):
        print(f"\n📷 Image {idx}:")
        print(f"  ID: {img.image_id}")
        print(f"  URL: {img.url}")
        print(f"  Alt: {img.alt_text}")
        print(f"  Type: {img.image_type}")
        print(f"  Position: {img.position_in_doc}")
        print(f"  Section: {img.parent_section or '(root)'}")

        if img.section_hierarchy:
            print(f"  Hierarchy: {' > '.join(img.section_hierarchy)}")

        print(f"  Description Source: {img.description_source}")
        print(f"  Description: {img.description[:100]}...")

        if img.text_before:
            print(f"  Context Before: {img.text_before[:60]}...")
        if img.text_after:
            print(f"  Context After: {img.text_after[:60]}...")

    # Generate metadata
    metadata_output = extractor.to_document_metadata(images)

    print("\n" + "=" * 80)
    print("METADATA OUTPUT:")
    print("=" * 80)
    print(json.dumps(metadata_output, indent=2))

    return images


if __name__ == "__main__":
    test_with_sample()
