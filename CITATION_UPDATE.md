# Citation Updates TODO

## Scope
**Phase 1: PDF Generation Only** (this document) ✅ IMPLEMENTED
- Citation formatting in PDF exports
- Template rendering updates
- Deduplication and merging logic

**Phase 2: LLM Prompt Updates** (deferred - requires separate planning)
- Update `src/prompts/q_a.py` citation instructions
- Update `src/services/rag/main.py` grounding prompts
- Remove `[qa-N]` differentiators from generated text

---

## Implementation Status

### ✅ Completed Items

#### 1. Citation Data Model (`format_citation` in pdf_generator.py) ✅
- ✅ Extract publisher field with fallback chain (publisher → author → domain → "Unknown")
- ✅ Extract published_date separately with full day formatting ("DD Month YYYY")
- ✅ Parse ISO 8601 strings and handle +00:00, Z timezone markers
- ✅ Support partial dates (YYYY-MM-DD → full date, YYYY-MM → Month YYYY)
- ✅ Missing date fallback changed to "ND" (not "n.d.")
- ✅ Handle null/nan/None/empty values
- ✅ Build formatted citation string: `{author} ({month_year}). {title}. Published by **{publisher}** on {full_date}. [doc_id: {doc_id}]`
- ✅ Return fields for raw source_url and formatted display text

#### 2. Deduplication & Merge (`build_pdf_html` in pdf_generator.py) ✅
- ✅ Merge summary and QA citations into single list
- ✅ Deduplicate by doc_id (primary) or source_url (fallback)
- ✅ Generate sequential numbering [1], [2], [3], etc.
- ✅ Remove QA distinction (no [qa-] prefixes)

#### 3. Template Update (`pdf_template.html`) ✅
- ✅ Remove separate "Summary Documents" and "Q&A Pairs" subsections
- ✅ Single "Citations & References" section with unified list
- ✅ Hyperlink entire citation text (including doc_id)
- ✅ Remove standalone URL display lines
- ✅ Use new citation format with formatted_text field

---

## Overview
Update PDF citation format to match the required specification:
- Current format: `[1] The Times of Israel (November 2023). Israeli troops roll into...` with separate URL line and doc_id
- New format: `[1] Cohen, Avi (November 2023). Israeli troops roll into Gaza City... Published by **The Times of Israel** on 23 November 2023. [doc_id: unknown]` with entire block hyperlinked

## Implementation Steps

### 1. Citation Data Model (`format_citation` in pdf_generator.py)
- **Extract publisher field** from `doc_metadata.get('publisher')` with fallback chain:
  - publisher → author → domain name from URL → "Unknown"
- **Extract published_date** separately from creation_date with full day formatting:
  - Parse ISO date strings to "DD Month YYYY" format (e.g., "23 November 2023")
  - Fallback: published_date → creation_date → publication_date → "ND"
- **Build formatted citation string** that matches new spec:
  - `{author} ({month_year}). {title}. Published by **{publisher}** on {published_date}. [doc_id: {doc_id}]`
- **Return fields**: Keep raw `source_url` for href attribute, formatted string for display text
- **Remove `is_qa` flag** or ensure it's not used for differentiation in final output

### 2. Deduplication & Merge (`build_pdf_html` in pdf_generator.py)
- **Merge citations**: Combine `summary_citations` and `qa_citations` into single list
- **Deduplicate** by `doc_id` (primary) or `source_url` (fallback if doc_id missing/duplicate)
- **Single numbering sequence**: Generate sequential `[1]`, `[2]`, etc. across all citations
- **Remove QA distinction**: No `[qa-1]` prefixes in final merged list

### 3. Template Update (`pdf_template.html`)
- **Replace dual sections**: Remove separate "Summary Documents" and "Q&A Pairs" subsections
- **Single "Citations & References" section** with unified citation list
- **Hyperlink entire entry**: Wrap complete citation text (including doc_id) in anchor tag:
  - `<a href="{source_url}">{formatted_citation_string}</a>`
- **Remove standalone URL line**: Display URL only appears as href, not as visible text
- **Citation format in template**:
  ```html
  <div class="citation-entry">
    <a href="{{ citation.url }}" target="_blank">[{{ citation.index }}] {{ citation.formatted_text }}</a>
  </div>
  ```

### 4. Field Fallbacks & Date Handling
- **Author fallback chain**:
  - `doc_metadata.author` → `doc_metadata.publisher` → domain from URL → "Unknown"
- **Publisher fallback chain**:
  - `doc_metadata.publisher` → `doc_metadata.author` → domain from URL → base URL domain
- **Date formatting**:
  - Parse ISO 8601 strings (handle +00:00, Z timezone markers)
  - Support partial dates (YYYY-MM-DD → full date, YYYY-MM → Month YYYY, YYYY → YYYY)
  - Month-year for inline citation: "November 2023"
  - Full date for published date: "23 November 2023"
  - Missing date fallback: "ND" (not "n.d." or "N/A")
- **Handle null/nan values**: Treat 'null', 'nan', None, empty string as missing

### 5. URL Decoding
- **Display text**: Use `urllib.parse.unquote()` to decode percent-encoded URLs for anchor text
- **href attribute**: Keep raw percent-encoded URL for proper link functionality
- **Handle Arabic/Unicode**: Ensure UTF-8 encoding preserves Arabic characters after unquote

### 6. QA Parity
- **No special prefixes**: QA citations numbered same as summaries (`[1]`, `[2]`, not `[qa-1]`)
- **Same format**: QA and summary citations indistinguishable in final reference list
- **Unified treatment**: Both use identical citation string format and hyperlink structure

### 7. Testing & Validation
- **Unit test**: `format_citation()` with missing date returns "ND"
- **Unit test**: `format_citation()` with missing publisher uses fallback chain
- **Unit test**: Deduplication removes duplicate doc_ids
- **Integration test**: Generate PDF with mixed summary/QA inputs, verify:
  - Citations are merged into single numbered list
  - No `[qa-]` prefixes appear
  - Entire citation text is hyperlinked
  - doc_id appears at end of citation text inside link
  - No standalone URL lines
  - Format matches: `[N] Author (Month Year). Title. Published by **Publisher** on DD Month YYYY. [doc_id: xxx]`

## Files to Modify

1. **`/home/electric/workspace/ScrapeLinkedWebSources/src/services/frontend/pdf_generator.py`**
   - `format_citation()` function (lines ~58-113)
   - `build_pdf_html()` function (lines ~220-295)

2. **`/home/electric/workspace/ScrapeLinkedWebSources/src/services/frontend/templates/pdf_template.html`**
   - Citations section (lines ~57-82)

3. **Tests** (create or update):
   - `tests/services/frontend/test_pdf_generator.py` (if exists)
   - Add fixtures for citation formatting edge cases

## Pre-Implementation Checklist

Before starting implementation, verify:

- [ ] **Current code reviewed**: Understand existing `format_citation()` and `build_pdf_html()` functions
- [ ] **Template structure understood**: Know how citations are currently rendered in `pdf_template.html`
- [ ] **Test environment ready**: Can generate test PDFs with sample data
- [ ] **Sample data prepared**: Have test cases with:
  - [ ] Missing dates (should render "ND")
  - [ ] Missing publisher (should use fallback chain)
  - [ ] Duplicate doc_ids (should deduplicate)
  - [ ] Mixed summary and QA citations
  - [ ] Arabic/Unicode URLs
  - [ ] Various date formats (ISO 8601, partial dates)
- [ ] **Backup created**: Current working version saved before changes
- [ ] **Dependencies identified**: No new libraries needed (uses existing urllib, datetime)

## Implementation Order

1. **Start with `format_citation()`** - Easier to test in isolation
2. **Update `build_pdf_html()`** - Uses updated citation objects
3. **Modify template** - Renders new format
4. **Add tests** - Verify all edge cases
5. **Generate sample PDF** - Manual validation
6. **Deploy and test** - End-to-end validation

## Acceptance Criteria

✅ Citations follow exact format: `[N] Author (Month Year). Title. Published by **Publisher** on DD Month YYYY. [doc_id: xxx]`
✅ Entire citation block is a single hyperlink to source_url
✅ No separate URL display line
✅ QA and summary citations merged into one numbered sequence
✅ No `[qa-]` prefixes in final output
✅ Deduplication by doc_id works correctly
✅ Missing dates render as "ND"
✅ Publisher fallback chain works (publisher → author → domain)
✅ URL decoding handles Arabic/Unicode characters
✅ Tests cover all edge cases (missing fields, duplicates, special characters)

## Risk Mitigation

- **Breaking changes**: Template changes affect all PDF exports - test thoroughly
- **Date parsing failures**: Handle malformed ISO strings gracefully (try/except with fallback)
- **Encoding issues**: Ensure UTF-8 handling throughout (especially for Arabic text)
- **Deduplication edge cases**: Handle missing doc_ids (use source_url as fallback key)
- **Performance**: Deduplication shouldn't significantly impact generation time for typical citation counts (<100)
