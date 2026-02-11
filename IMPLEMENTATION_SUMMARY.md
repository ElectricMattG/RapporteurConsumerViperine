# Citation Navigation & URL Decoding Implementation

**Date:** February 9, 2026
**Status:** ✅ Implementation Complete (Testing Pending)
**Features:** Clickable Citations + Arabic/Hebrew URL Display

---

## Summary

Successfully implemented two major UX enhancements for the RAG interface:

1. **Clickable Citations with Navigation**: Users can click citations in generated responses to jump directly to source cards with visual highlighting
2. **URL Decoding**: Arabic and Hebrew characters in URLs are now displayed in human-readable form instead of percent-encoded

---

## Changes Made

### 1. Backend Updates (`src/services/rag/main.py`)

#### New Functions

**`decode_url_for_display(url: str) -> str`**
- Decodes percent-encoded URLs using `urllib.parse.unquote()`
- Handles Arabic/Hebrew and other international characters
- Graceful error handling with fallback to original URL

**`build_citation_map(retrieved_docs, retrieved_qa) -> Dict`**
- Builds mapping from document IDs to citation display information
- Extracts author from metadata (tries `author`, then `publisher`, fallback to 'Unknown')
- Formats dates as "Month Year" (e.g., "February 2026")
- Tracks card type (`sum` or `qa`) and index for scroll targeting
- Returns structure:
  ```python
  {
      "doc_1f12284b": {
          "author": "IDF Spokesperson",
          "date": "January 2026",
          "card_type": "sum",
          "card_index": 0
      }
  }
  ```

#### Model Updates

**`DocumentResult` (line ~89)**
- Added `source_url_display: Optional[str] = None` field
- Original `source_url` preserved for navigation href
- `source_url_display` used for display text

**`QueryLLMResponse` (line ~111)**
- Added `citation_map: Optional[Dict[str, Dict]] = None` field
- Passed to frontend for citation resolution

#### Endpoint Updates

**`query_llm()` function (line ~880)**
- Decodes URLs when building DocumentResult objects
- Adds `source_url_display` to QA items after retrieval
- Builds citation_map before returning response
- Includes citation_map in QueryLLMResponse

**Lines Changed:**
- Line 11: Added `from urllib.parse import unquote` import
- Lines 228-318: Added helper functions
- Line 90: Updated DocumentResult model
- Line 113: Updated QueryLLMResponse model
- Lines 923-942: Updated document formatting with URL decoding
- Lines 990-992: Build and include citation_map

---

### 2. Frontend JavaScript (`src/services/frontend/static/app.js`)

#### New Functions

**`renderCitationsAsLinks(content, citationMap)` (line ~520)**
- Converts citation placeholders `(doc_XXX, qa-N)` to clickable links
- Uses regex pattern: `/\(([a-f0-9]+),\s*(qa-\d+|summary)\)/gi`
- Looks up author and date from citation_map
- Returns HTML anchor tags with:
  - Display text: `(Author, Month Year)`
  - href: `#sum-{index}` or `#qa-{index}`
  - class: `citation-link`
  - data-card-id: for click handling

**`scrollToAndHighlightCard(cardId)` (line ~547)**
- Scrolls target card into view with smooth animation
- Centers card in viewport
- Adds `car-highlighted` class for 3-second visual feedback
- Auto-removes highlight class via setTimeout

#### Function Modifications

**`addMessage()` (line ~559)**
- Now processes citations before markdown rendering
- Checks for `metadata.citation_map`
- Calls `renderCitationsAsLinks()` on content
- Then parses markdown with `marked.parse()`

**Card rendering in `updateDocumentList()` (lines ~1001-1145)**
- Added `id` attributes to summary cards: `sum-{index}`
- Added `id` attributes to QA cards: `qa-{index}`
- Updated URL display: `urlLink.textContent = doc.source_url_display || doc.source_url`
- Applied to both summary and QA card URL links

#### Event Listeners

**Citation Click Handler (line ~355)**
- Event delegation on document for `.citation-link` clicks
- Prevents default anchor behavior
- Extracts `cardId` from data attribute
- Calls `scrollToAndHighlightCard(cardId)`

**Lines Changed:**
- Lines 520-558: Added renderCitationsAsLinks() and scrollToAndHighlightCard()
- Lines 559-610: Modified addMessage() to process citations
- Line 355-362: Added citation click event listener
- Lines 1001-1006: Added card IDs to summary cards
- Lines 1050-1052: Updated summary URL display
- Line 1065: Added card ID to QA cards
- Lines 1135-1141: Updated QA URL display

---

### 3. Frontend CSS (`src/services/frontend/static/style.css`)

#### New Styles

**`.citation-link` (lines ~775-790)**
```css
.citation-link {
    color: var(--accent-blue-light);
    text-decoration: none;
    cursor: pointer;
    padding: 1px 3px;
    border-radius: 3px;
    transition: all 0.2s ease;
    display: inline-block;
}

.citation-link:hover {
    background-color: var(--accent-blue-bg);
    color: var(--accent-blue);
    text-decoration: underline;
}
```

**`.card-highlighted` (lines ~793-797)**
```css
.card-highlighted {
    animation: highlightPulse 3s ease-in-out;
    border-color: #f97316 !important;  /* Orange */
    box-shadow: 0 0 20px rgba(249, 115, 22, 0.5);
}
```

**`@keyframes highlightPulse` (lines ~799-813)**
- 3-second animation with orange highlight
- Scale effect (1.0 → 1.02 → 1.0)
- Background fades from 30% → 50% → 30% → transparent
- Orange color (#f97316) matches QA theme

**Lines Changed:**
- Lines 775-813: Added citation link styles and highlight animation

---

## Technical Implementation Details

### Citation Resolution Flow

1. **Backend Generation**: RAG service generates response with placeholders like `(1f12284b, qa-3)`
2. **Backend Mapping**: `build_citation_map()` creates lookup table from doc IDs
3. **API Response**: Frontend receives both `llm_response` and `citation_map`
4. **Frontend Processing**: `renderCitationsAsLinks()` regex-replaces placeholders
5. **Markdown Rendering**: Processed content passed to `marked.parse()`
6. **User Interaction**: Click triggers scroll and 3-second highlight animation

### URL Decoding Flow

1. **Backend Retrieval**: Documents fetched from index with encoded URLs
2. **Backend Decoding**: `decode_url_for_display()` converts `%D8%A7%D9%84...` to readable Arabic
3. **Dual Fields**: Both `source_url` (encoded) and `source_url_display` (decoded) sent to frontend
4. **Frontend Rendering**:
   - href uses `source_url` for proper navigation
   - Display text uses `source_url_display` for readability
   - Fallback chain: `source_url_display || source_url`

### Card ID Strategy

- **Summary Cards**: `sum-0`, `sum-1`, `sum-2`, ... (matches array index)
- **QA Cards**: `qa-0`, `qa-1`, `qa-2`, ... (matches array index)
- IDs added during card creation in `updateDocumentList()`
- IDs match citation_map `card_type` and `card_index` fields

---

## Testing Checklist

### Pre-Deployment Testing

- [ ] **Syntax Validation**: Run linters on Python and JavaScript (✅ No errors found)
- [ ] **Unit Tests**: Test helper functions in isolation
- [ ] **Integration Tests**: Test citation map building with sample data

### Post-Deployment Testing

#### Citation Navigation
- [ ] Citations appear as blue clickable links in generated text
- [ ] Clicking citation scrolls to correct card (verify with multiple cards)
- [ ] Orange highlight animation plays for 3 seconds
- [ ] Scroll behavior is smooth and centers card
- [ ] Multiple citations in same response all work correctly
- [ ] Citations to QA cards work (test with QA enabled queries)
- [ ] Citation format shows "(Author, Month Year)" correctly

#### URL Display
- [ ] Arabic URLs display readable text (not percent-encoded)
- [ ] Hebrew URLs display readable text
- [ ] English URLs still display correctly
- [ ] Mixed-language URLs handled properly
- [ ] URL links still navigate correctly (href not broken)
- [ ] Long Arabic URLs wrap without breaking layout
- [ ] RTL text rendering works in cards

#### Cross-Browser Testing
- [ ] Chrome/Chromium
- [ ] Firefox
- [ ] Safari (especially URL decoding)
- [ ] Mobile browsers (iOS Safari, Chrome Android)

#### Edge Cases
- [ ] Missing citation_map (graceful fallback)
- [ ] Missing source_url_display (fallback to source_url)
- [ ] Invalid card IDs in citations (no JS errors)
- [ ] Rapid clicking on citations (no animation conflicts)
- [ ] Very long author names or dates (layout handles overflow)
- [ ] Documents with no metadata (fallback to 'Unknown' author)

---

## Deployment Steps

### 1. Rebuild RAG Service

```bash
cd src/services/rag
docker build -t rag-service:latest .
# Or use your existing build script
./build-and-push.sh
```

### 2. Rebuild Frontend Service

```bash
cd src/services/frontend
docker build -t frontend-service:latest .
```

### 3. Deploy Updated Services

```bash
# Restart RAG service
docker-compose restart rag-service

# Restart Frontend service
docker-compose restart frontend-service
```

### 4. Verify Deployment

```bash
# Check service health
curl http://localhost:8000/health  # RAG service
curl http://localhost:5000/        # Frontend

# Check logs for errors
docker logs rag-service -f
docker logs frontend-service -f
```

---

## Files Modified

### Backend
- ✅ `src/services/rag/main.py` (107 lines added/modified)
  - Imports (line 11)
  - Helper functions (lines 228-318)
  - Model updates (lines 90, 113)
  - Endpoint logic (lines 923-992)

### Frontend
- ✅ `src/services/frontend/static/app.js` (95 lines added/modified)
  - Helper functions (lines 520-558)
  - Message rendering (lines 559-610)
  - Event handlers (lines 355-362)
  - Card rendering (lines 1001-1145)

- ✅ `src/services/frontend/static/style.css` (39 lines added)
  - Citation styles (lines 775-790)
  - Highlight animation (lines 793-813)

### Documentation
- ✅ `QA_CITATION_PLAN.md` (updated with implementation status)
- ✅ `IMPLEMENTATION_SUMMARY.md` (this file)

---

## Known Limitations

1. **Citation Regex Pattern**: Currently matches hex doc IDs (`[a-f0-9]+`). May need adjustment if doc ID format changes.
2. **Date Parsing**: Assumes ISO format dates. Other formats will show as-is without reformatting.
3. **Author Extraction**: Falls back to 'Unknown' if no author/publisher in metadata.
4. **Scroll Behavior**: Uses smooth scrolling which may not be supported in very old browsers.
5. **Highlight Animation**: Uses CSS animations (not supported in IE11, but who cares).

---

## Future Enhancements

- Add keyboard navigation for citations (Tab + Enter)
- Support citation preview on hover (tooltip with preview text)
- Add "Jump back" button after scrolling to citation
- Track citation click analytics
- Support multiple citations to same source (deduplicate)
- Add citation export functionality (copy citation text)
- Implement citation counts in cards ("Cited 3 times")
- Add search/filter by cited sources

---

## Success Metrics

Post-deployment, monitor:
- Citation click-through rate
- Average time to find sources
- User feedback on URL readability
- Error rates in citation resolution
- Mobile vs. desktop usage patterns

---

## Contact

For questions or issues with this implementation:
- Check logs in `docker logs rag-service` and `docker logs frontend-service`
- Review [QA_CITATION_PLAN.md](./QA_CITATION_PLAN.md) for detailed requirements
- Test with the IDF mini-sites dataset: [idf_mini_sites_records.jsonl](./idf_mini_sites_records.jsonl)

---

**Implementation completed by:** GitHub Copilot
**Date:** February 9, 2026
**Next Steps:** Deploy and test in staging environment
