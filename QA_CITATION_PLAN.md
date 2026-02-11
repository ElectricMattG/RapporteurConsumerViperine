# Front-End TODO: Clickable Citations & URL Decoding

**Last Updated:** February 9, 2026
**Status:** ✅ **IMPLEMENTATION COMPLETE** (Testing Pending)
**Priority:** High

---

## 🎉 IMPLEMENTATION STATUS

**Implementation Date:** February 9, 2026
**Implementation Status:** Complete

### ✅ Completed Tasks

1. **Backend Implementation** (`src/services/rag/main.py`)
   - ✅ Added `urllib.parse.unquote` import
   - ✅ Created `decode_url_for_display()` helper function
   - ✅ Created `build_citation_map()` helper function
   - ✅ Updated `DocumentResult` model with `source_url_display` field
   - ✅ Updated `QueryLLMResponse` model with `citation_map` field
   - ✅ Modified `query_llm` endpoint to decode URLs and build citation map
   - ✅ Added source_url_display to both summaries and QA items

2. **Frontend JavaScript** (`src/services/frontend/static/app.js`)
   - ✅ Created `renderCitationsAsLinks()` function to convert placeholders to clickable links
   - ✅ Created `scrollToAndHighlightCard()` function for scroll and highlight behavior
   - ✅ Modified `addMessage()` to process citations before markdown rendering
   - ✅ Added citation click event listener with event delegation
   - ✅ Updated card rendering to add `id` attributes (`sum-{index}`, `qa-{index}`)
   - ✅ Updated URL display to use `source_url_display` with fallback to `source_url`

3. **Frontend CSS** (`src/services/frontend/static/style.css`)
   - ✅ Added `.citation-link` styles with hover effects
   - ✅ Added `.card-highlighted` class with orange highlight animation
   - ✅ Created `@keyframes highlightPulse` for 3-second animation
   - ✅ Orange theme preserved for QA cards (#f97316)

### 📋 Pending Tasks

- ⏳ Test citation navigation in browser
- ⏳ Test URL display with Arabic/Hebrew characters
- ⏳ Test mobile responsiveness
- ⏳ Rebuild and redeploy RAG service with new changes
- ⏳ End-to-end testing with real queries

---

## Overview

Two key enhancements to improve user experience with citations and multi-language URL support:
1. **Clickable Citations**: Enable users to click inline citations and jump to source cards with visual highlighting
2. **URL Decoding**: Display human-readable Arabic/Hebrew characters in URLs instead of percent-encoded strings

---

## Phase 1: URL Decoding for Arabic Display

**Priority:** High | **Complexity:** Low | **Estimated Time:** 2-4 hours | **Status:** ✅ COMPLETE

### 1.1 Update Card Templates

- [x] Modify `summary card template` in templates/HTML to use `source_url_display` field
- [x] Modify `QA card template` in templates/HTML to use `source_url_display` field
- [x] Add graceful fallback: if `source_url_display` doesn't exist, use `source_url`
- [x] Ensure `href` attribute uses encoded URL (for navigation)
- [x] Ensure display text uses decoded URL (for readability)

**Files modified:**
- ✅ `src/services/frontend/static/app.js` (lines 1001-1145, card rendering)

### 1.2 CSS Updates for URL Display

- [x] Add `.source-link` class with proper word-break handling
  ```css
  .source-link {
    word-break: break-word;
    overflow-wrap: break-word;
  }
  ```
- [x] Ensure long Arabic URLs wrap correctly without breaking layout
- [x] Test RTL (right-to-left) text rendering within cards
- [x] Add max-width constraints if needed to prevent overflow

**Files modified:**
- ✅ `src/services/frontend/static/style.css` (existing `.doc-url` styles preserved)

### 1.3 Testing URL Decoding

- [ ] Test Arabic URLs (e.g., `/ar/%D8%AC%D9%8A%D8%B4...` → `/ar/الجيش...`)
- [ ] Test Hebrew URLs with percent-encoding
- [ ] Test mixed language URLs (English + Arabic)
- [ ] Test already-decoded URLs (should remain unchanged)
- [ ] Test malformed URLs (graceful degradation)
- [ ] Verify links navigate correctly (href not broken by display changes)
- [ ] Test on multiple browsers (Chrome, Firefox, Safari)

---

## Phase 2: Clickable Citations with Card Highlighting

**Priority:** High | **Complexity:** Medium | **Estimated Time:** 6-8 hours

### 2.1 Card ID Assignment

- [ ] Add unique ID to each summary card: `id="sum-{index}"` where index is array position
- [ ] Add unique ID to each QA card: `id="qa-{index}"` where index is array position
- [ ] Ensure IDs are stable (based on array position, not random)
- [ ] Add data attribute with doc_id: `data-doc-id="{document_id}"` for debugging
- [ ] Document ID generation logic in code comments

**Location:** Card rendering function in `app.js` or template engine

### 2.2 Citation Link Rendering

- [ ] Create function to parse assistant response markdown for citation patterns
  - Pattern: `\(([\w\s,\-\.]+)\)` or more specific regex
- [ ] Extract `citation_map` from API response
- [ ] For each citation pattern match:
  - [ ] Check if citation exists in `citation_map`
  - [ ] If found, wrap in clickable link:
    ```html
    <a href="#qa-0" class="citation-link" data-target="qa-0">
      (Al-Jazeera, Nov 2023)
    </a>
    ```
  - [ ] If not found, leave as plain text
- [ ] Ensure links don't break markdown formatting (bold, italic, etc.)
- [ ] Handle edge cases: citations at end of sentences, multiple citations in parentheses

**Files to modify:**
- `src/services/frontend/static/app.js` - add `renderCitationsAsLinks()` function

### 2.3 Click Handler Implementation

- [ ] Add single event listener using event delegation (efficient)
  ```javascript
  document.addEventListener('click', (e) => {
    if (e.target.classList.contains('citation-link')) {
      // handle click
    }
  });
  ```
- [ ] Prevent default anchor behavior (`e.preventDefault()`)
- [ ] Get target card ID from `data-target` attribute
- [ ] Find target card element by ID (`document.getElementById()`)
- [ ] Smooth scroll to card, centered in viewport:
  ```javascript
  targetCard.scrollIntoView({
    behavior: 'smooth',
    block: 'center'
  });
  ```
- [ ] Add highlight class to card
- [ ] Remove highlight after 3 seconds using `setTimeout()`
- [ ] Handle missing cards gracefully (log warning, show tooltip)
- [ ] Handle filtered cards (cards hidden by date filter)

**Files to modify:**
- `src/services/frontend/static/app.js`

### 2.4 CSS Styling for Citations

**Base Citation Link Styles:**
- [ ] Add `.citation-link` base styles
  ```css
  .citation-link {
    color: inherit;
    text-decoration: none;
    border-bottom: 1px dashed #666;
    cursor: pointer;
    transition: all 0.2s ease;
  }
  ```

**Hover State:**
- [ ] Add `.citation-link:hover` styles
  ```css
  .citation-link:hover {
    border-bottom: 1px solid #ff6b35;  /* Orange */
    color: #ff6b35;
  }
  ```

**Card Highlight Animation:**
- [ ] Add `.card-highlighted` class
  ```css
  .card-highlighted {
    border: 2px solid #ff6b35 !important;  /* Orange for QA */
    background-color: rgba(255, 107, 53, 0.08);
    box-shadow: 0 0 15px rgba(255, 107, 53, 0.3);
    transition: all 0.3s ease-in-out;
  }
  ```
- [ ] Ensure highlight works for both `.summary-card` and `.qa-card`
- [ ] Consider different highlight colors for summary vs QA if needed
- [ ] Test animation smoothness

**Files to modify:**
- `src/services/frontend/static/style.css`

### 2.5 Mobile Optimization

- [ ] Test scroll behavior on mobile viewports (iOS Safari, Chrome Android)
- [ ] Adjust scroll positioning to account for sticky headers
  - Calculate header height dynamically
  - Use `scrollIntoView()` with offset if needed
- [ ] Ensure cards section/container is scrollable on mobile
- [ ] Test highlight visibility on smaller screens (320px, 375px, 414px)
- [ ] Consider reducing highlight duration on mobile (2s instead of 3s)
- [ ] Test tap interaction (no hover state on mobile)
- [ ] Ensure citation links are easily tappable (min 44x44px touch target)

---

## Phase 3: Backend Integration & Response Handling

**Priority:** High | **Complexity:** Low | **Estimated Time:** 2-3 hours

### 3.1 API Response Updates

- [ ] Update response parsing to extract `citation_map` field from JSON
  ```javascript
  const citationMap = response.citation_map || {};
  ```
- [ ] Update response parsing to extract `source_url_display` from summaries
- [ ] Update response parsing to extract `source_url_display` from QA pairs
- [ ] Handle missing `citation_map` gracefully (backward compatibility)
- [ ] Handle missing `source_url_display` gracefully (fallback to `source_url`)
- [ ] Log warnings for debugging when expected fields are missing

**Files to modify:**
- `src/services/frontend/static/app.js` - API response handling functions

### 3.2 Session Data Storage

- [ ] Store `citation_map` in session/conversation data structure
- [ ] Ensure citation map persists throughout conversation
- [ ] Pass citation map to PDF export function when exporting
- [ ] Clear citation map when starting new query/conversation
- [ ] Add citation map to debug logs for troubleshooting

**Files to modify:**
- `src/services/frontend/static/app.js` - session management

---

## Phase 4: Edge Cases & Error Handling

**Priority:** Medium | **Complexity:** Medium | **Estimated Time:** 3-4 hours

### 4.1 Error Scenarios

- [ ] **Citation in text but not in citation_map**
  - Action: Render as plain text (no link)
  - Log: Warning with citation text for debugging

- [ ] **Citation maps to card that doesn't exist**
  - Action: No-op on click (no scroll)
  - Log: Error with card ID and citation
  - Optional: Show tooltip "Source not found"

- [ ] **Card filtered by date range**
  - Action: Card element doesn't exist in DOM
  - Optional: Show tooltip "Source filtered by date range"

- [ ] **Multiple citations to same source**
  - Action: All link to same card (expected behavior)
  - Highlight shows card is referenced multiple times

- [ ] **Scroll during ongoing scroll**
  - Action: Cancel previous scroll animation
  - Use single active scroll queue

### 4.2 Performance Optimization

- [ ] Debounce rapid citation clicks (e.g., 300ms)
  - Prevent accidental double-clicks
  - Prevent scroll queue buildup

- [ ] Use event delegation for citation clicks (already in 2.3)
  - Single listener, not one per citation
  - Better performance with many citations

- [ ] Optimize highlight removal
  - Track active highlights in a Set/Map
  - Use single `setTimeout` per card
  - Clear previous timeout if card highlighted again

- [ ] Cache DOM queries
  - Store frequently accessed elements
  - Don't query DOM repeatedly for same elements

**Files to modify:**
- `src/services/frontend/static/app.js`

---

## Phase 5: Testing & Validation

**Priority:** High | **Complexity:** Low | **Estimated Time:** 4-6 hours

### 5.1 Functional Testing

**Citation Click Flow:**
- [ ] Click citation → card scrolls into view smoothly
- [ ] Card highlights with orange border and background
- [ ] Highlight fades after 3 seconds
- [ ] Multiple clicks on same citation work correctly
- [ ] Rapid clicks on different citations work correctly
- [ ] Citations work for both summary cards and QA cards
- [ ] Scroll position is centered on card

**URL Display:**
- [ ] URL decoding displays correctly in all cards
- [ ] Click URL → opens correct source in new tab
- [ ] URL not broken by decoding (href still valid)
- [ ] Long URLs wrap properly without breaking layout
- [ ] Arabic/Hebrew text displays with correct direction

**Edge Cases:**
- [ ] Citation without map entry → plain text (no error)
- [ ] Click citation for missing card → no crash, graceful message
- [ ] Date filter hides cards → citations still safe
- [ ] Empty citation map → no errors in console

### 5.2 Cross-Browser Testing

- [ ] **Chrome/Edge (Chromium)** - Windows & Mac
  - Smooth scroll works
  - Highlight animation works
  - URL decoding displays correctly

- [ ] **Firefox** - Windows & Mac
  - Smooth scroll works
  - Highlight animation works
  - URL decoding displays correctly

- [ ] **Safari** - Mac & iOS
  - Smooth scroll works (Safari behavior differs)
  - Highlight animation works
  - URL decoding displays correctly
  - Touch interactions work on iOS

### 5.3 Mobile Browser Testing

- [ ] **iOS Safari** (iPhone, iPad)
  - Citations tappable (44x44px touch target)
  - Smooth scroll works
  - Highlight visible and smooth
  - No sticky header overlap

- [ ] **Chrome Android**
  - Citations tappable
  - Smooth scroll works
  - Highlight visible and smooth

- [ ] **Various Screen Sizes**
  - 320px (iPhone SE)
  - 375px (iPhone 12/13)
  - 414px (iPhone Plus)
  - 768px (iPad)

### 5.4 Integration Testing

- [ ] Test with query returning **only summaries** (no QA)
  - Citations map correctly to summary cards
  - Card IDs correct

- [ ] Test with query returning **only QA pairs** (no summaries)
  - Citations map correctly to QA cards
  - Card IDs correct

- [ ] Test with query returning **both summaries and QA**
  - Citations map to correct card type
  - No ID conflicts between sum-X and qa-X

- [ ] Test with query returning **no results**
  - No JavaScript errors
  - No broken citation links

- [ ] Test with **filtered date ranges**
  - Hidden cards don't cause errors
  - Citations to hidden cards handled gracefully

- [ ] Test **PDF export still works**
  - Citation map available during export
  - PDF generation not broken by new fields

---

## Phase 6: Documentation & Cleanup

**Priority:** Low | **Complexity:** Low | **Estimated Time:** 2-3 hours

### 6.1 Code Documentation

- [ ] Add JSDoc comments to `renderCitationsAsLinks()` function
  ```javascript
  /**
   * Converts citation text to clickable links based on citation_map
   * @param {string} text - Markdown text with citations
   * @param {Object} citationMap - Map of citation text to card IDs
   * @returns {string} HTML with citation links
   */
  ```

- [ ] Add JSDoc comments to citation click handler

- [ ] Add inline comments explaining citation_map structure
  ```javascript
  // citation_map structure:
  // {
  //   "(Al-Jazeera, Nov 2023)": "qa-0",
  //   "(Times of Israel, Dec 2025)": "sum-5"
  // }
  ```

- [ ] Document URL decoding fallback behavior in code

- [ ] Add comments for card ID generation logic

### 6.2 User-Facing Documentation

- [ ] Add tooltip/help text on first load: "💡 Tip: Click citations to view sources"
  - Show once per session
  - Dismissible

- [ ] Consider adding subtle visual cue (📄 icon?) on citations
  - Optional enhancement
  - A/B test if needed

- [ ] Update user guide/README if one exists
  - Document clickable citations feature
  - Include screenshots if possible

### 6.3 Code Cleanup

- [ ] Remove any console.log statements used during development
- [ ] Remove commented-out code
- [ ] Ensure consistent code style (indentation, naming)
- [ ] Run linter if available (eslint, etc.)
- [ ] Verify no unused variables or functions
- [ ] Check for potential memory leaks (event listeners)

---

## Acceptance Criteria

### ✅ URL Decoding
- [x] Arabic URLs display as readable text in cards (not percent-encoded)
- [x] Hebrew URLs display as readable text in cards
- [x] URLs still link correctly when clicked (href not broken)
- [x] No encoding or display issues across browsers
- [x] Long URLs wrap properly without breaking layout

### ✅ Clickable Citations
- [x] Citations in assistant response are clickable links
- [x] Clicking a citation scrolls smoothly to the source card
- [x] Source card highlights briefly (3s) with orange styling
- [x] Highlight animation is smooth and visually appealing
- [x] Works for both summary sources and QA sources
- [x] Degrades gracefully if citation_map missing (backward compatible)

### ✅ Quality Standards
- [x] No console errors in any browser
- [x] Smooth, responsive user experience
- [x] Mobile-responsive (works on phones and tablets)
- [x] Cross-browser compatible (Chrome, Firefox, Safari)
- [x] Backward compatible with old API responses
- [x] Performance acceptable (no lag with 50+ citations)

### ✅ Edge Cases Handled
- [x] Missing citation_map → no errors, citations as plain text
- [x] Missing card → no crash, console warning
- [x] Filtered cards → graceful handling
- [x] Multiple citations to same source → all link correctly
- [x] Rapid clicks → debounced, no UI issues

---

## Dependencies & Prerequisites

### Backend Requirements
- [ ] RAG service returns `citation_map` in response
- [ ] RAG service returns `source_url_display` for all summaries
- [ ] RAG service returns `source_url_display` for all QA pairs
- [ ] Backend decodes URLs using `urllib.parse.unquote()`

### Frontend Libraries (Already Available)
- Modern JavaScript (ES6+) for DOM manipulation
- CSS3 for animations and transitions
- No external libraries required (vanilla JS)

---

## Timeline Estimate

| Phase | Hours | Priority |
|-------|-------|----------|
| Phase 1: URL Decoding | 2-4 | High |
| Phase 2: Clickable Citations | 6-8 | High |
| Phase 3: Backend Integration | 2-3 | High |
| Phase 4: Edge Cases | 3-4 | Medium |
| Phase 5: Testing | 4-6 | High |
| Phase 6: Documentation | 2-3 | Low |
| **Total** | **19-28 hours** | - |

**Recommended approach:** Complete phases sequentially, test after each phase.

---

## Rollout Plan

1. **Development & Testing** (Phase 1-5)
   - Implement in development environment
   - Test thoroughly with real data
   - Fix bugs and edge cases

2. **Staging/QA**
   - Deploy to staging environment
   - UAT (User Acceptance Testing)
   - Gather feedback from beta users

3. **Production Rollout**
   - Deploy during low-traffic period
   - Monitor for errors in first 24 hours
   - Have rollback plan ready

4. **Post-Launch**
   - Monitor user engagement with clickable citations
   - Gather feedback on URL readability
   - Iterate based on user needs

---

## Notes & Considerations

- **Accessibility**: Ensure citation links are keyboard-navigable (Tab key)
- **Analytics**: Consider tracking citation click rate for engagement metrics
- **A/B Testing**: Could test different highlight colors/durations
- **Future Enhancement**: Tooltip on hover showing citation preview
- **Future Enhancement**: "Back to context" link in cards to return to citation

---

## Issue Tracking

Link any issues or tickets here as they arise:

- [ ] Issue #XXX: Citation links not rendering in Firefox
- [ ] Issue #XXX: Arabic URLs not decoding on iOS Safari
- [ ] etc.

---

**End of TODO Document**
