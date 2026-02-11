# Q&A Cards Debugging Guide

## Status
✅ Backend: Confirmed returning `retrieved_qa` with data (tested with 2 QA pairs)
✅ Frontend: Code updated to receive and render `retrieved_qa`
🔍 Browser: Need to verify data flow in browser console

## How to Debug in Browser

### Step 1: Open Browser Developer Tools
1. Open your browser to the RAG frontend (http://192.168.1.3:5000 or your URL)
2. Press F12 or right-click → Inspect to open Developer Tools
3. Go to the **Console** tab

### Step 2: Run a Query with Q&A Enabled
1. Make sure the "Include Q&A grounding" checkbox is **checked** ✓
2. Set "Q&A top K" to 5 (or any number 1-50)
3. Enter a query and click Send

### Step 3: Check Console Logs
Look for these debug messages in the console:

```
📊 Query response data: {
    retrieved_docs_count: 25,
    retrieved_qa_count: 5,      ← Should be > 0
    retrieved_qa_present: true,  ← Should be true
    retrieved_qa_sample: {...}   ← Should show first QA pair
}

🎨 Calling updateDocumentList with: {
    docs_count: 25,
    qa_count: 5                  ← Should match retrieved_qa_count
}

🔍 updateDocumentList called with: {
    documents_type: "object",
    documents_count: 25,
    qaItems_type: "object",
    qaItems_count: 5,            ← Should be > 0
    qaItems_isArray: true,       ← Should be true
    qaItems_sample: {...}        ← Should show first QA pair structure
}
```

### Step 4: Verify QA Card Rendering
After the query completes, check the right panel (Document List):
- Should see **"Q&A Pairs (N)"** section header with orange styling
- Below it, orange-bordered cards with:
  - Title [qa-1], [qa-2], etc.
  - Author
  - Date
  - Relevance (orange percentage)
  - Question
  - Answer
  - Context (if available)
  - Link (orange)

### Common Issues & Solutions

#### Issue 1: `retrieved_qa_count: 0`
**Problem**: Backend not returning QA pairs
**Solution**:
- Verify "Include Q&A grounding" checkbox is checked
- Check backend logs: `docker logs rag-service | grep "Searching QA index"`
- Should see: "Returning N QA results"

#### Issue 2: `qaItems_count: 0` but `retrieved_qa_count > 0`
**Problem**: Data not passing to render function correctly
**Check**:
- Console log for `retrieved_qa_sample` - verify structure has:
  - question
  - answer
  - similarity
  - source_url
  - doc_metadata (with author, creation_date)

#### Issue 3: No orange cards visible but `qaItems_count > 0`
**Problem**: Rendering logic issue or CSS not loaded
**Check**:
- Look for JavaScript errors in console (red text)
- Verify CSS loaded: In Elements tab, search for `.qa-card` class
- Check if cards exist in DOM but hidden: In Elements tab, look for `<div class="document-item qa-card">`

#### Issue 4: Backend returns empty array
**Problem**: Query doesn't match any QA pairs
**Solution**:
- Try a different query (e.g., "brigades", "casualties", "operations")
- Check if QA index loaded: `docker logs rag-service | grep "Loaded.*QA pairs"`
- Should see: "✅ Loaded 1417 QA pairs"

## Quick Verification Commands

### Test Backend Directly
```bash
docker exec rag-service curl -s -X POST http://localhost:8000/query_llm \
  -H "Content-Type: application/json" \
  -d '{"query":"brigades","top_k":2,"use_qa":true,"qa_top_k":3}' \
  | python3 -c "import json,sys; d=json.load(sys.stdin); \
  print('retrieved_qa:', len(d.get('retrieved_qa', [])), 'items'); \
  print('First QA:', d.get('retrieved_qa', [{}])[0].get('question', 'N/A')[:80])"
```

### Check Frontend Logs
```bash
docker logs frontend-service 2>&1 | grep -i "retrieved_qa" | tail -5
```

### Rebuild Frontend (if changes not applied)
```bash
cd /home/electric/workspace/ScrapeLinkedWebSources
docker-compose -f docker-compose.services.yml build frontend-service
docker-compose -f docker-compose.services.yml stop frontend-service
docker-compose -f docker-compose.services.yml rm -f frontend-service
docker-compose -f docker-compose.services.yml up -d frontend-service
```

## Expected Output Structure

### Backend Response (`retrieved_qa`)
```json
{
  "retrieved_qa": [
    {
      "question": "How many Palestinians...",
      "answer": "According to medical sources...",
      "context": "إصابة فلسطينيين...",
      "source_url": "https://...",
      "doc_metadata": {
        "document_id": "doc_xyz",
        "title": "...",
        "author": "الجزيرة نت",
        "creation_date": "2023-02-04T04:54:27+00:00",
        ...
      },
      "similarity": 0.89
    }
  ]
}
```

### Frontend Rendering
Orange cards should appear ABOVE the blue summary documents cards, with:
- Orange left border (vs blue for summaries)
- Orange relevance badge background
- Orange link color
- Section header: "Q&A Pairs (N)" with orange accent

## Next Steps if Still Not Working
1. Share console logs from browser
2. Check Network tab → find `/api/query` request → Response tab → search for `retrieved_qa`
3. Verify the response contains the array with QA items
4. If present in response but not rendering, check for JavaScript errors
