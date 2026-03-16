// ============================================================================
// RAG Query Interface - Main JavaScript
// ============================================================================

let conversations = [];
let currentConversationId = null;
let currentConversationMessages = []; // Track all messages in current conversation
let currentAbortController = null; // For canceling requests

// Map: messageId → { rawContent, metadata } — used to restore/re-render during edit mode
const messageEditState = new Map();

// Global Settings State
let settings = {
    metadata: {
        index_name: "research_summaries_with_embeddings_combined",
        index_version: "2026-02-06",
        description: "This document mines information related to the Israeli Defense Force and recent developments in the Israeli-Palestinian conflict. Specifically, the perspective from Middle Eastern Media",
        source: "Multiple Arabic and English sources",
        coverage: "2023-2026",
        entry_point: "Doha-Qatar",
        locale: "Modern Standard Arabic",
        query: "Israeli Defense Force Latest News",
        llm_model: "gemini-2.5-pro"
    },
    query: {
        summaries_top_k: 25
    },
    qa: {
        include_qa: true,
        qa_top_k: 5
    },
    display: {
        citation_debug: false
    },
    enrichment: {
        enabled: false,
        types: ["all"],
        get_linked: false
    }
};

// DOM Elements
const chatContainer = document.getElementById('chatContainer');
const queryInput = document.getElementById('queryInput');
const sendButton = document.getElementById('sendButton');
const cancelButton = document.getElementById('cancelButton');
const queryList = document.getElementById('queryList');
const documentList = document.getElementById('documentList');
const startDateInput = document.getElementById('startDate');
const endDateInput = document.getElementById('endDate');
const clearDatesButton = document.getElementById('clearDates');
const queryModeSelect = document.getElementById('queryMode');
const notificationOverlay = document.getElementById('notificationOverlay');
const notificationMessage = document.getElementById('notificationMessage');
const dismissNotification = document.getElementById('dismissNotification');
const exportButton = document.getElementById('exportButton');
const importButton = document.getElementById('importButton');
const importFileInput = document.getElementById('importFileInput');

// Settings Panel DOM Elements
const settingsButton = document.getElementById('settingsButton');
const settingsOverlay = document.getElementById('settingsOverlay');
const settingsPanel = document.getElementById('settingsPanel');
const closeSettingsBtn = document.getElementById('closeSettings');
const publishRecentBtn = document.getElementById('publishRecent');

// Settings Input Elements
const metaIndexName = document.getElementById('metaIndexName');
const metaIndexVersion = document.getElementById('metaIndexVersion');
const metaDescription = document.getElementById('metaDescription');
const metaSource = document.getElementById('metaSource');
const metaCoverage = document.getElementById('metaCoverage');
const metaEntryPoint = document.getElementById('metaEntryPoint');
const metaLocale = document.getElementById('metaLocale');
const metaQuery = document.getElementById('metaQuery');
const summariesTopKInput = document.getElementById('summariesTopK');
const includeQaCheckbox = document.getElementById('includeQa');
const qaTopKInput = document.getElementById('qaTopK');
const citationDebugCheckbox = document.getElementById('citationDebug');
const enrichToggle = document.getElementById('enrichToggle');
const enrichGetLinked = document.getElementById('enrichGetLinked');

// Index Corpus DOM Elements
const indexFolderSelect = document.getElementById('indexFolderSelect');
const switchIndexBtn = document.getElementById('switchIndexBtn');
const switchIndexStatus = document.getElementById('switchIndexStatus');

// EO / Events Overlay DOM Elements
const eoIndexToggle = document.getElementById('eoIndexToggle');
const eventsOverlayToggle = document.getElementById('eventsOverlayToggle');
const eoTopKInput = document.getElementById('eoTopKInput');
const eoGroundingModeSelect = document.getElementById('eoGroundingMode');
const eoToggleStatus = document.getElementById('eoToggleStatus');

// ============================================================================
// Server-Sent Events for Index Update Notifications
// ============================================================================

let eventSource = null;
let reconnectAttempts = 0;
const MAX_RECONNECT_DELAY = 30000; // Max 30 seconds

function initializeNotifications() {
    // Close existing connection if any
    if (eventSource) {
        eventSource.close();
    }

    try {
        eventSource = new EventSource('/api/notifications');

        eventSource.onopen = () => {
            console.log('✅ SSE connection opened');
            reconnectAttempts = 0; // Reset on successful connection
        };

        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                console.log('📬 Notification received:', data);

                if (data.type === 'index_update') {
                    showNotification(data);
                } else if (data.type === 'connected') {
                    console.log('✅ Connected to notification stream');
                }
            } catch (e) {
                console.error('Error parsing notification:', e);
            }
        };

        eventSource.onerror = (error) => {
            console.error('❌ SSE connection error:', error);
            eventSource.close();

            // Exponential backoff for reconnection
            const delay = Math.min(1000 * 2 ** reconnectAttempts, MAX_RECONNECT_DELAY);
            reconnectAttempts += 1;
            console.log(`⏳ Reconnecting to notifications in ${delay}ms`);
            setTimeout(initializeNotifications, delay);
        };

        console.log('🔔 Notification stream initialized');
    } catch (error) {
        console.error('Failed to initialize notifications:', error);
    }
}

function showNotification(data) {
    const newCount = data.new_summaries_count || data.new_summaries || 0;
    const totalCount = data.total_summaries_count || data.total_summaries || 0;

    // Update notification message
    notificationMessage.textContent = `${newCount} new summaries added (Total: ${totalCount})`;

    // Show the overlay
    notificationOverlay.classList.remove('hidden');

    // Auto-dismiss after 10 seconds
    setTimeout(() => {
        notificationOverlay.classList.add('hidden');
    }, 10000);
}

dismissNotification.addEventListener('click', () => {
    notificationOverlay.classList.add('hidden');
});

// ============================================================================
// Settings Panel Functions
// ============================================================================

function toggleSettingsPanel() {
    const isHidden = settingsPanel.classList.contains('hidden');
    if (isHidden) {
        settingsPanel.classList.remove('hidden');
        settingsOverlay.classList.remove('hidden');
    } else {
        settingsPanel.classList.add('hidden');
        settingsOverlay.classList.add('hidden');
    }
}

function closeSettingsPanel() {
    settingsPanel.classList.add('hidden');
    settingsOverlay.classList.add('hidden');
}

function loadSettings() {
    try {
        const saved = localStorage.getItem('rag_settings');
        if (saved) {
            const parsed = JSON.parse(saved);
            // Merge saved settings with defaults
            settings = {
                metadata: { ...settings.metadata, ...(parsed.metadata || {}) },
                query: { ...settings.query, ...(parsed.query || {}) },
                qa: { ...settings.qa, ...(parsed.qa || {}) },
                display: { ...settings.display, ...(parsed.display || {}) },
                enrichment: { ...settings.enrichment, ...(parsed.enrichment || {}) }
            };
            console.log('✅ Settings loaded from localStorage');
        }
    } catch (e) {
        console.error('Failed to load settings:', e);
    }

    // Populate UI with loaded settings
    metaIndexName.value = settings.metadata.index_name || '';
    metaIndexVersion.value = settings.metadata.index_version || '';
    metaDescription.value = settings.metadata.description || '';
    metaSource.value = settings.metadata.source || '';
    metaCoverage.value = settings.metadata.coverage || '';
    metaEntryPoint.value = settings.metadata.entry_point || '';
    metaLocale.value = settings.metadata.locale || '';
    metaQuery.value = settings.metadata.query || '';
    summariesTopKInput.value = settings.query.summaries_top_k || 25;
    includeQaCheckbox.checked = settings.qa.include_qa !== false;
    qaTopKInput.value = settings.qa.qa_top_k || 5;
    qaTopKInput.disabled = !includeQaCheckbox.checked;
    if (citationDebugCheckbox) {
        citationDebugCheckbox.checked = settings.display.citation_debug === true;
    }
    // Enrichment settings
    if (enrichToggle) enrichToggle.checked = settings.enrichment.enabled === true;
    if (enrichGetLinked) enrichGetLinked.checked = settings.enrichment.get_linked === true;
    const _enrichTypes = settings.enrichment.types || ['all'];
    const _allTypes = _enrichTypes.includes('all');
    document.querySelectorAll('.enrich-pill').forEach(pill => {
        const t = pill.dataset.type;
        pill.classList.toggle('active', _allTypes || _enrichTypes.includes(t));
    });
}

function saveSettings() {
    try {
        // Update settings object from UI
        settings.metadata.index_name = metaIndexName.value;
        settings.metadata.index_version = metaIndexVersion.value;
        settings.metadata.description = metaDescription.value;
        settings.metadata.source = metaSource.value;
        settings.metadata.coverage = metaCoverage.value;
        settings.metadata.entry_point = metaEntryPoint.value;
        settings.metadata.locale = metaLocale.value;
        settings.metadata.query = metaQuery.value;
        settings.query.summaries_top_k = parseInt(summariesTopKInput.value, 10) || 25;
        settings.qa.include_qa = includeQaCheckbox.checked;
        settings.qa.qa_top_k = parseInt(qaTopKInput.value, 10) || 5;
        settings.display.citation_debug = citationDebugCheckbox ? citationDebugCheckbox.checked : false;
        // Enrichment settings
        settings.enrichment.enabled = enrichToggle ? enrichToggle.checked : false;
        settings.enrichment.get_linked = enrichGetLinked ? enrichGetLinked.checked : false;
        const _activePills = [...document.querySelectorAll('.enrich-pill.active')].map(p => p.dataset.type);
        settings.enrichment.types = _activePills.length === 4 ? ['all'] : (_activePills.length > 0 ? _activePills : ['all']);

        // Save to localStorage
        localStorage.setItem('rag_settings', JSON.stringify(settings));
        console.log('💾 Settings saved to localStorage');
    } catch (e) {
        console.error('Failed to save settings:', e);
    }
}

// Debounce function for input events
let saveDebounceTimer = null;
function debouncedSave() {
    clearTimeout(saveDebounceTimer);
    saveDebounceTimer = setTimeout(saveSettings, 500);
}

async function exportMostRecentMessage() {
    try {
        // Find most recent assistant message
        if (currentConversationMessages.length === 0) {
            alert('No conversation to export');
            return;
        }

        const lastConversation = currentConversationMessages[currentConversationMessages.length - 1];

        const payload = {
            query: lastConversation.user_query,
            assistant_response: lastConversation.edited_content || lastConversation.assistant_response,
            message_id: lastConversation.assistant_message_id,
            metadata: settings.metadata
        };

        console.log('📄 Exporting PDF with metadata:', payload.metadata);

        const response = await fetch('/api/export_pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        // Download the PDF
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `rag-export-${Date.now()}.pdf`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);

        console.log('✅ PDF exported successfully');
    } catch (error) {
        console.error('Error exporting PDF:', error);
        alert(`Failed to export PDF: ${error.message}`);
    }
}

// Settings Panel Event Listeners
settingsButton.addEventListener('click', toggleSettingsPanel);
closeSettingsBtn.addEventListener('click', closeSettingsPanel);
settingsOverlay.addEventListener('click', closeSettingsPanel);
publishRecentBtn.addEventListener('click', exportMostRecentMessage);

if (switchIndexBtn) {
    switchIndexBtn.addEventListener('click', switchIndex);
}

// ============================================================================
// EO / Events Overlay
// ============================================================================

function updateEoControlsState() {
    const enabled = eoIndexToggle && eoIndexToggle.checked;
    if (eoGroundingModeSelect) eoGroundingModeSelect.disabled = !enabled;
    if (eoTopKInput) eoTopKInput.disabled = !enabled;
    if (eventsOverlayToggle) eventsOverlayToggle.disabled = !enabled;
}

async function syncEoStatus() {
    try {
        const resp = await fetch('/api/eo-status');
        if (!resp.ok) return;
        const data = await resp.json();
        if (eoIndexToggle) eoIndexToggle.checked = data.eo_overlay_enabled === true;
        if (eventsOverlayToggle) eventsOverlayToggle.checked = data.events_overlay_enabled === true;
        if (eoTopKInput && data.eo_top_k != null) eoTopKInput.value = data.eo_top_k;
        if (eoGroundingModeSelect && data.eo_grounding_mode) eoGroundingModeSelect.value = data.eo_grounding_mode;
        updateEoControlsState();
    } catch (e) {
        console.warn('Could not sync EO status:', e);
    }
}

async function applyEoToggle() {
    if (!eoIndexToggle || !eventsOverlayToggle) return;
    const payload = {
        eo_overlay_enabled: eoIndexToggle.checked,
        events_overlay_enabled: eventsOverlayToggle.checked,
        eo_top_k: parseInt(eoTopKInput ? eoTopKInput.value : '10', 10) || 10,
        eo_grounding_mode: eoGroundingModeSelect ? eoGroundingModeSelect.value : 'full_doc'
    };
    if (eoToggleStatus) eoToggleStatus.textContent = '⏳';
    try {
        const resp = await fetch('/api/toggle-eo-events', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (resp.ok) {
            updateEoControlsState();
            if (eoToggleStatus) {
                const mode = data.eo_grounding_mode || '';
                const qa = data.eo_qa_pairs_loaded || 0;
                const statusParts = [];
                if (data.eo_overlay_enabled) statusParts.push(mode === 'full_doc' ? `Full Doc (${qa} QA)` : 'Summary');
                eoToggleStatus.textContent = statusParts.length ? `✅ ${statusParts.join(', ')}` : '✅ Off';
                setTimeout(() => { if (eoToggleStatus) eoToggleStatus.textContent = ''; }, 3000);
            }
        } else {
            if (eoToggleStatus) eoToggleStatus.textContent = `❌ ${data.detail || data.error || ''}`;
            // Revert to actual server state on error
            await syncEoStatus();
        }
    } catch (e) {
        if (eoToggleStatus) eoToggleStatus.textContent = `❌ ${e.message}`;
    }
}

if (eoIndexToggle) eoIndexToggle.addEventListener('change', applyEoToggle);
if (eventsOverlayToggle) eventsOverlayToggle.addEventListener('change', applyEoToggle);
if (eoGroundingModeSelect) eoGroundingModeSelect.addEventListener('change', applyEoToggle);
let eoTopKDebounceTimer = null;
if (eoTopKInput) {
    eoTopKInput.addEventListener('input', () => {
        clearTimeout(eoTopKDebounceTimer);
        eoTopKDebounceTimer = setTimeout(applyEoToggle, 600);
    });
}

// Load index folders when settings panel is opened
settingsButton.addEventListener('click', () => {
    if (!settingsPanel.classList.contains('hidden')) {
        loadIndexFolders();
    }
});

// ESC key to close settings
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !settingsPanel.classList.contains('hidden')) {
        closeSettingsPanel();
    }
});

// Settings input event listeners
metaIndexName.addEventListener('input', debouncedSave);
metaIndexVersion.addEventListener('input', debouncedSave);
metaDescription.addEventListener('input', debouncedSave);
metaSource.addEventListener('input', debouncedSave);
metaCoverage.addEventListener('input', debouncedSave);
metaEntryPoint.addEventListener('input', debouncedSave);
metaLocale.addEventListener('input', debouncedSave);
metaQuery.addEventListener('input', debouncedSave);
summariesTopKInput.addEventListener('input', debouncedSave);
includeQaCheckbox.addEventListener('change', () => {
    qaTopKInput.disabled = !includeQaCheckbox.checked;
    saveSettings();
});
qaTopKInput.addEventListener('input', debouncedSave);
if (citationDebugCheckbox) {
    citationDebugCheckbox.addEventListener('change', () => {
        saveSettings();
    });
}
[enrichToggle, enrichGetLinked].forEach(el => {
    if (el) el.addEventListener('change', () => { saveSettings(); });
});
document.querySelectorAll('.enrich-pill').forEach(pill => {
    pill.addEventListener('click', () => {
        pill.classList.toggle('active');
        saveSettings();
    });
});

// ============================================================================
// Event Listeners
// ============================================================================

sendButton.addEventListener('click', handleSendQuery);

queryInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        handleSendQuery();
    }
});

clearDatesButton.addEventListener('click', () => {
    startDateInput.value = '';
    endDateInput.value = '';
});

// Mode change - keep context, just switch mode
queryModeSelect.addEventListener('change', () => {
    const newMode = queryModeSelect.value;
    console.log(`🔄 Mode changed to: ${newMode} (context preserved)`);
});

// Cancel button handler
cancelButton.addEventListener('click', () => {
    if (currentAbortController) {
        currentAbortController.abort();
        currentAbortController = null;
    }
});

// Export button handler
exportButton.addEventListener('click', () => {
    exportSession();
});

// Import button handler
importButton.addEventListener('click', () => {
    importFileInput.click();
});

// File input handler
importFileInput.addEventListener('change', (e) => {
    const file = e.target.files[0];
    if (file) {
        importSession(file);
        e.target.value = ''; // Reset input
    }
});

// Citation link click handler - using event delegation
document.addEventListener('click', (e) => {
    if (e.target.classList.contains('citation-link')) {
        e.preventDefault();

        // Try new dynamic lookup first (docId + refType)
        const docId = e.target.dataset.docId;
        const refType = e.target.dataset.refType;

        if (docId && refType) {
            // New method: lookup by document_id
            console.log(`📍 Citation clicked: docId='${docId}', refType='${refType}'`);
            scrollToAndHighlightCard(docId, refType);
        } else {
            // Fallback: legacy card ID lookup
            const cardId = e.target.dataset.cardId;
            console.log(`📍 Citation clicked (legacy): cardId='${cardId}'`);
            scrollToAndHighlightCard(cardId);
        }
    }
});

// Load conversations on page load
window.addEventListener('load', () => {
    loadSettings(); // Load settings from localStorage first
    clearConversation(); // Fresh context on page reload
    loadConversations();
    initializeNotifications();  // Start listening for index updates
    loadIndexFolders();          // Populate index corpus dropdown
    syncEoStatus();              // Sync EO overlay toggle state from server
});

// ============================================================================
// Index Corpus Switching
// ============================================================================

async function loadIndexFolders() {
    if (!indexFolderSelect) return;
    try {
        const resp = await fetch('/api/index-folders');
        if (!resp.ok) return;
        const data = await resp.json();
        indexFolderSelect.innerHTML = '';
        (data.folders || []).forEach(f => {
            const opt = document.createElement('option');
            opt.value = f.name;
            opt.textContent = f.name.replace('x_indices_', '').toUpperCase() +
                (f.has_enrichment ? ' ✦' : '') +
                (f.has_qa ? '' : ' (no QA)');
            if (f.active) opt.selected = true;
            opt.title = `summaries: ${f.has_summaries ? 'yes' : 'no'} | QA: ${f.has_qa ? 'yes' : 'no'} | enrichment: ${f.has_enrichment ? 'yes' : 'no'}`;
            indexFolderSelect.appendChild(opt);
        });
    } catch (e) {
        console.warn('Could not load index folders:', e);
    }
}

async function switchIndex() {
    if (!indexFolderSelect || !switchIndexBtn) return;
    const folder = indexFolderSelect.value;
    if (!folder) return;

    switchIndexBtn.disabled = true;
    switchIndexStatus.textContent = '⏳ Switching...';

    try {
        const resp = await fetch('/api/switch-index', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ folder }),
        });
        const data = await resp.json();
        if (resp.ok && data.switched) {
            switchIndexStatus.textContent =
                `✅ Switched to ${folder.replace('x_indices_', '').toUpperCase()} ` +
                `(${data.summaries} summaries)`;
            await loadIndexFolders(); // Refresh to reflect active flag
        } else {
            switchIndexStatus.textContent = `❌ ${data.error || data.detail || 'Switch failed'}`;
        }
    } catch (e) {
        switchIndexStatus.textContent = `❌ ${e.message}`;
    } finally {
        switchIndexBtn.disabled = false;
        // Clear status after 5s
        setTimeout(() => { switchIndexStatus.textContent = ''; }, 5000);
    }
}

// ============================================================================
// Main Functions
// ============================================================================

async function handleSendQuery() {
    const query = queryInput.value.trim();

    if (!query) {
        return;
    }

    // Clear welcome message if exists
    const welcomeMessage = chatContainer.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.remove();
    }

    // Add user message to chat and current conversation
    const userMessageId = addMessage('user', query);

    // Clear input
    queryInput.value = '';
    queryInput.style.height = 'auto';

    // Scroll to bottom when starting a new query
    scrollToBottom();

    // Disable input while processing
    queryInput.disabled = true;
    sendButton.disabled = true;

    // Show cancel button, hide send button
    sendButton.style.display = 'none';
    cancelButton.style.display = 'flex';

    // Create new abort controller for this request
    currentAbortController = new AbortController();

    // Show loading indicator
    const loadingId = addLoadingMessage();

    try {
        // Read QA settings from global settings object
        const useQa = settings.qa.include_qa;
        const qaTopK = Math.min(Math.max(settings.qa.qa_top_k, 1), 50);
        const summariesTopK = Math.min(Math.max(settings.query.summaries_top_k, 1), 100);

        // Prepare request payload
        const payload = {
            query,
            mode: queryModeSelect.value,
            context_depth: 3,
            top_k: summariesTopK,
            temperature: 0.5,
            max_tokens: 16000,
            use_qa: useQa,
            qa_top_k: qaTopK,
            enrich: settings.enrichment.enabled || false,
            enrich_types: settings.enrichment.types || ['all'],
            get_linked: settings.enrichment.get_linked || false
        };

        // Add date filters if set
        if (startDateInput.value) {
            payload.start_date = startDateInput.value;
        }
        if (endDateInput.value) {
            payload.end_date = endDateInput.value;
        }

        // Call backend API
        const response = await fetch('/api/query', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload),
            signal: currentAbortController.signal
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        // Remove loading indicator
        removeLoadingMessage(loadingId);

        // Ensure metadata exists and has message_id for PDF export
        if (!data.metadata) {
            data.metadata = {};
        }
        if (data.metadata.message_id === undefined || data.metadata.message_id === null) {
            data.metadata.message_id = data.id;
        }

        // Add assistant response
        const assistantMessageId = addMessage('assistant', data.assistant_response, data.metadata);

        // Store conversation exchange with message IDs
        const conversationEntry = {
            id: data.id,
            timestamp: data.timestamp,
            user_query: data.user_query,
            user_message_id: userMessageId,
            assistant_response: data.assistant_response,
            assistant_message_id: assistantMessageId,
            retrieved_docs: data.retrieved_docs,
            retrieved_qa: data.retrieved_qa || [],
            enrichment_results: data.enrichment_results || [],
            eo_results: data.eo_results || [],
            event_results: data.event_results || [],
            metadata: data.metadata,
            // Edit state (5.1)
            edited_content: null,
            edited: false,
            edited_at: null
        };

        // Debug logging
        console.log('📊 Query response data:', {
            retrieved_docs_count: data.retrieved_docs?.length || 0,
            retrieved_qa_count: data.retrieved_qa?.length || 0,
            retrieved_qa_present: !!data.retrieved_qa,
            retrieved_qa_sample: data.retrieved_qa?.[0],
            metadata: data.metadata,
            metadata_message_id: data.metadata?.message_id
        });

        // Add to conversations list
        conversations.push(conversationEntry);
        currentConversationMessages.push(conversationEntry);

        // Update or add query to list
        updateQueryList(conversationEntry);

        // Update document list with latest results (summaries and QA)
        console.log('🎨 Calling updateDocumentList with:', {
            docs_count: data.retrieved_docs?.length || 0,
            qa_count: (data.retrieved_qa || []).length
        });
        updateDocumentList(data.retrieved_docs, data.retrieved_qa || [], data.enrichment_results || [], data.eo_results || [], data.event_results || []);

        // Set as current conversation
        currentConversationId = data.id;
        highlightCurrentQuery();

    } catch (error) {
        console.error('Error sending query:', error);
        removeLoadingMessage(loadingId);

        if (error.name === 'AbortError') {
            addMessage('assistant', '🚫 Request canceled by user.');
        } else {
            addMessage('assistant', `Error: ${error.message}. Please ensure the RAG service is running.`);
        }
    } finally {
        // Reset UI state
        queryInput.disabled = false;
        sendButton.disabled = false;
        sendButton.style.display = 'flex';
        cancelButton.style.display = 'none';
        currentAbortController = null;
        queryInput.focus();
    }
}

/**
 * Convert citation placeholders into clickable links
 * @param {string} content - The markdown/HTML content with citation placeholders
 * @param {Object} citationMap - Map of doc_id to citation info {author, date, card_type, card_index}
 * @returns {string} Content with citations as clickable links
 */
function renderCitationsAsLinks(content, citationMap, citationDebug) {
    console.log('🔗 renderCitationsAsLinks called, debug:', citationDebug);
    console.log('   Citation map keys:', citationMap ? Object.keys(citationMap).length : 0);

    if (!citationMap || Object.keys(citationMap).length === 0) {
        console.warn('⚠️ No citation map provided or empty');
        return content;
    }

    // Step 1: Split semicolon-separated citation groups into individual citations.
    // e.g. "(doc_XXX, qa-7; doc_YYY, qa-15)" → "(doc_XXX, qa-7); (doc_YYY, qa-15)"
    let processed = content.replace(/\(\s*([^)]*;[^)]*)\)/g, (match, inner) => {
        // Only split if it looks like citation pairs (contains doc_ or req_)
        if (!inner.match(/doc_|req_/i)) return match;
        const parts = inner.split(/;\s*/).map(p => `(${p.trim()})`);
        return parts.join('; ');
    });

    // Step 1b: Normalize fused citations where LLM omits comma: (req_XXX_summary) → (req_XXX, summary)
    // Also handles (req_XXX_qa-N) → (req_XXX, qa-N)
    processed = processed.replace(/\(\s*((?:doc_|req_)[a-zA-Z0-9_]+?)_(summary|qa-\d+)\s*\)/gi, (match, docId, refType) => {
        console.log(`   🔧 Fused citation normalized: ${match} → (${docId}, ${refType})`);
        return `(${docId}, ${refType})`;
    });

    // Step 1c: Normalize prefixed citations: (e.g., req_XXX, summary) → (req_XXX, summary)
    // Strips any freetext before the doc_id when a type suffix follows
    processed = processed.replace(/\(\s*[^)]*?,\s*((?:doc_|req_)[a-zA-Z0-9_]+),\s*(summary|qa-\d+)\s*\)/gi, (match, docId, refType) => {
        console.log(`   🔧 Prefixed citation normalized: ${match} → (${docId}, ${refType})`);
        return `(${docId}, ${refType})`;
    });

    // Step 2: Match individual citations: (doc_XXX, qa-N) or (doc_XXX, summary) or (req_XXX, summary)
    // IMPORTANT: Pattern must match uppercase letters for req_ prefix
    const citationPattern = /\(\s*([a-zA-Z0-9_]+),\s*(qa-\d+|summary)\s*\)/gi;

    // Sentinel to mark resolved citations for post-processing
    const CITE_SENTINEL = '\x00CITE';
    let resolvedCitations = []; // collect citation objects for grouping
    let replacementCount = 0;

    let result = processed.replace(citationPattern, (match, docId, refType) => {
        // Try composite key first (for QA: "doc_XXX__qa-N"), then plain docId (for summaries)
        const compositeKey = `${docId}__${refType}`;
        const citationInfo = citationMap[compositeKey] || citationMap[docId];

        if (!citationInfo) {
            console.warn(`   ⚠️ Citation NOT RESOLVED: (${docId}, ${refType})`);
            console.warn(`      Tried composite_key='${compositeKey}' and doc_id='${docId}'`);
            console.warn(`      Available keys in citation_map:`, Object.keys(citationMap).slice(0, 20));
            return match;
        }

        const cardId = `${citationInfo.card_type}-${citationInfo.card_index}`;
        const isQa = citationInfo.card_type === 'qa';
        replacementCount++;

        if (citationDebug) {
            // Debug mode: always show [qa-N] for QA citations, render individually
            let displayText;
            if (isQa) {
                const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
                displayText = `(${text} [${refType}])`;
            } else {
                const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
                displayText = `(${text})`;
            }
            return `<a href="#${cardId}" class="citation-link" data-card-id="${cardId}" data-doc-id="${docId}" data-ref-type="${refType}">${displayText}</a>`;
        }

        // Non-debug mode: emit a sentinel placeholder for later grouping
        const idx = resolvedCitations.length;
        resolvedCitations.push({
            author: citationInfo.author,
            date: citationInfo.date,
            url: citationInfo.url || '',
            cardId: cardId,
            docId: docId,
            refType: refType,
            isQa: isQa,
            // Parse a sortable date value for ordering within groups
            dateSort: parseDateForSort(citationInfo.date)
        });
        return `${CITE_SENTINEL}[${idx}]`;
    });

    // Step 2b: Second pass — catch bare (doc_id) or (doc_id, doc_id, ...) where LLM omitted ref types
    // Matches single or comma-separated lists of doc_/req_ IDs with no qa-N/summary suffix
    const bareCitationPattern = /\(\s*((?:(?:doc_|req_)[a-zA-Z0-9_]+)(?:\s*,\s*(?:doc_|req_)[a-zA-Z0-9_]+)*)\s*\)/gi;
    result = result.replace(bareCitationPattern, (match, inner) => {
        // Split comma-separated doc_ids
        const docIds = inner.split(/\s*,\s*/).map(s => s.trim()).filter(Boolean);
        const sentinels = [];
        let anyResolved = false;

        for (const docId of docIds) {
            const citationInfo = citationMap[docId];
            if (!citationInfo) {
                console.warn(`   ⚠️ Bare citation NOT resolved: ${docId}`);
                continue; // Skip unknown doc_ids
            }

            const cardId = `${citationInfo.card_type}-${citationInfo.card_index}`;
            replacementCount++;
            anyResolved = true;
            console.log(`   🔧 Bare citation resolved: (${docId}) → ${citationInfo.author}, ${citationInfo.date}`);

            if (citationDebug) {
                const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
                sentinels.push(`<a href="#${cardId}" class="citation-link" data-card-id="${cardId}" data-doc-id="${docId}" data-ref-type="summary">(${text})</a>`);
            } else {
                const idx = resolvedCitations.length;
                resolvedCitations.push({
                    author: citationInfo.author,
                    date: citationInfo.date,
                    url: citationInfo.url || '',
                    cardId: cardId,
                    docId: docId,
                    refType: 'summary',
                    isQa: citationInfo.card_type === 'qa',
                    dateSort: parseDateForSort(citationInfo.date)
                });
                sentinels.push(`${CITE_SENTINEL}[${idx}]`);
            }
        }

        if (!anyResolved) return match; // None resolved, leave original
        if (citationDebug) return sentinels.join('; ');
        return sentinels.join(', '); // Comma-separated sentinels will group in Step 3
    });

    // Step 2c: Third pass — catch "(text - req_XXX)" or "(text - doc_XXX, req_YYY)" where LLM embeds doc_id(s) after a dash
    // e.g. "(July 2024 - req_20260203_072214_001)" or "(IAF strikes - req_XXX, req_YYY)"
    const dashDocIdPattern = /\(\s*([^)]+?)\s*[-–—]\s*((?:(?:doc_|req_)[a-zA-Z0-9_]+(?:\s*,\s*)?)+)\s*\)/gi;
    result = result.replace(dashDocIdPattern, (match, textBefore, docIdsPart) => {
        // Split the doc_ids portion on commas
        const docIds = docIdsPart.split(/\s*,\s*/).filter(id => /^(doc_|req_)/i.test(id));
        if (docIds.length === 0) return match;

        const sentinels = [];
        let anyResolved = false;
        for (const docId of docIds) {
            const citationInfo = citationMap[docId];
            if (!citationInfo) {
                console.warn(`   ⚠️ Dash-separated citation NOT resolved: ${docId}`);
                continue;
            }
            anyResolved = true;
            const cardId = `${citationInfo.card_type}-${citationInfo.card_index}`;
            replacementCount++;
            console.log(`   🔧 Dash citation resolved: (${textBefore} - ${docId}) → ${citationInfo.author}, ${citationInfo.date}`);

            if (citationDebug) {
                const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
                sentinels.push(`<a href="#${cardId}" class="citation-link" data-card-id="${cardId}" data-doc-id="${docId}" data-ref-type="summary">(${text})</a>`);
            } else {
                const idx = resolvedCitations.length;
                resolvedCitations.push({
                    author: citationInfo.author,
                    date: citationInfo.date,
                    url: citationInfo.url || '',
                    cardId: cardId,
                    docId: docId,
                    refType: 'summary',
                    isQa: citationInfo.card_type === 'qa',
                    dateSort: parseDateForSort(citationInfo.date)
                });
                sentinels.push(`${CITE_SENTINEL}[${idx}]`);
            }
        }

        if (!anyResolved) return match;
        if (citationDebug) return sentinels.join('; ');
        return sentinels.join(', ');
    });

    // Step 2d: Fourth pass — catch "(freetext, req_XXX)" or "(freetext, doc_XXX)"
    // e.g. "(Gabriel-5, req_20260108_014029_001)" where LLM mixes description with doc_id
    const commaDocIdPattern = /\(\s*([^)]+?),\s*((?:doc_|req_)[a-zA-Z0-9_]+)\s*\)/gi;
    result = result.replace(commaDocIdPattern, (match, textBefore, docId) => {
        // Skip if textBefore itself is a doc_/req_ id (already handled by Step 2)
        if (/^(doc_|req_)/i.test(textBefore.trim())) return match;
        const citationInfo = citationMap[docId];
        if (!citationInfo) {
            console.warn(`   ⚠️ Comma-separated citation NOT resolved: ${docId}`);
            return match;
        }

        const cardId = `${citationInfo.card_type}-${citationInfo.card_index}`;
        replacementCount++;
        console.log(`   🔧 Comma citation resolved: (${textBefore}, ${docId}) → ${citationInfo.author}, ${citationInfo.date}`);

        if (citationDebug) {
            const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
            return `<a href="#${cardId}" class="citation-link" data-card-id="${cardId}" data-doc-id="${docId}" data-ref-type="summary">(${text})</a>`;
        } else {
            const idx = resolvedCitations.length;
            resolvedCitations.push({
                author: citationInfo.author,
                date: citationInfo.date,
                url: citationInfo.url || '',
                cardId: cardId,
                docId: docId,
                refType: 'summary',
                isQa: citationInfo.card_type === 'qa',
                dateSort: parseDateForSort(citationInfo.date)
            });
            return `${CITE_SENTINEL}[${idx}]`;
        }
    });

    // Step 2e: Catch-all — find any remaining (freetext... req_XXX ...freetext) where doc_id is embedded anywhere
    // e.g. "(e.g., delayed response at Kibbutz Magen, as per req_20260107_232758_009)"
    const embeddedDocIdPattern = /\([^)]*?\b((?:doc_|req_)[a-zA-Z0-9_]+)\b[^)]*\)/gi;
    result = result.replace(embeddedDocIdPattern, (match, docId) => {
        const citationInfo = citationMap[docId];
        if (!citationInfo) return match;

        const cardId = `${citationInfo.card_type}-${citationInfo.card_index}`;
        replacementCount++;
        console.log(`   🔧 Embedded citation resolved: ${match.substring(0, 60)}... → ${citationInfo.author}, ${citationInfo.date}`);

        if (citationDebug) {
            const text = `${citationInfo.author}, ${citationInfo.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
            return `<a href="#${cardId}" class="citation-link" data-card-id="${cardId}" data-doc-id="${docId}" data-ref-type="summary">(${text})</a>`;
        } else {
            const idx = resolvedCitations.length;
            resolvedCitations.push({
                author: citationInfo.author,
                date: citationInfo.date,
                url: citationInfo.url || '',
                cardId: cardId,
                docId: docId,
                refType: 'summary',
                isQa: citationInfo.card_type === 'qa',
                dateSort: parseDateForSort(citationInfo.date)
            });
            return `${CITE_SENTINEL}[${idx}]`;
        }
    });

    console.log(`✅ Citation rendering complete: ${replacementCount} replacements made`);

    if (citationDebug || resolvedCitations.length === 0) {
        return mergeAdjacentCitations(result);
    }

    // Step 3 (non-debug): Group consecutive citation sentinels separated by "; " or ", "
    // Pattern: sentinel possibly followed by more sentinels with ; or , separators
    const groupPattern = new RegExp(
        `${escapeRegExp(CITE_SENTINEL)}\\[(\\d+)\\]` +
        `(?:\\s*[;,]\\s*${escapeRegExp(CITE_SENTINEL)}\\[(\\d+)\\])*`,
        'g'
    );

    const grouped = result.replace(groupPattern, (match) => {
        // Extract all indices from the match
        const idxPattern = new RegExp(`${escapeRegExp(CITE_SENTINEL)}\\[(\\d+)\\]`, 'g');
        const indices = [];
        let m;
        while ((m = idxPattern.exec(match)) !== null) {
            indices.push(parseInt(m[1], 10));
        }

        if (indices.length === 0) return match;

        // Single citation — render directly without grouping parentheses
        if (indices.length === 1) {
            const c = resolvedCitations[indices[0]];
            const text = `${c.author}, ${c.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
            const displayText = `(${text})`;
            return `<a href="#${c.cardId}" class="citation-link" data-card-id="${c.cardId}" data-doc-id="${c.docId}" data-ref-type="${c.refType}">${displayText}</a>`;
        }

        // Multiple citations — sort by date (newest first), group into single parenthetical
        const citesForGroup = indices.map(i => resolvedCitations[i]);
        citesForGroup.sort((a, b) => b.dateSort - a.dateSort);

        // Dedup by URL first (same source from summary & QA), fallback to author+date
        const seen = new Set();
        const dedupedCites = citesForGroup.filter(c => {
            const key = c.url ? c.url : `${c.author}|${c.date}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
        });

        // Build grouped citation: (Author, Date; Author2, Date2; ...)
        // Each segment is independently clickable
        // Remove double commas that might appear
        const segments = dedupedCites.map(c => {
            const text = `${c.author}, ${c.date}`.replace(/,,+/g, ',').replace(/,\s*,/g, ',').trim();
            return `<a href="#${c.cardId}" class="citation-link" data-card-id="${c.cardId}" data-doc-id="${c.docId}" data-ref-type="${c.refType}">${text}</a>`;
        });
        return `(${segments.join('; ')})`;
    });

    // Clean up any remaining ungrouped sentinels (shouldn't happen, but defensive)
    const leftover = grouped.replace(
        new RegExp(`${escapeRegExp(CITE_SENTINEL)}\\[(\\d+)\\]`, 'g'),
        (match, idxStr) => {
            const c = resolvedCitations[parseInt(idxStr, 10)];
            if (!c) return match;
            const text = `${c.author}, ${c.date}`.replace(/,,+/g, ',').replace(/,\\s*,/g, ',').trim();
            return `<a href="#${c.cardId}" class="citation-link" data-card-id="${c.cardId}" data-doc-id="${c.docId}" data-ref-type="${c.refType}">(${text})</a>`;
        }
    );

    return mergeAdjacentCitations(leftover);
}

/**
 * Merge adjacent citation links separated by "); (" into a single grouped parenthetical.
 * e.g. (Author, Date); (Author2, Date2) → (Author, Date; Author2, Date2)
 * Works on already-rendered <a> citation links.
 */
function mergeAdjacentCitations(html) {
    // Match: (<a ...>text</a>)  followed by ;/,/whitespace  then  (<a ...>text</a>)
    // Repeat to handle chains of 3+
    const adjacentPattern = /\(<a ([^>]+)>([^<]+)<\/a>\)\s*[;,]\s*\(<a ([^>]+)>([^<]+)<\/a>\)/g;
    let prev = '';
    let result = html;
    // Iteratively merge pairs until stable (handles chains of 3+)
    while (result !== prev) {
        prev = result;
        result = result.replace(adjacentPattern, '(<a $1>$2</a>; <a $3>$4</a>)');
    }
    return result;
}

/**
 * Parse a citation date string into a numeric value for sorting.
 * Returns epoch ms for valid dates, 0 for N.D./unknown.
 */
function parseDateForSort(dateStr) {
    if (!dateStr || dateStr === 'N.D.' || dateStr === 'N/A') return 0;
    try {
        const d = new Date(dateStr);
        return isNaN(d.getTime()) ? 0 : d.getTime();
    } catch {
        return 0;
    }
}

/** Escape a string for use in a RegExp */
function escapeRegExp(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Scroll to a card and highlight it when a citation is clicked.
 * Supports both legacy index-based lookup and new document_id-based lookup.
 * @param {string} cardIdOrDocId - Either a card ID (e.g., 'sum-3') or document_id (e.g., 'req_xyz')
 * @param {string|null} refType - Reference type ('qa-N' or 'summary'), null for legacy lookup
 */
function scrollToAndHighlightCard(cardIdOrDocId, refType = null) {
    let card;

    if (refType) {
        // New dynamic lookup by document_id
        console.log(`🎯 Dynamic lookup: docId='${cardIdOrDocId}', refType='${refType}'`);

        if (refType.startsWith('qa-')) {
            // For QA: find by doc_id AND qa number
            const qaNum = refType.replace('qa-', '');
            card = document.querySelector(`.document-item[data-doc-id="${cardIdOrDocId}"][data-qa-num="${qaNum}"]`);
            console.log(`   QA card search: doc_id='${cardIdOrDocId}', qa_num='${qaNum}'`, card ? '✅' : '❌');
        } else {
            // For summaries: find by doc_id
            card = document.querySelector(`.document-item[data-doc-id="${cardIdOrDocId}"]`);
            console.log(`   Summary card search: doc_id='${cardIdOrDocId}'`, card ? '✅' : '❌');
        }
    } else {
        // Legacy: lookup by card ID (sum-0, qa-5, etc.)
        card = document.getElementById(cardIdOrDocId);
        console.log(`   Legacy lookup: cardId='${cardIdOrDocId}'`, card ? '✅' : '❌');
    }

    if (!card) {
        console.warn(`⚠️ Card not found:`, {cardIdOrDocId, refType});
        return;
    }

    // Scroll the card into view smoothly
    card.scrollIntoView({ behavior: 'smooth', block: 'center' });

    // Add highlight class — blue for summaries, orange for QA
    card.classList.remove('card-highlighted', 'card-highlighted-blue');
    const isQa = card.dataset.qaNum || card.id?.startsWith('qa-');
    if (isQa) {
        card.classList.add('card-highlighted');
    } else {
        card.classList.add('card-highlighted-blue');
    }

    // Remove highlight after 3 seconds
    setTimeout(() => {
        card.classList.remove('card-highlighted', 'card-highlighted-blue');
    }, 3000);
}

/**
 * Format an ISO date string to human-readable "Month Year" format.
 * @param {string} isoDate - ISO date string (e.g. "2025-11-02T19:34:07+00:00")
 * @returns {string} Formatted date or 'N.D.' if invalid/missing
 */
function formatDisplayDate(isoDate) {
    if (!isoDate || isoDate === 'N/A' || isoDate === 'N.D.') return 'N.D.';
    try {
        const d = new Date(isoDate);
        if (isNaN(d.getTime())) return isoDate;
        const months = ['January','February','March','April','May','June',
                        'July','August','September','October','November','December'];
        return `${months[d.getMonth()]} ${d.getFullYear()}`;
    } catch {
        return isoDate;
    }
}

function addMessage(role, content, metadata = null, options = {}) {
    const messageId = `msg-${Date.now()}-${Math.random()}`;
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${role}`;
    messageDiv.id = messageId;
    messageDiv.dataset.role = role;

    const bubbleDiv = document.createElement('div');
    bubbleDiv.className = 'message-bubble';

    const contentDiv = document.createElement('div');
    contentDiv.className = 'message-content';

    // Render markdown for assistant messages
    if (role === 'assistant') {
        console.log('📝 Adding assistant message');
        console.log('   Metadata:', metadata);
        console.log('   Has citation_map:', !!(metadata && metadata.citation_map));

        // First convert citations to links if citation_map is provided
        let processedContent = content;
        if (metadata && metadata.citation_map) {
            console.log('✅ Citation map found, processing citations...');
            const citDebug = settings.display && settings.display.citation_debug === true;
            processedContent = renderCitationsAsLinks(content, metadata.citation_map, citDebug);
        } else {
            console.warn('⚠️ No citation_map in metadata');
        }

        // Then parse markdown
        contentDiv.innerHTML = marked.parse(processedContent);
    } else {
        contentDiv.textContent = content;
    }

    bubbleDiv.appendChild(contentDiv);

    // Store raw content + metadata for edit mode (assistant only)
    if (role === 'assistant') {
        messageEditState.set(messageId, { rawContent: content, metadata: metadata });
    }

    // Add metadata for assistant messages - ALWAYS create metadata div for assistant messages
    if (role === 'assistant') {
        // Ensure metadata exists (defensive programming)
        if (!metadata) {
            metadata = {};
        }

        const metadataDiv = document.createElement('div');
        metadataDiv.className = 'message-metadata';

        const items = [];

        if (metadata.total_retrieved !== undefined) {
            items.push(`📄 ${metadata.total_retrieved} docs`);
        }

        if (metadata.execution_time_ms !== undefined) {
            const seconds = (metadata.execution_time_ms / 1000).toFixed(1);
            items.push(`⏱️ ${seconds}s`);
        }

        if (metadata.llm_model) {
            items.push(`🤖 ${metadata.llm_model}`);
        }

        if (metadata.use_qa) {
            const topK = metadata.qa_top_k ? ` k=${metadata.qa_top_k}` : '';
            items.push(`🧠 QA${topK}`);
        }

        if (metadata.date_filters) {
            items.push(`📅 Filtered`);
        }

        metadataDiv.innerHTML = items.map(item =>
            `<span class="metadata-item">${item}</span>`
        ).join('');

        bubbleDiv.appendChild(metadataDiv);

        // Always add PDF export button for assistant messages
        // message_id can be 0 (integer) which is falsy, so check explicitly
        if (metadata.message_id !== undefined && metadata.message_id !== null) {
            console.log('✅ Adding PDF export button for conversation:', metadata.message_id);
            const exportBtn = document.createElement('button');
            exportBtn.className = 'pdf-export-icon';
            exportBtn.innerHTML = '📥';
            exportBtn.title = 'Export as PDF';
            exportBtn.onclick = () => exportMessagePDF(metadata.message_id);
            metadataDiv.appendChild(exportBtn);
        }

        // Edit button (pencil icon) — revealed on hover via CSS
        const editBtn = document.createElement('button');
        editBtn.className = 'edit-response-btn';
        editBtn.innerHTML = '✏';
        editBtn.title = 'Edit response';
        editBtn.setAttribute('aria-label', 'Edit response');
        editBtn.addEventListener('click', () => startEditMessage(messageId));
        bubbleDiv.appendChild(editBtn);

        // Restore "Edited" badge when loading from history
        if (options.editedAt) {
            const editedBadge = document.createElement('span');
            editedBadge.className = 'edited-badge';
            editedBadge.textContent = `Edited ${new Date(options.editedAt).toLocaleTimeString()}`;
            contentDiv.insertAdjacentElement('afterend', editedBadge);
        }
    }

    messageDiv.appendChild(bubbleDiv);
    chatContainer.appendChild(messageDiv);

    // Only auto-scroll if user is near bottom (within 100px)
    const isNearBottom = chatContainer.scrollHeight - chatContainer.scrollTop - chatContainer.clientHeight < 100;
    if (isNearBottom) {
        scrollToBottom();
    }

    return messageId;
}

function addLoadingMessage() {
    const loadingId = `loading-${Date.now()}`;
    const messageDiv = document.createElement('div');
    messageDiv.className = 'message assistant';
    messageDiv.id = loadingId;

    const bubbleDiv = document.createElement('div');
    bubbleDiv.className = 'message-bubble';

    const loadingDiv = document.createElement('div');
    loadingDiv.className = 'loading';
    loadingDiv.innerHTML = `
        <span>Thinking</span>
        <div class="loading-dots">
            <div class="loading-dot"></div>
            <div class="loading-dot"></div>
            <div class="loading-dot"></div>
        </div>
    `;

    bubbleDiv.appendChild(loadingDiv);
    messageDiv.appendChild(bubbleDiv);
    chatContainer.appendChild(messageDiv);

    // Only auto-scroll if user is near bottom
    const isNearBottom = chatContainer.scrollHeight - chatContainer.scrollTop - chatContainer.clientHeight < 100;
    if (isNearBottom) {
        scrollToBottom();
    }
    return loadingId;
}

function removeLoadingMessage(loadingId) {
    const loadingMessage = document.getElementById(loadingId);
    if (loadingMessage) {
        loadingMessage.remove();
    }
}

function scrollToBottom() {
    chatContainer.scrollTop = chatContainer.scrollHeight;
}

function clearConversation() {
    // Clear chat container except welcome message
    const messages = chatContainer.querySelectorAll('.message');
    messages.forEach(msg => msg.remove());

    // Reset conversation state
    currentConversationMessages = [];
    currentConversationId = null;

    // Clear document list
    documentList.innerHTML = '<div class="empty-state">No documents retrieved yet. Send a query to see results.</div>';

    console.log('🔄 Conversation cleared - fresh context');
}

function exportSession() {
    // Get all messages from the chat
    const messages = chatContainer.querySelectorAll('.message');

    if (messages.length === 0 || (messages.length === 1 && messages[0].classList.contains('welcome-message'))) {
        alert('No session to export');
        return;
    }

    // Build session data
    const sessionData = {
        version: '1.0',
        exported_at: new Date().toISOString(),
        mode: queryModeSelect.value,
        conversations: [],
        messages: []
    };

    // Extract conversation data
    messages.forEach((msg) => {
        if (msg.classList.contains('welcome-message')) return;

        const role = msg.dataset.role;
        const content = msg.querySelector('.message-content');
        const metadata = msg.querySelector('.message-metadata');

        const messageData = {
            role: role,
            content: content.innerHTML,
            text_content: content.textContent.trim()
        };

        if (metadata) {
            messageData.metadata = metadata.textContent.trim();
        }

        sessionData.messages.push(messageData);
    });

    // Include conversation history if available
    sessionData.conversations = currentConversationMessages;

    // Create and download JSON file
    const blob = new Blob([JSON.stringify(sessionData, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `rag-session-${new Date().toISOString().slice(0, 19).replace(/:/g, '-')}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    console.log('📥 Session exported as JSON');
}

function importSession(file) {
    const reader = new FileReader();

    reader.onload = (e) => {
        try {
            const sessionData = JSON.parse(e.target.result);

            // Validate session data
            if (!sessionData.version || !sessionData.messages) {
                alert('Invalid session file format');
                return;
            }

            // Clear existing conversation
            clearConversation();

            // Restore mode
            if (sessionData.mode) {
                queryModeSelect.value = sessionData.mode;
            }

            // Restore messages
            sessionData.messages.forEach((msgData) => {
                const messageDiv = document.createElement('div');
                messageDiv.className = `message ${msgData.role}`;
                messageDiv.dataset.role = msgData.role;

                const bubbleDiv = document.createElement('div');
                bubbleDiv.className = 'message-bubble';

                const contentDiv = document.createElement('div');
                contentDiv.className = 'message-content';
                contentDiv.innerHTML = msgData.content;

                bubbleDiv.appendChild(contentDiv);

                // Add metadata if present
                if (msgData.metadata) {
                    const metadataDiv = document.createElement('div');
                    metadataDiv.className = 'message-metadata';
                    metadataDiv.textContent = msgData.metadata;
                    bubbleDiv.appendChild(metadataDiv);
                }

                messageDiv.appendChild(bubbleDiv);
                chatContainer.appendChild(messageDiv);
            });

            // Restore conversation history if available
            if (sessionData.conversations && sessionData.conversations.length > 0) {
                currentConversationMessages = sessionData.conversations;
            }

            // Scroll to bottom
            scrollToBottom();

            console.log(`📤 Session imported: ${sessionData.messages.length} messages restored`);
            alert(`Session imported successfully! ${sessionData.messages.length} messages restored.`);

        } catch (error) {
            console.error('Error importing session:', error);
            alert('Failed to import session. Please check the file format.');
        }
    };

    reader.readAsText(file);
}

// ============================================================================
// Query List (Left Panel)
// ============================================================================

function updateQueryList(conversation) {
    // Check if query already exists in list
    const existingItem = document.querySelector(`.query-item[data-id="${conversation.id}"]`);

    if (existingItem) {
        // Update existing item (e.g., if it's part of ongoing conversation)
        existingItem.querySelector('.query-item-time').textContent = formatTimestamp(conversation.timestamp);
    } else {
        // Add new query item
        addQueryToList(conversation);
    }
}

function addQueryToList(conversation) {
    const queryItem = document.createElement('div');
    queryItem.className = 'query-item';
    queryItem.dataset.id = conversation.id;

    const queryText = document.createElement('div');
    queryText.className = 'query-item-text';
    queryText.textContent = conversation.user_query;

    const queryTime = document.createElement('div');
    queryTime.className = 'query-item-time';
    queryTime.textContent = formatTimestamp(conversation.timestamp);

    queryItem.appendChild(queryText);
    queryItem.appendChild(queryTime);

    // Click handler to view conversation
    queryItem.addEventListener('click', () => {
        viewConversationEntry(conversation.id);
    });

    // Add to list (newest at top)
    queryList.insertBefore(queryItem, queryList.firstChild);
}

function viewConversationEntry(conversationId) {
    const conversation = conversations.find(c => c.id === conversationId);
    if (!conversation) return;

    // Scroll to the user message for this conversation entry
    const userMessage = document.getElementById(conversation.user_message_id);
    if (userMessage) {
        // Scroll the message into view
        userMessage.scrollIntoView({ behavior: 'smooth', block: 'start' });

        // Highlight briefly
        userMessage.classList.add('highlight');
        setTimeout(() => {
            userMessage.classList.remove('highlight');
        }, 2000);
    }

    // Update document list with this conversation's retrieved docs
    updateDocumentList(conversation.retrieved_docs, conversation.retrieved_qa || [], conversation.enrichment_results || [], conversation.eo_results || [], conversation.event_results || []);

    // Set as current and highlight in query list
    currentConversationId = conversationId;
    highlightCurrentQuery();
}

function loadConversation(conversationId) {
    // This function is no longer used for clearing chat
    // Instead we maintain continuous conversation
    // Kept for backward compatibility
    viewConversationEntry(conversationId);
}

function highlightCurrentQuery() {
    // Remove all active classes
    document.querySelectorAll('.query-item').forEach(item => {
        item.classList.remove('active');
    });

    // Add active class to current
    if (currentConversationId !== null) {
        const currentItem = document.querySelector(`.query-item[data-id="${currentConversationId}"]`);
        if (currentItem) {
            currentItem.classList.add('active');
        }
    }
}

async function loadConversations() {
    try {
        const response = await fetch('/api/conversations');
        if (!response.ok) {
            throw new Error('Failed to load conversations');
        }

        const data = await response.json();

        // Clear existing state
        conversations = [];
        currentConversationMessages = [];
        chatContainer.innerHTML = '';
        queryList.innerHTML = '';

        if (data.length === 0) {
            // Show welcome message if no conversations
            chatContainer.innerHTML = `
                <div class="welcome-message">
                    <h2>Welcome to the RAG Query Interface</h2>
                    <p>Ask questions about your indexed summaries. The system will retrieve relevant documents and provide grounded responses.</p>
                </div>
            `;
            return;
        }

        // Rebuild conversation thread in chat container
        data.forEach((conv, index) => {
            const userMessageId = addMessage('user', conv.user_query);

            // Ensure metadata exists and has message_id for PDF export
            if (!conv.metadata) {
                conv.metadata = {};
            }
            if (conv.metadata.message_id === undefined || conv.metadata.message_id === null) {
                conv.metadata.message_id = conv.id;
            }

            const assistantMessageId = addMessage(
                'assistant',
                conv.edited_content || conv.assistant_response,
                conv.metadata,
                conv.edited ? { editedAt: conv.edited_at } : {}
            );

            // Update conversation with message IDs
            conv.user_message_id = userMessageId;
            conv.assistant_message_id = assistantMessageId;

            conversations.push(conv);
            currentConversationMessages.push(conv);

            // Add to query list
            addQueryToList(conv);
        });

        // Load most recent conversation's documents
        if (conversations.length > 0) {
            const mostRecent = conversations[conversations.length - 1];
            updateDocumentList(mostRecent.retrieved_docs, mostRecent.retrieved_qa || [], mostRecent.enrichment_results || [], mostRecent.eo_results || [], mostRecent.event_results || []);
            currentConversationId = mostRecent.id;
            highlightCurrentQuery();
        }

        // Scroll to bottom to show latest
        setTimeout(() => scrollToBottom(), 100);

    } catch (error) {
        console.error('Error loading conversations:', error);
    }
}

// ============================================================================
// Document List (Right Panel)
// ============================================================================

function updateDocumentList(documents, qaItems, enrichmentResults, eoResults, eventResults) {
    console.log('🔍 updateDocumentList called with:', {
        documents_type: typeof documents,
        documents_count: documents?.length || 0,
        qaItems_type: typeof qaItems,
        qaItems_count: qaItems?.length || 0,
        qaItems_isArray: Array.isArray(qaItems),
        qaItems_sample: qaItems?.[0]
    });

    eoResults = eoResults || [];
    eventResults = eventResults || [];

    documentList.innerHTML = '';

    const hasDocuments = documents && documents.length > 0;
    const hasQA = qaItems && qaItems.length > 0;
    const hasEo = eoResults.length > 0;
    const hasEoEvents = eventResults.length > 0;

    if (!hasDocuments && !hasQA && !hasEo && !hasEoEvents) {
        documentList.innerHTML = '<div class="empty-state">No index documents retrieved.<br><small>Search mode uses web grounding instead of the index.</small></div>';
        return;
    }

    // Render enrichment results FIRST so they are immediately visible
    const hasEnrichment = enrichmentResults && enrichmentResults.length > 0;
    if (hasEnrichment) {
        const enrichHeader = document.createElement('div');
        enrichHeader.className = 'section-header enrichment-header';
        enrichHeader.textContent = `Enrichment Results (${enrichmentResults.length})`;
        documentList.appendChild(enrichHeader);

        enrichmentResults.forEach((item, index) => {
            const card = document.createElement('div');
            // Determine link type: linked records have a non-empty event_key
            const isLinked = !!(item.event_key);
            card.className = `document-item enrichment-card ${isLinked ? 'enrichment-linked' : 'enrichment-unlinked'}`;

            // Badge: linked vs unlinked
            const badge = document.createElement('div');
            badge.className = `enrichment-badge ${isLinked ? 'badge-linked' : 'badge-unlinked'}`;
            badge.textContent = isLinked ? 'Event-Linked' : 'Unlinked Media';
            card.appendChild(badge);

            // Event type badges derived from record content
            const enrichTypes = [];
            if (item.event_descriptor) enrichTypes.push('events');
            if (item.eo_identifier) enrichTypes.push('executive order');
            if (item.media_framing) enrichTypes.push('media');
            const mfRisk = item.media_framing && (typeof item.media_framing === 'object') ? item.media_framing.risk : null;
            if (mfRisk) enrichTypes.push('risk');

            enrichTypes.forEach(t => {
                const typeBadge = document.createElement('span');
                typeBadge.className = 'enrichment-type-badge';
                typeBadge.textContent = t;
                card.appendChild(typeBadge);
            });

            // Publisher / source
            const publisher = item.publisher || item.source_name || '';
            if (publisher) {
                const pubDiv = document.createElement('div');
                pubDiv.className = 'doc-meta';
                pubDiv.innerHTML = `<strong>Publisher:</strong> ${publisher}`;
                card.appendChild(pubDiv);
            }

            // Time window
            if (item.time_window) {
                const twDiv = document.createElement('div');
                twDiv.className = 'doc-meta';
                twDiv.innerHTML = `<strong>Time Window:</strong> ${item.time_window}`;
                card.appendChild(twDiv);
            }

            // Salience score
            if (item.consumer_salience != null) {
                const salDiv = document.createElement('div');
                salDiv.className = 'doc-relevance';
                salDiv.textContent = `Salience: ${(item.consumer_salience * 100).toFixed(1)}%`;
                card.appendChild(salDiv);
            }

            // Event descriptor (more readable than event_key hash)
            if (item.event_descriptor) {
                const ekDiv = document.createElement('div');
                ekDiv.className = 'doc-meta';
                ekDiv.innerHTML = `<strong>Event:</strong> ${item.event_descriptor}`;
                card.appendChild(ekDiv);
            } else if (item.event_key) {
                const ekDiv = document.createElement('div');
                ekDiv.className = 'doc-meta';
                ekDiv.innerHTML = `<strong>Event ID:</strong> ${item.event_key}`;
                card.appendChild(ekDiv);
            }

            // EO identifier
            if (item.eo_identifier) {
                const eoDiv = document.createElement('div');
                eoDiv.className = 'doc-meta';
                eoDiv.innerHTML = `<strong>EO:</strong> ${item.eo_identifier}`;
                card.appendChild(eoDiv);
            }

            // Media framing (collapsible) — object with sub-fields
            const mf = item.media_framing;
            if (mf && typeof mf === 'object') {
                const framingToggle = document.createElement('button');
                framingToggle.className = 'enrichment-framing-toggle';
                framingToggle.textContent = 'Show Framing';
                framingToggle.setAttribute('aria-expanded', 'false');

                const framingContent = document.createElement('div');
                framingContent.className = 'enrichment-framing-content hidden';

                // Render framing sub-fields
                const framingFields = Object.entries(mf).filter(([k, v]) => v != null && k !== 'risk');
                if (framingFields.length > 0) {
                    framingContent.innerHTML = framingFields.map(([k, v]) =>
                        `<div><strong>${k.replace(/_/g,' ')}:</strong> ${typeof v === 'object' ? JSON.stringify(v) : v}</div>`
                    ).join('');
                } else {
                    framingContent.textContent = JSON.stringify(mf);
                }

                framingToggle.addEventListener('click', () => {
                    const expanded = framingToggle.getAttribute('aria-expanded') === 'true';
                    framingToggle.setAttribute('aria-expanded', String(!expanded));
                    framingToggle.textContent = expanded ? 'Show Framing' : 'Hide Framing';
                    framingContent.classList.toggle('hidden', expanded);
                });

                card.appendChild(framingToggle);
                card.appendChild(framingContent);
            } else if (mf && typeof mf === 'string') {
                const framingToggle = document.createElement('button');
                framingToggle.className = 'enrichment-framing-toggle';
                framingToggle.textContent = 'Show Framing';
                framingToggle.setAttribute('aria-expanded', 'false');

                const framingContent = document.createElement('div');
                framingContent.className = 'enrichment-framing-content hidden';
                framingContent.textContent = mf;

                framingToggle.addEventListener('click', () => {
                    const expanded = framingToggle.getAttribute('aria-expanded') === 'true';
                    framingToggle.setAttribute('aria-expanded', String(!expanded));
                    framingToggle.textContent = expanded ? 'Show Framing' : 'Hide Framing';
                    framingContent.classList.toggle('hidden', expanded);
                });

                card.appendChild(framingToggle);
                card.appendChild(framingContent);
            }

            // Risk badge — mf.risk is {rating, reasoning} or null
            const riskData = mf && typeof mf === 'object' ? mf.risk : null;
            if (riskData) {
                const riskLevel = (riskData.rating || '').toLowerCase();
                const riskBadge = document.createElement('span');
                riskBadge.className = `enrichment-risk-badge risk-${riskLevel || 'medium'}`;
                riskBadge.textContent = `Risk: ${riskData.rating || 'Unknown'}`;
                riskBadge.title = riskData.reasoning || '';
                card.appendChild(riskBadge);
            }

            // Source URL
            if (item.link) {
                const urlLink = document.createElement('a');
                urlLink.className = 'doc-url';
                urlLink.href = item.link;
                urlLink.target = '_blank';
                urlLink.rel = 'noopener noreferrer';
                urlLink.textContent = item.link;
                card.appendChild(urlLink);
            }

            documentList.appendChild(card);
        });
    }

    // Render summary documents after enrichment
    if (hasDocuments) {
        const summaryHeader = document.createElement('div');
        summaryHeader.className = 'section-header';
        summaryHeader.textContent = `Summary Documents (${documents.length})`;
        documentList.appendChild(summaryHeader);

        // Deduplicate documents by title and content hash
        const seenTitles = new Set();
        const seenHashes = new Set();
        const uniqueDocs = [];

        documents.forEach(doc => {
            // Get title for comparison
            const title = getDocumentTitle(doc).toLowerCase().trim();

            // Create simple hash from first 200 chars of summary
            const content = doc.summary_text || '';
            const contentHash = simpleHash(content.substring(0, 200));

            // Check if we've seen this title or content before
            if (!seenTitles.has(title) && !seenHashes.has(contentHash)) {
                seenTitles.add(title);
                seenHashes.add(contentHash);
                uniqueDocs.push(doc);
            }
        });

        // Sort by relevance (highest similarity first)
        uniqueDocs.sort((a, b) => (b.similarity_score || 0) - (a.similarity_score || 0));

        uniqueDocs.forEach((doc, index) => {
            const docItem = document.createElement('div');
            docItem.className = 'document-item';
            docItem.id = `sum-${index}`; // Add ID for citation targeting
            docItem.dataset.docId = doc.document_id || doc.doc_metadata?.document_id || ''; // Store doc_id for lookup

            // Extract title from metadata or URL
            const title = getDocumentTitle(doc);

            // Extract author
            const author = getDocumentAuthor(doc);

            // Title
            const titleDiv = document.createElement('div');
            titleDiv.className = 'doc-title';
            titleDiv.textContent = title;
            docItem.appendChild(titleDiv);

            // Author
            if (author) {
                const authorDiv = document.createElement('div');
                authorDiv.className = 'doc-meta';
                authorDiv.innerHTML = `<strong>Author:</strong> ${author}`;
                docItem.appendChild(authorDiv);
            }

            // Publication date
            const formattedPubDate = formatDisplayDate(doc.publication_date);
            const pubDateDiv = document.createElement('div');
            pubDateDiv.className = 'doc-meta';
            pubDateDiv.innerHTML = `<strong>Date:</strong> ${formattedPubDate}`;
            docItem.appendChild(pubDateDiv);

            // Relevance score
            const relevanceDiv = document.createElement('div');
            relevanceDiv.className = 'doc-relevance';
            relevanceDiv.textContent = `Relevance: ${(doc.similarity_score * 100).toFixed(1)}%`;
            docItem.appendChild(relevanceDiv);

            // Summary snippet (first 200 chars)
            const summaryDiv = document.createElement('div');
            summaryDiv.className = 'doc-summary';
            const summaryText = doc.summary_text.substring(0, 200);
            summaryDiv.textContent = summaryText + (doc.summary_text.length > 200 ? '...' : '');
            docItem.appendChild(summaryDiv);

            // URL link
            const urlLink = document.createElement('a');
            urlLink.className = 'doc-url';
            urlLink.href = doc.source_url;
            urlLink.target = '_blank';
            // Use source_url_display if available (decoded URLs), otherwise use source_url
            urlLink.textContent = doc.source_url_display || doc.source_url;
            docItem.appendChild(urlLink);

            documentList.appendChild(docItem);
        });
    }

    // Render Q&A items after summaries with orange theme
    if (hasQA) {
        const qaHeader = document.createElement('div');
        qaHeader.className = 'section-header qa-header';
        qaHeader.textContent = `Q&A Pairs (${qaItems.length})`;
        documentList.appendChild(qaHeader);

        qaItems.forEach((qa, index) => {
            const qaCard = document.createElement('div');
            qaCard.className = 'document-item qa-card';
            qaCard.id = `qa-${index}`; // Add ID for citation targeting
            qaCard.dataset.docId = qa.document_id || qa.doc_metadata?.document_id || ''; // Store doc_id for lookup
            qaCard.dataset.qaNum = index + 1; // Store 1-indexed QA number

            // Title with qa number
            const title = (qa.doc_metadata && qa.doc_metadata.title) || 'Q&A Pair';
            const titleDiv = document.createElement('div');
            titleDiv.className = 'doc-title qa-title';
            titleDiv.textContent = `${title} [qa-${index + 1}]`;
            qaCard.appendChild(titleDiv);

            // Author
            const author = (qa.doc_metadata && (qa.doc_metadata.author || qa.doc_metadata.publisher)) || 'Unknown';
            const authorDiv = document.createElement('div');
            authorDiv.className = 'doc-meta';
            authorDiv.innerHTML = `<strong>Author:</strong> ${author}`;
            qaCard.appendChild(authorDiv);

            // Date
            const rawDate = (qa.doc_metadata && (qa.doc_metadata.creation_date || qa.doc_metadata.publication_date)) || '';
            const formattedQaDate = formatDisplayDate(rawDate);
            const dateDiv = document.createElement('div');
            dateDiv.className = 'doc-meta';
            dateDiv.innerHTML = `<strong>Date:</strong> ${formattedQaDate}`;
            qaCard.appendChild(dateDiv);

            // Relevance score (orange)
            const relevanceDiv = document.createElement('div');
            relevanceDiv.className = 'doc-relevance qa-relevance';
            relevanceDiv.textContent = `Relevance: ${(qa.similarity * 100).toFixed(1)}%`;
            qaCard.appendChild(relevanceDiv);

            // Question
            const questionDiv = document.createElement('div');
            questionDiv.className = 'qa-section';
            questionDiv.innerHTML = `<strong>Question:</strong> ${qa.question || 'N/A'}`;
            qaCard.appendChild(questionDiv);

            // Answer
            const answerDiv = document.createElement('div');
            answerDiv.className = 'qa-section';
            answerDiv.innerHTML = `<strong>Answer:</strong> ${qa.answer || 'N/A'}`;
            qaCard.appendChild(answerDiv);

            // Context (if available)
            if (qa.context) {
                const contextDiv = document.createElement('div');
                contextDiv.className = 'qa-section qa-context';
                const contextSnippet = qa.context.substring(0, 150);
                contextDiv.innerHTML = `<strong>Context:</strong> ${contextSnippet}${qa.context.length > 150 ? '...' : ''}`;
                qaCard.appendChild(contextDiv);
            }

            // URL link (orange)
            if (qa.source_url) {
                const urlLink = document.createElement('a');
                urlLink.className = 'doc-url qa-url';
                urlLink.href = qa.source_url;
                urlLink.target = '_blank';
                // Use source_url_display if available (decoded URLs), otherwise use source_url
                urlLink.textContent = qa.source_url_display || qa.source_url;
                qaCard.appendChild(urlLink);
            }

            documentList.appendChild(qaCard);
        });
    }

    // -----------------------------------------------------------------------
    // Render EO Index hits (violet theme)
    // -----------------------------------------------------------------------
    if (hasEo) {
        const eoHeader = document.createElement('div');
        eoHeader.className = 'section-header eo-header';
        eoHeader.textContent = `Executive Orders (${eoResults.length})`;
        documentList.appendChild(eoHeader);

        eoResults.forEach((hit) => {
            const summary = hit.summary || {};
            const eoIds = hit.eo_ids || [];
            const similarity = hit.similarity || 0;
            const docMeta = summary.doc_metadata || {};
            const title = docMeta.title || 'Executive Orders';
            const summaryText = summary.summary_text || '';

            const card = document.createElement('div');
            card.className = 'document-item eo-card';

            // EO ID badges
            eoIds.forEach(eoId => {
                const badge = document.createElement('span');
                badge.className = 'eo-id-badge';
                badge.textContent = `EO ${eoId}`;
                card.appendChild(badge);
            });

            // Title
            const titleDiv = document.createElement('div');
            titleDiv.className = 'doc-title';
            titleDiv.textContent = title;
            card.appendChild(titleDiv);

            // Date
            const pubDate = formatDisplayDate(docMeta.publication_date || '');
            const dateDiv = document.createElement('div');
            dateDiv.className = 'doc-meta';
            dateDiv.innerHTML = `<strong>Date:</strong> ${pubDate}`;
            card.appendChild(dateDiv);

            // Similarity
            const simDiv = document.createElement('div');
            simDiv.className = 'doc-relevance';
            simDiv.textContent = `Relevance: ${(similarity * 100).toFixed(1)}%`;
            card.appendChild(simDiv);

            // Summary (collapsible)
            if (summaryText) {
                const toggleBtn = document.createElement('button');
                toggleBtn.className = 'eo-summary-toggle';
                toggleBtn.textContent = 'Show Summary';
                toggleBtn.setAttribute('aria-expanded', 'false');

                const summaryContent = document.createElement('div');
                summaryContent.className = 'eo-summary-content hidden';
                summaryContent.textContent = summaryText;

                toggleBtn.addEventListener('click', () => {
                    const expanded = toggleBtn.getAttribute('aria-expanded') === 'true';
                    toggleBtn.setAttribute('aria-expanded', String(!expanded));
                    toggleBtn.textContent = expanded ? 'Show Summary' : 'Hide Summary';
                    summaryContent.classList.toggle('hidden', expanded);
                });

                card.appendChild(toggleBtn);
                card.appendChild(summaryContent);
            }

            // Source URL
            if (summary.source_url) {
                const urlLink = document.createElement('a');
                urlLink.className = 'doc-url';
                urlLink.href = summary.source_url;
                urlLink.target = '_blank';
                urlLink.rel = 'noopener noreferrer';
                urlLink.textContent = summary.source_url_display || summary.source_url;
                card.appendChild(urlLink);
            }

            documentList.appendChild(card);
        });
    }

    // -----------------------------------------------------------------------
    // Render EO Events (lighter violet theme)
    // -----------------------------------------------------------------------
    if (hasEoEvents) {
        const evHeader = document.createElement('div');
        evHeader.className = 'section-header eo-events-header';
        evHeader.textContent = `EO Events (${eventResults.length})`;
        documentList.appendChild(evHeader);

        eventResults.forEach((ev) => {
            const card = document.createElement('div');
            card.className = 'document-item eo-event-card';

            // EO identifier badge
            if (ev.eo_identifier) {
                const badge = document.createElement('span');
                badge.className = 'eo-id-badge';
                badge.textContent = ev.eo_identifier;
                card.appendChild(badge);
            }

            // Descriptor
            if (ev.descriptor) {
                const descDiv = document.createElement('div');
                descDiv.className = 'doc-title';
                descDiv.textContent = ev.descriptor;
                card.appendChild(descDiv);
            }

            // EO title
            if (ev.eo_title) {
                const eoTitleDiv = document.createElement('div');
                eoTitleDiv.className = 'doc-meta';
                eoTitleDiv.innerHTML = `<strong>EO:</strong> ${ev.eo_title}`;
                card.appendChild(eoTitleDiv);
            }

            // Time window
            if (ev.window_start || ev.window_end) {
                const winDiv = document.createElement('div');
                winDiv.className = 'doc-meta';
                winDiv.innerHTML = `<strong>Window:</strong> ${ev.window_start || '?'} → ${ev.window_end || '?'}`;
                card.appendChild(winDiv);
            }

            // Executive official
            if (ev.exec_official) {
                const offDiv = document.createElement('div');
                offDiv.className = 'doc-meta';
                offDiv.innerHTML = `<strong>Official:</strong> ${ev.exec_official}`;
                card.appendChild(offDiv);
            }

            // Media count
            if (ev.media_count != null) {
                const mcDiv = document.createElement('div');
                mcDiv.className = 'doc-relevance';
                mcDiv.textContent = `Media Coverage: ${ev.media_count}`;
                card.appendChild(mcDiv);
            }

            // Source URL
            if (ev.source_url) {
                const urlLink = document.createElement('a');
                urlLink.className = 'doc-url';
                urlLink.href = ev.source_url;
                urlLink.target = '_blank';
                urlLink.rel = 'noopener noreferrer';
                urlLink.textContent = ev.source_url;
                card.appendChild(urlLink);
            }

            documentList.appendChild(card);
        });
    }

}

function getDocumentTitle(doc) {
    // Try to get title from metadata
    if (doc.doc_metadata && doc.doc_metadata.title) {
        return doc.doc_metadata.title;
    }

    // Try to extract from URL
    if (doc.source_url) {
        // For YouTube
        if (doc.source_url.includes('youtube.com')) {
            return 'YouTube Video';
        }

        // Try to get domain
        try {
            const url = new URL(doc.source_url);
            return url.hostname.replace('www.', '');
        } catch (e) {
            return doc.source_url.substring(0, 50) + '...';
        }
    }

    return doc.document_id || 'Unknown Document';
}

function getDocumentAuthor(doc) {
    if (doc.doc_metadata) {
        // Try different author field names
        if (doc.doc_metadata.authors) {
            return Array.isArray(doc.doc_metadata.authors)
                ? doc.doc_metadata.authors.join(', ')
                : doc.doc_metadata.authors;
        }

        if (doc.doc_metadata.author) {
            return doc.doc_metadata.author;
        }

        if (doc.doc_metadata.publisher) {
            return doc.doc_metadata.publisher;
        }
    }

    return null;
}

// ============================================================================
// PDF Export
// ============================================================================

async function exportMessagePDF(messageId) {
    try {
        console.log(`📄 Exporting PDF for message: ${messageId}`);

        // Call export API — always include current settings metadata
        const response = await fetch('/api/export_pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                message_id: messageId,
                metadata: settings.metadata
            })
        });

        if (!response.ok) {
            const error = await response.json();
            throw new Error(error.error || 'PDF export failed');
        }

        // Download PDF
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;

        // Extract filename from Content-Disposition header if available
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = 'report.pdf';
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="?(.+?)"?$/);
            if (match) filename = match[1];
        }

        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);

        console.log(`✅ PDF exported: ${filename}`);

    } catch (error) {
        console.error('Error exporting PDF:', error);
        alert(`Failed to export PDF: ${error.message}`);
    }
}


// ============================================================================
// Edit Assistant Response (Phase 5)
// ============================================================================

/**
 * Enter edit mode for an assistant message.
 * Hides the rendered content and shows a textarea pre-filled with raw markdown.
 */
function startEditMessage(messageId) {
    const messageDiv = document.getElementById(messageId);
    if (!messageDiv) return;

    const bubbleDiv = messageDiv.querySelector('.message-bubble');
    if (!bubbleDiv) return;

    const contentDiv = bubbleDiv.querySelector('.message-content');

    // Use edited_content if already edited, otherwise use stored raw content
    const conv = currentConversationMessages.find(c => c.assistant_message_id === messageId);
    const editState = messageEditState.get(messageId);
    const currentText = (conv && conv.edited_content) || (editState && editState.rawContent) || '';

    // Hide rendered content and edit button
    contentDiv.style.display = 'none';
    const editBtn = bubbleDiv.querySelector('.edit-response-btn');
    if (editBtn) editBtn.style.display = 'none';

    // Hide any existing badge during edit
    const existingBadge = bubbleDiv.querySelector('.edited-badge');
    if (existingBadge) existingBadge.style.display = 'none';

    // Build textarea
    const textarea = document.createElement('textarea');
    textarea.className = 'edit-textarea';
    textarea.value = currentText;

    // Build Save / Cancel buttons
    const actionsDiv = document.createElement('div');
    actionsDiv.className = 'edit-actions';

    const saveBtn = document.createElement('button');
    saveBtn.className = 'edit-save-btn';
    saveBtn.textContent = 'Save';
    saveBtn.addEventListener('click', () => saveEditMessage(messageId, textarea.value));

    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'edit-cancel-btn';
    cancelBtn.textContent = 'Cancel';
    cancelBtn.addEventListener('click', () => cancelEditMessage(messageId));

    actionsDiv.appendChild(saveBtn);
    actionsDiv.appendChild(cancelBtn);

    // Insert editor before the metadata row
    const metadataDiv = bubbleDiv.querySelector('.message-metadata');
    if (metadataDiv) {
        bubbleDiv.insertBefore(textarea, metadataDiv);
        bubbleDiv.insertBefore(actionsDiv, metadataDiv);
    } else {
        bubbleDiv.appendChild(textarea);
        bubbleDiv.appendChild(actionsDiv);
    }

    textarea.focus();
}

/**
 * Save an edit: re-render content with citation links, show "Edited" badge,
 * update in-memory state, and PATCH the server.
 */
async function saveEditMessage(messageId, newText) {
    const messageDiv = document.getElementById(messageId);
    if (!messageDiv) return;

    const bubbleDiv = messageDiv.querySelector('.message-bubble');
    const contentDiv = bubbleDiv.querySelector('.message-content');
    const editState = messageEditState.get(messageId);

    // Re-render content with citations
    let processedContent = newText;
    if (editState && editState.metadata && editState.metadata.citation_map) {
        const citDebug = settings.display && settings.display.citation_debug === true;
        processedContent = renderCitationsAsLinks(newText, editState.metadata.citation_map, citDebug);
    }
    contentDiv.innerHTML = marked.parse(processedContent);
    contentDiv.style.display = '';

    // Remove editor elements
    const textarea = bubbleDiv.querySelector('.edit-textarea');
    const actionsDiv = bubbleDiv.querySelector('.edit-actions');
    if (textarea) textarea.remove();
    if (actionsDiv) actionsDiv.remove();

    // Show edit button again
    const editBtn = bubbleDiv.querySelector('.edit-response-btn');
    if (editBtn) editBtn.style.display = '';

    // Show or update "Edited" badge
    let editedBadge = bubbleDiv.querySelector('.edited-badge');
    if (!editedBadge) {
        editedBadge = document.createElement('span');
        editedBadge.className = 'edited-badge';
        contentDiv.insertAdjacentElement('afterend', editedBadge);
    }
    const now = new Date();
    editedBadge.textContent = `Edited ${now.toLocaleTimeString()}`;
    editedBadge.style.display = '';

    // Update in-memory conversation state (5.4)
    const conv = currentConversationMessages.find(c => c.assistant_message_id === messageId);
    const editedAt = now.toISOString();
    if (conv) {
        conv.edited_content = newText;
        conv.edited = true;
        conv.edited_at = editedAt;
        // Keep messageEditState in sync so subsequent edits start from saved version
        if (editState) editState.rawContent = newText;

        // Sync to server (5.6 — server build_context uses edited_content)
        try {
            await fetch(`/api/conversations/${conv.id}`, {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ edited_content: newText, edited_at: editedAt })
            });
        } catch (e) {
            console.warn('Failed to sync edit to server:', e);
        }
    }
}

/**
 * Cancel edit mode: restore previous rendered content without changing state.
 */
function cancelEditMessage(messageId) {
    const messageDiv = document.getElementById(messageId);
    if (!messageDiv) return;

    const bubbleDiv = messageDiv.querySelector('.message-bubble');
    const contentDiv = bubbleDiv.querySelector('.message-content');

    // Restore content and edit button
    contentDiv.style.display = '';
    const editBtn = bubbleDiv.querySelector('.edit-response-btn');
    if (editBtn) editBtn.style.display = '';

    // Restore badge if it was hidden
    const editedBadge = bubbleDiv.querySelector('.edited-badge');
    if (editedBadge) editedBadge.style.display = '';

    // Remove editor elements
    const textarea = bubbleDiv.querySelector('.edit-textarea');
    const actionsDiv = bubbleDiv.querySelector('.edit-actions');
    if (textarea) textarea.remove();
    if (actionsDiv) actionsDiv.remove();
}

// ============================================================================
// Utility Functions
// ============================================================================

function simpleHash(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return hash.toString(16).substring(0, 8);
}

function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);

    if (diffMins < 1) {
        return 'Just now';
    } else if (diffMins < 60) {
        return `${diffMins} min${diffMins > 1 ? 's' : ''} ago`;
    } else if (diffMins < 1440) {
        const hours = Math.floor(diffMins / 60);
        return `${hours} hour${hours > 1 ? 's' : ''} ago`;
    } else {
        return date.toLocaleDateString();
    }
}

// Auto-resize textarea
queryInput.addEventListener('input', function() {
    this.style.height = 'auto';
    this.style.height = Math.min(this.scrollHeight, 150) + 'px';
});
