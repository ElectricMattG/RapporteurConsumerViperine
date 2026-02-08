// ============================================================================
// RAG Query Interface - Main JavaScript
// ============================================================================

let conversations = [];
let currentConversationId = null;
let currentConversationMessages = []; // Track all messages in current conversation
let currentAbortController = null; // For canceling requests

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
        query: "Israeli Defense Force Latest News"
    },
    query: {
        summaries_top_k: 25
    },
    qa: {
        include_qa: true,
        qa_top_k: 5
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
                qa: { ...settings.qa, ...(parsed.qa || {}) }
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
            assistant_response: lastConversation.assistant_response,
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

// Load conversations on page load
window.addEventListener('load', () => {
    loadSettings(); // Load settings from localStorage first
    clearConversation(); // Fresh context on page reload
    loadConversations();
    initializeNotifications();  // Start listening for index updates
});

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
            temperature: 0.7,
            max_tokens: 16000,
            use_qa: useQa,
            qa_top_k: qaTopK
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
            metadata: data.metadata
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
        updateDocumentList(data.retrieved_docs, data.retrieved_qa || []);

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

function addMessage(role, content, metadata = null) {
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
        contentDiv.innerHTML = marked.parse(content);
    } else {
        contentDiv.textContent = content;
    }

    bubbleDiv.appendChild(contentDiv);

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
    updateDocumentList(conversation.retrieved_docs, conversation.retrieved_qa || []);

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

            const assistantMessageId = addMessage('assistant', conv.assistant_response, conv.metadata);

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
            updateDocumentList(mostRecent.retrieved_docs, mostRecent.retrieved_qa || []);
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

function updateDocumentList(documents, qaItems) {
    console.log('🔍 updateDocumentList called with:', {
        documents_type: typeof documents,
        documents_count: documents?.length || 0,
        qaItems_type: typeof qaItems,
        qaItems_count: qaItems?.length || 0,
        qaItems_isArray: Array.isArray(qaItems),
        qaItems_sample: qaItems?.[0]
    });

    documentList.innerHTML = '';

    const hasDocuments = documents && documents.length > 0;
    const hasQA = qaItems && qaItems.length > 0;

    if (!hasDocuments && !hasQA) {
        documentList.innerHTML = '<div class="empty-state">No index documents retrieved.<br><small>Search mode uses web grounding instead of the index.</small></div>';
        return;
    }

    // Render summary documents first
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

        uniqueDocs.forEach(doc => {
            const docItem = document.createElement('div');
            docItem.className = 'document-item';

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
            if (doc.publication_date) {
                const dateDiv = document.createElement('div');
                dateDiv.className = 'doc-meta';
                dateDiv.innerHTML = `<strong>Date:</strong> ${doc.publication_date}`;
                docItem.appendChild(dateDiv);
            }

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
            urlLink.textContent = doc.source_url;
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
            const dateStr = (qa.doc_metadata && (qa.doc_metadata.creation_date || qa.doc_metadata.publication_date)) || 'N/A';
            const dateDiv = document.createElement('div');
            dateDiv.className = 'doc-meta';
            dateDiv.innerHTML = `<strong>Date:</strong> ${dateStr}`;
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
                urlLink.textContent = qa.source_url;
                qaCard.appendChild(urlLink);
            }

            documentList.appendChild(qaCard);
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

        // Call export API (no UI changes during generation)
        const response = await fetch('/api/export_pdf', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                message_id: messageId
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
