// ============================================================================
// RAG Query Interface - Main JavaScript
// ============================================================================

let conversations = [];
let currentConversationId = null;
let currentConversationMessages = []; // Track all messages in current conversation

// DOM Elements
const chatContainer = document.getElementById('chatContainer');
const queryInput = document.getElementById('queryInput');
const sendButton = document.getElementById('sendButton');
const queryList = document.getElementById('queryList');
const documentList = document.getElementById('documentList');
const startDateInput = document.getElementById('startDate');
const endDateInput = document.getElementById('endDate');
const clearDatesButton = document.getElementById('clearDates');
const notificationOverlay = document.getElementById('notificationOverlay');
const notificationMessage = document.getElementById('notificationMessage');
const dismissNotification = document.getElementById('dismissNotification');

// ============================================================================
// Server-Sent Events for Index Update Notifications
// ============================================================================

let eventSource = null;

function initializeNotifications() {
    try {
        eventSource = new EventSource('/api/notifications');

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
            // Attempt to reconnect after 5 seconds
            setTimeout(() => {
                console.log('🔄 Attempting to reconnect...');
                initializeNotifications();
            }, 5000);
        };

        console.log('🔔 Notification stream initialized');
    } catch (error) {
        console.error('Failed to initialize notifications:', error);
    }
}

function showNotification(data) {
    const newCount = data.new_summaries || 0;
    const totalCount = data.total_summaries || 0;

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

// Load conversations on page load
window.addEventListener('load', () => {
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

    // Disable input while processing
    queryInput.disabled = true;
    sendButton.disabled = true;

    // Show loading indicator
    const loadingId = addLoadingMessage();

    try {
        // Prepare request payload
        const payload = {
            query: query,
            context_depth: 3,
            top_k: 15,
            temperature: 0.7,
            max_tokens: 8000
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
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        // Remove loading indicator
        removeLoadingMessage(loadingId);

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
            metadata: data.metadata
        };

        // Add to conversations list
        conversations.push(conversationEntry);
        currentConversationMessages.push(conversationEntry);

        // Update or add query to list
        updateQueryList(conversationEntry);

        // Update document list with latest results
        updateDocumentList(data.retrieved_docs);

        // Set as current conversation
        currentConversationId = data.id;
        highlightCurrentQuery();

    } catch (error) {
        console.error('Error sending query:', error);
        removeLoadingMessage(loadingId);
        addMessage('assistant', `Error: ${error.message}. Please ensure the RAG service is running.`);
    } finally {
        // Re-enable input
        queryInput.disabled = false;
        sendButton.disabled = false;
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

    // Add metadata for assistant messages
    if (role === 'assistant' && metadata) {
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

        if (metadata.date_filters) {
            items.push(`📅 Filtered`);
        }

        metadataDiv.innerHTML = items.map(item =>
            `<span class="metadata-item">${item}</span>`
        ).join('');

        bubbleDiv.appendChild(metadataDiv);
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
    updateDocumentList(conversation.retrieved_docs);

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
            updateDocumentList(mostRecent.retrieved_docs);
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

function updateDocumentList(documents) {
    documentList.innerHTML = '';

    if (!documents || documents.length === 0) {
        documentList.innerHTML = '<div class="empty-state">No documents retrieved.</div>';
        return;
    }

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
