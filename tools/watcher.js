const ws = new WebSocket("/ws");
const imageElement = document.getElementById('image');
const placeholderElement = document.getElementById('placeholder');
const infoOverlay = document.getElementById('info-overlay');
const connectionDot = document.getElementById('connection-dot');
const connectionStatus = document.getElementById('connection-status');
const queueCounter = document.getElementById('queue-counter');
const filenameElement = document.getElementById('filename');
const timestampElement = document.getElementById('timestamp');
const imageStatusElement = document.getElementById('image-status');
const historyPositionElement = document.getElementById('history-position');
const historyList = document.getElementById('history-list');
const historyCountElement = document.getElementById('history-count');
const historyPanel = document.getElementById('history-panel');
const imageContainer = document.getElementById('image-container');
const historyToggle = document.getElementById('history-toggle');
const liveIndicator = document.getElementById('live-indicator');
const clearIndicator = document.getElementById('clear-indicator');
let lastDequeueTime = 0;
let imageQueue = [];
let isProcessingQueue = false;

let currentImagePath = null;
let overlayVisible = true;
let historyVisible = true;
let isLive = true;
let history = [];
let currentHistoryIndex = -1;
let latestImageData = null;
let renderedHistoryCount = 0;

function toggleHistory() {
    historyVisible = !historyVisible;
    if (historyVisible) {
        historyPanel.classList.remove('hidden');
        imageContainer.classList.remove('history-hidden');
        historyToggle.textContent = '📚 Hide History';
    } else {
        historyPanel.classList.add('hidden');
        imageContainer.classList.add('history-hidden');
        historyToggle.textContent = '📚 Show History';
    }
}

function goLive() {
    isLive = true;
    liveIndicator.textContent = '🔴 LIVE';
    liveIndicator.classList.remove('paused');
    liveIndicator.title = 'Currently live';


    if (latestImageData) {

        const existingIndex = history.findIndex(item => item.path === latestImageData.path);
        if (existingIndex === -1) {
            addToHistory(latestImageData);
        }

        currentHistoryIndex = 0;
        showImage(latestImageData, false, 0);
        renderHistory();
    }


    if (imageQueue.length > 0 && !isProcessingQueue) {
        processImageQueue();
    }
}


function pauseLive() {
    isLive = false;
    liveIndicator.textContent = '⏸️ PAUSED';
    liveIndicator.classList.add('paused');
    liveIndicator.title = 'Click to go live';
}

function addToHistory(imageData) {

    const existingIndex = history.findIndex(item => item.path === imageData.path);
    if (existingIndex !== -1) {
        return;
    }

    history.unshift(imageData);
    if (history.length > 100) {
        history.pop();

        const lastElement = historyList.lastElementChild;
        if (lastElement) {
            lastElement.remove();
        }
        renderedHistoryCount = Math.min(renderedHistoryCount, 49);
    }


    if (isLive) {
        currentHistoryIndex = 0;
    } else {

        currentHistoryIndex++;
    }

    updateHistoryCount();
    addNewHistoryItem(imageData, 0);
    updateActiveHistoryItem();
}

function addNewHistoryItem(item, index) {
    const div = document.createElement('div');
    div.className = 'history-item';
    div.title = item.filename + '\n' + new Date(item.timestamp * 1000).toLocaleString();
    div.dataset.index = index;

    const thumb = document.createElement('img');
    thumb.className = 'history-thumb';
    thumb.src = item.path + '?t=' + new Date().getTime();
    thumb.alt = item.filename;


    thumb.onerror = function () {
        this.style.background = '#da3633';
        this.alt = '❌';
    };

    const infoDiv = document.createElement('div');
    infoDiv.className = 'history-info';

    const filenameDiv = document.createElement('div');
    filenameDiv.className = 'history-filename';
    filenameDiv.textContent = item.filename;

    const timestampDiv = document.createElement('div');
    timestampDiv.className = 'history-timestamp';
    const date = new Date(item.timestamp * 1000);
    timestampDiv.textContent = date.toLocaleTimeString();

    infoDiv.appendChild(filenameDiv);
    infoDiv.appendChild(timestampDiv);

    div.appendChild(thumb);
    div.appendChild(infoDiv);

    div.addEventListener('click', () => {
        pauseLive();
        currentHistoryIndex = index;
        showImage(item, false, index);
        updateActiveHistoryItem();
    });


    historyList.insertBefore(div, historyList.firstChild);


    updateHistoryIndices();
    renderedHistoryCount++;
}

function updateHistoryIndices() {

    const items = historyList.querySelectorAll('.history-item');
    items.forEach((item, index) => {
        item.dataset.index = index;


        const newItem = item.cloneNode(true);
        const historyData = history[index];
        newItem.addEventListener('click', () => {
            pauseLive();
            currentHistoryIndex = index;
            showImage(historyData, false, index);
            updateActiveHistoryItem();
        });

        item.parentNode.replaceChild(newItem, item);
    });
}

function updateActiveHistoryItem() {

    const items = historyList.querySelectorAll('.history-item');
    items.forEach(item => item.classList.remove('active'));


    if (currentHistoryIndex >= 0 && currentHistoryIndex < items.length) {
        items[currentHistoryIndex].classList.add('active');
    }
}

function renderHistory() {

    if (renderedHistoryCount !== history.length) {

        historyList.innerHTML = '';
        renderedHistoryCount = 0;

        history.forEach((item, index) => {
            addNewHistoryItem(item, index);
        });
    } else {

        updateActiveHistoryItem();
    }
}


function updateHistoryCount() {
    historyCountElement.textContent = history.length;
}

function processImageQueue() {
    if (imageQueue.length === 0 || isProcessingQueue) {
        queueCounter.textContent = imageQueue.length;
        return;
    }

    isProcessingQueue = true;
    const imageData = imageQueue.shift();


    queueCounter.textContent = imageQueue.length;


    const img = new Image();
    let imageLoaded = false;

    const timeout = setTimeout(() => {
        if (!imageLoaded) {
            console.warn('⚠️ Image timeout:', imageData.filename);

            setTimeout(() => {
                isProcessingQueue = false;
                processImageQueue();
            }, 500);
        }
    }, 3000);

    img.onload = () => {
        if (!imageLoaded) {
            imageLoaded = true;
            clearTimeout(timeout);

            console.log('✅ Image chargée:', imageData.filename);
            addToHistory(imageData);
            showImage(imageData, true);


            setTimeout(() => {
                isProcessingQueue = false;
                processImageQueue();
            }, 500);
        }
    };

    img.onerror = () => {
        if (!imageLoaded) {
            imageLoaded = true;
            clearTimeout(timeout);
            console.warn('⚠️ Image erreur:', imageData.filename);


            setTimeout(() => {
                isProcessingQueue = false;
                processImageQueue();
            }, 500);
        }
    };

    img.src = imageData.path + '?t=' + new Date().getTime();
}


function addImageToQueue(imageData) {
    imageQueue.push(imageData);


    if (imageQueue.length > 100) {
        imageQueue.shift();
    }


    queueCounter.textContent = imageQueue.length;


    if (!isProcessingQueue) {
        processImageQueue();
    }
}

function clearQueue() {
    imageQueue = [];
    isProcessingQueue = false;
    queueCounter.textContent = '0';
}


function showImage(data, isNew = true, historyIndex = -1) {

    imageElement.onload = function () {
        placeholderElement.style.display = 'none';
        imageElement.style.display = 'block';
        imageElement.classList.add('loaded');
        infoOverlay.classList.add('visible');

        if (isNew) {
            imageElement.classList.add('image-updated');
            setTimeout(() => {
                imageElement.classList.remove('image-updated');
            }, 600);
        }
    };

    const timestamp = new Date().getTime();
    const newPath = data.path + '?t=' + timestamp;

    if (newPath !== currentImagePath) {
        currentImagePath = newPath;
        imageElement.src = newPath;
    }


    filenameElement.textContent = data.filename || 'Unknown';
    timestampElement.textContent = new Date(data.timestamp * 1000).toLocaleString();

    if (isNew && isLive) {
        imageStatusElement.textContent = '🔴 Live';
        historyPositionElement.textContent = 'Latest';
    } else if (isLive && historyIndex === 0) {
        imageStatusElement.textContent = '🔴 Live';
        historyPositionElement.textContent = 'Latest';
    } else {
        imageStatusElement.textContent = '📚 From History';
        historyPositionElement.textContent = `${historyIndex + 1} of ${history.length}`;
    }
}

function updateConnectionStatus(connected, text) {
    connectionDot.className = `status-dot ${connected ? 'connected' : 'disconnected'}`;
    connectionStatus.textContent = text;
}


historyToggle.addEventListener('click', toggleHistory);
liveIndicator.addEventListener('click', () => {
    if (isLive) {
        pauseLive();
    } else {
        goLive();
    }
});

clearIndicator.addEventListener('click', () => {
    console.log('Clearing history and going live');


    clearQueue();

    history = [];
    currentHistoryIndex = -1;
    renderedHistoryCount = 0;
    historyList.innerHTML = '';
    updateHistoryCount();
    imageElement.style.display = 'none';
    placeholderElement.style.display = 'flex';
    infoOverlay.classList.remove('visible');

    if (!isLive) {
        goLive();
    }
});


document.addEventListener('keydown', (e) => {
    if (e.code === 'Space') {
        e.preventDefault();
        overlayVisible = !overlayVisible;
        if (overlayVisible) {
            infoOverlay.classList.remove('overlay-hidden');
        } else {
            infoOverlay.classList.add('overlay-hidden');
        }
    } else if (e.code === 'ArrowUp' && history.length > 0) {
        e.preventDefault();
        pauseLive();
        currentHistoryIndex = Math.max(0, currentHistoryIndex - 1);
        showImage(history[currentHistoryIndex], false, currentHistoryIndex);
        renderHistory();
    } else if (e.code === 'ArrowDown' && history.length > 0) {
        e.preventDefault();
        pauseLive();
        currentHistoryIndex = Math.min(history.length - 1, currentHistoryIndex + 1);
        showImage(history[currentHistoryIndex], false, currentHistoryIndex);
        renderHistory();
    } else if (e.code === 'KeyH') {
        e.preventDefault();
        toggleHistory();
    } else if (e.code === 'KeyL') {
        e.preventDefault();
        if (isLive) {
            pauseLive();
        } else {
            goLive();
        }
    }
});

ws.onmessage = function (event) {
    try {
        const data = JSON.parse(event.data);
        console.log('Received:', data);

        if ((data.type === 'new_image' || data.type === 'current_image') && data.path) {
            latestImageData = data;


            if (isLive) {
                addImageToQueue(data);
            }
        }
    } catch (error) {
        console.error('Error parsing message:', error);
    }
};


ws.onopen = function () {
    console.log("WebSocket connected");
    updateConnectionStatus(true, 'Connected');
};

ws.onclose = function () {
    console.log("WebSocket disconnected");
    updateConnectionStatus(false, 'Disconnected');

    placeholderElement.innerHTML = `
                <div class="placeholder-icon">⚠️</div>
                <div>Connection lost. Please refresh the page.</div>
            `;
    placeholderElement.style.display = 'flex';
    imageElement.style.display = 'none';
    infoOverlay.classList.remove('visible');
};

ws.onerror = function (error) {
    console.error("WebSocket error:", error);
    updateConnectionStatus(false, 'Error');
};


setTimeout(() => {
    console.log('💡 Shortcuts:');
    console.log('  - SPACE: Toggle info overlay');
    console.log('  - ↑/↓: Navigate history');
    console.log('  - H: Toggle history panel');
    console.log('  - L: Toggle live/pause mode');
    console.log('  - Click history items to view');
    console.log('  - Click 🔴 LIVE indicator to resume live feed');
}, 3000);