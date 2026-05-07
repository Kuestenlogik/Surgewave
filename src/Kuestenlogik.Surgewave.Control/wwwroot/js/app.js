// Surgewave Control - JavaScript utilities

// Setup drag handle restriction for dashboard cards
function setupDragHandles() {
    // Disable draggable on all drop items initially
    document.querySelectorAll('.mud-drop-item').forEach(item => {
        item.setAttribute('draggable', 'false');
    });

    // Enable dragging only when mousedown on handle
    document.addEventListener('mousedown', function(e) {
        const handle = e.target.closest('.mud-drop-item-handle');
        if (handle) {
            const dropItem = handle.closest('.mud-drop-item');
            if (dropItem) {
                dropItem.setAttribute('draggable', 'true');
            }
        }
    }, true);

    // Disable dragging on mouseup
    document.addEventListener('mouseup', function(e) {
        document.querySelectorAll('.mud-drop-item[draggable="true"]').forEach(item => {
            item.setAttribute('draggable', 'false');
        });
    }, true);

    // Also disable after dragend
    document.addEventListener('dragend', function(e) {
        document.querySelectorAll('.mud-drop-item[draggable="true"]').forEach(item => {
            item.setAttribute('draggable', 'false');
        });
    }, true);
}

// Initialize on page load and after navigation
document.addEventListener('DOMContentLoaded', setupDragHandles);

// Re-initialize after Blazor updates the DOM
const observer = new MutationObserver(function(mutations) {
    mutations.forEach(function(mutation) {
        if (mutation.addedNodes.length) {
            document.querySelectorAll('.mud-drop-item:not([data-handle-setup])').forEach(item => {
                item.setAttribute('draggable', 'false');
                item.setAttribute('data-handle-setup', 'true');
            });
        }
    });
});

document.addEventListener('DOMContentLoaded', function() {
    observer.observe(document.body, { childList: true, subtree: true });
});

// Download a file from base64 content
function downloadFile(fileName, base64Content) {
    const byteCharacters = atob(base64Content);
    const byteNumbers = new Array(byteCharacters.length);
    for (let i = 0; i < byteCharacters.length; i++) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNumbers);
    const blob = new Blob([byteArray], { type: 'application/octet-stream' });

    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
}
