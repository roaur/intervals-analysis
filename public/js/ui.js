export function setLoading(isLoading, message) {
    const loadingEl = document.getElementById('loading');
    const dashboardEl = document.getElementById('dashboard');
    const statusText = document.getElementById('status-text');

    if (message && statusText) {
        statusText.textContent = message;
    }

    if (isLoading) {
        loadingEl.classList.remove('hidden');
        dashboardEl.classList.add('hidden');
    } else {
        loadingEl.classList.add('hidden');
        dashboardEl.classList.remove('hidden');
    }
}

export function setError(message) {
    console.error(message);
    const statusText = document.getElementById('status-text');
    if (statusText) {
        statusText.textContent = "Error: " + message;
        statusText.style.color = 'red';
    }
}

export function setupEventListeners({ onAddTrace, onClearTraces }) {
    const startInput = document.getElementById('start-date');
    const endInput = document.getElementById('end-date');

    document.getElementById('add-trace-btn').addEventListener('click', () => {
        const label = document.getElementById('trace-label').value || "Trace"; // Default needed? Logic in main/charts?
        const start = startInput.value;
        const end = endInput.value;
        onAddTrace(label, start, end);
    });

    document.getElementById('clear-traces-btn').addEventListener('click', onClearTraces);
}

export function setDateInputs(min, max) {
    const startInput = document.getElementById('start-date');
    const endInput = document.getElementById('end-date');
    if (min) startInput.value = min;
    if (max) endInput.value = max;
}

export function getDateInputs() {
    return {
        start: document.getElementById('start-date').value,
        end: document.getElementById('end-date').value
    };
}
