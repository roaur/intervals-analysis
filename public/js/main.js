import { initDuckDB, loadData, getBounds } from './db.js';
import { renderWorkoutCards } from './map.js';
import { addTrace, clearTraces, getTracesLength } from './charts.js';
import * as UI from './ui.js';

(async () => {
    try {
        UI.setLoading(true, "Initializing DuckDB-WASM...");
        await initDuckDB();

        UI.setLoading(true, "Loading Data...");
        await loadData();

        await renderWorkoutCards();

        UI.setLoading(false);

        // Initialize Date Inputs
        const bounds = await getBounds();
        UI.setDateInputs(bounds.min, bounds.max);

        // Add default traces (last 3 years)
        if (bounds.min) {
            const currentYear = new Date().getFullYear();
            for (let i = 0; i < 3; i++) {
                const year = currentYear - i;
                const startDate = `${year}-01-01`;
                const endDate = `${year}-12-31`;
                await addTrace(`${year}`, startDate, endDate, true);
            }
        }

        // Setup Event Listeners
        UI.setupEventListeners({
            onAddTrace: async (label, start, end) => {
                // Determine label if empty
                if (!label || label === "Trace") {
                    label = "Trace " + (getTracesLength() + 1);
                }
                await addTrace(label, start, end);
            },
            onClearTraces: () => {
                clearTraces();
            }
        });

    } catch (err) {
        UI.setError(err.message);
    }
})();
