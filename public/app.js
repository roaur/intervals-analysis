import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
let db = null;
let conn = null;
let traces = [];

// Color palette for traces
const COLORS = [
    '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6',
    '#ec4899', '#06b6d4', '#f97316'
];

async function initDuckDB() {
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );

    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    URL.revokeObjectURL(worker_url);
}

async function loadData() {
    const statusText = document.getElementById('status-text');
    statusText.textContent = "Loading Parquet Data...";

    const response = await fetch('/data/processed/fitness_metrics.parquet');
    if (!response.ok) throw new Error("Failed to fetch parquet file");

    const buffer = await response.arrayBuffer();
    await db.registerFileBuffer('fitness_metrics.parquet', new Uint8Array(buffer));

    conn = await db.connect();
    await conn.query(`
        CREATE VIEW metrics AS SELECT * FROM parquet_scan('fitness_metrics.parquet')
    `);
}

async function getBounds() {
    // Cast to VARCHAR to ensure YYYY-MM-DD format for date inputs
    const result = await conn.query(`
        SELECT CAST(MIN(date) AS VARCHAR) as min_d, CAST(MAX(date) AS VARCHAR) as max_d FROM metrics
    `);
    const row = result.toArray()[0];
    return {
        min: row.min_d,
        max: row.max_d
    };
}

async function addTrace(label, startDate, endDate, silent = false) {
    if (!startDate || !endDate) {
        if (!silent) alert("Please select start and end dates");
        return;
    }

    const maxWattsInput = document.getElementById('max-watts');
    const maxWatts = maxWattsInput ? maxWattsInput.value : 400;

    if (!silent) console.log(`Adding trace '${label}' with Max Watts: ${maxWatts}`);

    // HR vs Power: Group by 5W buckets to smooth data
    // FLOOR(watts / 5) * 5

    const query = `
        SELECT 
            CAST(FLOOR(watts / 5) * 5 AS INTEGER) as watt_bucket, 
            AVG(heartrate) as avg_hr
        FROM metrics
        WHERE date >= '${startDate}' 
          AND date <= '${endDate}'
          AND watts <= ${maxWatts}
          AND heartrate > 40 AND heartrate < 210 -- Basic outlier filtering
        GROUP BY watt_bucket
        ORDER BY watt_bucket
    `;

    const result = await conn.query(query);
    const watts = result.getChild('watt_bucket').toArray();
    const hrs = result.getChild('avg_hr').toArray();

    if (watts.length === 0) {
        if (!silent) alert(`No data found for this range: ${label}`);
        console.log(`No data for ${label}`);
        return;
    }

    const color = COLORS[traces.length % COLORS.length];

    const trace = {
        x: watts,
        y: hrs,
        type: 'scatter',
        mode: 'lines',
        name: `${label}`,
        line: {
            color: color,
            width: 3,
            shape: 'spline'
        }
    };

    traces.push(trace);
    renderChart();
}

function renderChart() {
    const layout = {
        title: 'Fitness Efficiency (Lower HR at same Power is better)',
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: '#e2e8f0' },
        xaxis: {
            title: 'Power (Watts)',
            gridcolor: 'rgba(255,255,255,0.1)'
        },
        yaxis: {
            title: 'Heart Rate (bpm)',
            gridcolor: 'rgba(255,255,255,0.1)'
        },
        hovermode: 'closest'
    };

    Plotly.newPlot('fitness-chart', traces, layout);
}

// App Initialization
(async () => {
    try {
        await initDuckDB();
        await loadData();

        document.getElementById('loading').classList.add('hidden');
        document.getElementById('dashboard').classList.remove('hidden');

        // Set default dates
        const bounds = await getBounds();
        const startInput = document.getElementById('start-date');
        const endInput = document.getElementById('end-date');

        if (bounds.min) {
            startInput.value = bounds.min;
            endInput.value = bounds.max;

            // Auto-populate previous 3 years (Current Year, Last Year, Year Before)
            const currentYear = new Date().getFullYear();

            for (let i = 0; i < 3; i++) {
                const year = currentYear - i;
                const startDate = `${year}-01-01`;
                const endDate = `${year}-12-31`;
                await addTrace(`${year}`, startDate, endDate, true); // Silent mode
            }
        }

        // Event Listeners
        document.getElementById('add-trace-btn').addEventListener('click', async () => {
            const label = document.getElementById('trace-label').value || "Trace " + (traces.length + 1);
            const start = startInput.value;
            const end = endInput.value;
            await addTrace(label, start, end);
        });

        document.getElementById('clear-traces-btn').addEventListener('click', () => {
            traces = [];
            Plotly.newPlot('fitness-chart', [], {});
        });

    } catch (err) {
        console.error(err);
        document.getElementById('status-text').textContent = "Error: " + err.message;
        document.getElementById('status-text').style.color = 'red';
    }
})();
