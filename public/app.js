import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
let db = null;
let conn = null;
let traces = [];
let distTraces = [];

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

    // 1. Fitness Query
    const fitnessQuery = `
        SELECT 
            CAST(FLOOR(watts / 5) * 5 AS INTEGER) as watt_bucket, 
            AVG(heartrate) as avg_hr
        FROM metrics
        WHERE date >= '${startDate}' 
          AND date <= '${endDate}'
          AND watts <= ${maxWatts}
          AND heartrate > 40 AND heartrate < 210
        GROUP BY watt_bucket
        ORDER BY watt_bucket
    `;

    // 2. Distribution Query
    const distQuery = `
        SELECT 
            CAST(FLOOR(watts / 5) * 5 AS INTEGER) as watt_bucket, 
            SUM(duration_seconds) / 3600.0 as hours
        FROM metrics
        WHERE date >= '${startDate}' 
          AND date <= '${endDate}'
          AND watts <= ${maxWatts}
        GROUP BY watt_bucket
        ORDER BY watt_bucket
    `;

    const [fitnessRes, distRes] = await Promise.all([
        conn.query(fitnessQuery),
        conn.query(distQuery)
    ]);

    const fitnessWatts = fitnessRes.getChild('watt_bucket').toArray();
    const fitnessHrs = fitnessRes.getChild('avg_hr').toArray();

    const distWatts = distRes.getChild('watt_bucket').toArray();
    const distHours = distRes.getChild('hours').toArray();

    if (fitnessWatts.length === 0) {
        if (!silent) alert(`No data found for this range: ${label}`);
        return;
    }

    const color = COLORS[traces.length % COLORS.length];

    traces.push({
        x: fitnessWatts,
        y: fitnessHrs,
        type: 'scatter',
        mode: 'lines',
        name: `${label}`,
        line: { color: color, width: 3, shape: 'spline' }
    });

    distTraces.push({
        x: distWatts,
        y: distHours,
        type: 'scatter',
        mode: 'lines',
        name: `${label}`,
        line: { color: color, width: 2, shape: 'spline' },
        fill: 'tozeroy'
    });

    renderCharts();
}

function renderCharts() {
    const commonLayout = {
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: '#e2e8f0' },
        hovermode: 'closest',
        xaxis: {
            title: 'Power (Watts)',
            gridcolor: 'rgba(255,255,255,0.1)'
        }
    };

    const fitnessLayout = {
        ...commonLayout,
        title: 'Fitness Efficiency (Lower HR at same Power is better)',
        yaxis: {
            title: 'Heart Rate (bpm)',
            gridcolor: 'rgba(255,255,255,0.1)'
        }
    };

    const distLayout = {
        ...commonLayout,
        title: 'Power Duration Distribution',
        yaxis: {
            title: 'Duration (Hours)',
            gridcolor: 'rgba(255,255,255,0.1)'
        }
    };

    Plotly.newPlot('fitness-chart', traces, fitnessLayout);
    Plotly.newPlot('distribution-chart', distTraces, distLayout);
}

// App Initialization
(async () => {
    try {
        await initDuckDB();
        await loadData();

        document.getElementById('loading').classList.add('hidden');
        document.getElementById('dashboard').classList.remove('hidden');

        const bounds = await getBounds();
        const startInput = document.getElementById('start-date');
        const endInput = document.getElementById('end-date');

        if (bounds.min) {
            startInput.value = bounds.min;
            endInput.value = bounds.max;

            const currentYear = new Date().getFullYear();
            for (let i = 0; i < 3; i++) {
                const year = currentYear - i;
                const startDate = `${year}-01-01`;
                const endDate = `${year}-12-31`;
                await addTrace(`${year}`, startDate, endDate, true);
            }
        }

        document.getElementById('add-trace-btn').addEventListener('click', async () => {
            const label = document.getElementById('trace-label').value || "Trace " + (traces.length + 1);
            const start = startInput.value;
            const end = endInput.value;
            await addTrace(label, start, end);
        });

        document.getElementById('clear-traces-btn').addEventListener('click', () => {
            traces = [];
            distTraces = [];
            Plotly.newPlot('fitness-chart', [], {});
            Plotly.newPlot('distribution-chart', [], {});
        });

    } catch (err) {
        console.error(err);
        document.getElementById('status-text').textContent = "Error: " + err.message;
        document.getElementById('status-text').style.color = 'red';
    }
})();
