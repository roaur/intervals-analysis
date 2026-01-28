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

    // Load Fitness Metrics
    const fitRes = await fetch('/data/processed/fitness_metrics.parquet');
    if (!fitRes.ok) throw new Error("Failed to fetch fitness_metrics.parquet");
    const fitBuf = await fitRes.arrayBuffer();
    await db.registerFileBuffer('fitness_metrics.parquet', new Uint8Array(fitBuf));

    // Load Workout Routes
    const routeRes = await fetch('/data/processed/workout_routes.parquet');
    if (!routeRes.ok) console.warn("Failed to fetch workout_routes.parquet"); // Optional
    else {
        const routeBuf = await routeRes.arrayBuffer();
        await db.registerFileBuffer('workout_routes.parquet', new Uint8Array(routeBuf));
    }

    conn = await db.connect();

    await conn.query(`
        CREATE VIEW metrics AS SELECT * FROM parquet_scan('fitness_metrics.parquet')
    `);

    // Check if routes file exists before creating view
    if (routeRes.ok) {
        await conn.query(`
            CREATE VIEW routes AS SELECT * FROM parquet_scan('workout_routes.parquet')
        `);
    }
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

function wktToSvgPath(wkt, width, height) {
    if (!wkt || !wkt.startsWith("LINESTRING")) return "";

    // Parse coordinates: LINESTRING (x y, x y, ...)
    const content = wkt.substring(wkt.indexOf('(') + 1, wkt.lastIndexOf(')'));
    const points = content.split(',').map(p => {
        const [x, y] = p.trim().split(' ').map(Number);
        return { x, y };
    });

    if (points.length === 0) return "";

    // Normalize to bounding box
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    points.forEach(p => {
        if (p.x < minX) minX = p.x;
        if (p.x > maxX) maxX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.y > maxY) maxY = p.y;
    });

    // Add padding (5%)
    const paddingX = (maxX - minX) * 0.05;
    const paddingY = (maxY - minY) * 0.05;
    minX -= paddingX; maxX += paddingX;
    minY -= paddingY; maxY += paddingY;

    const rangeX = maxX - minX || 1;
    const rangeY = maxY - minY || 1;

    // Scale to SVG dimensions
    // SVG coordinate system: y increases downwards, but lat increases upwards.
    // So we flip Y.
    const path = points.map((p, i) => {
        const sx = ((p.x - minX) / rangeX) * width;
        const sy = height - ((p.y - minY) / rangeY) * height;
        return `${i === 0 ? 'M' : 'L'} ${sx.toFixed(1)} ${sy.toFixed(1)}`;
    }).join(' ');

    return `<path d="${path}" fill="none" stroke="#22d3ee" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />`;
}

// Helper to generate a sparkline path
function generateSparkline(data, width, height, color) {
    if (!data || data.length === 0) return "";

    // Simple numeric array expected
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;

    const points = data.map((val, i) => {
        const x = (i / (data.length - 1)) * width;
        // Invert Y so higher values are higher (SVG Y is down)
        const y = height - ((val - min) / range) * height;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    return `<polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1" />`;
}

async function renderWorkoutCards() {
    try {
        const result = await conn.query(`
            SELECT 
                activity_id, 
                activity_name, 
                date, 
                wkt_geometry,
                watts_stream,
                hr_stream,
                avg_watts, 
                avg_hr, 
                duration_seconds 
            FROM routes 
            ORDER BY date DESC 
            LIMIT 25
        `);

        const grid = document.getElementById('workout-grid');
        grid.innerHTML = '';

        const routes = result.toArray();
        if (routes.length === 0) {
            grid.innerHTML = '<p style="color: #94a3b8; padding: 1rem;">No route data available.</p>';
            return;
        }

        routes.forEach(row => {
            const card = document.createElement('div');
            card.className = 'workout-card';

            const hours = (row.duration_seconds / 3600).toFixed(1);
            const watts = Math.round(row.avg_watts || 0);
            const hr = Math.round(row.avg_hr || 0);

            // Generate map SVG
            const svgPath = wktToSvgPath(row.wkt_geometry, 300, 120);

            // Generate sparklines
            const wattsData = row.watts_stream ? Array.from(row.watts_stream) : [];
            const hrData = row.hr_stream ? Array.from(row.hr_stream) : [];

            // Simple strict sampling to prevent huge SVGs
            const sample = (arr, target) => {
                if (!arr || arr.length <= target) return arr || [];
                const step = Math.ceil(arr.length / target);
                return arr.filter((_, i) => i % step === 0);
            };

            const wattsSample = sample(wattsData, 150);
            const hrSample = sample(hrData, 150);

            const powerLine = generateSparkline(wattsSample, 300, 40, '#93c5fd');
            const hrLine = generateSparkline(hrSample, 300, 40, '#fca5a5');

            card.innerHTML = `
                <div class="card-header">
                    <span class="card-title" title="${row.activity_name}">${row.activity_name}</span>
                    <span>${row.date}</span>
                </div>
                <div class="card-map">
                    <svg viewBox="0 0 300 120" preserveAspectRatio="xMidYMid meet" style="z-index: 1;">
                        ${svgPath}
                    </svg>
                    
                    <div class="card-charts-overlay">
                        <svg viewBox="0 0 300 40" preserveAspectRatio="none">
                            <g opacity="0.6">${powerLine}</g>
                            <g opacity="0.6">${hrLine}</g>
                        </svg>
                    </div>
                </div>
                <div class="card-stats">
                    <div class="stat-item">
                        <span style="color: #fca5a5;">${hr} bpm</span>
                        <span class="stat-label">Avg HR</span>
                    </div>
                    <div class="stat-item">
                        <span style="color: #93c5fd;">${watts}W</span>
                        <span class="stat-label">Avg Power</span>
                    </div>
                    <div class="stat-item">
                        <span style="color: #cbd5e1;">${hours}h</span>
                        <span class="stat-label">Duration</span>
                    </div>
                </div>
            `;

            grid.appendChild(card);
        });

    } catch (e) {
        console.error("Error rendering workout cards:", e);
    }
}

// App Initialization
(async () => {
    try {
        await initDuckDB();
        await loadData();
        await renderWorkoutCards();

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
