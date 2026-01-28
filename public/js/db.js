import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm';

const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
let db = null;
let conn = null;

export async function initDuckDB() {
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

export async function loadData() {
    const statusText = document.getElementById('status-text');
    if (statusText) statusText.textContent = "Loading Parquet Data...";

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

export function getConnection() {
    return conn;
}

export async function getBounds() {
    if (!conn) throw new Error("Database not connected");
    const result = await conn.query(`
        SELECT CAST(MIN(date) AS VARCHAR) as min_d, CAST(MAX(date) AS VARCHAR) as max_d FROM metrics
    `);
    const row = result.toArray()[0];
    return {
        min: row.min_d,
        max: row.max_d
    };
}
