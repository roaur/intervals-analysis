import { getConnection } from './db.js';

let traces = [];
let distTraces = [];

const COLORS = [
    '#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6',
    '#ec4899', '#06b6d4', '#f97316'
];

export async function addTrace(label, startDate, endDate, silent = false) {
    if (!startDate || !endDate) {
        if (!silent) alert("Please select start and end dates");
        return;
    }

    const maxWattsInput = document.getElementById('max-watts');
    const maxWatts = maxWattsInput ? maxWattsInput.value : 400;
    const conn = getConnection();

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

export function renderCharts() {
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

    // Assuming Plotly is loaded globally via script tag as per original index.html
    Plotly.newPlot('fitness-chart', traces, fitnessLayout);
    Plotly.newPlot('distribution-chart', distTraces, distLayout);
}

export function clearTraces() {
    traces = [];
    distTraces = [];
    Plotly.newPlot('fitness-chart', [], {});
    Plotly.newPlot('distribution-chart', [], {});
}

export function getTracesLength() {
    return traces.length;
}
