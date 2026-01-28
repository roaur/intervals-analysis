import { getConnection } from './db.js';

function parseWKT(wkt) {
    if (!wkt || !wkt.startsWith("LINESTRING")) return [];
    const content = wkt.substring(wkt.indexOf('(') + 1, wkt.lastIndexOf(')'));
    return content.split(',').map(p => {
        const [x, y] = p.trim().split(' ').map(Number);
        return [x, y]; // [lon, lat]
    });
}

// Helper to generate a sparkline path (SVG for charts overlay)
function generateSparkline(data, width, height, color) {
    if (!data || data.length === 0) return "";
    const min = Math.min(...data);
    const max = Math.max(...data);
    const range = max - min || 1;
    const points = data.map((val, i) => {
        const x = (i / (data.length - 1)) * width;
        const y = height - ((val - min) / range) * height;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    return `<polyline points="${points.join(' ')}" fill="none" stroke="${color}" stroke-width="1" />`;
}

async function createMap(target, coordinates) {
    if (!coordinates || coordinates.length === 0) return;

    // Center and Zoom will be auto-fitted
    const map = new ol.Map({
        target: target,
        layers: [], // Layers will be added by olms
        view: new ol.View({
            center: [0, 0],
            zoom: 1
        }),
        controls: [], // No controls for sparkline look
        interactions: [] // No interactions (static)
    });

    // Apply OpenFreeMap style
    await olms.apply(map, 'https://tiles.openfreemap.org/styles/liberty');

    // Add Route Layer (Red Line)
    const routeFeature = new ol.Feature({
        geometry: new ol.geom.LineString(coordinates).transform('EPSG:4326', 'EPSG:3857')
    });

    const vectorLayer = new ol.layer.Vector({
        source: new ol.source.Vector({
            features: [routeFeature]
        }),
        style: new ol.style.Style({
            stroke: new ol.style.Stroke({
                color: '#ef4444', // Red
                width: 3
            })
        })
    });

    map.addLayer(vectorLayer);

    // Fit bounds
    const extent = vectorLayer.getSource().getExtent();
    map.getView().fit(extent, {
        padding: [20, 20, 20, 20],
        maxZoom: 14 // Prevent excessive zoom on short routes
    });

    return map;
}

export async function renderWorkoutCards() {
    try {
        const conn = getConnection();
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

        // We need to render DOM elements first, then init maps
        const cardData = [];

        routes.forEach((row, index) => {
            const card = document.createElement('div');
            card.className = 'workout-card';
            const cardId = `map-container-${index}`;

            const hours = (row.duration_seconds / 3600).toFixed(1);
            const watts = Math.round(row.avg_watts || 0);
            const hr = Math.round(row.avg_hr || 0);

            // Generate sparklines
            const wattsData = row.watts_stream ? Array.from(row.watts_stream) : [];
            const hrData = row.hr_stream ? Array.from(row.hr_stream) : [];

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
                <div class="card-map" id="${cardId}">
                    <!-- Map will be injected here -->
                    
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

            // Queue for map init
            const coords = parseWKT(row.wkt_geometry);
            if (coords.length > 0) {
                cardData.push({ id: cardId, coords });
            }
        });

        // Initialize maps after DOM insertion
        // We use a small delay or strict sequential init to avoid UI freezing? 
        // OpenLayers/Canvas is fast, but 25 in a row might hitch.
        // Let's just do them sequentially or in small parallel batches.

        for (const data of cardData) {
            const el = document.getElementById(data.id);
            if (el) {
                createMap(el, data.coords).catch(e => console.error(e));
                // Small yield to UI thread
                await new Promise(r => setTimeout(r, 10));
            }
        }

    } catch (e) {
        console.error("Error rendering workout cards:", e);
    }
}
