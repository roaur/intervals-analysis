# Intervals.icu Analysis

A privacy-focused local application that pulls your workout streams (Power, Heart Rate, etc.) from Intervals.icu, calculates fitness trends (HR efficiency vs Power), and visualizes them in an interactive web dashboard.

## Features

- **Data Ingestion**: Incremental downloading of activity streams from the Intervals.icu API.
- **Local Processing**: detailed analysis using **DuckDB** to bin and aggregate heart rate data by power output.
- **Privacy First**: All data is stored and processed locally on your machine.
- **Interactive Visualization**: High-performance charting using **DuckDB-WASM** and Plotly.js directly in the browser.

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (recommended for dependency management)
- An Intervals.icu account (API Key and Athlete ID required)

## Setup

1.  **Clone the repository** (if you haven't already).

2.  **Environment Setup**:
    Copy the example environment file and fill in your credentials:
    ```bash
    cp .env.example .env
    ```
    Edit `.env`:
    - `INTERVALS_API_KEY`: Your API key from Intervals.icu settings.
    - `INTERVALS_ATHLETE_ID`: Your athlete ID (found in the URL of your profile).

3.  **Install Dependencies**:
    ```bash
    uv sync
    ```

## Usage

### 1. Ingest Data
Download your activity streams into a local DuckDB database (`data/intervals.duckdb`). This uses parallel requests for speed and incrementally updates new activities.
```bash
uv run python scripts/fetch_data.py
```

### 2. Process Data
Aggregate the raw streams from the DuckDB database into a summarized Parquet file for the frontend.
```bash
uv run python scripts/process_data.py
```
*Note: A `data/processed/fitness_metrics.parquet` file will be generated.*

### 3. Visualize
Start a local web server to view the dashboard:
```bash
python3 -m http.server 8000
```
Open **[http://localhost:8000/public/](http://localhost:8000/public/)** in your browser.

## Testing
To run the unit tests:
```bash
uv run python -m unittest discover tests
```

To generate synthetic data for testing without an API key:
```bash
uv run python scripts/generate_synthetic_data.py
```
