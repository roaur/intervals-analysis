import duckdb
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "intervals.duckdb")
OUTPUT_FILE = "fitness_metrics.parquet"


def process_data(db_path=DB_PATH, processed_dir=PROCESSED_DIR):
    os.makedirs(processed_dir, exist_ok=True)
    output_path = os.path.join(processed_dir, OUTPUT_FILE)

    if not os.path.exists(db_path):
        print(f"Error: {db_path} not found. Run fetch_data.py first.")
        return

    con = duckdb.connect(db_path)

    print("Processing streams from DuckDB...")

    # 4. Binning and Aggregation
    # We join raw_streams with activities to get the date.
    # Note: raw_streams might have evolved columns, but we specifically need 'watts' and 'heartrate'.
    # We should check if they exist.

    cols = [col[1] for col in con.execute("PRAGMA table_info(raw_streams)").fetchall()]
    if "watts" not in cols or "heartrate" not in cols:
        print("Error: 'watts' or 'heartrate' columns missing from raw_streams.")
        con.close()
        return

    # User wanted "Bin HR for each watt".
    # We cast watts to integer to 'bin' it.

    query = """
        CREATE OR REPLACE TABLE daily_fitness_metrics AS
        SELECT 
            CAST(a.start_date_local AS DATE) as date,
            CAST(s.watts AS INTEGER) as watts,
            AVG(s.heartrate) as heartrate
        FROM raw_streams s
        JOIN activities a ON s.activity_id = a.id
        WHERE s.watts IS NOT NULL 
          AND s.heartrate IS NOT NULL
          AND s.moving = 'true' -- Optional refinement: only consider moving time? 
                                -- The stream usually has 'moving' boolean if fetched, but user requested 'watts'/'hr'.
                                -- Let's stick to base request.
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

    # Check if 'moving' column exists dynamically?
    # Let's stick to the simpler query first.
    # Check if user needs filtering. "bin my HR for each watt of power I produced".
    # Usually you want to exclude zeros.

    query = """
        CREATE OR REPLACE TABLE daily_fitness_metrics AS
        SELECT 
            CAST(a.start_date_local AS DATE) as date,
            CAST(s.watts AS INTEGER) as watts,
            AVG(s.heartrate) as heartrate,
            COUNT(*) as duration_seconds
        FROM raw_streams s
        JOIN activities a ON s.activity_id = a.id
        WHERE s.watts > 0 
          AND s.heartrate > 0
        GROUP BY 1, 2
        ORDER BY 1, 2
    """

    con.execute(query)

    # 5. Export
    con.execute(query)

    # 5. Export Fitness Metrics
    con.execute(f"COPY daily_fitness_metrics TO '{output_path}' (FORMAT PARQUET)")

    count = con.execute("SELECT COUNT(*) FROM daily_fitness_metrics").fetchone()[0]
    print(f"Processed fitness metrics saved to {output_path}. Rows: {count}")

    # 6. Spatial Data Processing (Routes)
    print("Processing spatial data (routes)...")
    try:
        con.install_extension("spatial")
        con.load_extension("spatial")

        # We need to construct LineStrings from points.
        # ST_MakeLine(ST_Point(lat, lon)) grouped by activity and ordered by time.
        # Note: DuckDB's spatial extension might require points to be constructed first.
        # Also, we need to handle the list aggregation.

        # Strategy:
        # 1. Select relevant points (lat/lng not null) ordered by time per activity.
        # 2. Use list_aggr to create a list of points or geometries?
        #    Actually, `ST_MakeLine` can take a list of geometries (points).

        spatial_query = """
            CREATE OR REPLACE TABLE workout_routes AS
            WITH ordered_points AS (
                SELECT 
                    s.activity_id,
                    s.time,
                    ST_Point(s.lng, s.lat) as geom,
                    s.watts,
                    s.heartrate
                FROM raw_streams s
                WHERE s.lat IS NOT NULL AND s.lng IS NOT NULL
                ORDER BY s.activity_id, s.time
            ),
            routes AS (
                SELECT
                    activity_id,
                    ST_MakeLine(list(geom)) as geometry,
                    list(watts) as watts_stream,
                    list(heartrate) as hr_stream,
                    AVG(watts) as avg_watts,
                    AVG(heartrate) as avg_hr,
                    MAX(time) - MIN(time) as duration_seconds
                FROM ordered_points
                GROUP BY activity_id
            )
            SELECT
                r.*,
                a.name as activity_name,
                CAST(a.start_date_local AS DATE) as date
            FROM routes r
            JOIN activities a ON r.activity_id = a.id
        """
        con.execute(spatial_query)

        # Export to Parquet
        export_query = f"""
            COPY (
                SELECT 
                    activity_id,
                    activity_name,
                    date,
                    ST_AsText(geometry) as wkt_geometry,
                    watts_stream,
                    hr_stream,
                    avg_watts,
                    avg_hr,
                    duration_seconds
                FROM workout_routes
                ORDER BY date DESC
            ) TO '{os.path.join(processed_dir, "workout_routes.parquet")}' (FORMAT PARQUET)
        """
        con.execute(export_query)

        route_count = con.execute("SELECT COUNT(*) FROM workout_routes").fetchone()[0]
        print(
            f"Processed workout routes saved to {os.path.join(processed_dir, 'workout_routes.parquet')}. Rows: {route_count}"
        )

    except Exception as e:
        print(f"Spatial processing failed: {e}")
        # Don't fail the whole process if spatial fails, unless critical?
        # Let's verify if spatial extension issues are common.
        # For now, print error.

    con.close()


def main():
    try:
        process_data()
    except Exception as e:
        print(f"Processing failed: {e}")
        import traceback

        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
