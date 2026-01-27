import duckdb
import os

PROCESSED_DIR = "data/processed"
DB_PATH = "data/intervals.duckdb"
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "fitness_metrics.parquet")


def process_data():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    if not os.path.exists(DB_PATH):
        print(f"Error: {DB_PATH} not found. Run fetch_data.py first.")
        return

    con = duckdb.connect(DB_PATH)

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
    con.execute(f"COPY daily_fitness_metrics TO '{OUTPUT_FILE}' (FORMAT PARQUET)")

    count = con.execute("SELECT COUNT(*) FROM daily_fitness_metrics").fetchone()[0]
    print(f"Processed data saved to {OUTPUT_FILE}. Rows: {count}")

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
