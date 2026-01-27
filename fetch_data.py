import os
import requests
import json
import duckdb
import pandas as pd
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import io

# Load environment variables
load_dotenv()

API_KEY = os.getenv("INTERVALS_API_KEY")
ATHLETE_ID = os.getenv("INTERVALS_ATHLETE_ID")
BASE_URL = "https://intervals.icu/api/v1"

# Database Configuration
DB_PATH = os.path.join("data", "intervals.duckdb")


def validate_config():
    if not API_KEY or not ATHLETE_ID:
        print(
            "Error: INTERVALS_API_KEY and INTERVALS_ATHLETE_ID must be set in .env file."
        )
        return False
    return True


def get_db_connection():
    return duckdb.connect(DB_PATH)


def init_db(con):
    # Create activities table
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS activities (
            id VARCHAR PRIMARY KEY,
            start_date_local TIMESTAMP,
            name VARCHAR,
            type VARCHAR,
            moving_time INTEGER,
            elapsed_time INTEGER,
            trainer BOOLEAN,
            commute BOOLEAN,
            distance REAL,
            total_elevation_gain REAL,
            sport VARCHAR,
            -- Store the full JSON blob for future flexibility
            raw_json JSON
        )
    """
    )

    # Create streams table (initially minimal, will evolve)
    # We enforce activity_id and time at minimum.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_streams (
            activity_id VARCHAR,
            time INTEGER
        )
    """
    )

    # Index for performance
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_streams_activity_id ON raw_streams(activity_id)"
    )


def get_activities(start_date=None, end_date=None):
    """
    Fetch list of activities for the athlete.
    Defaults to last 3 years if no dates provided.
    """
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    url = f"{BASE_URL}/athlete/{ATHLETE_ID}/activities"
    params = {"oldest": start_date, "newest": end_date}
    auth = ("API_KEY", API_KEY)

    print(f"Fetching activities from {start_date} to {end_date}...")
    response = requests.get(url, auth=auth, params=params)

    if response.status_code != 200:
        print(f"Failed to fetch activities: {response.status_code} - {response.text}")
        return []

    return response.json()


def save_activities_metadata(con, activities):
    """
    Upsert activities metadata into DuckDB.
    """
    if not activities:
        return

    print(f"Saving metadata for {len(activities)} activities...")

    # Prepare data for insertion
    # We map specific fields and dump the rest as JSON
    data = []
    for act in activities:
        data.append(
            {
                "id": act.get("id"),
                "start_date_local": act.get("start_date_local"),
                "name": act.get("name"),
                "type": act.get("type"),
                "moving_time": act.get("moving_time"),
                "elapsed_time": act.get("elapsed_time"),
                "trainer": act.get("trainer"),
                "commute": act.get("commute"),
                "distance": act.get("distance"),
                "total_elevation_gain": act.get("total_elevation_gain"),
                "sport": act.get("sport"),
                "raw_json": json.dumps(act),
            }
        )

    df = pd.DataFrame(data)

    # Upsert (INSERT OR REPLACE)
    # DuckDB doesn't have native UPSERT in the same way for bulk insert via DF easily without temp table,
    # but INSERT OR REPLACE works if PK exists.
    # Let's register DF and insert.
    con.register("temp_activities", df)
    con.execute("INSERT OR REPLACE INTO activities SELECT * FROM temp_activities")
    con.unregister("temp_activities")


def fetch_stream_content(activity_id):
    """
    Fetch the raw CSV stream content for a single activity.
    """
    url = f"{BASE_URL}/activity/{activity_id}/streams.csv"
    auth = ("API_KEY", API_KEY)
    try:
        response = requests.get(url, auth=auth, timeout=10)
        if response.status_code == 200:
            return activity_id, response.content
        else:
            # 404 or other errors
            return activity_id, None
    except Exception as e:
        print(f"Error fetching {activity_id}: {e}")
        return activity_id, None


def process_stream_batch(con, streams_data):
    """
    Process a batch of downloaded stream contents (bytes) and insert into DuckDB.
    Handles schema evolution.
    """
    for activity_id, content in streams_data:
        if not content:
            continue

        # 1. Parse CSV to Relation
        # We wrap bytes in a BytesIO-like object or string
        try:
            csv_str = content.decode("utf-8")
            # Check if empty or just header
            if len(csv_str.strip().split("\n")) < 2:
                continue

            # DuckDB can read from CSV string?
            # Easiest way via python client is pandas or read_csv with file-like.
            # Let's use Pandas for robustness with small CSVs, then DuckDB.
            df = pd.read_csv(io.StringIO(csv_str))

            # Add activity_id
            df["activity_id"] = activity_id

            # 2. Schema Evolution
            # Get existing columns in raw_streams
            existing_cols_info = con.execute(
                "PRAGMA table_info(raw_streams)"
            ).fetchall()
            existing_cols = {row[1] for row in existing_cols_info}  # row[1] is name

            new_cols = [col for col in df.columns if col not in existing_cols]

            for col in new_cols:
                # Infer type from DF
                dtype = df[col].dtype
                sql_type = "VARCHAR"  # Fallback
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "BIGINT"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "DOUBLE"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"

                print(
                    f"Schema Evolution: Adding column '{col}' ({sql_type}) to raw_streams"
                )
                con.execute(f'ALTER TABLE raw_streams ADD COLUMN "{col}" {sql_type}')

            # 3. Insert
            # We assume activity_id is clean.
            # Delete existing streams for this activity to enable re-runs/updates
            con.execute(f"DELETE FROM raw_streams WHERE activity_id = '{activity_id}'")

            # Append
            con.register("temp_stream", df)
            # We need to insert matching columns.
            # 'INSERT INTO raw_streams BY NAME SELECT * FROM temp_stream' is supported in recent DuckDB!
            con.execute("INSERT INTO raw_streams BY NAME SELECT * FROM temp_stream")
            con.unregister("temp_stream")

        except Exception as e:
            print(f"Failed to process stream for {activity_id}: {e}")


def main():
    if not validate_config():
        exit(1)

    os.makedirs("data", exist_ok=True)
    con = get_db_connection()
    init_db(con)

    # 1. Get List of Activities
    activities = get_activities()
    save_activities_metadata(con, activities)

    # 2. Identify Missing Streams?
    # Or just sync all?
    # User said "incremental update" in original plan, but "bulk download" in verification.
    # Let's check which IDs are already in raw_streams.
    existing_ids = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT activity_id FROM raw_streams"
        ).fetchall()
    }

    to_download = [act["id"] for act in activities if act["id"] not in existing_ids]
    print(
        f"Found {len(activities)} activities. {len(existing_ids)} exist in DB. {len(to_download)} to download."
    )

    if not to_download:
        print("Nothing new to download.")
        con.close()
        return

    # 3. Parallel Download
    # Batch size for DB insertion to avoid creating too many small transactions or locking issues?
    # DuckDB single-writer. We should fetch in parallel, then write sequentially in batches.

    BATCH_SIZE = 50
    MAX_WORKERS = 10

    # Chunk the download list
    for i in range(0, len(to_download), BATCH_SIZE):
        batch_ids = to_download[i : i + BATCH_SIZE]
        print(f"Processing batch {i} to {i+len(batch_ids)}...")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {
                executor.submit(fetch_stream_content, aid): aid for aid in batch_ids
            }
            for future in as_completed(future_to_id):
                aid, content = future.result()
                if content:
                    results.append((aid, content))

        # Write batch to DB
        process_stream_batch(con, results)
        print(f"Committed batch of {len(results)} streams.")

    con.close()
    print("Done.")


if __name__ == "__main__":
    main()
