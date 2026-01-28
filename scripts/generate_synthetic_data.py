import os
import json
import duckdb
import pandas as pd
import random
from datetime import datetime, timedelta

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "intervals_test.duckdb")


def init_db(con):
    # Same schema as fetch_data.py
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
            raw_json JSON
        )
    """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_streams (
            activity_id VARCHAR,
            time INTEGER,
            watts INTEGER,
            heartrate INTEGER
        )
    """
    )


def generate_synthetic_data(num_activities=10):
    os.makedirs(os.path.join(PROJECT_ROOT, "data"), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    con.execute("DELETE FROM activities")
    con.execute("DELETE FROM raw_streams")

    base_date = datetime.now() - timedelta(days=num_activities)

    print(f"Generating {num_activities} synthetic activities in DuckDB...")

    activities_data = []

    for i in range(num_activities):
        activity_id = f"act_{i}"
        activity_date = (base_date + timedelta(days=i)).strftime("%Y-%m-%dT09:00:00")

        activities_data.append(
            {
                "id": activity_id,
                "start_date_local": activity_date,
                "name": f"Ride {i}",
                "type": "Ride",
                "moving_time": 3600,
                "elapsed_time": 3600,
                "trainer": False,
                "commute": False,
                "distance": 30000,
                "total_elevation_gain": 500,
                "sport": "Ride",
                "raw_json": "{}",
            }
        )

        # Generate Streams
        # 100 data points
        stream_data = []
        hr = 100
        for t in range(100):
            power = 100 + (t % 50) * 2
            target_hr = 100 + (power - 100) * 0.5 + (t * 0.05)
            hr += (target_hr - hr) * 0.1
            stream_data.append(
                {
                    "activity_id": activity_id,
                    "time": t,
                    "watts": int(power),
                    "heartrate": int(hr),
                }
            )

        # Bulk Insert Streams
        con.execute("INSERT INTO raw_streams SELECT * FROM pd.DataFrame(stream_data)")

    # Validating activities insert
    con.execute("INSERT INTO activities SELECT * FROM pd.DataFrame(activities_data)")

    count_act = con.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
    count_stream = con.execute("SELECT COUNT(*) FROM raw_streams").fetchone()[0]

    print(f"Generated {count_act} activities and {count_stream} stream points.")
    con.close()


if __name__ == "__main__":
    generate_synthetic_data()
