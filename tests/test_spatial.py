import pytest
import duckdb
import os
import shutil
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from process_data import process_data

# Use a temporary database for testing
TEST_DB_PATH = "tests/test_spatial.duckdb"
TEST_PROCESSED_DIR = "tests/data_spatial/processed"


@pytest.fixture(scope="module")
def db_path():
    # Setup
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(TEST_PROCESSED_DIR):
        shutil.rmtree(TEST_PROCESSED_DIR)

    os.makedirs(TEST_PROCESSED_DIR, exist_ok=True)

    # Pre-install spatial if possible/needed or let code do it
    # We just provide the path.

    yield TEST_DB_PATH

    # Teardown
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(TEST_PROCESSED_DIR):
        shutil.rmtree("tests/data_spatial")


def setup_data(db_path):
    con = duckdb.connect(db_path)

    # Create tables expected by process_data
    # Note: process_data EXPECTS tables to exist (created by fetch_data usually).
    # We must replicate the schema from fetch_data or process_data assumptions.

    con.execute(
        """
        CREATE TABLE activities (
            id VARCHAR, 
            name VARCHAR, 
            start_date_local VARCHAR,
            -- Add other cols so it doesn't crash if query uses * or specific cols?
            -- process_data uses: start_date_local, name, id.
        )
    """
    )

    con.execute(
        """
        CREATE TABLE raw_streams (
            activity_id VARCHAR, 
            time INTEGER, 
            lat DOUBLE, 
            lng DOUBLE, 
            watts DOUBLE, 
            heartrate DOUBLE,
            moving VARCHAR -- process_data checks moving='true' for fitness metrics but maybe not spatial? 
                           -- Actually spatial query checks 'lat IS NOT NULL'.
        )
    """
    )

    # Insert sample data for one activity
    # A simple diagonal line: (0,0) -> (1,1)
    con.execute(
        "INSERT INTO activities VALUES ('act1', 'Test Ride', '2023-01-01 10:00:00')"
    )

    # Point 1
    con.execute(
        "INSERT INTO raw_streams VALUES ('act1', 0, 0.0, 0.0, 100, 120, 'true')"
    )
    # Point 2
    con.execute(
        "INSERT INTO raw_streams VALUES ('act1', 10, 1.0, 1.0, 150, 130, 'true')"
    )

    con.close()


def test_spatial_processing_integration(db_path):
    # 1. Setup mock data in the DB
    setup_data(db_path)

    # 2. Run the ACTUAL application logic
    # This tests the query inside process_data.py, not a copy.
    process_data(db_path=db_path, processed_dir=TEST_PROCESSED_DIR)

    # 3. Verify Results
    con = duckdb.connect(db_path)
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Check table existence
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    assert "workout_routes" in tables

    # Check content
    row = con.execute(
        "SELECT * FROM workout_routes WHERE activity_id='act1'"
    ).fetchone()
    # Schema check: activity_id, geometry, avg_watts, avg_hr, duration, activity_name, date ... (order depends on query)

    # The query in process_data.py selects:
    # r.* (activity_id, geometry, avg_watts, avg_hr, duration), activity_name, date

    # Let's fetch as dict or verify specific columns
    df = con.execute("SELECT * FROM workout_routes").df()
    assert len(df) == 1
    assert df.iloc[0]["activity_id"] == "act1"
    assert df.iloc[0]["avg_watts"] == 125.0

    # Verify Geometry
    # ST_AsText might not be standard in basic duckdb result without function call,
    # but we can call it in SQL.
    wkt = con.execute("SELECT ST_AsText(geometry) FROM workout_routes").fetchone()[0]
    assert wkt == "LINESTRING (0 0, 1 1)"

    con.close()


def test_spatial_export_file(db_path):
    # Verify parquet file was created by process_data
    output_file = os.path.join(TEST_PROCESSED_DIR, "workout_routes.parquet")
    assert os.path.exists(output_file)

    # Verify we can read it
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Read parquet
    # Note: process_data exports as WKB (wkb_geometry)
    df = con.execute(f"SELECT * FROM '{output_file}'").df()
    assert len(df) == 1
    # Check for columns present in current process_data.py export
    assert "wkt_geometry" in df.columns
    assert "watts_stream" in df.columns
    assert "hr_stream" in df.columns
