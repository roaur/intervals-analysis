import pytest
import duckdb
import os
import shutil
import pandas as pd
from datetime import datetime, timedelta

# Import the refactored function
import sys

sys.path.append(
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts")
)
from process_data import process_data

# Constants for testing
TEST_DB_PATH = "tests/test_analysis.duckdb"
TEST_PROCESSED_DIR = "tests/data_analysis/processed"


@pytest.fixture(scope="module")
def setup_test_environment():
    # Cleanup before
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(TEST_PROCESSED_DIR):
        shutil.rmtree(TEST_PROCESSED_DIR)

    os.makedirs(TEST_PROCESSED_DIR, exist_ok=True)

    # Initialize DB
    con = duckdb.connect(TEST_DB_PATH)
    # Load Spatial for completeness as process_data tries to use it
    con.install_extension("spatial")
    con.load_extension("spatial")

    yield con

    # Cleanup after
    con.close()
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)
    if os.path.exists(TEST_PROCESSED_DIR):
        shutil.rmtree("tests/data_analysis")


def create_mock_data(con):
    # Create tables
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS activities (
            id VARCHAR PRIMARY KEY,
            start_date_local TIMESTAMP,
            name VARCHAR,
            type VARCHAR,
            moving_time INTEGER
        )
    """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_streams (
            activity_id VARCHAR,
            time INTEGER,
            watts DOUBLE,
            heartrate DOUBLE,
            lat DOUBLE,
            lng DOUBLE,
            moving VARCHAR
        )
    """
    )

    # 1. Activity 1: Steady 100W, 120HR (Valid)
    con.execute(
        "INSERT INTO activities VALUES ('act1', '2023-01-01 10:00:00', 'Steady Ride', 'Ride', 3600)"
    )
    # Generate 10 seconds of data
    for i in range(10):
        con.execute(
            f"INSERT INTO raw_streams VALUES ('act1', {i}, 100, 120, 0, 0, 'true')"
        )

    # 2. Activity 2: Varying Power (100W, 200W) (Valid)
    con.execute(
        "INSERT INTO activities VALUES ('act2', '2023-01-02 10:00:00', 'Intervals', 'Ride', 3600)"
    )
    for i in range(5):
        con.execute(
            f"INSERT INTO raw_streams VALUES ('act2', {i}, 100, 110, 0, 0, 'true')"
        )
    for i in range(5, 10):
        con.execute(
            f"INSERT INTO raw_streams VALUES ('act2', {i}, 200, 150, 0, 0, 'true')"
        )

    # 3. Activity 3: Zeros/Nulls (Should be filtered)
    con.execute(
        "INSERT INTO activities VALUES ('act3', '2023-01-03 10:00:00', 'Zero Ride', 'Ride', 3600)"
    )
    con.execute(
        "INSERT INTO raw_streams VALUES ('act3', 0, 0, 100, 0, 0, 'true')"
    )  # 0 Watts -> Filter
    con.execute(
        "INSERT INTO raw_streams VALUES ('act3', 1, 100, 0, 0, 0, 'true')"
    )  # 0 HR -> Filter
    con.execute(
        "INSERT INTO raw_streams VALUES ('act3', 2, NULL, 100, 0, 0, 'true')"
    )  # Null Watts -> Filter
    con.execute(
        "INSERT INTO raw_streams VALUES ('act3', 3, 100, 100, 0, 0, 'true')"
    )  # Valid


def test_process_fitness_metrics(setup_test_environment):
    con = setup_test_environment
    create_mock_data(con)
    con.close()  # Close to allow process_data to open it

    # Run process_data
    process_data(db_path=TEST_DB_PATH, processed_dir=TEST_PROCESSED_DIR)

    # Verify results
    # Re-open to check
    con = duckdb.connect(TEST_DB_PATH)

    # Check daily_fitness_metrics
    df = con.execute("SELECT * FROM daily_fitness_metrics ORDER BY date, watts").df()

    # Validate Activity 1 (100W, 120HR)
    row1 = df[df["date"] == pd.Timestamp("2023-01-01")]
    assert len(row1) == 1
    assert row1.iloc[0]["watts"] == 100
    assert row1.iloc[0]["heartrate"] == 120.0
    assert row1.iloc[0]["duration_seconds"] == 10

    # Validate Activity 2 (100W (avg HR 110) & 200W (avg HR 150))
    row2 = df[df["date"] == pd.Timestamp("2023-01-02")]
    assert len(row2) == 2

    # Bin 100W
    bin_100 = row2[row2["watts"] == 100].iloc[0]
    assert bin_100["heartrate"] == 110.0
    assert bin_100["duration_seconds"] == 5

    # Bin 200W
    bin_200 = row2[row2["watts"] == 200].iloc[0]
    assert bin_200["heartrate"] == 150.0
    assert bin_200["duration_seconds"] == 5

    # Validate Activity 3 (Only 1 valid point: 100W, 100HR)
    row3 = df[df["date"] == pd.Timestamp("2023-01-03")]
    assert len(row3) == 1
    assert row3.iloc[0]["watts"] == 100
    assert row3.iloc[0]["heartrate"] == 100.0
    assert row3.iloc[0]["duration_seconds"] == 1

    con.close()


def test_parquet_output_exists():
    output_file = os.path.join(TEST_PROCESSED_DIR, "fitness_metrics.parquet")
    assert os.path.exists(output_file)

    con = duckdb.connect()
    count = con.execute(f"SELECT COUNT(*) FROM '{output_file}'").fetchone()[0]
    # Total rows: 1 (act1) + 2 (act2) + 1 (act3) = 4
    assert count == 4


def test_missing_db_handling(capsys):
    # Test graceful exit if DB missing
    process_data(db_path="non_existent.duckdb", processed_dir=TEST_PROCESSED_DIR)
    captured = capsys.readouterr()
    assert "Error: non_existent.duckdb not found" in captured.out
