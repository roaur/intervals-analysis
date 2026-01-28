import unittest
from unittest.mock import patch, MagicMock
import os
import sys
import pandas as pd
import duckdb
import json

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fetch_data


class TestFetchDataDuckDB(unittest.TestCase):

    def setUp(self):
        # Use in-memory DuckDB for DB logic tests
        self.con = duckdb.connect(":memory:")

        # Setup mock env vars
        self.env_patcher = patch.dict(
            os.environ,
            {"INTERVALS_API_KEY": "test_key", "INTERVALS_ATHLETE_ID": "test_athlete"},
        )
        self.env_patcher.start()
        fetch_data.API_KEY = "test_key"
        fetch_data.ATHLETE_ID = "test_athlete"

    def tearDown(self):
        self.con.close()
        self.env_patcher.stop()
        patch.stopall()

    def test_init_db(self):
        """Test that tables are created with correct schema"""
        fetch_data.init_db(self.con)

        # Check activities table
        tables = self.con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        self.assertIn("activities", table_names)
        self.assertIn("raw_streams", table_names)

        # Check an index exists
        # DuckDB stores indexes in system catalog
        indexes = self.con.execute(
            "SELECT * FROM pg_indexes WHERE tablename = 'raw_streams'"
        ).fetchall()
        self.assertTrue(len(indexes) > 0)

    def test_save_activities_metadata(self):
        """Test upsert logic for activities"""
        fetch_data.init_db(self.con)

        # 1. Insert new activity
        activities = [
            {
                "id": "act1",
                "start_date_local": "2023-01-01T10:00:00",
                "name": "Ride 1",
                "type": "Ride",
                "moving_time": 3600,
                "elapsed_time": 4000,
            }
        ]
        fetch_data.save_activities_metadata(self.con, activities)

        res = self.con.execute(
            "SELECT id, name, moving_time FROM activities WHERE id='act1'"
        ).fetchone()
        self.assertEqual(res, ("act1", "Ride 1", 3600))

        # 2. Update existing activity (Upsert)
        activities_update = [
            {
                "id": "act1",
                "start_date_local": "2023-01-01T10:00:00",
                "name": "Ride 1 Updated",  # Changed
                "type": "Ride",
                "moving_time": 3600,
                "elapsed_time": 4000,
            }
        ]
        fetch_data.save_activities_metadata(self.con, activities_update)

        res_updated = self.con.execute(
            "SELECT name FROM activities WHERE id='act1'"
        ).fetchone()
        self.assertEqual(res_updated[0], "Ride 1 Updated")

        # Verify no meaningful duplicates (count should be 1)
        count = self.con.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
        self.assertEqual(count, 1)

    def test_process_stream_batch_schema_evolution(self):
        """Test adding new columns dynamically and inserting data"""
        fetch_data.init_db(self.con)

        # Batch 1: Basic stream (time, watts)
        csv_content = b"time,watts\n1,100\n2,110"
        streams_data = [("act1", csv_content)]

        fetch_data.process_stream_batch(self.con, streams_data)

        # Verify schema
        cols = [
            c[1] for c in self.con.execute("PRAGMA table_info(raw_streams)").fetchall()
        ]
        self.assertIn("watts", cols)

        # Verify data
        res = self.con.execute(
            "SELECT time, watts FROM raw_streams WHERE activity_id='act1' ORDER BY time"
        ).fetchall()
        self.assertEqual(res, [(1, 100), (2, 110)])

        # Batch 2: New column (heartrate) + Existing activity update (idempotency check)
        # We re-send act1 with HR data. Logic says it should DELETE and INSERT.
        csv_content_2 = b"time,watts,heartrate\n1,100,60\n2,110,65"
        streams_data_2 = [("act1", csv_content_2)]

        fetch_data.process_stream_batch(self.con, streams_data_2)

        # Verify schema update
        cols_2 = [
            c[1] for c in self.con.execute("PRAGMA table_info(raw_streams)").fetchall()
        ]
        self.assertIn("heartrate", cols_2)

        # Verify data updated
        res_2 = self.con.execute(
            "SELECT time, watts, heartrate FROM raw_streams WHERE activity_id='act1' ORDER BY time"
        ).fetchall()
        self.assertEqual(res_2, [(1, 100, 60), (2, 110, 65)])

        # Verify no duplicates
        count = self.con.execute(
            "SELECT COUNT(*) FROM raw_streams WHERE activity_id='act1'"
        ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_process_stream_batch_type_casting(self):
        """Test that types are inferred correctly (FLOAT/DOUBLE for decimals)"""
        fetch_data.init_db(self.con)

        csv_content = b"time,velocity_smooth\n1,10.5\n2,11.2"
        streams_data = [("act_float", csv_content)]

        fetch_data.process_stream_batch(self.con, streams_data)

        # Check type of velocity_smooth
        type_info = self.con.execute("PRAGMA table_info(raw_streams)").df()
        row = type_info[type_info["name"] == "velocity_smooth"].iloc[0]
        self.assertIn(row["type"], ["DOUBLE", "FLOAT"])

    @patch("fetch_data.requests.get")
    def test_get_activities(self, mock_get):
        """Test API interaction"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "act1"}]
        mock_get.return_value = mock_response

        acts = fetch_data.get_activities()
        self.assertEqual(len(acts), 1)


if __name__ == "__main__":
    unittest.main()
