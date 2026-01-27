import unittest
from unittest.mock import patch, MagicMock, ANY
import os
import sys
import pandas as pd
import duckdb

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import fetch_data


class TestFetchDataIDB(unittest.TestCase):

    def setUp(self):
        # Setup mock env vars
        self.env_patcher = patch.dict(
            os.environ,
            {"INTERVALS_API_KEY": "test_key", "INTERVALS_ATHLETE_ID": "test_athlete"},
        )
        self.env_patcher.start()
        fetch_data.API_KEY = "test_key"
        fetch_data.ATHLETE_ID = "test_athlete"

        # Mock DB connection
        self.mock_con = MagicMock()
        self.mock_duckdb_connect = patch(
            "fetch_data.duckdb.connect", return_value=self.mock_con
        ).start()

    def tearDown(self):
        self.env_patcher.stop()
        patch.stopall()

    @patch("fetch_data.requests.get")
    def test_get_activities(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "act1"}]
        mock_get.return_value = mock_response

        acts = fetch_data.get_activities()
        self.assertEqual(len(acts), 1)

    def test_save_activities_metadata(self):
        activities = [{"id": "act1", "name": "Ride 1"}]
        fetch_data.save_activities_metadata(self.mock_con, activities)

        # Verify register and insert were called
        self.mock_con.register.assert_called()
        self.mock_con.execute.assert_called()
        # Check if INSERT OR REPLACE was called
        args, _ = self.mock_con.execute.call_args
        self.assertIn("INSERT OR REPLACE INTO activities", args[0])

    def test_process_stream_batch_schema_evolution(self):
        # Setup mock for table info (existing columns)
        # Returns list of tuples (cid, name, type, ...)
        self.mock_con.execute.return_value.fetchall.return_value = [
            (0, "activity_id", "VARCHAR", 0, None, 0),
            (1, "time", "INTEGER", 0, None, 0),
        ]

        # New stream has 'watts' (new) and 'time' (existing)
        csv_content = b"time,watts\n1,100\n2,110"
        streams_data = [("act1", csv_content)]

        fetch_data.process_stream_batch(self.mock_con, streams_data)

        # Expect ALTER TABLE for 'watts'
        # Check all calls to execute
        calls = [args[0] for args, _ in self.mock_con.execute.call_args_list]

        # Verify ALTER TABLE
        alter_calls = [c for c in calls if "ALTER TABLE raw_streams ADD COLUMN" in c]
        self.assertTrue(any('"watts"' in c for c in alter_calls))

        # Verify INSERT
        insert_calls = [c for c in calls if "INSERT INTO raw_streams" in c]
        self.assertTrue(insert_calls)


if __name__ == "__main__":
    unittest.main()
