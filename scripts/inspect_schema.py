import duckdb

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
con = duckdb.connect(os.path.join(PROJECT_ROOT, "data", "intervals.duckdb"))
try:
    cols = con.execute("PRAGMA table_info(raw_streams)").fetchall()
    print("Columns in raw_streams:")
    for col in cols:
        print(f"- {col[1]} ({col[2]})")
except Exception as e:
    print(f"Error: {e}")
finally:
    con.close()
