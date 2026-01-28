import duckdb


import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def inspect():
    con = duckdb.connect(os.path.join(PROJECT_ROOT, "data", "intervals.duckdb"))

    # Check for spatial extension
    try:
        con.install_extension("spatial")
        con.load_extension("spatial")
        print("Spatial extension loaded successfully.")
    except Exception as e:
        print(f"Failed to load spatial extension: {e}")

    # Inspect raw_streams table
    print("\nTable: raw_streams")
    try:
        cols = con.execute("PRAGMA table_info(raw_streams)").fetchall()
        for col in cols:
            print(col)
    except Exception as e:
        print(f"Could not inspect raw_streams: {e}")

    con.close()


if __name__ == "__main__":
    inspect()
