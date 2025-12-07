import os
import sqlite3
import sys

def main():
    db_path = os.path.abspath("file_index.db")
    if not os.path.exists(db_path):
        print(f"[ERROR] file_index.db not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # List tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    print("Tables:", tables)

    for table in tables:
        cur.execute(f"PRAGMA table_info({table})")
        cols = cur.fetchall()
        print(f"Schema for {table}:")
        for col in cols:
            # (cid, name, type, notnull, dflt_value, pk)
            print("  ", col)

    conn.close()

if __name__ == "__main__":
    main()
