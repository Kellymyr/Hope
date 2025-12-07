#!/usr/bin/env python3
"""
Command‑line interface for managing SQLite databases.

Features
--------
* Auto‑detect ``*.db`` files in a directory (default: ``./Databases``)
* List databases, list tables, fetch rows, insert, update, delete
* Uses the same ``SQLiteManager`` implementation from ``db_manager_agent.py``,
  which already enables WAL mode and a 30 s busy timeout.
* All operations are performed via sub‑commands similar to typical CLI tools.

Usage
-----
python db_manager_cli.py --db-dir ./Databases list-dbs
python db_manager_cli.py --db-dir ./Databases list-tables <db_path>
python db_manager_cli.py --db-dir ./Databases fetch <db_path> <table> [--columns col1,col2] [--where '{"col":"value"}'] [--order-by col] [--desc] [--limit N]
python db_manager_cli.py --db-dir ./Databases insert <db_path> <table> --data '{"col1":"val1","col2":2}'
python db_manager_cli.py --db-dir ./Databases update <db_path> <table> --data '{"col1":"new"}' --where '{"id":1}'
python db_manager_cli.py --db-dir ./Databases delete <db_path> <table> --where '{"id":1}'
"""

import argparse
import json
import sys
import uuid
from typing import Any, Dict, List, Optional

# Import the manager implementation
from db_manager_agent import SQLiteManager
# Removed custom DB_DIR; will rely on SQLiteManager's default "./Databases" directory


def parse_json(arg: str) -> Dict:
    """Parse a JSON string into a dict for ``--data`` and ``--where``."""
    try:
        result = json.loads(arg)
        if not isinstance(result, dict):
            raise ValueError("JSON must represent an object")
        return result
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"Invalid JSON: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CLI SQLite database manager (uses db_manager_agent)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as machine‑readable JSON",
    )
    
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list‑dbs
    subparsers.add_parser("list-dbs", help="List discovered *.db files")

    # list‑tables
    lt_parser = subparsers.add_parser("list-tables", help="List tables in a database")
    lt_parser.add_argument("db_path", help="Path to a .db file (or name; will be resolved)")

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch rows from a table")
    fetch_parser.add_argument("db_path", help="Path to a .db file")
    fetch_parser.add_argument("table", help="Table name")
    fetch_parser.add_argument(
        "--columns",
        default="*",
        help="Comma‑separated column list (default: *)",
    )
    fetch_parser.add_argument(
        "--where",
        type=parse_json,
        help="JSON object for WHERE clause, e.g. '{\"id\": 1}'",
    )
    fetch_parser.add_argument(
        "--order-by", help="Column name to sort by"
    )
    fetch_parser.add_argument(
        "--desc", action="store_true", help="Sort descending"
    )
    fetch_parser.add_argument(
        "--limit", type=int, help="Maximum number of rows to return"
    )

    # insert
    insert_parser = subparsers.add_parser("insert", help="Insert a row")
    insert_parser.add_argument("db_path", help="Path to a .db file")
    insert_parser.add_argument("table", help="Table name")
    insert_parser.add_argument(
        "--data",
        required=True,
        type=parse_json,
        help="JSON dict of column/value pairs, e.g. '{\"name\":\"Bob\",\"age\":30}'",
    )

    # update
    update_parser = subparsers.add_parser("update", help="Update rows")
    update_parser.add_argument("db_path", help="Path to a .db file")
    update_parser.add_argument("table", help="Table name")
    update_parser.add_argument(
        "--data",
        required=True,
        type=parse_json,
        help="JSON dict of new values",
    )
    update_parser.add_argument(
        "--where",
        required=True,
        type=parse_json,
        help="JSON dict for WHERE clause",
    )

    # delete
    delete_parser = subparsers.add_parser("delete", help="Delete rows")
    delete_parser.add_argument("db_path", help="Path to a .db file")
    delete_parser.add_argument("table", help="Table name")
    delete_parser.add_argument(
        "--where",
        required=True,
        type=parse_json,
        help="JSON dict for WHERE clause",
    )

    args = parser.parse_args()
    mgr = SQLiteManager()

    if args.command == "list-dbs":
        dbs = mgr.list_databases()
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": dbs,
                "error": None,
            }
            print(json.dumps(result))
        else:
            for db in dbs:
                print(db)

    elif args.command == "list-tables":
        mgr.connect(args.db_path)
        tables = mgr.list_tables()
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": tables,
                "error": None,
            }
            print(json.dumps(result))
        else:
            for t in tables:
                print(t)
        mgr.close()

    elif args.command == "fetch":
        mgr.connect(args.db_path)
        rows = mgr.fetch(
            table=args.table,
            columns=args.columns,
            where=args.where,
            order_by=args.order_by,
            descending=args.desc,
            limit=args.limit,
        )
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": rows,
                "error": None,
            }
            print(json.dumps(result))
        else:
            for row in rows:
                print(row)
        mgr.close()

    elif args.command == "insert":
        mgr.connect(args.db_path)
        rowid = mgr.insert(table=args.table, data=args.data)
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": {"rowid": rowid},
                "error": None,
            }
            print(json.dumps(result))
        else:
            print(f"Inserted rowid: {rowid}")
        mgr.close()

    elif args.command == "update":
        mgr.connect(args.db_path)
        count = mgr.update(table=args.table, data=args.data, where=args.where)
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": {"rows_updated": count},
                "error": None,
            }
            print(json.dumps(result))
        else:
            print(f"Rows updated: {count}")
        mgr.close()

    elif args.command == "delete":
        mgr.connect(args.db_path)
        count = mgr.delete(table=args.table, where=args.where)
        if args.json:
            result = {
                "task_id": str(uuid.uuid4()),
                "status": "success",
                "data": {"rows_deleted": count},
                "error": None,
            }
            print(json.dumps(result))
        else:
            print(f"Rows deleted: {count}")
        mgr.close()

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
