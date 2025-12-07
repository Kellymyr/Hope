#!/usr/bin/env python3
"""
SQLite Database Manager – Agent Friendly Interface

This module provides a lightweight, pure‑standard‑library API for managing
SQLite databases.  It is purposely designed to be easy to call from an
Autogen agent (or any other automation tool) while still offering a useful
command‑line interface for manual use.

Features
--------
* Auto‑detect ``*.db`` files in a directory (default: ``./Databases``)
* Connect to a chosen database and keep the connection alive
* List tables, fetch rows, insert, update and delete records
* Simple “find” helper (WHERE column = value)
* Sorting support (ORDER BY column ASC/DESC)
* Toggle between databases (close current connection and open another)
* All operations use the built‑in ``sqlite3`` module with WAL mode
  and a 30 s busy timeout for safe concurrent access.

Typical usage from an Autogen agent
---------------------------------
>>> from db_manager_agent import SQLiteManager
>>> mgr = SQLiteManager()
>>> dbs = mgr.list_databases()
>>> mgr.connect(dbs[0])
>>> tables = mgr.list_tables()
>>> rows = mgr.fetch(table=tables[0], limit=10, order_by="id")
>>> mgr.insert(table="users", data={"name": "Alice", "age": 30})
>>> mgr.close()
"""

import os
import argparse
import json
import sqlite3
from typing import List, Tuple, Any, Dict, Optional
import threading
import queue
import uuid
import time

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def discover_databases(db_dir: str) -> List[str]:
    """Return absolute paths of all ``*.db`` files inside *db_dir*."""
    if not os.path.isdir(db_dir):
        return []
    return [
        os.path.abspath(os.path.join(db_dir, f))
        for f in os.listdir(db_dir)
        if f.lower().endswith(".db")
    ]


def _apply_wal_and_timeout(conn: sqlite3.Connection) -> None:
    """Enable WAL mode and a 30 s busy timeout on *conn*."""
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except sqlite3.Error as e:
        # Non‑critical – proceed anyway
        print(f"[WARN] Could not set WAL/timeout: {e}")


# ----------------------------------------------------------------------
# Core manager class
# ----------------------------------------------------------------------
class SQLiteManager:
    """
    Agent‑friendly SQLite manager.

    The manager holds a single active connection.  Methods raise
    ``sqlite3.Error`` on failure, which can be caught by the caller
    (e.g. an Autogen tool).
    """

    def __init__(self, db_dir: str = "Databases"):
        self.db_dir = db_dir
        self.conn: Optional[sqlite3.Connection] = None
        self.db_path: Optional[str] = None

    # ------------------------------------------------------------------
    # Database discovery / connection handling
    # ------------------------------------------------------------------
    def list_databases(self) -> List[str]:
        """Return a list of absolute paths to ``*.db`` files in *self.db_dir*."""
        return discover_databases(self.db_dir)

    def connect(self, db_path: str) -> None:
        """Open *db_path* and keep the connection alive."""
        if self.conn:
            self.close()
        self.conn = sqlite3.connect(db_path, timeout=30.0)
        _apply_wal_and_timeout(self.conn)
        self.db_path = db_path

    def close(self) -> None:
        """Close the current connection (if any)."""
        if self.conn:
            self.conn.close()
        self.conn = None
        self.db_path = None

    def toggle_database(self, new_db_path: str) -> None:
        """
        Switch to another database file.  Equivalent to ``close()`` + ``connect()``.
        """
        self.connect(new_db_path)

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------
    def _ensure_connection(self) -> sqlite3.Connection:
        if not self.conn:
            raise sqlite3.Error("No active database connection.")
        return self.conn

    def list_tables(self) -> List[str]:
        """Return a list of table names in the connected database."""
        conn = self._ensure_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
        )
        return [row[0] for row in cur.fetchall()]

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------
    def fetch(
        self,
        table: str,
        columns: str = "*",
        where: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
        limit: Optional[int] = None,
    ) -> List[Tuple]:
        """
        Retrieve rows from *table*.

        Parameters
        ----------
        table: str
            Table name.
        columns: str
            Comma‑separated column list or ``*`` (default).
        where: dict | None
            Mapping column → value for an ``AND``‑combined ``WHERE`` clause.
        order_by: str | None
            Column name to sort by.
        descending: bool
            If ``True``, sort ``DESC``; otherwise ``ASC``.
        limit: int | None
            Maximum number of rows to return.

        Returns
        -------
        List[Tuple]
            Rows as dictionaries (column‑name → value) for easier GUI consumption.
        """
        conn = self._ensure_connection()
        # Use Row factory to get dict‑like rows
        conn.row_factory = sqlite3.Row
        query = f"SELECT {columns} FROM {table}"
        params: List[Any] = []

        if where:
            clauses = [f"{col}=?" for col in where]
            query += " WHERE " + " AND ".join(clauses)
            params.extend(where.values())

        if order_by:
            query += f" ORDER BY {order_by} {'DESC' if descending else 'ASC'}"

        if limit is not None:
            query += f" LIMIT {limit}"

        cur = conn.cursor()
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        # Convert sqlite3.Row objects to plain dicts
        return [dict(row) for row in rows]

    def find(
        self,
        table: str,
        column: str,
        value: Any,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
        descending: bool = False,
    ) -> List[Tuple]:
        """
        Shortcut for ``fetch`` with a single ``WHERE column = value`` clause.
        """
        return self.fetch(
            table=table,
            where={column: value},
            limit=limit,
            order_by=order_by,
            descending=descending,
        )

    def insert(self, table: str, data: Dict[str, Any]) -> int:
        """
        Insert a row into *table*.

        Returns the ``rowid`` of the inserted row.
        """
        conn = self._ensure_connection()
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cur = conn.cursor()
        cur.execute(sql, tuple(data.values()))
        conn.commit()
        return cur.lastrowid

    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any],
    ) -> int:
        """
        Update rows in *table*.

        Returns the number of rows modified.
        """
        conn = self._ensure_connection()
        set_clause = ", ".join(f"{col}=?" for col in data)
        where_clause = " AND ".join(f"{col}=?" for col in where)
        sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(where.values())
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        conn.commit()
        return cur.rowcount

    def delete(self, table: str, where: Dict[str, Any]) -> int:
        """
        Delete rows from *table*.

        Returns the number of rows removed.
        """
        conn = self._ensure_connection()
        where_clause = " AND ".join(f"{col}=?" for col in where)
        sql = f"DELETE FROM {table} WHERE {where_clause}"
        cur = conn.cursor()
        cur.execute(sql, tuple(where.values()))
        conn.commit()
        return cur.rowcount

    # ------------------------------------------------------------------
    # Convenience CLI wrappers (used by the command‑line interface below)
    # ------------------------------------------------------------------
    # The CLI methods simply forward to the instance methods; they are kept
    # separate so that they can be called without instantiating a manager
    # when the module is executed as a script.

# ----------------------------------------------------------------------
# Command‑line interface

class SQLiteTaskPool:
    """
    Background task pool for executing SQLite operations in parallel.
    Uses standard‑library threading, queue and uuid. Each worker thread
    processes tasks from a Queue, ensuring that no two workers operate on
    the same database file simultaneously (db_locks).
    """

    def __init__(self, db_dir: str = "Databases"):
        # Discover databases and decide worker count
        self.db_dir = db_dir
        self.databases = discover_databases(db_dir)

        if len(self.databases) > 20:
            self.num_workers = 5
        else:
            self.num_workers = max(1, len(self.databases) // 4)

        # Task management structures
        self.task_queue: queue.Queue = queue.Queue()
        self.status: Dict[str, Dict] = {}  # task_id → metadata
        self.db_locks: Dict[str, threading.Lock] = {
            db_path: threading.Lock() for db_path in self.databases
        }
        self.workers: List[threading.Thread] = []
        self._shutdown = threading.Event()

        # Start daemon worker threads
        for _ in range(self.num_workers):
            t = threading.Thread(target=self._worker, daemon=True)
            t.start()
            self.workers.append(t)

    def _worker(self):
        """Continuously process tasks from the queue."""
        while not self._shutdown.is_set():
            try:
                task_id, db_path, op_name, kwargs = self.task_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            # Mark as running
            self.status[task_id]["status"] = "running"
            self.status[task_id]["started_at"] = time.time()

            # Ensure exclusive access to the specific DB
            lock = self.db_locks.setdefault(db_path, threading.Lock())
            with lock:
                mgr = SQLiteManager(db_dir=self.db_dir)
                mgr.connect(db_path)
                try:
                    method = getattr(mgr, op_name)
                    result = method(**kwargs)
                    self.status[task_id].update({
                        "status": "completed",
                        "result": result,
                        "error": None,
                    })
                except Exception as exc:
                    self.status[task_id].update({
                        "status": "error",
                        "error": str(exc),
                        "result": None,
                    })
                finally:
                    mgr.close()

            self.status[task_id]["finished_at"] = time.time()
            self.task_queue.task_done()

    def submit(self, db_path: str, op: str, **kwargs) -> str:
        """
        Queue a database operation.

        Parameters
        ----------
        db_path : str
            Path to the SQLite database file.
        op : str
            One of ``fetch``, ``insert``, ``update``, ``delete``.
        **kwargs
            Arguments forwarded to the corresponding ``SQLiteManager`` method.

        Returns
        -------
        str
            UUID of the created task.
        """
        if op not in {"fetch", "insert", "update", "delete"}:
            raise ValueError(f"Unsupported operation: {op}")

        task_id = str(uuid.uuid4())
        self.status[task_id] = {
            "task_id": task_id,
            "db_path": db_path,
            "op": op,
            "status": "queued",
            "result": None,
            "error": None,
            "started_at": None,
            "finished_at": None,
            "args": kwargs,
        }
        self.task_queue.put((task_id, db_path, op, kwargs))
        return task_id

    def get_status(self, task_id: str) -> Dict:
        """
        Retrieve the status dictionary for a given task.
        Returns ``{\"status\": \"unknown\"}`` if the task_id is not found.
        """
        return self.status.get(task_id, {"status": "unknown"})

    def shutdown(self, wait: bool = False):
        """
        Signal workers to stop. If *wait* is True, joins all threads.
        """
        self._shutdown.set()
        if wait:
            for t in self.workers:
                t.join()
# ----------------------------------------------------------------------
def _parse_json_arg(arg: str) -> Dict:
    """Parse a JSON string into a dict (used for ``--data`` and ``--where``)."""
    try:
        result = json.loads(arg)
        if not isinstance(result, dict):
            raise ValueError("JSON must represent an object")
        return result
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"Invalid JSON: {e}")

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Agent‑friendly SQLite manager"
    )
    parser.add_argument(
        "--db-dir",
        default="Databases",
        help="Directory containing *.db files (default: %(default)s)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list‑databases
    subparsers.add_parser("list-dbs", help="List discovered *.db files")

    # connect + list‑tables
    subparsers.add_parser("list-tables", help="List tables in the first db")

    # fetch
    fetch_parser = subparsers.add_parser("fetch", help="Fetch rows")
    fetch_parser.add_argument("table", help="Table name")
    fetch_parser.add_argument(
        "--columns", default="*", help="Comma‑separated column list (default: *)"
    )
    fetch_parser.add_argument(
        "--where", type=_parse_json_arg, help="JSON object for WHERE clause"
    )
    fetch_parser.add_argument(
        "--order-by", help="Column name to sort by"
    )
    fetch_parser.add_argument(
        "--desc", action="store_true", help="Sort descending"
    )
    fetch_parser.add_argument(
        "--limit", type=int, help="Maximum rows to return"
    )

    # insert
    insert_parser = subparsers.add_parser("insert", help="Insert a row")
    insert_parser.add_argument("table", help="Table name")
    insert_parser.add_argument(
        "--data", required=True, type=_parse_json_arg, help="JSON dict of column/value"
    )

    # update
    update_parser = subparsers.add_parser("update", help="Update rows")
    update_parser.add_argument("table", help="Table name")
    update_parser.add_argument(
        "--data", required=True, type=_parse_json_arg, help="JSON dict of new values"
    )
    update_parser.add_argument(
        "--where", required=True, type=_parse_json_arg, help="JSON dict for WHERE"
    )

    # delete
    delete_parser = subparsers.add_parser("delete", help="Delete rows")
    delete_parser.add_argument("table", help="Table name")
    delete_parser.add_argument(
        "--where", required=True, type=_parse_json_arg, help="JSON dict for WHERE"
    )

    args = parser.parse_args()
    mgr = SQLiteManager(db_dir=args.db_dir)

    if args.command == "list-dbs":
        dbs = mgr.list_databases()
        for db in dbs:
            print(db)

    elif args.command == "list-tables":
        dbs = mgr.list_databases()
        if not dbs:
            print("[ERROR] No .db files found.")
            return
        mgr.connect(dbs[0])
        tables = mgr.list_tables()
        for tbl in tables:
            print(tbl)
        mgr.close()

    elif args.command == "fetch":
        dbs = mgr.list_databases()
        if not dbs:
            print("[ERROR] No .db files found.")
            return
        mgr.connect(dbs[0])
        rows = mgr.fetch(
            table=args.table,
            columns=args.columns,
            where=args.where,
            order_by=args.order_by,
            descending=args.desc,
            limit=args.limit,
        )
        for row in rows:
            print(row)
        mgr.close()

    elif args.command == "insert":
        dbs = mgr.list_databases()
        if not dbs:
            print("[ERROR] No .db files found.")
            return
        mgr.connect(dbs[0])
        rowid = mgr.insert(table=args.table, data=args.data)
        print(f"Inserted rowid: {rowid}")
        mgr.close()

    elif args.command == "update":
        dbs = mgr.list_databases()
        if not dbs:
            print("[ERROR] No .db files found.")
            return
        mgr.connect(dbs[0])
        count = mgr.update(table=args.table, data=args.data, where=args.where)
        print(f"Rows updated: {count}")
        mgr.close()

    elif args.command == "delete":
        dbs = mgr.list_databases()
        if not dbs:
            print("[ERROR] No .db files found.")
            return
        mgr.connect(dbs[0])
        count = mgr.delete(table=args.table, where=args.where)
        print(f"Rows deleted: {count}")
        mgr.close()

if __name__ == "__main__":
    main()
