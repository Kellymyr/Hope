#!/usr/bin/env python3
"""
Utility wrappers for Autogen agents to interact with SQLite databases.

All functions use the :class:`SQLiteManager` from ``db_manager_agent.py`` and
operate on the ``./Databases`` directory (relative to the project root).
The manager is instantiated lazily and closed after each operation to keep
connections short‑lived and safe for concurrent use.
"""

from typing import List, Dict, Any, Optional

import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "DatabaseFiles"))
from db_manager_agent import SQLiteManager, SQLiteTaskPool

# A single manager instance that will be (re)connected as needed.
_mgr = SQLiteManager()
_pool = SQLiteTaskPool()


def list_databases() -> List[str]:
    """Return absolute paths of all discovered ``*.db`` files."""
    return _mgr.list_databases()


def list_tables(db_path: str) -> List[str]:
    """
    List tables in the specified database.

    Parameters
    ----------
    db_path: str
        Absolute or relative path to a ``.db`` file.

    Returns
    -------
    List[str]
        Table names in the database.
    """
    _mgr.connect(db_path)
    try:
        return _mgr.list_tables()
    finally:
        _mgr.close()


def fetch(
    db_path: str,
    table: str,
    columns: str = "*",
    where: Optional[Dict[str, Any]] = None,
    order_by: Optional[str] = None,
    descending: bool = False,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Retrieve rows from a table.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.
    table : str
        Table name.
    columns : str, optional
        Comma‑separated column list (default ``*``).
    where : dict, optional
        Mapping of column → value for an ``AND``‑combined ``WHERE`` clause.
    order_by : str, optional
        Column to sort by.
    descending : bool, optional
        Sort direction (``True`` for DESC).
    limit : int, optional
        Maximum number of rows to return.

    Returns
    -------
    List[Dict[str, Any]]
        Rows as dictionaries (column‑name → value) for easy consumption.
    """
    _mgr.connect(db_path)
    try:
        return _mgr.fetch(
            table=table,
            columns=columns,
            where=where,
            order_by=order_by,
            descending=descending,
            limit=limit,
        )
    finally:
        _mgr.close()


def insert(db_path: str, table: str, data: Dict[str, Any]) -> int:
    """
    Insert a new row.

    Returns the ``rowid`` of the inserted row.
    """
    _mgr.connect(db_path)
    try:
        return _mgr.insert(table, data)
    finally:
        _mgr.close()


def update(
    db_path: str,
    table: str,
    data: Dict[str, Any],
    where: Dict[str, Any],
) -> int:
    """
    Update existing rows.

    Returns the number of rows modified.
    """
    _mgr.connect(db_path)
    try:
        return _mgr.update(table, data, where)
    finally:
        _mgr.close()


def delete(db_path: str, table: str, where: Dict[str, Any]) -> int:
    """
    Delete rows from a table.

    Returns the number of rows removed.
    """
    _mgr.connect(db_path)
    try:
        return _mgr.delete(table, where)
    finally:
        _mgr.close()

def queue_task(
    db_path: str,
    table: str,
    op: str,
    data: Optional[Dict] = None,
    where: Optional[Dict] = None,
    columns: str = "*",
    order_by: Optional[str] = None,
    descending: bool = False,
    limit: Optional[int] = None,
) -> Dict:
    """
    Submit a non‑blocking database operation to the background pool.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.
    table : str
        Table name to operate on.
    op : str
        One of ``fetch``, ``insert``, ``update``, ``delete``.
    data : dict, optional
        Row data for ``insert`` or ``update`` operations.
    where : dict, optional
        Conditions for ``fetch``, ``update``, or ``delete``.
    columns : str, optional
        Column list for ``fetch`` (default ``*``).
    order_by : str, optional
        Column name for ordering results in ``fetch``.
    descending : bool, optional
        Use descending order when ``order_by`` is supplied.
    limit : int, optional
        Maximum number of rows for ``fetch``.

    Returns
    -------
    dict
        JSON‑serializable dict containing the generated ``task_id`` and initial status.
    """
    # Build kwargs for the specific operation
    kwargs: Dict = {"table": table}
    if op == "fetch":
        kwargs.update(
            {
                "columns": columns,
                "where": where,
                "order_by": order_by,
                "descending": descending,
                "limit": limit,
            }
        )
    elif op == "insert":
        kwargs["data"] = data or {}
    elif op == "update":
        kwargs.update({"data": data or {}, "where": where or {}})
    elif op == "delete":
        kwargs["where"] = where or {}
    else:
        raise ValueError(f"Unsupported operation: {op}")

    task_id = _pool.submit(db_path=db_path, op=op, **kwargs)
    return {"task_id": task_id, "status": "queued", "db_path": db_path, "op": op}


def task_status(task_id: str) -> Dict:
    """
    Retrieve the current status of a background task.

    Parameters
    ----------
    task_id : str
        The UUID of the task returned by ``queue_task``.

    Returns
    -------
    dict
        The status dictionary stored in the pool, or ``{\"status\": \"unknown\"}`` if not found.
    """
    return _pool.get_status(task_id)
