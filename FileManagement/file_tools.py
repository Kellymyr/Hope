"""
File: file_tools.py
Description: Helper utilities for interacting with the existing file index SQLite database
using the CRUD helpers defined in `autogen_tools`. All functions are pure, perform no I/O,
and return JSON‑serializable data structures.
"""

# Standard imports
from typing import Any, Dict, List, Optional

# Import the generic DB helpers – DO NOT import sqlite3 directly
from autogen_tools import fetch, insert, update, delete  # noqa: F401  (insert/update/delete may be unused now)

# -------------------------------------------------------------------------
# Constants
# -------------------------------------------------------------------------

# Default absolute path to the file index database.
FILE_INDEX_DB = r"E:\\Hope1\\file_index.db"


# -------------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------------

def get_file_index_db() -> str:
    """
    Return the default path to the file index database.
    This function allows the path to be overridden later if needed.
    """
    return FILE_INDEX_DB


def _resolve_db_path(db_path: Optional[str]) -> str:
    """
    Internal helper to resolve the database path, falling back to the default.
    """
    return db_path or get_file_index_db()


# -------------------------------------------------------------------------
# Public API
# -------------------------------------------------------------------------

def search_files(
    name_contains: Optional[str] = None,
    ext: Optional[str] = None,
    dir_contains: Optional[str] = None,
    limit: int = 100,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Search for files in the index.

    Parameters
    ----------
    name_contains : str | None
        Sub‑string that should appear in the file name (currently not used in the WHERE clause).
    ext : str | None
        File extension filter (e.g., "txt", "pdf").
    dir_contains : str | None
        Sub‑string that should appear in the directory path (currently not used in the WHERE clause).
    limit : int
        Maximum number of rows to return.
    db_path : str | None
        Override the database location; defaults to the constant.

    Returns
    -------
    dict
        {
            "db_path": <resolved path>,
            "criteria": {
                "name_contains": <value>,
                "ext": <value>,
                "dir_contains": <value>,
                "limit": <value>,
            },
            "rows": <list of rows from `fetch`>,
        }
    """
    resolved_path = _resolve_db_path(db_path)

    # Build a simple WHERE clause.
    where: Dict[str, Any] = {}
    if ext:
        where["ext"] = ext
    # NOTE: name_contains and dir_contains are intentionally omitted for now,
    # as per the specification – they can be added later by a planner.

    rows = fetch(
        db_path=resolved_path,
        table="files",
        columns="*",
        where=where if where else None,
        limit=limit,
    )

    return {
        "db_path": resolved_path,
        "criteria": {
            "name_contains": name_contains,
            "ext": ext,
            "dir_contains": dir_contains,
            "limit": limit,
        },
        "rows": rows,
    }


def get_file_by_path(
    path: str,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Retrieve a single file record identified by its absolute path.

    Parameters
    ----------
    path : str
        Absolute file path to look up.
    db_path : str | None
        Optional override for the SQLite file.

    Returns
    -------
    dict
        {
            "db_path": <resolved path>,
            "path": <input path>,
            "row": <first matching row or None>,
        }
    """
    resolved_path = _resolve_db_path(db_path)

    rows = fetch(
        db_path=resolved_path,
        table="files",
        columns="*",
        where={"path": path},
        limit=1,
    )

    # `fetch` returns a list; we want the first element or None.
    row = rows[0] if rows else None

    return {
        "db_path": resolved_path,
        "path": path,
        "row": row,
    }


def list_recent_files(
    limit: int = 50,
    db_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List the most recently modified files according to the `mtime` column.

    Parameters
    ----------
    limit : int
        Maximum number of recent files to return.
    db_path : str | None
        Optional override for the SQLite file.

    Returns
    -------
    dict
        {
            "db_path": <resolved path>,
            "limit": <limit>,
            "rows": <list of rows ordered by mtime descending>,
        }
    """
    resolved_path = _resolve_db_path(db_path)

    rows = fetch(
        db_path=resolved_path,
        table="files",
        columns="*",
        where=None,
        order_by="mtime",
        descending=True,
        limit=limit,
    )

    return {
        "db_path": resolved_path,
        "limit": limit,
        "rows": rows,
    }
