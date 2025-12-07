#!/usr/bin/env python3
"""
Utility for synchronizing the central file index (file_index.db) with
filesystem operations performed in the FileManagement utilities.
"""

import os
import sys
from typing import Optional

# Adjust path to import the existing SQLite manager
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.append(_ROOT)

from DatabaseFiles.db_manager_agent import SQLiteManager

# Path to the central index database (located at the project root)
INDEX_DB_PATH = os.path.join(_ROOT, "file_index.db")


def _connect_manager() -> SQLiteManager:
    """Create a temporary SQLiteManager connected to the index DB."""
    mgr = SQLiteManager(db_dir=os.path.dirname(INDEX_DB_PATH))
    mgr.connect(INDEX_DB_PATH)
    return mgr


def add_to_index(path: str, is_folder: bool = False) -> None:
    """
    Insert a new entry for ``path`` into the index.

    Parameters
    ----------
    path : str
        Absolute or relative path to the file/folder.
    is_folder : bool, optional
        ``True`` if the path points to a directory.
    """
    mgr = _connect_manager()
    try:
        # Simple schema assumption: table ``files`` with columns ``path`` and ``is_folder``
        mgr.insert(
            table="files",
            data={"path": os.path.abspath(path), "is_folder": int(is_folder)},
        )
    finally:
        mgr.close()


def remove_from_index(path: str) -> None:
    """
    Delete the entry for ``path`` from the index if it exists.

    Parameters
    ----------
    path : str
        Path to the file/folder to remove.
    """
    mgr = _connect_manager()
    try:
        mgr.delete(table="files", where={"path": os.path.abspath(path)})
    finally:
        mgr.close()


def update_index_after_move(old_path: str, new_path: str) -> None:
    """
    Update the ``path`` column after a rename/move operation.

    Parameters
    ----------
    old_path : str
        Original location of the file/folder.
    new_path : str
        New location after the operation.
    """
    mgr = _connect_manager()
    try:
        mgr.update(
            table="files",
            data={"path": os.path.abspath(new_path)},
            where={"path": os.path.abspath(old_path)},
        )
    finally:
        mgr.close()
