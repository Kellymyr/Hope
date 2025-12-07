"""
File: file_agent.py
Description: Agent wrapper around the file index utilities defined in `file_tools.py`.
All functions are pure, perform no I/O, and return JSON‑serializable data.
"""

from typing import Any, Dict, List, Optional
import os, sys
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.append(_ROOT)
from . import file_tools
from .agent_wrapper import get_default_file_manager_agent as get_default_file_manager_agent
from .agent_wrapper import FileManagerAgent


class FileAgent:
    """
    Agent wrapper around the file index database.

    This agent does **NOT** crawl or modify the filesystem itself.
    It assumes an existing SQLite index database (e.g. `E:\\Hope1\\file_index.db`)
    with a `files` table already populated.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Parameters
        ----------
        db_path : str | None
            Optional override for the file index DB.
            If None, uses `file_tools.get_file_index_db()`.
        """
        self.db_path = db_path or file_tools.get_file_index_db()

    def find_by_ext(self, ext: str, limit: int = 100) -> Dict[str, Any]:
        """
        Find files by file extension.

        Returns a dict containing the original query parameters, the DB path,
        and the matching rows.
        """
        result = file_tools.search_files(ext=ext, limit=limit, db_path=self.db_path)
        return {
            "query": {"ext": ext, "limit": limit},
            "db_path": self.db_path,
            "rows": result["rows"],
        }

    def find_recent(self, limit: int = 50) -> Dict[str, Any]:
        """
        Return the most recent files ordered by modification time.
        """
        result = file_tools.list_recent_files(limit=limit, db_path=self.db_path)
        return result

    def lookup_path(self, path: str) -> Dict[str, Any]:
        """
        Look up a single file record by its absolute path.
        """
        result = file_tools.get_file_by_path(path=path, db_path=self.db_path)
        return result


def get_default_file_agent() -> FileAgent:
    """
    Convenience function to obtain a `FileAgent` bound to the default index DB.
    """
    return FileAgent()

# Export the file‑manager wrapper for Autogen agents
from .agent_wrapper import get_default_file_manager_agent

def get_default_file_manager() -> FileManagerAgent:
    """
    Convenience accessor returning the JSON‑serializable FileManagerAgent.
    This can be imported by other Autogen agents that need direct file‑system
    operations via the wrapper.
    """
    return get_default_file_manager_agent()
