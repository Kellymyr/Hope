#!/usr/bin/env python3
"""
FileManagement.agent_wrapper

Thin, JSON‑serializable wrapper around the low‑level file operations defined in
`file_ops.py`.  The wrapper is intended to be imported and used by another
Autogen agent, so every public method returns a plain‑dict that can be safely
encoded to JSON.

Typical usage:

    from FileManagement.agent_wrapper import FileManagerAgent
    agent = FileManagerAgent()
    result = agent.rename("old.txt", "new.txt", overwrite=True)
    # result == {"ok": True, "message": "..."} or {"ok": False, "error": "..."}
"""

import os
from typing import Dict, Optional

# Ensure the project root is on sys.path so imports resolve correctly
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

# Import the concrete file‑system helpers
from .file_ops import (
    rename as _rename,
    copy as _copy,
    cut as _cut,
    make_folder as _make_folder,
    make_file as _make_file,
)

class FileManagerAgent:
    """
    Wrapper exposing the core file‑management actions in a JSON‑friendly way.
    All methods catch exceptions and return a dict with an ``ok`` flag and a
    human‑readable message or error description.
    """

    def __init__(self, root_dir: Optional[str] = None):
        """
        Parameters
        ----------
        root_dir : str | None
            Optional base directory.  If supplied, all relative paths given to the
            methods will be resolved against this directory.  If ``None`` the
            current working directory is used.
        """
        self.root_dir = Path(root_dir).resolve() if root_dir else Path.cwd()

    def _resolve(self, path: str) -> str:
        # Convert a possibly‑relative path to an absolute one using the root_dir
        p = Path(path)
        if not p.is_absolute():
            p = self.root_dir / p
        return str(p.resolve())

    def rename(self, src: str, dst: str, overwrite: bool = False) -> Dict:
        """Rename / move a file or folder."""
        try:
            _rename(self._resolve(src), self._resolve(dst), overwrite=overwrite)
            return {"ok": True, "message": f"Renamed {src} -> {dst}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def copy(self, src: str, dst: str, overwrite: bool = False) -> Dict:
        """Copy a file or folder."""
        try:
            _copy(self._resolve(src), self._resolve(dst), overwrite=overwrite)
            return {"ok": True, "message": f"Copied {src} -> {dst}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def cut(self, src: str, dst: str, overwrite: bool = False) -> Dict:
        """Move a file or folder (thin wrapper around rename)."""
        try:
            _cut(self._resolve(src), self._resolve(dst), overwrite=overwrite)
            return {"ok": True, "message": f"Cut {src} -> {dst}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def make_folder(self, path: str) -> Dict:
        """Create a new directory (including parents)."""
        try:
            _make_folder(self._resolve(path))
            return {"ok": True, "message": f"Folder created: {path}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def make_file(self, path: str, content: str = "") -> Dict:
        """Create a new file with optional initial content."""
        try:
            _make_file(self._resolve(path), content=content)
            return {"ok": True, "message": f"File created: {path}"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

def get_default_file_manager_agent() -> FileManagerAgent:
    """
    Convenience function used by other modules (including the Autogen
    orchestration layer) to obtain a ready‑to‑use agent bound to the default
    working directory.
    """
    return FileManagerAgent()
