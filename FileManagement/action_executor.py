#!/usr/bin/env python3
"""
Action executor for the LLM‑driven FileManagement system.

The LLM is expected to return a JSON array of action dictionaries, each
with an ``action`` key and the parameters required by the corresponding
function in ``file_ops``.  Example JSON:

[
    {"action": "make_folder", "path": "VSC"},
    {"action": "move", "src": "*vscode*", "dst": "VSC"},
    {"action": "make_file", "path": "VSC/.gitignore", "content": ""}
]

Supported actions:
- rename / move          -> rename(src, dst, overwrite)
- copy                    -> copy(src, dst, overwrite)
- cut                     -> cut(src, dst, overwrite)
- make_folder             -> make_folder(path)
- make_file               -> make_file(path, content)
- delete (optional)        -> not yet implemented
"""

import json
import glob
from typing import List, Dict, Any

from . import file_ops


# Map action names to the corresponding functions in file_ops
_ACTION_MAP = {
    "rename": file_ops.rename,
    "move": file_ops.rename,   # alias for rename
    "copy": file_ops.copy,
    "cut": file_ops.cut,
    "make_folder": file_ops.make_folder,
    "make_file": file_ops.make_file,
    # future: "delete": file_ops.delete,
}


def _expand_pattern(pattern: str) -> List[str]:
    """Expand a glob pattern relative to the current working directory."""
    return glob.glob(pattern, recursive=True)


def _execute_single(action: Dict[str, Any]) -> None:
    """Execute a single action dictionary."""
    name = action.get("action")
    if not name:
        raise ValueError(f"Action dictionary missing 'action' key: {action}")

    func = _ACTION_MAP.get(name)
    if not func:
        raise ValueError(f"Unsupported action '{name}'. Supported: {list(_ACTION_MAP)}")

    # Handle file‑operation actions
    if name in ("rename", "move", "copy", "cut"):
        src_pat = action["src"]
        dst = action["dst"]
        overwrite = bool(action.get("overwrite", False))

        src_paths = _expand_pattern(src_pat) if ("*" in src_pat or "?" in src_pat) else [src_pat]

        for src in src_paths:
            func(src, dst, overwrite=overwrite)

    elif name == "make_folder":
        func(action["path"])

    elif name == "make_file":
        func(action["path"], content=action.get("content", ""))

    else:
        # Should never reach here because of the earlier validation
        raise RuntimeError(f"Unhandled action '{name}'")


def execute_actions(json_payload: str) -> None:
    """
    Parse a JSON payload produced by the LLM and execute the actions.

    Parameters
    ----------
    json_payload : str
        JSON string representing a list of action dictionaries.
    """
    try:
        actions = json.loads(json_payload)
        if not isinstance(actions, list):
            raise ValueError("LLM response must be a JSON list of actions")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode LLM JSON output: {e}")

    for act in actions:
        _execute_single(act)
