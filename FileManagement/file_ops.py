#!/usr/bin/env python3
"""
File management utilities for native‑PC operations.

All functions perform the requested filesystem action and then keep the
central index (E:\\Hope4\\file_index.db) in sync via ``index_sync``.
"""

import os
import shutil
import sys
from typing import Optional

# Ensure the project root is on sys.path so we can import the index helper
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.append(_ROOT)

from .index_sync import add_to_index, remove_from_index, update_index_after_move


def _ensure_parent_dir(path: str) -> None:
    """Create parent directories if they do not exist."""
    parent = os.path.dirname(path)
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)


def rename(src: str, dst: str, overwrite: bool = False) -> None:
    """
    Rename (or move) a file or folder.

    Parameters
    ----------
    src : str
        Existing path.
    dst : str
        Desired new path.
    overwrite : bool, optional
        If ``True`` allow overwriting an existing destination.
    """
    if not os.path.exists(src):
        raise FileNotFoundError(f"Source does not exist: {src}")
    if os.path.exists(dst) and not overwrite:
        raise FileExistsError(f"Destination already exists: {dst}")

    _ensure_parent_dir(dst)
    os.rename(src, dst)
    update_index_after_move(src, dst)


def copy(src: str, dst: str, overwrite: bool = False) -> None:
    """
    Copy a file or folder to a new location.

    Parameters
    ----------
    src : str
        Path to the source file/folder.
    dst : str
        Destination path.
    overwrite : bool, optional
        If ``True`` allow overwriting an existing destination.
    """
    if not os.path.exists(src):
        raise FileNotFoundError(f"Source does not exist: {src}")
    if os.path.exists(dst) and not overwrite:
        raise FileExistsError(f"Destination already exists: {dst}")

    _ensure_parent_dir(dst)
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)

    # Register the new copy in the index
    add_to_index(dst, is_folder=os.path.isdir(dst))


def cut(src: str, dst: str, overwrite: bool = False) -> None:
    """
    Cut (move) a file or folder. This is a thin wrapper around ``rename``.

    Parameters
    ----------
    src : str
        Existing path.
    dst : str
        New location.
    overwrite : bool, optional
        If ``True`` allow overwriting the destination.
    """
    rename(src, dst, overwrite=overwrite)


def make_folder(path: str) -> None:
    """
    Create a new folder (including any missing parent directories).

    Parameters
    ----------
    path : str
        Directory path to create.
    """
    os.makedirs(path, exist_ok=True)
    add_to_index(path, is_folder=True)


def make_file(path: str, content: str = "") -> None:
    """
    Create a new empty file (or overwrite if it already exists) and register it.

    Parameters
    ----------
    path : str
        Full path of the file to create.
    content : str, optional
        Initial text content to write into the file.
    """
    _ensure_parent_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    add_to_index(path, is_folder=False)
