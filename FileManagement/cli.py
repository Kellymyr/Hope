#!/usr/bin/env python3
"""
Command‑line interface for the native‑PC file‑management utilities.

Usage examples:
  python -m FileManagement.cli rename old.txt new.txt
  python -m FileManagement.cli copy src.txt dst.txt
  python -m FileManagement.cli cut src.txt dst.txt
  python -m FileManagement.cli mkdir new_folder
  python -m FileManagement.cli mkfile new_file.txt "Initial content"
"""

import argparse
import sys
from pathlib import Path
from .ollama_client import run_prompt
from .action_executor import execute_actions
# Ensure project root is on sys.path for relative imports
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

from .file_ops import rename, copy, cut, make_folder, make_file


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FileManagement utilities")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # llm
    p_llm = subparsers.add_parser(
        "llm",
        help="Execute LLM‑driven file operations from a natural‑language prompt",
    )
    p_llm.add_argument("prompt", nargs="+", help="Prompt describing the desired file operations")

    # rename
    p_rename = subparsers.add_parser("rename", help="Rename or move a file/folder")
    p_rename.add_argument("src", help="Source path")
    p_rename.add_argument("dst", help="Destination path")
    p_rename.add_argument(
        "--overwrite", action="store_true", help="Allow overwriting destination"
    )

    # copy
    p_copy = subparsers.add_parser("copy", help="Copy a file/folder")
    p_copy.add_argument("src", help="Source path")
    p_copy.add_argument("dst", help="Destination path")
    p_copy.add_argument(
        "--overwrite", action="store_true", help="Allow overwriting destination"
    )

    # cut
    p_cut = subparsers.add_parser("cut", help="Cut (move) a file/folder")
    p_cut.add_argument("src", help="Source path")
    p_cut.add_argument("dst", help="Destination path")
    p_cut.add_argument(
        "--overwrite", action="store_true", help="Allow overwriting destination"
    )

    # mkdir
    p_mkdir = subparsers.add_parser("mkdir", help="Create a new folder")
    p_mkdir.add_argument("path", help="Folder path to create")

    # mkfile
    p_mkfile = subparsers.add_parser("mkfile", help="Create a new file with optional content")
    p_mkfile.add_argument("path", help="File path to create")
    p_mkfile.add_argument(
        "content", nargs="?", default="", help="Initial content for the new file"
    )

    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    try:
        if args.command == "rename":
            rename(args.src, args.dst, overwrite=args.overwrite)
        elif args.command == "copy":
            copy(args.src, args.dst, overwrite=args.overwrite)
        elif args.command == "cut":
            cut(args.src, args.dst, overwrite=args.overwrite)
        elif args.command == "mkdir":
            make_folder(args.path)
        elif args.command == "mkfile":
            make_file(args.path, content=args.content)
        elif args.command == "llm":
            # combine the prompt parts into a single string
            prompt_str = " ".join(args.prompt)
            response = run_prompt(prompt_str)
            execute_actions(response)
        else:
            raise ValueError(f"Unknown command: {args.command}")
        print(f"[OK] {args.command} executed successfully.")
    except Exception as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
