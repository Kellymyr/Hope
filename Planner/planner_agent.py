"""
Planner Agent

This module provides a lightweight planning agent that uses:
- Ollama LLM via LangChain (`langchain_community.llms.Ollama`)
- AutoGen's `AssistantAgent` wrapper (for future extensibility)

Public entry point: `run_planner(task: str) -> str`
"""

from autogen import AssistantAgent
from langchain_community.llms import Ollama

from .config import OLLAMA_BASE_URL, DEFAULT_MODEL
import os
import re
import subprocess
from .intent_extractor import extract_intent


def _get_llm() -> Ollama:
    """Create an Ollama LLM instance with configured model and base URL."""
    return Ollama(model=DEFAULT_MODEL, base_url=OLLAMA_BASE_URL)


# Simple prompt template – can be expanded later
# Prompt template removed – we pass the task directly to the LLM.


def _invoke_llm(task: str) -> str:
    """Send the task prompt to the Ollama LLM and return the generated text.
    If the LLM call fails (e.g., missing model), returns a placeholder string."""
    llm = _get_llm()
    try:
        return llm.invoke(task)
    except Exception:
        return "LLM unavailable - placeholder response."


def _run_system_command(cmd: str) -> str:
    """Execute a shell command and return its output or error."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, check=True
        )
        return result.stdout.strip() or "Command executed successfully."
    except subprocess.CalledProcessError as e:
        return f"Error: {e.stderr.strip()}"


def run_planner(task: str) -> str:
    """
    Execute a planning request using the LLM or perform file‑management actions.

    Parameters
    ----------
    task : str
        Natural‑language request, e.g.:
        - "make folder on desktop named Reports"
        - "list files in C:\\temp"
        - "read the contents of notes.txt"
        - "create a file named hello.txt with the text Hello World"
        - "move report.pdf to archive/report.pdf"
        - "copy data.csv to backup/data_backup.csv"

    Returns
    -------
    str
        Result of the requested operation or the LLM response for unknown intents.
    """
    # Extract intent via the LLM (or fallback)
    intent_data = extract_intent(task)
    intent = intent_data.get("intent")
    params = intent_data.get("parameters", {})

    # ---- Folder creation (already supported) ----
    if intent == "make_folder":
        folder_name = params.get("folder_name")
        if folder_name:
            # Resolve target location (default to desktop)
            location = params.get("location", "desktop").lower()
            if location == "desktop":
                desktop_root = os.path.expanduser("~/Desktop")
                target_path = os.path.join(desktop_root, folder_name)
            else:
                if os.path.isabs(location):
                    target_path = os.path.join(location, folder_name)
                else:
                    target_path = os.path.join(os.getcwd(), location, folder_name)
            # If folder already exists, treat as successful
            if os.path.isdir(target_path):
                return "Command executed successfully."
            cmd = f'mkdir "{target_path}"'
            return _run_system_command(cmd)

    # ---- List files ----
    if intent == "list_files":
        path = params.get("path", ".")
        try:
            files = os.listdir(path)
            return f"Files in {path}:\\n" + "\\n".join(files)
        except Exception as e:
            return f"Error listing files: {e}"

    # ---- Read file ----
    if intent == "read_file":
        path = params.get("path")
        if not path:
            return "Error: No file path provided."
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            return f"Error reading file: {e}"

    # ---- Write file ----
    if intent == "write_file":
        path = params.get("path")
        content = params.get("content", "")
        if not path:
            return "Error: No file path provided."
        try:
            # Use the file operation helper to ensure indexing
            from FileManagement.file_ops import make_file
            make_file(path, content)
            return f"File '{path}' created with provided content."
        except Exception as e:
            return f"Error creating file: {e}"

    # ---- Move (cut) file ----
    if intent == "move_file":
        src = params.get("src")
        dest = params.get("dest")
        if not src or not dest:
            return "Error: Source and destination paths required."
        try:
            from FileManagement.file_ops import cut
            cut(src, dest, overwrite=True)
            return f"Moved '{src}' to '{dest}'."
        except Exception as e:
            return f"Error moving file: {e}"

    # ---- Copy file ----
    if intent == "copy_file":
        src = params.get("src")
        dest = params.get("dest")
        if not src or not dest:
            return "Error: Source and destination paths required."
        try:
            from FileManagement.file_ops import copy
            copy(src, dest, overwrite=True)
            return f"Copied '{src}' to '{dest}'."
        except Exception as e:
            return f"Error copying file: {e}"

    # ---- Fallback: unknown intent, use LLM or generic response ----
    try:
        # Attempt to let the LLM answer generic requests
        return _invoke_llm(task)
    except Exception:
        # As a last resort, return a friendly placeholder
        return "Unable to process the request."
