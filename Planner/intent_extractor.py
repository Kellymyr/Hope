"""
Intent extraction utility for the Planner module.

Uses the configured Ollama LLM (via LangChain) to classify a user
request into a structured intent dictionary.

Expected JSON schema:
{
    "intent": "<intent_name>",
    "parameters": {
        ... intent‑specific fields ...
    }
}
If the request does not match any known intent, the LLM should return:
{
    "intent": "generic",
    "parameters": {"text": "<original request>"}
}
"""

import json
import re
from .config import OLLAMA_BASE_URL, DEFAULT_MODEL
from langchain_community.llms import Ollama


def _get_llm() -> Ollama:
    """Create an Ollama LLM instance with the configured model."""
    # Use the newer LangChain Ollama class if available
    try:
        from langchain_ollama import OllamaLLM
        return OllamaLLM(model=DEFAULT_MODEL, base_url=OLLAMA_BASE_URL)
    except ImportError:
        return Ollama(model=DEFAULT_MODEL, base_url=OLLAMA_BASE_URL)


_SYSTEM_PROMPT = """You are an intent classifier for a personal planner assistant.
Given a user request, return a **single‑line JSON** object with two keys:
- "intent": the name of the detected intent.
- "parameters": an object with extracted parameters.

Recognized intents (provide all relevant parameters):
1. "make_folder"
   Required: "folder_name" (string)
   Optional: "location" (string, defaults to "desktop")
2. "list_files"
   Optional: "path" (string, defaults to current directory)
3. "read_file"
   Required: "path" (string) – path to the file to read
4. "write_file"
   Required: "path" (string) – target file path
   Required: "content" (string) – text content to write
5. "move_file"
   Required: "src" (string) – source path
   Required: "dest" (string) – destination path
6. "copy_file"
   Required: "src" (string) – source path
   Required: "dest" (string) – destination path

If the request does not correspond to a known intent, set "intent" to "generic"
and include the whole request in parameters under the key "text".

Examples:
User request: make folder on desktop named MyDocs
=> {"intent": "make_folder", "parameters": {"folder_name": "MyDocs", "location": "desktop"}}
User request: list all files in the directory /tmp
=> {"intent": "list_files", "parameters": {"path": "/tmp"}}
User request: read the contents of file notes.txt
=> {"intent": "read_file", "parameters": {"path": "notes.txt"}}
User request: create a file named hello.txt with the text Hello World
=> {"intent": "write_file", "parameters": {"path": "hello.txt", "content": "Hello World"}}
User request: move report.pdf to archive/report.pdf"
=> {"intent": "move_file", "parameters": {"src": "report.pdf", "dest": "archive/report.pdf"}}
User request: copy data.csv to backup/data_backup.csv
=> {"intent": "copy_file", "parameters": {"src": "data.csv", "dest": "backup/data_backup.csv"}}
"""

def extract_intent(task: str) -> dict:
    """Classify the task using the Ollama LLM, with regex fallback for folder creation.

    Attempts LLM call; if it fails, tries a quick regex for "make folder" intent.
    If regex matches, returns structured intent; otherwise returns generic.
    """
    llm = _get_llm()
    try:
        prompt = _SYSTEM_PROMPT + "\nUser request: " + task
        response = llm.invoke(prompt)
        response = response.strip()
        return json.loads(response)
    except Exception:
        # Regex fallback for make_folder intent
        # Regex fallback for known intents when the LLM is unavailable
        folder_match = re.search(
            r"make\s+folder\s+on\s+desktop\s+named\s+(\w+)",
            task,
            re.IGNORECASE,
        )
        if folder_match:
            return {
                "intent": "make_folder",
                "parameters": {
                    "folder_name": folder_match.group(1),
                    "location": "desktop",
                },
            }

        # List files intent
        list_match = re.search(
            r"list\s+files\s+(?:in|on)\s+([^\s]+)", task, re.IGNORECASE
        )
        if list_match:
            return {
                "intent": "list_files",
                "parameters": {"path": list_match.group(1)},
            }

        # Read file intent
        read_match = re.search(
            r"read\s+(?:the\s+)?contents?\s+of\s+file\s+([^\s]+)",
            task,
            re.IGNORECASE,
        )
        if read_match:
            return {
                "intent": "read_file",
                "parameters": {"path": read_match.group(1)},
            }

        # Write file intent (simple pattern)
        write_match = re.search(
            r"create\s+file\s+named\s+([^\s]+)\s+with\s+the\s+text\s+(.+)",
            task,
            re.IGNORECASE,
        )
        if write_match:
            return {
                "intent": "write_file",
                "parameters": {
                    "path": write_match.group(1),
                    "content": write_match.group(2).strip(),
                },
            }

        # Move file intent
        move_match = re.search(
            r"move\s+([^\s]+)\s+to\s+([^\s]+)", task, re.IGNORECASE
        )
        if move_match:
            return {
                "intent": "move_file",
                "parameters": {"src": move_match.group(1), "dest": move_match.group(2)},
            }

        # Copy file intent
        copy_match = re.search(
            r"copy\s+([^\s]+)\s+to\s+([^\s]+)", task, re.IGNORECASE
        )
        if copy_match:
            return {
                "intent": "copy_file",
                "parameters": {"src": copy_match.group(1), "dest": copy_match.group(2)},
            }

        # Fallback to generic intent
        return {"intent": "generic", "parameters": {"text": task}}
