#!/usr/bin/env python3
"""
Ollama client wrapper for sending prompts to a local Ollama server.

The default endpoint is http://localhost:11434 and the default model
used is ``llama3``.  Both can be overridden with environment
variables ``OLLAMA_ENDPOINT`` and ``OLLAMA_MODEL``.
"""

import os
import json
import urllib.request
from urllib.error import URLError, HTTPError

DEFAULT_ENDPOINT = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "llama3")


def _post_json(url: str, payload: dict) -> dict:
    """Helper to POST a JSON payload and return the parsed JSON response."""
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as resp:
            resp_data = resp.read().decode("utf-8")
            return json.loads(resp_data)
    except (URLError, HTTPError) as e:
        raise RuntimeError(f"Failed to communicate with Ollama at {url}: {e}")


def run_prompt(prompt: str, model: str | None = None) -> str:
    """
    Send a prompt to Ollama and return the raw text response.

    Parameters
    ----------
    prompt : str
        The user instruction to send to the LLM.
    model : str, optional
        Model name to use.  If omitted, ``OLLAMA_MODEL`` or the default
        ``llama3`` will be used.

    Returns
    -------
    str
        The LLM's response text.
    """
    endpoint = DEFAULT_ENDPOINT.rstrip("/")
    model_name = model or DEFAULT_MODEL
    url = f"{endpoint}/api/chat"

    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
        # keep temperature low for deterministic JSON output
        "options": {"temperature": 0.0},
    }

    response = _post_json(url, payload)
    # Ollama returns a dict with a ``message`` field containing ``content``
    try:
        return response["message"]["content"]
    except KeyError:
        raise RuntimeError(f"Unexpected Ollama response format: {response}")
