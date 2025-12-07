#!/usr/bin/env python3
"""
Generic data filler for SQLite databases.

This module provides a lightweight agent that can enqueue bulk insert or update
operations against a SQLite database using the background task pool defined in
``autogen_tools``.  It does **not** contain any knowledge of how to obtain data;
callers must supply the ``db_path``, target ``table`` and a list of rows.

Typical usage::

    from filler_agent import fill_table

    rows = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ]

    result = fill_table(
        db_path="Databases/example.db",
        table="users",
        rows=rows,
        mode="insert",
        wait=True,
    )
    print(result)

"""

from typing import List, Dict, Any, Optional
import time

import autogen_tools  # provides queue_task() and task_status()


class FillerAgent:
    """
    Generic data filler for SQLite databases.

    This agent does NOT decide which database or table to use and does NOT
    design schemas. It assumes a planner or upstream agents provide:

    - ``db_path``: path to the target .db file
    - ``table``: name of the target table
    - ``rows``: list of JSON‑serializable dictionaries
    - ``mode``: ``"insert"`` or ``"update"``
    """

    def __init__(self, poll_interval: float = 0.2):
        """
        Parameters
        ----------
        poll_interval : float
            Seconds to sleep between status polls when ``wait=True``.
        """
        self.poll_interval = poll_interval

    def enqueue_rows(
        self,
        db_path: str,
        table: str,
        rows: List[Dict[str, Any]],
        mode: str = "insert",
        key_fields: Optional[List[str]] = None,
        wait: bool = False,
    ) -> Dict[str, Any]:
        """
        Enqueue multiple row operations against a single table.

        Parameters
        ----------
        db_path : str
            Path to the SQLite database file.
        table : str
            Target table name.
        rows : list of dict
            Each dict is a JSON‑serializable row payload.
        mode : {"insert", "update"}
            - ``insert``: always insert new rows.
            - ``update``: build ``WHERE`` from ``key_fields`` and update matching rows.
        key_fields : list of str, optional
            Column names used to build ``WHERE`` clauses in update mode.
        wait : bool
            If True, block until all tasks are completed or error.

        Returns
        -------
        dict
            {
                "mode": ...,
                "db_path": ...,
                "table": ...,
                "task_ids": [...],
                "summary": {"queued": N, "completed": x, "errors": y},
                "details": {task_id: status_dict, ...}  # only when wait=True
            }
        """
        if mode not in {"insert", "update"}:
            raise ValueError(f"Unsupported mode: {mode}")

        if mode == "update" and not key_fields:
            raise ValueError("key_fields is required for update mode")

        task_ids: List[str] = []

        for row in rows:
            if mode == "insert":
                info = autogen_tools.queue_task(
                    db_path=db_path,
                    table=table,
                    op="insert",
                    data=row,
                )
            else:  # update
                where = {k: row[k] for k in key_fields if k in row}
                info = autogen_tools.queue_task(
                    db_path=db_path,
                    table=table,
                    op="update",
                    data=row,
                    where=where,
                )
            task_ids.append(info["task_id"])

        result: Dict[str, Any] = {
            "mode": mode,
            "db_path": db_path,
            "table": table,
            "task_ids": task_ids,
            "summary": {
                "queued": len(rows),
                "completed": 0,
                "errors": 0,
            },
        }

        if not wait:
            return result

        # Wait for all tasks to finish
        details = self.wait_for_tasks(task_ids)
        result["summary"]["completed"] = details["completed"]
        result["summary"]["errors"] = details["errors"]
        result["details"] = details["details"]
        return result

    def wait_for_tasks(self, task_ids: List[str]) -> Dict[str, Any]:
        """
        Block until all given ``task_ids`` are in a terminal state.

        Parameters
        ----------
        task_ids : list of str

        Returns
        -------
        dict
            {
                "completed": int,
                "errors": int,
                "details": {task_id: status_dict, ...}
            }
        """
        remaining = set(task_ids)
        details: Dict[str, Any] = {}
        completed = 0
        errors = 0

        while remaining:
            done_now = []
            for tid in list(remaining):
                st = autogen_tools.task_status(tid)
                status = st.get("status")
                if status in {"completed", "error"}:
                    details[tid] = st
                    done_now.append(tid)
                    if status == "completed":
                        completed += 1
                    else:
                        errors += 1
            for tid in done_now:
                remaining.discard(tid)

            if remaining:
                time.sleep(self.poll_interval)

        return {
            "completed": completed,
            "errors": errors,
            "details": details,
        }


def fill_table(
    db_path: str,
    table: str,
    rows: List[Dict[str, Any]],
    mode: str = "insert",
    key_fields: Optional[List[str]] = None,
    wait: bool = False,
    poll_interval: float = 0.2,
) -> Dict[str, Any]:
    """
    Convenience wrapper for one‑off use without manually instantiating ``FillerAgent``.
    """
    agent = FillerAgent(poll_interval=poll_interval)
    return agent.enqueue_rows(
        db_path=db_path,
        table=table,
        rows=rows,
        mode=mode,
        key_fields=key_fields,
        wait=wait,
    )
