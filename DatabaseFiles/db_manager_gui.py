#!/usr/bin/env python3
"""
Tkinter GUI for managing SQLite databases.

Features
--------
* Auto‑detect ``*.db`` files in the ``./Databases`` directory (relative to the
  project root) – the same directory used by the CLI and the agent tools.
* List discovered databases, select one, and view its tables.
* Display the contents of a selected table in a scrollable ``ttk.Treeview``.
* Simple *Insert*, *Update* and *Delete* operations:
    - *Insert*: enter JSON‑encoded column/value mapping.
    - *Update*: edit a row selected in the view (JSON editor pre‑filled with
      current values).
    - *Delete*: remove the selected row(s) after confirmation.
* All database connections use the ``SQLiteManager`` from ``db_manager_agent.py``,
  which enables WAL mode and a 30 s busy timeout for safe concurrent reads.

The GUI is intentionally lightweight – it provides the core CRUD workflow
without trying to become a full‑featured admin console.
"""

import json
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
from typing import Any, Dict, List

from db_manager_agent import SQLiteManager


class DBGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQLite DB Manager")
        self.geometry("960x600")
        self.mgr = SQLiteManager()  # uses default "./Databases"

        # ------------------------------------------------------------------
        # UI layout
        # ------------------------------------------------------------------
        # Top frame – database selector
        top_frame = ttk.Frame(self)
        top_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(top_frame, text="Database:").pack(side=tk.LEFT)
        self.db_var = tk.StringVar()
        self.db_combo = ttk.Combobox(top_frame, textvariable=self.db_var, state="readonly", width=50)
        self.db_combo.pack(side=tk.LEFT, padx=5)
        self.db_combo.bind("<<ComboboxSelected>>", self._on_db_selected)

        ttk.Button(top_frame, text="Refresh", command=self._refresh_databases).pack(side=tk.LEFT, padx=5)

        # Middle frame – table list and operations
        middle = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        middle.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Left pane – table list
        left_pane = ttk.Frame(middle, width=200)
        middle.add(left_pane, weight=1)

        ttk.Label(left_pane, text="Tables").pack(anchor=tk.W)
        self.table_listbox = tk.Listbox(left_pane, exportselection=False)
        self.table_listbox.pack(fill=tk.BOTH, expand=True, pady=2)
        self.table_listbox.bind("<<ListboxSelect>>", self._on_table_selected)

        # Right pane – rows view + CRUD buttons
        right_pane = ttk.Frame(middle)
        middle.add(right_pane, weight=4)

        # Treeview for rows
        self.tree = ttk.Treeview(right_pane, show="headings")
        self.tree.pack(fill=tk.BOTH, expand=True, side=tk.TOP)

        # Scrollbars
        vsb = ttk.Scrollbar(right_pane, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(right_pane, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)

        # CRUD button bar
        btn_bar = ttk.Frame(right_pane)
        btn_bar.pack(fill=tk.X, pady=4)

        ttk.Button(btn_bar, text="Refresh rows", command=self._refresh_rows).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_bar, text="Insert", command=self._insert_row).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_bar, text="Update", command=self._update_row).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_bar, text="Delete", command=self._delete_rows).pack(side=tk.LEFT, padx=2)

        # Initialise UI state
        self._refresh_databases()

    # ----------------------------------------------------------------------
    # Helper methods
    # ----------------------------------------------------------------------
    def _refresh_databases(self) -> None:
        """Populate the database combobox with discovered *.db files."""
        dbs = self.mgr.list_databases()
        self.db_combo["values"] = dbs
        if dbs:
            self.db_var.set(dbs[0])
            self._load_database(dbs[0])
        else:
            self.db_var.set("")
            self._clear_tables_and_rows()

    def _load_database(self, path: str) -> None:
        """Connect to a database and list its tables."""
        self.mgr.connect(path)
        tables = self.mgr.list_tables()
        self.table_listbox.delete(0, tk.END)
        for tbl in tables:
            self.table_listbox.insert(tk.END, tbl)
        self._clear_rows()
        self.mgr.close()

    def _clear_tables_and_rows(self) -> None:
        self.table_listbox.delete(0, tk.END)
        self._clear_rows()

    def _clear_rows(self) -> None:
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
        self.tree["columns"] = ()
        for item in self.tree.get_children():
            self.tree.delete(item)

    # ----------------------------------------------------------------------
    # Event callbacks
    # ----------------------------------------------------------------------
    def _on_db_selected(self, event: tk.Event) -> None:
        db_path = self.db_var.get()
        if db_path:
            self._load_database(db_path)

    def _on_table_selected(self, event: tk.Event) -> None:
        self._refresh_rows()

    def _refresh_rows(self) -> None:
        """Fetch rows for the selected table and display them."""
        selected = self.table_listbox.curselection()
        if not selected:
            return
        table = self.table_listbox.get(selected[0])

        # Connect, fetch, disconnect
        self.mgr.connect(self.db_var.get())
        rows = self.mgr.fetch(table=table)
        self.mgr.close()

        # Determine columns from first row (if any)
        columns: List[str] = []
        if rows:
            # rows are dicts (thanks to manager fetch implementation)
            columns = list(rows[0].keys())
        self.tree["columns"] = columns
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, anchor=tk.W)

        # Clear previous data
        for item in self.tree.get_children():
            self.tree.delete(item)

        for row in rows:
            values = [row[col] for col in columns]
            self.tree.insert("", tk.END, values=values)

    # ----------------------------------------------------------------------
    # CRUD operations
    # ----------------------------------------------------------------------
    def _insert_row(self) -> None:
        selected = self.table_listbox.curselection()
        if not selected:
            messagebox.showwarning("No table", "Select a table first.")
            return
        table = self.table_listbox.get(selected[0])

        json_str = simpledialog.askstring(
            "Insert row",
            "Enter column/value mapping as JSON (e.g. {\"name\": \"Bob\", \"age\": 30}):",
        )
        if not json_str:
            return
        try:
            data: Dict[str, Any] = json.loads(json_str)
        except json.JSONDecodeError as exc:
            messagebox.showerror("Invalid JSON", f"Could not parse JSON:\n{exc}")
            return

        self.mgr.connect(self.db_var.get())
        try:
            rowid = self.mgr.insert(table, data)
            messagebox.showinfo("Success", f"Inserted row with rowid {rowid}")
        except Exception as exc:
            messagebox.showerror("Error", f"Insertion failed:\n{exc}")
        finally:
            self.mgr.close()
            self._refresh_rows()

    def _update_row(self) -> None:
        selected = self.table_listbox.curselection()
        if not selected:
            messagebox.showwarning("No table", "Select a table first.")
            return
        table = self.table_listbox.get(selected[0])

        sel_items = self.tree.selection()
        if not sel_items:
            messagebox.showwarning("No row", "Select a row to update.")
            return
        # For simplicity we only allow updating a single row at a time
        item = sel_items[0]
        old_values = self.tree.item(item, "values")
        columns = self.tree["columns"]
        current: Dict[str, Any] = dict(zip(columns, old_values))

        json_str = simpledialog.askstring(
            "Update row",
            f"Current values:\n{json.dumps(current, indent=2)}\n\n"
            "Edit the JSON and press OK:",
        )
        if not json_str:
            return
        try:
            new_data: Dict[str, Any] = json.loads(json_str)
        except json.JSONDecodeError as exc:
            messagebox.showerror("Invalid JSON", f"Could not parse JSON:\n{exc}")
            return

        # Build a WHERE clause that uniquely identifies the row using its primary key
        # If a column named "id" exists we use it; otherwise we fall back to all columns.
        where: Dict[str, Any] = {}
        if "id" in current:
            where["id"] = current["id"]
        else:
            where = current  # may affect multiple rows – user should ensure uniqueness

        self.mgr.connect(self.db_var.get())
        try:
            count = self.mgr.update(table, new_data, where)
            messagebox.showinfo("Success", f"Rows updated: {count}")
        except Exception as exc:
            messagebox.showerror("Error", f"Update failed:\n{exc}")
        finally:
            self.mgr.close()
            self._refresh_rows()

    def _delete_rows(self) -> None:
        selected = self.table_listbox.curselection()
        if not selected:
            messagebox.showwarning("No table", "Select a table first.")
            return
        table = self.table_listbox.get(selected[0])

        sel_items = self.tree.selection()
        if not sel_items:
            messagebox.showwarning("No row", "Select row(s) to delete.")
            return
        if not messagebox.askyesno(
            "Confirm delete", f"Delete {len(sel_items)} selected row(s)?"
        ):
            return

        columns = self.tree["columns"]
        total_deleted = 0
        self.mgr.connect(self.db_var.get())
        try:
            for item in sel_items:
                values = self.tree.item(item, "values")
                row_dict = dict(zip(columns, values))
                # Use "id" column if present, otherwise all columns as condition
                if "id" in row_dict:
                    where = {"id": row_dict["id"]}
                else:
                    where = row_dict
                deleted = self.mgr.delete(table, where)
                total_deleted += deleted
            messagebox.showinfo("Success", f"Deleted rows: {total_deleted}")
        except Exception as exc:
            messagebox.showerror("Error", f"Deletion failed:\n{exc}")
        finally:
            self.mgr.close()
            self._refresh_rows()

    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------
    def destroy(self) -> None:
        """Override to ensure any open connection is closed."""
        try:
            self.mgr.close()
        finally:
            super().destroy()


def main() -> None:
    app = DBGui()
    app.mainloop()


if __name__ == "__main__":
    main()
