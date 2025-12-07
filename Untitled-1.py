"""
LLM-driven DB generator
- Takes natural language input
- Uses Ollama (gpt-oss:120b-cloud) to design schema
- Creates SQLite DB from LLM-designed schema
- Databases go into ./Databases (relative to project)
"""

import os
import json
import sqlite3
import requests

# ============================================================================
# DATABASE CONFIGURATION (PORTABLE)
# ============================================================================

BASE_DIR = os.environ.get("DB_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "Databases")
os.makedirs(DATABASE_DIR, exist_ok=True)


# ============================================================================
# LLM SCHEMA GENERATION (OLLAMA)
# ============================================================================

def generate_schema_with_llm(requirements: str, model: str = "gpt-oss:120b-cloud") -> dict:
    """
    Ask the LLM to design a schema given natural language requirements.
    Returns a Python dict with 'tables' and optional 'indices'.
    """
    prompt = f"""
You are a senior database architect.

Task:
Design a relational database schema that satisfies these requirements:

\"\"\"{requirements}\"\"\"

Output strictly as valid JSON in this format (no extra text):

{{
  "tables": [
    {{
      "name": "table_name",
      "columns": [
        {{
          "name": "column_name",
          "type": "INTEGER|TEXT|REAL|BOOLEAN|TIMESTAMP",
          "primary_key": true|false,
          "not_null": true|false,
          "unique": true|false,
          "default": null or a SQL literal,
          "foreign_key": "other_table.other_column" or null
        }}
      ]
    }}
  ],
  "indices": [
    {{
      "table": "table_name",
      "column": "column_name"
    }}
  ]
}}
Only output JSON.
"""

    try:
        resp = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3},
            },
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data.get("response", "").strip()
        schema = json.loads(text)
        return schema
    except Exception as e:
        print(f"LLM schema generation failed: {e}")
        raise


# ============================================================================
# SCHEMA → SQL + DB CREATION
# ============================================================================

def format_schema_for_display(schema: dict) -> str:
    """Convert schema dict to human-readable SQL DDL."""
    sql_statements = []

    for table in schema.get("tables", []):
        table_name = table["name"]
        columns = []

        for col in table["columns"]:
            col_def = f"{col['name']} {col['type']}"

            if col.get("primary_key"):
                col_def += " PRIMARY KEY"
            if col.get("not_null"):
                col_def += " NOT NULL"
            if col.get("unique"):
                col_def += " UNIQUE"
            if col.get("default") is not None:
                col_def += f" DEFAULT {col['default']}"
            if col.get("foreign_key"):
                col_def += f" REFERENCES {col['foreign_key']}"

            columns.append(col_def)

        create_stmt = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n);"
        sql_statements.append(create_stmt)

    for idx in schema.get("indices", []):
        idx_name = f"idx_{idx['table']}_{idx['column']}"
        idx_stmt = f"CREATE INDEX {idx_name} ON {idx['table']} ({idx['column']});"
        sql_statements.append(idx_stmt)

    return "\n\n".join(sql_statements)


def create_database_from_schema(schema: dict, db_name: str) -> str:
    """Create SQLite DB file from schema in the project Databases folder (safe version)."""
    db_path = os.path.join(DATABASE_DIR, db_name if db_name.endswith(".db") else db_name + ".db")

    try:
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Collect table names to validate indices later
        table_names = set()

        # Create tables (SAFE: ignore foreign_key and default from LLM)
        for table in schema.get("tables", []):
            table_name = table.get("name")
            if not table_name:
                continue
            table_names.add(table_name)

            columns = []
            for col in table.get("columns", []):
                col_name = col.get("name")
                col_type = col.get("type", "TEXT")
                if not col_name:
                    continue

                col_def = f"{col_name} {col_type}"

                if col.get("primary_key"):
                    col_def += " PRIMARY KEY"
                if col.get("not_null"):
                    col_def += " NOT NULL"
                if col.get("unique"):
                    col_def += " UNIQUE"

                # IMPORTANT: ignore default and foreign_key completely to avoid bad SQL
                columns.append(col_def)

            if not columns:
                continue

            create_stmt = f"CREATE TABLE {table_name} ({', '.join(columns)})"
            # Debug:
            # print("EXECUTING TABLE:", create_stmt)
            cursor.execute(create_stmt)

        # Create indices (SAFE: only if table & column names are sane)
        for idx in schema.get("indices", []):
            table = idx.get("table")
            column = idx.get("column")
            if not table or not column:
                continue
            if table not in table_names:
                continue

            idx_name = f"idx_{table}_{column}"
            idx_stmt = f"CREATE INDEX {idx_name} ON {table} ({column})"
            # Debug:
            # print("EXECUTING INDEX:", idx_stmt)
            try:
                cursor.execute(idx_stmt)
            except sqlite3.OperationalError:
                # If column doesn't exist or SQL invalid, just skip index
                pass

        conn.commit()
        conn.close()

        return db_path
    except Exception as e:
        raise RuntimeError(f"Error creating database: {e}") from e



# ============================================================================
# MAIN FLOW: NATURAL LANGUAGE → LLM → DB
# ============================================================================

def main():
    print("=" * 80)
    print("LLM-DRIVEN DATABASE GENERATOR")
    print(f"Databases directory: {DATABASE_DIR}")
    print("=" * 80)

    # 1) Get natural language input
    print("\nDescribe the database you want (one line). Example:")
    print("- A DB to store LLM chat sessions with users, sessions, and messages")
    requirements = input("\nYour requirements: ").strip()

    if not requirements:
        print("No requirements provided. Exiting.")
        return

    # 2) Ask LLM to design schema
    print("\n🔮 Asking LLM to design schema...")
    schema = generate_schema_with_llm(requirements)

    # 3) Show proposed schema
    print("\n📋 LLM-PROPOSED SCHEMA (SQL DDL):\n")
    print(format_schema_for_display(schema))

    # 4) Ask for confirmation
    choice = input("\nCreate database from this schema? [y/N]: ").strip().lower()
    if choice != "y":
        print("Aborted by user.")
        return

    # 5) Create DB (name derived from a simple slug)
    default_name = "llm_generated.db"
    db_name_input = input(f"Database file name [{default_name}]: ").strip()
    db_name = db_name_input or default_name

    print(f"\n💾 Creating database '{db_name}' ...")
    db_path = create_database_from_schema(schema, db_name)
    print(f"\n✅ Database created at: {db_path}")

    # 6) Quick verification
    print("\n🔍 Quick verification (tables):")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cur.fetchall()]
    print("Tables:", tables)
    conn.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
