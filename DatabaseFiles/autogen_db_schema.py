"""
LLM-driven DB generator (wrapped for agent use)
- Takes natural language input + optional constraints
- Uses Ollama (gpt-oss:120b-cloud) to design schema
- Creates SQLite DB from LLM-designed schema
- Databases go into ./Databases (relative to project)
- No prints/input in core functions (agent-friendly)
"""

import os
import json
import sqlite3
try:
    import requests
except ImportError:  # pragma: no cover
    requests = None


# ============================================================================
# DATABASE CONFIGURATION (PORTABLE)
# ============================================================================

BASE_DIR = os.environ.get("DB_BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
DATABASE_DIR = os.path.join(BASE_DIR, "Databases")
os.makedirs(DATABASE_DIR, exist_ok=True)


# ============================================================================
# LLM SCHEMA GENERATION (OLLAMA) - AGENT-FRIENDLY
# ============================================================================

def generate_schema_with_llm(
    requirements: str,
    must_have_fields: list = None,
    model: str = "gpt-oss:120b-cloud"
) -> dict:
    """
    Attempt to generate a schema using the Ollama LLM. If the Ollama service is
    unavailable or an error occurs, a RuntimeError is raised.
    """
    # Build prompt (simplified JSON format)
    must_have_text = ""
    if must_have_fields:
        must_have_text = f"\n\nIMPORTANT: Ensure the schema includes these fields: {', '.join(must_have_fields)}"

    prompt = f"""You are a senior database architect.

Task:
Design a relational database schema that satisfies these requirements:

\"\"\"{requirements}\"\""{must_have_text}

Output strictly as valid JSON in this format (no extra text):

{{"tables":[{{"name":"table_name","columns":[{{"name":"column_name","type":"TEXT","primary_key":false,"not_null":false,"unique":false,"default":null,"foreign_key":null}}]}}],"indices":[]}}
"""

    if requests is None:
        raise RuntimeError("requests library is required for schema generation")
    # Try Ollama request (10 s timeout)
    try:
        resp = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.3},
            },
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        text = data.get("response", "").strip()
        schema = json.loads(text)
        return schema
    except Exception as e:
        raise RuntimeError(f"LLM schema generation failed: {e}")


# ============================================================================
# SCHEMA → SQL + DB CREATION - AGENT-FRIENDLY
# ============================================================================

def format_schema_for_display(schema: dict) -> str:
    """Convert schema dict to human-readable SQL DDL."""
    sql_statements = []

    for table in schema.get("tables", []):
        table_name = table.get("name")
        if not table_name:
            continue
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

            columns.append(col_def)

        if columns:
            create_stmt = f"CREATE TABLE {table_name} (\\n  " + ",\\n  ".join(columns) + "\\n);"
            sql_statements.append(create_stmt)

    # Safely handle indices that may lack expected keys
    for idx in schema.get("indices", []):
        table = idx.get("table")
        column = idx.get("column")
        if not table or not column:
            continue
        idx_name = f"idx_{table}_{column}"
        idx_stmt = f"CREATE INDEX {idx_name} ON {table} ({column});"
        sql_statements.append(idx_stmt)

    return "\\n\\n".join(sql_statements)


def create_database_from_schema(schema: dict, db_name: str) -> str:
    """
    Create SQLite DB file from schema.
    
    Args:
        schema: Schema dict with 'tables' and optional 'indices'.
        db_name: Name of the database file (auto-adds .db if needed).
    
    Returns:
        Full path to the created database file.
    
    Raises:
        RuntimeError: If database creation fails.
    """
    db_path = os.path.join(DATABASE_DIR, db_name if db_name.endswith(".db") else db_name + ".db")

    try:
        if os.path.exists(db_path):
            os.remove(db_path)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        table_names = set()

        # Create tables (ignore foreign_key and default)
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

                columns.append(col_def)

            if columns:
                create_stmt = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                cursor.execute(create_stmt)

        # Create indices (SAFE)
        for idx in schema.get("indices", []):
            table = idx.get("table")
            column = idx.get("column")
            if not table or not column:
                continue
            if table not in table_names:
                continue

            idx_name = f"idx_{table}_{column}"
            idx_stmt = f"CREATE INDEX {idx_name} ON {table} ({column})"
            try:
                cursor.execute(idx_stmt)
            except sqlite3.OperationalError:
                pass

        conn.commit()
        conn.close()

        return db_path
    except Exception as e:
        raise RuntimeError(f"Error creating database: {e}") from e


# ============================================================================
# MAIN WRAPPER (For agent/planner use)
# ============================================================================

def generate_db_from_nl(
    requirements: str,
    db_name: str,
    must_have_fields: list = None
) -> dict:
    """
    Main wrapper: NL requirements → schema → database.
    
    Args:
        requirements: Natural language description.
        db_name: Name for the database file.
        must_have_fields: Optional list of field names to enforce.
    
    Returns:
        Dict with keys:
            - db_path: Full path to created database.
            - schema: The generated schema dict.
            - sql_ddl: Human-readable SQL DDL.
            - tables: List of table names created.
    
    Raises:
        RuntimeError: If schema generation or DB creation fails.
    """
    # 1) Generate schema
    schema = generate_schema_with_llm(requirements, must_have_fields)

    # 2) Create database
    db_path = create_database_from_schema(schema, db_name)

    # 3) Get table names for verification
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cur.fetchall()]
    conn.close()

    return {
        "db_path": db_path,
        "schema": schema,
        "sql_ddl": format_schema_for_display(schema),
        "tables": tables,
    }


# ============================================================================
# CLI INTERFACE (for manual testing)
# ============================================================================

def main_cli():
    """Interactive CLI for testing (separate from agent wrapper)."""
    print("=" * 80)
    print("LLM-DRIVEN DATABASE GENERATOR")
    print(f"Databases directory: {DATABASE_DIR}")
    print("=" * 80)

    print("\\nDescribe the database you want (one line). Example:")
    print("- A DB to store LLM chat sessions with users, sessions, and messages")
    requirements = input("\\nYour requirements: ").strip()

    if not requirements:
        print("No requirements provided. Exiting.")
        return

    print("\\n🔮 Asking LLM to design schema...")

    try:
        # First generate schema proposal without creating DB
        proposal = generate_schema_from_nl(requirements)

        print("\\n📋 LLM-PROPOSED SCHEMA (SQL DDL):\\n")
        print(proposal["sql_ddl"])

        choice = input("\\nCreate database from this schema? [y/N]: ").strip().lower()
        if choice != "y":
            print("Aborted by user.")
            return

        # User confirmed, generate DB
        result = generate_db_from_nl(requirements, "llm_generated.db")

        print(f"\\n✅ Database created at: {result['db_path']}")
        print(f"📊 Tables: {result['tables']}")
    except RuntimeError as e:
        print(f"❌ Error: {e}")


def generate_schema_from_nl(requirements: str, must_have_fields: list = None) -> dict:
    """
    Generate only the schema (no DB creation) from natural language requirements.
    Returns a dict with:
        - "schema": raw schema dict
        - "sql_ddl": human‑readable SQL DDL string
    """
    schema = generate_schema_with_llm(requirements, must_have_fields)
    sql_ddl = format_schema_for_display(schema)
    return {"schema": schema, "sql_ddl": sql_ddl}


def generate_db_with_confirmation(
    requirements: str,
    db_name: str,
    must_have_fields: list = None,
    confirm: bool = False,
) -> dict:
    """
    Two‑step workflow:
    1. Generate schema and DDL.
    2. If `confirm` is True, create the SQLite DB and return details.
    Returns a dict containing:
        - "schema": raw schema dict
        - "sql_ddl": DDL string
        - "db_path": path to DB file (or None if not created)
        - "tables": list of created tables (empty if not created)
    """
    # Step 1: generate schema
    result = generate_schema_from_nl(requirements, must_have_fields)

    # Step 2: optional creation
    db_path = None
    tables = []
    if confirm:
        db_path = create_database_from_schema(result["schema"], db_name)
        # retrieve table names
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cur.fetchall()]
        conn.close()

    result.update({"db_path": db_path, "tables": tables})
    return result


if __name__ == "__main__":
    main_cli()
