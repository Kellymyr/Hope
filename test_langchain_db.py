# file: test_planner.py

"""
Simple planner-style caller for generate_db_from_nl
No LangChain, just plain Python to test the wrapper.
"""

import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "DatabaseFiles"))
from autogen_db_schema import generate_schema_from_nl, generate_db_with_confirmation

def simple_planner(requirements: str):
    # Here is where a future agent/LangChain would add logic:
    # - expand requirements
    # - add must_have_fields
    # - choose db_name based on context
    must_have_fields = ["mileage_km", "owner_count", "fuel_type", "city"]

    # Step 1: generate schema only (no DB creation yet)
    schema_result = generate_schema_from_nl(requirements, must_have_fields)

    print("=== SCHEMA PROPOSED ===")
    print(schema_result["sql_ddl"])

    # Step 2: create DB after (auto‑confirm for test purposes)
    result = generate_db_with_confirmation(
        requirements=requirements,
        db_name="planner_test.db",
        must_have_fields=must_have_fields,
        confirm=True,
    )

    print("=== PLANNER RESULT ===")
    print("DB path:", result["db_path"])
    print("Tables:", result["tables"])
    print("\nSQL DDL:\n")
    print(result["sql_ddl"])


if __name__ == "__main__":
    user_request = (
        "Create a database for used-car listings under 10 lakhs, "
        "including mileage, owner count, fuel type, and city."
    )
    simple_planner(user_request)
