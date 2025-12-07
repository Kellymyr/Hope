#!/usr/bin/env python3
import importlib.util
import pathlib
import sys

root = pathlib.Path(__file__).resolve().parents[1]  # project root (e:\Hope4)
sys.path.insert(0, str(root))

failed = []

for py_file in root.rglob("*.py"):
    # Skip this script itself
    if py_file.name == "temp_check_imports.py":
        continue
    # Derive module name relative to root, replace path separators with dots, strip .py
    rel_path = py_file.relative_to(root).with_suffix("")
    module_name = ".".join(rel_path.parts)
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            # Try loading directly from file location
            spec = importlib.util.spec_from_file_location(module_name, py_file)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)  # type: ignore
        else:
            __import__(module_name)
    except Exception as e:
        failed.append((module_name, str(e)))

if failed:
    print("Import errors detected:")
    for name, err in failed:
        print(f"- {name}: {err}")
    sys.exit(1)
else:
    print("All modules imported successfully.")
