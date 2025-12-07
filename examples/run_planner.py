"""
Example script demonstrating how to use the Planner module.

Run with:
    python examples/run_planner.py
"""

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Planner.task_manager import plan

if __name__ == "__main__":
    # Prompt from user input
    prompt = input("Enter your planning prompt: ")
    result = plan(prompt)
    print("=== Planner Output ===")
    print(result)
