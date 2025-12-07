"""
Interactive Planner CLI

Run with:
    python main.py
"""

from Planner.task_manager import plan

def main() -> None:
    print("Enter a request (type 'exit' or 'quit' to end):")
    while True:
        try:
            user_input = input("> ").strip()
        except EOFError:
            break
        if user_input.lower() in ("exit", "quit"):
            break
        if not user_input:
            continue
        result = plan(user_input)
        print("\n=== Planner Output ===")
        print(result)
        print("\nEnter another request (or 'exit'/'quit'):")

if __name__ == "__main__":
    main()
