"""
Task Manager for the Planner module.

Provides a simple `plan(prompt: str) -> str` function that forwards
the prompt to the planner agent and returns the generated plan.
"""

from .planner_agent import run_planner


def plan(prompt: str) -> str:
    """
    Get a planning result for the given prompt.

    Parameters
    ----------
    prompt : str
        The user's planning request.

    Returns
    -------
    str
        The planner's response.
    """
    return run_planner(prompt)
