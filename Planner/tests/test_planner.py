import unittest
import uuid
from Planner.task_manager import plan

class TestPlanner(unittest.TestCase):
    def test_plan_returns_nonempty(self):
        prompt = "List three steps to set up a local development environment."
        result = plan(prompt)
        # Verify the result is a non‑empty string
        self.assertTrue(isinstance(result, str) and len(result.strip()) > 0)

    def test_folder_creation_intent(self):
        """Ensure the LLM‑based intent extractor correctly triggers folder creation."""
        folder_name = f"IntentTestFolder_{uuid.uuid4().hex[:8]}"
        prompt = f"make folder on desktop named {folder_name}"
        result = plan(prompt)

        # The planner should return a success message from the system command
        self.assertTrue(isinstance(result, str))
        self.assertIn("Command executed successfully", result)

        # Verify that the folder was actually created on the appropriate Desktop path
        import os
        if os.environ.get("OneDrive"):
            desktop_root = os.path.join(os.environ["OneDrive"], "Desktop")
        else:
            desktop_root = os.path.join(os.environ.get("USERPROFILE", ""), "Desktop")
        expected_path = os.path.join(desktop_root, folder_name)
        self.assertTrue(os.path.isdir(expected_path))

        # Clean‑up the created folder to keep the environment tidy
        try:
            os.rmdir(expected_path)
        except Exception:
            pass

if __name__ == "__main__":
    unittest.main()
