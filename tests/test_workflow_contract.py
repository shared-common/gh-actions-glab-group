import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "group-sync.yml"


class WorkflowContractTests(unittest.TestCase):
    def test_wrapper_targets_shared_workflow_and_config_path(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "uses: shared-common/glab-groups-shared/.github/workflows/group-sync-core.yml@main",
            text,
        )
        self.assertIn("config-path: glab-groups-kali", text)
        self.assertIn("emit-parquet: true", text)


if __name__ == "__main__":
    unittest.main()
