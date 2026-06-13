import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "group-sync.yml"
PROJECTS_ONLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "projects-only-sync.yml"


class WorkflowContractTests(unittest.TestCase):
    def test_wrapper_targets_shared_workflow_and_config_path(self) -> None:
        text = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "shared-common/glab-groups-shared/.github/workflows/group-sync-core.yml@mcr/main",
            text,
        )
        self.assertIn("shared-ref: mcr/main", text)
        self.assertIn("config-ref: mcr/main", text)
        self.assertIn("config-path: glab-groups-kali", text)
        self.assertIn("target-token-secret: GL_PAT_GROUP_KALI_SVC", text)
        self.assertIn('cron: "5 8 2-30/2 * *"', text)

    def test_projects_only_wrapper_targets_shared_workflow_and_config_path(self) -> None:
        text = PROJECTS_ONLY_WORKFLOW.read_text(encoding="utf-8")
        self.assertIn(
            "shared-common/glab-groups-shared/.github/workflows/group-sync-core.yml@mcr/main",
            text,
        )
        self.assertIn("config-path: glab-groups-kali", text)
        self.assertIn("target-token-secret: GL_PAT_GROUP_KALI_SVC", text)
        self.assertIn("projects-only: true", text)
        self.assertIn("workflow_dispatch:", text)
        self.assertNotIn("schedule:", text)
        self.assertIn("emit-parquet: true", text)


if __name__ == "__main__":
    unittest.main()
