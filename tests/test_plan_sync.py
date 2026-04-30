import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / ".github" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import plan_sync  # noqa: E402
from _common import GitLabClient  # noqa: E402
from glab_sync import TargetSpec  # noqa: E402


def make_target(path: str, mirror_path: str = "") -> TargetSpec:
    return TargetSpec(
        mode="group",
        target_project_path=path,
        target_mirror_path=mirror_path,
        source=f"https://gitlab.example/{path}.git",
        repo_name=path.rsplit("/", 1)[-1],
    )


class PlanSyncTests(unittest.TestCase):
    def test_build_batch_matrix_uses_50_target_slices(self):
        self.assertEqual(
            plan_sync.build_batch_matrix(120, 50),
            [{"batch_index": 0}, {"batch_index": 1}, {"batch_index": 2}],
        )

    def test_main_writes_batch_outputs_and_summary(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        targets = [
            make_target(f"glab-forks/kalilinux/packages/project-{index}", "workyard/glab-forks/kalilinux/packages/project-0")
            if index == 0
            else make_target(f"glab-forks/kalilinux/packages/project-{index}")
            for index in range(51)
        ]

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "plan.json"
            summary_path = Path(temp_dir) / "plan.md"
            github_output = Path(temp_dir) / "github-output.txt"
            env = {
                "SYNC_MODE": "group",
                "BATCH_SIZE": "50",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "GITHUB_OUTPUT": str(github_output),
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(plan_sync, "load_gitlab_client", return_value=client):
                    with mock.patch.object(plan_sync, "load_targets", return_value=targets):
                        exit_code = plan_sync.main()
                payload = json.loads(output_path.read_text(encoding="utf-8"))
                summary = summary_path.read_text(encoding="utf-8")
                output_text = github_output.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["target_count"], 51)
        self.assertEqual(payload["batch_matrix"], [{"batch_index": 0}, {"batch_index": 1}])
        self.assertTrue(payload["has_mirror_targets"])
        self.assertIn("- discovered targets: 51", summary)
        self.assertIn("- batches: 2", summary)
        self.assertIn("target_count=51", output_text)
        self.assertIn('batch_matrix=[{"batch_index":0},{"batch_index":1}]', output_text)


if __name__ == "__main__":
    unittest.main()
