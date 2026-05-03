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

import reconcile_planned_targets  # noqa: E402
from _common import GitLabClient  # noqa: E402
from glab_sync import TargetSpec  # noqa: E402


def make_target(path: str) -> TargetSpec:
    return TargetSpec(
        mode="group",
        target_project_path=path,
        source=f"https://gitlab.example/{path}.git",
        repo_name=path.rsplit("/", 1)[-1],
    )


class ReconcilePlannedTargetsTests(unittest.TestCase):
    def test_slice_batch_returns_requested_window(self):
        targets = [make_target(f"top/group/project-{index}") for index in range(6)]
        batch = reconcile_planned_targets.slice_batch(targets, 1, 2)
        self.assertEqual(
            [target.target_project_path for target in batch],
            ["top/group/project-2", "top/group/project-3"],
        )

    def test_main_reconciles_only_actionable_targets_in_batch(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        policy = object()
        targets = [
            make_target("top/group/project-0"),
            make_target("top/group/project-1"),
            make_target("top/group/project-2"),
        ]

        def inspect_side_effect(target, _policy, _client):
            if target.target_project_path.endswith("project-1"):
                return {"target_id": target.target_id, "needs_reconcile": True}
            return {"target_id": target.target_id, "needs_reconcile": False}

        reconcile_payload = {
            "mode": "group",
            "target_id": targets[1].target_id,
            "target_project_path": targets[1].target_project_path,
            "results": {
                "created": [],
                "updated": [],
                "skipped": ["gitlab/mcr/rev (source missing: feature/rev)"],
                "protected": [],
                "pruned": [],
                "unprotected": [],
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "reconcile.json"
            summary_path = Path(temp_dir) / "reconcile.md"
            env = {
                "SYNC_MODE": "group",
                "BATCH_INDEX": "0",
                "BATCH_SIZE": "2",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "TARGETS_CONFIG_PATH": "gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(reconcile_planned_targets, "load_branch_policy", return_value=policy):
                    with mock.patch.object(reconcile_planned_targets, "load_gitlab_client", return_value=client):
                        with mock.patch.object(reconcile_planned_targets, "load_targets", return_value=targets):
                            with mock.patch.object(
                                reconcile_planned_targets,
                                "inspect_target",
                                side_effect=inspect_side_effect,
                            ):
                                with mock.patch.object(
                                    reconcile_planned_targets,
                                    "reconcile_target",
                                    return_value=reconcile_payload,
                                ) as reconcile_target:
                                    exit_code = reconcile_planned_targets.main()
                                payload = json.loads(output_path.read_text(encoding="utf-8"))
                                summary = summary_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        reconcile_target.assert_called_once_with(targets[1], policy, client)
        self.assertEqual(payload["inspected_count"], 2)
        self.assertEqual(payload["clean_count"], 1)
        self.assertEqual(len(payload["reconciled"]), 1)
        self.assertIn("- batch index: 0", summary)
        self.assertIn("skipped refs: gitlab/mcr/rev (source missing: feature/rev)", summary)


if __name__ == "__main__":
    unittest.main()
