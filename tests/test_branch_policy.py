import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / ".github" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import branch_policy  # noqa: E402


class BranchPolicyTests(unittest.TestCase):
    def test_load_branch_policy_builds_managed_refs(self):
        values = {
            "GIT_BRANCH_PREFIX": "mcr",
            "GIT_BRANCH_MAIN": "main",
            "GIT_BRANCH_RELEASE": "release",
            "GIT_BRANCH_STAGING": "staging",
            "GIT_BRANCH_REV": "rev",
        }
        with mock.patch.object(branch_policy, "require_secret", side_effect=lambda name: values[name]):
            policy = branch_policy.load_branch_policy()

        self.assertEqual(policy.default_branch, "gitlab/mcr/main")
        self.assertEqual(
            [item.target_name for item in policy.mirrors],
            [
                "gitlab/mcr/main",
                "gitlab/mcr/staging",
                "gitlab/mcr/release",
            ],
        )
        self.assertEqual([item.label for item in policy.mirrors], ["main", "staging", "release"])
        self.assertEqual(policy.rev.target_name, "gitlab/mcr/rev")
        self.assertEqual(policy.rev.label, "rev")


if __name__ == "__main__":
    unittest.main()
