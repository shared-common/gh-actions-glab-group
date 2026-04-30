import contextlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / ".github" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import glab_sync  # noqa: E402
from _common import GitLabClient  # noqa: E402
from branch_policy import BranchPolicy, BranchSpec  # noqa: E402


def make_policy() -> BranchPolicy:
    mirrors = (
        BranchSpec("main", "GIT_BRANCH_MAIN", "gitlab/mcr/main", True),
        BranchSpec("staging", "GIT_BRANCH_STAGING", "gitlab/mcr/staging", True),
        BranchSpec("release", "GIT_BRANCH_RELEASE", "gitlab/mcr/release", True),
    )
    rev = BranchSpec("rev", "GIT_BRANCH_REV", "gitlab/mcr/rev", True)
    return BranchPolicy(prefix="mcr", mirror_prefix="gitlab", mirrors=mirrors, rev=rev)


def write_config(payload: dict) -> str:
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
    with handle:
        json.dump(payload, handle)
        handle.write("\n")
    return handle.name


class GlabSyncTests(unittest.TestCase):
    def test_load_targets_expands_group_projects_into_target_specs(self):
        path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_group": "ghgl-forks/mainline/packages",
                        "target_mirror_group": "ghgl-mirror/mainline/packages",
                        "source_project_group_url": "https://gitlab.example/upstream/packages",
                        "git_timeout_seconds": 900,
                        "branch_rev": "feature/login",
                        "branches": [
                            {"name": "dev/test", "protected": True, "upstream": False},
                        ],
                        "tags": [
                            {"name": "v1.0.0", "protected": True, "upstream": True},
                        ],
                    }
                ],
            }
        )
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        projects = [
            {
                "path_with_namespace": "upstream/packages/nested/keepsecret",
                "http_url_to_repo": "https://gitlab.example/upstream/packages/nested/keepsecret.git",
            },
            {
                "path_with_namespace": "upstream/packages/plain/demo",
                "http_url_to_repo": "https://gitlab.example/upstream/packages/plain/demo.git",
            },
        ]

        with mock.patch.object(glab_sync, "list_gitlab_group_projects", return_value=projects):
            targets = glab_sync.load_targets("group", client=client, path=path)

        self.assertEqual(
            [target.target_project_path for target in targets],
            [
                "ghgl-forks/mainline/packages/nested/keepsecret",
                "ghgl-forks/mainline/packages/plain/demo",
            ],
        )
        self.assertEqual(
            [target.target_mirror_path for target in targets],
            [
                "ghgl-mirror/mainline/packages/nested/keepsecret",
                "ghgl-mirror/mainline/packages/plain/demo",
            ],
        )
        self.assertEqual(targets[0].source, "https://gitlab.example/upstream/packages/nested/keepsecret.git")
        self.assertIsNone(targets[0].git_lfs)
        self.assertEqual(targets[0].git_timeout_seconds, 900)
        self.assertEqual(targets[0].branch_rev, "feature/login")
        self.assertEqual(targets[0].branches[0].name, "dev/test")
        self.assertEqual(targets[0].tags[0].name, "v1.0.0")
        self.assertIsNone(targets[1].git_lfs)

    def test_load_targets_rejects_source_group_urls_on_another_host(self):
        path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_group": "glab-forks/system",
                        "target_mirror_group": "",
                        "source_project_group_url": "https://gitlab.com/kalilinux/packages",
                        "branches": [],
                        "tags": [],
                    }
                ],
            }
        )
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")

        with self.assertRaisesRegex(SystemExit, "must use https://gitlab.example"):
            glab_sync.load_targets("group", client=client, path=path)

    def test_load_targets_rejects_empty_target_list(self):
        path = write_config({"version": 1, "targets": []})
        with self.assertRaisesRegex(SystemExit, "must contain at least one target"):
            glab_sync.load_targets(
                "group",
                client=GitLabClient(base_url="https://gitlab.example", username="svc", token="token"),
                path=path,
            )

    def test_load_targets_applies_project_overrides(self):
        group_path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_group": "glab-forks/kalilinux/packages",
                        "target_mirror_group": "",
                        "source_project_group_url": "https://gitlab.example/kalilinux/packages",
                        "branches": [],
                        "tags": [],
                    }
                ],
            }
        )
        project_path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_path": "glab-forks/kalilinux/packages/bloodhound",
                        "source_import": True,
                        "git_lfs": True,
                        "git_timeout_seconds": 900,
                    }
                ],
            }
        )
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        projects = [
            {
                "path_with_namespace": "kalilinux/packages/bloodhound",
                "http_url_to_repo": "https://gitlab.example/kalilinux/packages/bloodhound.git",
            }
        ]

        with mock.patch.object(glab_sync, "list_gitlab_group_projects", return_value=projects):
            targets = glab_sync.load_targets("group", client=client, path=group_path, project_path=project_path)

        self.assertEqual(len(targets), 1)
        self.assertTrue(targets[0].source_import)
        self.assertTrue(targets[0].git_lfs)
        self.assertEqual(targets[0].git_timeout_seconds, 900)

    def test_load_targets_rejects_unknown_project_overrides(self):
        group_path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_group": "glab-forks/kalilinux/packages",
                        "target_mirror_group": "",
                        "source_project_group_url": "https://gitlab.example/kalilinux/packages",
                        "branches": [],
                        "tags": [],
                    }
                ],
            }
        )
        project_path = write_config(
            {
                "version": 1,
                "targets": [
                    {
                        "target_project_path": "glab-forks/kalilinux/packages/missing",
                        "git_lfs": True,
                    }
                ],
            }
        )
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        projects = [
            {
                "path_with_namespace": "kalilinux/packages/bloodhound",
                "http_url_to_repo": "https://gitlab.example/kalilinux/packages/bloodhound.git",
            }
        ]

        with mock.patch.object(glab_sync, "list_gitlab_group_projects", return_value=projects):
            with self.assertRaisesRegex(SystemExit, "unknown target projects: glab-forks/kalilinux/packages/missing"):
                glab_sync.load_targets("group", client=client, path=group_path, project_path=project_path)

    def test_target_spec_requires_repo_name_to_match_target_path(self):
        with self.assertRaises(SystemExit):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "internal",
                    "target_project_path": "top/sub/demo",
                    "source": "top/source/demo",
                    "repo_name": "other",
                }
            )

    def test_target_spec_rejects_internal_self_sync(self):
        with self.assertRaises(SystemExit):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "internal",
                    "target_project_path": "top/sub/demo",
                    "source": "top/sub/demo",
                }
            )

    def test_target_spec_preserves_external_source_url(self):
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "external",
                "target_project_path": "top/sub/demo",
                "source": "https://invent.kde.org/utilities/keepsecret",
                "branch_rev": "",
                "branches": [],
                "tags": [],
            }
        )
        self.assertEqual(target.source, "https://invent.kde.org/utilities/keepsecret")

    def test_target_spec_rejects_non_boolean_git_lfs(self):
        with self.assertRaisesRegex(SystemExit, "git_lfs must be a boolean when set"):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "external",
                    "target_project_path": "top/sub/demo",
                    "source": "https://example.com/group/demo.git",
                    "git_lfs": "true",
                    "branches": [],
                    "tags": [],
                }
            )

    def test_target_spec_rejects_invalid_git_timeout_seconds(self):
        with self.assertRaisesRegex(SystemExit, "git_timeout_seconds must be between 60 and 7200"):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "external",
                    "target_project_path": "top/sub/demo",
                    "source": "https://example.com/group/demo.git",
                    "git_timeout_seconds": 30,
                    "branches": [],
                    "tags": [],
                }
            )

    def test_target_spec_rejects_target_mirror_path_with_dot_git_suffix(self):
        with self.assertRaisesRegex(SystemExit, "must not include a .git suffix"):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "external",
                    "target_project_path": "top/sub/demo",
                    "target_mirror_path": "mirror/sub/demo.git",
                    "source": "https://example.com/group/demo.git",
                    "branches": [],
                    "tags": [],
                }
            )

    def test_target_spec_rejects_target_mirror_path_self_reference(self):
        with self.assertRaisesRegex(SystemExit, "must differ from target_project_path"):
            glab_sync.TargetSpec.from_payload(
                {
                    "mode": "external",
                    "target_project_path": "top/sub/demo",
                    "target_mirror_path": "top/sub/demo",
                    "source": "https://example.com/group/demo.git",
                    "branches": [],
                    "tags": [],
                }
            )

    def test_managed_branches_include_main_staging_release_rev_and_extra(self):
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "external",
                "target_project_path": "top/sub/demo",
                "source": "https://example.com/group/demo",
                "branch_rev": "feature/login",
                "branches": [
                    {"name": "dev/test", "protected": True, "upstream": False},
                ],
                "tags": [],
            }
        )

        branches = target.managed_branches(make_policy(), "main")

        self.assertEqual(
            [branch.target_name for branch in branches],
            [
                "gitlab/mcr/main",
                "gitlab/mcr/staging",
                "gitlab/mcr/release",
                "gitlab/mcr/rev",
                "gitlab/mcr/dev/test",
            ],
        )

    def test_redact_target_context_replaces_target_identifiers(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="internal",
            target_project_path="top/sub/demo",
            source="top/upstream/demo",
            repo_name="demo",
        )

        message = (
            "Command failed: git push https://gitlab.example/top/sub/demo.git "
            "https://gitlab.example/top/upstream/demo.git top/sub"
        )
        redacted = glab_sync.redact_target_context(message, target, client)

        self.assertNotIn("top/sub/demo", redacted)
        self.assertNotIn("top/upstream/demo", redacted)
        self.assertNotIn("https://gitlab.example/top/sub/demo.git", redacted)
        self.assertNotIn("https://gitlab.example/top/upstream/demo.git", redacted)
        self.assertIn("[REDACTED]", redacted)

    def test_inspect_target_flags_missing_project(self):
        client = GitLabClient(base_url="https://gitlab.com", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="external",
            target_project_path="top/sub/demo",
            source="https://gitlab.example/group/demo.git",
            repo_name="demo",
        )
        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(glab_sync, "get_gitlab_project", return_value=None):
                planned = glab_sync.inspect_target(target, make_policy(), client)

        self.assertTrue(planned["needs_reconcile"])
        self.assertEqual(planned["reasons"], ["project_missing"])
        self.assertTrue(planned["target_id"].startswith("target-"))

    def test_inspect_target_flags_branch_drift_tag_drift_and_protection(self):
        client = GitLabClient(base_url="https://gitlab.com", username="svc", token="token")
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "internal",
                "target_project_path": "top/sub/demo",
                "source": "top/upstream/demo",
                "branch_rev": "feature/login",
                "branches": [
                    {"name": "dev/test", "protected": False, "upstream": False},
                ],
                "tags": [
                    {"name": "v1.0.0", "protected": True, "upstream": True},
                ],
            }
        )
        project = {"id": 77, "default_branch": "wrong-default"}

        branch_shas = {
            "gitlab/mcr/main": "b" * 40,
            "gitlab/mcr/staging": "a" * 40,
            "gitlab/mcr/release": "a" * 40,
            "gitlab/mcr/rev": None,
            "gitlab/mcr/dev/test": "c" * 40,
        }

        def branch_sha_side_effect(_client, _project_id, branch):
            return branch_shas[branch]

        def remote_ref_side_effect(_remote_url, ref_namespace, ref_name, **_kwargs):
            lookup = {
                ("heads", "feature/login"): "d" * 40,
                ("tags", "v1.0.0"): "e" * 40,
                ("tags", "v1.0.0-target"): None,
            }
            if ref_namespace == "tags" and "top/sub/demo.git" in _remote_url:
                return None
            return lookup.get((ref_namespace, ref_name))

        def protected_branch_side_effect(_client, _project_id, branch):
            if branch == "gitlab/mcr/dev/test":
                return {"name": branch}
            if branch == "gitlab/mcr/staging":
                return {
                    "push_access_levels": [{"access_level": 40}],
                    "merge_access_levels": [{"access_level": 40}],
                    "unprotect_access_levels": [{"access_level": 40}],
                    "allow_force_push": True,
                }
            return None

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(glab_sync, "get_gitlab_project", return_value=project):
                with mock.patch.object(glab_sync, "get_gitlab_branch_sha", side_effect=branch_sha_side_effect):
                    with mock.patch.object(glab_sync, "git_remote_ref_sha", side_effect=remote_ref_side_effect):
                        with mock.patch("glab_sync.get_gitlab_protected_branch", side_effect=protected_branch_side_effect):
                            with mock.patch("glab_sync.get_gitlab_protected_tag", return_value=None):
                                with mock.patch.object(glab_sync, "list_gitlab_branches", return_value=[]):
                                    with mock.patch.object(glab_sync, "list_gitlab_tags", return_value=[]):
                                        planned = glab_sync.inspect_target(target, make_policy(), client)

        self.assertIn("sha_diverged:gitlab/mcr/main", planned["reasons"])
        self.assertIn("branch_missing:gitlab/mcr/rev", planned["reasons"])
        self.assertIn("protection_present:gitlab/mcr/dev/test", planned["reasons"])
        self.assertIn("tag_missing:v1.0.0", planned["reasons"])
        self.assertIn("protection_missing:v1.0.0", planned["reasons"])
        self.assertIn("default_branch_mismatch:gitlab/mcr/main", planned["reasons"])

    def test_inspect_target_flags_unmanaged_refs_for_imported_project(self):
        client = GitLabClient(base_url="https://gitlab.com", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="external",
            target_project_path="top/sub/demo",
            source="https://example.com/top/upstream/demo.git",
            repo_name="demo",
        )
        project = {"id": 77, "default_branch": "gitlab/mcr/main", "import_status": "finished"}

        def branch_sha_side_effect(_client, _project_id, branch):
            return {
                "gitlab/mcr/main": "a" * 40,
                "gitlab/mcr/staging": "a" * 40,
                "gitlab/mcr/release": "a" * 40,
            }[branch]

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(glab_sync, "get_gitlab_project", return_value=project):
                with mock.patch.object(glab_sync, "get_gitlab_branch_sha", side_effect=branch_sha_side_effect):
                    with mock.patch.object(glab_sync, "git_remote_ref_sha", return_value=None):
                        with mock.patch.object(glab_sync, "get_gitlab_protected_branch", return_value={
                            "push_access_levels": [{"access_level": 40}],
                            "merge_access_levels": [{"access_level": 40}],
                            "unprotect_access_levels": [{"access_level": 40}],
                            "allow_force_push": True,
                        }):
                            with mock.patch.object(glab_sync, "list_gitlab_branches", return_value=[
                                {"name": "main"},
                                {"name": "gitlab/mcr/main"},
                                {"name": "gitlab/mcr/staging"},
                                {"name": "gitlab/mcr/release"},
                            ]):
                                with mock.patch.object(glab_sync, "list_gitlab_tags", return_value=[{"name": "v1.0.0"}]):
                                    planned = glab_sync.inspect_target(target, make_policy(), client)

        self.assertIn("unmanaged_branches_present", planned["reasons"])
        self.assertIn("unmanaged_tags_present", planned["reasons"])

    def test_inspect_target_flags_source_import_pending_when_import_not_finished(self):
        client = GitLabClient(base_url="https://gitlab.com", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="group",
            target_project_path="top/sub/demo",
            source="https://example.com/top/upstream/demo.git",
            repo_name="demo",
            source_import=True,
        )
        project = {"id": 77, "default_branch": "gitlab/mcr/main", "import_status": "none"}

        def branch_sha_side_effect(_client, _project_id, branch):
            return {
                "gitlab/mcr/main": "a" * 40,
                "gitlab/mcr/staging": "a" * 40,
                "gitlab/mcr/release": "a" * 40,
            }[branch]

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(glab_sync, "get_gitlab_project", return_value=project):
                with mock.patch.object(glab_sync, "get_gitlab_branch_sha", side_effect=branch_sha_side_effect):
                    with mock.patch.object(glab_sync, "git_remote_ref_sha", return_value=None):
                        with mock.patch.object(glab_sync, "get_gitlab_protected_branch", return_value={
                            "push_access_levels": [{"access_level": 40}],
                            "merge_access_levels": [{"access_level": 40}],
                            "unprotect_access_levels": [{"access_level": 40}],
                            "allow_force_push": True,
                        }):
                            with mock.patch.object(glab_sync, "list_gitlab_branches", return_value=[
                                {"name": "main"},
                                {"name": "gitlab/mcr/main"},
                                {"name": "gitlab/mcr/staging"},
                                {"name": "gitlab/mcr/release"},
                            ]):
                                with mock.patch.object(glab_sync, "list_gitlab_tags", return_value=[{"name": "v1.0.0"}]):
                                    planned = glab_sync.inspect_target(target, make_policy(), client)

        self.assertIn("source_import_pending", planned["reasons"])

    def test_render_plan_summary_counts_actionable_items(self):
        summary = glab_sync.render_plan_summary(
            "external",
            [
                {
                    "target_id": "target-111111111111",
                    "repo_name": "demo",
                    "target_project_path": "a/b/demo",
                    "source": "https://example/demo",
                    "needs_reconcile": True,
                    "reasons": ["project_missing", "default_branch_mismatch:gitlab/mcr/main"],
                    "branches": {
                        "gitlab/mcr/main": {
                            "label": "main",
                            "reasons": ["missing", "protection_missing"],
                        }
                    },
                    "tags": {},
                },
                {"target_id": "target-222222222222", "target_project_path": "a/b/clean", "source": "https://example/clean", "needs_reconcile": False, "reasons": []},
            ],
            [{"target_id": "target-333333333333", "error": "boom"}],
        )
        self.assertIn("- inspected: 2", summary)
        self.assertIn("- actionable: 1", summary)
        self.assertIn("- errors: 1", summary)
        self.assertIn("a/b/demo", summary)
        self.assertIn("main missing", summary)
        self.assertIn("default branch mismatch", summary)
        self.assertNotIn("target-111111111111", summary)

    def test_render_reconcile_summary_redacts_target_identity(self):
        summary = glab_sync.render_reconcile_summary(
            {
                "target_id": "target-aaaaaaaaaaaa",
                "target_project_path": "top/sub/demo",
                "source": "https://gitlab.example/top/demo.git",
                "mode": "external",
                "source_default_branch": "main",
                "source_sha": "a" * 40,
                "results": {
                    "created": ["gitlab/mcr/main"],
                    "updated": [],
                    "skipped": [],
                    "protected": [],
                    "pruned": [],
                    "unprotected": [],
                },
            }
        )
        self.assertIn("top/sub/demo", summary)
        self.assertIn("target-aaaaaaaaaaaa", summary)
        self.assertNotIn("https://gitlab.example/top/demo.git", summary)

    def test_reconcile_target_uses_normal_creation_path(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="internal",
            target_project_path="glab-forks/team/demo",
            source="kalilinux/demo",
            repo_name="demo",
        )

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(
                glab_sync,
                    "git_askpass_env",
                    return_value=contextlib.nullcontext({"GIT_ASKPASS": "/tmp/askpass"}),
            ):
                with mock.patch.object(glab_sync, "ensure_gitlab_project", return_value=({"id": 77}, True)) as ensure_project:
                    with mock.patch.object(glab_sync, "_sync_target_refs") as sync_refs:
                        with mock.patch.object(glab_sync, "ensure_gitlab_default_branch", return_value=False):
                            payload = glab_sync.reconcile_target(target, make_policy(), client)

        self.assertEqual(payload["target_project_path"], "glab-forks/team/demo")
        ensure_project.assert_called_once_with(
            client,
            "glab-forks/team/demo",
        )
        sync_refs.assert_called_once()

    def test_reconcile_target_propagates_413(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="internal",
            target_project_path="glab-forks/team/demo",
            source="kalilinux/demo",
            repo_name="demo",
        )

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(
                glab_sync,
                "git_askpass_env",
                return_value=contextlib.nullcontext({"GIT_ASKPASS": "/tmp/askpass"}),
            ):
                with mock.patch.object(glab_sync, "ensure_gitlab_project", return_value=({"id": 77}, True)):
                    with mock.patch.object(
                        glab_sync,
                        "_sync_target_refs",
                        side_effect=SystemExit(
                            "error: RPC failed; HTTP 413 curl 22 The requested URL returned error: 413"
                        ),
                    ):
                        with self.assertRaises(SystemExit) as exc:
                            glab_sync.reconcile_target(target, make_policy(), client)

        self.assertIn("HTTP 413", str(exc.exception))

    def test_import_target_project_creates_missing_group_target_with_import_url(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="group",
            target_project_path="glab-forks/team/demo",
            source="https://gitlab.example/kalilinux/demo.git",
            repo_name="demo",
            source_import=True,
        )
        project_states = [
            None,
            {"id": 29, "import_status": "started"},
            {"id": 29, "import_status": "finished", "path_with_namespace": "glab-forks/team/demo"},
        ]

        with mock.patch.object(glab_sync, "get_gitlab_project", side_effect=project_states):
            with mock.patch.object(glab_sync, "get_gitlab_group_id", return_value=55):
                with mock.patch.object(
                    glab_sync,
                    "gitlab_request",
                    return_value={"id": 29, "import_status": "scheduled"},
                ) as request:
                    with mock.patch("glab_sync.time.sleep") as sleep:
                        project, created = glab_sync._import_target_project(
                            client,
                            target=target,
                            target_project_path="glab-forks/team/demo",
                            timeout_seconds=30,
                        )

        self.assertTrue(created)
        self.assertEqual(project["id"], 29)
        self.assertEqual(sleep.call_count, 1)
        request.assert_called_once_with(
            client,
            "POST",
            "/projects",
            {
                "import_url": "https://svc:token@gitlab.example/kalilinux/demo.git",
                "name": "demo",
                "namespace_id": 55,
                "path": "demo",
                "shared_runners_enabled": False,
                "visibility": "private",
            },
        )

    def test_reconcile_target_uses_source_import_when_requested_for_group_target(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        target = glab_sync.TargetSpec(
            mode="group",
            target_project_path="glab-forks/team/demo",
            source="https://gitlab.example/kalilinux/demo.git",
            repo_name="demo",
            source_import=True,
        )

        with mock.patch.object(glab_sync, "git_source_head", return_value=("main", "a" * 40)):
            with mock.patch.object(
                glab_sync,
                "git_askpass_env",
                return_value=contextlib.nullcontext({"GIT_ASKPASS": "/tmp/askpass"}),
            ):
                with mock.patch.object(
                    glab_sync,
                    "_import_target_project",
                    return_value=({"id": 77, "import_status": "finished", "path_with_namespace": "glab-forks/team/demo"}, True),
                ) as import_project:
                    with mock.patch.object(glab_sync, "_sync_target_refs") as sync_refs:
                        with mock.patch.object(glab_sync, "ensure_gitlab_default_branch", return_value=False):
                            with mock.patch.object(glab_sync, "_prune_imported_refs") as prune_refs:
                                with mock.patch.object(glab_sync, "get_gitlab_project", return_value=None):
                                    payload = glab_sync.reconcile_target(target, make_policy(), client)

        import_project.assert_called_once_with(
            client,
            target=target,
            target_project_path="glab-forks/team/demo",
            timeout_seconds=300,
        )
        sync_refs.assert_called_once()
        prune_refs.assert_not_called()
        self.assertIn("seed:source_import", payload["results"]["updated"])

    def test_sync_branch_creates_missing_target_branch_with_push(self):
        client = GitLabClient(base_url="https://gitlab.example", username="svc", token="token")
        branch = glab_sync.ManagedBranch(
            display_name="main",
            source_name="main",
            target_name="gitlab/mcr/main",
            protected=True,
            upstream=True,
        )
        target = glab_sync.TargetSpec(
            mode="external",
            target_project_path="top/sub/demo",
            source="https://gitlab.example/source/demo.git",
            repo_name="demo",
        )
        results = {
            "created": [],
            "updated": [],
            "skipped": [],
            "protected": [],
            "pruned": [],
            "unprotected": [],
        }

        with mock.patch.object(glab_sync, "_push_ref", return_value="updated") as push_ref:
            with mock.patch.object(glab_sync, "ensure_gitlab_protected_branch", return_value=True):
                glab_sync._sync_branch(
                    branch,
                    target=target,
                    repo_path="/tmp/repo.git",
                    source_url="https://gitlab.example/source/demo.git",
                    target_url="https://gitlab.example/target/demo.git",
                    project_id=77,
                    client=client,
                    existing_sha=None,
                    source_sha="a" * 40,
                    git_lfs_enabled=False,
                    git_timeout_seconds=300,
                    secrets=("token", "svc"),
                    git_env={"GIT_ASKPASS": "/tmp/askpass"},
                    results=results,
                )

        push_ref.assert_called_once()
        self.assertEqual(results["created"], ["gitlab/mcr/main"])
        self.assertEqual(results["protected"], ["gitlab/mcr/main"])

    def test_load_gitlab_client_uses_mode_specific_secret_names(self):
        values = {
            "GL_BASE_URL": "https://gitlab.com",
            "GL_BRIDGE_FORK_USER_GLAB": "glab",
            "GL_PAT_FORK_GLAB_SVC": "glab-token",
        }
        with mock.patch.object(glab_sync, "require_secret", side_effect=lambda name: values[name]):
            external = glab_sync.load_gitlab_client("external")
            internal = glab_sync.load_gitlab_client("internal")
            group = glab_sync.load_gitlab_client("group")
        self.assertEqual((external.username, external.token), ("glab", "glab-token"))
        self.assertEqual((internal.username, internal.token), ("glab", "glab-token"))
        self.assertEqual((group.username, group.token), ("glab", "glab-token"))

    def test_load_mirror_target_client_uses_mirror_secret_names(self):
        values = {
            "GL_BASE_URL": "https://gitlab.com",
            "GL_USER_FORK_MIRROR_SVC": "mirror-user",
            "GL_PAT_FORK_MIRROR_SVC": "mirror-token",
        }
        with mock.patch.object(glab_sync, "require_secret", side_effect=lambda name: values[name]):
            client = glab_sync.load_mirror_target_client()
        self.assertEqual((client.username, client.token), ("mirror-user", "mirror-token"))

    def test_ref_declares_git_lfs_detects_gitattributes_rule(self):
        run_results = [
            subprocess.CompletedProcess(["git"], 0, ".gitattributes\n", ""),
            subprocess.CompletedProcess(["git"], 0, "*.img filter=lfs diff=lfs merge=lfs -text\n", ""),
        ]
        with mock.patch.object(glab_sync, "run_command", side_effect=run_results):
            self.assertTrue(
                glab_sync._ref_declares_git_lfs(
                    "/tmp/repo.git",
                    "refs/heads/main",
                    secrets=(),
                    env_overrides=None,
                )
            )

    def test_push_ref_skips_lfs_commands_when_disabled(self):
        push_envs: list[dict[str, str]] = []

        def fake_push(command, **kwargs):
            push_envs.append(kwargs["env"])
            return subprocess.CompletedProcess(command, 0, "", "")

        with mock.patch.object(glab_sync, "run_command") as run_command:
            with mock.patch("subprocess.run", side_effect=fake_push):
                outcome = glab_sync._push_ref(
                    "/tmp/repo.git",
                    "https://example.com/source.git",
                    "https://example.com/target.git",
                    "main",
                    "gitlab/mcr/main",
                    ref_namespace="heads",
                    source_remote="source",
                    target_remote="target",
                    expected_remote_sha=None,
                )
        self.assertEqual(outcome, "updated")
        run_command.assert_not_called()
        self.assertNotIn("GIT_LFS_SKIP_PUSH", push_envs[0])

    def test_push_ref_runs_lfs_commands_and_skips_pre_push_hook_when_enabled(self):
        push_envs: list[dict[str, str]] = []

        def fake_push(command, **kwargs):
            push_envs.append(kwargs["env"])
            return subprocess.CompletedProcess(command, 0, "", "")

        with mock.patch.object(glab_sync, "run_command") as run_command:
            with mock.patch("subprocess.run", side_effect=fake_push):
                outcome = glab_sync._push_ref(
                    "/tmp/repo.git",
                    "https://example.com/source.git",
                    "https://example.com/target.git",
                    "main",
                    "gitlab/mcr/main",
                    ref_namespace="heads",
                    source_remote="source",
                    target_remote="target",
                    expected_remote_sha=None,
                    timeout_seconds=900,
                    git_lfs_enabled=True,
                )
        self.assertEqual(outcome, "updated")
        self.assertEqual(run_command.call_count, 2)
        self.assertEqual(run_command.call_args_list[0].args[0][3:5], ["lfs", "fetch"])
        self.assertEqual(run_command.call_args_list[1].args[0][3:5], ["lfs", "push"])
        self.assertEqual(run_command.call_args_list[0].kwargs["timeout"], 900)
        self.assertEqual(run_command.call_args_list[1].kwargs["timeout"], 900)
        self.assertEqual(push_envs[0]["GIT_TERMINAL_PROMPT"], "0")
        self.assertEqual(push_envs[0]["GIT_LFS_SKIP_PUSH"], "1")


if __name__ == "__main__":
    unittest.main()
