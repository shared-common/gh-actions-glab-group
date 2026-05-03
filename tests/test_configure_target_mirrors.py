import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPTS_DIR = Path(__file__).resolve().parents[1] / ".github" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import configure_target_mirrors  # noqa: E402
import glab_sync  # noqa: E402
from _common import GitLabClient  # noqa: E402


class ConfigureTargetMirrorsTests(unittest.TestCase):
    def test_render_mirror_summary_lists_paths(self):
        summary = configure_target_mirrors.render_mirror_summary(
            "group",
            [
                {
                    "target_project_path": "glab-forks/debian/mako",
                    "target_mirror_path": "glab-mirror/debian/mako",
                    "mirror_project_created": True,
                    "remote_mirror_created": False,
                    "remote_mirror_sync_triggered": True,
                }
            ],
            [
                {
                    "target_project_path": "glab-forks/debian/wofi",
                    "target_mirror_path": "glab-mirror/debian/wofi",
                    "error": "boom",
                }
            ],
        )
        self.assertIn("glab-forks/debian/mako", summary)
        self.assertIn("glab-mirror/debian/mako", summary)
        self.assertIn("remote mirror=updated", summary)
        self.assertIn("forced sync=yes", summary)
        self.assertIn("glab-forks/debian/wofi", summary)
        self.assertIn("boom", summary)

    def test_main_configures_targets_with_target_mirror_path(self):
        target_with_mirror = glab_sync.TargetSpec.from_payload(
            {
                "mode": "external",
                "target_project_path": "glab-forks/debian/mako",
                "target_mirror_path": "glab-mirror/debian/mako",
                "source": "https://salsa.debian.org/swaywm-team/mako",
                "branches": [],
                "tags": [],
            }
        )
        target_without_mirror = glab_sync.TargetSpec.from_payload(
            {
                "mode": "external",
                "target_project_path": "glab-forks/debian/wofi",
                "target_mirror_path": "",
                "source": "https://salsa.debian.org/swaywm-team/wofi",
                "branches": [],
                "tags": [],
            }
        )
        source_client = GitLabClient("https://gitlab.example.com", "sync-user", "sync-token")
        mirror_client = GitLabClient("https://gitlab.example.com", "mirror-user", "mirror-token")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "mirrors.json"
            summary_path = Path(temp_dir) / "mirrors.md"
            env = {
                "SYNC_MODE": "group",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "TARGETS_CONFIG_PATH": "gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(
                    configure_target_mirrors, "load_targets", return_value=[target_with_mirror, target_without_mirror]
                ):
                    with mock.patch.object(configure_target_mirrors, "load_gitlab_client", return_value=source_client):
                        with mock.patch.object(
                            configure_target_mirrors,
                            "load_mirror_target_client",
                            return_value=mirror_client,
                        ):
                            with mock.patch.object(
                                configure_target_mirrors,
                                "get_gitlab_project",
                                return_value={"id": 77},
                            ) as get_project:
                                with mock.patch.object(
                                    configure_target_mirrors,
                                    "ensure_gitlab_project",
                                    return_value=({"id": 88}, True),
                                ) as ensure_project:
                                    with mock.patch.object(
                                        configure_target_mirrors,
                                        "ensure_gitlab_push_mirror",
                                        return_value=({"id": 99}, True),
                                    ) as ensure_mirror:
                                        with mock.patch.object(
                                            configure_target_mirrors,
                                            "sync_gitlab_remote_mirror",
                                        ) as sync_mirror:
                                            exit_code = configure_target_mirrors.main()
                        self.assertEqual(exit_code, 0)
                        get_project.assert_called_once_with(source_client, "glab-forks/debian/mako")
                        ensure_project.assert_called_once_with(mirror_client, "glab-mirror/debian/mako")
                        ensure_mirror.assert_called_once()
                        sync_mirror.assert_called_once_with(source_client, 77, 99)
                        self.assertTrue(output_path.exists())
                        self.assertTrue(summary_path.exists())
                        summary = summary_path.read_text(encoding="utf-8")
                        self.assertIn("glab-forks/debian/mako", summary)
                        self.assertNotIn("glab-forks/debian/wofi", summary)

    def test_main_force_syncs_internal_targets_when_mirror_or_project_is_new(self):
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "internal",
                "target_project_path": "glab-forks/debian/mako",
                "target_mirror_path": "glab-mirror/debian/mako",
                "source": "fbb-git/mako",
                "branches": [],
                "tags": [],
            }
        )
        source_client = GitLabClient("https://gitlab.example.com", "sync-user", "sync-token")
        mirror_client = GitLabClient("https://gitlab.example.com", "mirror-user", "mirror-token")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "mirrors.json"
            summary_path = Path(temp_dir) / "mirrors.md"
            env = {
                "SYNC_MODE": "group",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "TARGETS_CONFIG_PATH": "gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(configure_target_mirrors, "load_targets", return_value=[target]):
                    with mock.patch.object(configure_target_mirrors, "load_gitlab_client", return_value=source_client):
                        with mock.patch.object(
                            configure_target_mirrors,
                            "load_mirror_target_client",
                            return_value=mirror_client,
                        ):
                            with mock.patch.object(
                                configure_target_mirrors,
                                "get_gitlab_project",
                                return_value={"id": 77},
                            ):
                                with mock.patch.object(
                                    configure_target_mirrors,
                                    "ensure_gitlab_project",
                                    return_value=({"id": 88}, True),
                                ):
                                    with mock.patch.object(
                                        configure_target_mirrors,
                                        "ensure_gitlab_push_mirror",
                                        return_value=({"id": 99}, True),
                                    ):
                                        with mock.patch.object(
                                            configure_target_mirrors,
                                            "sync_gitlab_remote_mirror",
                                        ) as sync_mirror:
                                            exit_code = configure_target_mirrors.main()
        self.assertEqual(exit_code, 0)
        sync_mirror.assert_called_once_with(source_client, 77, 99)

    def test_main_does_not_force_sync_unchanged_external_targets(self):
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "external",
                "target_project_path": "glab-forks/debian/mako",
                "target_mirror_path": "glab-mirror/debian/mako",
                "source": "https://salsa.debian.org/swaywm-team/mako",
                "branches": [],
                "tags": [],
            }
        )
        source_client = GitLabClient("https://gitlab.example.com", "sync-user", "sync-token")
        mirror_client = GitLabClient("https://gitlab.example.com", "mirror-user", "mirror-token")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "mirrors.json"
            summary_path = Path(temp_dir) / "mirrors.md"
            env = {
                "SYNC_MODE": "group",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "TARGETS_CONFIG_PATH": "gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(configure_target_mirrors, "load_targets", return_value=[target]):
                    with mock.patch.object(configure_target_mirrors, "load_gitlab_client", return_value=source_client):
                        with mock.patch.object(
                            configure_target_mirrors,
                            "load_mirror_target_client",
                            return_value=mirror_client,
                        ):
                            with mock.patch.object(
                                configure_target_mirrors,
                                "get_gitlab_project",
                                return_value={"id": 77},
                            ):
                                with mock.patch.object(
                                    configure_target_mirrors,
                                    "ensure_gitlab_project",
                                    return_value=({"id": 88}, False),
                                ):
                                    with mock.patch.object(
                                        configure_target_mirrors,
                                        "ensure_gitlab_push_mirror",
                                        return_value=({"id": 99}, False),
                                    ):
                                        with mock.patch.object(
                                            configure_target_mirrors,
                                            "sync_gitlab_remote_mirror",
                                        ) as sync_mirror:
                                            exit_code = configure_target_mirrors.main()
        self.assertEqual(exit_code, 0)
        sync_mirror.assert_not_called()

    def test_main_does_not_force_sync_unchanged_internal_targets(self):
        target = glab_sync.TargetSpec.from_payload(
            {
                "mode": "internal",
                "target_project_path": "glab-forks/debian/mako",
                "target_mirror_path": "glab-mirror/debian/mako",
                "source": "fbb-git/mako",
                "branches": [],
                "tags": [],
            }
        )
        source_client = GitLabClient("https://gitlab.example.com", "sync-user", "sync-token")
        mirror_client = GitLabClient("https://gitlab.example.com", "mirror-user", "mirror-token")

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "mirrors.json"
            summary_path = Path(temp_dir) / "mirrors.md"
            env = {
                "SYNC_MODE": "group",
                "OUTPUT_PATH": str(output_path),
                "SUMMARY_PATH": str(summary_path),
                "TARGETS_CONFIG_PATH": "gh-actions-cfg/gh-actions-glab-group/gl_forks_group.json",
            }
            with mock.patch.dict(os.environ, env, clear=False):
                with mock.patch.object(configure_target_mirrors, "load_targets", return_value=[target]):
                    with mock.patch.object(configure_target_mirrors, "load_gitlab_client", return_value=source_client):
                        with mock.patch.object(
                            configure_target_mirrors,
                            "load_mirror_target_client",
                            return_value=mirror_client,
                        ):
                            with mock.patch.object(
                                configure_target_mirrors,
                                "get_gitlab_project",
                                return_value={"id": 77},
                            ):
                                with mock.patch.object(
                                    configure_target_mirrors,
                                    "ensure_gitlab_project",
                                    return_value=({"id": 88}, False),
                                ):
                                    with mock.patch.object(
                                        configure_target_mirrors,
                                        "ensure_gitlab_push_mirror",
                                        return_value=({"id": 99}, False),
                                    ):
                                        with mock.patch.object(
                                            configure_target_mirrors,
                                            "sync_gitlab_remote_mirror",
                                        ) as sync_mirror:
                                            exit_code = configure_target_mirrors.main()
        self.assertEqual(exit_code, 0)
        sync_mirror.assert_not_called()


if __name__ == "__main__":
    unittest.main()
