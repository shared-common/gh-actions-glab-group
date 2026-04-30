from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from _common import (
    ApiError,
    canonicalize_remote_mirror_url,
    ensure_gitlab_project,
    ensure_gitlab_push_mirror,
    get_gitlab_project,
    inject_basic_auth_into_url,
    require_env,
    sanitize,
    sync_gitlab_remote_mirror,
)
from glab_sync import load_gitlab_client, load_mirror_target_client, load_targets, mode_title, write_json


def render_mirror_summary(
    mode: str,
    configured: list[dict[str, Any]],
    errors: list[dict[str, str]],
) -> str:
    title = mode_title(mode)
    lines = [
        f"## {title} target mirror configuration",
        "",
        f"- configured: {len(configured)}",
        f"- errors: {len(errors)}",
        "",
    ]
    if configured:
        lines.append("### Configured mirror targets")
        lines.append("")
        for item in configured:
            sync_triggered = bool(item.get("remote_mirror_sync_triggered"))
            lines.append(
                "- "
                f"`{item['target_project_path']}` -> `{item['target_mirror_path']}`: "
                f"mirror project created={'yes' if item['mirror_project_created'] else 'no'}, "
                f"remote mirror={'created' if item['remote_mirror_created'] else 'updated'}, "
                f"forced sync={'yes' if sync_triggered else 'no'}"
            )
        lines.append("")
    if errors:
        lines.append("### Mirror configuration errors")
        lines.append("")
        for item in errors:
            lines.append(
                f"- `{item['target_project_path']}` -> `{item['target_mirror_path']}`: {item['error']}"
            )
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    mode = require_env("SYNC_MODE")
    output_path = os.environ.get("OUTPUT_PATH", "mirror.json")
    summary_path = os.environ.get("SUMMARY_PATH", "mirror.md")

    source_client = load_gitlab_client(mode)
    mirror_client = load_mirror_target_client()
    targets = [target for target in load_targets(mode, client=source_client) if target.target_mirror_path]

    configured: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for target in targets:
        try:
            source_project = get_gitlab_project(source_client, target.target_project_path)
            if not source_project:
                raise SystemExit(
                    f"target project missing in source GitLab: {target.target_project_path}"
                )
            project_id = int(source_project["id"])
            _, mirror_project_created = ensure_gitlab_project(
                mirror_client, target.target_mirror_path
            )
            mirror_url = mirror_client.project_git_url(target.target_mirror_path)
            mirror_url = canonicalize_remote_mirror_url(mirror_url, "target mirror url")
            authenticated_mirror_url = inject_basic_auth_into_url(
                mirror_url,
                mirror_client.username,
                mirror_client.token,
                "target mirror url",
            )
            remote_mirror, remote_mirror_created = ensure_gitlab_push_mirror(
                source_client,
                project_id,
                authenticated_mirror_url,
                enabled=True,
                only_protected_branches=True,
                auth_method="password",
            )
            remote_mirror_id = remote_mirror.get("id")
            if not isinstance(remote_mirror_id, int):
                raise SystemExit("GitLab remote mirror response is missing id")
            mirror_should_sync = mirror_project_created or remote_mirror_created
            if mirror_should_sync:
                sync_gitlab_remote_mirror(source_client, project_id, remote_mirror_id)
            configured.append(
                {
                    "target_project_path": target.target_project_path,
                    "target_mirror_path": target.target_mirror_path,
                    "mirror_project_created": mirror_project_created,
                    "remote_mirror_created": remote_mirror_created,
                    "remote_mirror_sync_triggered": mirror_should_sync,
                }
            )
        except (ApiError, SystemExit) as exc:
            error = sanitize(
                str(exc) or "mirror configuration failed",
                (
                    source_client.token,
                    source_client.username,
                    mirror_client.token,
                    mirror_client.username,
                ),
            )
            errors.append(
                {
                    "target_project_path": target.target_project_path,
                    "target_mirror_path": target.target_mirror_path,
                    "error": error,
                }
            )

    payload = {
        "mode": mode,
        "configured": configured,
        "errors": errors,
    }
    write_json(output_path, payload)
    Path(summary_path).write_text(
        render_mirror_summary(mode, configured, errors),
        encoding="utf-8",
    )
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
