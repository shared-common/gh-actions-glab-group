from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from _common import require_env
from glab_sync import load_gitlab_client, load_targets, write_json


def _require_positive_int(raw: str, label: str, *, minimum: int = 1, maximum: int = 500) -> int:
    value = raw.strip()
    if not value:
        raise SystemExit(f"Missing required env var: {label}")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise SystemExit(f"{label} must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise SystemExit(f"{label} must be between {minimum} and {maximum}")
    return parsed


def build_batch_matrix(target_count: int, batch_size: int) -> list[dict[str, int]]:
    return [
        {
            "batch_index": batch_index,
        }
        for batch_index in range((target_count + batch_size - 1) // batch_size)
    ]


def render_plan_summary(target_count: int, batch_size: int, batch_count: int, has_mirror_targets: bool) -> str:
    lines = [
        "## Group sync plan",
        "",
        f"- discovered targets: {target_count}",
        f"- batch size: {batch_size}",
        f"- batches: {batch_count}",
        f"- has mirror targets: {'yes' if has_mirror_targets else 'no'}",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    mode = require_env("SYNC_MODE")
    output_path = os.environ.get("OUTPUT_PATH", "plan.json")
    summary_path = os.environ.get("SUMMARY_PATH", "plan.md")
    github_output = os.environ.get("GITHUB_OUTPUT", "")
    batch_size = _require_positive_int(os.environ.get("BATCH_SIZE", "50"), "BATCH_SIZE")

    config_path = require_env("TARGETS_CONFIG_PATH")
    client = load_gitlab_client(mode, path=config_path)
    targets = load_targets(mode, client=client, path=config_path)
    batch_matrix = build_batch_matrix(len(targets), batch_size)
    has_mirror_targets = any(bool(target.target_mirror_path) for target in targets)

    payload: dict[str, Any] = {
        "mode": mode,
        "batch_size": batch_size,
        "target_count": len(targets),
        "batch_matrix": batch_matrix,
        "has_mirror_targets": has_mirror_targets,
    }
    write_json(output_path, payload)
    Path(summary_path).write_text(
        render_plan_summary(len(targets), batch_size, len(batch_matrix), has_mirror_targets),
        encoding="utf-8",
    )

    if github_output:
        with open(github_output, "a", encoding="utf-8") as handle:
            handle.write(f"has_targets={'true' if bool(targets) else 'false'}\n")
            handle.write(f"should_run={'true' if bool(batch_matrix) else 'false'}\n")
            handle.write(f"target_count={len(targets)}\n")
            handle.write(f"batch_count={len(batch_matrix)}\n")
            handle.write(f"has_mirror_targets={'true' if has_mirror_targets else 'false'}\n")
            handle.write(f"batch_matrix={json.dumps(batch_matrix, separators=(',', ':'))}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
