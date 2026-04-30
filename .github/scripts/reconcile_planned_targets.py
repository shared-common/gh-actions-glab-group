from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from _common import require_env
from branch_policy import load_branch_policy
from glab_sync import (
    TargetSpec,
    inspect_target,
    load_gitlab_client,
    load_targets,
    reconcile_target,
    redact_target_context,
    render_reconcile_batch_summary,
    write_json,
)


def _require_batch_value(name: str, *, minimum: int, maximum: int) -> int:
    raw = require_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc
    if value < minimum or value > maximum:
        raise SystemExit(f"{name} must be between {minimum} and {maximum}")
    return value


def slice_batch(targets: list[TargetSpec], batch_index: int, batch_size: int) -> list[TargetSpec]:
    start = batch_index * batch_size
    end = start + batch_size
    return targets[start:end]


def main() -> int:
    mode = require_env("SYNC_MODE")
    batch_index = _require_batch_value("BATCH_INDEX", minimum=0, maximum=100000)
    batch_size = _require_batch_value("BATCH_SIZE", minimum=1, maximum=500)
    output_path = os.environ.get("OUTPUT_PATH", "reconcile.json")
    summary_path = os.environ.get("SUMMARY_PATH", "reconcile.md")

    policy = load_branch_policy()
    config_path = os.environ.get("TARGETS_CONFIG_PATH")
    client = load_gitlab_client(mode, path=config_path)
    all_targets = load_targets(mode, client=client, path=config_path)
    batch_targets = slice_batch(all_targets, batch_index, batch_size)
    if not batch_targets:
        raise SystemExit(f"No targets resolved for batch {batch_index}")

    reconciled: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    clean: list[str] = []

    for target in batch_targets:
        try:
            planned = inspect_target(target, policy, client)
        except SystemExit as exc:
            errors.append(
                {
                    "target_id": target.target_id,
                    "target_project_path": target.target_project_path,
                    "error": redact_target_context(str(exc) or "inspection_failed", target, client),
                }
            )
            continue
        if not planned.get("needs_reconcile"):
            clean.append(target.target_project_path)
            continue
        try:
            reconciled.append(reconcile_target(target, policy, client))
        except SystemExit as exc:
            errors.append(
                {
                    "target_id": target.target_id,
                    "target_project_path": target.target_project_path,
                    "error": redact_target_context(str(exc) or "reconcile_failed", target, client),
                }
            )

    payload = {
        "mode": mode,
        "batch_index": batch_index,
        "batch_size": batch_size,
        "inspected_count": len(batch_targets),
        "clean_count": len(clean),
        "reconciled": reconciled,
        "errors": errors,
    }
    write_json(output_path, payload)
    Path(summary_path).write_text(
        render_reconcile_batch_summary(
            mode,
            batch_index,
            len(batch_targets),
            len(clean),
            reconciled,
            errors,
        ),
        encoding="utf-8",
    )
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
