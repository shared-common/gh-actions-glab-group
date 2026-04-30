from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from _common import config_path, load_json_file, require_secret, validate_ref_name


@dataclass(frozen=True)
class BranchSpec:
    label: str
    name_env: str
    target_name: str
    protected: bool


@dataclass(frozen=True)
class BranchPolicy:
    prefix: str
    mirror_prefix: str
    mirrors: tuple[BranchSpec, ...]
    rev: BranchSpec

    @property
    def default_branch(self) -> str:
        return self.mirrors[0].target_name

    def prefixed_branch(self, source_branch: str) -> str:
        validate_ref_name(source_branch, "source branch")
        target_name = f"{self.mirror_prefix}/{self.prefix}/{source_branch}"
        validate_ref_name(target_name, "prefixed target branch")
        return target_name


def _require_dict(value: object, label: str) -> dict:
    if not isinstance(value, dict):
        raise SystemExit(f"{label} must be an object")
    return value


def _require_list(value: object, label: str) -> list:
    if not isinstance(value, list):
        raise SystemExit(f"{label} must be a list")
    return value


def _require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"{label} must be a non-empty string")
    return value.strip()


def _load_target_branch(name_env: str, prefix: str, mirror_prefix: str) -> str:
    branch_name = require_secret(name_env)
    validate_ref_name(branch_name, name_env)
    target_name = f"{mirror_prefix}/{prefix}/{branch_name}"
    validate_ref_name(target_name, f"{name_env} target ref")
    return target_name


def _branch_label(name_env: str) -> str:
    suffix = name_env.removeprefix("GIT_BRANCH_").strip().lower()
    if not suffix:
        raise SystemExit(f"Unable to derive branch label from {name_env}")
    return suffix


def load_branch_policy(path: str | None = None) -> BranchPolicy:
    policy_path = path or config_path("branch-policy.json")
    policy = _require_dict(load_json_file(policy_path, "branch policy"), "branch policy")

    mirror_prefix = _require_string(policy.get("mirrorPrefix"), "mirrorPrefix")
    validate_ref_name(mirror_prefix, "mirrorPrefix")
    prefix_env = _require_string(policy.get("prefixEnv"), "prefixEnv")
    prefix = require_secret(prefix_env)
    validate_ref_name(prefix, prefix_env)

    mirrors: list[BranchSpec] = []
    seen: set[str] = set()
    for item in _require_list(policy.get("mirrors"), "mirrors"):
        spec = _require_dict(item, "mirror entry")
        name_env = _require_string(spec.get("nameEnv"), "mirror.nameEnv")
        target_name = _load_target_branch(name_env, prefix, mirror_prefix)
        if target_name in seen:
            raise SystemExit(f"Duplicate managed branch: {target_name}")
        seen.add(target_name)
        mirrors.append(
            BranchSpec(
                label=_branch_label(name_env),
                name_env=name_env,
                target_name=target_name,
                protected=bool(spec.get("protected", False)),
            )
        )
    if not mirrors:
        raise SystemExit("Branch policy must define at least one mirror branch")

    rev_spec = _require_dict(policy.get("rev"), "rev")
    rev_env = _require_string(rev_spec.get("nameEnv"), "rev.nameEnv")
    rev_target = _load_target_branch(rev_env, prefix, mirror_prefix)
    if rev_target in seen:
        raise SystemExit(f"Duplicate managed branch: {rev_target}")

    rev = BranchSpec(
        label=_branch_label(rev_env),
        name_env=rev_env,
        target_name=rev_target,
        protected=bool(rev_spec.get("protected", False)),
    )
    return BranchPolicy(
        prefix=prefix,
        mirror_prefix=mirror_prefix,
        mirrors=tuple(mirrors),
        rev=rev,
    )


def branch_names(branches: Iterable[BranchSpec]) -> list[str]:
    return [branch.target_name for branch in branches]
