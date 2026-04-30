from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from _common import (
    ApiError,
    GitLabClient,
    create_gitlab_branch,
    delete_gitlab_branch,
    delete_gitlab_protected_branch,
    delete_gitlab_tag,
    delete_gitlab_protected_tag,
    ensure_gitlab_default_branch,
    ensure_gitlab_project,
    ensure_gitlab_protected_branch,
    ensure_gitlab_protected_tag,
    get_gitlab_branch_sha,
    get_gitlab_group_id,
    get_gitlab_project,
    get_gitlab_protected_branch,
    get_gitlab_protected_tag,
    git_askpass_env,
    gitlab_request,
    git_source_head,
    inject_basic_auth_into_url,
    list_gitlab_group_projects,
    list_gitlab_branches,
    list_gitlab_tags,
    load_json_file,
    normalized_https_base_url,
    normalize_gitlab_project_url,
    parse_gitlab_group_url,
    protected_branch_allows_sync,
    protected_tag_allows_sync,
    require_env,
    require_secret,
    run_command,
    sanitize,
    validate_project_path,
    validate_ref_name,
)
from branch_policy import BranchPolicy


@dataclass(frozen=True)
class NamedSyncSpec:
    name: str
    protected: bool
    upstream: bool


@dataclass(frozen=True)
class ManagedBranch:
    display_name: str
    source_name: str
    target_name: str
    protected: bool
    upstream: bool


@dataclass(frozen=True)
class ManagedTag:
    display_name: str
    source_name: str
    target_name: str
    protected: bool
    upstream: bool


@dataclass(frozen=True)
class GroupSpec:
    target_project_group: str
    target_mirror_group: str
    source_project_group_url: str
    source_group_base_url: str
    source_group_path: str
    git_timeout_seconds: int = 300
    branches: tuple[NamedSyncSpec, ...] = ()
    tags: tuple[NamedSyncSpec, ...] = ()
    branch_rev: str = ""


@dataclass(frozen=True)
class TargetOverrideSpec:
    target_project_path: str
    source_import: bool | None = None
    git_lfs: bool | None = None
    git_timeout_seconds: int | None = None


@dataclass(frozen=True)
class TargetSpec:
    mode: str
    target_project_path: str
    source: str
    repo_name: str
    target_mirror_path: str = ""
    source_import: bool = False
    git_lfs: bool | None = None
    git_timeout_seconds: int = 300
    branches: tuple[NamedSyncSpec, ...] = ()
    tags: tuple[NamedSyncSpec, ...] = ()
    branch_rev: str = ""

    def to_payload(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "target_project_path": self.target_project_path,
            "target_mirror_path": self.target_mirror_path,
            "source": self.source,
            "repo_name": self.repo_name,
            "source_import": self.source_import,
            "git_lfs": self.git_lfs,
            "git_timeout_seconds": self.git_timeout_seconds,
            "branch_rev": self.branch_rev,
            "branches": [item.__dict__ for item in self.branches],
            "tags": [item.__dict__ for item in self.tags],
        }

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TargetSpec":
        mode = _require_string(payload.get("mode"), "mode")
        target_project_path = _require_string(payload.get("target_project_path"), "target_project_path")
        target_mirror_path = str(payload.get("target_mirror_path") or "").strip()
        source = _require_string(payload.get("source"), "source")
        repo_name_raw = str(payload.get("repo_name") or "").strip()
        source_import = _require_optional_bool(payload.get("source_import"), "source_import") or False
        git_lfs = _require_optional_bool(payload.get("git_lfs"), "git_lfs")
        git_timeout_seconds = _require_optional_int(payload.get("git_timeout_seconds"), "git_timeout_seconds") or 300
        branch_rev = str(payload.get("branch_rev") or "").strip()
        if mode not in {"external", "internal", "group"}:
            raise SystemExit(f"Unsupported sync mode: {mode}")

        validate_project_path(target_project_path, "target_project_path")
        if target_mirror_path:
            validate_project_path(target_mirror_path, "target_mirror_path")
            if target_mirror_path.endswith(".git"):
                raise SystemExit("target_mirror_path must not include a .git suffix")
            if target_mirror_path == target_project_path:
                raise SystemExit("target_mirror_path must differ from target_project_path")
        expected_repo_name = target_project_path.rsplit("/", 1)[-1]
        repo_name = repo_name_raw or expected_repo_name
        if repo_name != expected_repo_name:
            raise SystemExit("repo_name must match the final segment of target_project_path")

        if mode in {"external", "group"}:
            source_label = "group source url" if mode == "group" else "external source url"
            normalized_source = normalize_gitlab_project_url(source, source_label)
        else:
            validate_project_path(source, "internal source path")
            if source == target_project_path:
                raise SystemExit("internal source path must differ from target_project_path")
            normalized_source = source

        if branch_rev:
            validate_ref_name(branch_rev, "branch_rev")

        branches = _load_named_sync_specs(payload.get("branches"), "branches")
        tags = _load_named_sync_specs(payload.get("tags"), "tags")
        return cls(
            mode=mode,
            target_project_path=target_project_path,
            target_mirror_path=target_mirror_path,
            source=normalized_source,
            repo_name=repo_name,
            source_import=source_import,
            git_lfs=git_lfs,
            git_timeout_seconds=git_timeout_seconds,
            branches=tuple(branches),
            tags=tuple(tags),
            branch_rev=branch_rev,
        )

    @property
    def target_id(self) -> str:
        digest = hashlib.sha256(self.target_project_path.encode("utf-8")).hexdigest()
        return f"target-{digest[:12]}"

    @property
    def source_display(self) -> str:
        return self.source

    def managed_branches(self, policy: BranchPolicy, source_default_branch: str) -> tuple[ManagedBranch, ...]:
        managed: list[ManagedBranch] = []
        seen_targets: set[str] = set()

        for branch in policy.mirrors:
            managed.append(
                _append_unique_branch(
                    seen_targets,
                    ManagedBranch(
                        display_name=branch.label,
                        source_name=source_default_branch,
                        target_name=branch.target_name,
                        protected=branch.protected,
                        upstream=True,
                    ),
                )
            )

        if self.branch_rev:
            managed.append(
                _append_unique_branch(
                    seen_targets,
                    ManagedBranch(
                        display_name=policy.rev.label,
                        source_name=self.branch_rev,
                        target_name=policy.rev.target_name,
                        protected=policy.rev.protected,
                        upstream=True,
                    ),
                )
            )

        for branch in self.branches:
            managed.append(
                _append_unique_branch(
                    seen_targets,
                    ManagedBranch(
                        display_name=f"branch {branch.name}",
                        source_name=branch.name,
                        target_name=policy.prefixed_branch(branch.name),
                        protected=branch.protected,
                        upstream=branch.upstream,
                    ),
                )
            )

        return tuple(managed)

    def managed_tags(self) -> tuple[ManagedTag, ...]:
        managed: list[ManagedTag] = []
        seen_targets: set[str] = set()
        for tag in self.tags:
            if tag.name in seen_targets:
                raise SystemExit(f"Duplicate managed tag: {tag.name}")
            seen_targets.add(tag.name)
            managed.append(
                ManagedTag(
                    display_name=f"tag {tag.name}",
                    source_name=tag.name,
                    target_name=tag.name,
                    protected=tag.protected,
                    upstream=tag.upstream,
                )
            )
        return tuple(managed)


def _require_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"{label} must be a non-empty string")
    return value.strip()


def _require_dict(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SystemExit(f"{label} must be an object")
    return value


def _require_optional_bool(value: object, label: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise SystemExit(f"{label} must be a boolean when set")
    return value


def _require_optional_int(value: object, label: str, *, minimum: int = 60, maximum: int = 7200) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise SystemExit(f"{label} must be an integer when set")
    if value < minimum or value > maximum:
        raise SystemExit(f"{label} must be between {minimum} and {maximum}")
    return value


def _require_list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise SystemExit(f"{label} must be a list")
    return value


def _load_named_sync_specs(value: object, label: str) -> list[NamedSyncSpec]:
    if value is None:
        return []
    specs: list[NamedSyncSpec] = []
    seen: set[str] = set()
    for index, item in enumerate(_require_list(value, label)):
        spec = _require_dict(item, f"{label}[{index}]")
        name = _require_string(spec.get("name"), f"{label}[{index}].name")
        validate_ref_name(name, f"{label}[{index}].name")
        if name in seen:
            raise SystemExit(f"Duplicate {label} entry: {name}")
        seen.add(name)
        protected = spec.get("protected")
        upstream = spec.get("upstream")
        if not isinstance(protected, bool):
            raise SystemExit(f"{label}[{index}].protected must be a boolean")
        if not isinstance(upstream, bool):
            raise SystemExit(f"{label}[{index}].upstream must be a boolean")
        specs.append(NamedSyncSpec(name=name, protected=protected, upstream=upstream))
    return specs


def _group_spec_from_payload(payload: dict[str, Any], label: str) -> GroupSpec:
    target_project_group = _require_string(payload.get("target_project_group"), f"{label}.target_project_group")
    target_mirror_group = str(payload.get("target_mirror_group") or "").strip()
    source_project_group_url = _require_string(
        payload.get("source_project_group_url"),
        f"{label}.source_project_group_url",
    )
    git_timeout_seconds = _require_optional_int(
        payload.get("git_timeout_seconds"),
        f"{label}.git_timeout_seconds",
    ) or 300
    branch_rev = str(payload.get("branch_rev") or "").strip()
    if branch_rev:
        validate_ref_name(branch_rev, f"{label}.branch_rev")

    validate_project_path(target_project_group, f"{label}.target_project_group")
    if target_mirror_group:
        validate_project_path(target_mirror_group, f"{label}.target_mirror_group")
        if target_mirror_group == target_project_group:
            raise SystemExit(f"{label}.target_mirror_group must differ from target_project_group")

    source_group_base_url, source_group_path = parse_gitlab_group_url(
        source_project_group_url,
        f"{label}.source_project_group_url",
    )
    branches = _load_named_sync_specs(payload.get("branches"), f"{label}.branches")
    tags = _load_named_sync_specs(payload.get("tags"), f"{label}.tags")
    return GroupSpec(
        target_project_group=target_project_group,
        target_mirror_group=target_mirror_group,
        source_project_group_url=source_project_group_url,
        source_group_base_url=source_group_base_url,
        source_group_path=source_group_path,
        git_timeout_seconds=git_timeout_seconds,
        branches=tuple(branches),
        tags=tuple(tags),
        branch_rev=branch_rev,
    )


def _group_relative_project_path(group: GroupSpec, project: dict[str, Any], label: str) -> tuple[str, str]:
    source_project_path = _require_string(project.get("path_with_namespace"), f"{label}.path_with_namespace")
    prefix = f"{group.source_group_path}/"
    if not source_project_path.startswith(prefix):
        raise SystemExit(
            f"{label}.path_with_namespace must stay within source group {group.source_group_path}: {source_project_path}"
        )
    relative_path = source_project_path[len(prefix) :]
    validate_project_path(relative_path, f"{label}.relative_path", min_segments=1)
    source_url_raw = str(project.get("http_url_to_repo") or "").strip()
    source_url = (
        normalize_gitlab_project_url(source_url_raw, f"{label}.http_url_to_repo")
        if source_url_raw
        else normalize_gitlab_project_url(
            f"{group.source_group_base_url}/{source_project_path}.git",
            f"{label}.fallback_http_url_to_repo",
        )
    )
    return relative_path, source_url


def _expand_group_targets(group: GroupSpec, client: GitLabClient, label: str) -> list[TargetSpec]:
    client_base_url = normalized_https_base_url(client.base_url, "GL_BASE_URL")
    if group.source_group_base_url != client_base_url:
        raise SystemExit(
            f"{label}.source_project_group_url must use {client_base_url}: {group.source_project_group_url}"
        )

    expanded: list[TargetSpec] = []
    seen_relative_paths: set[str] = set()
    seen_target_paths: set[str] = set()
    projects = sorted(
        list_gitlab_group_projects(client, group.source_group_path, include_subgroups=True),
        key=lambda item: str(item.get("path_with_namespace") or ""),
    )

    for project_index, project in enumerate(projects):
        project_label = f"{label}.discovered[{project_index}]"
        relative_path, source_url = _group_relative_project_path(group, project, project_label)
        if relative_path in seen_relative_paths:
            raise SystemExit(f"Duplicate discovered project path in {label}: {relative_path}")
        seen_relative_paths.add(relative_path)

        target_project_path = f"{group.target_project_group}/{relative_path}"
        if target_project_path in seen_target_paths:
            raise SystemExit(f"Duplicate target project path discovered in {label}: {target_project_path}")
        seen_target_paths.add(target_project_path)

        target_mirror_path = ""
        if group.target_mirror_group:
            target_mirror_path = f"{group.target_mirror_group}/{relative_path}"

        expanded.append(
            TargetSpec(
                mode="group",
                target_project_path=target_project_path,
                target_mirror_path=target_mirror_path,
                source=source_url,
                repo_name=target_project_path.rsplit("/", 1)[-1],
                source_import=False,
                git_lfs=None,
                git_timeout_seconds=group.git_timeout_seconds,
                branches=group.branches,
                tags=group.tags,
                branch_rev=group.branch_rev,
            )
        )
    return expanded


def _target_override_from_payload(payload: dict[str, Any], label: str) -> TargetOverrideSpec:
    target_project_path = _require_string(payload.get("target_project_path"), f"{label}.target_project_path")
    validate_project_path(target_project_path, f"{label}.target_project_path")
    source_import = _require_optional_bool(payload.get("source_import"), f"{label}.source_import")
    git_lfs = _require_optional_bool(payload.get("git_lfs"), f"{label}.git_lfs")
    git_timeout_seconds = _require_optional_int(
        payload.get("git_timeout_seconds"),
        f"{label}.git_timeout_seconds",
    )
    if source_import is None and git_lfs is None and git_timeout_seconds is None:
        raise SystemExit(f"{label} must set at least one override field")
    return TargetOverrideSpec(
        target_project_path=target_project_path,
        source_import=source_import,
        git_lfs=git_lfs,
        git_timeout_seconds=git_timeout_seconds,
    )


def load_target_overrides(path: str, label: str) -> dict[str, TargetOverrideSpec]:
    payload = _require_dict(load_json_file(path, label), label)
    version = payload.get("version")
    if version != 1:
        raise SystemExit(f"{label} must set version to 1")
    items = _require_list(payload.get("targets"), f"{label}.targets")
    overrides: dict[str, TargetOverrideSpec] = {}
    for index, item in enumerate(items):
        override = _target_override_from_payload(
            _require_dict(item, f"{label}.targets[{index}]"),
            f"{label}.targets[{index}]",
        )
        if override.target_project_path in overrides:
            raise SystemExit(f"Duplicate target override path in {label}: {override.target_project_path}")
        overrides[override.target_project_path] = override
    return overrides


def _apply_target_override(target: TargetSpec, override: TargetOverrideSpec | None) -> TargetSpec:
    if override is None:
        return target
    return TargetSpec(
        mode=target.mode,
        target_project_path=target.target_project_path,
        target_mirror_path=target.target_mirror_path,
        source=target.source,
        repo_name=target.repo_name,
        source_import=override.source_import if override.source_import is not None else target.source_import,
        git_lfs=override.git_lfs if override.git_lfs is not None else target.git_lfs,
        git_timeout_seconds=(
            override.git_timeout_seconds if override.git_timeout_seconds is not None else target.git_timeout_seconds
        ),
        branches=target.branches,
        tags=target.tags,
        branch_rev=target.branch_rev,
    )


def _append_unique_branch(seen_targets: set[str], branch: ManagedBranch) -> ManagedBranch:
    if branch.target_name in seen_targets:
        raise SystemExit(f"Duplicate managed branch: {branch.target_name}")
    seen_targets.add(branch.target_name)
    return branch


def redact_target_context(message: str, target: TargetSpec, client: GitLabClient | None = None) -> str:
    redacted = message
    candidates = {
        target.target_project_path,
        target.target_project_path.rsplit("/", 1)[0],
        target.source,
    }
    if "/" in target.source:
        candidates.add(target.source.rsplit("/", 1)[0])
    if target.mode in {"external", "group"} and target.source.endswith(".git"):
        candidates.add(target.source[:-4])
    if client is not None:
        candidates.update(
            {
                client.project_git_url(target.target_project_path),
                client.project_web_url(target.target_project_path),
            }
        )
        if target.mode == "internal":
            candidates.update(
                {
                    client.project_git_url(target.source),
                    client.project_web_url(target.source),
                }
            )
    for value in sorted((item for item in candidates if item), key=len, reverse=True):
        redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def load_gitlab_client(mode: str, *, path: str | None = None) -> GitLabClient:
    if mode not in {"external", "internal", "group"}:
        raise SystemExit(f"Unsupported sync mode: {mode}")
    return GitLabClient(
        base_url=require_secret("GL_BASE_URL"),
        username=require_secret("GL_BRIDGE_FORK_USER_GLAB"),
        token=require_secret("GL_PAT_FORK_GLAB_SVC"),
    )


def load_mirror_target_client(*, path: str | None = None) -> GitLabClient:
    return GitLabClient(
        base_url=require_secret("GL_BASE_URL"),
        username=require_secret("GL_USER_FORK_MIRROR_SVC"),
        token=require_secret("GL_PAT_FORK_MIRROR_SVC"),
    )


def load_targets(
    mode: str,
    *,
    client: GitLabClient | None = None,
    path: str | None = None,
    project_path: str | None = None,
) -> list[TargetSpec]:
    if mode != "group":
        raise SystemExit(f"Unsupported sync mode: {mode}")

    config_path = path or require_env("TARGETS_CONFIG_PATH")
    override_path = project_path or os.environ.get("TARGET_PROJECTS_CONFIG_PATH", "").strip()
    discovery_client = client or load_gitlab_client(mode, path=config_path)
    label = "group targets config"
    payload = _require_dict(load_json_file(config_path, label), label)
    version = payload.get("version")
    if version != 1:
        raise SystemExit(f"{label} must set version to 1")
    overrides = (
        load_target_overrides(override_path, "project targets config")
        if override_path
        else {}
    )

    group_payloads = _require_list(payload.get("targets"), f"{label}.targets")
    if not group_payloads:
        raise SystemExit(f"{label}.targets must contain at least one target")

    targets: list[TargetSpec] = []
    seen_target_paths: set[str] = set()
    applied_overrides: set[str] = set()
    for index, item in enumerate(group_payloads):
        entry = _require_dict(item, f"{label}.targets[{index}]")
        group = _group_spec_from_payload(entry, f"{label}.targets[{index}]")
        for target in _expand_group_targets(group, discovery_client, f"{label}.targets[{index}]"):
            if target.target_project_path in seen_target_paths:
                raise SystemExit(f"Duplicate target project path in {label}: {target.target_project_path}")
            seen_target_paths.add(target.target_project_path)
            override = overrides.get(target.target_project_path)
            if override is not None:
                applied_overrides.add(target.target_project_path)
            targets.append(_apply_target_override(target, override))

    missing_overrides = sorted(set(overrides) - applied_overrides)
    if missing_overrides:
        raise SystemExit(f"project targets config contains unknown target projects: {', '.join(missing_overrides)}")

    if not targets:
        raise SystemExit(f"{label} resolved no source projects")

    return sorted(targets, key=lambda item: item.target_project_path)


def build_source_git_url(target: TargetSpec, client: GitLabClient) -> str:
    if target.mode in {"external", "group"}:
        return target.source
    return client.project_git_url(target.source)


def _wait_for_project_import(
    client: GitLabClient,
    project_path: str,
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_status = "unavailable"
    while time.monotonic() < deadline:
        project = get_gitlab_project(client, project_path)
        if isinstance(project, dict):
            status = str(project.get("import_status") or "").strip().lower()
            if status == "finished":
                return project
            if status == "failed":
                import_error = str(project.get("import_error") or "unknown import failure").strip()
                raise SystemExit(f"GitLab project import failed for {project_path}: {import_error}")
            last_status = status or "unavailable"
        time.sleep(2)
    raise SystemExit(
        f"Timed out waiting for GitLab project import for {project_path} (last import_status: {last_status})"
    )


def _build_project_import_url(client: GitLabClient, target: TargetSpec) -> str:
    if target.mode == "internal":
        source_project = get_gitlab_project(client, target.source)
        if source_project is None:
            raise SystemExit(f"Source project not found or not accessible: {target.source}")
        return inject_basic_auth_into_url(
            client.project_git_url(target.source),
            client.username,
            client.token,
            "internal source import url",
        )
    if target.mode == "group":
        return inject_basic_auth_into_url(
            target.source,
            client.username,
            client.token,
            "group source import url",
        )
    return target.source


def _import_target_project(
    client: GitLabClient,
    *,
    target: TargetSpec,
    target_project_path: str,
    timeout_seconds: int,
) -> tuple[dict[str, Any], bool]:
    existing = get_gitlab_project(client, target_project_path)
    if existing is not None:
        status = str(existing.get("import_status") or "").strip().lower()
        if status == "finished":
            return existing, False
        if status and status != "none":
            return _wait_for_project_import(client, target_project_path, timeout_seconds=timeout_seconds), False

    import_url = _build_project_import_url(client, target)

    if existing is None:
        group_path, project_name = target_project_path.rsplit("/", 1)
        payload = {
            "import_url": import_url,
            "name": project_name,
            "namespace_id": get_gitlab_group_id(client, group_path),
            "path": project_name,
            "shared_runners_enabled": False,
            "visibility": "private",
        }
        try:
            created = gitlab_request(client, "POST", "/projects", payload)
        except ApiError as exc:
            message = str(exc).lower()
            if exc.status in {400, 409, 422} and (
                "already exists" in message
                or "has already been taken" in message
                or "path has already been taken" in message
                or "name has already been taken" in message
            ):
                return _wait_for_project_import(client, target_project_path, timeout_seconds=timeout_seconds), False
            raise SystemExit(f"Unable to start source import from {target.source}: {exc}") from exc
        if created is not None and not isinstance(created, dict):
            raise SystemExit("GitLab project create returned an invalid response")
        return _wait_for_project_import(client, target_project_path, timeout_seconds=timeout_seconds), True

    project_id = int(existing["id"])
    payload = {
        "import_url": import_url,
        "shared_runners_enabled": False,
    }

    try:
        updated = gitlab_request(client, "PUT", f"/projects/{project_id}", payload)
    except ApiError as exc:
        raise SystemExit(f"Unable to start source import from {target.source}: {exc}") from exc

    if updated is not None and not isinstance(updated, dict):
        raise SystemExit("GitLab project update returned an invalid response")
    return _wait_for_project_import(client, target_project_path, timeout_seconds=timeout_seconds), True


def git_remote_ref_sha(
    remote_url: str,
    ref_namespace: str,
    ref_name: str,
    *,
    secrets: tuple[str, ...] = (),
    env_overrides: dict[str, str] | None = None,
) -> str | None:
    validate_ref_name(ref_name, f"{ref_namespace} ref")
    proc = run_command(
        ["git", "ls-remote", remote_url, f"refs/{ref_namespace}/{ref_name}"],
        secrets=secrets,
        timeout=120,
        env_overrides=env_overrides,
    )
    for line in proc.stdout.splitlines():
        parts = line.strip().split()
        if len(parts) == 2 and parts[1] == f"refs/{ref_namespace}/{ref_name}":
            return parts[0]
    return None


def _unmanaged_ref_names(
    client: GitLabClient,
    *,
    project_id: int,
    branches: tuple[ManagedBranch, ...],
    tags: tuple[ManagedTag, ...],
) -> tuple[list[str], list[str]]:
    keep_branches = {branch.target_name for branch in branches}
    keep_tags = {tag.target_name for tag in tags}
    unmanaged_branches: list[str] = []
    unmanaged_tags: list[str] = []

    for item in list_gitlab_branches(client, project_id):
        branch_name = str(item.get("name") or "").strip()
        if not branch_name or branch_name in keep_branches:
            continue
        unmanaged_branches.append(branch_name)

    for item in list_gitlab_tags(client, project_id):
        tag_name = str(item.get("name") or "").strip()
        if not tag_name or tag_name in keep_tags:
            continue
        unmanaged_tags.append(tag_name)
    return unmanaged_branches, unmanaged_tags


def _sync_target_refs(
    target: TargetSpec,
    *,
    branches: tuple[ManagedBranch, ...],
    tags: tuple[ManagedTag, ...],
    source_url: str,
    source_default_branch: str,
    source_sha: str,
    target_url: str,
    project_id: int,
    client: GitLabClient,
    source_env: dict[str, str] | None,
    git_env: dict[str, str],
    results: dict[str, list[str]],
    secrets: tuple[str, ...],
) -> None:
    branch_existing: dict[str, str | None] = {
        branch.target_name: get_gitlab_branch_sha(client, project_id, branch.target_name) for branch in branches
    }
    branch_source: dict[str, str | None] = {source_default_branch: source_sha}
    for branch in branches:
        if branch.upstream or branch_existing[branch.target_name] is None:
            if branch.source_name not in branch_source:
                branch_source[branch.source_name] = git_remote_ref_sha(
                    source_url,
                    "heads",
                    branch.source_name,
                    secrets=secrets,
                    env_overrides=source_env,
                )

    tag_existing: dict[str, str | None] = {
        tag.target_name: git_remote_ref_sha(
            target_url,
            "tags",
            tag.target_name,
            secrets=secrets,
            env_overrides=git_env,
        )
        for tag in tags
    }
    tag_source: dict[str, str | None] = {}
    for tag in tags:
        if tag.upstream or tag_existing[tag.target_name] is None:
            if tag.source_name not in tag_source:
                tag_source[tag.source_name] = git_remote_ref_sha(
                    source_url,
                    "tags",
                    tag.source_name,
                    secrets=secrets,
                    env_overrides=source_env,
                )

    with tempfile.TemporaryDirectory() as repo_dir:
        repo_path = str(Path(repo_dir) / "repo.git")
        run_command(["git", "init", "--bare", repo_path], secrets=secrets, timeout=120, env_overrides=git_env)
        run_command(
            ["git", "-C", repo_path, "remote", "add", "source", source_url],
            secrets=secrets,
            timeout=120,
            env_overrides=git_env,
        )
        run_command(
            ["git", "-C", repo_path, "remote", "add", "target", target_url],
            secrets=secrets,
            timeout=120,
            env_overrides=git_env,
        )
        fetched_branch_sources: set[str] = set()
        fetched_tag_sources: set[str] = set()

        for branch in branches:
            needs_source = branch.upstream or branch_existing[branch.target_name] is None
            if (
                needs_source
                and branch_source.get(branch.source_name) is not None
                and branch.source_name not in fetched_branch_sources
            ):
                _fetch_source_ref(
                    repo_path,
                    "source",
                    "heads",
                    branch.source_name,
                    timeout_seconds=target.git_timeout_seconds,
                    secrets=secrets,
                    env_overrides=source_env,
                )
                fetched_branch_sources.add(branch.source_name)

        for tag in tags:
            needs_source = tag.upstream or tag_existing[tag.target_name] is None
            if (
                needs_source
                and tag_source.get(tag.source_name) is not None
                and tag.source_name not in fetched_tag_sources
            ):
                _fetch_source_ref(
                    repo_path,
                    "source",
                    "tags",
                    tag.source_name,
                    timeout_seconds=target.git_timeout_seconds,
                    secrets=secrets,
                    env_overrides=source_env,
                )
                fetched_tag_sources.add(tag.source_name)
        fetched_refs = tuple(
            sorted(
                [f"refs/heads/{name}" for name in fetched_branch_sources]
                + [f"refs/tags/{name}" for name in fetched_tag_sources]
            )
        )
        git_lfs_enabled = _target_uses_git_lfs(
            target,
            repo_path,
            fetched_refs,
            secrets=secrets,
            env_overrides=git_env,
        )
        if git_lfs_enabled:
            run_command(
                ["git", "-C", repo_path, "lfs", "install", "--local"],
                secrets=secrets,
                timeout=120,
                env_overrides=git_env,
            )

        for branch in branches:
            _sync_branch(
                branch,
                target=target,
                repo_path=repo_path,
                source_url=source_url,
                target_url=target_url,
                project_id=project_id,
                client=client,
                existing_sha=branch_existing[branch.target_name],
                source_sha=branch_source.get(branch.source_name),
                git_lfs_enabled=git_lfs_enabled,
                git_timeout_seconds=target.git_timeout_seconds,
                secrets=secrets,
                git_env=git_env,
                results=results,
            )

        for tag in tags:
            _sync_tag(
                tag,
                repo_path=repo_path,
                source_url=source_url,
                target_url=target_url,
                project_id=project_id,
                client=client,
                existing_sha=tag_existing[tag.target_name],
                source_sha=tag_source.get(tag.source_name),
                git_lfs_enabled=git_lfs_enabled,
                git_timeout_seconds=target.git_timeout_seconds,
                secrets=secrets,
                git_env=git_env,
                results=results,
            )


def _prune_imported_refs(
    client: GitLabClient,
    *,
    project_id: int,
    branches: tuple[ManagedBranch, ...],
    tags: tuple[ManagedTag, ...],
    results: dict[str, list[str]],
) -> None:
    unmanaged_branches, unmanaged_tags = _unmanaged_ref_names(
        client,
        project_id=project_id,
        branches=branches,
        tags=tags,
    )
    for branch_name in unmanaged_branches:
        delete_gitlab_protected_branch(client, project_id, branch_name)
        if delete_gitlab_branch(client, project_id, branch_name):
            results["pruned"].append(f"branch:{branch_name}")

    for tag_name in unmanaged_tags:
        delete_gitlab_protected_tag(client, project_id, tag_name)
        if delete_gitlab_tag(client, project_id, tag_name):
            results["pruned"].append(f"tag:{tag_name}")


def _desired_branch_protection(branch: ManagedBranch, current: dict[str, Any] | None) -> tuple[bool, str | None]:
    if branch.protected:
        if not protected_branch_allows_sync(current):
            return False, "protection_missing"
        return True, None
    if current is not None:
        return False, "protection_present"
    return True, None


def _desired_tag_protection(tag: ManagedTag, current: dict[str, Any] | None) -> tuple[bool, str | None]:
    if tag.protected:
        if not protected_tag_allows_sync(current):
            return False, "protection_missing"
        return True, None
    if current is not None:
        return False, "protection_present"
    return True, None


def inspect_target(target: TargetSpec, policy: BranchPolicy, client: GitLabClient) -> dict[str, Any]:
    source_url = build_source_git_url(target, client)
    with git_askpass_env(client) as git_env:
        source_env = git_env if target.mode == "internal" else None
        source_default_branch, source_sha = git_source_head(
            source_url,
            secrets=(client.token, client.username),
            env_overrides=source_env,
        )
        branches = target.managed_branches(policy, source_default_branch)
        tags = target.managed_tags()

        reasons: list[str] = []
        branch_state: dict[str, dict[str, Any]] = {}
        tag_state: dict[str, dict[str, Any]] = {}
        project = get_gitlab_project(client, target.target_project_path)
        project_id = int(project["id"]) if isinstance(project, dict) and project.get("id") else None

        if project is None:
            reasons.append("project_missing")
        else:
            target_url = client.project_git_url(target.target_project_path)
            branch_source_shas: dict[str, str | None] = {source_default_branch: source_sha}

            for branch in branches:
                current_sha = get_gitlab_branch_sha(client, project_id, branch.target_name)
                branch_reasons: list[str] = []
                source_branch_sha = branch_source_shas.get(branch.source_name)
                if source_branch_sha is None and (branch.upstream or current_sha is None):
                    source_branch_sha = git_remote_ref_sha(
                        source_url,
                        "heads",
                        branch.source_name,
                        secrets=(client.token, client.username),
                        env_overrides=source_env,
                    )
                    branch_source_shas[branch.source_name] = source_branch_sha
                if current_sha is None:
                    branch_reasons.append("missing")
                    reasons.append(f"branch_missing:{branch.target_name}")
                elif branch.upstream and source_branch_sha is not None and current_sha != source_branch_sha:
                    branch_reasons.append("sha_diverged")
                    reasons.append(f"sha_diverged:{branch.target_name}")
                if (branch.upstream or current_sha is None) and source_branch_sha is None:
                    branch_reasons.append("source_missing")
                    reasons.append(f"source_missing:{branch.target_name}")
                current_protected = get_gitlab_protected_branch(client, project_id, branch.target_name)
                _, protection_reason = _desired_branch_protection(branch, current_protected)
                if protection_reason:
                    branch_reasons.append(protection_reason)
                    reasons.append(f"{protection_reason}:{branch.target_name}")
                branch_state[branch.target_name] = {
                    "label": branch.display_name,
                    "source": branch.source_name,
                    "sha": current_sha,
                    "upstream": branch.upstream,
                    "protected": branch.protected,
                    "reasons": branch_reasons,
                }

            for tag in tags:
                current_sha = git_remote_ref_sha(
                    target_url,
                    "tags",
                    tag.target_name,
                    secrets=(client.token, client.username),
                    env_overrides=git_env,
                )
                tag_reasons: list[str] = []
                source_tag_sha: str | None = None
                if tag.upstream or current_sha is None:
                    source_tag_sha = git_remote_ref_sha(
                        source_url,
                        "tags",
                        tag.source_name,
                        secrets=(client.token, client.username),
                        env_overrides=source_env,
                    )
                if current_sha is None:
                    tag_reasons.append("missing")
                    reasons.append(f"tag_missing:{tag.target_name}")
                elif tag.upstream and source_tag_sha is not None and current_sha != source_tag_sha:
                    tag_reasons.append("sha_diverged")
                    reasons.append(f"tag_diverged:{tag.target_name}")
                if (tag.upstream or current_sha is None) and source_tag_sha is None:
                    tag_reasons.append("source_missing")
                    reasons.append(f"source_missing:{tag.target_name}")
                current_protected = get_gitlab_protected_tag(client, project_id, tag.target_name)
                _, protection_reason = _desired_tag_protection(tag, current_protected)
                if protection_reason:
                    tag_reasons.append(protection_reason)
                    reasons.append(f"{protection_reason}:{tag.target_name}")
                tag_state[tag.target_name] = {
                    "label": tag.display_name,
                    "source": tag.source_name,
                    "sha": current_sha,
                    "upstream": tag.upstream,
                    "protected": tag.protected,
                    "reasons": tag_reasons,
                }

            if str(project.get("default_branch") or "") != policy.default_branch:
                reasons.append(f"default_branch_mismatch:{policy.default_branch}")
            project_import_status = str(project.get("import_status") or "").strip().lower()
            if target.source_import and project_import_status != "finished":
                reasons.append("source_import_pending")
            if not target.source_import and project_import_status == "finished":
                unmanaged_branches, unmanaged_tags = _unmanaged_ref_names(
                    client,
                    project_id=project_id,
                    branches=branches,
                    tags=tags,
                )
                if unmanaged_branches:
                    reasons.append("unmanaged_branches_present")
                if unmanaged_tags:
                    reasons.append("unmanaged_tags_present")

    return {
        "mode": target.mode,
        "target_id": target.target_id,
        "repo_name": target.repo_name,
        "target_project_path": target.target_project_path,
        "source": target.source_display,
        "source_default_branch": source_default_branch,
        "source_sha": source_sha,
        "target_exists": project is not None,
        "project_id": project_id,
        "needs_reconcile": bool(reasons),
        "reasons": reasons,
        "branches": branch_state,
        "tags": tag_state,
        "branch_rev": target.branch_rev,
    }


def _fetch_source_ref(
    repo_path: str,
    remote_name: str,
    ref_namespace: str,
    ref_name: str,
    *,
    timeout_seconds: int,
    secrets: tuple[str, ...],
    env_overrides: dict[str, str] | None,
) -> None:
    run_command(
        [
            "git",
            "-C",
            repo_path,
            "fetch",
            "--force",
            remote_name,
            f"refs/{ref_namespace}/{ref_name}:refs/{ref_namespace}/{ref_name}",
        ],
        secrets=secrets,
        timeout=timeout_seconds,
        env_overrides=env_overrides,
    )


def _ref_declares_git_lfs(
    repo_path: str,
    ref_name: str,
    *,
    secrets: tuple[str, ...],
    env_overrides: dict[str, str] | None,
) -> bool:
    proc = run_command(
        ["git", "-C", repo_path, "ls-tree", "-r", "--name-only", ref_name],
        secrets=secrets,
        timeout=300,
        env_overrides=env_overrides,
    )
    candidate_paths = [
        path
        for path in (line.strip() for line in proc.stdout.splitlines())
        if path == ".gitattributes"
        or path.endswith("/.gitattributes")
        or path == ".lfsconfig"
        or path.endswith("/.lfsconfig")
    ]
    for path in candidate_paths:
        contents = run_command(
            ["git", "-C", repo_path, "show", f"{ref_name}:{path}"],
            secrets=secrets,
            timeout=300,
            env_overrides=env_overrides,
        ).stdout
        if "filter=lfs" in contents or "lfs.url" in contents or "lfs.pushurl" in contents:
            return True
    return False


def _target_uses_git_lfs(
    target: TargetSpec,
    repo_path: str,
    fetched_refs: tuple[str, ...],
    *,
    secrets: tuple[str, ...],
    env_overrides: dict[str, str] | None,
) -> bool:
    if target.git_lfs is not None:
        return target.git_lfs
    for ref_name in fetched_refs:
        if _ref_declares_git_lfs(
            repo_path,
            ref_name,
            secrets=secrets,
            env_overrides=env_overrides,
        ):
            return True
    return False


def _push_ref(
    repo_path: str,
    source_url: str,
    target_url: str,
    source_ref: str,
    target_ref: str,
    *,
    ref_namespace: str,
    source_remote: str,
    target_remote: str,
    expected_remote_sha: str | None,
    allow_existing: bool = False,
    git_lfs_enabled: bool = False,
    timeout_seconds: int = 300,
    secrets: tuple[str, ...] = (),
    env_overrides: dict[str, str] | None = None,
) -> str:
    import subprocess

    def run_push(command: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
        command_text = sanitize(" ".join(command), secrets)
        try:
            return subprocess.run(
                command,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=timeout_seconds,
                env=env,
            )
        except subprocess.TimeoutExpired as exc:
            raise SystemExit(f"Command timed out after {timeout_seconds}s: {command_text}") from exc

    source_refspec = f"refs/{ref_namespace}/{source_ref}"
    target_refspec = f"refs/{ref_namespace}/{target_ref}"
    if git_lfs_enabled:
        run_command(
            ["git", "-C", repo_path, "lfs", "fetch", source_remote, source_refspec],
            secrets=secrets,
            timeout=timeout_seconds,
            env_overrides=env_overrides,
        )
        run_command(
            ["git", "-C", repo_path, "lfs", "push", target_remote, source_refspec],
            secrets=secrets,
            timeout=timeout_seconds,
            env_overrides=env_overrides,
        )

    command = [
        "git",
        "-C",
        repo_path,
        "push",
        target_url,
        f"{source_refspec}:{target_refspec}",
    ]
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    if env_overrides:
        env.update(env_overrides)
    if git_lfs_enabled:
        env["GIT_LFS_SKIP_PUSH"] = "1"
    push_proc = run_push(command, env)
    if push_proc.returncode == 0:
        return "updated"

    stderr_text = sanitize(push_proc.stderr.strip(), secrets)
    stderr = stderr_text.lower()
    if allow_existing and "already exists" in stderr:
        return "skipped"
    if any(pattern in stderr for pattern in ("non-fast-forward", "[rejected]", "fetch first", "stale info")):
        lease = target_refspec
        if expected_remote_sha:
            lease = f"{lease}:{expected_remote_sha.lower()}"
        force_command = [
            "git",
            "-C",
            repo_path,
            "push",
            f"--force-with-lease={lease}",
            target_url,
            f"{source_refspec}:{target_refspec}",
        ]
        force_proc = run_push(force_command, env)
        if force_proc.returncode == 0:
            return "updated"
        raise SystemExit(sanitize(force_proc.stderr.strip(), secrets))

    raise SystemExit(stderr_text)


def _sync_branch(
    branch: ManagedBranch,
    *,
    target: TargetSpec,
    repo_path: str,
    source_url: str,
    target_url: str,
    project_id: int,
    client: GitLabClient,
    existing_sha: str | None,
    source_sha: str | None,
    git_lfs_enabled: bool,
    git_timeout_seconds: int,
    secrets: tuple[str, ...],
    git_env: dict[str, str],
    results: dict[str, list[str]],
) -> None:
    needs_source = branch.upstream or existing_sha is None
    if needs_source and source_sha is None:
        results["skipped"].append(f"{branch.target_name} (source missing: {branch.source_name})")
        if existing_sha is not None:
            if branch.protected:
                if ensure_gitlab_protected_branch(client, project_id, branch.target_name):
                    results["protected"].append(branch.target_name)
            else:
                if delete_gitlab_protected_branch(client, project_id, branch.target_name):
                    results["unprotected"].append(branch.target_name)
        return

    if existing_sha is None:
        outcome = None
        if target.source_import and source_sha is not None:
            imported_source_sha = get_gitlab_branch_sha(client, project_id, branch.source_name)
            if imported_source_sha == source_sha:
                created = create_gitlab_branch(client, project_id, branch.target_name, branch.source_name)
                outcome = "updated" if created else "skipped"
        if outcome is None:
            outcome = _push_ref(
                repo_path,
                source_url,
                target_url,
                branch.source_name,
                branch.target_name,
                ref_namespace="heads",
                source_remote="source",
                target_remote="target",
                expected_remote_sha=None,
                allow_existing=True,
                git_lfs_enabled=git_lfs_enabled,
                timeout_seconds=git_timeout_seconds,
                secrets=secrets,
                env_overrides=git_env,
            )
        results["created" if outcome != "skipped" else "skipped"].append(branch.target_name)
    elif branch.upstream:
        if source_sha is not None and existing_sha == source_sha:
            results["skipped"].append(branch.target_name)
        else:
            outcome = _push_ref(
                repo_path,
                source_url,
                target_url,
                branch.source_name,
                branch.target_name,
                ref_namespace="heads",
                source_remote="source",
                target_remote="target",
                expected_remote_sha=existing_sha,
                git_lfs_enabled=git_lfs_enabled,
                timeout_seconds=git_timeout_seconds,
                secrets=secrets,
                env_overrides=git_env,
            )
            results["updated" if outcome != "skipped" else "skipped"].append(branch.target_name)
    else:
        results["skipped"].append(branch.target_name)

    if branch.protected:
        if ensure_gitlab_protected_branch(client, project_id, branch.target_name):
            results["protected"].append(branch.target_name)
    else:
        if delete_gitlab_protected_branch(client, project_id, branch.target_name):
            results["unprotected"].append(branch.target_name)


def _sync_tag(
    tag: ManagedTag,
    *,
    repo_path: str,
    source_url: str,
    target_url: str,
    project_id: int,
    client: GitLabClient,
    existing_sha: str | None,
    source_sha: str | None,
    git_lfs_enabled: bool,
    git_timeout_seconds: int,
    secrets: tuple[str, ...],
    git_env: dict[str, str],
    results: dict[str, list[str]],
) -> None:
    needs_source = tag.upstream or existing_sha is None
    if needs_source and source_sha is None:
        results["skipped"].append(f"tag:{tag.target_name} (source missing: {tag.source_name})")
        if existing_sha is not None:
            if tag.protected:
                if ensure_gitlab_protected_tag(client, project_id, tag.target_name):
                    results["protected"].append(f"tag:{tag.target_name}")
            else:
                if delete_gitlab_protected_tag(client, project_id, tag.target_name):
                    results["unprotected"].append(f"tag:{tag.target_name}")
        return

    if existing_sha is None:
        outcome = _push_ref(
            repo_path,
            source_url,
            target_url,
            tag.source_name,
            tag.target_name,
            ref_namespace="tags",
            source_remote="source",
            target_remote="target",
            expected_remote_sha=None,
            allow_existing=True,
            git_lfs_enabled=git_lfs_enabled,
            timeout_seconds=git_timeout_seconds,
            secrets=secrets,
            env_overrides=git_env,
        )
        results["created" if outcome != "skipped" else "skipped"].append(f"tag:{tag.target_name}")
    elif tag.upstream:
        if source_sha is not None and existing_sha == source_sha:
            results["skipped"].append(f"tag:{tag.target_name}")
        else:
            outcome = _push_ref(
                repo_path,
                source_url,
                target_url,
                tag.source_name,
                tag.target_name,
                ref_namespace="tags",
                source_remote="source",
                target_remote="target",
                expected_remote_sha=existing_sha,
                git_lfs_enabled=git_lfs_enabled,
                timeout_seconds=git_timeout_seconds,
                secrets=secrets,
                env_overrides=git_env,
            )
            results["updated" if outcome != "skipped" else "skipped"].append(f"tag:{tag.target_name}")
    else:
        results["skipped"].append(f"tag:{tag.target_name}")

    if tag.protected:
        if ensure_gitlab_protected_tag(client, project_id, tag.target_name):
            results["protected"].append(f"tag:{tag.target_name}")
    else:
        if delete_gitlab_protected_tag(client, project_id, tag.target_name):
            results["unprotected"].append(f"tag:{tag.target_name}")


def reconcile_target(target: TargetSpec, policy: BranchPolicy, client: GitLabClient) -> dict[str, Any]:
    source_url = build_source_git_url(target, client)
    with git_askpass_env(client) as git_env:
        source_env = git_env if target.mode == "internal" else None
        source_default_branch, source_sha = git_source_head(
            source_url,
            secrets=(client.token, client.username),
            env_overrides=source_env,
        )
        branches = target.managed_branches(policy, source_default_branch)
        tags = target.managed_tags()

        if target.source_import:
            existing_before = get_gitlab_project(client, target.target_project_path)
            status_before = str(existing_before.get("import_status") or "").strip().lower() if isinstance(existing_before, dict) else ""
            project, created = _import_target_project(
                client,
                target=target,
                target_project_path=target.target_project_path,
                timeout_seconds=target.git_timeout_seconds,
            )
            used_source_import = existing_before is None or status_before in {"", "none"}
        else:
            project, created = ensure_gitlab_project(client, target.target_project_path)
            used_source_import = False
        project_id = int(project["id"])
        project_import_status = str(project.get("import_status") or "").strip().lower()
        target_url = client.project_git_url(target.target_project_path)
        secrets = (client.token, client.username)

        results: dict[str, list[str]] = {
            "created": [],
            "updated": [],
            "skipped": [],
            "protected": [],
            "pruned": [],
            "unprotected": [],
        }

        if created:
            results["created"].append(f"project:{target.target_project_path}")

        if target.source_import and used_source_import:
            results["updated"].append("seed:source_import")

        _sync_target_refs(
            target,
            branches=branches,
            tags=tags,
            source_url=source_url,
            source_default_branch=source_default_branch,
            source_sha=source_sha,
            target_url=target_url,
            project_id=project_id,
            client=client,
            source_env=source_env,
            git_env=git_env,
            results=results,
            secrets=secrets,
        )

        default_branch_changed = ensure_gitlab_default_branch(client, project_id, policy.default_branch)
        if default_branch_changed:
            results["updated"].append(f"default_branch:{policy.default_branch}")
        if not target.source_import and project_import_status == "finished":
            _prune_imported_refs(
                client,
                project_id=project_id,
                branches=branches,
                tags=tags,
                results=results,
            )

        return {
            "mode": target.mode,
            "target_id": target.target_id,
            "repo_name": target.repo_name,
            "target_project_path": target.target_project_path,
            "source": target.source_display,
            "source_default_branch": source_default_branch,
            "source_sha": source_sha,
            "results": results,
            "branch_rev": target.branch_rev,
        }


def write_json(path: str, payload: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _target_summary_name(payload: dict[str, Any]) -> str:
    target_project_path = str(payload.get("target_project_path") or "").strip()
    if target_project_path:
        return target_project_path
    repo_name = str(payload.get("repo_name") or "").strip()
    if repo_name:
        return repo_name
    return str(payload.get("target_id") or "unknown-target")


def mode_title(mode: str) -> str:
    if mode == "external":
        return "External"
    if mode == "internal":
        return "Internal"
    if mode == "group":
        return "Group"
    return mode.capitalize()


def _summarize_ref_reasons(items: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    for state in items.values():
        if not isinstance(state, dict):
            continue
        label = str(state.get("label") or "ref").strip()
        for reason in state.get("reasons", []):
            if reason == "missing":
                labels.append(f"{label} missing")
            elif reason == "sha_diverged":
                labels.append(f"{label} diverged")
            elif reason == "protection_missing":
                labels.append(f"{label} protection missing")
            elif reason == "protection_present":
                labels.append(f"{label} protection present")
            elif reason == "source_missing":
                labels.append(f"{label} source missing")
            else:
                labels.append(f"{label} {reason}")
    return labels


def summarize_target_reasons(payload: dict[str, Any]) -> str:
    labels: list[str] = []
    reasons = payload.get("reasons", [])
    if "project_missing" in reasons:
        labels.append("project missing")
    labels.extend(_summarize_ref_reasons(payload.get("branches", {})))
    labels.extend(_summarize_ref_reasons(payload.get("tags", {})))
    if any(str(reason).startswith("default_branch_mismatch:") for reason in reasons):
        labels.append("default branch mismatch")
    if "unmanaged_branches_present" in reasons:
        labels.append("unexpected branches present")
    if "unmanaged_tags_present" in reasons:
        labels.append("unexpected tags present")
    return ", ".join(labels) if labels else "reconcile required"


def render_plan_summary(mode: str, inspected: list[dict[str, Any]], errors: list[dict[str, str]]) -> str:
    title = mode_title(mode)
    actionable = [item for item in inspected if item.get("needs_reconcile")]
    clean = len(inspected) - len(actionable)
    lines = [
        f"## {title} sync plan",
        "",
        f"- inspected: {len(inspected)}",
        f"- actionable: {len(actionable)}",
        f"- clean: {clean}",
        f"- errors: {len(errors)}",
        "",
    ]
    if actionable:
        lines.append("### Targets queued for reconcile")
        lines.append("")
        for item in actionable:
            lines.append(f"- `{_target_summary_name(item)}`: {summarize_target_reasons(item)}")
        lines.append("")
    if errors:
        lines.append("### Inspection errors")
        lines.append("")
        for item in errors:
            lines.append(f"- `{_target_summary_name(item)}`: {item['error']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_reconcile_summary(payload: dict[str, Any]) -> str:
    results = payload.get("results", {})
    created = results.get("created", [])
    updated = results.get("updated", [])
    skipped = results.get("skipped", [])
    protected = results.get("protected", [])
    pruned = results.get("pruned", [])
    unprotected = results.get("unprotected", [])
    lines = [
        f"## Reconciled `{_target_summary_name(payload)}`",
        "",
        f"- target id: `{payload['target_id']}`",
        f"- mode: `{payload['mode']}`",
        f"- source default branch: `{payload['source_default_branch']}`",
        f"- source sha: `{payload['source_sha']}`",
        "",
        f"- created: {len(created)}",
        f"- updated: {len(updated)}",
        f"- skipped: {len(skipped)}",
        f"- protected repaired: {len(protected)}",
        f"- pruned imported refs: {len(pruned)}",
        f"- protection removed: {len(unprotected)}",
        "",
    ]
    for label, values in (
        ("Created", created),
        ("Updated", updated),
        ("Skipped", skipped),
        ("Protected", protected),
        ("Pruned", pruned),
        ("Unprotected", unprotected),
    ):
        if values:
            lines.append(f"### {label}")
            lines.append("")
            for value in values:
                lines.append(f"- `{value}`")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_reconcile_batch_summary(
    mode: str,
    batch_index: int,
    inspected_count: int,
    clean_count: int,
    reconciled: list[dict[str, Any]],
    errors: list[dict[str, str]],
) -> str:
    title = mode_title(mode)
    lines = [
        f"## {title} reconcile run",
        "",
        f"- batch index: {batch_index}",
        f"- inspected: {inspected_count}",
        f"- actionable: {len(reconciled)}",
        f"- clean: {clean_count}",
        f"- reconciled: {len(reconciled)}",
        f"- errors: {len(errors)}",
        "",
    ]
    if reconciled:
        lines.append("### Reconciled targets")
        lines.append("")
        for payload in reconciled:
            results = payload.get("results", {})
            summary_line = (
                "- "
                f"`{_target_summary_name(payload)}`: "
                f"created={len(results.get('created', []))}, "
                f"updated={len(results.get('updated', []))}, "
                f"skipped={len(results.get('skipped', []))}, "
                f"protected={len(results.get('protected', []))}, "
                f"pruned={len(results.get('pruned', []))}, "
                f"unprotected={len(results.get('unprotected', []))}"
            )
            skipped_values = [str(value) for value in results.get("skipped", []) if str(value).strip()]
            if skipped_values:
                summary_line += f"; skipped refs: {', '.join(skipped_values)}"
            lines.append(summary_line)
        lines.append("")
    if errors:
        lines.append("### Reconcile errors")
        lines.append("")
        for item in errors:
            lines.append(f"- `{_target_summary_name(item)}`: {item['error']}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
