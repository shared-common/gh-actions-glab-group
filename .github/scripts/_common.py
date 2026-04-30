from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "configs"

_REF_INVALID = re.compile(r"[ ~^:?*[\]\\]")
_PROJECT_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SECRET_NAME = re.compile(r"^[A-Z0-9_]+$")

PROTECTED_BRANCH_PUSH_LEVEL = 40
PROTECTED_BRANCH_MERGE_LEVEL = 40
PROTECTED_BRANCH_UNPROTECT_LEVEL = 40
PROTECTED_TAG_CREATE_LEVEL = 40


class ApiError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class GitLabClient:
    base_url: str
    username: str
    token: str

    def project_git_url(self, project_path: str) -> str:
        validate_project_path(project_path, "project_path")
        host = self.base_url.replace("https://", "").replace("http://", "").rstrip("/")
        return f"https://{host}/{project_path}.git"

    def project_web_url(self, project_path: str) -> str:
        validate_project_path(project_path, "project_path")
        return f"{self.base_url.rstrip('/')}/{project_path}"


def sanitize(text: str, secrets: Iterable[str]) -> str:
    sanitized = text
    for secret in secrets:
        if secret:
            sanitized = sanitized.replace(secret, "[REDACTED]")
    return sanitized


def config_path(name: str) -> str:
    return str(CONFIG_DIR / name)


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required env var: {name}")
    return value


def _read_secret_file(name: str, *, required: bool, allow_empty: bool) -> Optional[str]:
    env_name = f"{name}_FILE"
    path_value = os.environ.get(env_name, "").strip()
    if not path_value:
        if required:
            raise SystemExit(f"Missing required secret file env var: {env_name}")
        return None
    path = Path(path_value)
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError as exc:
        raise SystemExit(f"Missing secret file for {name}: {path_value}") from exc
    except OSError as exc:
        raise SystemExit(f"Unable to read secret file for {name}: {path_value}") from exc
    if not value and required and not allow_empty:
        raise SystemExit(f"Empty secret file for {name}")
    return value


def require_secret(name: str) -> str:
    value = _read_secret_file(name, required=True, allow_empty=False)
    assert value is not None
    return value


def load_json_file(path: str, label: str) -> Any:
    try:
        text = Path(path).read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise SystemExit(f"{label} file not found: {path}") from exc
    except OSError as exc:
        raise SystemExit(f"Unable to read {label} file: {path}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} file is not valid JSON: {exc.msg}") from exc


def load_json_mapping(raw: str, label: str) -> dict[str, str]:
    if not raw.strip():
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{label} is not valid JSON: {exc.msg}") from exc
    if not isinstance(data, dict):
        raise SystemExit(f"{label} must be a JSON object")
    mapping: dict[str, str] = {}
    for key, value in data.items():
        if not isinstance(key, str) or not key.strip():
            raise SystemExit(f"{label} contains an invalid key")
        if not isinstance(value, str):
            raise SystemExit(f"{label} contains a non-string value for {key!r}")
        mapping[key.strip()] = value.strip()
    return mapping


def validate_secret_name(value: str) -> None:
    if not _SECRET_NAME.match(value):
        raise SystemExit(f"Invalid secret name: {value}")


def validate_ref_name(value: str, label: str = "ref") -> None:
    if not value or value.strip() != value:
        raise SystemExit(f"{label} is empty or has surrounding whitespace")
    if value.startswith("/") or value.endswith("/"):
        raise SystemExit(f"{label} must not start or end with '/'")
    if value.endswith(".lock"):
        raise SystemExit(f"{label} must not end with .lock")
    if "//" in value or ".." in value or "@{" in value:
        raise SystemExit(f"{label} contains an invalid sequence")
    if _REF_INVALID.search(value):
        raise SystemExit(f"{label} contains invalid characters")
    for ch in value:
        if ord(ch) < 32 or ord(ch) == 127:
            raise SystemExit(f"{label} contains control characters")


def validate_project_segment(value: str, label: str) -> None:
    if not value or not _PROJECT_SEGMENT.match(value):
        raise SystemExit(f"Invalid {label}: {value}")


def validate_project_path(value: str, label: str, *, min_segments: int = 2) -> None:
    if not isinstance(value, str) or value.strip() != value or not value:
        raise SystemExit(f"Invalid {label}")
    parts = value.split("/")
    if len(parts) < min_segments:
        raise SystemExit(f"{label} must include at least {min_segments} path segments")
    for part in parts:
        validate_project_segment(part, label)


def validate_https_url(value: str, label: str) -> urllib.parse.ParseResult:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "https":
        raise SystemExit(f"{label} must use https")
    if not parsed.netloc:
        raise SystemExit(f"{label} is missing a host")
    if parsed.username or parsed.password:
        raise SystemExit(f"{label} must not embed credentials")
    if parsed.query or parsed.fragment:
        raise SystemExit(f"{label} must not contain query or fragment components")
    path = parsed.path.rstrip("/")
    if not path or path == "/":
        raise SystemExit(f"{label} is missing a project path")
    return parsed


def normalized_https_base_url(value: str, label: str) -> str:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "https":
        raise SystemExit(f"{label} must use https")
    if not parsed.netloc:
        raise SystemExit(f"{label} is missing a host")
    if parsed.username or parsed.password:
        raise SystemExit(f"{label} must not embed credentials")
    if parsed.query or parsed.fragment:
        raise SystemExit(f"{label} must not contain query or fragment components")
    host = parsed.hostname or ""
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urllib.parse.urlunparse((parsed.scheme.lower(), netloc.lower(), "", "", "", "")).rstrip("/")


def parse_gitlab_group_url(value: str, label: str) -> tuple[str, str]:
    parsed = validate_https_url(value, label)
    path = parsed.path.rstrip("/")
    group_path = path.lstrip("/")
    if group_path.endswith(".git"):
        raise SystemExit(f"{label} must point to a GitLab group, not a repository")
    validate_project_path(group_path, label, min_segments=1)
    host = parsed.hostname or ""
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    base_url = urllib.parse.urlunparse((parsed.scheme.lower(), netloc.lower(), "", "", "", "")).rstrip("/")
    return base_url, group_path


def normalize_gitlab_project_url(value: str, label: str) -> str:
    if not isinstance(value, str) or value.strip() != value or not value:
        raise SystemExit(f"Invalid {label}")

    scp_like = re.fullmatch(r"[^@\s:]+@[^:\s/]+:.+", value)
    if scp_like:
        return value.rstrip("/")

    parsed = urllib.parse.urlparse(value)
    if not parsed.scheme:
        raise SystemExit(f"{label} must include a URL scheme")
    if parsed.scheme not in {"file", "git", "http", "https", "ssh"}:
        raise SystemExit(f"{label} must use a supported git URL scheme")
    if parsed.scheme != "file" and not parsed.netloc:
        raise SystemExit(f"{label} is missing a host")
    if parsed.password:
        raise SystemExit(f"{label} must not embed passwords")
    if parsed.query or parsed.fragment:
        raise SystemExit(f"{label} must not contain query or fragment components")

    path = parsed.path.rstrip("/")
    if not path or path == "/":
        raise SystemExit(f"{label} is missing a repository path")

    return urllib.parse.urlunparse(parsed._replace(path=path))


def git_source_head(
    remote_url: str,
    *,
    secrets: Iterable[str] = (),
    env_overrides: Optional[dict[str, str]] = None,
) -> tuple[str, str]:
    proc = run_command(
        ["git", "ls-remote", "--symref", remote_url, "HEAD"],
        secrets=secrets,
        timeout=120,
        env_overrides=env_overrides,
    )
    branch = ""
    sha = ""
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("ref: "):
            ref_value = line.split("\t", 1)[0][len("ref: ") :].strip()
            if ref_value.startswith("refs/heads/"):
                branch = ref_value[len("refs/heads/") :]
        else:
            parts = line.split()
            if len(parts) == 2 and parts[1] == "HEAD":
                sha = parts[0]
    if not branch:
        raise SystemExit(f"Unable to resolve source default branch for {remote_url}")
    validate_ref_name(branch, "source default branch")
    if not sha or len(sha) < 7:
        raise SystemExit(f"Unable to resolve source HEAD sha for {remote_url}")
    return branch, sha


def run_command(
    cmd: list[str],
    *,
    cwd: Optional[str] = None,
    secrets: Iterable[str] = (),
    timeout: int = 120,
    env_overrides: Optional[dict[str, str]] = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    if env_overrides:
        env.update(env_overrides)
    command_text = sanitize(" ".join(cmd), secrets)
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"Command timed out after {timeout}s: {command_text}") from exc
    if proc.returncode != 0:
        stderr = sanitize(proc.stderr.strip(), secrets)
        raise SystemExit(stderr or f"Command failed: {command_text}")
    return proc


@contextmanager
def git_askpass_env(client: GitLabClient) -> Iterator[dict[str, str]]:
    expected_host = urllib.parse.urlparse(client.base_url).netloc.strip()
    if not expected_host:
        raise SystemExit("GL_BASE_URL does not contain a valid host")

    with tempfile.TemporaryDirectory(prefix="gitlab-auth-") as temp_dir:
        temp_path = Path(temp_dir)
        username_path = temp_path / "username"
        token_path = temp_path / "token"
        host_path = temp_path / "host"
        askpass_path = temp_path / "askpass.sh"

        username_path.write_text(client.username, encoding="utf-8")
        token_path.write_text(client.token, encoding="utf-8")
        host_path.write_text(expected_host, encoding="utf-8")
        os.chmod(username_path, 0o600)
        os.chmod(token_path, 0o600)
        os.chmod(host_path, 0o600)

        askpass_path.write_text(
            "\n".join(
                [
                    "#!/bin/sh",
                    "set -eu",
                    'prompt="${1:-}"',
                    'expected_host="$(cat "${GH_ACTIONS_GITLAB_HOST_FILE}")"',
                    'case "${prompt}" in',
                    '  *"${expected_host}"*) ;;',
                    "  *) exit 1 ;;",
                    "esac",
                    'case "${prompt}" in',
                    '  Username*) cat "${GH_ACTIONS_GITLAB_USERNAME_FILE}" ;;',
                    '  Password*) cat "${GH_ACTIONS_GITLAB_TOKEN_FILE}" ;;',
                    "  *) exit 1 ;;",
                    "esac",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        os.chmod(askpass_path, 0o700)

        yield {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_ASKPASS": str(askpass_path),
            "GH_ACTIONS_GITLAB_USERNAME_FILE": str(username_path),
            "GH_ACTIONS_GITLAB_TOKEN_FILE": str(token_path),
            "GH_ACTIONS_GITLAB_HOST_FILE": str(host_path),
        }


def gitlab_request(
    client: GitLabClient,
    method: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    *,
    retries: int = 3,
    timeout: int = 30,
) -> Any:
    url = f"{client.base_url.rstrip('/')}/api/v4{path}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "PRIVATE-TOKEN": client.token,
        "Content-Type": "application/json",
    }
    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                body = response.read()
                if not body:
                    return None
                try:
                    return json.loads(body.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise ApiError(response.status, "Invalid JSON response from GitLab API") from exc
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
            body = sanitize(body, (client.token, client.username))
            if exc.code in {500, 502, 503, 504} and attempt < retries - 1:
                time.sleep(1 + attempt)
                attempt += 1
                continue
            raise ApiError(exc.code, body or "GitLab API error") from exc
        except urllib.error.URLError as exc:
            if attempt < retries - 1:
                time.sleep(1 + attempt)
                attempt += 1
                continue
            raise ApiError(0, f"Network error contacting GitLab API: {exc}") from exc


def get_gitlab_project(client: GitLabClient, project_path: str) -> Optional[dict[str, Any]]:
    encoded = urllib.parse.quote(project_path, safe="")
    try:
        data = gitlab_request(client, "GET", f"/projects/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return data if isinstance(data, dict) else None


def get_gitlab_group(client: GitLabClient, group_path: str) -> Optional[dict[str, Any]]:
    encoded = urllib.parse.quote(group_path, safe="")
    try:
        data = gitlab_request(client, "GET", f"/groups/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return data if isinstance(data, dict) else None


def search_gitlab_group(client: GitLabClient, group_path: str) -> Optional[dict[str, Any]]:
    target_full = group_path.lower()
    target_name = group_path.rsplit("/", 1)[-1].lower()
    search = urllib.parse.quote(target_name, safe="")
    page = 1
    while True:
        data = gitlab_request(
            client,
            "GET",
            f"/groups?search={search}&per_page=100&page={page}",
        )
        if not isinstance(data, list) or not data:
            return None
        for item in data:
            if not isinstance(item, dict):
                continue
            if str(item.get("full_path", "")).lower() == target_full:
                return item
        if len(data) < 100:
            return None
        page += 1


def get_gitlab_group_id(client: GitLabClient, group_path: str) -> int:
    encoded = urllib.parse.quote(group_path, safe="")
    try:
        data = gitlab_request(client, "GET", f"/groups/{encoded}")
    except ApiError as exc:
        if exc.status != 404:
            raise
        data = search_gitlab_group(client, group_path)
        if data is None:
            project = get_gitlab_project(client, group_path)
            if project:
                raise SystemExit(f"GitLab path exists as a project, not a group: {group_path}") from exc
            raise SystemExit(f"GitLab group not found or not accessible: {group_path}") from exc
    if not isinstance(data, dict) or not data.get("id"):
        raise SystemExit(f"Unable to resolve GitLab group id for {group_path}")
    return int(data["id"])


def ensure_gitlab_group(client: GitLabClient, group_path: str) -> tuple[dict[str, Any], bool]:
    validate_project_path(group_path, "group_path", min_segments=1)
    existing = get_gitlab_group(client, group_path)
    if existing:
        return existing, False

    parent_id: int | None = None
    group_name = group_path
    if "/" in group_path:
        parent_path, group_name = group_path.rsplit("/", 1)
        parent, _ = ensure_gitlab_group(client, parent_path)
        parent_raw = parent.get("id")
        if not isinstance(parent_raw, int):
            raise SystemExit(f"Unable to resolve GitLab group id for {parent_path}")
        parent_id = parent_raw

    payload: dict[str, Any] = {
        "name": group_name,
        "path": group_name,
        "visibility": "private",
    }
    if parent_id is not None:
        payload["parent_id"] = parent_id

    try:
        created = gitlab_request(client, "POST", "/groups", payload)
    except ApiError as exc:
        if not _is_already_exists_conflict(exc):
            raise
        existing = get_gitlab_group(client, group_path)
        if existing:
            return existing, False
        raise
    if not isinstance(created, dict):
        raise SystemExit("GitLab group create returned an invalid response")
    return created, True


def list_gitlab_group_projects(
    client: GitLabClient,
    group_path: str,
    *,
    include_subgroups: bool = True,
) -> list[dict[str, Any]]:
    encoded = urllib.parse.quote(group_path, safe="")
    page = 1
    projects: list[dict[str, Any]] = []
    include_subgroups_flag = "true" if include_subgroups else "false"
    while True:
        data = gitlab_request(
            client,
            "GET",
            (
                f"/groups/{encoded}/projects"
                f"?include_subgroups={include_subgroups_flag}"
                f"&with_shared=false&per_page=100&page={page}"
            ),
        )
        if not isinstance(data, list):
            raise SystemExit("GitLab group project list returned an invalid response")
        if not data:
            return projects
        for item in data:
            if isinstance(item, dict):
                projects.append(item)
        if len(data) < 100:
            return projects
        page += 1


def find_project_in_group(client: GitLabClient, group_id: int, project_path: str, project_name: str) -> Optional[dict[str, Any]]:
    target_path = project_name.lower()
    target_full = project_path.lower()
    search = urllib.parse.quote(project_name, safe="")
    page = 1
    while True:
        data = gitlab_request(
            client,
            "GET",
            f"/groups/{group_id}/projects?search={search}&per_page=100&page={page}",
        )
        if not isinstance(data, list) or not data:
            return None
        for item in data:
            if not isinstance(item, dict):
                continue
            path_value = str(item.get("path", "")).lower()
            path_ns = str(item.get("path_with_namespace", "")).lower()
            if path_ns == target_full or path_value == target_path:
                return item
        if len(data) < 100:
            return None
        page += 1


def ensure_gitlab_project(client: GitLabClient, project_path: str) -> tuple[dict[str, Any], bool]:
    existing = get_gitlab_project(client, project_path)
    if existing:
        return existing, False

    group_path, project_name = project_path.rsplit("/", 1)
    group, _ = ensure_gitlab_group(client, group_path)
    group_id_raw = group.get("id")
    if not isinstance(group_id_raw, int):
        raise SystemExit(f"Unable to resolve GitLab group id for {group_path}")
    group_id = group_id_raw
    existing = find_project_in_group(client, group_id, project_path, project_name)
    if existing:
        return existing, False

    payload = {
        "name": project_name,
        "path": project_name,
        "namespace_id": group_id,
        "shared_runners_enabled": False,
        "visibility": "private",
    }
    try:
        created = gitlab_request(client, "POST", "/projects", payload)
    except ApiError as exc:
        message = str(exc).lower()
        if exc.status not in {400, 409} or (
            "already exists" not in message
            and "has already been taken" not in message
            and "path has already been taken" not in message
        ):
            raise
        existing = get_gitlab_project(client, project_path)
        if existing:
            return existing, False
        existing = find_project_in_group(client, group_id, project_path, project_name)
        if existing:
            return existing, False
        raise
    if not isinstance(created, dict):
        raise SystemExit("GitLab project create returned an invalid response")
    return created, True


def canonicalize_remote_mirror_url(value: str, label: str) -> str:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "https":
        raise SystemExit(f"{label} must use https")
    if not parsed.hostname:
        raise SystemExit(f"{label} is missing a host")
    if parsed.query or parsed.fragment:
        raise SystemExit(f"{label} must not contain query or fragment components")
    path = parsed.path.rstrip("/")
    if not path or path == "/":
        raise SystemExit(f"{label} is missing a repository path")
    host = parsed.hostname.lower()
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urllib.parse.urlunparse((parsed.scheme.lower(), netloc, path, "", "", ""))


def inject_basic_auth_into_url(value: str, username: str, password: str, label: str) -> str:
    parsed = urllib.parse.urlparse(canonicalize_remote_mirror_url(value, label))
    host = parsed.hostname or ""
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    user_enc = urllib.parse.quote(username, safe="")
    pass_enc = urllib.parse.quote(password, safe="")
    return urllib.parse.urlunparse(
        parsed._replace(netloc=f"{user_enc}:{pass_enc}@{netloc}")
    )


def list_gitlab_remote_mirrors(client: GitLabClient, project_id: int) -> list[dict[str, Any]]:
    data = gitlab_request(client, "GET", f"/projects/{project_id}/remote_mirrors")
    if data is None:
        return []
    if not isinstance(data, list):
        raise SystemExit("GitLab remote mirror list returned an invalid response")
    return [item for item in data if isinstance(item, dict)]


def find_gitlab_remote_mirror(
    mirrors: list[dict[str, Any]],
    mirror_url: str,
) -> Optional[dict[str, Any]]:
    target = canonicalize_remote_mirror_url(mirror_url, "remote mirror url")
    for item in mirrors:
        current_url = str(item.get("url") or "").strip()
        if not current_url:
            continue
        try:
            current = canonicalize_remote_mirror_url(current_url, "remote mirror url")
        except SystemExit:
            continue
        if current == target:
            return item
    return None


def ensure_gitlab_push_mirror(
    client: GitLabClient,
    project_id: int,
    mirror_url: str,
    *,
    enabled: bool = True,
    only_protected_branches: bool = True,
    auth_method: str = "password",
) -> tuple[dict[str, Any], bool]:
    mirrors = list_gitlab_remote_mirrors(client, project_id)
    existing = find_gitlab_remote_mirror(mirrors, mirror_url)
    payload = {
        "url": mirror_url,
        "auth_method": auth_method,
        "enabled": enabled,
        "only_protected_branches": only_protected_branches,
    }
    if existing is None:
        created = gitlab_request(client, "POST", f"/projects/{project_id}/remote_mirrors", payload)
        if not isinstance(created, dict):
            raise SystemExit("GitLab remote mirror create returned an invalid response")
        return created, True

    mirror_id = existing.get("id")
    if not isinstance(mirror_id, int):
        raise SystemExit("GitLab remote mirror response is missing id")
    updated = gitlab_request(client, "PUT", f"/projects/{project_id}/remote_mirrors/{mirror_id}", payload)
    if not isinstance(updated, dict):
        raise SystemExit("GitLab remote mirror update returned an invalid response")
    return updated, False


def sync_gitlab_remote_mirror(client: GitLabClient, project_id: int, mirror_id: int) -> None:
    gitlab_request(client, "POST", f"/projects/{project_id}/remote_mirrors/{mirror_id}/sync")


def get_gitlab_branch(client: GitLabClient, project_id: int, branch: str) -> Optional[dict[str, Any]]:
    encoded = urllib.parse.quote(branch, safe="")
    try:
        data = gitlab_request(client, "GET", f"/projects/{project_id}/repository/branches/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return data if isinstance(data, dict) else None


def get_gitlab_branch_sha(client: GitLabClient, project_id: int, branch: str) -> Optional[str]:
    data = get_gitlab_branch(client, project_id, branch)
    if not isinstance(data, dict):
        return None
    commit = data.get("commit")
    if not isinstance(commit, dict):
        return None
    commit_id = commit.get("id")
    return str(commit_id) if isinstance(commit_id, str) else None


def create_gitlab_branch(client: GitLabClient, project_id: int, branch: str, ref: str) -> bool:
    payload = {
        "branch": branch,
        "ref": ref,
    }
    try:
        created = gitlab_request(client, "POST", f"/projects/{project_id}/repository/branches", payload)
    except ApiError as exc:
        if _is_already_exists_conflict(exc):
            return False
        raise
    if not isinstance(created, dict):
        raise SystemExit("GitLab branch create returned an invalid response")
    return True


def list_gitlab_branches(client: GitLabClient, project_id: int) -> list[dict[str, Any]]:
    page = 1
    branches: list[dict[str, Any]] = []
    while True:
        data = gitlab_request(client, "GET", f"/projects/{project_id}/repository/branches?per_page=100&page={page}")
        if not isinstance(data, list):
            raise SystemExit("GitLab branch list returned an invalid response")
        if not data:
            return branches
        for item in data:
            if isinstance(item, dict):
                branches.append(item)
        if len(data) < 100:
            return branches
        page += 1


def delete_gitlab_branch(client: GitLabClient, project_id: int, branch: str) -> bool:
    encoded = urllib.parse.quote(branch, safe="")
    try:
        gitlab_request(client, "DELETE", f"/projects/{project_id}/repository/branches/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return False
        raise
    return True


def get_gitlab_protected_branch(client: GitLabClient, project_id: int, branch: str) -> Optional[dict[str, Any]]:
    encoded = urllib.parse.quote(branch, safe="")
    try:
        data = gitlab_request(client, "GET", f"/projects/{project_id}/protected_branches/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return data if isinstance(data, dict) else None


def get_gitlab_protected_tag(client: GitLabClient, project_id: int, tag: str) -> Optional[dict[str, Any]]:
    encoded = urllib.parse.quote(tag, safe="")
    try:
        data = gitlab_request(client, "GET", f"/projects/{project_id}/protected_tags/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    return data if isinstance(data, dict) else None


def list_gitlab_tags(client: GitLabClient, project_id: int) -> list[dict[str, Any]]:
    page = 1
    tags: list[dict[str, Any]] = []
    while True:
        data = gitlab_request(client, "GET", f"/projects/{project_id}/repository/tags?per_page=100&page={page}")
        if not isinstance(data, list):
            raise SystemExit("GitLab tag list returned an invalid response")
        if not data:
            return tags
        for item in data:
            if isinstance(item, dict):
                tags.append(item)
        if len(data) < 100:
            return tags
        page += 1


def delete_gitlab_tag(client: GitLabClient, project_id: int, tag: str) -> bool:
    encoded = urllib.parse.quote(tag, safe="")
    try:
        gitlab_request(client, "DELETE", f"/projects/{project_id}/repository/tags/{encoded}")
    except ApiError as exc:
        if exc.status == 404:
            return False
        raise
    return True


def _access_level_set(data: Optional[dict[str, Any]], key: str) -> set[int]:
    if not isinstance(data, dict):
        return set()
    raw = data.get(key)
    if not isinstance(raw, list):
        return set()
    levels: set[int] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        access_level = item.get("access_level")
        if isinstance(access_level, int):
            levels.add(access_level)
    return levels


def protected_branch_allows_sync(data: Optional[dict[str, Any]]) -> bool:
    if not isinstance(data, dict):
        return False
    push_levels = _access_level_set(data, "push_access_levels")
    merge_levels = _access_level_set(data, "merge_access_levels")
    unprotect_levels = _access_level_set(data, "unprotect_access_levels")
    return (
        push_levels == {PROTECTED_BRANCH_PUSH_LEVEL}
        and merge_levels == {PROTECTED_BRANCH_MERGE_LEVEL}
        and unprotect_levels == {PROTECTED_BRANCH_UNPROTECT_LEVEL}
        and bool(data.get("allow_force_push"))
    )


def protected_tag_allows_sync(data: Optional[dict[str, Any]]) -> bool:
    if not isinstance(data, dict):
        return False
    create_levels = _access_level_set(data, "create_access_levels")
    return create_levels == {PROTECTED_TAG_CREATE_LEVEL}


def _is_already_exists_conflict(exc: ApiError) -> bool:
    if exc.status not in {400, 409}:
        return False
    message = str(exc).lower()
    return (
        "already exists" in message
        or "has already been taken" in message
        or "name has already been taken" in message
        or "protected branch" in message
        or "protected tag" in message
    )


def ensure_gitlab_protected_branch(client: GitLabClient, project_id: int, branch: str) -> bool:
    current = get_gitlab_protected_branch(client, project_id, branch)
    if protected_branch_allows_sync(current):
        return False

    encoded = urllib.parse.quote(branch, safe="")
    if current:
        gitlab_request(client, "DELETE", f"/projects/{project_id}/protected_branches/{encoded}")

    payload = {
        "name": branch,
        "push_access_level": PROTECTED_BRANCH_PUSH_LEVEL,
        "merge_access_level": PROTECTED_BRANCH_MERGE_LEVEL,
        "unprotect_access_level": PROTECTED_BRANCH_UNPROTECT_LEVEL,
        "allow_force_push": True,
    }
    try:
        gitlab_request(client, "POST", f"/projects/{project_id}/protected_branches", payload)
    except ApiError as exc:
        if not _is_already_exists_conflict(exc):
            raise
        return False
    return True


def delete_gitlab_protected_branch(client: GitLabClient, project_id: int, branch: str) -> bool:
    current = get_gitlab_protected_branch(client, project_id, branch)
    if not current:
        return False
    encoded = urllib.parse.quote(branch, safe="")
    gitlab_request(client, "DELETE", f"/projects/{project_id}/protected_branches/{encoded}")
    return True


def ensure_gitlab_protected_tag(client: GitLabClient, project_id: int, tag: str) -> bool:
    current = get_gitlab_protected_tag(client, project_id, tag)
    if protected_tag_allows_sync(current):
        return False

    encoded = urllib.parse.quote(tag, safe="")
    if current:
        gitlab_request(client, "DELETE", f"/projects/{project_id}/protected_tags/{encoded}")

    payload = {
        "name": tag,
        "create_access_level": PROTECTED_TAG_CREATE_LEVEL,
    }
    try:
        gitlab_request(client, "POST", f"/projects/{project_id}/protected_tags", payload)
    except ApiError as exc:
        if not _is_already_exists_conflict(exc):
            raise
        return False
    return True


def delete_gitlab_protected_tag(client: GitLabClient, project_id: int, tag: str) -> bool:
    current = get_gitlab_protected_tag(client, project_id, tag)
    if not current:
        return False
    encoded = urllib.parse.quote(tag, safe="")
    gitlab_request(client, "DELETE", f"/projects/{project_id}/protected_tags/{encoded}")
    return True


def ensure_gitlab_default_branch(client: GitLabClient, project_id: int, branch: str) -> bool:
    project = gitlab_request(client, "GET", f"/projects/{project_id}")
    if isinstance(project, dict) and str(project.get("default_branch") or "") == branch:
        return False
    gitlab_request(client, "PUT", f"/projects/{project_id}", {"default_branch": branch})
    return True
