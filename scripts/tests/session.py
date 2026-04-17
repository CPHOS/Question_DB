"""HTTP client and infrastructure for E2E tests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
import uuid
import zipfile
from pathlib import Path
from typing import Any

from .config import (
    API_LOG_PATH,
    API_PORT,
    CONTAINER_NAME,
    DB_URL,
    DOWNLOADS_DIR,
    POSTGRES_DB,
    POSTGRES_IMAGE,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    ROOT_DIR,
    SAMPLES_DIR,
    TMP_DIR,
)


# ── Response helpers ─────────────────────────────────────────────


def parse_json(body: str) -> Any:
    return json.loads(body) if body else None


def paginated_items(body: str) -> list:
    data = parse_json(body)
    if isinstance(data, dict) and "items" in data:
        return data["items"]
    return data


def question_ids_from_body(body: str) -> list[str]:
    return [item["question_id"] for item in paginated_items(body)]


def build_question_fields(
    *,
    description: str,
    category: str | None = None,
    tags: list[str] | None = None,
) -> dict[str, str]:
    fields: dict[str, str] = {
        "description": description,
    }
    if category is not None:
        fields["category"] = category
    if tags is not None:
        fields["tags"] = json.dumps(tags, ensure_ascii=False)
    return fields


# ── HTTP Client ──────────────────────────────────────────────────


class ApiClient:
    """Thin HTTP client + test infrastructure manager."""

    def __init__(self) -> None:
        self._api_proc: subprocess.Popen | None = None
        self._api_log: Any = None
        self._access_token: str | None = None

    # -- Auth helpers --

    def login(self, username: str = "admin", password: str = "changeme") -> dict:
        """Login and store the access token for subsequent requests."""
        _, body, _ = self._do(
            "POST", "/auth/login", expect=200,
            headers={"content-type": "application/json"},
            body=json.dumps({"username": username, "password": password}).encode(),
        )
        data = parse_json(body)
        self._access_token = data["access_token"]
        return data

    def login_as(self, username: str, password: str) -> dict:
        """Login as a specific user and store the token."""
        return self.login(username, password)

    def ensure_user(self, payload: dict) -> dict:
        """Create a user or, if the username already exists, reset password and reactivate.

        Returns the user profile dict (with ``user_id``).
        """
        username = payload["username"]
        role = payload.get("role", "viewer")
        password = payload.get("password")
        url = f"http://127.0.0.1:{API_PORT}/admin/users"
        req = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode(),
            method="POST",
            headers={"content-type": "application/json", **self._auth_headers()},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return parse_json(resp.read().decode())
        except urllib.error.HTTPError as err:
            if err.code != 409:
                raise
            err.read()  # drain

        # User already exists — look up by username.
        _, body, _ = self.get(f"/admin/users?limit=200")
        users = parse_json(body)["items"]
        user = next(u for u in users if u["username"] == username)
        uid = user["user_id"]

        # Reactivate + set correct role / leader_expires_at.
        patch: dict[str, Any] = {"is_active": True}
        if "role" in payload:
            patch["role"] = payload["role"]
        # Explicitly set or clear leader_expires_at so stale values don't leak.
        if "leader_expires_at" in payload:
            patch["leader_expires_at"] = payload["leader_expires_at"]
        else:
            patch["leader_expires_at"] = None
        if "display_name" in payload:
            patch["display_name"] = payload["display_name"]
        self.patch_json(f"/admin/users/{uid}", patch)

        if role == "bot":
            _, body, _ = self.post_json(f"/admin/users/{uid}/access-token", {})
            return parse_json(body)

        assert password is not None, "non-bot test users must provide a password"

        # Reset password so the test can login with the expected credentials.
        self.post_json(f"/admin/users/{uid}/reset-password", {
            "new_password": password,
        })

        # Re-fetch profile to return fresh data.
        _, body, _ = self.get(f"/admin/users?limit=200")
        users = parse_json(body)["items"]
        return next(u for u in users if u["user_id"] == uid)

    def set_token(self, token: str | None) -> None:
        """Manually set or clear the auth token."""
        self._access_token = token

    def _auth_headers(self) -> dict[str, str]:
        """Return Authorization header if logged in."""
        if self._access_token:
            return {"Authorization": f"Bearer {self._access_token}"}
        return {}

    # -- HTTP verbs --

    def get(self, path: str, *, expect: int = 200):
        return self._do("GET", path, expect=expect, headers=self._auth_headers())

    def delete(self, path: str, *, expect: int = 200):
        return self._do("DELETE", path, expect=expect, headers=self._auth_headers())

    def post_json(self, path: str, payload: dict, *, expect: int = 200):
        return self._do(
            "POST",
            path,
            expect=expect,
            headers={"content-type": "application/json", **self._auth_headers()},
            body=json.dumps(payload, ensure_ascii=False).encode(),
        )

    def patch_json(self, path: str, payload: dict, *, expect: int = 200):
        return self._do(
            "PATCH",
            path,
            expect=expect,
            headers={"content-type": "application/json", **self._auth_headers()},
            body=json.dumps(payload, ensure_ascii=False).encode(),
        )

    def upload(
        self,
        path: str,
        *,
        fields: dict[str, str] | None = None,
        file_path: Path | None = None,
        file_content_type: str = "application/zip",
        method: str = "POST",
        expect: int = 200,
    ):
        boundary = f"----B{uuid.uuid4().hex}"
        raw = bytearray()
        for k, v in (fields or {}).items():
            raw += (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{k}"\r\n\r\n'
                f"{v}\r\n"
            ).encode()
        if file_path is not None:
            raw += (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; '
                f'filename="{file_path.name}"\r\n'
                f"Content-Type: {file_content_type}\r\n\r\n"
            ).encode()
            raw += file_path.read_bytes() + b"\r\n"
        raw += f"--{boundary}--\r\n".encode()
        return self._do(
            method,
            path,
            expect=expect,
            headers={
                "content-type": f"multipart/form-data; boundary={boundary}",
                **self._auth_headers(),
            },
            body=bytes(raw),
        )

    def download_file(
        self,
        path: str,
        output: Path,
        *,
        method: str = "GET",
        payload: dict | None = None,
        expect: int = 200,
    ) -> tuple[bytes, dict[str, str]]:
        data = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None
        headers = {**self._auth_headers()}
        if payload is not None:
            headers["content-type"] = "application/json"
        req = urllib.request.Request(
            f"http://127.0.0.1:{API_PORT}{path}",
            data=data,
            method=method,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.status
                body = resp.read()
                rh = {k.lower(): v for k, v in resp.headers.items()}
        except urllib.error.HTTPError as err:
            raise AssertionError(
                f"expected {expect}, got {err.code}: "
                f"{err.read().decode(errors='replace')[:500]}"
            ) from err
        assert status == expect, f"expected {expect}, got {status}"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(body)
        return body, rh

    def download_zip(
        self,
        path: str,
        payload: dict,
        output: Path,
        *,
        expect: int = 200,
    ) -> tuple[dict, list[str]]:
        req = urllib.request.Request(
            f"http://127.0.0.1:{API_PORT}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode(),
            method="POST",
            headers={"content-type": "application/json", **self._auth_headers()},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.status
                data = resp.read()
        except urllib.error.HTTPError as err:
            raise AssertionError(
                f"expected {expect}, got {err.code}: "
                f"{err.read().decode(errors='replace')[:500]}"
            ) from err
        assert status == expect, f"expected {expect}, got {status}"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(data)
        with zipfile.ZipFile(output) as zf:
            names = zf.namelist()
            manifest = (
                json.loads(zf.read("manifest.json"))
                if "manifest.json" in names
                else None
            )
        assert manifest is not None, "zip should contain manifest.json"
        return manifest, names

    # -- Infrastructure --

    def prepare_workspace(self) -> None:
        shutil.rmtree(TMP_DIR, ignore_errors=True)
        SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    def start_postgres(self) -> None:
        self._docker_rm_if_exists()
        subprocess.run(
            [
                "docker", "run", "-d", "--name", CONTAINER_NAME,
                "-e", f"POSTGRES_USER={POSTGRES_USER}",
                "-e", f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
                "-e", f"POSTGRES_DB={POSTGRES_DB}",
                "-p", f"{POSTGRES_PORT}:5432",
                POSTGRES_IMAGE,
            ],
            cwd=ROOT_DIR, check=True, capture_output=True,
        )
        self._wait_pg()

    def apply_migration(self) -> None:
        migrations_dir = ROOT_DIR / "migrations"
        for sql_file in sorted(migrations_dir.glob("*.sql")):
            sql = sql_file.read_bytes()
            subprocess.run(
                [
                    "docker", "exec", "-i", CONTAINER_NAME,
                    "psql", "-U", POSTGRES_USER, "-d", POSTGRES_DB,
                ],
                input=sql, cwd=ROOT_DIR, check=True, capture_output=True,
            )

    def start_api(self) -> None:
        self._api_log = API_LOG_PATH.open("wb")
        env = {
            **os.environ,
            "QB_DATABASE_URL": DB_URL,
            "QB_BIND_ADDR": f"127.0.0.1:{API_PORT}",
            "QB_EXPORT_DIR": str(TMP_DIR),
            "QB_POSTGRES_CONTAINER_NAME": CONTAINER_NAME,
        }
        self._api_proc = subprocess.Popen(
            ["cargo", "run"], cwd=ROOT_DIR, env=env,
            stdout=self._api_log, stderr=subprocess.STDOUT,
        )
        self._wait_api()

    def cleanup(self) -> None:
        if self._api_proc and self._api_proc.poll() is None:
            self._api_proc.terminate()
            try:
                self._api_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._api_proc.kill()
                self._api_proc.wait(timeout=5)
        if self._api_log and not self._api_log.closed:
            self._api_log.close()
        self._docker_rm_if_exists()

    # -- Private --

    def _do(self, method, path, *, expect, headers=None, body=None):
        url = f"http://127.0.0.1:{API_PORT}{path}"
        req = urllib.request.Request(
            url, data=body, method=method, headers=headers or {},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.status
                rh = {k.lower(): v for k, v in resp.headers.items()}
                rb = resp.read().decode()
        except urllib.error.HTTPError as err:
            status = err.code
            rh = {k.lower(): v for k, v in err.headers.items()}
            rb = err.read().decode(errors="replace")
        assert status == expect, (
            f"{method} {path}: expected {expect}, got {status}\n{rb[:500]}"
        )
        return status, rb, rh

    def _docker_rm_if_exists(self) -> None:
        r = subprocess.run(
            ["docker", "ps", "-a", "--format", "{{.Names}}"],
            capture_output=True, text=True, check=False,
        )
        if CONTAINER_NAME in r.stdout.splitlines():
            subprocess.run(
                ["docker", "rm", "-f", CONTAINER_NAME],
                capture_output=True, check=False,
            )

    def _wait_pg(self) -> None:
        for _ in range(60):
            r = subprocess.run(
                [
                    "docker", "exec", CONTAINER_NAME,
                    "pg_isready", "-U", POSTGRES_USER, "-d", POSTGRES_DB,
                ],
                capture_output=True, check=False,
            )
            if r.returncode == 0:
                return
            time.sleep(1)
        raise RuntimeError("PostgreSQL did not become ready in 60s")

    def _wait_api(self) -> None:
        for _ in range(120):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{API_PORT}/health"
                ) as r:
                    if r.status == 200:
                        return
            except Exception:
                time.sleep(1)
        raise RuntimeError("API did not become ready in 120s")
