"""Export, quality-check, database backup/restore, and permission tests."""

from __future__ import annotations

import gzip
import json
import tarfile
import uuid

from .config import DB_BACKUP_PATH, DOWNLOADS_DIR, EXPORT_PATH, QUALITY_PATH
from .session import build_question_fields, parse_json
from .specs import QUESTION_SPECS


# ── Tests ────────────────────────────────────────────────────────


def _ensure_bundle_fixture(api, state) -> tuple[str, str]:
    if state.q_ids and state.all_paper_ids:
        return state.q_ids[0], state.all_paper_ids[0]

    spec = QUESTION_SPECS[0]
    question = parse_json(
        api.upload(
            "/questions",
            fields=build_question_fields(
                description=spec["patch"]["description"],
                category=spec["patch"]["category"],
                tags=spec["patch"]["tags"],
            ),
            file_path=state.synthetic_zips[0],
        )[1]
    )
    question_id = question["question_id"]

    paper = parse_json(
        api.upload(
            "/papers",
            fields={
                "description": "ops viewer fallback paper",
                "title": "Ops Viewer Fallback",
                "subtitle": "Bundle Permission Fixture",
                "question_ids": json.dumps([question_id]),
            },
            file_path=state.appendix_paths["mock-a"],
        )[1]
    )
    return question_id, paper["paper_id"]


def test_export_jsonl(api, state):
    _, body, _ = api.post_json(
        "/exports/run",
        {
            "format": "jsonl",
            "public": False,
            "output_path": EXPORT_PATH.name,
        },
    )
    resp = parse_json(body)
    actual_total = parse_json(api.get("/questions?limit=1")[1])["total"]
    assert resp["exported_questions"] == actual_total


def test_export_path_traversal(api):
    """Reject directory-traversal and absolute paths."""
    api.post_json(
        "/exports/run",
        {"format": "jsonl", "output_path": "../../../etc/passwd"},
        expect=400,
    )
    api.post_json(
        "/exports/run",
        {"format": "jsonl", "output_path": "/absolute/path.jsonl"},
        expect=400,
    )


def test_quality_check(api):
    _, body, _ = api.post_json(
        "/quality-checks/run", {"output_path": QUALITY_PATH.name},
    )
    assert "empty_papers" in parse_json(body)["report"]


def test_quality_check_path_traversal(api):
    api.post_json(
        "/quality-checks/run",
        {"output_path": "../../etc/shadow"},
        expect=400,
    )


def test_database_backup_download(api):
    backup_bytes, headers = api.download_file("/database/backup", DB_BACKUP_PATH)

    assert headers["content-type"] == "application/gzip"
    assert "attachment;" in headers["content-disposition"]
    assert ".tar.gz" in headers["content-disposition"]

    # Verify it's a valid gzip/tar archive containing metadata.sql
    with tarfile.open(fileobj=gzip.open(DB_BACKUP_PATH), mode="r:") as tar:
        names = tar.getnames()
    assert "metadata.sql" in names


def test_database_restore_round_trip(api, state):
    pre_total = parse_json(api.get("/questions?limit=1")[1])["total"]
    api.download_file("/database/backup", DB_BACKUP_PATH)

    username = f"restore_probe_{uuid.uuid4().hex[:8]}"
    password = "restore123"
    created = parse_json(api.post_json("/admin/users", {
        "username": username,
        "password": password,
        "role": "viewer",
    })[1])
    assert created["username"] == username

    restored = parse_json(
        api.upload(
            "/database/restore",
            file_path=DB_BACKUP_PATH,
            file_content_type="application/gzip",
        )[1]
    )
    assert restored == {
        "file_name": DB_BACKUP_PATH.name,
        "restored_bytes": DB_BACKUP_PATH.stat().st_size,
        "status": "restored",
    }

    api.post_json(
        "/auth/login",
        {"username": username, "password": password},
        expect=401,
    )
    assert parse_json(api.get("/questions?limit=100")[1])["total"] == pre_total
    assert parse_json(api.get("/auth/me")[1])["role"] == "admin"


def test_database_restore_requires_non_empty_file(api):
    api.upload("/database/restore", expect=400)


def test_viewer_can_download_bundles_but_not_ops(api, state):
    """Viewer can download bundles, but ops endpoints are admin-only."""
    question_id, paper_id = _ensure_bundle_fixture(api, state)
    viewer = api.ensure_user({
        "username": "bundle_viewer",
        "password": "viewer123",
        "role": "viewer",
    })

    saved = api._access_token
    try:
        api.login("bundle_viewer", "viewer123")

        q_manifest, _ = api.download_zip(
            "/questions/bundles",
            {"question_ids": [question_id]},
            DOWNLOADS_DIR / "viewer_questions_bundle.zip",
        )
        assert q_manifest["kind"] == "question_bundle"

        p_manifest, _ = api.download_zip(
            "/papers/bundles",
            {"paper_ids": [paper_id]},
            DOWNLOADS_DIR / "viewer_papers_bundle.zip",
        )
        assert p_manifest["kind"] == "paper_bundle"

        api.post_json("/exports/run", {"format": "jsonl"}, expect=403)
        api.post_json("/quality-checks/run", {}, expect=403)
        api.get("/database/backup", expect=403)
        api.upload("/database/restore", expect=403)
    finally:
        api.set_token(saved)
        api.delete(f"/admin/users/{viewer['user_id']}")
