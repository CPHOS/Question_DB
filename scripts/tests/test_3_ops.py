"""Export, quality-check, and bundle permission tests."""

from __future__ import annotations

from .config import DOWNLOADS_DIR, EXPORT_PATH, QUALITY_PATH
from .session import parse_json


# ── Tests ────────────────────────────────────────────────────────


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
    assert resp["exported_questions"] == state.total_question_count


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


def test_viewer_can_download_bundles_but_not_ops(api, state):
    """Viewer can download bundles, but exports and quality checks stay editor-only."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "bundle_viewer",
        "password": "viewer123",
        "role": "viewer",
    })
    viewer = parse_json(body)

    saved = api._access_token
    try:
        api.login("bundle_viewer", "viewer123")

        q_manifest, _ = api.download_zip(
            "/questions/bundles",
            {"question_ids": [state.q_ids[0]]},
            DOWNLOADS_DIR / "viewer_questions_bundle.zip",
        )
        assert q_manifest["kind"] == "question_bundle"

        p_manifest, _ = api.download_zip(
            "/papers/bundles",
            {"paper_ids": [state.all_paper_ids[0]]},
            DOWNLOADS_DIR / "viewer_papers_bundle.zip",
        )
        assert p_manifest["kind"] == "paper_bundle"

        api.post_json("/exports/run", {"format": "jsonl"}, expect=403)
        api.post_json("/quality-checks/run", {}, expect=403)
    finally:
        api.set_token(saved)
        api.delete(f"/admin/users/{viewer['user_id']}")
