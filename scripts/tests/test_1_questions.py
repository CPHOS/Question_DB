"""Question CRUD, filtering, file replacement, and real data upload."""

from __future__ import annotations

import json
import urllib.parse

from .config import DOWNLOADS_DIR, INVALID_PAPER_UPLOAD_PATH
from .fixtures import RealQuestionFixture
from .session import (
    ApiClient,
    build_question_fields,
    parse_json,
    question_ids_from_body,
)
from .specs import QUESTION_SPECS
from .validators import validate_question_bundle


# ── Helpers (also used by test_2_papers) ─────────────────────────


def assert_question_query(
    api: ApiClient, path: str, expected_ids: list[str],
) -> None:
    _, body, _ = api.get(path)
    actual = question_ids_from_body(body)
    if expected_ids:
        missing = set(expected_ids) - set(actual)
        assert not missing, (
            f"query {path}: missing {sorted(missing)} from {sorted(actual)}"
        )
    else:
        assert actual == [], (
            f"query {path}: expected empty, got {sorted(actual)}"
        )


def upload_real_questions(
    api: ApiClient,
    fixtures: list[RealQuestionFixture],
    *,
    category: str,
    tag: str,
) -> tuple[list[str], dict[str, str], dict[str, RealQuestionFixture]]:
    ids: list[str] = []
    by_slug: dict[str, str] = {}
    fixtures_by_slug = {f.slug: f for f in fixtures}

    for f in fixtures:
        _, body, _ = api.upload(
            "/questions",
            fields=build_question_fields(
                description=f.patch["description"],
                category=f.patch["category"],
                tags=f.patch["tags"],
                status=f.patch["status"],
                difficulty=f.patch["difficulty"],
                author=f.patch.get("author"),
                reviewers=f.patch.get("reviewers"),
            ),
            file_path=f.upload_path,
        )
        resp = parse_json(body)
        assert resp["status"] == "imported"
        assert resp["imported_assets"] == f.asset_count
        ids.append(resp["question_id"])
        by_slug[f.slug] = resp["question_id"]

    # Spot-check first question
    detail = parse_json(api.get(f"/questions/{ids[0]}")[1])
    assert detail["category"] == category
    assert detail["status"] in {"reviewed", "used"}
    assert isinstance(detail["score"], int)  # real questions always have a score
    assert_question_query(
        api, f"/questions?category={category}&tag={tag}", ids,
    )
    return ids, by_slug, fixtures_by_slug


# ── Tests ────────────────────────────────────────────────────────


def test_health(api):
    _, body, _ = api.get("/health")
    assert parse_json(body)["status"] == "ok"


def test_create_question_validation(api, state):
    """Negative cases for POST /questions."""
    spec = QUESTION_SPECS[0]
    zp = state.synthetic_zips[0]

    # Missing required fields
    api.upload("/questions", fields=None, file_path=zp, expect=400)
    api.upload(
        "/questions",
        fields={"description": spec["create_description"]},
        file_path=zp,
        expect=400,
    )

    # Invalid description (contains /)
    api.upload(
        "/questions",
        fields=build_question_fields(
            description="bad/name", difficulty=spec["create_difficulty"],
        ),
        file_path=zp,
        expect=400,
    )

    # Invalid category
    api.upload(
        "/questions",
        fields=build_question_fields(
            description=spec["create_description"],
            category="X",
            difficulty=spec["create_difficulty"],
        ),
        file_path=zp,
        expect=400,
    )

    # Invalid tags (not an array)
    api.upload(
        "/questions",
        fields={
            "description": spec["create_description"],
            "difficulty": json.dumps(spec["create_difficulty"]),
            "tags": '"not-an-array"',
        },
        file_path=zp,
        expect=400,
    )

    # Difficulty: missing required human key
    api.upload(
        "/questions",
        fields=build_question_fields(
            description=spec["create_description"],
            difficulty={"heuristic": {"score": 5}},
        ),
        file_path=zp,
        expect=400,
    )

    # Difficulty: score out of range
    api.upload(
        "/questions",
        fields=build_question_fields(
            description=spec["create_description"],
            difficulty={"human": {"score": 11}},
        ),
        file_path=zp,
        expect=400,
    )


def test_create_synthetic_questions(api, state):
    """Create 3 synthetic questions and store IDs in shared state."""
    for spec, zp in zip(QUESTION_SPECS, state.synthetic_zips):
        _, body, _ = api.upload(
            "/questions",
            fields=build_question_fields(
                description=spec["patch"]["description"],
                category=spec["patch"]["category"],
                tags=spec["patch"]["tags"],
                status=spec["patch"]["status"],
                difficulty=spec["patch"]["difficulty"],
                author=spec["patch"].get("author"),
                reviewers=spec["patch"].get("reviewers"),
            ),
            file_path=zp,
        )
        resp = parse_json(body)
        assert resp["status"] == "imported"
        state.q_ids.append(resp["question_id"])
        state.q_by_slug[spec["slug"]] = resp["question_id"]


def test_patch_questions(api, state):
    """Patch validation + valid patches."""
    qs = state.q_by_slug

    # Empty patch body → 400
    api.patch_json(f"/questions/{qs['mechanics']}", {}, expect=400)

    # Difficulty without human → 400
    api.patch_json(
        f"/questions/{qs['mechanics']}",
        {"difficulty": {"heuristic": {"score": 5}}},
        expect=400,
    )

    # Valid: clear tags + set multi-source difficulty
    _, body, _ = api.patch_json(
        f"/questions/{qs['thermal']}",
        {
            "tags": [],
            "difficulty": {
                "human": {"score": 5, "notes": ""},
                "heuristic": {"score": 4, "notes": "direct model"},
                "simulator": {"score": 6},
            },
        },
    )
    assert parse_json(body)["tags"] == []


def test_filter_questions(api, state):
    """List, search, and difficulty-range filters."""
    page = parse_json(api.get("/questions?limit=100&offset=0")[1])
    assert page["total"] >= 3
    page_ids = {item["question_id"] for item in page["items"]}
    for qid in state.q_ids:
        assert qid in page_ids, f"synthetic question {qid} not in list"
    for item in page["items"]:
        assert "created_by" in item

    qs = state.q_by_slug

    assert_question_query(
        api,
        "/questions?q=%E7%83%AD%E5%AD%A6"
        "&difficulty_tag=human&difficulty_min=5&difficulty_max=5",
        [qs["thermal"]],
    )
    assert_question_query(
        api,
        "/questions?category=T&tag=mechanics"
        "&difficulty_tag=human&difficulty_max=4",
        [qs["mechanics"]],
    )
    assert_question_query(
        api,
        "/questions?difficulty_tag=heuristic&difficulty_max=5",
        [qs["mechanics"], qs["thermal"]],
    )
    assert_question_query(
        api,
        "/questions?tag=optics&difficulty_tag=symbolic&difficulty_min=8",
        [qs["optics"]],
    )
    assert_question_query(
        api,
        "/questions?difficulty_tag=ml&difficulty_min=8&tag=optics&category=E",
        [qs["optics"]],
    )

    # Invalid: difficulty range without tag
    api.get("/questions?difficulty_min=5", expect=400)
    # Invalid: min > max
    api.get(
        "/questions?difficulty_tag=human&difficulty_min=8&difficulty_max=3",
        expect=400,
    )
    # Invalid: score_min > score_max
    api.get("/questions?score_min=50&score_max=10", expect=400)

    # Score filter: all synthetic questions have score=20
    assert_question_query(
        api,
        "/questions?score_min=20&score_max=20",
        list(qs.values()),
    )
    # Verify score filtering works: synthetic questions (score=20)
    # should NOT appear with score_min=21
    page_21 = parse_json(api.get("/questions?score_min=21")[1])
    ids_21 = {item["question_id"] for item in page_21["items"]}
    for qid in qs.values():
        assert qid not in ids_21, f"synthetic question {qid} should not appear with score_min=21"
    # should NOT appear with score_max=19
    page_19 = parse_json(api.get("/questions?score_max=19")[1])
    ids_19 = {item["question_id"] for item in page_19["items"]}
    for qid in qs.values():
        assert qid not in ids_19, f"synthetic question {qid} should not appear with score_max=19"

    # Date range filters
    # created_after far future → no results
    assert_question_query(
        api,
        "/questions?created_after=2099-01-01",
        [],
    )
    # created_before far future → all results
    assert_question_query(
        api,
        "/questions?created_before=2099-12-31",
        list(qs.values()),
    )
    # created_after very old → all results
    assert_question_query(
        api,
        "/questions?created_after=2000-01-01",
        list(qs.values()),
    )
    # updated_after far future → no results
    assert_question_query(
        api,
        "/questions?updated_after=2099-01-01T00:00:00Z",
        [],
    )
    # updated_before far future → all results
    assert_question_query(
        api,
        "/questions?updated_before=2099-12-31T23:59:59Z",
        list(qs.values()),
    )
    # Combined: created_after old + created_before future → all
    assert_question_query(
        api,
        "/questions?created_after=2000-01-01&created_before=2099-12-31",
        list(qs.values()),
    )
    # Invalid date format → 400
    api.get("/questions?created_after=not-a-date", expect=400)
    api.get("/questions?updated_before=abc", expect=400)


def test_filter_questions_by_reviewer(api, state):
    """Filter questions by reviewer name."""
    qs = state.q_by_slug

    # 李四 is a reviewer on mechanics
    assert_question_query(
        api,
        "/questions?reviewer=%E6%9D%8E%E5%9B%9B",
        [qs["mechanics"]],
    )
    # 钱七 is a reviewer on optics
    assert_question_query(
        api,
        "/questions?reviewer=%E9%92%B1%E4%B8%83",
        [qs["optics"]],
    )
    # 吴十 is a reviewer on thermal
    assert_question_query(
        api,
        "/questions?reviewer=%E5%90%B4%E5%8D%81",
        [qs["thermal"]],
    )
    # Non-existent reviewer → none of our synthetic questions
    page = parse_json(api.get("/questions?reviewer=nobody")[1])
    for qid in qs.values():
        assert qid not in {i["question_id"] for i in page["items"]}

    # Combine reviewer + category
    assert_question_query(
        api,
        "/questions?reviewer=%E6%9D%8E%E5%9B%9B&category=T",
        [qs["mechanics"]],
    )


def test_list_question_tags(api, state):
    """List active question tags for frontend autocomplete."""
    tags = parse_json(api.get("/questions/tags")[1])["tags"]
    for expected in ["kinematics", "lenses", "mechanics", "optics"]:
        assert expected in tags, f"tag {expected!r} not in {tags}"
    assert tags == sorted(tags), "tags should be sorted alphabetically"


def test_list_difficulty_tags(api, state):
    """List active difficulty tags for frontend dropdown selection."""
    resp = parse_json(api.get("/questions/difficulty-tags")[1])
    dtags = resp["difficulty_tags"]
    # After patching, the synthetic questions use: human, heuristic, ml, symbolic, simulator
    assert "human" in dtags, "human must always be present"
    assert "heuristic" in dtags
    assert "ml" in dtags
    assert "symbolic" in dtags
    assert "simulator" in dtags
    # Should be sorted alphabetically
    assert dtags == sorted(dtags)


def test_question_detail(api, state):
    qs = state.q_by_slug

    m = parse_json(api.get(f"/questions/{qs['mechanics']}")[1])
    assert m["difficulty"]["human"]["score"] == 4
    assert m["difficulty"]["heuristic"]["notes"] == "fast estimate"
    assert m["score"] == 20  # from \begin{problem}[20]
    assert "created_by" in m  # ownership tracking

    o = parse_json(api.get(f"/questions/{qs['optics']}")[1])
    assert o["difficulty"]["symbolic"]["score"] == 9
    assert o["difficulty"]["ml"]["notes"] == "vision model struggle"
    assert o["score"] == 20  # from \begin{problem}[20]
    assert "created_by" in o


def test_question_bundle(api, state):
    output = DOWNLOADS_DIR / "questions_bundle_synthetic.zip"
    manifest, names = api.download_zip(
        "/questions/bundles", {"question_ids": state.q_ids}, output,
    )
    validate_question_bundle(manifest, names, state.q_ids)


def test_question_bundle_validation(api):
    """Empty and malformed IDs are rejected before bundling."""
    api.post_json("/questions/bundles", {"question_ids": []}, expect=400)
    api.post_json(
        "/questions/bundles", {"question_ids": ["not-a-uuid"]}, expect=400,
    )


def test_question_file_replacement(api, state):
    mid = state.q_by_slug["mechanics"]
    original = parse_json(api.get(f"/questions/{mid}")[1])
    replacement_zip = state.synthetic_zips[1]

    # Negative cases
    api.upload(
        "/questions/not-a-uuid/file",
        file_path=replacement_zip, method="PUT", expect=400,
    )
    api.upload(
        "/questions/550e8400-e29b-41d4-a716-446655440000/file",
        file_path=replacement_zip, method="PUT", expect=404,
    )
    api.upload(f"/questions/{mid}/file", method="PUT", expect=400)  # no file
    api.upload(
        f"/questions/{mid}/file",
        file_path=INVALID_PAPER_UPLOAD_PATH, method="PUT", expect=400,
    )
    api.upload(
        f"/questions/{mid}/file",
        file_path=state.appendix_paths["mock-a"], method="PUT", expect=400,
    )

    # Positive
    _, body, _ = api.upload(
        f"/questions/{mid}/file", file_path=replacement_zip, method="PUT",
    )
    resp = parse_json(body)
    assert resp["status"] == "replaced"
    assert resp["question_id"] == mid

    # Verify metadata preserved, file changed
    replaced = parse_json(api.get(f"/questions/{mid}")[1])
    assert replaced["source"]["tex"] == QUESTION_SPECS[1]["tex_name"]
    assert replaced["category"] == original["category"]
    assert replaced["tags"] == original["tags"]
    assert replaced["difficulty"] == original["difficulty"]
    assert replaced["status"] == original["status"]
    assert replaced["description"] == original["description"]
    assert replaced["score"] == original["score"]  # same tex template, score preserved
    assert replaced["tex_object_id"] != original["tex_object_id"]
    assert replaced["updated_at"] != original["updated_at"]


def test_upload_real_theory_questions(api, state):
    ids, by_slug, fixtures = upload_real_questions(
        api, state.real_theory_fixtures, category="T", tag="real-batch",
    )
    state.rt_q_ids = ids
    state.rt_q_by_slug = by_slug
    state.rt_fixtures = fixtures


def test_upload_real_experiment_questions(api, state):
    ids, by_slug, fixtures = upload_real_questions(
        api, state.real_experiment_fixtures, category="E", tag="real-exp-batch",
    )
    state.re_q_ids = ids
    state.re_q_by_slug = by_slug
    state.re_fixtures = fixtures


# ── Reviewer management tests ───────────────────────────────────


def test_reviewer_crud(api, state):
    """Assign, list, and remove a reviewer on a question."""
    question_id = state.q_ids[0]

    # Create a user-role account to be the reviewer
    reviewer = api.ensure_user({
        "username": "e2e_reviewer",
        "password": "reviewer123",
        "role": "user",
    })
    reviewer_id = reviewer["user_id"]

    try:
        # Assign reviewer (admin can do this)
        _, body, _ = api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": reviewer_id},
        )
        resp = parse_json(body)
        assert len(resp["reviewers"]) == 1
        assert resp["reviewers"][0]["reviewer_id"] == reviewer_id
        assert resp["reviewers"][0]["username"] == "e2e_reviewer"

        # List reviewers
        _, body, _ = api.get(f"/questions/{question_id}/reviewers")
        resp = parse_json(body)
        assert len(resp["reviewers"]) == 1

        # Duplicate assign is idempotent
        _, body, _ = api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": reviewer_id},
        )
        resp = parse_json(body)
        assert len(resp["reviewers"]) == 1

        # Remove reviewer
        _, body, _ = api.delete(
            f"/questions/{question_id}/reviewers/{reviewer_id}",
        )
        resp = parse_json(body)
        assert len(resp["reviewers"]) == 0
    finally:
        api.delete(f"/admin/users/{reviewer_id}")


def test_reviewer_role_restriction(api, state):
    """Only user-role accounts can be assigned as reviewers."""
    question_id = state.q_ids[0]

    # Create a viewer-role account
    viewer = api.ensure_user({
        "username": "e2e_viewer_no_review",
        "password": "viewer12345",
        "role": "viewer",
    })

    try:
        # Assigning a viewer as reviewer should fail
        api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": viewer["user_id"]},
            expect=400,
        )
    finally:
        api.delete(f"/admin/users/{viewer['user_id']}")


def test_viewer_cannot_assign_reviewer(api, state):
    """Viewer cannot assign reviewers (requires leader or above)."""
    question_id = state.q_ids[0]

    # Create a user to be the reviewer and a viewer to attempt the action
    target = api.ensure_user({
        "username": "e2e_review_target",
        "password": "target12345",
        "role": "user",
    })

    viewer = api.ensure_user({
        "username": "e2e_viewer_assign",
        "password": "viewer12345",
        "role": "viewer",
    })

    saved = api._access_token
    try:
        api.login("e2e_viewer_assign", "viewer12345")
        api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": target["user_id"]},
            expect=403,
        )
    finally:
        api.set_token(saved)
        api.delete(f"/admin/users/{target['user_id']}")
        api.delete(f"/admin/users/{viewer['user_id']}")


# ── assigned_reviewer_id filter tests ────────────────────────────


def test_filter_questions_by_assigned_reviewer_id(api, state):
    """Filter questions by assigned_reviewer_id (reviewer management UUID)."""
    question_id = state.q_ids[0]

    # Create a user to assign as reviewer
    reviewer_user = api.ensure_user({
        "username": "e2e_assigned_filter",
        "password": "filter12345",
        "role": "user",
    })
    reviewer_id = reviewer_user["user_id"]

    try:
        # Assign reviewer to the first question
        api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": reviewer_id},
        )

        # Filter by assigned_reviewer_id — should find the question
        _, body, _ = api.get(f"/questions?assigned_reviewer_id={reviewer_id}")
        data = parse_json(body)
        found_ids = [q["question_id"] for q in data["items"]]
        assert question_id in found_ids, (
            f"expected {question_id} in results when filtering by assigned reviewer"
        )

        # A non-assigned user should NOT have this question
        _, body2, _ = api.get(
            "/questions?assigned_reviewer_id=00000000-0000-0000-0000-000000000000"
        )
        data2 = parse_json(body2)
        found_ids2 = [q["question_id"] for q in data2["items"]]
        assert question_id not in found_ids2

        # Invalid UUID → 400
        api.get("/questions?assigned_reviewer_id=not-a-uuid", expect=400)

        # Cleanup: remove reviewer assignment
        api.delete(f"/questions/{question_id}/reviewers/{reviewer_id}")
    finally:
        api.delete(f"/admin/users/{reviewer_id}")


def test_filter_assigned_reviewer_id_for_my_reviews(api, state):
    """User can filter 'my reviews' using assigned_reviewer_id with own user_id."""
    q1 = state.q_ids[0]
    q2 = state.q_ids[1] if len(state.q_ids) > 1 else None

    reviewer_user = api.ensure_user({
        "username": "e2e_my_reviews",
        "password": "myrev12345",
        "role": "user",
    })
    reviewer_id = reviewer_user["user_id"]

    try:
        # Assign to q1 only
        api.post_json(
            f"/questions/{q1}/reviewers",
            {"reviewer_id": reviewer_id},
        )

        # Login as the reviewer user and filter by own ID
        saved = api._access_token
        api.login("e2e_my_reviews", "myrev12345")

        _, body, _ = api.get(f"/questions?assigned_reviewer_id={reviewer_id}")
        data = parse_json(body)
        found_ids = [q["question_id"] for q in data["items"]]
        assert q1 in found_ids
        if q2:
            assert q2 not in found_ids

        api.set_token(saved)

        # Cleanup
        api.delete(f"/questions/{q1}/reviewers/{reviewer_id}")
    finally:
        api.login("admin", "changeme")
        api.delete(f"/admin/users/{reviewer_id}")


# ── difficulty updated_by tests ──────────────────────────────────


def test_difficulty_updated_by_present(api, state):
    """Difficulty values include updated_by with editor info after PATCH."""
    question_id = state.q_ids[0]

    # Get the question detail and check difficulty updated_by
    _, body, _ = api.get(f"/questions/{question_id}")
    detail = parse_json(body)
    difficulty = detail["difficulty"]

    # After the patch phase, difficulty has been set by admin
    assert "human" in difficulty
    human = difficulty["human"]
    assert "updated_by" in human
    assert human["updated_by"] is not None
    assert "user_id" in human["updated_by"]
    assert "username" in human["updated_by"]
    assert "display_name" in human["updated_by"]


def test_difficulty_updated_by_tracks_editor(api, state):
    """updated_by correctly tracks which user last edited a difficulty tag."""
    question_id = state.q_ids[0]

    # Create a user and assign as reviewer
    reviewer = api.ensure_user({
        "username": "e2e_diff_editor",
        "password": "editor12345",
        "role": "user",
    })
    reviewer_id = reviewer["user_id"]

    try:
        # Assign as reviewer
        api.post_json(
            f"/questions/{question_id}/reviewers",
            {"reviewer_id": reviewer_id},
        )

        # Login as reviewer and add a new difficulty tag
        saved = api._access_token
        api.login("e2e_diff_editor", "editor12345")
        api.patch_json(f"/questions/{question_id}", {
            "difficulty": {
                "e2e_test_tag": {"score": 3, "notes": "test tag"},
            },
        })

        # Check updated_by on the new tag
        _, body, _ = api.get(f"/questions/{question_id}")
        detail = parse_json(body)
        test_tag = detail["difficulty"].get("e2e_test_tag")
        assert test_tag is not None
        assert test_tag["updated_by"] is not None
        assert test_tag["updated_by"]["user_id"] == reviewer_id
        assert test_tag["updated_by"]["username"] == "e2e_diff_editor"

        # Cleanup: switch back to admin and delete the tag
        api.set_token(saved)
        # Re-patch as admin with full replace (removes e2e_test_tag)
        _, body2, _ = api.get(f"/questions/{question_id}")
        current_diff = parse_json(body2)["difficulty"]
        # Build new difficulty without e2e_test_tag, keeping human
        clean_diff = {}
        for k, v in current_diff.items():
            if k != "e2e_test_tag":
                clean_diff[k] = {"score": v["score"]}
                if v.get("notes"):
                    clean_diff[k]["notes"] = v["notes"]
        api.patch_json(f"/questions/{question_id}", {
            "difficulty": clean_diff,
        })

        api.delete(f"/questions/{question_id}/reviewers/{reviewer_id}")
    finally:
        api.login("admin", "changeme")
        api.delete(f"/admin/users/{reviewer_id}")
