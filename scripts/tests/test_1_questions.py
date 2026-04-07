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
    assert sorted(actual) == sorted(expected_ids), (
        f"query {path}: expected {sorted(expected_ids)}, got {sorted(actual)}"
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
    page = parse_json(api.get("/questions?limit=10&offset=0")[1])
    assert len(page["items"]) == 3
    assert page["total"] == 3
    # All synthetic questions have score=20 from \begin{problem}[20]
    for item in page["items"]:
        assert item["score"] == 20

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
    assert_question_query(
        api,
        "/questions?score_min=21",
        [],
    )
    assert_question_query(
        api,
        "/questions?score_max=19",
        [],
    )


def test_list_question_tags(api, state):
    """List active question tags for frontend autocomplete."""
    tags = parse_json(api.get("/questions/tags")[1])["tags"]
    assert tags == ["kinematics", "lenses", "mechanics", "optics"]


def test_question_detail(api, state):
    qs = state.q_by_slug

    m = parse_json(api.get(f"/questions/{qs['mechanics']}")[1])
    assert m["difficulty"]["human"]["score"] == 4
    assert m["difficulty"]["heuristic"]["notes"] == "fast estimate"
    assert m["score"] == 20  # from \begin{problem}[20]

    o = parse_json(api.get(f"/questions/{qs['optics']}")[1])
    assert o["difficulty"]["symbolic"]["score"] == 9
    assert o["difficulty"]["ml"]["notes"] == "vision model struggle"
    assert o["score"] == 20  # from \begin{problem}[20]


def test_question_bundle(api, state):
    output = DOWNLOADS_DIR / "questions_bundle_synthetic.zip"
    manifest, names = api.download_zip(
        "/questions/bundles", {"question_ids": state.q_ids}, output,
    )
    validate_question_bundle(manifest, names, state.q_ids)


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
