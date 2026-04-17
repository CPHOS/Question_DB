"""Paper CRUD, filtering, file replacement, and bundle tests.

Provides a single ``_run_paper_flow`` helper parameterised for both
theory and experiment categories.
"""

from __future__ import annotations

import json
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

from .config import DOWNLOADS_DIR, INVALID_PAPER_UPLOAD_PATH
from .fixtures import RealQuestionFixture
from .session import ApiClient, parse_json
from .test_1_questions import assert_question_query
from .validators import validate_paper_bundle


@dataclass
class PaperConfig:
    label: str
    category: str
    tag: str
    first_n: int
    paper_a: dict
    paper_b: dict
    patched_a: dict
    template_source: str
    sample_problem_title: str


THEORY_CONFIG = PaperConfig(
    label="theory", category="T", tag="real-batch", first_n=4,
    paper_a={
        "description": "真实理论联考 A", "title": "真实理论联考 A 卷",
        "subtitle": "回归测试 初版",
    },
    paper_b={
        "description": "真实理论联考 B", "title": "真实理论联考 B 卷",
        "subtitle": "六题完整版",
    },
    patched_a={
        "description": "真实理论联考 A（修订）", "title": "真实理论联考 A 卷（修订）",
        "subtitle": "回归测试 终版",
    },
    template_source="CPHOS-Latex/theory/examples/example-paper.tex",
    sample_problem_title="太阳物理初步",
)

EXPERIMENT_CONFIG = PaperConfig(
    label="experiment", category="E", tag="real-exp-batch", first_n=3,
    paper_a={
        "description": "真实实验联考 A", "title": "真实实验联考 A 卷",
        "subtitle": "回归测试 初版",
    },
    paper_b={
        "description": "真实实验联考 B", "title": "真实实验联考 B 卷",
        "subtitle": "四题完整版",
    },
    patched_a={
        "description": "真实实验联考 A（修订）", "title": "真实实验联考 A 卷（修订）",
        "subtitle": "回归测试 终版",
    },
    template_source="CPHOS-Latex/experiment/examples/example-paper.tex",
    sample_problem_title="弗兰克-赫兹实验",
)


# ── Helpers ──────────────────────────────────────────────────────


def _paper_fields(meta: dict, question_ids: list[str]) -> dict[str, str]:
    return {
        "description": meta["description"],
        "title": meta["title"],
        "subtitle": meta["subtitle"],
        "question_ids": json.dumps(question_ids, ensure_ascii=False),
    }


def _exercise_paper_file_replacement(
    api: ApiClient, appendix_paths: dict, paper_id: str,
) -> Path:
    replacement = appendix_paths["mock-b"]

    # Negative
    api.upload(
        "/papers/not-a-uuid/file",
        file_path=replacement, method="PUT", expect=400,
    )
    api.upload(
        "/papers/550e8400-e29b-41d4-a716-446655440000/file",
        file_path=replacement, method="PUT", expect=404,
    )
    api.upload(f"/papers/{paper_id}/file", method="PUT", expect=400)
    api.upload(
        f"/papers/{paper_id}/file",
        file_path=INVALID_PAPER_UPLOAD_PATH, method="PUT", expect=400,
    )

    # Positive
    _, body, _ = api.upload(
        f"/papers/{paper_id}/file", file_path=replacement, method="PUT",
    )
    resp = parse_json(body)
    assert resp["status"] == "replaced"
    assert resp["paper_id"] == paper_id
    return replacement


def _invalid_question_pair(
    category: str, by_slug: dict[str, str],
) -> list[str]:
    if category == "T":
        return [by_slug["mechanics"], by_slug["thermal"]]
    return [by_slug["optics"], by_slug["thermal"]]


def _run_paper_flow(
    api: ApiClient,
    config: PaperConfig,
    appendix_paths: dict[str, Path],
    sample_by_slug: dict[str, str],
    real_q_ids: list[str],
    real_q_by_slug: dict[str, str],
    real_fixtures_by_slug: dict[str, RealQuestionFixture],
) -> list[str]:
    """Create, query, patch, file-replace, bundle-validate papers.

    Returns paper_ids.
    """
    first_n = real_q_ids[: config.first_n]
    reversed_n = list(reversed(first_n))
    a_fields = _paper_fields(config.paper_a, first_n)
    b_fields = _paper_fields(config.paper_b, real_q_ids)

    # ── Negative cases ───────────────────────────────────────
    api.upload(
        "/papers",
        fields={k: v for k, v in a_fields.items() if k != "title"},
        file_path=appendix_paths["mock-a"],
        expect=400,
    )
    api.upload(
        "/papers",
        fields={**a_fields, "description": "bad/name"},
        file_path=appendix_paths["mock-a"],
        expect=400,
    )
    api.upload(
        "/papers",
        fields=a_fields,
        file_path=INVALID_PAPER_UPLOAD_PATH,
        expect=400,
    )
    api.upload(
        "/papers",
        fields={
            **a_fields,
            "question_ids": json.dumps(
                [real_q_ids[0], "550e8400-e29b-41d4-a716-446655440000"]
            ),
        },
        file_path=appendix_paths["mock-a"],
        expect=400,
    )
    api.upload(
        "/papers",
        fields={
            **a_fields,
            "question_ids": json.dumps(
                [sample_by_slug["mechanics"], sample_by_slug["optics"]]
            ),
        },
        file_path=appendix_paths["mock-a"],
        expect=400,
    )
    api.upload(
        "/papers",
        fields={
            **a_fields,
            "question_ids": json.dumps(
                _invalid_question_pair(config.category, sample_by_slug)
            ),
        },
        file_path=appendix_paths["mock-a"],
        expect=400,
    )

    # ── Create ───────────────────────────────────────────────
    paper_a_id = parse_json(
        api.upload(
            "/papers", fields=a_fields, file_path=appendix_paths["mock-a"],
        )[1]
    )["paper_id"]
    paper_b_id = parse_json(
        api.upload(
            "/papers", fields=b_fields, file_path=appendix_paths["mock-b"],
        )[1]
    )["paper_id"]
    paper_ids = [paper_a_id, paper_b_id]

    # ── List & search ────────────────────────────────────────
    items = parse_json(api.get("/papers")[1])["items"]
    ids_in_list = {i["paper_id"] for i in items}
    assert paper_a_id in ids_in_list and paper_b_id in ids_in_list
    for item in items:
        assert "created_by" in item

    _, body, _ = api.get(
        f"/papers?q={urllib.parse.quote(config.paper_b['subtitle'])}"
    )
    assert paper_b_id in body

    # Date range filters
    # created_after far future → no papers
    items_future = parse_json(
        api.get("/papers?created_after=2099-01-01")[1]
    )["items"]
    assert not any(i["paper_id"] in paper_ids for i in items_future)

    # created_before far future → both papers present
    items_past = parse_json(
        api.get("/papers?created_before=2099-12-31")[1]
    )["items"]
    found_ids = {i["paper_id"] for i in items_past}
    assert paper_a_id in found_ids and paper_b_id in found_ids

    # updated_after far future → no papers
    items_upd = parse_json(
        api.get("/papers?updated_after=2099-01-01T00:00:00Z")[1]
    )["items"]
    assert not any(i["paper_id"] in paper_ids for i in items_upd)

    # updated_before far future → both present
    items_upd2 = parse_json(
        api.get("/papers?updated_before=2099-12-31T23:59:59Z")[1]
    )["items"]
    found_ids2 = {i["paper_id"] for i in items_upd2}
    assert paper_a_id in found_ids2 and paper_b_id in found_ids2

    # Invalid date → 400
    api.get("/papers?created_after=not-a-date", expect=400)
    api.get("/papers?updated_before=xyz", expect=400)

    # ── Detail ───────────────────────────────────────────────
    detail = parse_json(api.get(f"/papers/{paper_a_id}")[1])
    assert [q["question_id"] for q in detail["questions"]] == first_n
    assert "created_by" in detail  # paper-level ownership tracking
    # Verify each question carries full metadata (QuestionSummary shape)
    for q in detail["questions"]:
        assert "description" in q
        assert q["category"] == config.category
        assert q["status"] in ("reviewed", "used")
        assert isinstance(q["tags"], list)
        assert isinstance(q["difficulty"], dict)
        assert "author" in q
        assert isinstance(q["reviewers"], list)
        assert "created_at" in q
        assert "updated_at" in q
        assert "created_by" in q  # may be null for legacy data
        # score may be null
        assert "score" in q

    # ── Patch ────────────────────────────────────────────────
    _, body, _ = api.patch_json(
        f"/papers/{paper_a_id}",
        {
            "description": config.patched_a["description"],
            "title": config.patched_a["title"],
            "subtitle": config.patched_a["subtitle"],
            "question_ids": reversed_n,
        },
    )
    assert parse_json(body)["title"] == config.patched_a["title"]

    # Patch negative
    api.patch_json(
        f"/papers/{paper_a_id}", {"description": "bad/name"}, expect=400,
    )
    api.patch_json(
        f"/papers/{paper_a_id}", {"question_ids": []}, expect=400,
    )
    mixed_id = (
        sample_by_slug["optics"]
        if config.category == "T"
        else sample_by_slug["mechanics"]
    )
    api.patch_json(
        f"/papers/{paper_a_id}",
        {"question_ids": [real_q_ids[0], mixed_id]},
        expect=400,
    )

    # Verify patch
    detail = parse_json(api.get(f"/papers/{paper_a_id}")[1])
    assert [q["question_id"] for q in detail["questions"]] == reversed_n

    # Cross-check question→paper queries
    assert_question_query(
        api,
        f"/questions?paper_id={urllib.parse.quote(paper_a_id)}",
        reversed_n,
    )
    assert_question_query(
        api,
        f"/questions?paper_id={urllib.parse.quote(paper_b_id)}"
        f"&tag={config.tag}&category={config.category}",
        real_q_ids,
    )

    # ── File replacement (theory only) ───────────────────────
    if config.label == "theory":
        replaced_path = _exercise_paper_file_replacement(
            api, appendix_paths, paper_a_id,
        )
    else:
        replaced_path = appendix_paths["mock-a"]

    # ── Bundle ───────────────────────────────────────────────
    bundle = DOWNLOADS_DIR / f"papers_bundle_{config.label}.zip"
    manifest, names = api.download_zip(
        "/papers/bundles", {"paper_ids": paper_ids}, bundle,
    )
    acounts = {
        real_q_by_slug[f.slug]: f.asset_count
        for f in real_fixtures_by_slug.values()
    }
    validate_paper_bundle(
        manifest,
        names,
        paper_ids,
        bundle,
        {
            paper_a_id: {
                "appendix_path": replaced_path,
                "title": config.patched_a["title"],
                "subtitle": config.patched_a["subtitle"],
                "question_ids": reversed_n,
                "asset_total": sum(acounts[q] for q in reversed_n),
            },
            paper_b_id: {
                "appendix_path": appendix_paths["mock-b"],
                "title": config.paper_b["title"],
                "subtitle": config.paper_b["subtitle"],
                "question_ids": real_q_ids,
                "asset_total": sum(acounts[q] for q in real_q_ids),
            },
        },
        config.template_source,
        config.category,
        config.sample_problem_title,
    )

    return paper_ids


# ── Tests ────────────────────────────────────────────────────────


def test_theory_papers(api, state):
    state.theory_paper_ids = _run_paper_flow(
        api,
        THEORY_CONFIG,
        state.appendix_paths,
        state.q_by_slug,
        state.rt_q_ids,
        state.rt_q_by_slug,
        state.rt_fixtures,
    )


def test_experiment_papers(api, state):
    state.experiment_paper_ids = _run_paper_flow(
        api,
        EXPERIMENT_CONFIG,
        state.appendix_paths,
        state.q_by_slug,
        state.re_q_ids,
        state.re_q_by_slug,
        state.re_fixtures,
    )


def test_paper_bundle_validation(api):
    """Empty and malformed IDs are rejected before bundling."""
    api.post_json("/papers/bundles", {"paper_ids": []}, expect=400)
    api.post_json(
        "/papers/bundles", {"paper_ids": ["not-a-uuid"]}, expect=400,
    )


def test_bot_can_modify_any_paper(api, state):
    """Bot role can PATCH/PUT/DELETE papers created by other users."""
    bot = api.ensure_user({
        "username": "e2e_bot_paper",
        "role": "bot",
    })
    bot_token = bot["access_token"]

    # Papers in state were created by admin; bot should still be able to modify them.
    paper_id = state.theory_paper_ids[0]
    original = parse_json(api.get(f"/papers/{paper_id}")[1])

    saved = api._access_token
    try:
        api.set_token(bot_token)

        # PATCH — bot patches a paper it does not own
        _, body, _ = api.patch_json(
            f"/papers/{paper_id}",
            {"subtitle": "bot 修订"},
        )
        assert parse_json(body)["subtitle"] == "bot 修订"

        # PUT file — bot replaces appendix on a paper it does not own
        api.upload(
            f"/papers/{paper_id}/file",
            file_path=state.appendix_paths["mock-a"],
            method="PUT",
        )

        # Restore original subtitle before further tests depend on it
        api.patch_json(
            f"/papers/{paper_id}",
            {"subtitle": original["subtitle"]},
        )
    finally:
        api.set_token(saved)
        api.delete(f"/admin/users/{bot['user_id']}")


def test_user_cannot_create_paper(api, state):
    """User role cannot create papers (requires leader or above)."""
    user = api.ensure_user({
        "username": "e2e_user_nopaper",
        "password": "usernopaper1",
        "role": "user",
    })

    saved = api._access_token
    try:
        api.login("e2e_user_nopaper", "usernopaper1")
        api.upload(
            "/papers",
            fields=_paper_fields(
                {"description": "forbidden", "title": "No", "subtitle": "No"},
                state.rt_q_ids[:2],
            ),
            file_path=state.appendix_paths["mock-a"],
            expect=403,
        )
    finally:
        api.set_token(saved)
        api.delete(f"/admin/users/{user['user_id']}")
