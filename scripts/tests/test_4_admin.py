"""Admin soft-delete, restore, and garbage-collection tests."""

from __future__ import annotations

from .session import paginated_items, parse_json


# ── Tests ────────────────────────────────────────────────────────


def test_delete_question_in_active_paper(api, state):
    """Cannot delete a question that belongs to an active paper."""
    api.delete(f"/questions/{state.rt_q_ids[4]}", expect=409)


def test_soft_delete_and_admin_visibility(api, state):
    """Soft-delete one paper and one question, verify admin view."""
    restorable_paper = state.theory_paper_ids[1]
    restorable_question = state.rt_q_ids[4]
    restorable_tag = state.real_theory_fixtures[4].patch["tags"][-1]

    # Soft-delete paper
    api.delete(f"/papers/{restorable_paper}")
    api.get(f"/papers/{restorable_paper}", expect=404)

    detail = parse_json(api.get(f"/admin/papers/{restorable_paper}")[1])
    assert detail["is_deleted"]
    assert detail["deleted_at"] is not None

    deleted_pids = [
        i["paper_id"]
        for i in paginated_items(
            api.get("/admin/papers?state=deleted&limit=10")[1]
        )
    ]
    assert sorted(deleted_pids) == [restorable_paper]

    # Soft-delete question
    api.delete(f"/questions/{restorable_question}")
    api.get(f"/questions/{restorable_question}", expect=404)

    detail = parse_json(
        api.get(f"/admin/questions/{restorable_question}")[1],
    )
    assert detail["is_deleted"]
    assert detail["deleted_at"] is not None

    active_tags = parse_json(api.get("/questions/tags")[1])["tags"]
    assert restorable_tag not in active_tags


def test_restore_flow(api, state):
    """Restore blocked by deleted dependency, then succeed in order."""
    paper_id = state.theory_paper_ids[1]
    question_id = state.rt_q_ids[4]
    restored_tag = state.real_theory_fixtures[4].patch["tags"][-1]

    # Paper restore blocked because its question is still deleted
    api.post_json(f"/admin/papers/{paper_id}/restore", {}, expect=409)

    # Restore question first
    resp = parse_json(
        api.post_json(f"/admin/questions/{question_id}/restore", {})[1],
    )
    assert not resp["is_deleted"]
    api.get(f"/questions/{question_id}")
    assert restored_tag in parse_json(api.get("/questions/tags")[1])["tags"]

    # Now paper restore succeeds
    resp = parse_json(
        api.post_json(f"/admin/papers/{paper_id}/restore", {})[1],
    )
    assert not resp["is_deleted"]
    api.get(f"/papers/{paper_id}")


def test_gc_flow(api, state):
    """Bulk delete everything → GC preview → GC run → verify empty."""
    all_papers = state.all_paper_ids
    all_qs = state.all_real_q_ids + state.q_ids
    total_papers = len(all_papers)
    total_questions = len(all_qs)

    # Soft-delete all papers
    for pid in reversed(all_papers):
        api.delete(f"/papers/{pid}")
    api.get(f"/papers/{all_papers[0]}", expect=404)

    deleted_pids = sorted(
        i["paper_id"]
        for i in paginated_items(
            api.get("/admin/papers?state=deleted&limit=10")[1]
        )
    )
    assert deleted_pids == sorted(all_papers)

    # Soft-delete all questions
    for qid in reversed(all_qs):
        api.delete(f"/questions/{qid}")
    api.get(f"/questions/{all_qs[0]}", expect=404)

    deleted_qids = sorted(
        i["question_id"]
        for i in paginated_items(
            api.get("/admin/questions?state=deleted&limit=50")[1]
        )
    )
    assert deleted_qids == sorted(all_qs)

    # GC preview (dry run, rolls back)
    preview = parse_json(
        api.post_json("/admin/garbage-collections/preview", {})[1],
    )
    assert preview["dry_run"] is True
    assert preview["deleted_papers"] == total_papers
    assert preview["deleted_questions"] == total_questions
    assert preview["deleted_objects"] > 0
    assert preview["freed_bytes"] > 0

    # Verify preview rolled back (data still visible)
    still_deleted = sorted(
        i["question_id"]
        for i in paginated_items(
            api.get("/admin/questions?state=deleted&limit=50")[1]
        )
    )
    assert still_deleted == sorted(all_qs)

    # GC run (permanent)
    gc = parse_json(
        api.post_json("/admin/garbage-collections/run", {})[1],
    )
    assert gc["dry_run"] is False
    assert (
        {k: v for k, v in gc.items() if k != "dry_run"}
        == {k: v for k, v in preview.items() if k != "dry_run"}
    )

    # Everything gone
    assert paginated_items(
        api.get("/admin/papers?state=all&limit=10")[1]
    ) == []
    assert paginated_items(
        api.get("/admin/questions?state=all&limit=50")[1]
    ) == []

    # Restore after GC → 404
    api.post_json(
        f"/admin/questions/{state.rt_q_ids[4]}/restore", {}, expect=404,
    )
    api.post_json(
        f"/admin/papers/{state.theory_paper_ids[1]}/restore", {}, expect=404,
    )

    # GC preview now empty
    empty_gc = parse_json(
        api.post_json("/admin/garbage-collections/preview", {})[1],
    )
    assert empty_gc == {
        "dry_run": True,
        "deleted_questions": 0,
        "deleted_papers": 0,
        "deleted_objects": 0,
        "freed_bytes": 0,
    }
