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
    assert restorable_paper in deleted_pids

    # Soft-delete question
    api.delete(f"/questions/{restorable_question}")
    api.get(f"/questions/{restorable_question}", expect=404)

    detail = parse_json(
        api.get(f"/admin/questions/{restorable_question}")[1],
    )
    assert detail["is_deleted"]
    assert detail["deleted_at"] is not None

    # Verify the deleted question no longer appears in active listings
    active_qids = [
        i["question_id"]
        for i in paginated_items(
            api.get("/questions?limit=100")[1]
        )
    ]
    assert restorable_question not in active_qids


def test_restore_flow(api, state):
    """Restore blocked by deleted dependency, then succeed in order."""
    paper_id = state.theory_paper_ids[1]
    question_id = state.rt_q_ids[4]

    # Paper restore blocked because its question is still deleted
    api.post_json(f"/admin/papers/{paper_id}/restore", {}, expect=409)

    # Restore question first
    resp = parse_json(
        api.post_json(f"/admin/questions/{question_id}/restore", {})[1],
    )
    assert not resp["is_deleted"]
    api.get(f"/questions/{question_id}")

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
    for pid in all_papers:
        assert pid in deleted_pids, f"paper {pid} should be in deleted list"

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
    for qid in all_qs:
        assert qid in deleted_qids, f"question {qid} should be in deleted list"

    # GC preview (dry run, rolls back)
    preview = parse_json(
        api.post_json("/admin/garbage-collections/preview", {})[1],
    )
    assert preview["dry_run"] is True
    assert preview["deleted_papers"] >= total_papers
    assert preview["deleted_questions"] >= total_questions
    assert preview["deleted_objects"] > 0
    assert preview["freed_bytes"] > 0

    # Verify preview rolled back (data still visible)
    still_deleted = sorted(
        i["question_id"]
        for i in paginated_items(
            api.get("/admin/questions?state=deleted&limit=50")[1]
        )
    )
    for qid in all_qs:
        assert qid in still_deleted, f"question {qid} should still be deleted after preview"

    # GC run (permanent)
    gc = parse_json(
        api.post_json("/admin/garbage-collections/run", {})[1],
    )
    assert gc["dry_run"] is False
    assert gc["deleted_papers"] >= total_papers
    assert gc["deleted_questions"] >= total_questions
    assert gc["deleted_objects"] > 0
    assert gc["freed_bytes"] > 0

    # Our papers and questions should be permanently gone
    remaining_pids = {
        i["paper_id"]
        for i in paginated_items(
            api.get("/admin/papers?state=all&limit=50")[1]
        )
    }
    for pid in all_papers:
        assert pid not in remaining_pids, f"paper {pid} should be GC'd"
    remaining_qids = {
        i["question_id"]
        for i in paginated_items(
            api.get("/admin/questions?state=all&limit=100")[1]
        )
    }
    for qid in all_qs:
        assert qid not in remaining_qids, f"question {qid} should be GC'd"

    # Restore after GC → 404
    api.post_json(
        f"/admin/questions/{state.rt_q_ids[4]}/restore", {}, expect=404,
    )
    api.post_json(
        f"/admin/papers/{state.theory_paper_ids[1]}/restore", {}, expect=404,
    )

    # GC preview now shows nothing for our items
    empty_gc = parse_json(
        api.post_json("/admin/garbage-collections/preview", {})[1],
    )
    assert empty_gc["dry_run"] is True
    assert empty_gc["deleted_questions"] == 0
    assert empty_gc["deleted_papers"] == 0
    assert empty_gc["deleted_objects"] == 0
    assert empty_gc["freed_bytes"] == 0
