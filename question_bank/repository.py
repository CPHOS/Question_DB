from __future__ import annotations

import json
from pathlib import Path

from .db import connect


def list_papers(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT paper_id, edition, paper_type, title, paper_latex_path, source_pdf_path, question_index_json, notes FROM papers ORDER BY edition, paper_id"
        ).fetchall()
        payload = []
        for row in rows:
            item = dict(row)
            item["question_index"] = json.loads(item.pop("question_index_json"))
            payload.append(item)
        return payload


def get_paper_detail(db_path: Path, paper_id: str) -> dict | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT paper_id, edition, paper_type, title, paper_latex_path, paper_latex_source,
                   source_pdf_path, question_index_json, notes, created_at, updated_at
            FROM papers
            WHERE paper_id = ?
            """,
            (paper_id,),
        ).fetchone()
        if row is None:
            return None
        questions = conn.execute(
            """
            SELECT question_id, paper_index, question_no, category, latex_path, answer_latex_path,
                   latex_anchor, status, tags_json
            FROM questions
            WHERE paper_id = ?
            ORDER BY paper_index
            """,
            (paper_id,),
        ).fetchall()
        payload = dict(row)
        payload["question_index"] = json.loads(payload.pop("question_index_json"))
        payload["questions"] = [
            {**dict(item), "tags": json.loads(item["tags_json"])} for item in questions
        ]
        return payload


def list_score_workbooks(db_path: Path, *, paper_id: str | None = None, exam_session: str | None = None) -> list[dict]:
    clauses = ["1 = 1"]
    params: list[object] = []
    if paper_id is not None:
        clauses.append("paper_id = ?")
        params.append(paper_id)
    if exam_session is not None:
        clauses.append("exam_session = ?")
        params.append(exam_session)
    sql = f"""
        SELECT workbook_id, paper_id, exam_session, workbook_kind, source_filename,
               file_path, mime_type, sheet_names_json, file_size, sha256, notes,
               created_at, updated_at
        FROM score_workbooks
        WHERE {' AND '.join(clauses)}
        ORDER BY paper_id, exam_session, workbook_id
    """
    with connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
        payload = []
        for row in rows:
            item = dict(row)
            item["sheet_names"] = json.loads(item.pop("sheet_names_json"))
            payload.append(item)
        return payload


def get_score_workbook_metadata(db_path: Path, workbook_id: str) -> dict | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT workbook_id, paper_id, exam_session, workbook_kind, source_filename,
                   file_path, mime_type, sheet_names_json, file_size, sha256, notes,
                   created_at, updated_at
            FROM score_workbooks
            WHERE workbook_id = ?
            """,
            (workbook_id,),
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        payload["sheet_names"] = json.loads(payload.pop("sheet_names_json"))
        return payload


def list_questions(
    db_path: Path,
    *,
    edition: int | None = None,
    paper_id: str | None = None,
    paper_type: str | None = None,
    category: str | None = None,
    has_assets: bool | None = None,
    has_answer: bool | None = None,
    min_avg_score: float | None = None,
    max_avg_score: float | None = None,
    tag: str | None = None,
    query: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    clauses = ["1 = 1"]
    params: list[object] = []
    if edition is not None:
        clauses.append("p.edition = ?")
        params.append(edition)
    if paper_id is not None:
        clauses.append("q.paper_id = ?")
        params.append(paper_id)
    if paper_type is not None:
        clauses.append("p.paper_type = ?")
        params.append(paper_type)
    if category is not None:
        clauses.append("q.category = ?")
        params.append(category)
    if has_assets is not None:
        clauses.append(
            "EXISTS (SELECT 1 FROM question_assets qa WHERE qa.question_id = q.question_id)"
            if has_assets
            else "NOT EXISTS (SELECT 1 FROM question_assets qa WHERE qa.question_id = q.question_id)"
        )
    if has_answer is not None:
        clauses.append(
            "COALESCE(q.answer_latex_source, '') <> ''"
            if has_answer
            else "COALESCE(q.answer_latex_source, '') = ''"
        )
    if min_avg_score is not None:
        clauses.append("EXISTS (SELECT 1 FROM question_stats qs WHERE qs.question_id = q.question_id AND qs.avg_score >= ?)")
        params.append(min_avg_score)
    if max_avg_score is not None:
        clauses.append("EXISTS (SELECT 1 FROM question_stats qs WHERE qs.question_id = q.question_id AND qs.avg_score <= ?)")
        params.append(max_avg_score)
    if tag is not None:
        clauses.append("q.tags_json LIKE ?")
        params.append(f'%"{tag}"%')
    if query is not None:
        clauses.append(
            "(" \
            "COALESCE(q.search_text, '') LIKE ? OR "
            "q.latex_path LIKE ? OR "
            "COALESCE(q.latex_anchor, '') LIKE ? OR "
            "COALESCE(q.latex_source, '') LIKE ? OR "
            "COALESCE(q.answer_latex_source, '') LIKE ?" \
            ")"
        )
        params.extend([f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%"])

    sql = f"""
        SELECT q.question_id, q.paper_id, q.paper_index, q.question_no, q.category, q.status,
               q.search_text, q.latex_path, q.answer_latex_path, q.tags_json,
               p.edition, p.paper_type, p.title
        FROM questions q
        JOIN papers p ON p.paper_id = q.paper_id
        WHERE {' AND '.join(clauses)}
        ORDER BY p.edition DESC, q.paper_id, q.paper_index
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    with connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
        payload = []
        for row in rows:
            item = dict(row)
            item["tags"] = json.loads(item.pop("tags_json"))
            payload.append(item)
        return payload


def get_question_detail(db_path: Path, question_id: str) -> dict | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT q.*, p.edition, p.paper_type, p.title AS paper_title, p.paper_latex_path,
                   p.paper_latex_source, p.source_pdf_path, p.question_index_json
            FROM questions q
            JOIN papers p ON p.paper_id = q.paper_id
            WHERE q.question_id = ?
            """,
            (question_id,),
        ).fetchone()
        if row is None:
            return None
        assets = conn.execute(
            """
            SELECT asset_id, kind, file_path, sha256, caption, sort_order
            FROM question_assets
            WHERE question_id = ?
            ORDER BY sort_order, asset_id
            """,
            (question_id,),
        ).fetchall()
        stats = conn.execute(
            """
            SELECT exam_session, source_workbook_id, participant_count, avg_score, score_std, full_mark_rate,
                   zero_score_rate, max_score, min_score, stats_source, stats_version
            FROM question_stats
            WHERE question_id = ?
            ORDER BY exam_session
            """,
            (question_id,),
        ).fetchall()
        difficulty = conn.execute(
            """
            SELECT exam_session, manual_level, derived_score, method, method_version,
                   confidence, feature_json
            FROM difficulty_scores
            WHERE question_id = ?
            ORDER BY exam_session
            """,
            (question_id,),
        ).fetchall()
        workbooks = conn.execute(
            """
            SELECT workbook_id, exam_session, workbook_kind, source_filename, file_path,
                   sheet_names_json, file_size, sha256
            FROM score_workbooks
            WHERE paper_id = ?
            ORDER BY exam_session, workbook_id
            """,
            (row["paper_id"],),
        ).fetchall()
        payload = dict(row)
        payload["tags"] = json.loads(payload.pop("tags_json"))
        payload["paper_question_index"] = json.loads(payload.pop("question_index_json"))
        payload["assets"] = [dict(item) for item in assets]
        payload["stats"] = [dict(item) for item in stats]
        payload["difficulty_scores"] = [
            {**dict(item), "feature_json": json.loads(item["feature_json"])} for item in difficulty
        ]
        payload["score_workbooks"] = [
            {**dict(item), "sheet_names": json.loads(item["sheet_names_json"])} for item in workbooks
        ]
        return payload
