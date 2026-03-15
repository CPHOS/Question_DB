from __future__ import annotations

import csv
import json
from pathlib import Path

from .db import connect


def _question_payload(conn, include_answers: bool) -> list[dict]:
    rows = conn.execute(
        """
        SELECT q.*, p.title AS paper_title, p.edition, p.paper_type, p.paper_latex_path,
               p.paper_latex_source, p.source_pdf_path, p.question_index_json
        FROM questions q
        JOIN papers p ON p.paper_id = q.paper_id
        ORDER BY p.edition, p.paper_id, q.paper_index
        """
    ).fetchall()
    payload: list[dict] = []
    for row in rows:
        assets = conn.execute(
            """
            SELECT asset_id, kind, file_path, caption, sort_order
            FROM question_assets
            WHERE question_id = ?
            ORDER BY sort_order, asset_id
            """,
            (row["question_id"],),
        ).fetchall()
        stats = conn.execute(
            """
            SELECT exam_session, source_workbook_id, participant_count, avg_score, score_std, full_mark_rate,
                   zero_score_rate, max_score, min_score, stats_source, stats_version
            FROM question_stats
            WHERE question_id = ?
            ORDER BY exam_session
            """,
            (row["question_id"],),
        ).fetchall()
        workbooks = conn.execute(
            """
            SELECT workbook_id, exam_session, workbook_kind, source_filename, file_path, sheet_names_json
            FROM score_workbooks
            WHERE paper_id = ?
            ORDER BY exam_session, workbook_id
            """,
            (row["paper_id"],),
        ).fetchall()
        item = {
            "question_id": row["question_id"],
            "paper_id": row["paper_id"],
            "paper_title": row["paper_title"],
            "edition": row["edition"],
            "paper_type": row["paper_type"],
            "paper_latex_path": row["paper_latex_path"],
            "paper_latex_source": row["paper_latex_source"],
            "source_pdf_path": row["source_pdf_path"],
            "paper_question_index": json.loads(row["question_index_json"]),
            "paper_index": row["paper_index"],
            "question_no": row["question_no"],
            "category": row["category"],
            "latex_path": row["latex_path"],
            "latex_source": row["latex_source"],
            "latex_anchor": row["latex_anchor"],
            "search_text": row["search_text"],
            "status": row["status"],
            "tags": json.loads(row["tags_json"]),
            "assets": [dict(asset) for asset in assets],
            "stats": [dict(stat) for stat in stats],
            "score_workbooks": [
                {**dict(workbook), "sheet_names": json.loads(workbook["sheet_names_json"])} for workbook in workbooks
            ],
        }
        if include_answers:
            item["answer_latex_path"] = row["answer_latex_path"]
            item["answer_latex_source"] = row["answer_latex_source"]
        payload.append(item)
    return payload


def export_jsonl(db_path: Path, output_path: Path, include_answers: bool) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn, output_path.open("w", encoding="utf-8") as handle:
        payload = _question_payload(conn, include_answers=include_answers)
        for item in payload:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")
    return len(payload)


def export_csv(db_path: Path, output_path: Path, include_answers: bool) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn, output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "question_id",
                "paper_id",
                "paper_index",
                "question_no",
                "category",
                "status",
                "edition",
                "paper_type",
                "latex_path",
                "answer_latex_path",
                "search_text",
                "tags",
            ],
        )
        writer.writeheader()
        rows = conn.execute(
            """
            SELECT q.question_id, q.paper_id, q.paper_index, q.question_no, q.category, q.status,
                   p.edition, p.paper_type, q.latex_path, q.answer_latex_path, q.search_text, q.tags_json
            FROM questions q
            JOIN papers p ON p.paper_id = q.paper_id
            ORDER BY p.edition, q.paper_index
            """
        ).fetchall()
        for row in rows:
            writer.writerow(
                {
                    "question_id": row["question_id"],
                    "paper_id": row["paper_id"],
                    "paper_index": row["paper_index"],
                    "question_no": row["question_no"],
                    "category": row["category"],
                    "status": row["status"],
                    "edition": row["edition"],
                    "paper_type": row["paper_type"],
                    "latex_path": row["latex_path"],
                    "answer_latex_path": row["answer_latex_path"] if include_answers else "",
                    "search_text": row["search_text"],
                    "tags": row["tags_json"],
                }
            )
    return len(rows)
