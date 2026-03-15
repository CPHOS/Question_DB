from __future__ import annotations

import json
from pathlib import Path

from .config import RAW_DIR
from .db import connect


def _resolve_path(project_root: Path, stored_path: str | None) -> Path | None:
    if not stored_path:
        return None
    path = Path(stored_path)
    if path.is_absolute():
        return path
    for root in (project_root, RAW_DIR):
        candidate = root / path
        if candidate.exists():
            return candidate
    return project_root / path


def run_quality_checks(db_path: Path, project_root: Path) -> dict:
    report = {
        "missing_question_latex": [],
        "missing_question_latex_source": [],
        "missing_answer_latex": [],
        "missing_answer_latex_source": [],
        "missing_paper_latex": [],
        "missing_paper_latex_source": [],
        "missing_assets": [],
        "missing_workbook_blob": [],
        "duplicate_question_numbers": [],
    }
    with connect(db_path) as conn:
        for row in conn.execute(
            "SELECT question_id, latex_path, latex_source, answer_latex_path, answer_latex_source FROM questions"
        ).fetchall():
            latex_path = _resolve_path(project_root, row["latex_path"])
            if latex_path is None or not latex_path.exists():
                report["missing_question_latex"].append(row["question_id"])
            if not row["latex_source"]:
                report["missing_question_latex_source"].append(row["question_id"])
            answer_latex_path = _resolve_path(project_root, row["answer_latex_path"])
            if row["answer_latex_path"] and (answer_latex_path is None or not answer_latex_path.exists()):
                report["missing_answer_latex"].append(row["question_id"])
            if row["answer_latex_path"] and not row["answer_latex_source"]:
                report["missing_answer_latex_source"].append(row["question_id"])
        for row in conn.execute("SELECT paper_id, paper_latex_path, paper_latex_source FROM papers").fetchall():
            paper_latex_path = _resolve_path(project_root, row["paper_latex_path"])
            if paper_latex_path is None or not paper_latex_path.exists():
                report["missing_paper_latex"].append(row["paper_id"])
            if not row["paper_latex_source"]:
                report["missing_paper_latex_source"].append(row["paper_id"])
        duplicates = conn.execute(
            """
            SELECT paper_id, question_no, COUNT(*) AS duplicate_count
            FROM questions
            GROUP BY paper_id, question_no
            HAVING COUNT(*) > 1
            """
        ).fetchall()
        report["duplicate_question_numbers"] = [dict(item) for item in duplicates]
        for row in conn.execute("SELECT question_id, file_path FROM question_assets").fetchall():
            asset_path = _resolve_path(project_root, row["file_path"])
            if asset_path is None or not asset_path.exists():
                report["missing_assets"].append(dict(row))
        for row in conn.execute("SELECT workbook_id, LENGTH(workbook_blob) AS blob_length FROM score_workbooks").fetchall():
            if not row["blob_length"]:
                report["missing_workbook_blob"].append(row["workbook_id"])
    return report


def write_quality_report(db_path: Path, project_root: Path, output_path: Path) -> dict:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report = run_quality_checks(db_path, project_root)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
    return report
