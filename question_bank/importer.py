from __future__ import annotations

import difflib
from pathlib import Path

from .bundle import load_bundle, validate_bundle
from .config import PROJECT_ROOT, RAW_DIR
from .db import connect
from .utils import dumps_json, load_text, normalize_search_text, sha256_file, utc_now_iso
from .workbooks import upsert_score_workbook


def _find_similar_questions(conn, comparison_text: str, threshold: float = 0.92) -> list[str]:
    matches: list[str] = []
    if not comparison_text:
        return matches
    rows = conn.execute(
        "SELECT question_id, COALESCE(search_text, latex_source, '') AS comparison_text FROM questions"
    ).fetchall()
    for row in rows:
        if not row["comparison_text"]:
            continue
        ratio = difflib.SequenceMatcher(a=comparison_text, b=row["comparison_text"]).ratio()
        if ratio >= threshold:
            matches.append(f"{row['question_id']} ({ratio:.3f})")
    return matches


def _stored_path(target_path: Path) -> str:
    resolved = target_path.resolve()
    for root in (RAW_DIR, PROJECT_ROOT):
        try:
            return resolved.relative_to(root.resolve()).as_posix()
        except ValueError:
            continue
    return resolved.as_posix()


def import_bundle(bundle_path: Path, db_path: Path, dry_run: bool = True, allow_similar: bool = False) -> dict:
    validation = validate_bundle(bundle_path)
    manifest, questions = load_bundle(bundle_path)
    warnings = list(validation.warnings)
    errors = list(validation.errors)
    imported_questions = 0
    imported_assets = 0
    imported_workbooks = 0
    started_at = utc_now_iso()
    finished_at = started_at

    paper = manifest["paper"]
    paper_latex_file = (bundle_path / paper["paper_latex_path"]).resolve()
    paper_latex_source = load_text(paper_latex_file) if paper_latex_file.exists() else ""

    hydrated_questions: list[dict] = []
    for question in questions:
        latex_file = (bundle_path / question["latex_path"]).resolve()
        latex_source = load_text(latex_file) if latex_file.exists() else ""
        answer_latex_source = None
        answer_latex_path = question.get("answer_latex_path")
        if answer_latex_path:
            answer_file = (bundle_path / answer_latex_path).resolve()
            if answer_file.exists():
                answer_latex_source = load_text(answer_file)
        comparison_text = normalize_search_text(question.get("search_text"), latex_source, answer_latex_source)
        hydrated_question = dict(question)
        hydrated_question["latex_source"] = latex_source
        hydrated_question["answer_latex_source"] = answer_latex_source
        hydrated_question["comparison_text"] = comparison_text
        hydrated_questions.append(hydrated_question)

    with connect(db_path) as conn:
        for question in hydrated_questions:
            existing = conn.execute(
                "SELECT question_id FROM questions WHERE paper_id = ? AND question_no = ?",
                (paper["paper_id"], question["question_no"]),
            ).fetchone()
            if existing and existing["question_id"] != question["question_id"]:
                errors.append(
                    f"题号冲突: 同一试卷 {paper['paper_id']} 的题号 {question['question_no']} 已被 {existing['question_id']} 使用。"
                )
            similar = _find_similar_questions(conn, question.get("comparison_text", ""))
            if similar and question["question_id"] not in {item.split()[0] for item in similar}:
                message = f"{question['question_id']} 与已有题目文本高度相似: {', '.join(similar)}"
                if allow_similar:
                    warnings.append(message)
                else:
                    errors.append(message)

        status = "failed" if errors else ("dry_run" if dry_run else "committed")
        details = {
            "bundle_name": manifest.get("bundle_name"),
            "paper_id": paper.get("paper_id"),
            "warnings": warnings,
            "errors": errors,
        }
        if not errors and not dry_run:
            now = utc_now_iso()
            question_index = [
                {
                    "paper_index": question["paper_index"],
                    "question_id": question["question_id"],
                    "question_no": question["question_no"],
                    "latex_path": question["latex_path"],
                }
                for question in sorted(hydrated_questions, key=lambda item: item["paper_index"])
            ]
            paper_latex_path = _stored_path(paper_latex_file)
            source_pdf_path = None
            if paper.get("source_pdf_path"):
                source_pdf_path = _stored_path((bundle_path / paper["source_pdf_path"]).resolve())
            conn.execute(
                """
                INSERT OR REPLACE INTO papers (
                    paper_id, edition, paper_type, title, paper_latex_path, paper_latex_source,
                    source_pdf_path, question_index_json, notes, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE((SELECT created_at FROM papers WHERE paper_id = ?), ?), ?)
                """,
                (
                    paper["paper_id"],
                    paper["edition"],
                    paper["paper_type"],
                    paper["title"],
                    paper_latex_path,
                    paper_latex_source,
                    source_pdf_path,
                    dumps_json(question_index),
                    paper.get("notes"),
                    paper["paper_id"],
                    now,
                    now,
                ),
            )
            for question in hydrated_questions:
                latex_path = _stored_path((bundle_path / question["latex_path"]).resolve())
                answer_latex_path = None
                if question.get("answer_latex_path"):
                    answer_latex_path = _stored_path((bundle_path / question["answer_latex_path"]).resolve())
                conn.execute(
                    """
                    INSERT OR REPLACE INTO questions (
                        question_id, paper_id, paper_index, question_no, category,
                        latex_path, latex_source, answer_latex_path, answer_latex_source,
                        latex_anchor, search_text, status, tags_json, notes, created_at, updated_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        COALESCE((SELECT created_at FROM questions WHERE question_id = ?), ?), ?
                    )
                    """,
                    (
                        question["question_id"],
                        paper["paper_id"],
                        question["paper_index"],
                        question["question_no"],
                        question["category"],
                        latex_path,
                        question["latex_source"],
                        answer_latex_path,
                        question.get("answer_latex_source"),
                        question.get("latex_anchor"),
                        question.get("search_text") or question.get("comparison_text"),
                        question["status"],
                        dumps_json(question.get("tags", [])),
                        question.get("notes"),
                        question["question_id"],
                        now,
                        now,
                    ),
                )
                imported_questions += 1
                conn.execute("DELETE FROM question_assets WHERE question_id = ?", (question["question_id"],))
                for asset in question.get("assets", []):
                    asset_path = (bundle_path / asset["file_path"]).resolve()
                    conn.execute(
                        """
                        INSERT INTO question_assets (
                            asset_id, question_id, kind, file_path, sha256, caption, sort_order, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            asset["asset_id"],
                            question["question_id"],
                            asset["kind"],
                            _stored_path(asset_path),
                            asset.get("sha256") or sha256_file(asset_path),
                            asset.get("caption"),
                            asset.get("sort_order", 0),
                            now,
                        ),
                    )
                    imported_assets += 1
            conn.commit()
            for workbook in manifest.get("score_workbooks", []):
                upsert_score_workbook(
                    db_path,
                    paper_id=paper["paper_id"],
                    workbook=workbook,
                    bundle_path=bundle_path,
                    storage_root=RAW_DIR if str(bundle_path.resolve()).startswith(str(RAW_DIR.resolve())) else PROJECT_ROOT,
                )
                imported_workbooks += 1
            finished_at = utc_now_iso()
        else:
            finished_at = utc_now_iso()

        conn.execute(
            """
            INSERT INTO import_runs (
                run_label, bundle_path, dry_run, status, item_count, warning_count, error_count,
                details_json, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest.get("run_label", bundle_path.name),
                str(bundle_path.resolve()),
                1 if dry_run else 0,
                status,
                len(hydrated_questions) + len(manifest.get("score_workbooks", [])),
                len(warnings),
                len(errors),
                dumps_json(details),
                started_at,
                finished_at,
            ),
        )
        conn.commit()

    return {
        "bundle_name": manifest.get("bundle_name"),
        "paper_id": manifest["paper"]["paper_id"],
        "status": status,
        "question_count": len(hydrated_questions),
        "imported_questions": imported_questions,
        "imported_assets": imported_assets,
        "imported_workbooks": imported_workbooks,
        "warnings": warnings,
        "errors": errors,
    }
