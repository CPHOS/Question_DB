from __future__ import annotations

import json
from pathlib import Path

from .db import connect
from .utils import XLSX_MIME_TYPE, dumps_json, sha256_bytes, utc_now_iso, xlsx_sheet_names


def _stored_path(target_path: Path, storage_root: Path | None = None) -> str:
    resolved = target_path.resolve()
    if storage_root is not None:
        try:
            return resolved.relative_to(storage_root.resolve()).as_posix()
        except ValueError:
            pass
    return resolved.as_posix()


def upsert_score_workbook(
    db_path: Path,
    *,
    paper_id: str,
    workbook: dict,
    bundle_path: Path,
    storage_root: Path | None = None,
) -> str:
    now = utc_now_iso()
    workbook_path = (bundle_path / workbook["file_path"]).resolve()
    workbook_bytes = workbook_path.read_bytes()
    stored_path = _stored_path(workbook_path, storage_root=storage_root)
    sheet_names = xlsx_sheet_names(workbook_path)
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO score_workbooks (
                workbook_id, paper_id, exam_session, workbook_kind, source_filename,
                file_path, mime_type, sheet_names_json, file_size, sha256,
                workbook_blob, notes, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                COALESCE((SELECT created_at FROM score_workbooks WHERE workbook_id = ?), ?), ?
            )
            """,
            (
                workbook["workbook_id"],
                paper_id,
                workbook["exam_session"],
                workbook["workbook_kind"],
                workbook_path.name,
                stored_path,
                XLSX_MIME_TYPE,
                dumps_json(sheet_names),
                len(workbook_bytes),
                sha256_bytes(workbook_bytes),
                workbook_bytes,
                workbook.get("notes"),
                workbook["workbook_id"],
                now,
                now,
            ),
        )
        conn.commit()
    return workbook["workbook_id"]


def list_score_workbooks(db_path: Path, *, paper_id: str | None = None, exam_session: str | None = None) -> list[dict]:
    clauses = ["1 = 1"]
    params: list[object] = []
    if paper_id is not None:
        clauses.append("paper_id = ?")
        params.append(paper_id)
    if exam_session is not None:
        clauses.append("exam_session = ?")
        params.append(exam_session)
    query = f"""
        SELECT workbook_id, paper_id, exam_session, workbook_kind, source_filename,
               file_path, mime_type, sheet_names_json, file_size, sha256, notes,
               created_at, updated_at
        FROM score_workbooks
        WHERE {' AND '.join(clauses)}
        ORDER BY paper_id, exam_session, workbook_id
    """
    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
        return [{**dict(row), "sheet_names": json.loads(row["sheet_names_json"])} for row in rows]


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


def get_score_workbook_blob(db_path: Path, workbook_id: str) -> tuple[dict, bytes] | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT workbook_id, paper_id, exam_session, workbook_kind, source_filename,
                   mime_type, workbook_blob
            FROM score_workbooks
            WHERE workbook_id = ?
            """,
            (workbook_id,),
        ).fetchone()
        if row is None:
            return None
        metadata = {
            "workbook_id": row["workbook_id"],
            "paper_id": row["paper_id"],
            "exam_session": row["exam_session"],
            "workbook_kind": row["workbook_kind"],
            "source_filename": row["source_filename"],
            "mime_type": row["mime_type"],
        }
        return metadata, row["workbook_blob"]
