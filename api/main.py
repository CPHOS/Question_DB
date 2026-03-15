from __future__ import annotations

from pathlib import Path
from typing import Literal

try:
    from fastapi import FastAPI, HTTPException, Query, Response
    from pydantic import BaseModel
except ModuleNotFoundError as exc:
    raise RuntimeError(
        "FastAPI 未安装。请先执行 `pip install -r requirements.txt` 后再启动 API。"
    ) from exc

from question_bank.bundle import validate_bundle
from question_bank.config import DEFAULT_DB_PATH, EXPORTS_DIR, PROJECT_ROOT, RAW_DIR
from question_bank.db import initialize_database
from question_bank.exporter import export_csv, export_jsonl
from question_bank.importer import import_bundle
from question_bank.quality import write_quality_report
from question_bank.repository import (
    get_paper_detail,
    get_question_detail,
    get_score_workbook_metadata,
    list_papers,
    list_questions,
    list_score_workbooks,
)
from question_bank.stats import aggregate_score_rows, upsert_stats
from question_bank.workbooks import get_score_workbook_blob, upsert_score_workbook

initialize_database(DEFAULT_DB_PATH)


class BundleImportRequest(BaseModel):
    bundle_path: str
    allow_similar: bool = False


class WorkbookImportRequest(BaseModel):
    workbook_path: str
    paper_id: str
    exam_session: str
    workbook_kind: str
    workbook_id: str
    notes: str = ""


class StatsImportRequest(BaseModel):
    csv_path: str
    stats_source: str
    stats_version: str
    source_workbook_id: str | None = None


class ExportRequest(BaseModel):
    format: Literal["jsonl", "csv"] = "jsonl"
    public: bool = False
    output_path: str | None = None


class QualityCheckRequest(BaseModel):
    output_path: str | None = None


app = FastAPI(title="CPHOS Question Bank API", version="1.2.0")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/papers")
def papers() -> list[dict]:
    return list_papers(DEFAULT_DB_PATH)


@app.get("/papers/{paper_id}")
def paper_detail(paper_id: str) -> dict:
    result = get_paper_detail(DEFAULT_DB_PATH, paper_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Paper not found")
    return result


@app.get("/questions")
def questions(
    edition: int | None = None,
    paper_id: str | None = None,
    paper_type: str | None = Query(default=None, pattern="^(regular|semifinal|final|other)$"),
    category: str | None = Query(default=None, pattern="^(theory|experiment)$"),
    has_assets: bool | None = None,
    has_answer: bool | None = None,
    min_avg_score: float | None = None,
    max_avg_score: float | None = None,
    tag: str | None = None,
    q: str | None = None,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    return list_questions(
        DEFAULT_DB_PATH,
        edition=edition,
        paper_id=paper_id,
        paper_type=paper_type,
        category=category,
        has_assets=has_assets,
        has_answer=has_answer,
        min_avg_score=min_avg_score,
        max_avg_score=max_avg_score,
        tag=tag,
        query=q,
        limit=limit,
        offset=offset,
    )


@app.get("/questions/{question_id}")
def question_detail(question_id: str) -> dict:
    result = get_question_detail(DEFAULT_DB_PATH, question_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Question not found")
    return result


@app.get("/score-workbooks")
def score_workbooks(
    paper_id: str | None = None,
    exam_session: str | None = None,
) -> list[dict]:
    return list_score_workbooks(DEFAULT_DB_PATH, paper_id=paper_id, exam_session=exam_session)


@app.get("/score-workbooks/{workbook_id}")
def score_workbook_detail(workbook_id: str) -> dict:
    result = get_score_workbook_metadata(DEFAULT_DB_PATH, workbook_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Workbook not found")
    return result


@app.get("/score-workbooks/{workbook_id}/download")
def score_workbook_download(workbook_id: str) -> Response:
    result = get_score_workbook_blob(DEFAULT_DB_PATH, workbook_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Workbook not found")
    metadata, payload = result
    headers = {"Content-Disposition": f'attachment; filename="{metadata["source_filename"]}"'}
    return Response(content=payload, media_type=metadata["mime_type"], headers=headers)


@app.post("/imports/bundle/validate")
def validate_bundle_import(request: BundleImportRequest) -> dict:
    bundle_path = Path(request.bundle_path).expanduser()
    result = validate_bundle(bundle_path)
    return {
        "bundle_path": str(bundle_path.resolve()),
        "ok": result.ok,
        "warnings": result.warnings,
        "errors": result.errors,
    }


@app.post("/imports/bundle/commit")
def commit_bundle_import(request: BundleImportRequest) -> dict:
    bundle_path = Path(request.bundle_path).expanduser()
    return import_bundle(bundle_path, db_path=DEFAULT_DB_PATH, dry_run=False, allow_similar=request.allow_similar)


@app.post("/imports/workbooks/commit")
def commit_workbook_import(request: WorkbookImportRequest) -> dict:
    workbook_path = Path(request.workbook_path).expanduser().resolve()
    workbook = {
        "workbook_id": request.workbook_id,
        "exam_session": request.exam_session,
        "workbook_kind": request.workbook_kind,
        "file_path": workbook_path.name,
        "notes": request.notes,
    }
    storage_root = RAW_DIR if str(workbook_path).startswith(str(RAW_DIR.resolve())) else None
    workbook_id = upsert_score_workbook(
        DEFAULT_DB_PATH,
        paper_id=request.paper_id,
        workbook=workbook,
        bundle_path=workbook_path.parent,
        storage_root=storage_root,
    )
    metadata = get_score_workbook_metadata(DEFAULT_DB_PATH, workbook_id)
    return metadata or {"workbook_id": workbook_id}


@app.post("/imports/stats/commit")
def commit_stats_import(request: StatsImportRequest) -> dict:
    csv_path = Path(request.csv_path).expanduser()
    rows = aggregate_score_rows(csv_path)
    count = upsert_stats(
        DEFAULT_DB_PATH,
        rows,
        stats_source=request.stats_source,
        stats_version=request.stats_version,
        source_workbook_id=request.source_workbook_id,
    )
    return {
        "csv_path": str(csv_path.resolve()),
        "imported_stats": count,
        "stats_source": request.stats_source,
        "stats_version": request.stats_version,
        "source_workbook_id": request.source_workbook_id,
    }


@app.post("/exports/run")
def run_export(request: ExportRequest) -> dict:
    suffix = "public" if request.public else "internal"
    output_path = Path(request.output_path).expanduser() if request.output_path else EXPORTS_DIR / f"question_bank_{suffix}.{request.format}"
    include_answers = not request.public
    if request.format == "jsonl":
        count = export_jsonl(DEFAULT_DB_PATH, output_path, include_answers=include_answers)
    else:
        count = export_csv(DEFAULT_DB_PATH, output_path, include_answers=include_answers)
    return {
        "format": request.format,
        "public": request.public,
        "output_path": str(output_path.resolve()),
        "exported_questions": count,
    }


@app.post("/quality-checks/run")
def run_quality_check(request: QualityCheckRequest) -> dict:
    output_path = Path(request.output_path).expanduser() if request.output_path else EXPORTS_DIR / "quality_report.json"
    report = write_quality_report(DEFAULT_DB_PATH, PROJECT_ROOT, output_path)
    return {"output_path": str(output_path.resolve()), "report": report}


@app.get("/search")
def search(
    q: str = Query(..., min_length=1),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    return list_questions(DEFAULT_DB_PATH, query=q, limit=limit, offset=offset)
