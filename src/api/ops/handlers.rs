use std::{fs, path::Path};

use anyhow::Context;
use axum::{extract::State, Json};
use serde_json::json;

use super::{
    exports::{default_export_path, ensure_parent_dir, export_csv, export_jsonl, exported_path},
    models::{ExportFormat, ExportRequest, ExportResponse, QualityCheckRequest},
    quality::build_quality_report,
};
use crate::api::{
    shared::{
        error::{ApiError, ApiResult},
        utils::{canonical_or_original, resolve_export_path},
    },
    AppState,
};

pub(crate) async fn run_export(
    State(state): State<AppState>,
    Json(request): Json<ExportRequest>,
) -> ApiResult<ExportResponse> {
    let output_path = resolve_export_path(
        request.output_path.as_deref(),
        default_export_path(request.format, request.public),
        &state.export_dir,
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))?;
    ensure_parent_dir(&output_path, "export")?;

    let exported_count = match request.format {
        ExportFormat::Jsonl => export_jsonl(&state.pool, &output_path, request.public).await?,
        ExportFormat::Csv => export_csv(&state.pool, &output_path, request.public).await?,
    };

    Ok(Json(ExportResponse {
        format: match request.format {
            ExportFormat::Jsonl => "jsonl",
            ExportFormat::Csv => "csv",
        },
        public: request.public,
        output_path: exported_path(&output_path),
        exported_questions: exported_count,
    }))
}

pub(crate) async fn run_quality_check(
    State(state): State<AppState>,
    Json(request): Json<QualityCheckRequest>,
) -> ApiResult<serde_json::Value> {
    let output_path = resolve_export_path(
        request.output_path.as_deref(),
        std::path::PathBuf::from("quality_report.json"),
        &state.export_dir,
    )
    .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let report = build_quality_report(&state.pool).await?;
    ensure_parent_dir(&output_path, "quality report")?;
    let serialized =
        serde_json::to_string_pretty(&report).context("serialize quality report failed")?;
    fs::write(&output_path, serialized).with_context(|| {
        format!(
            "write quality report failed: {}",
            output_path.to_string_lossy()
        )
    })?;

    Ok(Json(json!({
        "output_path": canonical_or_original(Path::new(&output_path)),
        "report": report,
    })))
}
