use std::{fs, path::Path};

use anyhow::Context;
use axum::{
    extract::{Multipart, State},
    response::Response,
    Json,
};
use serde_json::json;

use super::{
    database::{
        backup_download_name, finish_backup_download_response, generate_database_backup,
        normalize_uploaded_backup_name, restore_database_backup as restore_database_from_backup,
        temp_backup_path, temp_restore_upload_path,
    },
    exports::{default_export_path, ensure_parent_dir, export_csv, export_jsonl, exported_path},
    models::{
        DatabaseRestoreResponse, ExportFormat, ExportRequest, ExportResponse, QualityCheckRequest,
    },
    quality::build_quality_report,
};
use crate::api::{
    shared::{
        error::{ApiError, ApiResult},
        multipart::read_uploaded_file,
        utils::{canonical_or_original, resolve_export_path},
    },
    AppState,
};

fn ops_internal(err: anyhow::Error) -> ApiError {
    tracing::error!("ops command failed: {err:#}");
    ApiError::internal(err.to_string())
}

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
        ExportFormat::Jsonl => {
            export_jsonl(
                &state.pool,
                &state.object_store,
                &output_path,
                request.public,
            )
            .await?
        }
        ExportFormat::Csv => {
            export_csv(
                &state.pool,
                &state.object_store,
                &output_path,
                request.public,
            )
            .await?
        }
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

pub(crate) async fn download_database_backup(
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let backup_path = temp_backup_path();
    generate_database_backup(
        state.database_url.clone(),
        state.object_store.store_dir().to_path_buf(),
        backup_path.clone(),
    )
    .await
    .map_err(ops_internal)?;

    finish_backup_download_response(backup_path, &backup_download_name())
        .await
        .map_err(ops_internal)
}

pub(crate) async fn restore_database_backup(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<DatabaseRestoreResponse> {
    let (file_name, bytes) = read_uploaded_file(&mut multipart).await?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request(
            "multipart form must include a non-empty 'file' field",
        ));
    }

    let normalized_name = normalize_uploaded_backup_name(file_name.as_deref());
    let upload_path = temp_restore_upload_path(file_name.as_deref());
    tokio::fs::write(&upload_path, &bytes)
        .await
        .with_context(|| {
            format!(
                "write uploaded backup temp file failed: {}",
                upload_path.to_string_lossy()
            )
        })
        .map_err(ops_internal)?;

    let restore_result = restore_database_from_backup(
        state.database_url.clone(),
        state.object_store.store_dir().to_path_buf(),
        upload_path.clone(),
    )
    .await;
    std::fs::remove_file(&upload_path).ok();
    restore_result.map_err(ops_internal)?;

    Ok(Json(DatabaseRestoreResponse {
        file_name: normalized_name,
        restored_bytes: bytes.len(),
        status: "restored",
    }))
}
