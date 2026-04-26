use std::collections::HashSet;

use anyhow::Context;
use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    response::Response,
    Extension, Json,
};
use sqlx::{query, Row};
use uuid::Uuid;

use super::{
    bundles::build_paper_bundle_response,
    imports::{import_paper_zip, replace_paper_zip, MAX_UPLOAD_BYTES},
    models::{
        CreatePaperRequest, PaperBundleRequest, PaperDeleteResponse, PaperDetail,
        PaperFileReplaceResponse, PaperImportResponse, PaperSummary, PapersParams,
        UpdatePaperRequest,
    },
    queries::{
        count_papers, ensure_paper_questions_valid, execute_papers_query, map_paper_summary,
        validate_and_build_papers_query,
    },
};
use crate::api::{
    auth::models::CurrentUser,
    shared::{
        details::{load_paper_detail, DetailVisibility},
        error::{ApiError, ApiResult},
        multipart::{
            next_multipart_field, parse_uuid_param, read_file_field, read_json_field,
            read_text_field, read_uploaded_file, validate_upload_size,
        },
        pagination::Paginated,
    },
    AppState,
};

pub(crate) async fn list_papers(
    Query(params): Query<PapersParams>,
    State(state): State<AppState>,
) -> ApiResult<Paginated<PaperSummary>> {
    let mut plan = validate_and_build_papers_query(&params)
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let limit = plan.limit;
    let offset = plan.offset;
    let total = count_papers(&state.pool, &params)
        .await
        .context("count papers failed")
        .map_err(ApiError::from)?;
    let rows = execute_papers_query(&state.pool, &mut plan)
        .await
        .context("query papers failed")
        .map_err(ApiError::from)?;

    let items = rows.into_iter().map(map_paper_summary).collect();

    Ok(Json(Paginated {
        items,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn create_paper(
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<PaperImportResponse> {
    if !current.role.can_create_paper() {
        return Err(ApiError::forbidden(
            "leader role or above required to create papers",
        ));
    }

    let mut file_name = None;
    let mut description = None;
    let mut title = None;
    let mut subtitle = None;
    let mut question_ids = None;
    let mut bytes = Vec::new();

    while let Some(field) = next_multipart_field(&mut multipart).await? {
        let Some(name) = field.name() else {
            continue;
        };
        match name {
            "file" => {
                let (fname, data) = read_file_field(field).await?;
                file_name = fname;
                bytes = data;
            }
            "description" => {
                description = Some(read_text_field(field, "description").await?);
            }
            "title" => {
                title = Some(read_text_field(field, "title").await?);
            }
            "subtitle" => {
                subtitle = Some(read_text_field(field, "subtitle").await?);
            }
            "question_ids" => {
                question_ids = Some(read_json_field(field, "question_ids").await?);
            }
            _ => {}
        }
    }

    if !bytes.is_empty() {
        validate_upload_size(&bytes, MAX_UPLOAD_BYTES)?;
    }

    let request = CreatePaperRequest {
        description: description.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'description' field")
        })?,
        title: title.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'title' field")
        })?,
        subtitle: subtitle.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'subtitle' field")
        })?,
        question_ids: question_ids.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'question_ids' field")
        })?,
    }
    .normalize()
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    validate_question_ids(&request.question_ids)?;
    ensure_paper_questions_valid(&state.pool, &request.question_ids).await?;

    Ok(Json(
        import_paper_zip(
            &state.object_store,
            file_name.as_deref(),
            &request,
            bytes,
            &current.user_id,
        )
        .await
        .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn download_papers_bundle(
    State(state): State<AppState>,
    Json(request): Json<PaperBundleRequest>,
) -> Result<Response, ApiError> {
    let paper_ids = request
        .normalize()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    build_paper_bundle_response(&state.object_store, &paper_ids)
        .await
        .map_err(ApiError::from)
}

pub(crate) async fn replace_paper_file(
    AxumPath(paper_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<PaperFileReplaceResponse> {
    parse_uuid_param(&paper_id, "paper_id")?;
    check_paper_write_access(&state, &current, &paper_id).await?;

    let (file_name, bytes) = read_uploaded_file(&mut multipart).await?;
    validate_upload_size(&bytes, MAX_UPLOAD_BYTES)?;

    Ok(Json(
        replace_paper_zip(&state.object_store, &paper_id, file_name.as_deref(), bytes)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn update_paper(
    AxumPath(paper_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdatePaperRequest>,
) -> ApiResult<PaperDetail> {
    parse_uuid_param(&paper_id, "paper_id")?;
    check_paper_write_access(&state, &current, &paper_id).await?;

    let update = request
        .normalize()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    if let Some(question_ids) = &update.question_ids {
        validate_question_ids(question_ids)?;
    }

    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin paper update tx failed")?;
    let exists =
        query("SELECT 1 FROM papers WHERE paper_id = $1::uuid AND deleted_at IS NULL FOR UPDATE")
            .bind(&paper_id)
            .fetch_optional(&mut *tx)
            .await
            .context("check paper existence failed")?
            .is_some();
    if !exists {
        return Err(ApiError::not_found(format!("paper not found: {paper_id}")));
    }

    let final_question_ids = if let Some(question_ids) = &update.question_ids {
        question_ids.clone()
    } else {
        query(
            "SELECT question_id::text AS question_id FROM paper_questions WHERE paper_id = $1::uuid ORDER BY sort_order",
        )
        .bind(&paper_id)
        .fetch_all(&mut *tx)
        .await
        .context("load paper question refs for validation failed")?
        .into_iter()
        .map(|row| row.get::<String, _>("question_id"))
        .collect()
    };

    ensure_paper_questions_valid(&mut *tx, &final_question_ids).await?;

    if let Some(description) = &update.description {
        query("UPDATE papers SET description = $2, updated_at = NOW() WHERE paper_id = $1::uuid")
            .bind(&paper_id)
            .bind(description)
            .execute(&mut *tx)
            .await
            .context("update paper description failed")?;
    }

    if let Some(title) = &update.title {
        query("UPDATE papers SET title = $2, updated_at = NOW() WHERE paper_id = $1::uuid")
            .bind(&paper_id)
            .bind(title)
            .execute(&mut *tx)
            .await
            .context("update paper title failed")?;
    }

    if let Some(subtitle) = &update.subtitle {
        query("UPDATE papers SET subtitle = $2, updated_at = NOW() WHERE paper_id = $1::uuid")
            .bind(&paper_id)
            .bind(subtitle)
            .execute(&mut *tx)
            .await
            .context("update paper subtitle failed")?;
    }

    if let Some(question_ids) = &update.question_ids {
        query("DELETE FROM paper_questions WHERE paper_id = $1::uuid")
            .bind(&paper_id)
            .execute(&mut *tx)
            .await
            .context("replace paper questions failed")?;

        for (idx, question_id) in question_ids.iter().enumerate() {
            query(
                r#"
                INSERT INTO paper_questions (paper_id, question_id, sort_order, created_at)
                VALUES ($1::uuid, $2::uuid, $3, NOW())
                "#,
            )
            .bind(&paper_id)
            .bind(question_id)
            .bind(i32::try_from(idx + 1).unwrap_or(i32::MAX))
            .execute(&mut *tx)
            .await
            .with_context(|| format!("replace paper question ref failed: {question_id}"))?;
        }

        query("UPDATE papers SET updated_at = NOW() WHERE paper_id = $1::uuid")
            .bind(&paper_id)
            .execute(&mut *tx)
            .await
            .context("touch paper updated_at after question update failed")?;
    }

    tx.commit().await.context("commit paper update failed")?;

    Ok(Json(fetch_paper_detail(&state, &paper_id).await?))
}

pub(crate) async fn delete_paper(
    AxumPath(paper_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<PaperDeleteResponse> {
    parse_uuid_param(&paper_id, "paper_id")?;
    check_paper_write_access(&state, &current, &paper_id).await?;

    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin paper delete tx failed")?;

    let exists =
        query("SELECT 1 FROM papers WHERE paper_id = $1::uuid AND deleted_at IS NULL FOR UPDATE")
            .bind(&paper_id)
            .fetch_optional(&mut *tx)
            .await
            .context("lock paper row for delete failed")?
            .is_some();
    if !exists {
        return Err(ApiError::not_found(format!("paper not found: {paper_id}")));
    }

    query(
        "UPDATE papers SET deleted_at = NOW(), deleted_by = $2, updated_at = NOW() WHERE paper_id = $1::uuid",
    )
    .bind(&paper_id)
    .bind(&current.user_id)
    .execute(&mut *tx)
    .await
    .context("soft delete paper failed")?;

    tx.commit().await.context("commit paper delete failed")?;

    Ok(Json(PaperDeleteResponse {
        paper_id,
        status: "deleted",
    }))
}

pub(crate) async fn get_paper_detail(
    AxumPath(paper_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<PaperDetail> {
    parse_uuid_param(&paper_id, "paper_id")?;
    Ok(Json(fetch_paper_detail(&state, &paper_id).await?))
}

async fn fetch_paper_detail(state: &AppState, paper_id: &str) -> Result<PaperDetail, ApiError> {
    load_paper_detail(
        &state.pool,
        paper_id,
        DetailVisibility::ActiveOnly,
        DetailVisibility::ActiveOnly,
    )
    .await
    .map(|loaded| loaded.detail)
    .map_err(ApiError::from)
}

fn validate_question_ids(question_ids: &[String]) -> Result<(), ApiError> {
    let mut seen_question_ids = HashSet::new();
    for question_id in question_ids {
        if !seen_question_ids.insert(question_id.clone()) {
            return Err(ApiError::bad_request(format!(
                "duplicate question_id in question_ids: {question_id}"
            )));
        }
        Uuid::parse_str(question_id)
            .map_err(|_| ApiError::bad_request(format!("invalid question_id: {question_id}")))?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Permission helpers
// ---------------------------------------------------------------------------

/// Check that the current user has write access to a paper.
/// Admin/Bot: always. Leader: if they are the owner. User/Viewer: never.
async fn check_paper_write_access(
    state: &AppState,
    current: &CurrentUser,
    paper_id: &str,
) -> Result<(), ApiError> {
    if current.role.is_admin_or_bot() {
        return Ok(());
    }

    if !current.role.can_create_paper() {
        return Err(ApiError::forbidden(
            "you do not have permission to modify this paper",
        ));
    }

    // Leader: must be the owner.
    let is_owner = query(
        "SELECT 1 FROM papers WHERE paper_id = $1::uuid AND deleted_at IS NULL AND created_by = $2::uuid",
    )
    .bind(paper_id)
    .bind(&current.user_id)
    .fetch_optional(&state.pool)
    .await
    .context("check paper ownership failed")
    .map_err(ApiError::from)?
    .is_some();

    if !is_owner {
        return Err(ApiError::forbidden(
            "you can only modify papers you created",
        ));
    }

    Ok(())
}
