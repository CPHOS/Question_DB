use anyhow::Context;
use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    response::Response,
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{query, Row};

use super::{
    bundles::build_question_bundle_response,
    imports::{import_question_zip, replace_question_zip, MAX_UPLOAD_BYTES},
    models::{
        CreateDifficultyRequest, CreateQuestionRequest, QuestionBundleRequest,
        QuestionDeleteResponse, QuestionDetail, QuestionDifficultyTagsResponse,
        QuestionFileReplaceResponse, QuestionImportResponse, QuestionSummary, QuestionTagsResponse,
        QuestionsParams, UpdateAuthorRequest, UpdateCategoryRequest, UpdateDescriptionRequest,
        UpdateDifficultyRequest, UpdateReviewerNamesRequest, UpdateStatusRequest,
        UpdateTagsRequest,
    },
    queries::{
        execute_questions_query, list_active_difficulty_tags, list_active_question_tags,
        load_question_difficulties_batch, load_question_tags_batch, map_question_summary,
        validate_question_filters,
    },
};
use crate::api::{
    auth::models::{CurrentUser, Role},
    shared::{
        details::{load_question_detail, DetailVisibility},
        error::{ApiError, ApiResult},
        multipart::{
            next_multipart_field, parse_uuid_param, read_file_field, read_json_field,
            read_text_field, read_uploaded_file, validate_upload_size,
        },
        pagination::Paginated,
    },
    AppState,
};

pub(crate) async fn list_questions(
    Query(params): Query<QuestionsParams>,
    State(state): State<AppState>,
) -> ApiResult<Paginated<QuestionSummary>> {
    validate_question_filters(&params).map_err(|e| ApiError::bad_request(e.to_string()))?;
    let mut plan = params.build_query();
    let limit = plan.limit;
    let offset = plan.offset;
    let rows = execute_questions_query(&state.pool, &mut plan)
        .await
        .context("query questions failed")
        .map_err(ApiError::from)?;

    let total = rows
        .first()
        .map(|r| r.get::<i64, _>("total_count"))
        .unwrap_or(0);
    let question_ids: Vec<String> = rows.iter().map(|r| r.get("question_id")).collect();
    let tags_map = load_question_tags_batch(&state.pool, &question_ids)
        .await
        .context("load question tags failed")
        .map_err(ApiError::from)?;
    let difficulty_map = load_question_difficulties_batch(&state.pool, &question_ids)
        .await
        .context("load question difficulties failed")
        .map_err(ApiError::from)?;

    let items = rows
        .into_iter()
        .map(|row| {
            let qid: String = row.get("question_id");
            let tags = tags_map.get(&qid).cloned().unwrap_or_default();
            let difficulty = difficulty_map.get(&qid).cloned().unwrap_or_default();
            map_question_summary(row, tags, difficulty)
        })
        .collect();

    Ok(Json(Paginated {
        items,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn list_question_tags(
    State(state): State<AppState>,
) -> ApiResult<QuestionTagsResponse> {
    let tags = list_active_question_tags(&state.pool)
        .await
        .context("list question tags failed")
        .map_err(ApiError::from)?;
    Ok(Json(QuestionTagsResponse { tags }))
}

pub(crate) async fn list_difficulty_tags(
    State(state): State<AppState>,
) -> ApiResult<QuestionDifficultyTagsResponse> {
    let difficulty_tags = list_active_difficulty_tags(&state.pool)
        .await
        .context("list difficulty tags failed")
        .map_err(ApiError::from)?;
    Ok(Json(QuestionDifficultyTagsResponse { difficulty_tags }))
}

pub(crate) async fn get_question_detail(
    AxumPath(question_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn download_questions_bundle(
    State(state): State<AppState>,
    Json(request): Json<QuestionBundleRequest>,
) -> Result<Response, ApiError> {
    let question_ids = request
        .normalize()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    build_question_bundle_response(&state.pool, &question_ids)
        .await
        .map_err(ApiError::from)
}

pub(crate) async fn create_question(
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<QuestionImportResponse> {
    if !current.role.can_upload_question() {
        return Err(ApiError::forbidden(
            "user role or above required to upload questions",
        ));
    }

    let mut file_name = None;
    let mut description = None;
    let mut category = None;
    let mut tags = None;
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
            "category" => {
                category = Some(read_text_field(field, "category").await?);
            }
            "tags" => {
                tags = Some(read_json_field(field, "tags").await?);
            }
            _ => {}
        }
    }

    validate_upload_size(&bytes, MAX_UPLOAD_BYTES)?;
    let request = CreateQuestionRequest {
        description: description.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'description' field")
        })?,
        category,
        tags,
    }
    .normalize()
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok(Json(
        import_question_zip(
            &state.pool,
            file_name.as_deref(),
            &request,
            bytes,
            &current.user_id,
            &current.display_name,
        )
        .await
        .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn replace_question_file(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<QuestionFileReplaceResponse> {
    parse_uuid_param(&question_id, "question_id")?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_owner) {
        return Err(ApiError::forbidden(
            "you do not have permission to replace this question's file",
        ));
    }

    let (file_name, bytes) = read_uploaded_file(&mut multipart).await?;
    validate_upload_size(&bytes, MAX_UPLOAD_BYTES)?;

    // Look up the creator's current display_name for resetting the author field.
    let creator_display_name = if let Some(ref created_by) = access.created_by {
        query("SELECT display_name FROM users WHERE user_id = $1::uuid")
            .bind(created_by)
            .fetch_optional(&state.pool)
            .await
            .context("look up creator display_name failed")
            .map_err(ApiError::from)?
            .map(|r| r.get::<String, _>("display_name"))
            .unwrap_or_default()
    } else {
        String::new()
    };

    Ok(Json(
        replace_question_zip(
            &state.pool,
            &question_id,
            file_name.as_deref(),
            bytes,
            &creator_display_name,
        )
        .await
        .map_err(ApiError::from)?,
    ))
}

// ---------------------------------------------------------------------------
// Per-field update handlers
// ---------------------------------------------------------------------------

pub(crate) async fn update_question_description(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateDescriptionRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let description = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_owner) {
        return Err(ApiError::forbidden(
            "you do not have permission to update this question's description",
        ));
    }
    query("UPDATE questions SET description = $2, updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .bind(&description)
        .execute(&state.pool)
        .await
        .context("update question description failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_category(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateCategoryRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let category = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_owner) {
        return Err(ApiError::forbidden(
            "you do not have permission to update this question's category",
        ));
    }
    query("UPDATE questions SET category = $2, updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .bind(&category)
        .execute(&state.pool)
        .await
        .context("update question category failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_tags(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateTagsRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let tags = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot
        || access.is_leader
        || access.is_owner
        || access.is_assigned_reviewer)
    {
        return Err(ApiError::forbidden(
            "you do not have permission to update this question's tags",
        ));
    }
    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin tags update tx failed")?;
    query("DELETE FROM question_tags WHERE question_id = $1::uuid")
        .bind(&question_id)
        .execute(&mut *tx)
        .await
        .context("delete old question tags failed")?;
    for (idx, tag) in tags.iter().enumerate() {
        query("INSERT INTO question_tags (question_id, tag, sort_order) VALUES ($1::uuid, $2, $3)")
            .bind(&question_id)
            .bind(tag)
            .bind(i32::try_from(idx).unwrap_or(i32::MAX))
            .execute(&mut *tx)
            .await
            .with_context(|| format!("insert question tag failed: {tag}"))
            .map_err(ApiError::from)?;
    }
    query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .execute(&mut *tx)
        .await
        .context("touch question updated_at failed")
        .map_err(ApiError::from)?;
    // Auto-add reviewer display_name to questions.reviewers if acting as reviewer.
    if access.is_assigned_reviewer {
        auto_add_reviewer_display_name(&mut tx, &question_id, &current.display_name).await?;
    }
    tx.commit()
        .await
        .context("commit tags update failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_status(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateStatusRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let status = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if access.is_admin_or_bot {
        // admin/bot can set any valid status
    } else if access.is_leader {
        // leader can only set none or reviewed
        if status == "used" {
            return Err(ApiError::forbidden(
                "leader cannot set status to 'used'; admin role required",
            ));
        }
    } else {
        return Err(ApiError::forbidden(
            "leader role or above required to update question status",
        ));
    }
    query("UPDATE questions SET status = $2, updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .bind(&status)
        .execute(&state.pool)
        .await
        .context("update question status failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_author(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateAuthorRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    if !current.role.is_admin_or_bot() {
        return Err(ApiError::forbidden(
            "admin or bot role required to update author",
        ));
    }
    let author = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let rows_affected = query("UPDATE questions SET author = $2, updated_at = NOW() WHERE question_id = $1::uuid AND deleted_at IS NULL")
        .bind(&question_id)
        .bind(&author)
        .execute(&state.pool)
        .await
        .context("update question author failed")
        .map_err(ApiError::from)?
        .rows_affected();
    if rows_affected == 0 {
        return Err(ApiError::not_found(format!(
            "question not found: {question_id}"
        )));
    }
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_reviewer_names(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateReviewerNamesRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    if !current.role.is_admin_or_bot() {
        return Err(ApiError::forbidden(
            "admin or bot role required to update reviewer names",
        ));
    }
    let reviewers = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let rows_affected = query("UPDATE questions SET reviewers = $2, updated_at = NOW() WHERE question_id = $1::uuid AND deleted_at IS NULL")
        .bind(&question_id)
        .bind(&reviewers)
        .execute(&state.pool)
        .await
        .context("update question reviewer names failed")
        .map_err(ApiError::from)?
        .rows_affected();
    if rows_affected == 0 {
        return Err(ApiError::not_found(format!(
            "question not found: {question_id}"
        )));
    }
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

// ---------------------------------------------------------------------------
// Difficulty CRUD handlers
// ---------------------------------------------------------------------------

pub(crate) async fn create_question_difficulty(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<CreateDifficultyRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let (algorithm_tag, score, notes) = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_assigned_reviewer) {
        return Err(ApiError::forbidden(
            "you do not have permission to create difficulty entries on this question",
        ));
    }
    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin difficulty create tx failed")?;
    // Check for duplicate
    let exists = query(
        "SELECT 1 FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2",
    )
    .bind(&question_id)
    .bind(&algorithm_tag)
    .fetch_optional(&mut *tx)
    .await
    .context("check existing difficulty tag failed")
    .map_err(ApiError::from)?
    .is_some();
    if exists {
        return Err(ApiError::conflict(format!(
            "difficulty tag already exists: {algorithm_tag}"
        )));
    }
    query(
        "INSERT INTO question_difficulties (question_id, algorithm_tag, score, notes, created_by, updated_by) VALUES ($1::uuid, $2, $3, $4, $5::uuid, $5::uuid)",
    )
    .bind(&question_id)
    .bind(&algorithm_tag)
    .bind(score)
    .bind(notes.as_deref())
    .bind(&current.user_id)
    .execute(&mut *tx)
    .await
    .context("insert difficulty entry failed")
    .map_err(ApiError::from)?;
    query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .execute(&mut *tx)
        .await
        .context("touch question updated_at failed")
        .map_err(ApiError::from)?;
    if access.is_assigned_reviewer {
        auto_add_reviewer_display_name(&mut tx, &question_id, &current.display_name).await?;
    }
    tx.commit()
        .await
        .context("commit difficulty create failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn update_question_difficulty(
    AxumPath((question_id, algorithm_tag)): AxumPath<(String, String)>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateDifficultyRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let algorithm_tag = algorithm_tag.trim().to_string();
    if algorithm_tag.is_empty() {
        return Err(ApiError::bad_request("algorithm_tag must not be empty"));
    }
    let (score, notes) = request
        .normalize()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_assigned_reviewer) {
        return Err(ApiError::forbidden(
            "you do not have permission to update difficulty entries on this question",
        ));
    }
    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin difficulty update tx failed")?;
    // Check entry exists and ownership
    let existing = query(
        "SELECT created_by::text AS created_by FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2",
    )
    .bind(&question_id)
    .bind(&algorithm_tag)
    .fetch_optional(&mut *tx)
    .await
    .context("check existing difficulty tag failed")
    .map_err(ApiError::from)?;
    let existing = existing
        .ok_or_else(|| ApiError::not_found(format!("difficulty tag not found: {algorithm_tag}")))?;
    // Reviewer can only modify entries they created.
    if access.is_assigned_reviewer && !access.is_admin_or_bot && !access.is_leader {
        let tag_created_by: Option<String> = existing.get("created_by");
        if tag_created_by.as_deref() != Some(&current.user_id) {
            return Err(ApiError::forbidden(format!(
                "you can only modify difficulty entries you created; tag: {algorithm_tag}"
            )));
        }
    }
    query(
        "UPDATE question_difficulties SET score = $3, notes = $4, updated_by = $5::uuid WHERE question_id = $1::uuid AND algorithm_tag = $2",
    )
    .bind(&question_id)
    .bind(&algorithm_tag)
    .bind(score)
    .bind(notes.as_deref())
    .bind(&current.user_id)
    .execute(&mut *tx)
    .await
    .context("update difficulty entry failed")
    .map_err(ApiError::from)?;
    query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .execute(&mut *tx)
        .await
        .context("touch question updated_at failed")
        .map_err(ApiError::from)?;
    if access.is_assigned_reviewer {
        auto_add_reviewer_display_name(&mut tx, &question_id, &current.display_name).await?;
    }
    tx.commit()
        .await
        .context("commit difficulty update failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn delete_question_difficulty(
    AxumPath((question_id, algorithm_tag)): AxumPath<(String, String)>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let algorithm_tag = algorithm_tag.trim().to_string();
    if algorithm_tag.is_empty() {
        return Err(ApiError::bad_request("algorithm_tag must not be empty"));
    }
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader || access.is_assigned_reviewer) {
        return Err(ApiError::forbidden(
            "you do not have permission to delete difficulty entries on this question",
        ));
    }
    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin difficulty delete tx failed")?;
    // Check entry exists and ownership
    let existing = query(
        "SELECT created_by::text AS created_by FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2",
    )
    .bind(&question_id)
    .bind(&algorithm_tag)
    .fetch_optional(&mut *tx)
    .await
    .context("check existing difficulty tag failed")
    .map_err(ApiError::from)?;
    let existing = existing
        .ok_or_else(|| ApiError::not_found(format!("difficulty tag not found: {algorithm_tag}")))?;
    if access.is_assigned_reviewer && !access.is_admin_or_bot && !access.is_leader {
        let tag_created_by: Option<String> = existing.get("created_by");
        if tag_created_by.as_deref() != Some(&current.user_id) {
            return Err(ApiError::forbidden(format!(
                "you can only delete difficulty entries you created; tag: {algorithm_tag}"
            )));
        }
    }
    query("DELETE FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2")
        .bind(&question_id)
        .bind(&algorithm_tag)
        .execute(&mut *tx)
        .await
        .context("delete difficulty entry failed")
        .map_err(ApiError::from)?;
    query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
        .bind(&question_id)
        .execute(&mut *tx)
        .await
        .context("touch question updated_at failed")
        .map_err(ApiError::from)?;
    if access.is_assigned_reviewer {
        auto_add_reviewer_display_name(&mut tx, &question_id, &current.display_name).await?;
    }
    tx.commit()
        .await
        .context("commit difficulty delete failed")
        .map_err(ApiError::from)?;
    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

// ---------------------------------------------------------------------------
// Delete handler
// ---------------------------------------------------------------------------

pub(crate) async fn delete_question(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<QuestionDeleteResponse> {
    parse_uuid_param(&question_id, "question_id")?;
    let access = load_question_access(&state.pool, &current, &question_id).await?;
    if !(access.is_admin_or_bot || access.is_leader) {
        return Err(ApiError::forbidden(
            "leader role or above required to delete questions",
        ));
    }

    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin question delete tx failed")?;

    let row = query(
        "SELECT status FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(&question_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock question row for delete failed")?;
    let row =
        row.ok_or_else(|| ApiError::not_found(format!("question not found: {question_id}")))?;
    let status: String = row.get("status");

    // Leader cannot delete used questions; admin/bot can delete anything.
    if status == "used" && !current.role.is_admin_or_bot() {
        return Err(ApiError::forbidden(
            "cannot delete a question with status 'used'; admin role required",
        ));
    }

    let active_paper_ref = query(
        r#"
        SELECT p.paper_id::text AS paper_id
        FROM paper_questions pq
        JOIN papers p ON p.paper_id = pq.paper_id
        WHERE pq.question_id = $1::uuid AND p.deleted_at IS NULL
        LIMIT 1
        "#,
    )
    .bind(&question_id)
    .fetch_optional(&mut *tx)
    .await
    .context("check active paper references before question delete failed")?;
    if let Some(row) = active_paper_ref {
        let paper_id: String = row.get("paper_id");
        return Err(ApiError::conflict(format!(
            "question {question_id} is still referenced by active paper {paper_id}"
        )));
    }

    query(
        "UPDATE questions SET deleted_at = NOW(), deleted_by = $2, updated_at = NOW() WHERE question_id = $1::uuid",
    )
    .bind(&question_id)
    .bind(&current.user_id)
    .execute(&mut *tx)
    .await
    .context("soft delete question failed")?;

    tx.commit().await.context("commit question delete failed")?;

    Ok(Json(QuestionDeleteResponse {
        question_id,
        status: "deleted",
    }))
}

async fn fetch_question_detail(
    state: &AppState,
    question_id: &str,
) -> Result<QuestionDetail, ApiError> {
    load_question_detail(
        &state.pool,
        question_id,
        DetailVisibility::ActiveOnly,
        DetailVisibility::ActiveOnly,
    )
    .await
    .map(|loaded| loaded.detail)
    .map_err(ApiError::from)
}

// ---------------------------------------------------------------------------
// Permission / access helpers
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct QuestionAccessInfo {
    #[allow(dead_code)]
    status: String,
    created_by: Option<String>,
    is_admin_or_bot: bool,
    is_leader: bool,
    is_owner: bool,
    is_assigned_reviewer: bool,
}

/// Load a question row and determine the current user's access level.
async fn load_question_access(
    pool: &sqlx::PgPool,
    current: &CurrentUser,
    question_id: &str,
) -> Result<QuestionAccessInfo, ApiError> {
    let row = query(
        "SELECT status, created_by::text AS created_by FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL",
    )
    .bind(question_id)
    .fetch_optional(pool)
    .await
    .context("load question for access check failed")
    .map_err(ApiError::from)?
    .ok_or_else(|| ApiError::not_found(format!("question not found: {question_id}")))?;

    let status: String = row.get("status");
    let created_by: Option<String> = row.get("created_by");

    let is_admin_or_bot = current.role.is_admin_or_bot();
    let is_leader = current.role == Role::Leader && status != "used";
    let is_owner = created_by.as_deref() == Some(&current.user_id);

    // Only users with 'user' role can be assigned reviewers.
    let is_assigned_reviewer = if current.role == Role::User {
        query(
            "SELECT 1 FROM question_reviews WHERE question_id = $1::uuid AND reviewer_id = $2::uuid",
        )
        .bind(question_id)
        .bind(&current.user_id)
        .fetch_optional(pool)
        .await
        .context("check reviewer assignment failed")
        .map_err(ApiError::from)?
        .is_some()
    } else {
        false
    };

    Ok(QuestionAccessInfo {
        status,
        created_by,
        is_admin_or_bot,
        is_leader,
        is_owner,
        is_assigned_reviewer,
    })
}

/// Auto-add the reviewer's display_name to the questions.reviewers TEXT[] array (deduplicated).
async fn auto_add_reviewer_display_name(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    question_id: &str,
    display_name: &str,
) -> Result<(), ApiError> {
    // Append to array only if not already present.
    query(
        r#"UPDATE questions
           SET reviewers = CASE
               WHEN $2 = ANY(reviewers) THEN reviewers
               ELSE array_append(reviewers, $2)
           END,
           updated_at = NOW()
           WHERE question_id = $1::uuid"#,
    )
    .bind(question_id)
    .bind(display_name)
    .execute(&mut **tx)
    .await
    .context("auto-add reviewer display_name failed")
    .map_err(ApiError::from)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Review management endpoints
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct AssignReviewerRequest {
    pub(crate) reviewer_id: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionReviewer {
    pub(crate) reviewer_id: String,
    pub(crate) username: String,
    pub(crate) display_name: String,
    pub(crate) assigned_by: String,
    pub(crate) created_at: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionReviewersResponse {
    pub(crate) reviewers: Vec<QuestionReviewer>,
}

/// POST /questions/:question_id/reviewers — assign a user as reviewer.
pub(crate) async fn assign_reviewer(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(req): Json<AssignReviewerRequest>,
) -> ApiResult<QuestionReviewersResponse> {
    parse_uuid_param(&question_id, "question_id")?;
    parse_uuid_param(&req.reviewer_id, "reviewer_id")?;

    // Only leader/bot/admin can assign reviewers.
    if !current.role.is_leader_or_above() {
        return Err(ApiError::forbidden(
            "leader role or above required to assign reviewers",
        ));
    }

    // Verify the question exists and is active.
    let exists =
        query("SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL")
            .bind(&question_id)
            .fetch_optional(&state.pool)
            .await
            .context("check question existence failed")
            .map_err(ApiError::from)?
            .is_some();
    if !exists {
        return Err(ApiError::not_found(format!(
            "question not found: {question_id}"
        )));
    }

    // Verify the reviewer is an active user with 'user' role.
    let reviewer_row = query("SELECT role, is_active FROM users WHERE user_id = $1::uuid")
        .bind(&req.reviewer_id)
        .fetch_optional(&state.pool)
        .await
        .context("check reviewer user failed")
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found(format!("user not found: {}", req.reviewer_id)))?;
    let reviewer_role: String = reviewer_row.get("role");
    let reviewer_active: bool = reviewer_row.get("is_active");
    if !reviewer_active {
        return Err(ApiError::bad_request("reviewer account is disabled"));
    }
    if reviewer_role != "user" {
        return Err(ApiError::bad_request(
            "only users with 'user' role can be assigned as reviewers",
        ));
    }

    // Insert (ignore duplicate).
    query(
        r#"INSERT INTO question_reviews (question_id, reviewer_id, assigned_by)
           VALUES ($1::uuid, $2::uuid, $3::uuid)
           ON CONFLICT (question_id, reviewer_id) DO NOTHING"#,
    )
    .bind(&question_id)
    .bind(&req.reviewer_id)
    .bind(&current.user_id)
    .execute(&state.pool)
    .await
    .context("assign reviewer failed")
    .map_err(ApiError::from)?;

    list_question_reviewers_inner(&state, &question_id).await
}

/// DELETE /questions/:question_id/reviewers/:reviewer_id — remove a reviewer.
pub(crate) async fn remove_reviewer(
    AxumPath((question_id, reviewer_id)): AxumPath<(String, String)>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<QuestionReviewersResponse> {
    parse_uuid_param(&question_id, "question_id")?;
    parse_uuid_param(&reviewer_id, "reviewer_id")?;

    if !current.role.is_leader_or_above() {
        return Err(ApiError::forbidden(
            "leader role or above required to remove reviewers",
        ));
    }

    query("DELETE FROM question_reviews WHERE question_id = $1::uuid AND reviewer_id = $2::uuid")
        .bind(&question_id)
        .bind(&reviewer_id)
        .execute(&state.pool)
        .await
        .context("remove reviewer failed")
        .map_err(ApiError::from)?;

    list_question_reviewers_inner(&state, &question_id).await
}

/// GET /questions/:question_id/reviewers — list all assigned reviewers.
pub(crate) async fn list_question_reviewers(
    AxumPath(question_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<QuestionReviewersResponse> {
    parse_uuid_param(&question_id, "question_id")?;
    list_question_reviewers_inner(&state, &question_id).await
}

async fn list_question_reviewers_inner(
    state: &AppState,
    question_id: &str,
) -> ApiResult<QuestionReviewersResponse> {
    let rows = query(
        r#"SELECT qr.reviewer_id::text AS reviewer_id, u.username, u.display_name,
                  qr.assigned_by::text AS assigned_by,
                  to_char(qr.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at
           FROM question_reviews qr
           JOIN users u ON u.user_id = qr.reviewer_id
           WHERE qr.question_id = $1::uuid
           ORDER BY qr.created_at ASC"#,
    )
    .bind(question_id)
    .fetch_all(&state.pool)
    .await
    .context("list question reviewers failed")
    .map_err(ApiError::from)?;

    let reviewers = rows
        .into_iter()
        .map(|r| QuestionReviewer {
            reviewer_id: r.get("reviewer_id"),
            username: r.get("username"),
            display_name: r.get("display_name"),
            assigned_by: r.get("assigned_by"),
            created_at: r.get("created_at"),
        })
        .collect();

    Ok(Json(QuestionReviewersResponse { reviewers }))
}
