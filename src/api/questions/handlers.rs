use anyhow::Context;
use axum::{
    extract::{Multipart, Path as AxumPath, Query, State},
    response::Response,
    Extension, Json,
};
use sqlx::{query, Row};

use super::{
    bundles::build_question_bundle_response,
    imports::{import_question_zip, replace_question_zip, MAX_UPLOAD_BYTES},
    models::{
        CreateQuestionRequest, QuestionBundleRequest, QuestionDeleteResponse, QuestionDetail,
        QuestionDifficulty, QuestionDifficultyTagsResponse, QuestionFileReplaceResponse,
        QuestionImportResponse, QuestionSummary, QuestionTagsResponse, QuestionsParams,
        UpdateQuestionMetadataRequest,
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
        return Err(ApiError::forbidden("user role or above required to upload questions"));
    }

    let mut file_name = None;
    let mut description = None;
    let mut category = None;
    let mut tags = None;
    let mut status = None;
    let mut difficulty = None;
    let mut author = None;
    let mut reviewers = None;
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
            "status" => {
                status = Some(read_text_field(field, "status").await?);
            }
            "difficulty" => {
                difficulty =
                    Some(read_json_field::<QuestionDifficulty>(field, "difficulty").await?);
            }
            "author" => {
                author = Some(read_text_field(field, "author").await?);
            }
            "reviewers" => {
                reviewers = Some(read_json_field(field, "reviewers").await?);
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
        status,
        difficulty: difficulty.ok_or_else(|| {
            ApiError::bad_request("multipart form must include a non-empty 'difficulty' field")
        })?,
        author,
        reviewers,
    }
    .normalize()
    .map_err(|err| ApiError::bad_request(err.to_string()))?;

    Ok(Json(
        import_question_zip(&state.pool, file_name.as_deref(), &request, bytes, &current.user_id)
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
    check_question_write_access(&state, &current, &question_id).await?;

    let (file_name, bytes) = read_uploaded_file(&mut multipart).await?;
    validate_upload_size(&bytes, MAX_UPLOAD_BYTES)?;

    Ok(Json(
        replace_question_zip(&state.pool, &question_id, file_name.as_deref(), bytes)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn update_question_metadata(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(request): Json<UpdateQuestionMetadataRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let update = request
        .normalize()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

    // Determine access level.
    let access = determine_question_access(&state, &current, &question_id).await?;
    match access {
        QuestionAccess::Full => {
            // Full access replaces all difficulty tags — must include human.
            if let Some(ref d) = update.difficulty {
                if !d.contains_key("human") {
                    return Err(ApiError::bad_request(
                        "difficulty must include a human entry".to_string(),
                    ));
                }
            }
        }
        QuestionAccess::ReviewerOnly => {
            // Reviewers can only update difficulty tags.
            if update.category.is_some()
                || update.description.is_some()
                || update.tags.is_some()
                || update.status.is_some()
                || update.author.is_some()
                || update.reviewers.is_some()
                || update.allow_auto_reviewer.is_some()
            {
                return Err(ApiError::forbidden(
                    "as a reviewer you can only update difficulty tags",
                ));
            }
        }
        QuestionAccess::None => {
            return Err(ApiError::forbidden("you do not have permission to update this question"));
        }
    }

    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin question metadata update tx failed")?;

    // Lock the parent row up front so concurrent writers on the same question
    // serialize even when child-table replacement starts from an empty set.
    let exists = query(
        "SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(&question_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock question row for metadata update failed")?
    .is_some();
    if !exists {
        return Err(ApiError::not_found(format!(
            "question not found: {question_id}"
        )));
    }

    if let Some(category) = &update.category {
        query(
            "UPDATE questions SET category = $2, updated_at = NOW() WHERE question_id = $1::uuid",
        )
        .bind(&question_id)
        .bind(category)
        .execute(&mut *tx)
        .await
        .context("update question category failed")?;
    }

    if let Some(description) = &update.description {
        query(
            "UPDATE questions SET description = $2, updated_at = NOW() WHERE question_id = $1::uuid",
        )
            .bind(&question_id)
            .bind(description)
            .execute(&mut *tx)
            .await
            .context("update question description failed")?;
    }

    if let Some(status) = &update.status {
        query("UPDATE questions SET status = $2, updated_at = NOW() WHERE question_id = $1::uuid")
            .bind(&question_id)
            .bind(status)
            .execute(&mut *tx)
            .await
            .context("update question status failed")?;
    }

    if let Some(author) = &update.author {
        query("UPDATE questions SET author = $2, updated_at = NOW() WHERE question_id = $1::uuid")
            .bind(&question_id)
            .bind(author)
            .execute(&mut *tx)
            .await
            .context("update question author failed")?;
    }

    if let Some(reviewers) = &update.reviewers {
        query(
            "UPDATE questions SET reviewers = $2, updated_at = NOW() WHERE question_id = $1::uuid",
        )
        .bind(&question_id)
        .bind(reviewers)
        .execute(&mut *tx)
        .await
        .context("update question reviewers failed")?;
    }

    if let Some(allow_auto_reviewer) = update.allow_auto_reviewer {
        query(
            "UPDATE questions SET allow_auto_reviewer = $2, updated_at = NOW() WHERE question_id = $1::uuid",
        )
        .bind(&question_id)
        .bind(allow_auto_reviewer)
        .execute(&mut *tx)
        .await
        .context("update question allow_auto_reviewer failed")?;
    }

    // Handle difficulty tag deletions.
    if let Some(delete_tags) = &update.delete_difficulty_tags {
        for tag in delete_tags {
            if access == QuestionAccess::ReviewerOnly {
                // Reviewer can only delete tags they created.
                let owned = query(
                    "SELECT 1 FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2 AND created_by = $3::uuid",
                )
                .bind(&question_id)
                .bind(tag)
                .bind(&current.user_id)
                .fetch_optional(&mut *tx)
                .await
                .context("check difficulty tag ownership failed")?
                .is_some();
                if !owned {
                    return Err(ApiError::forbidden(format!(
                        "you can only delete difficulty tags you created; tag: {tag}"
                    )));
                }
            }
            query(
                "DELETE FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2",
            )
            .bind(&question_id)
            .bind(tag)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("delete difficulty tag failed: {tag}"))?;
        }

        query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
            .bind(&question_id)
            .execute(&mut *tx)
            .await
            .context("touch question updated_at after difficulty delete failed")?;
    }

    if let Some(difficulty) = &update.difficulty {
        if access == QuestionAccess::ReviewerOnly {
            // Reviewer: merge mode — only update/insert tags they have permission on.
            for (algorithm_tag, value) in difficulty {
                // Check if tag already exists.
                let existing = query(
                    "SELECT created_by::text AS created_by, updated_by::text AS updated_by FROM question_difficulties WHERE question_id = $1::uuid AND algorithm_tag = $2",
                )
                .bind(&question_id)
                .bind(algorithm_tag)
                .fetch_optional(&mut *tx)
                .await
                .context("check existing difficulty tag failed")?;

                if let Some(row) = existing {
                    // Tag exists — check if reviewer can modify it.
                    let tag_created_by: Option<String> = row.get("created_by");
                    let tag_updated_by: Option<String> = row.get("updated_by");
                    let is_own = tag_created_by.as_deref() == Some(&current.user_id);
                    let is_human_last_editor = algorithm_tag == "human"
                        && tag_updated_by.as_deref() == Some(&current.user_id);
                    if !is_own && !is_human_last_editor {
                        return Err(ApiError::forbidden(format!(
                            "you do not have permission to modify difficulty tag: {algorithm_tag}"
                        )));
                    }
                    query(
                        "UPDATE question_difficulties SET score = $3, notes = $4, updated_by = $5::uuid WHERE question_id = $1::uuid AND algorithm_tag = $2",
                    )
                    .bind(&question_id)
                    .bind(algorithm_tag)
                    .bind(value.score)
                    .bind(value.notes.as_deref())
                    .bind(&current.user_id)
                    .execute(&mut *tx)
                    .await
                    .with_context(|| format!("update difficulty tag failed: {algorithm_tag}"))?;
                } else {
                    // New tag — insert with ownership.
                    query(
                        "INSERT INTO question_difficulties (question_id, algorithm_tag, score, notes, created_by, updated_by) VALUES ($1::uuid, $2, $3, $4, $5::uuid, $5::uuid)",
                    )
                    .bind(&question_id)
                    .bind(algorithm_tag)
                    .bind(value.score)
                    .bind(value.notes.as_deref())
                    .bind(&current.user_id)
                    .execute(&mut *tx)
                    .await
                    .with_context(|| format!("insert difficulty tag failed: {algorithm_tag}"))?;
                }
            }
        } else {
            // Full access: replace all difficulty tags.
            query("DELETE FROM question_difficulties WHERE question_id = $1::uuid")
                .bind(&question_id)
                .execute(&mut *tx)
                .await
                .context("replace question difficulties failed")?;

            for (algorithm_tag, value) in difficulty {
                query(
                    "INSERT INTO question_difficulties (question_id, algorithm_tag, score, notes, created_by, updated_by) VALUES ($1::uuid, $2, $3, $4, $5::uuid, $5::uuid)",
                )
                .bind(&question_id)
                .bind(algorithm_tag)
                .bind(value.score)
                .bind(value.notes.as_deref())
                .bind(&current.user_id)
                .execute(&mut *tx)
                .await
                .with_context(|| format!("insert updated question difficulty failed: {algorithm_tag}"))?;
            }
        }

        query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
            .bind(&question_id)
            .execute(&mut *tx)
            .await
            .context("touch question updated_at after difficulty update failed")?;
    }

    if let Some(tags) = &update.tags {
        query("DELETE FROM question_tags WHERE question_id = $1::uuid")
            .bind(&question_id)
            .execute(&mut *tx)
            .await
            .context("replace question tags failed")?;

        for (idx, tag) in tags.iter().enumerate() {
            query("INSERT INTO question_tags (question_id, tag, sort_order) VALUES ($1::uuid, $2, $3)")
                .bind(&question_id)
                .bind(tag)
                .bind(i32::try_from(idx).unwrap_or(i32::MAX))
                .execute(&mut *tx)
                .await
                .with_context(|| format!("insert updated question tag failed: {tag}"))?;
        }

        query("UPDATE questions SET updated_at = NOW() WHERE question_id = $1::uuid")
            .bind(&question_id)
            .execute(&mut *tx)
            .await
            .context("touch question updated_at after tag update failed")?;
    }

    tx.commit()
        .await
        .context("commit question metadata update failed")?;

    Ok(Json(fetch_question_detail(&state, &question_id).await?))
}

pub(crate) async fn delete_question(
    AxumPath(question_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<QuestionDeleteResponse> {
    parse_uuid_param(&question_id, "question_id")?;

    // Only leader/bot (non-used status) or admin can delete questions.
    if !current.role.is_leader_or_above() {
        return Err(ApiError::forbidden("leader role or above required to delete questions"));
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
    let row = row.ok_or_else(|| ApiError::not_found(format!("question not found: {question_id}")))?;
    let status: String = row.get("status");

    // Leader/bot cannot delete used questions; admin can delete anything.
    if status == "used" && !current.role.is_admin() {
        return Err(ApiError::forbidden("cannot delete a question with status 'used'; admin role required"));
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
// Permission helpers
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuestionAccess {
    Full,
    ReviewerOnly,
    None,
}

/// Determine what level of access the current user has on a question.
async fn determine_question_access(
    state: &AppState,
    current: &CurrentUser,
    question_id: &str,
) -> Result<QuestionAccess, ApiError> {
    // Admin: full access on everything.
    if current.role.is_admin() {
        return Ok(QuestionAccess::Full);
    }

    // Leader/Bot: full access on non-used questions.
    if current.role.is_leader_or_above() {
        let status: Option<String> = query(
            "SELECT status FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL",
        )
        .bind(question_id)
        .fetch_optional(&state.pool)
        .await
        .context("check question status failed")
        .map_err(ApiError::from)?
        .map(|r| r.get("status"));
        let status = status.ok_or_else(|| ApiError::not_found(format!("question not found: {question_id}")))?;
        if status == "used" {
            return Ok(QuestionAccess::None);
        }
        return Ok(QuestionAccess::Full);
    }

    // User: full access if owner.
    if current.role == Role::User {
        let is_owner = query(
            "SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL AND created_by = $2::uuid",
        )
        .bind(question_id)
        .bind(&current.user_id)
        .fetch_optional(&state.pool)
        .await
        .context("check question ownership failed")
        .map_err(ApiError::from)?
        .is_some();
        if is_owner {
            return Ok(QuestionAccess::Full);
        }

        // Check if assigned as reviewer.
        let is_reviewer = query(
            "SELECT 1 FROM question_reviews WHERE question_id = $1::uuid AND reviewer_id = $2::uuid",
        )
        .bind(question_id)
        .bind(&current.user_id)
        .fetch_optional(&state.pool)
        .await
        .context("check reviewer assignment failed")
        .map_err(ApiError::from)?
        .is_some();
        if is_reviewer {
            return Ok(QuestionAccess::ReviewerOnly);
        }
    }

    Ok(QuestionAccess::None)
}

/// Check that the current user has write access to a question (for file replace, etc.).
/// Reviewers cannot replace files — only full access users can.
async fn check_question_write_access(
    state: &AppState,
    current: &CurrentUser,
    question_id: &str,
) -> Result<(), ApiError> {
    let access = determine_question_access(state, current, question_id).await?;
    if access != QuestionAccess::Full {
        return Err(ApiError::forbidden("you do not have permission to modify this question"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Review management endpoints
// ---------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

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
        return Err(ApiError::forbidden("leader role or above required to assign reviewers"));
    }

    // Verify the question exists and is active.
    let exists = query("SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL")
        .bind(&question_id)
        .fetch_optional(&state.pool)
        .await
        .context("check question existence failed")
        .map_err(ApiError::from)?
        .is_some();
    if !exists {
        return Err(ApiError::not_found(format!("question not found: {question_id}")));
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
        return Err(ApiError::bad_request("only users with 'user' role can be assigned as reviewers"));
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
        return Err(ApiError::forbidden("leader role or above required to remove reviewers"));
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
