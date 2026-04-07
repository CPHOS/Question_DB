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
        QuestionDifficulty, QuestionFileReplaceResponse, QuestionImportResponse, QuestionSummary,
        QuestionTagsResponse, QuestionsParams, UpdateQuestionMetadataRequest,
    },
    queries::{
        execute_questions_query, list_active_question_tags, load_question_difficulties_batch,
        load_question_tags_batch, map_question_summary, validate_question_filters,
    },
};
use crate::api::{
    auth::models::CurrentUser,
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
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<QuestionImportResponse> {
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
        import_question_zip(&state.pool, file_name.as_deref(), &request, bytes)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn replace_question_file(
    AxumPath(question_id): AxumPath<String>,
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<QuestionFileReplaceResponse> {
    parse_uuid_param(&question_id, "question_id")?;

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
    State(state): State<AppState>,
    Json(request): Json<UpdateQuestionMetadataRequest>,
) -> ApiResult<QuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    let update = request
        .normalize()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;

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

    if let Some(difficulty) = &update.difficulty {
        query("DELETE FROM question_difficulties WHERE question_id = $1::uuid")
            .bind(&question_id)
            .execute(&mut *tx)
            .await
            .context("replace question difficulties failed")?;

        for (algorithm_tag, value) in difficulty {
            query(
                "INSERT INTO question_difficulties (question_id, algorithm_tag, score, notes) VALUES ($1::uuid, $2, $3, $4)",
            )
            .bind(&question_id)
            .bind(algorithm_tag)
            .bind(value.score)
            .bind(value.notes.as_deref())
            .execute(&mut *tx)
            .await
            .with_context(|| format!("insert updated question difficulty failed: {algorithm_tag}"))?;
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

    let mut tx = state
        .pool
        .begin()
        .await
        .context("begin question delete tx failed")?;

    let exists = query(
        "SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(&question_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock question row for delete failed")?
    .is_some();
    if !exists {
        return Err(ApiError::not_found(format!(
            "question not found: {question_id}"
        )));
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
