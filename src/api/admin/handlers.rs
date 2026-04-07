use axum::{
    extract::{Path as AxumPath, Query, State},
    Extension, Json,
};

use super::{
    models::{
        AdminPaperDetail, AdminPaperSummary, AdminPapersParams, AdminQuestionDetail,
        AdminQuestionSummary, AdminQuestionsParams, GarbageCollectionRequest,
        GarbageCollectionResponse,
    },
    queries::{
        list_admin_papers, list_admin_questions, load_admin_paper_detail,
        load_admin_question_detail, preview_garbage_collection,
        restore_paper as restore_paper_record, restore_question as restore_question_record,
        run_garbage_collection,
    },
};
use crate::api::{
    auth::{
        models::{
            AdminUsersParams, CreateUserRequest, CurrentUser, MessageResponse,
            ResetPasswordRequest, Role, UpdateUserRequest, UserProfile,
        },
        password::hash_password,
        queries as auth_queries,
    },
    shared::{
        error::{ApiError, ApiResult},
        multipart::parse_uuid_param,
        pagination::Paginated,
    },
    AppState,
};

pub(crate) async fn list_questions(
    Query(params): Query<AdminQuestionsParams>,
    State(state): State<AppState>,
) -> ApiResult<Paginated<AdminQuestionSummary>> {
    let record_state = params
        .validate_filters()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let limit = params.normalized_limit();
    let offset = params.normalized_offset();
    let (questions, total) = list_admin_questions(&state.pool, &params, record_state)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(Paginated {
        items: questions,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn get_question_detail(
    AxumPath(question_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<AdminQuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    Ok(Json(
        load_admin_question_detail(&state.pool, &question_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn restore_question(
    AxumPath(question_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<AdminQuestionDetail> {
    parse_uuid_param(&question_id, "question_id")?;
    Ok(Json(
        restore_question_record(&state.pool, &question_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn list_papers(
    Query(params): Query<AdminPapersParams>,
    State(state): State<AppState>,
) -> ApiResult<Paginated<AdminPaperSummary>> {
    let record_state = params
        .validate_filters()
        .map_err(|err| ApiError::bad_request(err.to_string()))?;
    let limit = params.normalized_limit();
    let offset = params.normalized_offset();
    let (papers, total) = list_admin_papers(&state.pool, &params, record_state)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(Paginated {
        items: papers,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn get_paper_detail(
    AxumPath(paper_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<AdminPaperDetail> {
    parse_uuid_param(&paper_id, "paper_id")?;
    Ok(Json(
        load_admin_paper_detail(&state.pool, &paper_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn restore_paper(
    AxumPath(paper_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<AdminPaperDetail> {
    parse_uuid_param(&paper_id, "paper_id")?;
    Ok(Json(
        restore_paper_record(&state.pool, &paper_id)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn preview_gc(
    State(state): State<AppState>,
    Json(_request): Json<GarbageCollectionRequest>,
) -> ApiResult<GarbageCollectionResponse> {
    Ok(Json(
        preview_garbage_collection(&state.pool)
            .await
            .map_err(ApiError::from)?,
    ))
}

pub(crate) async fn run_gc(
    State(state): State<AppState>,
    Json(_request): Json<GarbageCollectionRequest>,
) -> ApiResult<GarbageCollectionResponse> {
    Ok(Json(
        run_garbage_collection(&state.pool)
            .await
            .map_err(ApiError::from)?,
    ))
}

// ---------------------------------------------------------------------------
// User management
// ---------------------------------------------------------------------------

pub(crate) async fn list_users(
    Query(params): Query<AdminUsersParams>,
    State(state): State<AppState>,
) -> ApiResult<Paginated<UserProfile>> {
    let (users, total) = auth_queries::list_users(&state.pool, params.limit, params.offset)
        .await
        .map_err(ApiError::from)?;
    let limit = crate::api::shared::pagination::normalize_limit(params.limit);
    let offset = crate::api::shared::pagination::normalize_offset(params.offset);
    Ok(Json(Paginated {
        items: users,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn create_user(
    State(state): State<AppState>,
    Json(req): Json<CreateUserRequest>,
) -> ApiResult<UserProfile> {
    let username = req.username.trim();
    if username.is_empty() {
        return Err(ApiError::bad_request("username must not be empty"));
    }
    if req.password.len() < 6 {
        return Err(ApiError::bad_request(
            "password must be at least 6 characters",
        ));
    }

    let role_str = req.role.as_deref().unwrap_or("viewer");
    if Role::from_str(role_str).is_none() {
        return Err(ApiError::bad_request(
            "role must be one of: viewer, editor, admin",
        ));
    }

    let display_name = req.display_name.as_deref().unwrap_or("");
    let pw_hash =
        hash_password(&req.password).map_err(|_| ApiError::internal("password hash error"))?;

    let profile =
        auth_queries::create_user(&state.pool, username, display_name, &pw_hash, role_str)
            .await
            .map_err(ApiError::from)?;

    Ok(Json(profile))
}

pub(crate) async fn update_user(
    AxumPath(user_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(req): Json<UpdateUserRequest>,
) -> ApiResult<UserProfile> {
    parse_uuid_param(&user_id, "user_id")?;

    // Prevent admin from deactivating themselves
    if req.is_active == Some(false) && current.user_id == user_id {
        return Err(ApiError::bad_request("cannot deactivate your own account"));
    }

    if let Some(role_str) = &req.role {
        if Role::from_str(role_str).is_none() {
            return Err(ApiError::bad_request(
                "role must be one of: viewer, editor, admin",
            ));
        }
    }

    let profile = auth_queries::update_user(
        &state.pool,
        &user_id,
        req.display_name.as_deref(),
        req.role.as_deref(),
        req.is_active,
    )
    .await
    .map_err(ApiError::from)?;

    Ok(Json(profile))
}

pub(crate) async fn delete_user(
    AxumPath(user_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<MessageResponse> {
    parse_uuid_param(&user_id, "user_id")?;

    if current.user_id == user_id {
        return Err(ApiError::bad_request("cannot delete your own account"));
    }

    auth_queries::delete_user(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(MessageResponse {
        message: "user deactivated",
    }))
}

pub(crate) async fn reset_password(
    AxumPath(user_id): AxumPath<String>,
    State(state): State<AppState>,
    Json(req): Json<ResetPasswordRequest>,
) -> ApiResult<MessageResponse> {
    parse_uuid_param(&user_id, "user_id")?;

    if req.new_password.len() < 6 {
        return Err(ApiError::bad_request(
            "password must be at least 6 characters",
        ));
    }

    // Verify user exists
    auth_queries::find_user_by_id(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;

    let new_hash =
        hash_password(&req.new_password).map_err(|_| ApiError::internal("password hash error"))?;
    auth_queries::update_password(&state.pool, &user_id, &new_hash)
        .await
        .map_err(ApiError::from)?;

    // Revoke all existing refresh tokens so the user must re-login
    auth_queries::revoke_all_refresh_tokens(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(MessageResponse {
        message: "password reset",
    }))
}
