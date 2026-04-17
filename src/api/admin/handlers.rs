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
            AdminUserResponse, AdminUsersParams, CreateUserRequest, CurrentUser, MessageResponse,
            ResetPasswordRequest, Role, UpdateUserRequest, UserProfile,
        },
        password::hash_password,
        queries as auth_queries,
        token::{generate_bot_access_token, hash_bot_access_token},
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
) -> ApiResult<AdminUserResponse> {
    let username = req.username.trim();
    if username.is_empty() {
        return Err(ApiError::bad_request("username must not be empty"));
    }

    let role_str = req.role.as_deref().unwrap_or("viewer");
    let role = if let Some(role) = Role::from_str(role_str) {
        role
    } else {
        return Err(ApiError::bad_request(
            "role must be one of: viewer, user, leader, bot, admin",
        ));
    };

    let password = req.password.as_deref().map(str::trim);
    match role {
        Role::Bot => {
            if password.is_some() {
                return Err(ApiError::bad_request(
                    "bot users do not accept password; use access token instead",
                ));
            }
        }
        _ => {
            let password = password
                .ok_or_else(|| ApiError::bad_request("password is required for non-bot users"))?;
            if password.len() < 6 {
                return Err(ApiError::bad_request(
                    "password must be at least 6 characters",
                ));
            }
        }
    }

    // Parse leader_expires_at if provided (required for leader role).
    let leader_expires_at = if let Some(ref expires_str) = req.leader_expires_at {
        let dt = chrono::DateTime::parse_from_rfc3339(expires_str).map_err(|_| {
            ApiError::bad_request("leader_expires_at must be a valid RFC 3339 timestamp")
        })?;
        Some(dt.with_timezone(&chrono::Utc))
    } else {
        None
    };

    if role == Role::Leader && leader_expires_at.is_none() {
        return Err(ApiError::bad_request(
            "leader_expires_at is required when creating a leader",
        ));
    }

    let display_name = req.display_name.as_deref().unwrap_or("");
    let (password_hash, access_token, bot_token_hash) = match role {
        Role::Bot => {
            let access_token = generate_bot_access_token();
            let token_hash = hash_bot_access_token(&access_token);
            (None, Some(access_token), Some(token_hash))
        }
        _ => {
            let password = password.expect("validated password should exist");
            let pw_hash =
                hash_password(password).map_err(|_| ApiError::internal("password hash error"))?;
            (Some(pw_hash), None, None)
        }
    };

    let profile = auth_queries::create_user(
        &state.pool,
        username,
        display_name,
        password_hash.as_deref(),
        role_str,
        leader_expires_at,
        bot_token_hash.as_deref(),
    )
    .await
    .map_err(ApiError::from)?;

    Ok(Json(AdminUserResponse::new(profile, access_token)))
}

pub(crate) async fn update_user(
    AxumPath(user_id): AxumPath<String>,
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(req): Json<UpdateUserRequest>,
) -> ApiResult<AdminUserResponse> {
    parse_uuid_param(&user_id, "user_id")?;

    // Prevent admin from deactivating themselves
    if req.is_active == Some(false) && current.user_id == user_id {
        return Err(ApiError::bad_request("cannot deactivate your own account"));
    }

    let existing = auth_queries::find_user_by_id(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    let existing_role = Role::from_str(&existing.role)
        .ok_or_else(|| ApiError::internal("invalid role in database"))?;

    if let Some(role_str) = &req.role {
        if Role::from_str(role_str).is_none() {
            return Err(ApiError::bad_request(
                "role must be one of: viewer, user, leader, bot, admin",
            ));
        }
    }

    let target_role_str = req.role.as_deref().unwrap_or(&existing.role);
    let target_role = Role::from_str(target_role_str)
        .ok_or_else(|| ApiError::internal("invalid role in database"))?;

    // Parse leader_expires_at if provided.
    let mut leader_expires_at: Option<Option<chrono::DateTime<chrono::Utc>>> =
        if let Some(ref outer) = req.leader_expires_at {
            if let Some(ref expires_str) = outer {
                let dt = chrono::DateTime::parse_from_rfc3339(expires_str).map_err(|_| {
                    ApiError::bad_request("leader_expires_at must be a valid RFC 3339 timestamp")
                })?;
                Some(Some(dt.with_timezone(&chrono::Utc)))
            } else {
                Some(None) // explicitly set to null
            }
        } else {
            None // not provided — don't change
        };

    // If setting role to leader, require leader_expires_at.
    if target_role == Role::Leader {
        // Must either provide leader_expires_at in this request, or it must already be set.
        let will_have_expiry = match &leader_expires_at {
            Some(Some(_)) => true,
            _ => false,
        };
        if !will_have_expiry {
            if existing.leader_expires_at.is_none() {
                return Err(ApiError::bad_request(
                    "leader_expires_at is required when setting role to leader",
                ));
            }
        }
    } else if req.role.is_some() && req.leader_expires_at.is_none() {
        leader_expires_at = Some(None);
    }

    let mut issued_access_token = None;
    let mut password_hash_update: Option<Option<&str>> = None;
    let mut bot_token_hash_update: Option<Option<&str>> = None;
    let bot_token_hash = if target_role == Role::Bot && existing_role != Role::Bot {
        let access_token = generate_bot_access_token();
        let token_hash = hash_bot_access_token(&access_token);
        issued_access_token = Some(access_token);
        password_hash_update = Some(None);
        Some(token_hash)
    } else {
        None
    };

    if let Some(token_hash) = bot_token_hash.as_deref() {
        bot_token_hash_update = Some(Some(token_hash));
    } else if target_role != Role::Bot && existing_role == Role::Bot {
        bot_token_hash_update = Some(None);
    }

    let profile = auth_queries::update_user(
        &state.pool,
        &user_id,
        req.display_name.as_deref(),
        req.role.as_deref(),
        req.is_active,
        leader_expires_at,
        password_hash_update,
        bot_token_hash_update,
    )
    .await
    .map_err(ApiError::from)?;

    if target_role == Role::Bot {
        auth_queries::revoke_all_refresh_tokens(&state.pool, &user_id)
            .await
            .map_err(ApiError::from)?;
    }

    Ok(Json(AdminUserResponse::new(profile, issued_access_token)))
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

    // Verify user exists and supports password login.
    let user = auth_queries::find_user_by_id(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    let role =
        Role::from_str(&user.role).ok_or_else(|| ApiError::internal("invalid role in database"))?;
    if role == Role::Bot {
        return Err(ApiError::bad_request(
            "bot users do not support password login",
        ));
    }

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

pub(crate) async fn rotate_bot_access_token(
    AxumPath(user_id): AxumPath<String>,
    State(state): State<AppState>,
) -> ApiResult<AdminUserResponse> {
    parse_uuid_param(&user_id, "user_id")?;

    let user = auth_queries::find_user_by_id(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;
    let role =
        Role::from_str(&user.role).ok_or_else(|| ApiError::internal("invalid role in database"))?;
    if role != Role::Bot {
        return Err(ApiError::bad_request(
            "access token rotation is only supported for bot users",
        ));
    }

    let access_token = generate_bot_access_token();
    let token_hash = hash_bot_access_token(&access_token);
    let profile = auth_queries::update_user(
        &state.pool,
        &user_id,
        None,
        None,
        None,
        None,
        None,
        Some(Some(token_hash.as_str())),
    )
    .await
    .map_err(ApiError::from)?;

    auth_queries::revoke_all_refresh_tokens(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(AdminUserResponse::new(profile, Some(access_token))))
}
