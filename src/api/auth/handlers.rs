//! Auth handlers: login, refresh, logout, me, change password.

use axum::{extract::State, Extension, Json};

use super::models::Role;
use super::{
    models::{
        ChangePasswordRequest, CurrentUser, LoginRequest, MessageResponse, RefreshRequest,
        TokenResponse, UserProfile,
    },
    password::{hash_password, verify_password},
    queries::{
        consume_refresh_token, find_user_by_id, insert_refresh_token, load_user_profile,
        revoke_refresh_token, update_password,
    },
    token::{
        create_access_token, generate_refresh_token, hash_refresh_token, refresh_token_expires_at,
        ACCESS_TOKEN_LIFETIME_SECS,
    },
};
use crate::api::{
    shared::error::{ApiError, ApiResult},
    AppState,
};

pub(crate) async fn login(
    State(state): State<AppState>,
    Json(req): Json<LoginRequest>,
) -> ApiResult<TokenResponse> {
    let username = req.username.trim();
    let password = &req.password;

    if username.is_empty() || password.is_empty() {
        return Err(ApiError::bad_request("username and password are required"));
    }

    let user = super::queries::find_user_by_username(&state.pool, username)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::unauthorized("invalid username or password"))?;

    if !user.is_active {
        return Err(ApiError::unauthorized("account is disabled"));
    }

    let valid = verify_password(password, &user.password_hash)
        .map_err(|_| ApiError::internal("password verification error"))?;
    if !valid {
        return Err(ApiError::unauthorized("invalid username or password"));
    }

    let role =
        Role::from_str(&user.role).ok_or_else(|| ApiError::internal("invalid role in database"))?;

    issue_tokens(&state, &user.user_id, &user.username, role, user.leader_expires_at).await
}

pub(crate) async fn refresh(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> ApiResult<TokenResponse> {
    if req.refresh_token.is_empty() {
        return Err(ApiError::bad_request("refresh_token is required"));
    }

    let token_hash = hash_refresh_token(&req.refresh_token);
    let user_id = consume_refresh_token(&state.pool, &token_hash)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::unauthorized("invalid or expired refresh token"))?;

    let user = find_user_by_id(&state.pool, &user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::unauthorized("user no longer exists"))?;

    if !user.is_active {
        return Err(ApiError::unauthorized("account is disabled"));
    }

    let role =
        Role::from_str(&user.role).ok_or_else(|| ApiError::internal("invalid role in database"))?;

    issue_tokens(&state, &user.user_id, &user.username, role, user.leader_expires_at).await
}

pub(crate) async fn logout(
    State(state): State<AppState>,
    Json(req): Json<RefreshRequest>,
) -> ApiResult<MessageResponse> {
    if !req.refresh_token.is_empty() {
        let token_hash = hash_refresh_token(&req.refresh_token);
        revoke_refresh_token(&state.pool, &token_hash)
            .await
            .map_err(ApiError::from)?;
    }
    Ok(Json(MessageResponse {
        message: "logged out",
    }))
}

pub(crate) async fn me(
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
) -> ApiResult<UserProfile> {
    let profile = load_user_profile(&state.pool, &current.user_id)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(profile))
}

pub(crate) async fn change_password(
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    Json(req): Json<ChangePasswordRequest>,
) -> ApiResult<MessageResponse> {
    if req.new_password.len() < 6 {
        return Err(ApiError::bad_request(
            "new password must be at least 6 characters",
        ));
    }

    let user = find_user_by_id(&state.pool, &current.user_id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::not_found("user not found"))?;

    let valid = verify_password(&req.old_password, &user.password_hash)
        .map_err(|_| ApiError::internal("password verification error"))?;
    if !valid {
        return Err(ApiError::unauthorized("old password is incorrect"));
    }

    let new_hash =
        hash_password(&req.new_password).map_err(|_| ApiError::internal("hash error"))?;
    update_password(&state.pool, &current.user_id, &new_hash)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(MessageResponse {
        message: "password changed",
    }))
}

pub(crate) async fn search_users(
    Extension(current): Extension<CurrentUser>,
    State(state): State<AppState>,
    axum::extract::Query(params): axum::extract::Query<super::models::UserSearchParams>,
) -> ApiResult<crate::api::shared::pagination::Paginated<UserProfile>> {
    if !current.role.is_leader_or_above() {
        return Err(ApiError::forbidden("leader role or above required"));
    }
    let keyword = params.q.as_deref().unwrap_or("").trim();
    if keyword.is_empty() {
        return Err(ApiError::bad_request("q parameter is required and must not be empty"));
    }
    let limit = crate::api::shared::pagination::normalize_limit(params.limit);
    let offset = crate::api::shared::pagination::normalize_offset(params.offset);
    let (users, total) = super::queries::search_users(&state.pool, keyword, limit, offset)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(crate::api::shared::pagination::Paginated {
        items: users,
        total,
        limit,
        offset,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn issue_tokens(
    state: &AppState,
    user_id: &str,
    username: &str,
    role: Role,
    leader_expires_at: Option<chrono::DateTime<chrono::Utc>>,
) -> ApiResult<TokenResponse> {
    let access = create_access_token(user_id, username, role, leader_expires_at, &state.jwt_secret)
        .map_err(|_| ApiError::internal("token creation failed"))?;

    let refresh = generate_refresh_token();
    let refresh_hash = hash_refresh_token(&refresh);
    let expires = refresh_token_expires_at();

    insert_refresh_token(&state.pool, user_id, &refresh_hash, expires)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(TokenResponse {
        access_token: access,
        refresh_token: refresh,
        token_type: "Bearer",
        expires_in: ACCESS_TOKEN_LIFETIME_SECS,
    }))
}
