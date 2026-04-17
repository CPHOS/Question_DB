//! Authentication and authorization middleware.

use axum::{
    body::Body, extract::State, http::Request, middleware::Next, response::Response, Extension,
};
use chrono::Utc;

use super::{
    models::{CurrentUser, Role},
    queries::{find_bot_user_by_access_token_hash, find_user_by_id},
    token::{decode_access_token, hash_bot_access_token},
};
use crate::api::{shared::error::ApiError, AppState};

/// Middleware: accept either a JWT access token (regular users) or an
/// admin-issued opaque bot access token, then inject `CurrentUser`.
pub(crate) async fn require_auth(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let token = extract_bearer_token(&req)?;
    let current = if let Ok(claims) = decode_access_token(token, &state.jwt_secret) {
        let user = find_user_by_id(&state.pool, &claims.sub)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::unauthorized("invalid or expired token"))?;

        if !user.is_active {
            return Err(ApiError::unauthorized("account is disabled"));
        }

        let mut role = Role::from_str(&user.role)
            .ok_or_else(|| ApiError::unauthorized("invalid role in token"))?;
        if role == Role::Bot {
            return Err(ApiError::unauthorized(
                "bot users must use admin-issued access token",
            ));
        }

        if role == Role::Leader {
            if let Some(leader_exp) = user.leader_expires_at {
                if Utc::now() > leader_exp {
                    role = Role::User;
                }
            }
        }

        CurrentUser {
            user_id: user.user_id,
            username: user.username,
            display_name: user.display_name,
            role,
        }
    } else {
        let token_hash = hash_bot_access_token(token);
        let user = find_bot_user_by_access_token_hash(&state.pool, &token_hash)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::unauthorized("invalid or expired token"))?;

        CurrentUser {
            user_id: user.user_id,
            username: user.username,
            display_name: user.display_name,
            role: Role::Bot,
        }
    };

    req.extensions_mut().insert(current);
    Ok(next.run(req).await)
}

/// Middleware: require the caller to have `admin` role.
pub(crate) async fn require_admin(
    Extension(current): Extension<CurrentUser>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    if !current.role.is_admin() {
        return Err(ApiError::forbidden("admin role required"));
    }
    Ok(next.run(req).await)
}

fn extract_bearer_token<'a>(req: &'a Request<Body>) -> Result<&'a str, ApiError> {
    let header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::unauthorized("missing Authorization header"))?;

    header
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::unauthorized("Authorization header must start with 'Bearer '"))
}
