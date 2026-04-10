//! Authentication and authorization middleware.

use axum::{
    body::Body, extract::State, http::Request, middleware::Next, response::Response, Extension,
};
use chrono::Utc;

use super::{
    models::{CurrentUser, Role},
    token::decode_access_token,
};
use crate::api::{shared::error::ApiError, AppState};

/// Middleware: extract and validate JWT, inject `CurrentUser` into extensions.
/// If the token role is `leader` but `leader_exp` has passed, downgrade to `user`.
pub(crate) async fn require_auth(
    State(state): State<AppState>,
    mut req: Request<Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let token = extract_bearer_token(&req)?;
    let claims = decode_access_token(token, &state.jwt_secret)
        .map_err(|_| ApiError::unauthorized("invalid or expired token"))?;

    let mut role = Role::from_str(&claims.role)
        .ok_or_else(|| ApiError::unauthorized("invalid role in token"))?;

    // Leader expiry check: downgrade expired leader to user.
    if role == Role::Leader {
        if let Some(leader_exp) = claims.leader_exp {
            if Utc::now().timestamp() > leader_exp {
                role = Role::User;
            }
        }
    }

    let current = CurrentUser {
        user_id: claims.sub,
        username: claims.username,
        role,
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
