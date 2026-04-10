pub(crate) mod handlers;
pub(crate) mod middleware;
pub(crate) mod models;
pub(crate) mod password;
pub(crate) mod queries;
pub(crate) mod token;

use axum::{
    routing::{get, patch, post},
    Router,
};

use super::AppState;

/// Public auth routes (no authentication required).
pub(crate) fn public_router() -> Router<AppState> {
    Router::new()
        .route("/auth/login", post(handlers::login))
        .route("/auth/refresh", post(handlers::refresh))
}

/// Authenticated auth routes (require a valid JWT).
/// Note: the auth middleware is applied in api/mod.rs, not here.
pub(crate) fn authenticated_router() -> Router<AppState> {
    Router::new()
        .route("/auth/me", get(handlers::me))
        .route("/auth/me/password", patch(handlers::change_password))
        .route("/auth/logout", post(handlers::logout))
        .route("/users/search", get(handlers::search_users))
}
