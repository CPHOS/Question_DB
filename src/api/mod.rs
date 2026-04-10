//! HTTP API composition for the question bank service.

mod admin;
mod auth;
mod ops;
mod papers;
mod questions;
mod shared;
mod system;
mod tests;

use std::path::PathBuf;

use axum::{extract::DefaultBodyLimit, middleware as axum_middleware, Router};
use sqlx::PgPool;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

pub use self::auth::password::hash_password;
pub use self::auth::queries::seed_admin_if_empty;
pub use self::papers::models::{PaperDetail, PaperSummary};
pub use self::questions::models::{
    QuestionAssetRef, QuestionDetail, QuestionPaperRef, QuestionSummary,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub database_url: String,
    pub export_dir: PathBuf,
    pub jwt_secret: String,
}

/// Build the complete Axum router for the service.
pub fn router(state: AppState, cors_origins: &[String]) -> Router {
    let cors = if cors_origins.is_empty() {
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any)
    } else {
        let origins = cors_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect::<Vec<_>>();
        CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
    };

    // Public routes (no auth required)
    let public = Router::new()
        .merge(system::router())
        .merge(auth::public_router());

    // Authenticated routes: any logged-in user can access.
    // Fine-grained permission checks are done inside handlers.
    let authenticated_routes = Router::new()
        .merge(auth::authenticated_router())
        // Read-only (viewer+)
        .route(
            "/questions",
            axum::routing::get(questions::handlers::list_questions)
                .post(questions::handlers::create_question),
        )
        .route(
            "/questions/tags",
            axum::routing::get(questions::handlers::list_question_tags),
        )
        .route(
            "/questions/difficulty-tags",
            axum::routing::get(questions::handlers::list_difficulty_tags),
        )
        .route(
            "/questions/:question_id",
            axum::routing::get(questions::handlers::get_question_detail)
                .patch(questions::handlers::update_question_metadata)
                .delete(questions::handlers::delete_question),
        )
        .route(
            "/questions/:question_id/file",
            axum::routing::put(questions::handlers::replace_question_file),
        )
        .route(
            "/questions/:question_id/reviewers",
            axum::routing::get(questions::handlers::list_question_reviewers)
                .post(questions::handlers::assign_reviewer),
        )
        .route(
            "/questions/:question_id/reviewers/:reviewer_id",
            axum::routing::delete(questions::handlers::remove_reviewer),
        )
        .route(
            "/questions/bundles",
            axum::routing::post(questions::handlers::download_questions_bundle),
        )
        .route(
            "/papers",
            axum::routing::get(papers::handlers::list_papers)
                .post(papers::handlers::create_paper),
        )
        .route(
            "/papers/:paper_id",
            axum::routing::get(papers::handlers::get_paper_detail)
                .patch(papers::handlers::update_paper)
                .delete(papers::handlers::delete_paper),
        )
        .route(
            "/papers/:paper_id/file",
            axum::routing::put(papers::handlers::replace_paper_file),
        )
        .route(
            "/papers/bundles",
            axum::routing::post(papers::handlers::download_papers_bundle),
        )
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            auth::middleware::require_auth,
        ));

    // Admin-level routes, including restricted ops
    let admin_routes = Router::new()
        .merge(ops::router())
        .merge(admin::router())
        .layer(axum_middleware::from_fn(auth::middleware::require_admin))
        .layer(axum_middleware::from_fn_with_state(
            state.clone(),
            auth::middleware::require_auth,
        ));

    Router::new()
        .merge(public)
        .merge(authenticated_routes)
        .merge(admin_routes)
        .layer(DefaultBodyLimit::max(
            questions::MAX_UPLOAD_BYTES
                .max(papers::MAX_UPLOAD_BYTES)
                .max(ops::MAX_RESTORE_UPLOAD_BYTES),
        ))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
