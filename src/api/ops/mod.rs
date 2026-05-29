pub(crate) mod database;
pub(crate) mod exports;
pub(crate) mod handlers;
pub(crate) mod models;
pub(crate) mod paper_render;
pub(crate) mod quality;

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};

pub(crate) fn router() -> Router<super::AppState> {
    Router::new()
        .route("/exports/run", post(handlers::run_export))
        .route("/quality-checks/run", post(handlers::run_quality_check))
        .route("/database/backup", get(handlers::download_database_backup))
        .route(
            "/database/restore",
            post(handlers::restore_database_backup).layer(DefaultBodyLimit::disable()),
        )
}
