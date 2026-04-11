use axum::{extract::State, Json};
use sqlx::query;

use crate::api::{
    shared::error::{ApiError, ApiResult, HealthResponse, VersionResponse},
    AppState,
};

pub(crate) async fn health(State(state): State<AppState>) -> ApiResult<HealthResponse> {
    if let Err(_err) = query("SELECT 1").execute(&state.pool).await {
        return Err(ApiError::service_unavailable("database is unreachable"));
    }

    Ok(Json(HealthResponse {
        status: "ok",
        service: "qb_api_rust",
    }))
}

pub(crate) async fn version() -> Json<VersionResponse> {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION"),
    })
}
