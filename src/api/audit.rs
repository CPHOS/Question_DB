//! HTTP request audit logging middleware.
//!
//! Captures every API call (method, path, status, duration, caller identity,
//! client IP, user-agent) into the `api_audit_log` table so administrators can
//! review and export activity.  The response is never delayed: the DB insert is
//! spawned and any error is only logged, never surfaced to the caller.
//!
//! The middleware is layered *inside* `require_auth` on the authenticated and
//! admin route groups, so `CurrentUser` is already present in the request
//! extensions there.  On public routes (login, refresh, ...) it runs without a
//! user and records the anonymous caller instead.

use std::time::Instant;

use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::Request,
    middleware::Next,
    response::Response,
};
use sqlx::query;

use crate::api::{auth::models::CurrentUser, AppState};

/// Health/version probes are polled frequently and would flood the audit log.
fn is_noise(path: &str) -> bool {
    matches!(path, "/health" | "/version")
}

pub(crate) async fn audit_log(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query_string = req.uri().query().map(str::to_string);

    // Prefer X-Forwarded-For (set by the reverse proxy in production), fall back
    // to the direct socket peer address.
    let client_ip = req
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.split(',')
                .next()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .or_else(|| {
            req.extensions()
                .get::<ConnectInfo<std::net::SocketAddr>>()
                .map(|ci| ci.0.ip().to_string())
        });

    let user_agent = req
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    let user = req.extensions().get::<CurrentUser>().cloned();

    if is_noise(&path) {
        return next.run(req).await;
    }

    let response = next.run(req).await;
    let status_code = response.status().as_u16() as i32;
    let duration_ms = start.elapsed().as_millis().min(i32::MAX as u128) as i32;

    let user_id = user.as_ref().map(|u| u.user_id.clone());
    let username = user.as_ref().map(|u| u.username.clone());
    let role = user.as_ref().map(|u| u.role.as_str().to_string());

    tracing::info!(
        target: "qb_api::audit",
        method = %method,
        path = %path,
        status = status_code,
        user = ?username,
        ip = ?client_ip,
        dur_ms = duration_ms,
        "api call"
    );

    let pool = state.pool.clone();
    tokio::spawn(async move {
        if let Err(e) = query(
            "INSERT INTO api_audit_log \
             (method, path, query, status_code, duration_ms, user_id, username, role, client_ip, user_agent) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&method)
        .bind(&path)
        .bind(&query_string)
        .bind(status_code)
        .bind(duration_ms)
        .bind(&user_id)
        .bind(&username)
        .bind(&role)
        .bind(&client_ip)
        .bind(&user_agent)
        .execute(&pool)
        .await
        {
            tracing::warn!(error = %e, "failed to write audit log row");
        }
    });

    response
}
