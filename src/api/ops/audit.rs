//! Audit log query/export endpoints.  Mounted under the admin-only `ops`
//! router, so both `GET /audit/logs` and `GET /audit/logs/export` require the
//! `admin` role.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
    Json,
};
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, Postgres, Row};

use crate::api::{
    shared::pagination::{normalize_limit, normalize_offset, Paginated},
    AppState,
};

#[derive(Debug, Deserialize)]
pub(crate) struct AuditLogParams {
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
    pub(crate) method: Option<String>,
    /// Substring match on the request path.
    pub(crate) path: Option<String>,
    pub(crate) user_id: Option<String>,
    /// Substring match on the caller username.
    pub(crate) username: Option<String>,
    pub(crate) status_min: Option<i32>,
    pub(crate) status_max: Option<i32>,
    /// Inclusive bounds; accept `YYYY-MM-DD` or full RFC 3339 timestamps.
    pub(crate) created_after: Option<String>,
    pub(crate) created_before: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AuditLogEntry {
    pub(crate) id: i64,
    pub(crate) created_at: String,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: Option<String>,
    pub(crate) status_code: i32,
    pub(crate) duration_ms: i32,
    pub(crate) user_id: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) client_ip: Option<String>,
    pub(crate) user_agent: Option<String>,
}

const SELECT_COLUMNS: &str = "id, created_at::text AS created_at, method, path, query, \
     status_code, duration_ms, user_id, username, role, client_ip, user_agent, \
     COUNT(*) OVER ()::bigint AS total_count";

fn apply_filters(b: &mut QueryBuilder<'_, Postgres>, p: &AuditLogParams) {
    if let Some(m) = &p.method {
        b.push(" AND method = ").push_bind(m.clone());
    }
    if let Some(path) = &p.path {
        b.push(" AND path ILIKE ").push_bind(format!("%{path}%"));
    }
    if let Some(u) = &p.user_id {
        b.push(" AND user_id = ").push_bind(u.clone());
    }
    if let Some(un) = &p.username {
        b.push(" AND username ILIKE ").push_bind(format!("%{un}%"));
    }
    if let Some(s) = p.status_min {
        b.push(" AND status_code >= ").push_bind(s);
    }
    if let Some(s) = p.status_max {
        b.push(" AND status_code <= ").push_bind(s);
    }
    if let Some(after) = &p.created_after {
        b.push(" AND created_at >= ").push_bind(after.clone());
    }
    if let Some(before) = &p.created_before {
        b.push(" AND created_at <= ").push_bind(before.clone());
    }
}

fn row_to_entry(row: sqlx::postgres::PgRow) -> AuditLogEntry {
    AuditLogEntry {
        id: row.get("id"),
        created_at: row.get("created_at"),
        method: row.get("method"),
        path: row.get("path"),
        query: row.get("query"),
        status_code: row.get("status_code"),
        duration_ms: row.get("duration_ms"),
        user_id: row.get("user_id"),
        username: row.get("username"),
        role: row.get("role"),
        client_ip: row.get("client_ip"),
        user_agent: row.get("user_agent"),
    }
}

pub(crate) async fn list_audit_logs(
    State(state): State<AppState>,
    Query(params): Query<AuditLogParams>,
) -> Result<Json<Paginated<AuditLogEntry>>, crate::api::shared::error::ApiError> {
    let limit = normalize_limit(params.limit);
    let offset = normalize_offset(params.offset);

    let mut b = QueryBuilder::<Postgres>::new("SELECT ");
    b.push(SELECT_COLUMNS);
    b.push(" FROM api_audit_log WHERE 1=1");
    apply_filters(&mut b, &params);
    b.push(" ORDER BY created_at DESC, id DESC LIMIT ").push_bind(limit);
    b.push(" OFFSET ").push_bind(offset);

    let rows = b
        .build()
        .fetch_all(&state.pool)
        .await
        .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))?;

    let total = rows
        .first()
        .map(|r| r.get::<i64, _>("total_count"))
        .unwrap_or(0);
    let items = rows.into_iter().map(row_to_entry).collect();

    Ok(Json(Paginated {
        items,
        total,
        limit,
        offset,
    }))
}

pub(crate) async fn export_audit_logs(
    State(state): State<AppState>,
    Query(params): Query<AuditLogParams>,
) -> Result<Response, crate::api::shared::error::ApiError> {
    // Allow larger pulls than the paginated view, but cap to keep memory bounded.
    let limit = params.limit.unwrap_or(10000).clamp(1, 50000);

    let mut b = QueryBuilder::<Postgres>::new("SELECT ");
    b.push(SELECT_COLUMNS);
    b.push(" FROM api_audit_log WHERE 1=1");
    apply_filters(&mut b, &params);
    b.push(" ORDER BY created_at DESC, id DESC LIMIT ").push_bind(limit);

    let rows = b
        .build()
        .fetch_all(&state.pool)
        .await
        .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))?;

    let mut wtr = csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(Vec::<u8>::new());
    wtr.write_record([
        "id",
        "created_at",
        "method",
        "path",
        "query",
        "status_code",
        "duration_ms",
        "user_id",
        "username",
        "role",
        "client_ip",
        "user_agent",
    ])
    .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))?;

    for row in rows {
        let fields = vec![
            row.get::<i64, _>("id").to_string(),
            row.get::<String, _>("created_at"),
            row.get::<String, _>("method"),
            row.get::<String, _>("path"),
            row.get::<Option<String>, _>("query").unwrap_or_default(),
            row.get::<i32, _>("status_code").to_string(),
            row.get::<i32, _>("duration_ms").to_string(),
            row.get::<Option<String>, _>("user_id").unwrap_or_default(),
            row.get::<Option<String>, _>("username").unwrap_or_default(),
            row.get::<Option<String>, _>("role").unwrap_or_default(),
            row.get::<Option<String>, _>("client_ip").unwrap_or_default(),
            row.get::<Option<String>, _>("user_agent").unwrap_or_default(),
        ];
        wtr.write_record(&fields)
            .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))?;
    }

    let data = wtr
        .into_inner()
        .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))?;

    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let disposition =
        HeaderValue::from_str(&format!("attachment; filename=\"audit-logs-{stamp}.csv\""))
            .unwrap_or_else(|_| HeaderValue::from_static("attachment; filename=\"audit-logs.csv\""));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, HeaderValue::from_static("text/csv; charset=utf-8"))
        .header(header::CONTENT_DISPOSITION, disposition)
        .body(Body::from(data))
        .map_err(|e| crate::api::shared::error::ApiError::internal(e.to_string()))
}
