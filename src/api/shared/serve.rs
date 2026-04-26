//! Individual object file serving endpoint.

use axum::{
    body::Body,
    extract::{Path as AxumPath, State},
    http::{
        header::{self, HeaderMap},
        HeaderValue, StatusCode,
    },
    response::Response,
};

use super::{
    error::ApiError,
    multipart::parse_uuid_param,
};
use crate::api::AppState;

/// `GET /objects/:object_id` — serve a single stored object with HTTP caching.
pub(crate) async fn get_object(
    AxumPath(object_id): AxumPath<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    parse_uuid_param(&object_id, "object_id")?;

    let meta = state
        .object_store
        .fetch_object_meta(&object_id)
        .await
        .map_err(|e| {
            tracing::error!("fetch object meta failed: {e:#}");
            ApiError::internal("internal server error")
        })?
        .ok_or_else(|| ApiError::not_found(format!("object not found: {object_id}")))?;

    // ETag-based conditional request support.
    if let Some(hash) = &meta.content_hash {
        let etag = format!("\"{hash}\"");
        if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
            if let Ok(value) = if_none_match.to_str() {
                if value == etag || value == "*" {
                    return Response::builder()
                        .status(StatusCode::NOT_MODIFIED)
                        .body(Body::empty())
                        .map_err(|e| ApiError::internal(e.to_string()));
                }
            }
        }
    }

    // Read the file content.
    let bytes = state
        .object_store
        .fetch_object_bytes(&object_id)
        .await
        .map_err(|e| {
            tracing::error!("fetch object bytes failed: {e:#}");
            ApiError::internal("internal server error")
        })?;

    let content_type = meta
        .mime_type
        .as_deref()
        .unwrap_or("application/octet-stream");

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_str(content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        )
        .header(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&bytes.len().to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("0")),
        )
        .header(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("inline; filename=\"{}\"", meta.file_name))
                .unwrap_or_else(|_| HeaderValue::from_static("inline")),
        );

    // Objects are immutable (UUID-addressed, write-once) — cache aggressively.
    builder = builder.header(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );

    if let Some(hash) = &meta.content_hash {
        if let Ok(val) = HeaderValue::from_str(&format!("\"{hash}\"")) {
            builder = builder.header(header::ETAG, val);
        }
    }

    builder
        .body(Body::from(bytes))
        .map_err(|e| ApiError::internal(e.to_string()))
}
