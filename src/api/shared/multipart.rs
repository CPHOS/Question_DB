//! Shared multipart form helpers used by question and paper imports.

use axum::extract::multipart::Field;
use axum::extract::Multipart;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::error::ApiError;

/// Parse a path parameter as a UUID, returning a 400 error on failure.
pub(crate) fn parse_uuid_param(id: &str, field_name: &str) -> Result<(), ApiError> {
    Uuid::parse_str(id)
        .map_err(|_| ApiError::bad_request(format!("invalid {field_name}: {id}")))?;
    Ok(())
}

/// Validate that an uploaded file is non-empty and within the size limit.
pub(crate) fn validate_upload_size(bytes: &[u8], max_bytes: usize) -> Result<(), ApiError> {
    validate_upload_size_with_label(bytes, max_bytes, "uploaded zip")
}

/// Validate that an uploaded file is non-empty and within the size limit.
pub(crate) fn validate_upload_size_with_label(
    bytes: &[u8],
    max_bytes: usize,
    label: &str,
) -> Result<(), ApiError> {
    if bytes.is_empty() {
        return Err(ApiError::bad_request(
            "multipart form must include a non-empty 'file' field",
        ));
    }
    if bytes.len() > max_bytes {
        return Err(ApiError::bad_request(format!(
            "{label} exceeds {} MiB limit",
            max_bytes / (1024 * 1024)
        )));
    }
    Ok(())
}

/// Read the single `file` field from a multipart form.
pub(crate) async fn read_uploaded_file(
    multipart: &mut Multipart,
) -> Result<(Option<String>, Vec<u8>), ApiError> {
    let mut file_name = None;
    let mut bytes = Vec::new();

    while let Some(field) = next_multipart_field(multipart).await? {
        if field.name() != Some("file") {
            continue;
        }
        let (fname, data) = read_file_field(field).await?;
        file_name = fname;
        bytes = data;
    }

    Ok((file_name, bytes))
}

/// Advance to the next multipart field with unified error handling.
pub(crate) async fn next_multipart_field(
    multipart: &mut Multipart,
) -> Result<Option<axum::extract::multipart::Field<'_>>, ApiError> {
    multipart
        .next_field()
        .await
        .map_err(|err| ApiError::bad_request(format!("read multipart field failed: {err}")))
}

pub(crate) async fn read_text_field(
    field: Field<'_>,
    field_name: &str,
) -> Result<String, ApiError> {
    field
        .text()
        .await
        .map_err(|err| ApiError::bad_request(format!("read {field_name} field failed: {err}")))
}

pub(crate) async fn read_json_field<T: DeserializeOwned>(
    field: Field<'_>,
    field_name: &str,
) -> Result<T, ApiError> {
    let text = read_text_field(field, field_name).await?;
    serde_json::from_str(&text)
        .map_err(|err| ApiError::bad_request(format!("invalid {field_name} field: {err}")))
}

pub(crate) async fn read_file_field(
    field: Field<'_>,
) -> Result<(Option<String>, Vec<u8>), ApiError> {
    let file_name = field.file_name().map(str::to_string);
    let bytes = field
        .bytes()
        .await
        .map_err(|err| ApiError::bad_request(format!("read uploaded file failed: {err}")))?
        .to_vec();
    Ok((file_name, bytes))
}
