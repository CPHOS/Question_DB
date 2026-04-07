use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use serde::Serialize;
use tokio::fs;
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use zip::{write::SimpleFileOptions, ZipWriter};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct BundleFileEntry {
    pub(crate) zip_path: String,
    pub(crate) original_path: String,
    pub(crate) file_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_question_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) object_id: Option<String>,
    pub(crate) mime_type: Option<String>,
}

pub(crate) fn temp_zip_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "qb_{prefix}_bundle_{}_{}.zip",
        timestamp_unix(),
        Uuid::new_v4()
    ))
}

pub(crate) fn timestamp_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

pub(crate) fn write_bundle_file(
    writer: &mut ZipWriter<File>,
    zip_path: &str,
    bytes: &[u8],
) -> Result<()> {
    writer
        .start_file(zip_path, SimpleFileOptions::default())
        .context("start bundle file entry failed")?;
    writer
        .write_all(bytes)
        .with_context(|| format!("write bundle file failed: {zip_path}"))?;
    Ok(())
}

pub(crate) fn write_manifest<T: Serialize>(
    writer: &mut ZipWriter<File>,
    manifest: &T,
) -> Result<()> {
    writer
        .start_file("manifest.json", SimpleFileOptions::default())
        .context("start manifest.json failed")?;
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize manifest.json failed")?;
    writer
        .write_all(&bytes)
        .context("write manifest.json failed")?;
    Ok(())
}

pub(crate) async fn finish_zip_response(
    writer: ZipWriter<File>,
    zip_path: PathBuf,
    bundle_name: &str,
) -> Result<Response> {
    let file = writer.finish().context("finish zip archive failed")?;
    let size = file
        .metadata()
        .context("read zip metadata failed")?
        .len()
        .to_string();
    drop(file);

    let std_file = File::open(&zip_path)
        .with_context(|| format!("open finished zip failed: {}", zip_path.to_string_lossy()))?;
    std::fs::remove_file(&zip_path).ok();
    let stream = ReaderStream::new(fs::File::from_std(std_file));
    let body = Body::from_stream(stream);

    let content_type = HeaderValue::from_static("application/zip");
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{bundle_name}\""))
        .context("build content-disposition header failed")?;
    let content_length =
        HeaderValue::from_str(&size).context("build content-length header failed")?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::CONTENT_LENGTH, content_length)
        .body(body)
        .context("build zip response failed")
}
