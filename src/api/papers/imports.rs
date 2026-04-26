use std::io::Cursor;

use anyhow::{bail, Context, Result};
use sqlx::{query, Row};
use uuid::Uuid;
use zip::ZipArchive;

use super::models::{NormalizedCreatePaperRequest, PaperFileReplaceResponse, PaperImportResponse};
use crate::api::shared::{
    db::{normalize_upload_file_name, ObjectStore},
    error::{NotFoundError, ValidationError},
};

pub(crate) const MAX_UPLOAD_BYTES: usize = 20 * 1024 * 1024;

pub(crate) async fn import_paper_zip(
    object_store: &ObjectStore,
    file_name: Option<&str>,
    request: &NormalizedCreatePaperRequest,
    zip_bytes: Vec<u8>,
    created_by: &str,
) -> Result<PaperImportResponse> {
    let paper_id = Uuid::new_v4().to_string();
    let normalized_file_name = normalize_optional_paper_file_name(file_name, &zip_bytes)?;
    let mut tx = object_store
        .pool()
        .begin()
        .await
        .context("begin paper import tx failed")?;
    let append_object_id = if let Some(file_name) = normalized_file_name.as_deref() {
        Some(
            object_store
                .insert_object_tx(&mut tx, file_name, &zip_bytes, Some("application/zip"))
                .await?,
        )
    } else {
        None
    };

    query(
        r#"
        INSERT INTO papers (
            paper_id, description, title, subtitle,
            append_object_id, created_by, created_at, updated_at
        )
        VALUES ($1::uuid, $2, $3, $4, $5::uuid, $6::uuid, NOW(), NOW())
        "#,
    )
    .bind(&paper_id)
    .bind(&request.description)
    .bind(&request.title)
    .bind(&request.subtitle)
    .bind(append_object_id.as_deref())
    .bind(created_by)
    .execute(&mut *tx)
    .await
    .context("insert paper failed")?;

    for (idx, question_id) in request.question_ids.iter().enumerate() {
        query(
            r#"
            INSERT INTO paper_questions (paper_id, question_id, sort_order, created_at)
            VALUES ($1::uuid, $2::uuid, $3, NOW())
            "#,
        )
        .bind(&paper_id)
        .bind(question_id)
        .bind(i32::try_from(idx + 1).unwrap_or(i32::MAX))
        .execute(&mut *tx)
        .await
        .with_context(|| format!("insert paper question ref failed: {question_id}"))?;
    }

    tx.commit().await.context("commit paper import failed")?;

    Ok(PaperImportResponse {
        paper_id,
        file_name: normalized_file_name,
        question_count: request.question_ids.len(),
        status: "imported",
    })
}

pub(crate) async fn replace_paper_zip(
    object_store: &ObjectStore,
    paper_id: &str,
    file_name: Option<&str>,
    zip_bytes: Vec<u8>,
) -> Result<PaperFileReplaceResponse> {
    if zip_bytes.is_empty() {
        bail!("uploaded file is empty");
    }
    if zip_bytes.len() > MAX_UPLOAD_BYTES {
        bail!("uploaded zip exceeds 20 MiB limit");
    }

    validate_uploaded_zip(&zip_bytes)?;

    let normalized_file_name = normalize_upload_file_name(file_name, "paper.zip");
    let mut tx = object_store
        .pool()
        .begin()
        .await
        .context("begin paper file replace tx failed")?;

    let previous_row = query(
        "SELECT p.append_object_id::text AS append_object_id, o.storage_path FROM papers p LEFT JOIN objects o ON o.object_id = p.append_object_id WHERE p.paper_id = $1::uuid AND p.deleted_at IS NULL FOR UPDATE OF p",
    )
    .bind(paper_id)
    .fetch_optional(&mut *tx)
    .await
    .context("load paper appendix reference failed")?
    .ok_or_else(|| NotFoundError(format!("paper not found: {paper_id}")))?;
    let previous_object_id: Option<String> = previous_row.get("append_object_id");
    let previous_storage_path: Option<String> = previous_row.get("storage_path");

    let append_object_id = object_store
        .insert_object_tx(
            &mut tx,
            &normalized_file_name,
            &zip_bytes,
            Some("application/zip"),
        )
        .await?;

    query("UPDATE papers SET append_object_id = $2::uuid, updated_at = NOW() WHERE paper_id = $1::uuid")
        .bind(paper_id)
        .bind(&append_object_id)
        .execute(&mut *tx)
        .await
        .context("update paper appendix object failed")?;

    if let Some(previous_object_id) = &previous_object_id {
        query("DELETE FROM objects WHERE object_id = $1::uuid")
            .bind(previous_object_id)
            .execute(&mut *tx)
            .await
            .context("delete previous paper appendix object failed")?;
    }

    tx.commit()
        .await
        .context("commit paper file replace failed")?;

    // Clean up old file from disk after successful commit.
    if let Some(path) = previous_storage_path {
        object_store.delete_object_files(&[path]);
    }

    Ok(PaperFileReplaceResponse {
        paper_id: paper_id.to_string(),
        file_name: normalized_file_name,
        status: "replaced",
    })
}

fn validate_uploaded_zip(zip_bytes: &[u8]) -> Result<()> {
    let cursor = Cursor::new(zip_bytes);
    ZipArchive::new(cursor).map_err(|e| ValidationError(format!("invalid zip archive: {e}")))?;
    Ok(())
}

fn normalize_optional_paper_file_name(
    file_name: Option<&str>,
    zip_bytes: &[u8],
) -> Result<Option<String>> {
    if zip_bytes.is_empty() {
        return Ok(None);
    }
    if zip_bytes.len() > MAX_UPLOAD_BYTES {
        return Err(ValidationError("uploaded zip exceeds 20 MiB limit".into()).into());
    }

    validate_uploaded_zip(zip_bytes)?;
    Ok(Some(normalize_upload_file_name(file_name, "paper.zip")))
}

#[cfg(test)]
mod tests {
    use super::{normalize_optional_paper_file_name, validate_uploaded_zip, MAX_UPLOAD_BYTES};
    use std::io::Write;
    use zip::{write::SimpleFileOptions, ZipWriter};

    fn build_zip() -> Vec<u8> {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("meta/info.json", options).unwrap();
        writer.write_all(br#"{"kind":"paper"}"#).unwrap();
        writer.start_file("appendices/raw.bin", options).unwrap();
        writer.write_all(b"payload").unwrap();

        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn validate_uploaded_zip_accepts_any_non_empty_layout() {
        validate_uploaded_zip(&build_zip()).expect("zip should parse");
    }

    #[test]
    fn validate_uploaded_zip_rejects_invalid_zip_bytes() {
        let err = validate_uploaded_zip(b"not-a-zip").expect_err("should reject");
        assert!(err.to_string().contains("invalid zip archive"));
    }

    #[test]
    fn normalize_optional_paper_file_name_accepts_missing_upload() {
        assert_eq!(
            normalize_optional_paper_file_name(None, &[]).expect("empty upload should be allowed"),
            None
        );
    }

    #[test]
    fn normalize_optional_paper_file_name_returns_normalized_name_for_zip() {
        assert_eq!(
            normalize_optional_paper_file_name(Some("nested/paper_appendix.zip"), &build_zip())
                .expect("zip should validate"),
            Some("paper_appendix.zip".into())
        );
    }

    #[test]
    fn upload_limit_constant_matches_requirement() {
        assert_eq!(MAX_UPLOAD_BYTES, 20 * 1024 * 1024);
    }
}
