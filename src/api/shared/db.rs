//! Shared database helpers and filesystem-backed object storage.
//!
//! The [`ObjectStore`] struct encapsulates both the PostgreSQL pool and the
//! on-disk object directory.  New objects are written to disk and only metadata
//! is persisted in the database.  Legacy rows that still carry a BYTEA
//! `content` column are transparently served and can be migrated to the
//! filesystem with [`ObjectStore::migrate_legacy_objects`].

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use sqlx::{query, PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// ObjectStore
// ---------------------------------------------------------------------------

/// Filesystem-backed object store with PostgreSQL metadata.
#[derive(Clone)]
pub struct ObjectStore {
    pool: PgPool,
    store_dir: PathBuf,
}

impl ObjectStore {
    /// Create a new object store, ensuring the base directory exists.
    pub fn new(pool: PgPool, store_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&store_dir).with_context(|| {
            format!(
                "create object store directory failed: {}",
                store_dir.to_string_lossy()
            )
        })?;
        Ok(Self { pool, store_dir })
    }

    /// Return the inner pool reference (for callers that only need SQL access).
    pub(crate) fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Return the base directory of the object store.
    pub fn store_dir(&self) -> &Path {
        &self.store_dir
    }

    // -----------------------------------------------------------------------
    // Write
    // -----------------------------------------------------------------------

    /// Insert a binary object: write bytes to the filesystem and persist
    /// metadata in the `objects` table.  Returns the new `object_id`.
    pub(crate) async fn insert_object_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        file_name: &str,
        bytes: &[u8],
        mime_type: Option<&str>,
    ) -> Result<String> {
        let object_id = Uuid::new_v4().to_string();
        let normalized_file_name = Path::new(file_name)
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .unwrap_or_else(|| "blob.bin".to_string());

        // Compute SHA-256 content hash.
        let content_hash = hex_sha256(bytes);

        // Write to filesystem *before* the DB insert so that a crash between
        // the two leaves an orphan file (harmless) rather than a DB row
        // pointing at nothing.
        let storage_path = object_storage_path(&object_id);
        let full_path = self.store_dir.join(&storage_path);
        write_object_file(&full_path, bytes)?;

        query(
            r#"
            INSERT INTO objects (object_id, file_name, mime_type, size_bytes, content, content_hash, storage_path, created_at)
            VALUES ($1::uuid, $2, $3, $4, NULL, $5, $6, NOW())
            "#,
        )
        .bind(&object_id)
        .bind(&normalized_file_name)
        .bind(mime_type)
        .bind(i64::try_from(bytes.len()).context("object bytes exceed i64 range")?)
        .bind(&content_hash)
        .bind(&storage_path)
        .execute(&mut **tx)
        .await
        .context("insert object failed")?;

        Ok(object_id)
    }

    // -----------------------------------------------------------------------
    // Read
    // -----------------------------------------------------------------------

    /// Fetch the raw binary content of an object.
    ///
    /// Reads from the filesystem if `storage_path` is set, otherwise falls
    /// back to the legacy `content` BYTEA column.
    pub(crate) async fn fetch_object_bytes(&self, object_id: &str) -> Result<Vec<u8>> {
        let row = query(
            "SELECT content, storage_path FROM objects WHERE object_id = $1::uuid",
        )
        .bind(object_id)
        .fetch_one(&self.pool)
        .await
        .with_context(|| format!("fetch object failed: {object_id}"))?;

        let storage_path: Option<String> = row.get("storage_path");
        if let Some(rel_path) = storage_path {
            let full_path = self.store_dir.join(&rel_path);
            tokio::fs::read(&full_path)
                .await
                .with_context(|| {
                    format!(
                        "read object file failed: {} (object_id={object_id})",
                        full_path.to_string_lossy()
                    )
                })
        } else {
            // Legacy fallback: read from BYTEA column.
            let content: Option<Vec<u8>> = row.get("content");
            content.ok_or_else(|| {
                anyhow::anyhow!(
                    "object has neither storage_path nor content: {object_id}"
                )
            })
        }
    }

    /// Fetch the content of a text object (UTF-8 with lossy fallback).
    pub(crate) async fn fetch_text_object(&self, object_id: &str) -> Result<String> {
        let bytes = self.fetch_object_bytes(object_id).await?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Fetch object metadata needed for serving a single file.
    pub(crate) async fn fetch_object_meta(
        &self,
        object_id: &str,
    ) -> Result<Option<ObjectMeta>> {
        let row = query(
            r#"
            SELECT file_name, mime_type, size_bytes, content_hash, storage_path
            FROM objects
            WHERE object_id = $1::uuid
            "#,
        )
        .bind(object_id)
        .fetch_optional(&self.pool)
        .await
        .with_context(|| format!("fetch object meta failed: {object_id}"))?;

        Ok(row.map(|r| ObjectMeta {
            file_name: r.get("file_name"),
            mime_type: r.get("mime_type"),
            size_bytes: r.get("size_bytes"),
            content_hash: r.get("content_hash"),
            storage_path: r.get("storage_path"),
        }))
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Best-effort delete of object files from the filesystem.
    ///
    /// This should be called *after* the corresponding `DELETE FROM objects`
    /// query has committed, so that a crash between the two leaves orphan
    /// files rather than dangling DB rows.
    pub(crate) fn delete_object_files(&self, storage_paths: &[String]) {
        for rel_path in storage_paths {
            let full_path = self.store_dir.join(rel_path);
            if let Err(err) = fs::remove_file(&full_path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        path = %full_path.to_string_lossy(),
                        "delete object file failed: {err}"
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Legacy migration
    // -----------------------------------------------------------------------

    /// Migrate objects that still have BYTEA content to the filesystem.
    ///
    /// This is safe to call on every startup — it is a no-op once all objects
    /// have been migrated.  Objects are processed in batches to limit memory.
    pub async fn migrate_legacy_objects(&self) -> Result<usize> {
        const BATCH_SIZE: i64 = 50;
        let mut total_migrated: usize = 0;

        loop {
            let rows = query(
                r#"
                SELECT object_id::text AS object_id, content
                FROM objects
                WHERE content IS NOT NULL AND storage_path IS NULL
                ORDER BY created_at
                LIMIT $1
                "#,
            )
            .bind(BATCH_SIZE)
            .fetch_all(&self.pool)
            .await
            .context("query legacy objects for migration failed")?;

            if rows.is_empty() {
                break;
            }

            for row in &rows {
                let object_id: String = row.get("object_id");
                let content: Vec<u8> = row.get("content");
                let content_hash = hex_sha256(&content);
                let storage_path = object_storage_path(&object_id);
                let full_path = self.store_dir.join(&storage_path);

                write_object_file(&full_path, &content)?;

                query(
                    r#"
                    UPDATE objects
                    SET storage_path = $2, content_hash = $3, content = NULL
                    WHERE object_id = $1::uuid
                    "#,
                )
                .bind(&object_id)
                .bind(&storage_path)
                .bind(&content_hash)
                .execute(&self.pool)
                .await
                .with_context(|| {
                    format!("update migrated object failed: {object_id}")
                })?;
            }

            total_migrated += rows.len();
        }

        if total_migrated > 0 {
            tracing::info!(
                count = total_migrated,
                "migrated legacy BYTEA objects to filesystem"
            );
        }

        Ok(total_migrated)
    }
}

// ---------------------------------------------------------------------------
// ObjectMeta (for the file serving endpoint)
// ---------------------------------------------------------------------------

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ObjectMeta {
    pub(crate) file_name: String,
    pub(crate) mime_type: Option<String>,
    pub(crate) size_bytes: i64,
    pub(crate) content_hash: Option<String>,
    pub(crate) storage_path: Option<String>,
}

// ---------------------------------------------------------------------------
// Free helpers (kept public for callers that don't need the full ObjectStore)
// ---------------------------------------------------------------------------

/// Normalize an upload file name, using the given default when the name is
/// missing or blank.
pub(crate) fn normalize_upload_file_name(file_name: Option<&str>, default: &str) -> String {
    file_name
        .and_then(|value| Path::new(value).file_name())
        .map(|value| value.to_string_lossy().to_string())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Compute the relative storage path for an object: `<first 2 chars>/<object_id>`.
fn object_storage_path(object_id: &str) -> String {
    let prefix = &object_id[..2.min(object_id.len())];
    format!("{prefix}/{object_id}")
}

/// Compute hex-encoded SHA-256 of the given bytes.
fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    // Format as lowercase hex without pulling in another crate.
    digest
        .iter()
        .fold(String::with_capacity(64), |mut acc, byte| {
            use std::fmt::Write;
            let _ = write!(acc, "{byte:02x}");
            acc
        })
}

/// Write bytes to a file, creating parent directories as needed.
fn write_object_file(full_path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = full_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create object directory failed: {}",
                parent.to_string_lossy()
            )
        })?;
    }
    fs::write(full_path, bytes).with_context(|| {
        format!(
            "write object file failed: {}",
            full_path.to_string_lossy()
        )
    })?;
    Ok(())
}
