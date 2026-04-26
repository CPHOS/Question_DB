use std::{
    env,
    fs::File,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
};

use anyhow::{bail, Context, Result};
use axum::{
    body::Body,
    http::{header, HeaderValue, StatusCode},
    response::Response,
};
use chrono::Utc;
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use tokio::fs;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

pub(crate) const MAX_RESTORE_UPLOAD_BYTES: usize = 256 * 1024 * 1024;

pub(crate) fn backup_download_name() -> String {
    format!("qb_backup_{}.tar.gz", Utc::now().format("%Y%m%d_%H%M%S"))
}

pub(crate) fn normalize_uploaded_backup_name(file_name: Option<&str>) -> String {
    file_name
        .and_then(|name| Path::new(name).file_name())
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("database_backup.tar.gz")
        .to_string()
}

pub(crate) fn temp_backup_path() -> PathBuf {
    env::temp_dir().join(format!(
        "qb_database_backup_{}_{}.tar.gz",
        Utc::now().format("%Y%m%d_%H%M%S"),
        Uuid::new_v4()
    ))
}

pub(crate) fn temp_restore_upload_path(file_name: Option<&str>) -> PathBuf {
    let extension = file_name
        .and_then(|name| Path::new(name).extension())
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.trim().is_empty())
        .unwrap_or("tar.gz");

    // Handle double extension like .tar.gz
    let ext = if file_name
        .map(|n| n.ends_with(".tar.gz"))
        .unwrap_or(false)
    {
        "tar.gz"
    } else {
        extension
    };

    env::temp_dir().join(format!(
        "qb_database_restore_upload_{}.{}",
        Uuid::new_v4(),
        ext
    ))
}

/// Generate a full backup archive (tar.gz) containing:
/// - `metadata.sql`: pg_dump of the database (with BYTEA content excluded)
/// - `objects/`: copy of the filesystem object store
pub(crate) async fn generate_database_backup(
    database_url: String,
    object_store_dir: PathBuf,
    output_path: PathBuf,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let result =
            build_backup_archive(&database_url, &object_store_dir, &output_path);

        if result.is_err() {
            std::fs::remove_file(&output_path).ok();
        }
        result
    })
    .await
    .context("wait backup task failed")?
}

fn build_backup_archive(
    database_url: &str,
    object_store_dir: &Path,
    output_path: &Path,
) -> Result<()> {
    // Step 1: pg_dump to a temp file
    let sql_path = env::temp_dir().join(format!("qb_pgdump_{}.sql", Uuid::new_v4()));
    let dump_result = run_native_backup(database_url, &sql_path).or_else(|err| {
        if let Some(container_name) = postgres_container_name_if_missing_client(&err) {
            run_container_backup(&container_name, &sql_path)
        } else {
            Err(err)
        }
    });
    if dump_result.is_err() {
        std::fs::remove_file(&sql_path).ok();
        return dump_result;
    }

    // Step 2: Build tar.gz archive
    let tar_file =
        File::create(output_path).context("create backup archive file failed")?;
    let gz = GzEncoder::new(tar_file, Compression::default());
    let mut tar_builder = tar::Builder::new(gz);

    // Add metadata.sql
    tar_builder
        .append_path_with_name(&sql_path, "metadata.sql")
        .context("add metadata.sql to backup archive failed")?;
    std::fs::remove_file(&sql_path).ok();

    // Add objects directory if it exists
    if object_store_dir.is_dir() {
        tar_builder
            .append_dir_all("objects", object_store_dir)
            .context("add objects directory to backup archive failed")?;
    }

    tar_builder
        .finish()
        .context("finalize backup archive failed")?;

    Ok(())
}

/// Restore from a backup archive.
///
/// Accepts both the new tar.gz format and the legacy raw SQL format.
pub(crate) async fn restore_database_backup(
    database_url: String,
    object_store_dir: PathBuf,
    input_path: PathBuf,
) -> Result<()> {
    let input_path_clone = input_path.clone();
    tokio::task::spawn_blocking(move || {
        // Detect format: try to read as gzip/tar first, fall back to raw SQL
        if is_tar_gz(&input_path_clone) {
            restore_from_archive(&database_url, &object_store_dir, &input_path_clone)
        } else {
            // Legacy: raw .sql file
            let sql_path = input_path_clone.to_string_lossy().to_string();
            run_native_restore(&database_url, &sql_path).or_else(|err| {
                if let Some(container_name) = postgres_container_name_if_missing_client(&err) {
                    run_container_restore(&container_name, &sql_path)
                } else {
                    Err(err)
                }
            })
        }
    })
    .await
    .context("wait database restore task failed")?
}

fn is_tar_gz(path: &Path) -> bool {
    // Check magic bytes: gzip starts with 0x1f 0x8b
    let Ok(mut file) = File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 2];
    file.read_exact(&mut magic).is_ok() && magic == [0x1f, 0x8b]
}

fn restore_from_archive(
    database_url: &str,
    object_store_dir: &Path,
    archive_path: &Path,
) -> Result<()> {
    let file = File::open(archive_path).context("open backup archive failed")?;
    let gz = GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);

    let extract_dir = env::temp_dir().join(format!("qb_restore_{}", Uuid::new_v4()));
    std::fs::create_dir_all(&extract_dir).context("create restore temp dir failed")?;

    archive
        .unpack(&extract_dir)
        .context("extract backup archive failed")?;

    // Apply the SQL dump
    let sql_path = extract_dir.join("metadata.sql");
    if !sql_path.exists() {
        // Cleanup temp dir
        std::fs::remove_dir_all(&extract_dir).ok();
        bail!("backup archive does not contain metadata.sql");
    }

    let sql_path_str = sql_path.to_string_lossy().to_string();
    let restore_result = run_native_restore(database_url, &sql_path_str).or_else(|err| {
        if let Some(container_name) = postgres_container_name_if_missing_client(&err) {
            run_container_restore(&container_name, &sql_path_str)
        } else {
            Err(err)
        }
    });

    if let Err(err) = restore_result {
        std::fs::remove_dir_all(&extract_dir).ok();
        return Err(err);
    }

    // Copy objects directory to the object store
    let extracted_objects = extract_dir.join("objects");
    if extracted_objects.is_dir() {
        copy_dir_contents(&extracted_objects, object_store_dir)
            .context("copy restored objects to object store failed")?;
    }

    std::fs::remove_dir_all(&extract_dir).ok();
    Ok(())
}

/// Recursively copy contents of `src` into `dst`.
fn copy_dir_contents(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_contents(&src_path, &dst_path)?;
        } else {
            std::fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

pub(crate) async fn finish_backup_download_response(
    archive_path: PathBuf,
    download_name: &str,
) -> Result<Response> {
    let file = File::open(&archive_path)
        .with_context(|| format!("open backup file failed: {}", archive_path.to_string_lossy()))?;
    let size = file
        .metadata()
        .context("read backup metadata failed")?
        .len()
        .to_string();
    std::fs::remove_file(&archive_path).ok();

    let stream = ReaderStream::new(fs::File::from_std(file));
    let body = Body::from_stream(stream);

    let content_type = HeaderValue::from_static("application/gzip");
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{download_name}\""))
        .context("build content-disposition header failed")?;
    let content_length =
        HeaderValue::from_str(&size).context("build content-length header failed")?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_DISPOSITION, disposition)
        .header(header::CONTENT_LENGTH, content_length)
        .body(body)
        .context("build backup download response failed")
}

fn run_psql_command(database_url: &str, args: &[&str]) -> Result<()> {
    let output = Command::new("psql")
        .arg("--dbname")
        .arg(database_url)
        .args(args)
        .output()
        .context("spawn psql failed")?;
    ensure_command_success("psql", output)
}

fn run_native_backup(database_url: &str, output_path: &Path) -> Result<()> {
    let output = Command::new("pg_dump")
        .arg("--dbname")
        .arg(database_url)
        .arg("--file")
        .arg(output_path)
        .output()
        .context("spawn pg_dump failed")?;
    ensure_command_success("pg_dump", output)
}

fn run_native_restore(database_url: &str, input_path: &str) -> Result<()> {
    run_psql_command(
        database_url,
        &[
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            "DROP SCHEMA public CASCADE; CREATE SCHEMA public;",
        ],
    )?;
    run_psql_command(database_url, &["-v", "ON_ERROR_STOP=1", "-f", input_path])?;
    run_native_migrations(database_url)
}

fn run_container_backup(container_name: &str, output_path: &Path) -> Result<()> {
    let file = File::create(output_path).with_context(|| {
        format!(
            "create backup temp file failed: {}",
            output_path.to_string_lossy()
        )
    })?;
    let output = Command::new("docker")
        .args([
            "exec",
            "-i",
            container_name,
            "sh",
            "-lc",
            r#"pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB""#,
        ])
        .stdout(Stdio::from(file))
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn docker pg_dump fallback failed")?
        .wait_with_output()
        .context("wait docker pg_dump fallback failed")?;
    ensure_command_success("docker exec pg_dump", output)
}

fn run_container_restore(container_name: &str, input_path: &str) -> Result<()> {
    let reset_output = Command::new("docker")
        .args([
            "exec",
            "-i",
            container_name,
            "sh",
            "-lc",
            r#"psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;""#,
        ])
        .output()
        .context("spawn docker psql reset fallback failed")?;
    ensure_command_success("docker exec psql reset", reset_output)?;

    let file = File::open(input_path)
        .with_context(|| format!("open uploaded backup failed: {input_path}"))?;
    let restore_output = Command::new("docker")
        .args([
            "exec",
            "-i",
            container_name,
            "sh",
            "-lc",
            r#"psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB""#,
        ])
        .stdin(Stdio::from(file))
        .stderr(Stdio::piped())
        .spawn()
        .context("spawn docker psql restore fallback failed")?
        .wait_with_output()
        .context("wait docker psql restore fallback failed")?;
    ensure_command_success("docker exec psql restore", restore_output)?;
    run_container_migrations(container_name)
}

fn run_native_migrations(database_url: &str) -> Result<()> {
    for migration in list_migration_files()? {
        let path = migration.to_string_lossy();
        run_psql_command(database_url, &["-v", "ON_ERROR_STOP=1", "-f", &path])
            .with_context(|| format!("apply migration failed: {path}"))?;
    }
    Ok(())
}

fn run_container_migrations(container_name: &str) -> Result<()> {
    for migration in list_migration_files()? {
        let path_display = migration.to_string_lossy().to_string();
        let file = File::open(&migration)
            .with_context(|| format!("open migration file failed: {path_display}"))?;
        let output = Command::new("docker")
            .args([
                "exec",
                "-i",
                container_name,
                "sh",
                "-lc",
                r#"psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" "$POSTGRES_DB""#,
            ])
            .stdin(Stdio::from(file))
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn docker psql migration failed: {path_display}"))?
            .wait_with_output()
            .with_context(|| format!("wait docker psql migration failed: {path_display}"))?;
        ensure_command_success("docker exec psql migration", output)?;
    }
    Ok(())
}

fn list_migration_files() -> Result<Vec<PathBuf>> {
    let migrations_dir = PathBuf::from("/app/migrations");
    if !migrations_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(&migrations_dir)
        .context("read migrations directory failed")?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "sql"))
        .collect();
    files.sort();
    Ok(files)
}

fn ensure_command_success(command_name: &str, output: Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("{command_name} exited with status {}", output.status)
    };

    bail!("{}", build_command_failure_message(command_name, &detail));
}

fn build_command_failure_message(command_name: &str, detail: &str) -> String {
    if detail.contains("server version mismatch") {
        return format!(
            "{command_name} failed: {detail}\n\
             hint: rebuild the API image with a PostgreSQL client matching the server major version"
        );
    }

    format!("{command_name} failed: {detail}")
}

fn postgres_container_name_if_missing_client(err: &anyhow::Error) -> Option<String> {
    if !is_command_not_found(err) {
        return None;
    }
    env::var("QB_POSTGRES_CONTAINER_NAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn is_command_not_found(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_err| io_err.kind() == ErrorKind::NotFound)
    })
}

#[cfg(test)]
mod tests {
    use super::{
        build_command_failure_message, normalize_uploaded_backup_name, temp_restore_upload_path,
    };

    #[test]
    fn normalize_uploaded_backup_name_strips_parent_directories() {
        assert_eq!(
            normalize_uploaded_backup_name(Some("../../nested/qb_backup.tar.gz")),
            "qb_backup.tar.gz"
        );
    }

    #[test]
    fn temp_restore_upload_path_defaults_to_tar_gz_extension() {
        let path = temp_restore_upload_path(None);
        // Default extension
        let path_str = path.to_string_lossy();
        assert!(path_str.ends_with(".tar.gz"));
    }

    #[test]
    fn temp_restore_upload_path_uses_sql_for_legacy() {
        let path = temp_restore_upload_path(Some("backup.sql"));
        assert_eq!(path.extension().and_then(|ext| ext.to_str()), Some("sql"));
    }

    #[test]
    fn command_failure_message_adds_version_mismatch_hint() {
        let message = build_command_failure_message(
            "pg_dump",
            "pg_dump: error: aborting because of server version mismatch",
        );
        assert!(message.contains("server version mismatch"));
        assert!(message.contains("rebuild the API image"));
    }
}
