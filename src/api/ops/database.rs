use std::{
    env,
    fs::File,
    io::ErrorKind,
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
use tokio::fs;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

pub(crate) const MAX_RESTORE_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

pub(crate) fn backup_download_name() -> String {
    format!("qb_backup_{}.sql", Utc::now().format("%Y%m%d_%H%M%S"))
}

pub(crate) fn normalize_uploaded_backup_name(file_name: Option<&str>) -> String {
    file_name
        .and_then(|name| Path::new(name).file_name())
        .and_then(|name| name.to_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("database_backup.sql")
        .to_string()
}

pub(crate) fn temp_backup_path() -> PathBuf {
    env::temp_dir().join(format!(
        "qb_database_backup_{}_{}.sql",
        Utc::now().format("%Y%m%d_%H%M%S"),
        Uuid::new_v4()
    ))
}

pub(crate) fn temp_restore_upload_path(file_name: Option<&str>) -> PathBuf {
    let extension = file_name
        .and_then(|name| Path::new(name).extension())
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.trim().is_empty())
        .unwrap_or("sql");

    env::temp_dir().join(format!(
        "qb_database_restore_upload_{}.{}",
        Uuid::new_v4(),
        extension
    ))
}

pub(crate) async fn generate_database_backup(
    database_url: String,
    output_path: PathBuf,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let result = run_native_backup(&database_url, &output_path).or_else(|err| {
            if let Some(container_name) = postgres_container_name_if_missing_client(&err) {
                run_container_backup(&container_name, &output_path)
            } else {
                Err(err)
            }
        });

        if result.is_err() {
            std::fs::remove_file(&output_path).ok();
        }
        result
    })
    .await
    .context("wait pg_dump task failed")?
}

pub(crate) async fn restore_database_backup(
    database_url: String,
    input_path: PathBuf,
) -> Result<()> {
    let input_path = input_path.to_string_lossy().to_string();
    tokio::task::spawn_blocking(move || {
        run_native_restore(&database_url, &input_path).or_else(|err| {
            if let Some(container_name) = postgres_container_name_if_missing_client(&err) {
                run_container_restore(&container_name, &input_path)
            } else {
                Err(err)
            }
        })?;
        Ok(())
    })
    .await
    .context("wait database restore task failed")?
}

pub(crate) async fn finish_sql_download_response(
    sql_path: PathBuf,
    download_name: &str,
) -> Result<Response> {
    let file = File::open(&sql_path)
        .with_context(|| format!("open backup file failed: {}", sql_path.to_string_lossy()))?;
    let size = file
        .metadata()
        .context("read backup metadata failed")?
        .len()
        .to_string();
    std::fs::remove_file(&sql_path).ok();

    let stream = ReaderStream::new(fs::File::from_std(file));
    let body = Body::from_stream(stream);

    let content_type = HeaderValue::from_static("application/sql");
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
            normalize_uploaded_backup_name(Some("../../nested/qb_backup.sql")),
            "qb_backup.sql"
        );
    }

    #[test]
    fn temp_restore_upload_path_defaults_to_sql_extension() {
        let path = temp_restore_upload_path(None);
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
