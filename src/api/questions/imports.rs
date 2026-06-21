//! ZIP-based question import helpers.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    path::{Component, Path},
};

use anyhow::{bail, Context, Result};
use mime_guess::MimeGuess;
use regex::Regex;
use sqlx::{query, Postgres, QueryBuilder, Row, Transaction};
use uuid::Uuid;
use zip::ZipArchive;

use super::models::{
    NormalizedCreateQuestionRequest, QuestionFileReplaceResponse, QuestionImportResponse,
};
use crate::api::shared::{
    db::{normalize_upload_file_name, ObjectStore},
    error::{NotFoundError, ValidationError},
};

pub(crate) const MAX_UPLOAD_BYTES: usize = 20 * 1024 * 1024;
const MAX_TOTAL_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
struct ArchiveFile {
    path: String,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct LoadedQuestionZip {
    tex_file: ArchiveFile,
    asset_files: Vec<ArchiveFile>,
    score: Option<i32>,
}

pub(crate) async fn import_question_zip(
    object_store: &ObjectStore,
    file_name: Option<&str>,
    request: &NormalizedCreateQuestionRequest,
    zip_bytes: Vec<u8>,
    created_by: &str,
    author_display_name: &str,
) -> Result<QuestionImportResponse> {
    if zip_bytes.is_empty() {
        return Err(ValidationError("uploaded file is empty".into()).into());
    }
    if zip_bytes.len() > MAX_UPLOAD_BYTES {
        return Err(ValidationError("uploaded zip exceeds 20 MiB limit".into()).into());
    }

    let loaded = load_question_zip(&zip_bytes).map_err(|e| ValidationError(format!("{e:#}")))?;
    let question_id = Uuid::new_v4().to_string();
    let mut tx = object_store
        .pool()
        .begin()
        .await
        .context("begin question import tx failed")?;

    // Backend sets: difficulty=empty, status="none", author=display_name, reviewers=[]
    let empty_reviewers: Vec<String> = vec![];
    query(
        r#"
        INSERT INTO questions (
            question_id, source_tex_path, category, status, description, score, author, reviewers, created_by, created_at, updated_at
        )
        VALUES (
            $1::uuid, $2, $3, 'none', $4, $5, $6, $7, $8::uuid, NOW(), NOW()
        )
        "#,
    )
    .bind(&question_id)
    .bind(&loaded.tex_file.path)
    .bind(&request.category)
    .bind(&request.description)
    .bind(loaded.score)
    .bind(author_display_name)
    .bind(&empty_reviewers)
    .bind(created_by)
    .execute(&mut *tx)
    .await
    .context("insert uploaded question failed")?;

    insert_loaded_question_files_tx(object_store, &mut tx, &question_id, &loaded).await?;
    insert_question_tags_tx(&mut tx, &question_id, &request.tags).await?;
    // No difficulty entries inserted — difficulty starts empty.

    tx.commit().await.context("commit question import failed")?;

    Ok(QuestionImportResponse {
        question_id,
        file_name: normalize_upload_file_name(file_name, "question.zip"),
        imported_assets: loaded.asset_files.len(),
        status: "imported",
    })
}

pub(crate) async fn replace_question_zip(
    object_store: &ObjectStore,
    question_id: &str,
    file_name: Option<&str>,
    zip_bytes: Vec<u8>,
) -> Result<QuestionFileReplaceResponse> {
    if zip_bytes.is_empty() {
        return Err(ValidationError("uploaded file is empty".into()).into());
    }
    if zip_bytes.len() > MAX_UPLOAD_BYTES {
        return Err(ValidationError("uploaded zip exceeds 20 MiB limit".into()).into());
    }

    let loaded = load_question_zip(&zip_bytes).map_err(|e| ValidationError(format!("{e:#}")))?;
    let normalized_file_name = normalize_upload_file_name(file_name, "question.zip");
    let mut tx = object_store
        .pool()
        .begin()
        .await
        .context("begin question file replace tx failed")?;

    let exists = query(
        "SELECT 1 FROM questions WHERE question_id = $1::uuid AND deleted_at IS NULL FOR UPDATE",
    )
    .bind(question_id)
    .fetch_optional(&mut *tx)
    .await
    .context("lock question row for file replace failed")?
    .is_some();
    if !exists {
        return Err(NotFoundError(format!("question not found: {question_id}")).into());
    }

    replace_question_files_tx(object_store, &mut tx, question_id, &loaded).await?;

    query(
        "UPDATE questions SET source_tex_path = $2, score = $3, updated_at = NOW() WHERE question_id = $1::uuid",
    )
    .bind(question_id)
    .bind(&loaded.tex_file.path)
    .bind(loaded.score)
    .execute(&mut *tx)
    .await
    .context("update question on file replace failed")?;

    tx.commit()
        .await
        .context("commit question file replace failed")?;

    Ok(QuestionFileReplaceResponse {
        question_id: question_id.to_string(),
        file_name: normalized_file_name,
        source_tex_path: loaded.tex_file.path,
        imported_assets: loaded.asset_files.len(),
        status: "replaced",
    })
}

fn load_question_zip(zip_bytes: &[u8]) -> Result<LoadedQuestionZip> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = ZipArchive::new(cursor)
        .map_err(|e| ValidationError(format!("invalid zip archive: {e}")))?;
    let mut files = BTreeMap::new();
    let mut directories = BTreeSet::new();
    let mut total_uncompressed = 0usize;

    for idx in 0..archive.len() {
        let mut entry = archive
            .by_index(idx)
            .with_context(|| format!("read zip entry #{idx} failed"))?;
        let raw_name = entry.name().to_string();
        let path = sanitize_archive_path(&raw_name)?;
        if raw_name.ends_with('/') {
            directories.insert(path);
            continue;
        }
        let size_hint = usize::try_from(entry.size()).unwrap_or(usize::MAX);
        total_uncompressed = total_uncompressed.saturating_add(size_hint);
        if total_uncompressed > MAX_TOTAL_UNCOMPRESSED_BYTES {
            bail!("zip expands beyond the allowed uncompressed size");
        }

        let mut bytes = Vec::with_capacity(size_hint.min(1024 * 1024));
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("read zip entry bytes failed: {path}"))?;
        register_parent_directories(&mut directories, &path);
        files.insert(path.clone(), ArchiveFile { path, bytes });
    }

    let (files, directories) = normalize_archive_root(files, directories);
    let (tex_file, asset_files) = validate_standard_layout(&files, &directories)?;
    let score = extract_problem_score(&tex_file.bytes);

    Ok(LoadedQuestionZip {
        tex_file,
        asset_files,
        score,
    })
}

fn sanitize_archive_path(path: &str) -> Result<String> {
    let normalized = path.replace('\\', "/");
    let candidate = Path::new(&normalized);
    if candidate.is_absolute() {
        bail!("zip entry must be relative: {path}");
    }

    let mut cleaned = Vec::new();
    for component in candidate.components() {
        match component {
            Component::Normal(part) => cleaned.push(part.to_string_lossy().to_string()),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("zip entry has unsafe path: {path}");
            }
        }
    }

    let joined = cleaned.join("/");
    if joined.is_empty() {
        bail!("zip entry has empty path");
    }
    Ok(joined)
}

fn normalize_archive_root(
    files: BTreeMap<String, ArchiveFile>,
    directories: BTreeSet<String>,
) -> (BTreeMap<String, ArchiveFile>, BTreeSet<String>) {
    let has_root_file = files.keys().any(|path| !path.contains('/'));
    let root_directories = directories
        .iter()
        .filter(|dir| !dir.contains('/'))
        .cloned()
        .collect::<Vec<_>>();

    if has_root_file || root_directories.len() != 1 {
        return (files, directories);
    }

    let root_directory = &root_directories[0];
    let prefix = format!("{root_directory}/");

    let normalized_files = files
        .into_values()
        .filter_map(|mut file| {
            let stripped = file.path.strip_prefix(&prefix)?.to_string();
            file.path = stripped.clone();
            Some((stripped, file))
        })
        .collect::<BTreeMap<_, _>>();

    let normalized_directories = directories
        .into_iter()
        .filter_map(|directory| {
            if directory == *root_directory {
                None
            } else {
                directory.strip_prefix(&prefix).map(str::to_string)
            }
        })
        .collect::<BTreeSet<_>>();

    (normalized_files, normalized_directories)
}

fn validate_standard_layout(
    files: &BTreeMap<String, ArchiveFile>,
    directories: &BTreeSet<String>,
) -> Result<(ArchiveFile, Vec<ArchiveFile>)> {
    let mut root_tex_files = Vec::new();
    let mut asset_files = Vec::new();
    let root_directories = directories
        .iter()
        .filter(|dir| !dir.contains('/'))
        .cloned()
        .collect::<BTreeSet<_>>();

    for file in files.values() {
        let components = file.path.split('/').collect::<Vec<_>>();
        match components.as_slice() {
            [file_name] => {
                if is_tex_file(file_name) {
                    root_tex_files.push(file.clone());
                }
            }
            [root_dir, ..] => {
                if *root_dir != "assets" {
                    bail!(
                        "all non-root files must be inside the root assets/ directory, found: {}",
                        file.path
                    );
                }
                asset_files.push(file.clone());
            }
            [] => bail!("zip entry has empty path"),
        }
    }

    if root_tex_files.len() != 1 {
        bail!(
            "zip root must contain exactly one .tex file, found {}",
            root_tex_files.len()
        );
    }

    if root_directories.iter().any(|dir| dir != "assets") {
        bail!("zip root may only contain one optional assets/ directory");
    }

    Ok((root_tex_files.remove(0), asset_files))
}

fn is_tex_file(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("tex"))
        .unwrap_or(false)
}

/// Extract the score from a `\begin{problem}[<score>]` marker in a TeX file.
/// Returns `None` if no such marker is found or the content is not valid UTF-8.
fn extract_problem_score(tex_bytes: &[u8]) -> Option<i32> {
    let content = std::str::from_utf8(tex_bytes).ok()?;
    let re = Regex::new(r"\\begin\{problem\}\[(\d+)\]").ok()?;
    re.captures(content)
        .and_then(|caps| caps.get(1))
        .and_then(|m| m.as_str().parse::<i32>().ok())
}

fn register_parent_directories(directories: &mut BTreeSet<String>, path: &str) {
    let components = path.split('/').collect::<Vec<_>>();
    if components.len() <= 1 {
        return;
    }

    for idx in 1..components.len() {
        directories.insert(components[..idx].join("/"));
    }
}

async fn insert_loaded_question_files_tx(
    object_store: &ObjectStore,
    tx: &mut Transaction<'_, Postgres>,
    question_id: &str,
    loaded: &LoadedQuestionZip,
) -> Result<()> {
    let tex_object_id = object_store
        .insert_object_tx(
            tx,
            &loaded.tex_file.path,
            &loaded.tex_file.bytes,
            Some("text/x-tex"),
        )
        .await?;
    insert_question_file_tx(
        tx,
        question_id,
        &tex_object_id,
        "tex",
        &loaded.tex_file.path,
        Some("text/x-tex"),
    )
    .await?;

    for asset in &loaded.asset_files {
        let mime = MimeGuess::from_path(&asset.path)
            .first_raw()
            .map(str::to_string);
        let object_id = object_store
            .insert_object_tx(tx, &asset.path, &asset.bytes, mime.as_deref())
            .await?;
        insert_question_file_tx(
            tx,
            question_id,
            &object_id,
            "asset",
            &asset.path,
            mime.as_deref(),
        )
        .await?;
    }

    Ok(())
}

async fn insert_question_tags_tx(
    tx: &mut Transaction<'_, Postgres>,
    question_id: &str,
    tags: &[String],
) -> Result<()> {
    for (idx, tag) in tags.iter().enumerate() {
        query("INSERT INTO question_tags (question_id, tag, sort_order) VALUES ($1::uuid, $2, $3)")
            .bind(question_id)
            .bind(tag)
            .bind(i32::try_from(idx).unwrap_or(i32::MAX))
            .execute(&mut **tx)
            .await
            .with_context(|| format!("insert question tag failed: {tag}"))?;
    }

    Ok(())
}

async fn replace_question_files_tx(
    object_store: &ObjectStore,
    tx: &mut Transaction<'_, Postgres>,
    question_id: &str,
    loaded: &LoadedQuestionZip,
) -> Result<()> {
    let old_rows = query(
        "SELECT qf.object_id::text AS object_id, o.storage_path FROM question_files qf JOIN objects o ON o.object_id = qf.object_id WHERE qf.question_id = $1::uuid",
    )
    .bind(question_id)
    .fetch_all(&mut **tx)
    .await
    .context("load existing question file objects failed")?;
    let old_object_ids: Vec<String> = old_rows.iter().map(|r| r.get("object_id")).collect();
    let old_storage_paths: Vec<String> = old_rows
        .iter()
        .filter_map(|r| r.get::<Option<String>, _>("storage_path"))
        .collect();

    query("DELETE FROM question_files WHERE question_id = $1::uuid")
        .bind(question_id)
        .execute(&mut **tx)
        .await
        .context("delete existing question files failed")?;

    if !old_object_ids.is_empty() {
        let mut builder = QueryBuilder::<Postgres>::new("DELETE FROM objects WHERE object_id IN (");
        for (idx, object_id) in old_object_ids.iter().enumerate() {
            if idx > 0 {
                builder.push(", ");
            }
            builder.push_bind(object_id).push("::uuid");
        }
        builder.push(')');
        builder
            .build()
            .execute(&mut **tx)
            .await
            .context("delete previous question file objects failed")?;
    }

    insert_loaded_question_files_tx(object_store, tx, question_id, loaded).await?;

    // Delete old files from disk after the DB transaction has removed them.
    object_store.delete_object_files(&old_storage_paths);

    Ok(())
}

async fn insert_question_file_tx(
    tx: &mut Transaction<'_, Postgres>,
    question_id: &str,
    object_id: &str,
    file_kind: &str,
    file_path: &str,
    mime_type: Option<&str>,
) -> Result<()> {
    query(
        r#"
        INSERT INTO question_files (
            question_file_id, question_id, object_id, file_kind, file_path, mime_type, created_at
        )
        VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6, NOW())
        "#,
    )
    .bind(Uuid::new_v4().to_string())
    .bind(question_id)
    .bind(object_id)
    .bind(file_kind)
    .bind(file_path)
    .bind(mime_type)
    .execute(&mut **tx)
    .await
    .with_context(|| format!("insert question file failed: {file_path}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{load_question_zip, MAX_UPLOAD_BYTES};
    use std::io::Write;
    use zip::write::SimpleFileOptions;
    use zip::ZipWriter;

    fn build_zip() -> Vec<u8> {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();
        writer.start_file("assets/fig1.png", options).unwrap();
        writer.write_all(b"png").unwrap();

        writer.finish().unwrap().into_inner()
    }

    #[test]
    fn load_question_zip_reads_standard_layout() {
        let loaded = load_question_zip(&build_zip()).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert_eq!(loaded.asset_files.len(), 1);
        assert_eq!(loaded.score, None);
    }

    #[test]
    fn load_question_zip_ignores_extra_root_file() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();
        writer.start_file("readme.txt", options).unwrap();
        writer.write_all(b"nope").unwrap();
        writer.start_file("assets/fig1.png", options).unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert_eq!(loaded.asset_files.len(), 1);
        assert_eq!(loaded.asset_files[0].path, "assets/fig1.png");
    }

    #[test]
    fn upload_limit_constant_matches_requirement() {
        assert_eq!(MAX_UPLOAD_BYTES, 20 * 1024 * 1024);
    }

    #[test]
    fn load_question_zip_rejects_path_traversal() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("../etc/passwd", options).unwrap();
        writer.write_all(b"root").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let err = load_question_zip(&zip).expect_err("zip should be rejected");
        assert!(err.to_string().contains("unsafe path"));
    }

    #[test]
    fn load_question_zip_rejects_absolute_path() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("/etc/passwd", options).unwrap();
        writer.write_all(b"root").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let err = load_question_zip(&zip).expect_err("zip should be rejected");
        assert!(err.to_string().contains("must be relative"));
    }

    #[test]
    fn load_question_zip_rejects_zip_bomb() {
        use super::MAX_TOTAL_UNCOMPRESSED_BYTES;

        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();

        // Write a single large file that exceeds the limit
        writer.start_file("assets/big.bin", options).unwrap();
        let chunk = vec![0u8; 1024 * 1024]; // 1 MiB chunks
        let chunks_needed = (MAX_TOTAL_UNCOMPRESSED_BYTES / chunk.len()) + 2;
        for _ in 0..chunks_needed {
            writer.write_all(&chunk).unwrap();
        }

        let zip = writer.finish().unwrap().into_inner();
        let err = load_question_zip(&zip).expect_err("zip should be rejected");
        assert!(err.to_string().contains("uncompressed size"));
    }

    #[test]
    fn load_question_zip_accepts_missing_assets_directory() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert!(loaded.asset_files.is_empty());
    }

    #[test]
    fn load_question_zip_rejects_multiple_tex_files() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("a.tex", options).unwrap();
        writer.write_all(br"\section{A}").unwrap();
        writer.start_file("b.tex", options).unwrap();
        writer.write_all(br"\section{B}").unwrap();
        writer.start_file("assets/fig.png", options).unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let err = load_question_zip(&zip).expect_err("zip should be rejected");
        assert!(err.to_string().contains("exactly one .tex file"));
    }

    #[test]
    fn load_question_zip_rejects_wrong_root_directory() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();
        writer.start_file("images/fig.png", options).unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let err = load_question_zip(&zip).expect_err("zip should be rejected");
        assert!(err.to_string().contains("assets/"));
    }

    #[test]
    fn load_question_zip_accepts_single_wrapping_directory() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("wrapped/problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();
        writer
            .start_file("wrapped/assets/fig.png", options)
            .unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert_eq!(loaded.asset_files.len(), 1);
        assert_eq!(loaded.asset_files[0].path, "assets/fig.png");
    }

    #[test]
    fn load_question_zip_ignores_extra_root_file_in_wrapping_directory() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("wrapped/problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();
        writer.start_file("wrapped/README.txt", options).unwrap();
        writer.write_all(b"ignored").unwrap();
        writer
            .start_file("wrapped/assets/fig.png", options)
            .unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert_eq!(loaded.asset_files.len(), 1);
        assert_eq!(loaded.asset_files[0].path, "assets/fig.png");
    }

    #[test]
    fn load_question_zip_accepts_wrapping_directory_without_assets() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("wrapped/problem.tex", options).unwrap();
        writer.write_all(br"\section{Demo}").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.tex_file.path, "problem.tex");
        assert!(loaded.asset_files.is_empty());
    }

    #[test]
    fn extract_problem_score_parses_score_from_tex() {
        use super::extract_problem_score;

        assert_eq!(
            extract_problem_score(br"\begin{problem}[20]{Title}"),
            Some(20),
        );
        assert_eq!(extract_problem_score(br"\begin{problem}[60]{}"), Some(60),);
        assert_eq!(
            extract_problem_score(br"\begin{problem}[5]{Basic}"),
            Some(5),
        );
        assert_eq!(extract_problem_score(br"\section{Demo}"), None,);
        assert_eq!(extract_problem_score(br"\begin{problem}{No bracket}"), None,);
    }

    #[test]
    fn load_question_zip_extracts_score() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default();

        writer.start_file("main.tex", options).unwrap();
        writer
            .write_all(br"\begin{problem}[40]{Test Title}")
            .unwrap();
        writer.start_file("assets/fig.png", options).unwrap();
        writer.write_all(b"png").unwrap();

        let zip = writer.finish().unwrap().into_inner();
        let loaded = load_question_zip(&zip).expect("zip should parse");
        assert_eq!(loaded.score, Some(40));
    }
}
