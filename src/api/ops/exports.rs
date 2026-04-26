//! Export pipelines for uploaded question metadata.

use std::{
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use csv::WriterBuilder;
use serde_json::json;
use sqlx::{query, PgPool, Row};

use super::models::ExportFormat;
use crate::api::{
    questions::{
        models::QuestionSourceRef,
        queries::{load_question_difficulties, load_question_files, load_question_tags},
    },
    shared::{db::ObjectStore, utils::canonical_or_original},
};

pub(crate) fn default_export_path(format: ExportFormat, is_public: bool) -> PathBuf {
    let suffix = if is_public { "public" } else { "internal" };
    let ext = match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Csv => "csv",
    };
    PathBuf::from("exports").join(format!("question_bank_{suffix}.{ext}"))
}

pub(crate) fn ensure_parent_dir(output_path: &Path, label: &str) -> Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create {label} parent directory failed: {}",
                parent.to_string_lossy()
            )
        })?;
    }
    Ok(())
}

pub(crate) async fn export_jsonl(
    pool: &PgPool,
    object_store: &ObjectStore,
    output_path: &Path,
    include_tex_source: bool,
) -> Result<usize> {
    let rows = query(
        r#"
        SELECT question_id::text AS question_id, source_tex_path, category, status,
               COALESCE(description, '') AS description, score,
               to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM questions
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC, question_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("query questions for jsonl export failed")?;

    let file = fs::File::create(output_path).with_context(|| {
        format!(
            "create export file failed: {}",
            output_path.to_string_lossy()
        )
    })?;
    let mut writer = BufWriter::new(file);

    for row in &rows {
        let question_id: String = row.get("question_id");
        let tex_files = load_question_files(pool, &question_id, "tex").await?;
        let assets = load_question_files(pool, &question_id, "asset").await?;
        let tags = load_question_tags(pool, &question_id).await?;
        let difficulty = load_question_difficulties(pool, &question_id).await?;
        let tex_object_id = tex_files
            .first()
            .map(|file| file.object_id.clone())
            .unwrap_or_default();

        let mut payload = json!({
            "question_id": question_id,
            "tex_object_id": tex_object_id,
            "source": QuestionSourceRef {
                tex: row.get("source_tex_path"),
            },
            "category": row.get::<String, _>("category"),
            "status": row.get::<String, _>("status"),
            "description": row.get::<String, _>("description"),
            "score": row.get::<Option<i32>, _>("score"),
            "difficulty": difficulty,
            "tags": tags,
            "assets": assets,
            "created_at": row.get::<String, _>("created_at"),
            "updated_at": row.get::<String, _>("updated_at"),
        });

        if include_tex_source && !tex_object_id.is_empty() {
            payload["tex_source"] =
                serde_json::Value::String(object_store.fetch_text_object(&tex_object_id).await?);
        }

        writer
            .write_all(serde_json::to_string(&payload)?.as_bytes())
            .context("write jsonl line failed")?;
        writer.write_all(b"\n").context("write newline failed")?;
    }

    writer.flush().context("flush jsonl writer failed")?;
    Ok(rows.len())
}

pub(crate) async fn export_csv(
    pool: &PgPool,
    object_store: &ObjectStore,
    output_path: &Path,
    include_tex_source: bool,
) -> Result<usize> {
    let rows = query(
        r#"
        SELECT question_id::text AS question_id, source_tex_path, category, status,
               COALESCE(description, '') AS description, score,
               to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM questions
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC, question_id
        "#,
    )
    .fetch_all(pool)
    .await
    .context("query questions for csv export failed")?;

    let file = fs::File::create(output_path).with_context(|| {
        format!(
            "create export csv failed: {}",
            output_path.to_string_lossy()
        )
    })?;
    let mut writer = WriterBuilder::new().from_writer(file);

    writer.write_record([
        "question_id",
        "tex_object_id",
        "source_tex_path",
        "category",
        "status",
        "description",
        "score",
        "difficulty",
        "tags",
        "created_at",
        "updated_at",
        "tex_source",
    ])?;

    for row in &rows {
        let question_id: String = row.get("question_id");
        let tex_files = load_question_files(pool, &question_id, "tex").await?;
        let tags = load_question_tags(pool, &question_id).await?;
        let difficulty = load_question_difficulties(pool, &question_id).await?;
        let tex_object_id = tex_files
            .first()
            .map(|file| file.object_id.clone())
            .unwrap_or_default();

        let score_str = row
            .get::<Option<i32>, _>("score")
            .map(|s| s.to_string())
            .unwrap_or_default();

        writer.write_record([
            question_id,
            tex_object_id.clone(),
            row.get::<String, _>("source_tex_path"),
            row.get::<String, _>("category"),
            row.get::<String, _>("status"),
            row.get::<String, _>("description"),
            score_str,
            serde_json::to_string(&difficulty)?,
            serde_json::to_string(&tags)?,
            row.get::<String, _>("created_at"),
            row.get::<String, _>("updated_at"),
            if include_tex_source && !tex_object_id.is_empty() {
                object_store.fetch_text_object(&tex_object_id).await?
            } else {
                String::new()
            },
        ])?;
    }

    writer.flush().context("flush csv writer failed")?;
    Ok(rows.len())
}

pub(crate) fn exported_path(path: &Path) -> String {
    canonical_or_original(path)
}
