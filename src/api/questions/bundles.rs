use std::fs::File;

use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::PgPool;
use zip::ZipWriter;

use super::{
    models::{QuestionAssetRef, QuestionDetail, QuestionSourceRef, QuestionSummary},
    queries::load_question_files,
};
use crate::api::shared::{
    bundles::{
        finish_zip_response, temp_zip_path, timestamp_unix, write_bundle_file, write_manifest,
        BundleFileEntry,
    },
    db::ObjectStore,
    details::{load_question_detail, DetailVisibility},
    utils::bundle_directory_name,
};

#[derive(Debug, Serialize)]
struct QuestionBundleManifest {
    kind: &'static str,
    generated_at_unix: u64,
    question_count: usize,
    questions: Vec<QuestionBundleManifestItem>,
}

#[derive(Debug, Serialize)]
struct QuestionBundleManifestItem {
    question_id: String,
    directory: String,
    metadata: QuestionDetail,
    files: Vec<BundleFileEntry>,
}

#[derive(Debug)]
pub(crate) struct QuestionBundleData {
    pub(crate) metadata: QuestionDetail,
    pub(crate) files: Vec<QuestionAssetRef>,
}

pub(crate) async fn build_question_bundle_response(
    object_store: &ObjectStore,
    question_ids: &[String],
) -> Result<axum::response::Response> {
    let bundle_name = format!("questions_bundle_{}.zip", timestamp_unix());
    let zip_path = temp_zip_path("questions");
    let file = File::create(&zip_path).with_context(|| {
        format!(
            "create question bundle zip failed: {}",
            zip_path.to_string_lossy()
        )
    })?;
    let mut writer = ZipWriter::new(file);
    let mut manifest_items = Vec::with_capacity(question_ids.len());

    for question_id in question_ids {
        let bundle = load_question_bundle_data(object_store.pool(), question_id).await?;
        let directory = bundle_directory_name(&bundle.metadata.description, question_id);
        let manifest_files =
            write_question_bundle_files(object_store, &mut writer, &bundle.files, &directory).await?;
        manifest_items.push(QuestionBundleManifestItem {
            question_id: question_id.clone(),
            directory,
            metadata: bundle.metadata,
            files: manifest_files,
        });
    }

    let manifest = QuestionBundleManifest {
        kind: "question_bundle",
        generated_at_unix: timestamp_unix(),
        question_count: manifest_items.len(),
        questions: manifest_items,
    };
    write_manifest(&mut writer, &manifest)?;
    finish_zip_response(writer, zip_path, &bundle_name).await
}

pub(crate) async fn load_question_bundle_data(
    pool: &PgPool,
    question_id: &str,
) -> Result<QuestionBundleData> {
    let loaded = load_question_detail(
        pool,
        question_id,
        DetailVisibility::ActiveOnly,
        DetailVisibility::ActiveOnly,
    )
    .await?;

    let all_files = load_question_files(pool, question_id, "tex")
        .await
        .with_context(|| format!("load question tex files for bundle failed: {question_id}"))?;
    let mut files = all_files;
    files.extend(
        load_question_files(pool, question_id, "asset")
            .await
            .with_context(|| format!("load question assets for bundle failed: {question_id}"))?,
    );

    Ok(QuestionBundleData {
        metadata: loaded.detail,
        files,
    })
}

pub(crate) fn question_detail_to_summary(detail: &QuestionDetail) -> QuestionSummary {
    QuestionSummary {
        question_id: detail.question_id.clone(),
        source: QuestionSourceRef {
            tex: detail.source.tex.clone(),
        },
        category: detail.category.clone(),
        status: detail.status.clone(),
        description: detail.description.clone(),
        score: detail.score,
        author: detail.author.clone(),
        reviewers: detail.reviewers.clone(),
        tags: detail.tags.clone(),
        difficulty: detail.difficulty.clone(),
        allow_auto_reviewer: detail.allow_auto_reviewer,
        created_by: detail.created_by.clone(),
        created_at: detail.created_at.clone(),
        updated_at: detail.updated_at.clone(),
    }
}

async fn write_question_bundle_files(
    object_store: &ObjectStore,
    writer: &mut ZipWriter<File>,
    files: &[QuestionAssetRef],
    directory: &str,
) -> Result<Vec<BundleFileEntry>> {
    let mut manifest_entries = Vec::with_capacity(files.len());

    for file in files {
        let zip_path = format!("{directory}/{}", file.path);
        let bytes = object_store.fetch_object_bytes(&file.object_id).await?;
        write_bundle_file(writer, &zip_path, &bytes)?;

        manifest_entries.push(BundleFileEntry {
            zip_path,
            original_path: file.path.clone(),
            file_kind: file.file_kind.clone(),
            source_question_id: None,
            object_id: Some(file.object_id.clone()),
            mime_type: file.mime_type.clone(),
        });
    }

    Ok(manifest_entries)
}
