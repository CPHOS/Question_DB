use std::{
    fs::File,
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Body,
    http::{header, HeaderValue, Response, StatusCode},
};
use serde::Serialize;
use sqlx::{query, PgPool, Row};
use tokio::fs;
use tokio_util::io::ReaderStream;
use uuid::Uuid;
use zip::{write::SimpleFileOptions, ZipWriter};

use crate::api::{
    ops::paper_render::{
        render_paper_bundle, PaperTemplateKind, RenderPaperInput, RenderQuestionAssetInput,
        RenderQuestionInput,
    },
    papers::{models::PaperDetail, queries::map_paper_detail},
    questions::{
        models::{QuestionAssetRef, QuestionDetail, QuestionSummary},
        queries::load_question_files,
    },
    shared::{
        db::fetch_object_bytes,
        details::{load_question_detail, DetailVisibility},
        utils::bundle_directory_name,
    },
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

#[derive(Debug, Serialize)]
struct PaperBundleManifest {
    kind: &'static str,
    generated_at_unix: u64,
    paper_count: usize,
    papers: Vec<PaperBundleManifestItem>,
}

#[derive(Debug, Serialize)]
struct PaperBundleManifestItem {
    paper_id: String,
    directory: String,
    metadata: PaperDetail,
    template_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    append_file: Option<BundleFileEntry>,
    main_tex_file: BundleFileEntry,
    assets: Vec<BundleFileEntry>,
    questions: Vec<PaperBundleQuestionManifestItem>,
}

#[derive(Debug, Serialize)]
struct PaperBundleQuestionManifestItem {
    question_id: String,
    sequence: usize,
    source_tex_path: String,
    asset_prefix: String,
    metadata: QuestionDetail,
}

#[derive(Debug, Clone, Serialize)]
struct BundleFileEntry {
    zip_path: String,
    original_path: String,
    file_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_question_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_id: Option<String>,
    mime_type: Option<String>,
}

#[derive(Debug)]
struct QuestionBundleData {
    metadata: QuestionDetail,
    files: Vec<QuestionAssetRef>,
}

#[derive(Debug)]
struct PaperBundleData {
    metadata: PaperDetail,
    appendix: Option<PaperAppendixData>,
    questions: Vec<QuestionBundleData>,
}

#[derive(Debug)]
struct PaperAppendixData {
    object_id: String,
    original_file_name: String,
    mime_type: Option<String>,
}

pub(crate) async fn build_question_bundle_response(
    pool: &PgPool,
    question_ids: &[String],
) -> Result<Response<Body>> {
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
        let bundle = load_question_bundle_data(pool, question_id).await?;
        let directory = bundle_directory_name(&bundle.metadata.description, question_id);
        let manifest_files =
            write_question_bundle_files(pool, &mut writer, &bundle.files, &directory).await?;
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

pub(crate) async fn build_paper_bundle_response(
    pool: &PgPool,
    paper_ids: &[String],
) -> Result<Response<Body>> {
    let bundle_name = format!("papers_bundle_{}.zip", timestamp_unix());
    let zip_path = temp_zip_path("papers");
    let file = File::create(&zip_path).with_context(|| {
        format!(
            "create paper bundle zip failed: {}",
            zip_path.to_string_lossy()
        )
    })?;
    let mut writer = ZipWriter::new(file);
    let mut manifest_items = Vec::with_capacity(paper_ids.len());

    for paper_id in paper_ids {
        let bundle = load_paper_bundle_data(pool, paper_id).await?;
        let directory = bundle_directory_name(&bundle.metadata.description, paper_id);
        let append_file =
            write_paper_appendix_file(pool, &mut writer, bundle.appendix.as_ref(), &directory)
                .await?;
        let rendered = render_paper_bundle(build_render_paper_input(pool, &bundle).await?)?;

        let main_tex_zip_path = format!("{directory}/main.tex");
        write_bundle_file(
            &mut writer,
            &main_tex_zip_path,
            rendered.main_tex.as_bytes(),
        )?;
        let main_tex_file = BundleFileEntry {
            zip_path: main_tex_zip_path,
            original_path: rendered.template_source_path.to_string(),
            file_kind: "rendered_tex".to_string(),
            source_question_id: None,
            object_id: None,
            mime_type: Some("text/x-tex".to_string()),
        };

        let mut rendered_asset_entries = Vec::with_capacity(rendered.assets.len());
        for asset in &rendered.assets {
            let zip_path = format!("{directory}/{}", asset.output_path);
            write_bundle_file(&mut writer, &zip_path, &asset.bytes)?;
            rendered_asset_entries.push(BundleFileEntry {
                zip_path,
                original_path: asset.original_path.clone(),
                file_kind: "asset".to_string(),
                source_question_id: Some(asset.question_id.clone()),
                object_id: Some(asset.object_id.clone()),
                mime_type: asset.mime_type.clone(),
            });
        }

        let question_entries = bundle
            .questions
            .into_iter()
            .zip(rendered.questions.into_iter())
            .map(
                |(question, rendered_question)| PaperBundleQuestionManifestItem {
                    question_id: rendered_question.question_id,
                    sequence: rendered_question.sequence,
                    source_tex_path: rendered_question.source_tex_path,
                    asset_prefix: rendered_question.asset_prefix,
                    metadata: question.metadata,
                },
            )
            .collect::<Vec<_>>();

        manifest_items.push(PaperBundleManifestItem {
            paper_id: paper_id.clone(),
            directory,
            metadata: bundle.metadata,
            template_source: rendered.template_source_path.to_string(),
            append_file,
            main_tex_file,
            assets: rendered_asset_entries,
            questions: question_entries,
        });
    }

    let manifest = PaperBundleManifest {
        kind: "paper_bundle",
        generated_at_unix: timestamp_unix(),
        paper_count: manifest_items.len(),
        papers: manifest_items,
    };
    write_manifest(&mut writer, &manifest)?;
    finish_zip_response(writer, zip_path, &bundle_name).await
}

async fn write_question_bundle_files(
    pool: &PgPool,
    writer: &mut ZipWriter<File>,
    files: &[QuestionAssetRef],
    directory: &str,
) -> Result<Vec<BundleFileEntry>> {
    let mut manifest_entries = Vec::with_capacity(files.len());

    for file in files {
        let zip_path = format!("{directory}/{}", file.path);
        let bytes = fetch_object_bytes(pool, &file.object_id).await?;
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

async fn write_paper_appendix_file(
    pool: &PgPool,
    writer: &mut ZipWriter<File>,
    appendix: Option<&PaperAppendixData>,
    directory: &str,
) -> Result<Option<BundleFileEntry>> {
    let Some(appendix) = appendix else {
        return Ok(None);
    };

    let zip_path = format!("{directory}/append.zip");
    let bytes = fetch_object_bytes(pool, &appendix.object_id).await?;
    write_bundle_file(writer, &zip_path, &bytes)?;

    Ok(Some(BundleFileEntry {
        zip_path,
        original_path: appendix.original_file_name.clone(),
        file_kind: "appendix".to_string(),
        source_question_id: None,
        object_id: Some(appendix.object_id.clone()),
        mime_type: appendix.mime_type.clone(),
    }))
}

fn write_bundle_file(writer: &mut ZipWriter<File>, zip_path: &str, bytes: &[u8]) -> Result<()> {
    writer
        .start_file(zip_path, SimpleFileOptions::default())
        .context("start bundle file entry failed")?;
    writer
        .write_all(bytes)
        .with_context(|| format!("write bundle file failed: {zip_path}"))?;
    Ok(())
}

fn write_manifest<T: Serialize>(writer: &mut ZipWriter<File>, manifest: &T) -> Result<()> {
    writer
        .start_file("manifest.json", SimpleFileOptions::default())
        .context("start manifest.json failed")?;
    let bytes = serde_json::to_vec_pretty(manifest).context("serialize manifest.json failed")?;
    writer
        .write_all(&bytes)
        .context("write manifest.json failed")?;
    Ok(())
}

async fn finish_zip_response(
    writer: ZipWriter<File>,
    zip_path: PathBuf,
    bundle_name: &str,
) -> Result<Response<Body>> {
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

async fn load_question_bundle_data(pool: &PgPool, question_id: &str) -> Result<QuestionBundleData> {
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

async fn load_paper_bundle_data(pool: &PgPool, paper_id: &str) -> Result<PaperBundleData> {
    let paper_row = query(
        r#"
        SELECT p.paper_id::text AS paper_id, p.description, p.title, p.subtitle,
               p.append_object_id::text AS append_object_id,
               o.file_name AS append_file_name, o.mime_type AS append_mime_type,
               to_char(p.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM papers p
        LEFT JOIN objects o ON o.object_id = p.append_object_id
        WHERE p.paper_id = $1::uuid AND p.deleted_at IS NULL
        "#,
    )
    .bind(paper_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("load paper detail failed: {paper_id}"))?
    .ok_or_else(|| anyhow!("paper not found: {paper_id}"))?;

    let question_rows = query(
        r#"
        SELECT q.question_id::text AS question_id, pq.sort_order, q.category, q.status
        FROM paper_questions pq
        JOIN questions q ON q.question_id = pq.question_id
        WHERE pq.paper_id = $1::uuid AND q.deleted_at IS NULL
        ORDER BY pq.sort_order
        "#,
    )
    .bind(paper_id)
    .fetch_all(pool)
    .await
    .with_context(|| format!("load paper questions failed: {paper_id}"))?;

    let mut question_summaries = Vec::with_capacity(question_rows.len());
    let mut questions = Vec::with_capacity(question_rows.len());
    for row in question_rows {
        let question_id: String = row.get("question_id");
        let bundle_data = load_question_bundle_data(pool, &question_id).await?;
        question_summaries.push(question_detail_to_summary(&bundle_data.metadata));
        questions.push(bundle_data);
    }

    let appendix = paper_row
        .get::<Option<String>, _>("append_object_id")
        .map(|object_id| PaperAppendixData {
            object_id,
            original_file_name: paper_row
                .get::<Option<String>, _>("append_file_name")
                .unwrap_or_else(|| "append.zip".to_string()),
            mime_type: paper_row.get("append_mime_type"),
        });

    Ok(PaperBundleData {
        metadata: map_paper_detail(paper_row, question_summaries),
        appendix,
        questions,
    })
}

async fn build_render_paper_input(
    pool: &PgPool,
    bundle: &PaperBundleData,
) -> Result<RenderPaperInput> {
    let template_kind = determine_paper_template_kind(&bundle.questions)?;
    let mut questions = Vec::with_capacity(bundle.questions.len());

    for (index, question) in bundle.questions.iter().enumerate() {
        let tex_bytes = fetch_object_bytes(pool, &question.metadata.tex_object_id).await?;
        let source_tex = String::from_utf8(tex_bytes).with_context(|| {
            format!(
                "question tex object is not valid UTF-8: {}",
                question.metadata.tex_object_id
            )
        })?;

        let mut assets = Vec::with_capacity(question.metadata.assets.len());
        for asset in &question.metadata.assets {
            assets.push(RenderQuestionAssetInput {
                original_path: asset.path.clone(),
                object_id: asset.object_id.clone(),
                mime_type: asset.mime_type.clone(),
                bytes: fetch_object_bytes(pool, &asset.object_id).await?,
            });
        }

        questions.push(RenderQuestionInput {
            question_id: question.metadata.question_id.clone(),
            sequence: index + 1,
            source_tex_path: question.metadata.source.tex.clone(),
            source_tex,
            assets,
        });
    }

    // Collect authors from questions in sort order, deduplicate preserving order.
    let mut authors = Vec::new();
    let mut seen_authors = std::collections::HashSet::new();
    for question in &bundle.questions {
        let author = question.metadata.author.trim();
        if !author.is_empty() && seen_authors.insert(author.to_string()) {
            authors.push(author.to_string());
        }
    }

    // Collect reviewers from all questions, deduplicate preserving order.
    let mut reviewers = Vec::new();
    let mut seen_reviewers = std::collections::HashSet::new();
    for question in &bundle.questions {
        for reviewer in &question.metadata.reviewers {
            let reviewer = reviewer.trim();
            if !reviewer.is_empty() && seen_reviewers.insert(reviewer.to_string()) {
                reviewers.push(reviewer.to_string());
            }
        }
    }

    Ok(RenderPaperInput {
        title: bundle.metadata.title.clone(),
        subtitle: bundle.metadata.subtitle.clone(),
        authors,
        reviewers,
        template_kind,
        questions,
    })
}

fn determine_paper_template_kind(questions: &[QuestionBundleData]) -> Result<PaperTemplateKind> {
    let first_question = questions
        .first()
        .ok_or_else(|| anyhow!("paper does not contain any questions"))?;
    let expected_category = first_question.metadata.category.as_str();
    let template_kind = match expected_category {
        "T" => PaperTemplateKind::Theory,
        "E" => PaperTemplateKind::Experiment,
        other => {
            return Err(anyhow!(
                "paper questions must all be category T or E before rendering, found {other}"
            ));
        }
    };

    for question in questions.iter().skip(1) {
        if question.metadata.category != expected_category {
            return Err(anyhow!(
                "paper questions must share one category before rendering, found {} and {}",
                expected_category,
                question.metadata.category
            ));
        }
    }

    Ok(template_kind)
}

fn question_detail_to_summary(detail: &QuestionDetail) -> QuestionSummary {
    use crate::api::questions::models::QuestionSourceRef;
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
        created_at: detail.created_at.clone(),
        updated_at: detail.updated_at.clone(),
    }
}

fn temp_zip_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "qb_{prefix}_bundle_{}_{}.zip",
        timestamp_unix(),
        Uuid::new_v4()
    ))
}

fn timestamp_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
