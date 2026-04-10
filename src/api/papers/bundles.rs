use std::fs::File;

use anyhow::{Context, Result};
use serde::Serialize;
use sqlx::{query, PgPool, Row};
use zip::ZipWriter;

use super::{models::PaperDetail, queries::map_paper_detail};
use crate::api::{
    ops::paper_render::{
        render_paper_bundle, PaperTemplateKind, RenderPaperInput, RenderQuestionAssetInput,
        RenderQuestionInput,
    },
    questions::{
        bundles::{load_question_bundle_data, question_detail_to_summary, QuestionBundleData},
        models::QuestionDetail,
    },
    shared::{
        bundles::{
            finish_zip_response, temp_zip_path, timestamp_unix, write_bundle_file, write_manifest,
            BundleFileEntry,
        },
        db::fetch_object_bytes,
        error::NotFoundError,
        utils::bundle_directory_name,
    },
};

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

pub(crate) async fn build_paper_bundle_response(
    pool: &PgPool,
    paper_ids: &[String],
) -> Result<axum::response::Response> {
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

async fn load_paper_bundle_data(pool: &PgPool, paper_id: &str) -> Result<PaperBundleData> {
    let paper_row = query(
        r#"
        SELECT p.paper_id::text AS paper_id, p.description, p.title, p.subtitle,
               p.append_object_id::text AS append_object_id,
               o.file_name AS append_file_name, o.mime_type AS append_mime_type,
               p.created_by::text AS created_by,
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
    .ok_or_else(|| NotFoundError(format!("paper not found: {paper_id}")))?;

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

    let mut authors = Vec::new();
    let mut seen_authors = std::collections::HashSet::new();
    for question in &bundle.questions {
        let author = question.metadata.author.trim();
        if !author.is_empty() && seen_authors.insert(author.to_string()) {
            authors.push(author.to_string());
        }
    }

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
        .ok_or_else(|| anyhow::anyhow!("paper does not contain any questions"))?;
    let expected_category = first_question.metadata.category.as_str();
    let template_kind = match expected_category {
        "T" => PaperTemplateKind::Theory,
        "E" => PaperTemplateKind::Experiment,
        other => {
            return Err(anyhow::anyhow!(
                "paper questions must all be category T or E before rendering, found {other}"
            ));
        }
    };

    for question in questions.iter().skip(1) {
        if question.metadata.category != expected_category {
            return Err(anyhow::anyhow!(
                "paper questions must share one category before rendering, found {} and {}",
                expected_category,
                question.metadata.category
            ));
        }
    }

    Ok(template_kind)
}
