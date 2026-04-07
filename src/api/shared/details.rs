use anyhow::{anyhow, Context, Result};
use sqlx::{query, PgPool, Row};

use super::error::NotFoundError;

use crate::api::{
    papers::{models::PaperDetail, queries::map_paper_detail},
    questions::{
        models::{QuestionDetail, QuestionPaperRef},
        queries::{
            load_question_difficulties, load_question_files, load_question_tags,
            map_question_detail, map_question_paper_ref, map_question_summary,
        },
    },
};

pub(crate) const TIMESTAMP_SQL: &str = "'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetailVisibility {
    ActiveOnly,
    All,
}

#[derive(Debug)]
pub(crate) struct LoadedQuestionDetail {
    pub(crate) detail: QuestionDetail,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
}

#[derive(Debug)]
pub(crate) struct LoadedPaperDetail {
    pub(crate) detail: PaperDetail,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
}

pub(crate) async fn load_question_detail(
    pool: &PgPool,
    question_id: &str,
    question_visibility: DetailVisibility,
    paper_visibility: DetailVisibility,
) -> Result<LoadedQuestionDetail> {
    let question_filter = visibility_clause("q", question_visibility);
    let paper_filter = visibility_clause("p", paper_visibility);

    let row = query(&format!(
        r#"
        SELECT q.question_id::text AS question_id, q.source_tex_path, q.category, q.status,
               COALESCE(q.description, '') AS description,
               q.score,
               q.author,
               q.reviewers,
               to_char(q.created_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS created_at,
               to_char(q.updated_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS updated_at,
               to_char(q.deleted_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS deleted_at,
               q.deleted_by
        FROM questions q
        WHERE q.question_id = $1::uuid{question_filter}
        "#,
    ))
    .bind(question_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("load question detail failed: {question_id}"))?
    .ok_or_else(|| NotFoundError(format!("question not found: {question_id}")))?;

    let deleted_at: Option<String> = row.get("deleted_at");
    let deleted_by: Option<String> = row.get("deleted_by");
    let tex_files = load_question_files(pool, question_id, "tex")
        .await
        .with_context(|| format!("load question tex files failed: {question_id}"))?;
    let tex_object_id = tex_files
        .first()
        .map(|file| file.object_id.clone())
        .ok_or_else(|| anyhow!("question is missing a tex object: {question_id}"))?;
    let assets = load_question_files(pool, question_id, "asset")
        .await
        .with_context(|| format!("load question assets failed: {question_id}"))?;
    let tags = load_question_tags(pool, question_id)
        .await
        .with_context(|| format!("load question tags failed: {question_id}"))?;
    let difficulty = load_question_difficulties(pool, question_id)
        .await
        .with_context(|| format!("load question difficulties failed: {question_id}"))?;

    let papers = query(&format!(
        r#"
        SELECT p.paper_id::text AS paper_id, p.description, p.title, p.subtitle, pq.sort_order
        FROM paper_questions pq
        JOIN papers p ON p.paper_id = pq.paper_id
        WHERE pq.question_id = $1::uuid{paper_filter}
        ORDER BY p.created_at DESC, pq.sort_order
        "#,
    ))
    .bind(question_id)
    .fetch_all(pool)
    .await
    .with_context(|| format!("load question papers failed: {question_id}"))?
    .into_iter()
    .map(map_question_paper_ref)
    .collect::<Vec<QuestionPaperRef>>();

    Ok(LoadedQuestionDetail {
        detail: map_question_detail(row, tex_object_id, tags, difficulty, assets, papers),
        deleted_at,
        deleted_by,
    })
}

pub(crate) async fn load_paper_detail(
    pool: &PgPool,
    paper_id: &str,
    paper_visibility: DetailVisibility,
    question_visibility: DetailVisibility,
) -> Result<LoadedPaperDetail> {
    let paper_filter = visibility_clause("p", paper_visibility);
    let question_filter = visibility_clause("q", question_visibility);

    let paper_row = query(&format!(
        r#"
        SELECT p.paper_id::text AS paper_id, p.description, p.title, p.subtitle,
               to_char(p.created_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS created_at,
               to_char(p.updated_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS updated_at,
               to_char(p.deleted_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS deleted_at,
               p.deleted_by
        FROM papers p
        WHERE p.paper_id = $1::uuid{paper_filter}
        "#,
    ))
    .bind(paper_id)
    .fetch_optional(pool)
    .await
    .with_context(|| format!("load paper detail failed: {paper_id}"))?
    .ok_or_else(|| NotFoundError(format!("paper not found: {paper_id}")))?;

    let deleted_at: Option<String> = paper_row.get("deleted_at");
    let deleted_by: Option<String> = paper_row.get("deleted_by");
    let question_rows = query(&format!(
        r#"
        SELECT q.question_id::text AS question_id,
               q.source_tex_path,
               q.category,
               q.status,
               COALESCE(q.description, '') AS description,
               q.score,
               q.author,
               q.reviewers,
               to_char(q.created_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS created_at,
               to_char(q.updated_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS updated_at
        FROM paper_questions pq
        JOIN questions q ON q.question_id = pq.question_id
        WHERE pq.paper_id = $1::uuid{question_filter}
        ORDER BY pq.sort_order
        "#,
    ))
    .bind(paper_id)
    .fetch_all(pool)
    .await
    .with_context(|| format!("load paper questions failed: {paper_id}"))?;

    let mut questions = Vec::with_capacity(question_rows.len());
    for row in question_rows {
        let question_id: String = row.get("question_id");
        let tags = load_question_tags(pool, &question_id)
            .await
            .with_context(|| format!("load paper question tags failed: {question_id}"))?;
        let difficulty = load_question_difficulties(pool, &question_id)
            .await
            .with_context(|| format!("load paper question difficulties failed: {question_id}"))?;
        questions.push(map_question_summary(row, tags, difficulty));
    }

    Ok(LoadedPaperDetail {
        detail: map_paper_detail(paper_row, questions),
        deleted_at,
        deleted_by,
    })
}

fn visibility_clause(alias: &str, visibility: DetailVisibility) -> String {
    match visibility {
        DetailVisibility::ActiveOnly => format!(" AND {alias}.deleted_at IS NULL"),
        DetailVisibility::All => String::new(),
    }
}
