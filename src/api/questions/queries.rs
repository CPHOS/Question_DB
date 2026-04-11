//! Query planning and row-to-response mapping for question endpoints.

use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, Result};
use sqlx::{postgres::PgRow, query, PgPool, Postgres, QueryBuilder, Row};

use super::models::{
    validate_question_category, DifficultyEditor, QuestionAssetRef, QuestionDetail,
    QuestionDifficulty, QuestionDifficultyValue, QuestionPaperRef, QuestionSourceRef,
    QuestionSummary, QuestionsParams,
};
use crate::api::shared::{
    pagination::{normalize_limit, normalize_offset},
    utils::{escape_ilike, validate_timestamp_param},
};

/// Returned from `build_query` so callers can inspect limit/offset and the SQL
/// (for tests) without needing a separate execute step.
pub(crate) struct QuestionsQueryPlan<'a> {
    pub(crate) builder: QueryBuilder<'a, Postgres>,
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl QuestionsParams {
    pub(crate) fn normalized_limit(&self) -> i64 {
        normalize_limit(self.limit)
    }

    pub(crate) fn normalized_offset(&self) -> i64 {
        normalize_offset(self.offset)
    }

    /// Build + bind a query in one step.  The returned `QueryBuilder` already
    /// holds all parameter bindings – call `builder.build().fetch_all(pool)` to
    /// execute.
    pub(crate) fn build_query(&self) -> QuestionsQueryPlan<'_> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "
            SELECT q.question_id::text AS question_id,
                   q.source_tex_path,
                   q.category,
                   q.status,
                   COALESCE(q.description, '') AS description,
                   q.score,
                   q.author,
                   q.reviewers,
                   q.created_by::text AS created_by,
                   q.allow_auto_reviewer,
                   to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at,
                   to_char(q.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS updated_at,
                   COUNT(*) OVER() AS total_count
            FROM questions q
            WHERE q.deleted_at IS NULL",
        );

        if let Some(category) = &self.category {
            builder.push(" AND q.category = ").push_bind(category);
        }
        if let Some(author) = &self.author {
            builder.push(" AND q.author = ").push_bind(author);
        }
        if let Some(tag) = &self.tag {
            builder
                .push(" AND EXISTS (SELECT 1 FROM question_tags qt WHERE qt.question_id = q.question_id AND qt.tag = ")
                .push_bind(tag)
                .push(")");
        }
        if let Some(reviewer) = &self.reviewer {
            let names: Vec<&str> = reviewer
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .collect();
            if names.len() == 1 {
                builder
                    .push(" AND q.reviewers @> ARRAY[")
                    .push_bind(names[0].to_string())
                    .push("]::text[]");
            } else if names.len() > 1 {
                builder.push(" AND q.reviewers && ARRAY[");
                for (i, name) in names.iter().enumerate() {
                    if i > 0 {
                        builder.push(", ");
                    }
                    builder.push_bind(name.to_string());
                }
                builder.push("]::text[]");
            }
        }
        if let Some(assigned_reviewer_id) = &self.assigned_reviewer_id {
            builder
                .push(" AND EXISTS (SELECT 1 FROM question_reviews qr WHERE qr.question_id = q.question_id AND qr.reviewer_id = ")
                .push_bind(assigned_reviewer_id)
                .push("::uuid)");
        }
        if let Some(score_min) = self.score_min {
            builder.push(" AND q.score >= ").push_bind(score_min);
        }
        if let Some(score_max) = self.score_max {
            builder.push(" AND q.score <= ").push_bind(score_max);
        }
        if let Some(difficulty_tag) = &self.difficulty_tag {
            builder
                .push(" AND EXISTS (SELECT 1 FROM question_difficulties qd WHERE qd.question_id = q.question_id AND qd.algorithm_tag = ")
                .push_bind(difficulty_tag);
            if let Some(difficulty_min) = self.difficulty_min {
                builder.push(" AND qd.score >= ").push_bind(difficulty_min);
            }
            if let Some(difficulty_max) = self.difficulty_max {
                builder.push(" AND qd.score <= ").push_bind(difficulty_max);
            }
            builder.push(")");
        }
        if let Some(paper_id) = &self.paper_id {
            builder
                .push(" AND EXISTS (SELECT 1 FROM paper_questions pq JOIN papers p ON p.paper_id = pq.paper_id WHERE pq.question_id = q.question_id AND p.deleted_at IS NULL AND pq.paper_id = ")
                .push_bind(paper_id)
                .push("::uuid)");
        }
        if let Some(search) = &self.q {
            let needle = format!("%{}%", escape_ilike(search));
            builder
                .push(" AND COALESCE(q.description, '') ILIKE ")
                .push_bind(needle);
        }
        if let Some(created_after) = &self.created_after {
            builder
                .push(" AND q.created_at >= ")
                .push_bind(created_after)
                .push("::timestamptz");
        }
        if let Some(created_before) = &self.created_before {
            builder
                .push(" AND q.created_at <= ")
                .push_bind(created_before)
                .push("::timestamptz");
        }
        if let Some(updated_after) = &self.updated_after {
            builder
                .push(" AND q.updated_at >= ")
                .push_bind(updated_after)
                .push("::timestamptz");
        }
        if let Some(updated_before) = &self.updated_before {
            builder
                .push(" AND q.updated_at <= ")
                .push_bind(updated_before)
                .push("::timestamptz");
        }

        let limit = self.normalized_limit();
        let offset = self.normalized_offset();
        builder
            .push(" ORDER BY q.created_at DESC, q.question_id LIMIT ")
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(offset);

        QuestionsQueryPlan {
            builder,
            limit,
            offset,
        }
    }
}

pub(crate) fn validate_question_filters(params: &QuestionsParams) -> Result<()> {
    if let Some(category) = &params.category {
        validate_question_category(category)
            .map_err(|_| anyhow!("category must be one of: none, T, E"))?;
    }
    if let Some(score_min) = params.score_min {
        if score_min < 0 {
            return Err(anyhow!("score_min must be non-negative"));
        }
    }
    if let Some(score_max) = params.score_max {
        if score_max < 0 {
            return Err(anyhow!("score_max must be non-negative"));
        }
    }
    if let (Some(score_min), Some(score_max)) = (params.score_min, params.score_max) {
        if score_min > score_max {
            return Err(anyhow!("score_min must be less than or equal to score_max"));
        }
    }
    if let Some(difficulty_tag) = &params.difficulty_tag {
        if difficulty_tag.trim().is_empty() {
            return Err(anyhow!("difficulty_tag must not be empty"));
        }
    }
    if (params.difficulty_min.is_some() || params.difficulty_max.is_some())
        && params.difficulty_tag.is_none()
    {
        return Err(anyhow!(
            "difficulty_tag is required when difficulty_min or difficulty_max is provided"
        ));
    }
    if let Some(difficulty_min) = params.difficulty_min {
        if !(1..=10).contains(&difficulty_min) {
            return Err(anyhow!("difficulty_min must be between 1 and 10"));
        }
    }
    if let Some(difficulty_max) = params.difficulty_max {
        if !(1..=10).contains(&difficulty_max) {
            return Err(anyhow!("difficulty_max must be between 1 and 10"));
        }
    }
    if let (Some(difficulty_min), Some(difficulty_max)) =
        (params.difficulty_min, params.difficulty_max)
    {
        if difficulty_min > difficulty_max {
            return Err(anyhow!(
                "difficulty_min must be less than or equal to difficulty_max"
            ));
        }
    }
    if let Some(q) = &params.q {
        if q.trim().is_empty() {
            return Err(anyhow!("q must not be empty"));
        }
    }
    if let Some(v) = &params.created_after {
        validate_timestamp_param("created_after", v)?;
    }
    if let Some(v) = &params.created_before {
        validate_timestamp_param("created_before", v)?;
    }
    if let Some(v) = &params.updated_after {
        validate_timestamp_param("updated_after", v)?;
    }
    if let Some(v) = &params.updated_before {
        validate_timestamp_param("updated_before", v)?;
    }
    if let Some(v) = &params.assigned_reviewer_id {
        uuid::Uuid::parse_str(v.trim())
            .map_err(|_| anyhow!("assigned_reviewer_id must be a valid UUID"))?;
    }
    Ok(())
}

pub(crate) async fn execute_questions_query(
    pool: &PgPool,
    plan: &mut QuestionsQueryPlan<'_>,
) -> Result<Vec<PgRow>, sqlx::Error> {
    plan.builder.build().fetch_all(pool).await
}

// ---------------------------------------------------------------------------
// Batch loading helpers (eliminate N+1 queries for list endpoints)
// ---------------------------------------------------------------------------

pub(crate) async fn load_question_tags_batch(
    pool: &PgPool,
    question_ids: &[String],
) -> Result<HashMap<String, Vec<String>>, sqlx::Error> {
    if question_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT question_id::text AS question_id, tag FROM question_tags WHERE question_id IN (",
    );
    push_uuid_list(&mut builder, question_ids);
    builder.push(") ORDER BY question_id, sort_order, tag");

    let rows = builder.build().fetch_all(pool).await?;
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for row in rows {
        let qid: String = row.get("question_id");
        let tag: String = row.get("tag");
        map.entry(qid).or_default().push(tag);
    }
    Ok(map)
}

pub(crate) async fn load_question_difficulties_batch(
    pool: &PgPool,
    question_ids: &[String],
) -> Result<HashMap<String, QuestionDifficulty>, sqlx::Error> {
    if question_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT qd.question_id::text AS question_id, qd.algorithm_tag, qd.score, qd.notes, qd.updated_by::text AS updated_by_id, u.username AS updated_by_username, u.display_name AS updated_by_display_name FROM question_difficulties qd LEFT JOIN users u ON u.user_id = qd.updated_by WHERE qd.question_id IN (",
    );
    push_uuid_list(&mut builder, question_ids);
    builder.push(") ORDER BY qd.question_id, qd.algorithm_tag");

    let rows = builder.build().fetch_all(pool).await?;
    let mut map: HashMap<String, BTreeMap<String, QuestionDifficultyValue>> = HashMap::new();
    for row in rows {
        let qid: String = row.get("question_id");
        let tag: String = row.get("algorithm_tag");
        let updated_by =
            row.get::<Option<String>, _>("updated_by_id")
                .map(|uid| DifficultyEditor {
                    user_id: uid,
                    username: row
                        .get::<Option<String>, _>("updated_by_username")
                        .unwrap_or_default(),
                    display_name: row
                        .get::<Option<String>, _>("updated_by_display_name")
                        .unwrap_or_default(),
                });
        map.entry(qid).or_default().insert(
            tag,
            QuestionDifficultyValue {
                score: row.get("score"),
                notes: row.get("notes"),
                updated_by,
            },
        );
    }
    Ok(map
        .into_iter()
        .map(|(qid, entries)| (qid, QuestionDifficulty { entries }))
        .collect())
}

fn push_uuid_list<'a>(builder: &mut QueryBuilder<'a, Postgres>, ids: &'a [String]) {
    for (idx, id) in ids.iter().enumerate() {
        if idx > 0 {
            builder.push(", ");
        }
        builder.push_bind(id).push("::uuid");
    }
}

pub(crate) async fn load_question_tags(
    pool: &PgPool,
    question_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    query("SELECT tag FROM question_tags WHERE question_id = $1::uuid ORDER BY sort_order, tag")
        .bind(question_id)
        .fetch_all(pool)
        .await
        .map(|rows| {
            rows.into_iter()
                .map(|row| row.get::<String, _>("tag"))
                .collect()
        })
}

pub(crate) async fn list_active_question_tags(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    query(
        "
        SELECT DISTINCT qt.tag
        FROM question_tags qt
        JOIN questions q ON q.question_id = qt.question_id
        WHERE q.deleted_at IS NULL
        ORDER BY qt.tag
        ",
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| row.get::<String, _>("tag"))
            .collect()
    })
}

pub(crate) async fn list_active_difficulty_tags(pool: &PgPool) -> Result<Vec<String>, sqlx::Error> {
    query(
        "
        SELECT DISTINCT qd.algorithm_tag
        FROM question_difficulties qd
        JOIN questions q ON q.question_id = qd.question_id
        WHERE q.deleted_at IS NULL
        ORDER BY qd.algorithm_tag
        ",
    )
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| row.get::<String, _>("algorithm_tag"))
            .collect()
    })
}

pub(crate) async fn load_question_difficulties(
    pool: &PgPool,
    question_id: &str,
) -> Result<QuestionDifficulty, sqlx::Error> {
    query(
        "SELECT qd.algorithm_tag, qd.score, qd.notes, qd.updated_by::text AS updated_by_id, u.username AS updated_by_username, u.display_name AS updated_by_display_name FROM question_difficulties qd LEFT JOIN users u ON u.user_id = qd.updated_by WHERE qd.question_id = $1::uuid ORDER BY qd.algorithm_tag",
    )
    .bind(question_id)
    .fetch_all(pool)
    .await
    .map(|rows| QuestionDifficulty {
        entries: rows
            .into_iter()
            .map(|row| {
                let updated_by = row.get::<Option<String>, _>("updated_by_id").map(|uid| {
                    DifficultyEditor {
                        user_id: uid,
                        username: row.get::<Option<String>, _>("updated_by_username").unwrap_or_default(),
                        display_name: row.get::<Option<String>, _>("updated_by_display_name").unwrap_or_default(),
                    }
                });
                (
                    row.get("algorithm_tag"),
                    QuestionDifficultyValue {
                        score: row.get("score"),
                        notes: row.get("notes"),
                        updated_by,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>(),
    })
}

pub(crate) async fn load_question_files(
    pool: &PgPool,
    question_id: &str,
    file_kind: &str,
) -> Result<Vec<QuestionAssetRef>, sqlx::Error> {
    query(
        r#"
        SELECT qf.file_path, qf.file_kind, qf.mime_type, qf.object_id::text AS object_id
        FROM question_files qf
        WHERE qf.question_id = $1::uuid AND qf.file_kind = $2
        ORDER BY qf.file_path
        "#,
    )
    .bind(question_id)
    .bind(file_kind)
    .fetch_all(pool)
    .await
    .map(|rows| {
        rows.into_iter()
            .map(|row| QuestionAssetRef {
                path: row.get("file_path"),
                file_kind: row.get("file_kind"),
                object_id: row.get("object_id"),
                mime_type: row.get("mime_type"),
            })
            .collect()
    })
}

pub(crate) fn map_question_summary(
    row: PgRow,
    tags: Vec<String>,
    difficulty: QuestionDifficulty,
) -> QuestionSummary {
    QuestionSummary {
        question_id: row.get("question_id"),
        source: QuestionSourceRef {
            tex: row.get("source_tex_path"),
        },
        category: row.get("category"),
        status: row.get("status"),
        description: row.get("description"),
        score: row.get("score"),
        author: row.get("author"),
        reviewers: row.get("reviewers"),
        tags,
        difficulty,
        allow_auto_reviewer: row.get("allow_auto_reviewer"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn map_question_paper_ref(row: PgRow) -> QuestionPaperRef {
    QuestionPaperRef {
        paper_id: row.get("paper_id"),
        description: row.get("description"),
        title: row.get("title"),
        subtitle: row.get("subtitle"),
        sort_order: row.get("sort_order"),
    }
}

pub(crate) fn map_question_detail(
    row: PgRow,
    tex_object_id: String,
    tags: Vec<String>,
    difficulty: QuestionDifficulty,
    assets: Vec<QuestionAssetRef>,
    papers: Vec<QuestionPaperRef>,
) -> QuestionDetail {
    QuestionDetail {
        question_id: row.get("question_id"),
        tex_object_id,
        source: QuestionSourceRef {
            tex: row.get("source_tex_path"),
        },
        category: row.get("category"),
        status: row.get("status"),
        description: row.get("description"),
        score: row.get("score"),
        author: row.get("author"),
        reviewers: row.get("reviewers"),
        tags,
        difficulty,
        allow_auto_reviewer: row.get("allow_auto_reviewer"),
        created_by: row.get("created_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        assets,
        papers,
    }
}
