use std::collections::HashSet;

use anyhow::{Context, Result};
use sqlx::{postgres::PgRow, PgPool, Postgres, QueryBuilder, Row};

use super::models::{validate_paper_filters, PapersParams};
use crate::api::shared::{error::ValidationError, utils::escape_ilike};

/// Returned from `build_query` with parameter bindings already attached.
pub(crate) struct PapersQueryPlan<'a> {
    pub(crate) builder: QueryBuilder<'a, Postgres>,
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl PapersParams {
    pub(crate) fn build_query(&self) -> PapersQueryPlan<'_> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "
            SELECT p.paper_id::text AS paper_id,
                   p.description,
                   p.title,
                   p.subtitle,

                   COUNT(pq_count.question_id) AS question_count,
                   to_char(p.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at,
                   to_char(p.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS updated_at
            FROM papers p
            LEFT JOIN paper_questions pq_count ON pq_count.paper_id = p.paper_id
            WHERE p.deleted_at IS NULL",
        );

        if let Some(question_id) = &self.question_id {
            builder
                .push(" AND EXISTS (SELECT 1 FROM paper_questions pq WHERE pq.paper_id = p.paper_id AND pq.question_id = ")
                .push_bind(question_id)
                .push("::uuid)");
        }
        if let Some(category) = &self.category {
            builder
                .push(" AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.deleted_at IS NULL AND q.category = ")
                .push_bind(category)
                .push(')');
        }
        if let Some(tag) = &self.tag {
            builder
                .push(" AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id JOIN question_tags qt ON qt.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.deleted_at IS NULL AND qt.tag = ")
                .push_bind(tag)
                .push(')');
        }
        if let Some(search) = &self.q {
            let needle = format!("%{}%", escape_ilike(search));
            builder
                .push(" AND CONCAT_WS(' ', p.description, p.title, p.subtitle) ILIKE ")
                .push_bind(needle);
        }

        let limit = self.normalized_limit();
        let offset = self.normalized_offset();
        builder
            .push(
                " GROUP BY p.paper_id, p.description, p.title, p.subtitle, p.created_at, p.updated_at",
            )
            .push(" ORDER BY p.created_at DESC, p.paper_id LIMIT ")
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(offset);

        PapersQueryPlan {
            builder,
            limit,
            offset,
        }
    }
}

pub(crate) async fn execute_papers_query(
    pool: &PgPool,
    plan: &mut PapersQueryPlan<'_>,
) -> Result<Vec<PgRow>, sqlx::Error> {
    plan.builder.build().fetch_all(pool).await
}

pub(crate) async fn count_papers(pool: &PgPool, params: &PapersParams) -> Result<i64> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT COUNT(DISTINCT p.paper_id) AS total FROM papers p WHERE p.deleted_at IS NULL",
    );
    push_paper_filters(&mut builder, params);
    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .context("count papers failed")?;
    Ok(row.get("total"))
}

pub(crate) fn validate_and_build_papers_query(
    params: &PapersParams,
) -> Result<PapersQueryPlan<'_>> {
    validate_paper_filters(params)?;
    Ok(params.build_query())
}

pub(crate) fn map_paper_summary(row: PgRow) -> super::models::PaperSummary {
    super::models::PaperSummary {
        paper_id: row.get("paper_id"),
        description: row.get("description"),
        title: row.get("title"),
        subtitle: row.get("subtitle"),
        question_count: row.get("question_count"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

pub(crate) fn map_paper_detail(
    row: PgRow,
    questions: Vec<crate::api::questions::models::QuestionSummary>,
) -> super::models::PaperDetail {
    super::models::PaperDetail {
        paper_id: row.get("paper_id"),
        description: row.get("description"),
        title: row.get("title"),
        subtitle: row.get("subtitle"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        questions,
    }
}

fn push_paper_filters<'a>(builder: &mut QueryBuilder<'a, Postgres>, params: &'a PapersParams) {
    if let Some(question_id) = &params.question_id {
        builder
            .push(" AND EXISTS (SELECT 1 FROM paper_questions pq WHERE pq.paper_id = p.paper_id AND pq.question_id = ")
            .push_bind(question_id)
            .push("::uuid)");
    }
    if let Some(category) = &params.category {
        builder
            .push(" AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.deleted_at IS NULL AND q.category = ")
            .push_bind(category)
            .push(')');
    }
    if let Some(tag) = &params.tag {
        builder
            .push(" AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id JOIN question_tags qt ON qt.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.deleted_at IS NULL AND qt.tag = ")
            .push_bind(tag)
            .push(')');
    }
    if let Some(search) = &params.q {
        let needle = format!("%{}%", escape_ilike(search));
        builder
            .push(" AND CONCAT_WS(' ', p.description, p.title, p.subtitle) ILIKE ")
            .push_bind(needle);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PaperQuestionValidationRow {
    pub(crate) question_id: String,
    pub(crate) category: String,
    pub(crate) status: String,
}

pub(crate) async fn ensure_paper_questions_valid<'e, E>(
    executor: E,
    question_ids: &[String],
) -> Result<()>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let question_rows = load_paper_question_validation_rows(executor, question_ids).await?;
    validate_paper_question_rows(&question_rows)
}

async fn load_paper_question_validation_rows<'e, E>(
    executor: E,
    question_ids: &[String],
) -> Result<Vec<PaperQuestionValidationRow>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if question_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT q.question_id::text AS question_id, q.category, q.status FROM questions q WHERE q.question_id IN (",
    );
    for (idx, question_id) in question_ids.iter().enumerate() {
        if idx > 0 {
            builder.push(", ");
        }
        builder.push_bind(question_id).push("::uuid");
    }
    builder.push(") AND q.deleted_at IS NULL");

    let question_rows = builder
        .build()
        .fetch_all(executor)
        .await
        .context("load questions for paper validation failed")?
        .into_iter()
        .map(|row| PaperQuestionValidationRow {
            question_id: row.get("question_id"),
            category: row.get("category"),
            status: row.get("status"),
        })
        .collect::<Vec<_>>();

    let existing_question_ids = question_rows
        .iter()
        .map(|row| row.question_id.as_str())
        .collect::<HashSet<_>>();

    for question_id in question_ids {
        if !existing_question_ids.contains(question_id.as_str()) {
            return Err(ValidationError(format!(
                "unknown question_id in question_ids: {question_id}"
            ))
            .into());
        }
    }

    Ok(question_rows)
}

pub(crate) fn validate_paper_question_rows(
    question_rows: &[PaperQuestionValidationRow],
) -> Result<()> {
    let mut expected_category = None;

    for row in question_rows {
        match row.category.as_str() {
            "T" | "E" => {}
            other => {
                return Err(ValidationError(format!(
                    "question {} has category {other}; paper questions must all have category T or all have category E",
                    row.question_id
                )).into());
            }
        }

        if let Some(expected) = expected_category {
            if expected != row.category {
                return Err(ValidationError(format!(
                    "paper questions must all have the same category; found both {expected} and {}",
                    row.category
                ))
                .into());
            }
        } else {
            expected_category = Some(row.category.as_str());
        }

        if !matches!(row.status.as_str(), "reviewed" | "used") {
            return Err(ValidationError(format!(
                "question {} has status {}; paper questions must all have status reviewed or used",
                row.question_id, row.status
            ))
            .into());
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_paper_question_rows, PaperQuestionValidationRow};

    fn row(question_id: &str, category: &str, status: &str) -> PaperQuestionValidationRow {
        PaperQuestionValidationRow {
            question_id: question_id.to_string(),
            category: category.to_string(),
            status: status.to_string(),
        }
    }

    #[test]
    fn paper_question_validation_accepts_uniform_category_with_reviewed_or_used_status() {
        let rows = vec![row("q1", "T", "reviewed"), row("q2", "T", "used")];

        validate_paper_question_rows(&rows).expect("paper questions should validate");
    }

    #[test]
    fn paper_question_validation_rejects_none_category() {
        let err = validate_paper_question_rows(&[row("q1", "none", "reviewed")])
            .expect_err("none category should be rejected");

        assert!(err
            .to_string()
            .contains("paper questions must all have category T or all have category E"));
    }

    #[test]
    fn paper_question_validation_rejects_mixed_categories() {
        let err =
            validate_paper_question_rows(&[row("q1", "T", "reviewed"), row("q2", "E", "used")])
                .expect_err("mixed categories should be rejected");

        assert!(err
            .to_string()
            .contains("paper questions must all have the same category"));
    }

    #[test]
    fn paper_question_validation_rejects_none_status() {
        let err = validate_paper_question_rows(&[row("q1", "E", "none")])
            .expect_err("none status should be rejected");

        assert!(err
            .to_string()
            .contains("paper questions must all have status reviewed or used"));
    }
}
