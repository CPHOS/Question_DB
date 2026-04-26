use anyhow::{Context, Result};
use sqlx::{query, PgPool, Postgres, QueryBuilder, Row};

use super::models::{
    admin_paper_detail, admin_paper_summary, admin_question_detail, admin_question_summary,
    AdminPaperDetail, AdminPaperSummary, AdminPapersParams, AdminQuestionDetail,
    AdminQuestionSummary, AdminQuestionsParams, GarbageCollectionResponse, RecordState,
};
use crate::api::{
    papers::queries::{ensure_paper_questions_valid, map_paper_summary},
    questions::queries::{
        load_question_difficulties_batch, load_question_tags_batch, map_question_summary,
    },
    shared::{
        db::ObjectStore,
        details::{load_paper_detail, load_question_detail, DetailVisibility, TIMESTAMP_SQL},
        error::{ConflictError, NotFoundError},
        utils::escape_ilike,
    },
};

pub(crate) async fn list_admin_questions(
    pool: &PgPool,
    params: &AdminQuestionsParams,
    state: RecordState,
) -> Result<(Vec<AdminQuestionSummary>, i64)> {
    let mut builder = QueryBuilder::<Postgres>::new(&format!(
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
               to_char(q.created_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS created_at,
               to_char(q.updated_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS updated_at,
               to_char(q.deleted_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS deleted_at,
               q.deleted_by,
               COUNT(*) OVER() AS total_count
        FROM questions q
        WHERE 1 = 1"
    ));

    push_question_state_filter(&mut builder, state);

    if let Some(category) = &params.category {
        builder.push(" AND q.category = ").push_bind(category);
    }
    if let Some(author) = &params.author {
        builder.push(" AND q.author = ").push_bind(author);
    }
    if let Some(tag) = &params.tag {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM question_tags qt WHERE qt.question_id = q.question_id AND qt.tag = ",
            )
            .push_bind(tag)
            .push(")");
    }
    if let Some(reviewer) = &params.reviewer {
        builder
            .push(" AND q.reviewers @> ARRAY[")
            .push_bind(reviewer)
            .push("]::text[]");
    }
    if let Some(assigned_reviewer_id) = &params.assigned_reviewer_id {
        builder
            .push(" AND EXISTS (SELECT 1 FROM question_reviews qr WHERE qr.question_id = q.question_id AND qr.reviewer_id = ")
            .push_bind(assigned_reviewer_id)
            .push("::uuid)");
    }
    if let Some(score_min) = params.score_min {
        builder.push(" AND q.score >= ").push_bind(score_min);
    }
    if let Some(score_max) = params.score_max {
        builder.push(" AND q.score <= ").push_bind(score_max);
    }
    if let Some(difficulty_tag) = &params.difficulty_tag {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM question_difficulties qd WHERE qd.question_id = q.question_id AND qd.algorithm_tag = ",
            )
            .push_bind(difficulty_tag);
        if let Some(difficulty_min) = params.difficulty_min {
            builder.push(" AND qd.score >= ").push_bind(difficulty_min);
        }
        if let Some(difficulty_max) = params.difficulty_max {
            builder.push(" AND qd.score <= ").push_bind(difficulty_max);
        }
        builder.push(")");
    }
    if let Some(paper_id) = &params.paper_id {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq WHERE pq.question_id = q.question_id AND pq.paper_id = ",
            )
            .push_bind(paper_id)
            .push("::uuid)");
    }
    if let Some(search) = &params.q {
        let needle = format!("%{}%", escape_ilike(search));
        builder
            .push(" AND COALESCE(q.description, '') ILIKE ")
            .push_bind(needle);
    }
    if let Some(created_after) = &params.created_after {
        builder
            .push(" AND q.created_at >= ")
            .push_bind(created_after)
            .push("::timestamptz");
    }
    if let Some(created_before) = &params.created_before {
        builder
            .push(" AND q.created_at <= ")
            .push_bind(created_before)
            .push("::timestamptz");
    }
    if let Some(updated_after) = &params.updated_after {
        builder
            .push(" AND q.updated_at >= ")
            .push_bind(updated_after)
            .push("::timestamptz");
    }
    if let Some(updated_before) = &params.updated_before {
        builder
            .push(" AND q.updated_at <= ")
            .push_bind(updated_before)
            .push("::timestamptz");
    }

    builder
        .push(" ORDER BY q.created_at DESC, q.question_id LIMIT ")
        .push_bind(params.normalized_limit())
        .push(" OFFSET ")
        .push_bind(params.normalized_offset());

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("query admin questions failed")?;

    let total = rows
        .first()
        .map(|r| r.get::<i64, _>("total_count"))
        .unwrap_or(0);
    let question_ids: Vec<String> = rows.iter().map(|r| r.get("question_id")).collect();
    let tags_map = load_question_tags_batch(pool, &question_ids)
        .await
        .context("load admin question tags failed")?;
    let difficulty_map = load_question_difficulties_batch(pool, &question_ids)
        .await
        .context("load admin question difficulties failed")?;

    let mut questions = Vec::with_capacity(rows.len());
    for row in rows {
        let question_id: String = row.get("question_id");
        let deleted_at: Option<String> = row.get("deleted_at");
        let deleted_by: Option<String> = row.get("deleted_by");
        let tags = tags_map.get(&question_id).cloned().unwrap_or_default();
        let difficulty = difficulty_map
            .get(&question_id)
            .cloned()
            .unwrap_or_default();
        questions.push(admin_question_summary(
            map_question_summary(row, tags, difficulty),
            deleted_at,
            deleted_by,
        ));
    }

    Ok((questions, total))
}

pub(crate) async fn list_admin_papers(
    pool: &PgPool,
    params: &AdminPapersParams,
    state: RecordState,
) -> Result<(Vec<AdminPaperSummary>, i64)> {
    let mut builder = QueryBuilder::<Postgres>::new(&format!(
        "
        SELECT p.paper_id::text AS paper_id,
               p.description,
               p.title,
               p.subtitle,
               COUNT(pq_count.question_id) AS question_count,
               p.created_by::text AS created_by,
               to_char(p.created_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS created_at,
               to_char(p.updated_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS updated_at,
               to_char(p.deleted_at AT TIME ZONE 'UTC', {TIMESTAMP_SQL}) AS deleted_at,
               p.deleted_by
        FROM papers p
        LEFT JOIN paper_questions pq_count ON pq_count.paper_id = p.paper_id
        WHERE 1 = 1"
    ));

    push_paper_state_filter(&mut builder, state);

    if let Some(question_id) = &params.question_id {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq WHERE pq.paper_id = p.paper_id AND pq.question_id = ",
            )
            .push_bind(question_id)
            .push("::uuid)");
    }
    if let Some(category) = &params.category {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.category = ",
            )
            .push_bind(category)
            .push(')');
    }
    if let Some(tag) = &params.tag {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq JOIN question_tags qt ON qt.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND qt.tag = ",
            )
            .push_bind(tag)
            .push(')');
    }
    if let Some(search) = &params.q {
        let needle = format!("%{}%", escape_ilike(search));
        builder
            .push(" AND CONCAT_WS(' ', p.description, p.title, p.subtitle) ILIKE ")
            .push_bind(needle);
    }
    if let Some(created_after) = &params.created_after {
        builder
            .push(" AND p.created_at >= ")
            .push_bind(created_after)
            .push("::timestamptz");
    }
    if let Some(created_before) = &params.created_before {
        builder
            .push(" AND p.created_at <= ")
            .push_bind(created_before)
            .push("::timestamptz");
    }
    if let Some(updated_after) = &params.updated_after {
        builder
            .push(" AND p.updated_at >= ")
            .push_bind(updated_after)
            .push("::timestamptz");
    }
    if let Some(updated_before) = &params.updated_before {
        builder
            .push(" AND p.updated_at <= ")
            .push_bind(updated_before)
            .push("::timestamptz");
    }

    builder
        .push(
            " GROUP BY p.paper_id, p.description, p.title, p.subtitle, p.created_by, p.created_at, p.updated_at, p.deleted_at, p.deleted_by",
        )
        .push(" ORDER BY p.created_at DESC, p.paper_id LIMIT ")
        .push_bind(params.normalized_limit())
        .push(" OFFSET ")
        .push_bind(params.normalized_offset());

    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .context("query admin papers failed")?;

    let total = count_admin_papers(pool, params, state)
        .await
        .context("count admin papers failed")?;

    Ok((
        rows.into_iter()
            .map(|row| {
                let deleted_at: Option<String> = row.get("deleted_at");
                let deleted_by: Option<String> = row.get("deleted_by");
                admin_paper_summary(map_paper_summary(row), deleted_at, deleted_by)
            })
            .collect(),
        total,
    ))
}

pub(crate) async fn load_admin_question_detail(
    pool: &PgPool,
    question_id: &str,
) -> Result<AdminQuestionDetail> {
    let loaded = load_question_detail(
        pool,
        question_id,
        DetailVisibility::All,
        DetailVisibility::All,
    )
    .await?;

    Ok(admin_question_detail(
        loaded.detail,
        loaded.deleted_at,
        loaded.deleted_by,
    ))
}

pub(crate) async fn load_admin_paper_detail(
    pool: &PgPool,
    paper_id: &str,
) -> Result<AdminPaperDetail> {
    let loaded =
        load_paper_detail(pool, paper_id, DetailVisibility::All, DetailVisibility::All).await?;

    Ok(admin_paper_detail(
        loaded.detail,
        loaded.deleted_at,
        loaded.deleted_by,
    ))
}

pub(crate) async fn restore_question(
    pool: &PgPool,
    question_id: &str,
) -> Result<AdminQuestionDetail> {
    let mut tx = pool
        .begin()
        .await
        .context("begin admin question restore tx failed")?;

    let row = query(
        "SELECT (deleted_at IS NOT NULL) AS is_deleted FROM questions WHERE question_id = $1::uuid FOR UPDATE",
    )
        .bind(question_id)
        .fetch_optional(&mut *tx)
        .await
        .context("lock question row for restore failed")?
        .ok_or_else(|| NotFoundError(format!("question not found: {question_id}")))?;

    let is_deleted: bool = row.get("is_deleted");
    if !is_deleted {
        return Err(ConflictError(format!("question is not deleted: {question_id}")).into());
    }

    query(
        "UPDATE questions SET deleted_at = NULL, deleted_by = NULL, updated_at = NOW() WHERE question_id = $1::uuid",
    )
    .bind(question_id)
    .execute(&mut *tx)
    .await
    .context("restore question failed")?;

    tx.commit()
        .await
        .context("commit question restore failed")?;

    load_admin_question_detail(pool, question_id).await
}

pub(crate) async fn restore_paper(pool: &PgPool, paper_id: &str) -> Result<AdminPaperDetail> {
    let mut tx = pool
        .begin()
        .await
        .context("begin admin paper restore tx failed")?;

    let row = query(
        "SELECT (deleted_at IS NOT NULL) AS is_deleted FROM papers WHERE paper_id = $1::uuid FOR UPDATE",
    )
        .bind(paper_id)
        .fetch_optional(&mut *tx)
        .await
        .context("lock paper row for restore failed")?
        .ok_or_else(|| NotFoundError(format!("paper not found: {paper_id}")))?;

    let is_deleted: bool = row.get("is_deleted");
    if !is_deleted {
        return Err(ConflictError(format!("paper is not deleted: {paper_id}")).into());
    }

    let question_ids = query(
        "SELECT question_id::text AS question_id FROM paper_questions WHERE paper_id = $1::uuid ORDER BY sort_order",
    )
    .bind(paper_id)
    .fetch_all(&mut *tx)
    .await
    .context("load paper question refs for restore failed")?
    .into_iter()
    .map(|row| row.get::<String, _>("question_id"))
    .collect::<Vec<_>>();

    // Check whether any referenced question has been soft-deleted — this is a
    // state conflict rather than a validation error.
    if !question_ids.is_empty() {
        let mut qb = QueryBuilder::<Postgres>::new(
            "SELECT question_id::text AS question_id FROM questions WHERE deleted_at IS NOT NULL AND question_id IN (",
        );
        for (idx, qid) in question_ids.iter().enumerate() {
            if idx > 0 {
                qb.push(", ");
            }
            qb.push_bind(qid).push("::uuid");
        }
        qb.push(")");
        let deleted_rows = qb
            .build()
            .fetch_all(&mut *tx)
            .await
            .context("check deleted question refs failed")?;
        if !deleted_rows.is_empty() {
            let deleted_ids: Vec<String> =
                deleted_rows.iter().map(|r| r.get("question_id")).collect();
            return Err(ConflictError(format!(
                "cannot restore paper: referenced questions are deleted: {}",
                deleted_ids.join(", ")
            ))
            .into());
        }
    }

    ensure_paper_questions_valid(&mut *tx, &question_ids).await?;

    query(
        "UPDATE papers SET deleted_at = NULL, deleted_by = NULL, updated_at = NOW() WHERE paper_id = $1::uuid",
    )
    .bind(paper_id)
    .execute(&mut *tx)
    .await
    .context("restore paper failed")?;

    tx.commit().await.context("commit paper restore failed")?;

    load_admin_paper_detail(pool, paper_id).await
}

pub(crate) async fn preview_garbage_collection(pool: &PgPool) -> Result<GarbageCollectionResponse> {
    execute_garbage_collection(pool, true, None).await
}

pub(crate) async fn run_garbage_collection(
    pool: &PgPool,
    object_store: &ObjectStore,
) -> Result<GarbageCollectionResponse> {
    execute_garbage_collection(pool, false, Some(object_store)).await
}

async fn execute_garbage_collection(
    pool: &PgPool,
    dry_run: bool,
    object_store: Option<&ObjectStore>,
) -> Result<GarbageCollectionResponse> {
    let mut tx = pool
        .begin()
        .await
        .context("begin garbage collection tx failed")?;

    let deleted_papers = query(
        "DELETE FROM papers p WHERE p.deleted_at IS NOT NULL RETURNING p.paper_id::text AS paper_id",
    )
    .fetch_all(&mut *tx)
    .await
    .context("purge deleted papers failed")?
    .len();

    let deleted_questions = query(
        r#"
        DELETE FROM questions q
        WHERE q.deleted_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM paper_questions pq
              JOIN papers p ON p.paper_id = pq.paper_id
              WHERE pq.question_id = q.question_id AND p.deleted_at IS NULL
          )
        RETURNING q.question_id::text AS question_id
        "#,
    )
    .fetch_all(&mut *tx)
    .await
    .context("purge deleted questions failed")?
    .len();

    let deleted_object_rows = query(
        r#"
        DELETE FROM objects o
        WHERE NOT EXISTS (
                  SELECT 1 FROM question_files qf WHERE qf.object_id = o.object_id
              )
          AND NOT EXISTS (
                  SELECT 1 FROM papers p WHERE p.append_object_id = o.object_id
              )
        RETURNING o.size_bytes, o.storage_path
        "#,
    )
    .fetch_all(&mut *tx)
    .await
    .context("purge orphaned objects failed")?;
    let deleted_objects = deleted_object_rows.len();
    let freed_bytes = deleted_object_rows
        .iter()
        .map(|row| row.get::<i64, _>("size_bytes"))
        .sum();
    let deleted_storage_paths: Vec<String> = deleted_object_rows
        .iter()
        .filter_map(|row| row.get::<Option<String>, _>("storage_path"))
        .collect();

    if dry_run {
        tx.rollback()
            .await
            .context("rollback garbage collection preview failed")?;
    } else {
        tx.commit()
            .await
            .context("commit garbage collection failed")?;
        // Clean up object files from disk after successful commit.
        if let Some(store) = object_store {
            store.delete_object_files(&deleted_storage_paths);
        }
    }

    Ok(GarbageCollectionResponse {
        dry_run,
        deleted_questions,
        deleted_papers,
        deleted_objects,
        freed_bytes,
    })
}

fn push_question_state_filter(builder: &mut QueryBuilder<'_, Postgres>, state: RecordState) {
    match state {
        RecordState::Active => {
            builder.push(" AND q.deleted_at IS NULL");
        }
        RecordState::Deleted => {
            builder.push(" AND q.deleted_at IS NOT NULL");
        }
        RecordState::All => {}
    }
}

fn push_paper_state_filter(builder: &mut QueryBuilder<'_, Postgres>, state: RecordState) {
    match state {
        RecordState::Active => {
            builder.push(" AND p.deleted_at IS NULL");
        }
        RecordState::Deleted => {
            builder.push(" AND p.deleted_at IS NOT NULL");
        }
        RecordState::All => {}
    }
}

async fn count_admin_papers(
    pool: &PgPool,
    params: &AdminPapersParams,
    state: RecordState,
) -> Result<i64> {
    let mut builder = QueryBuilder::<Postgres>::new(
        "SELECT COUNT(DISTINCT p.paper_id) AS total FROM papers p WHERE 1 = 1",
    );
    push_paper_state_filter(&mut builder, state);
    if let Some(question_id) = &params.question_id {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq WHERE pq.paper_id = p.paper_id AND pq.question_id = ",
            )
            .push_bind(question_id)
            .push("::uuid)");
    }
    if let Some(category) = &params.category {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq JOIN questions q ON q.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND q.category = ",
            )
            .push_bind(category)
            .push(')');
    }
    if let Some(tag) = &params.tag {
        builder
            .push(
                " AND EXISTS (SELECT 1 FROM paper_questions pq JOIN question_tags qt ON qt.question_id = pq.question_id WHERE pq.paper_id = p.paper_id AND qt.tag = ",
            )
            .push_bind(tag)
            .push(')');
    }
    if let Some(search) = &params.q {
        let needle = format!("%{}%", escape_ilike(search));
        builder
            .push(" AND CONCAT_WS(' ', p.description, p.title, p.subtitle) ILIKE ")
            .push_bind(needle);
    }
    if let Some(created_after) = &params.created_after {
        builder
            .push(" AND p.created_at >= ")
            .push_bind(created_after)
            .push("::timestamptz");
    }
    if let Some(created_before) = &params.created_before {
        builder
            .push(" AND p.created_at <= ")
            .push_bind(created_before)
            .push("::timestamptz");
    }
    if let Some(updated_after) = &params.updated_after {
        builder
            .push(" AND p.updated_at >= ")
            .push_bind(updated_after)
            .push("::timestamptz");
    }
    if let Some(updated_before) = &params.updated_before {
        builder
            .push(" AND p.updated_at <= ")
            .push_bind(updated_before)
            .push("::timestamptz");
    }
    let row = builder
        .build()
        .fetch_one(pool)
        .await
        .context("count admin papers failed")?;
    Ok(row.get("total"))
}
