use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, query, PgPool, Postgres, QueryBuilder, Row};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
}

#[derive(Debug, Serialize)]
pub struct PaperSummary {
    paper_id: String,
    edition: String,
    paper_type: String,
    title: String,
    paper_tex_object_id: Option<String>,
    source_pdf_object_id: Option<String>,
    question_index: Value,
    notes: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PaperQuestionSummary {
    question_id: String,
    paper_index: i32,
    question_no: Option<String>,
    category: String,
    question_tex_object_id: Option<String>,
    answer_tex_object_id: Option<String>,
    status: String,
    tags: Value,
}

#[derive(Debug, Serialize)]
pub struct PaperDetail {
    paper_id: String,
    edition: String,
    paper_type: String,
    title: String,
    paper_tex_object_id: Option<String>,
    source_pdf_object_id: Option<String>,
    question_index: Value,
    notes: Option<String>,
    created_at: String,
    updated_at: String,
    questions: Vec<PaperQuestionSummary>,
}

#[derive(Debug, Serialize)]
pub struct QuestionSummary {
    question_id: String,
    paper_id: String,
    paper_index: i32,
    question_no: Option<String>,
    category: String,
    status: String,
    search_text: Option<String>,
    question_tex_object_id: Option<String>,
    answer_tex_object_id: Option<String>,
    tags: Value,
    edition: String,
    paper_type: String,
    title: String,
}

#[derive(Debug, Serialize)]
pub struct QuestionAsset {
    asset_id: String,
    kind: String,
    object_id: String,
    caption: Option<String>,
    sort_order: i32,
}

#[derive(Debug, Serialize)]
pub struct QuestionStat {
    exam_session: String,
    source_workbook_id: Option<String>,
    participant_count: i32,
    avg_score: f64,
    score_std: f64,
    full_mark_rate: f64,
    zero_score_rate: f64,
    max_score: f64,
    min_score: f64,
    stats_source: String,
    stats_version: String,
}

#[derive(Debug, Serialize)]
pub struct DifficultyScore {
    exam_session: Option<String>,
    manual_level: Option<String>,
    derived_score: Option<f64>,
    method: String,
    method_version: String,
    confidence: Option<f64>,
    feature_json: Value,
}

#[derive(Debug, Serialize)]
pub struct ScoreWorkbookSummary {
    workbook_id: String,
    paper_id: String,
    exam_session: String,
    workbook_kind: String,
    workbook_object_id: String,
    source_filename: String,
    mime_type: Option<String>,
    sheet_names: Value,
    file_size: i64,
    sha256: String,
    notes: Option<String>,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct QuestionDetail {
    question_id: String,
    paper_id: String,
    paper_index: i32,
    question_no: Option<String>,
    category: String,
    question_tex_object_id: Option<String>,
    answer_tex_object_id: Option<String>,
    search_text: Option<String>,
    status: String,
    tags: Value,
    notes: Option<String>,
    created_at: String,
    updated_at: String,
    edition: String,
    paper_type: String,
    paper_title: String,
    paper_tex_object_id: Option<String>,
    source_pdf_object_id: Option<String>,
    paper_question_index: Value,
    assets: Vec<QuestionAsset>,
    stats: Vec<QuestionStat>,
    difficulty_scores: Vec<DifficultyScore>,
    score_workbooks: Vec<ScoreWorkbookSummary>,
}

#[derive(Debug, Deserialize)]
pub struct QuestionsParams {
    edition: Option<String>,
    paper_id: Option<String>,
    paper_type: Option<String>,
    category: Option<String>,
    has_assets: Option<bool>,
    has_answer: Option<bool>,
    min_avg_score: Option<f64>,
    max_avg_score: Option<f64>,
    tag: Option<String>,
    q: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ScoreWorkbookParams {
    paper_id: Option<String>,
    exam_session: Option<String>,
}

#[derive(Debug)]
struct QuestionsQuery {
    sql: String,
    bind_count: usize,
    limit: i64,
    offset: i64,
}

impl QuestionsParams {
    fn normalized_limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }

    fn normalized_offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }

    fn build_query(&self) -> QuestionsQuery {
        let mut builder = QueryBuilder::<Postgres>::new(
            "
            SELECT q.question_id, q.paper_id, q.paper_index, q.question_no, q.category, q.status,
                   q.search_text, q.question_tex_object_id::text AS question_tex_object_id,
                   q.answer_tex_object_id::text AS answer_tex_object_id, q.tags_json,
                   p.edition, p.paper_type, p.title
            FROM questions q
            JOIN papers p ON p.paper_id = q.paper_id
            WHERE 1 = 1",
        );
        let mut bind_count = 0;

        if let Some(edition) = &self.edition {
            builder.push(" AND p.edition = ").push_bind(edition);
            bind_count += 1;
        }
        if let Some(paper_id) = &self.paper_id {
            builder.push(" AND q.paper_id = ").push_bind(paper_id);
            bind_count += 1;
        }
        if let Some(paper_type) = &self.paper_type {
            builder.push(" AND p.paper_type = ").push_bind(paper_type);
            bind_count += 1;
        }
        if let Some(category) = &self.category {
            builder.push(" AND q.category = ").push_bind(category);
            bind_count += 1;
        }
        if let Some(has_assets) = self.has_assets {
            if has_assets {
                builder.push(" AND EXISTS (SELECT 1 FROM question_assets qa WHERE qa.question_id = q.question_id)");
            } else {
                builder.push(" AND NOT EXISTS (SELECT 1 FROM question_assets qa WHERE qa.question_id = q.question_id)");
            }
        }
        if let Some(has_answer) = self.has_answer {
            if has_answer {
                builder.push(" AND q.answer_tex_object_id IS NOT NULL");
            } else {
                builder.push(" AND q.answer_tex_object_id IS NULL");
            }
        }
        if let Some(min_avg_score) = self.min_avg_score {
            builder
                .push(" AND EXISTS (SELECT 1 FROM question_stats qs WHERE qs.question_id = q.question_id AND qs.avg_score >= ")
                .push_bind(min_avg_score)
                .push(')');
            bind_count += 1;
        }
        if let Some(max_avg_score) = self.max_avg_score {
            builder
                .push(" AND EXISTS (SELECT 1 FROM question_stats qs WHERE qs.question_id = q.question_id AND qs.avg_score <= ")
                .push_bind(max_avg_score)
                .push(')');
            bind_count += 1;
        }
        if let Some(tag) = &self.tag {
            builder
                .push(" AND q.tags_json @> ")
                .push_bind(serde_json::json!([tag]));
            bind_count += 1;
        }
        if let Some(search) = &self.q {
            let needle = format!("%{search}%");
            builder
                .push(" AND (COALESCE(q.search_text, '') ILIKE ")
                .push_bind(needle.clone())
                .push(" OR q.question_id ILIKE ")
                .push_bind(needle.clone())
                .push(" OR COALESCE(q.question_no, '') ILIKE ")
                .push_bind(needle)
                .push(')');
            bind_count += 3;
        }

        let limit = self.normalized_limit();
        let offset = self.normalized_offset();
        builder
            .push(" ORDER BY p.edition DESC, q.paper_id, q.paper_index LIMIT ")
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(offset);

        QuestionsQuery {
            sql: builder.sql().to_owned(),
            bind_count: bind_count + 2,
            limit,
            offset,
        }
    }
}

impl ScoreWorkbookParams {
    fn build_query(&self) -> (String, usize) {
        let mut builder = QueryBuilder::<Postgres>::new(
            "
            SELECT workbook_id, paper_id, exam_session, workbook_kind,
                   workbook_object_id::text AS workbook_object_id, source_filename,
                   mime_type, sheet_names_json, file_size, sha256, notes,
                   to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS created_at,
                   to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"') AS updated_at
            FROM score_workbooks
            WHERE 1 = 1",
        );
        let mut bind_count = 0;
        if let Some(paper_id) = &self.paper_id {
            builder.push(" AND paper_id = ").push_bind(paper_id);
            bind_count += 1;
        }
        if let Some(exam_session) = &self.exam_session {
            builder.push(" AND exam_session = ").push_bind(exam_session);
            bind_count += 1;
        }
        builder.push(" ORDER BY paper_id, exam_session, workbook_id");
        (builder.sql().to_owned(), bind_count)
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/papers", get(list_papers))
        .route("/papers/:paper_id", get(get_paper_detail))
        .route("/questions", get(list_questions))
        .route("/questions/:question_id", get(get_question_detail))
        .route("/score-workbooks", get(list_score_workbooks))
        .route(
            "/score-workbooks/:workbook_id",
            get(get_score_workbook_detail),
        )
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, StatusCode> {
    let pool = &state.pool;

    if let Err(_err) = query("SELECT 1").execute(pool).await {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    Ok(Json(HealthResponse {
        status: "ok",
        service: "qb_api_rust",
    }))
}

async fn list_papers(State(state): State<AppState>) -> Result<Json<Vec<PaperSummary>>, StatusCode> {
    let rows = query(
        r#"
        SELECT paper_id, edition, paper_type, title,
               paper_tex_object_id::text AS paper_tex_object_id,
               source_pdf_object_id::text AS source_pdf_object_id,
               question_index_json, notes
        FROM papers
        ORDER BY edition, paper_id
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let payload = rows.into_iter().map(map_paper_summary).collect();
    Ok(Json(payload))
}

async fn get_paper_detail(
    Path(paper_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<PaperDetail>, StatusCode> {
    let paper_row = query(
        r#"
        SELECT paper_id, edition, paper_type, title,
               paper_tex_object_id::text AS paper_tex_object_id,
               source_pdf_object_id::text AS source_pdf_object_id,
               question_index_json, notes,
               to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM papers
        WHERE paper_id = $1
        "#,
    )
    .bind(&paper_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let question_rows = query(
        r#"
        SELECT question_id, paper_index, question_no, category,
               question_tex_object_id::text AS question_tex_object_id,
               answer_tex_object_id::text AS answer_tex_object_id,
               status, tags_json
        FROM questions
        WHERE paper_id = $1
        ORDER BY paper_index
        "#,
    )
    .bind(&paper_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(PaperDetail {
        paper_id: paper_row.get("paper_id"),
        edition: paper_row.get("edition"),
        paper_type: paper_row.get("paper_type"),
        title: paper_row.get("title"),
        paper_tex_object_id: paper_row.get("paper_tex_object_id"),
        source_pdf_object_id: paper_row.get("source_pdf_object_id"),
        question_index: paper_row.get::<Value, _>("question_index_json"),
        notes: paper_row.get("notes"),
        created_at: paper_row.get("created_at"),
        updated_at: paper_row.get("updated_at"),
        questions: question_rows
            .into_iter()
            .map(map_paper_question_summary)
            .collect(),
    }))
}

async fn list_questions(
    Query(params): Query<QuestionsParams>,
    State(state): State<AppState>,
) -> Result<Json<Vec<QuestionSummary>>, StatusCode> {
    let plan = params.build_query();
    let rows = execute_questions_query(&state.pool, &params, &plan)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let payload = rows.into_iter().map(map_question_summary).collect();
    Ok(Json(payload))
}

async fn get_question_detail(
    Path(question_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<QuestionDetail>, StatusCode> {
    let row = query(
        r#"
        SELECT q.question_id, q.paper_id, q.paper_index, q.question_no, q.category,
               q.question_tex_object_id::text AS question_tex_object_id,
               q.answer_tex_object_id::text AS answer_tex_object_id,
               q.search_text, q.status, q.tags_json, q.notes,
               to_char(q.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(q.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at,
               p.edition, p.paper_type, p.title AS paper_title,
               p.paper_tex_object_id::text AS paper_tex_object_id,
               p.source_pdf_object_id::text AS source_pdf_object_id,
               p.question_index_json
        FROM questions q
        JOIN papers p ON p.paper_id = q.paper_id
        WHERE q.question_id = $1
        "#,
    )
    .bind(&question_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    let paper_id: String = row.get("paper_id");

    let assets = query(
        r#"
        SELECT asset_id, kind, object_id::text AS object_id, caption, sort_order
        FROM question_assets
        WHERE question_id = $1
        ORDER BY sort_order, asset_id
        "#,
    )
    .bind(&question_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .into_iter()
    .map(map_question_asset)
    .collect();

    let stats = query(
        r#"
        SELECT exam_session, source_workbook_id, participant_count, avg_score, score_std,
               full_mark_rate, zero_score_rate, max_score, min_score, stats_source, stats_version
        FROM question_stats
        WHERE question_id = $1
        ORDER BY exam_session
        "#,
    )
    .bind(&question_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .into_iter()
    .map(map_question_stat)
    .collect();

    let difficulty_scores = query(
        r#"
        SELECT exam_session, manual_level, derived_score, method, method_version,
               confidence, feature_json
        FROM difficulty_scores
        WHERE question_id = $1
        ORDER BY exam_session NULLS FIRST, method, method_version
        "#,
    )
    .bind(&question_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .into_iter()
    .map(map_difficulty_score)
    .collect();

    let score_workbooks = query(
        r#"
        SELECT workbook_id, paper_id, exam_session, workbook_kind,
               workbook_object_id::text AS workbook_object_id, source_filename,
               mime_type, sheet_names_json, file_size, sha256, notes,
               to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM score_workbooks
        WHERE paper_id = $1
        ORDER BY exam_session, workbook_id
        "#,
    )
    .bind(&paper_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .into_iter()
    .map(map_score_workbook_summary)
    .collect();

    Ok(Json(QuestionDetail {
        question_id: row.get("question_id"),
        paper_id,
        paper_index: row.get("paper_index"),
        question_no: row.get("question_no"),
        category: row.get("category"),
        question_tex_object_id: row.get("question_tex_object_id"),
        answer_tex_object_id: row.get("answer_tex_object_id"),
        search_text: row.get("search_text"),
        status: row.get("status"),
        tags: row.get::<Value, _>("tags_json"),
        notes: row.get("notes"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
        edition: row.get("edition"),
        paper_type: row.get("paper_type"),
        paper_title: row.get("paper_title"),
        paper_tex_object_id: row.get("paper_tex_object_id"),
        source_pdf_object_id: row.get("source_pdf_object_id"),
        paper_question_index: row.get::<Value, _>("question_index_json"),
        assets,
        stats,
        difficulty_scores,
        score_workbooks,
    }))
}

async fn list_score_workbooks(
    Query(params): Query<ScoreWorkbookParams>,
    State(state): State<AppState>,
) -> Result<Json<Vec<ScoreWorkbookSummary>>, StatusCode> {
    let (sql, bind_count) = params.build_query();
    let rows = execute_score_workbooks_query(&state.pool, &params, &sql, bind_count)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let payload = rows.into_iter().map(map_score_workbook_summary).collect();
    Ok(Json(payload))
}

async fn get_score_workbook_detail(
    Path(workbook_id): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<ScoreWorkbookSummary>, StatusCode> {
    let row = query(
        r#"
        SELECT workbook_id, paper_id, exam_session, workbook_kind,
               workbook_object_id::text AS workbook_object_id, source_filename,
               mime_type, sheet_names_json, file_size, sha256, notes,
               to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
               to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
        FROM score_workbooks
        WHERE workbook_id = $1
        "#,
    )
    .bind(&workbook_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(map_score_workbook_summary(row)))
}

async fn execute_questions_query(
    pool: &PgPool,
    params: &QuestionsParams,
    plan: &QuestionsQuery,
) -> Result<Vec<PgRow>, sqlx::Error> {
    let mut query = query(&plan.sql);
    if let Some(edition) = &params.edition {
        query = query.bind(edition);
    }
    if let Some(paper_id) = &params.paper_id {
        query = query.bind(paper_id);
    }
    if let Some(paper_type) = &params.paper_type {
        query = query.bind(paper_type);
    }
    if let Some(category) = &params.category {
        query = query.bind(category);
    }
    if let Some(min_avg_score) = params.min_avg_score {
        query = query.bind(min_avg_score);
    }
    if let Some(max_avg_score) = params.max_avg_score {
        query = query.bind(max_avg_score);
    }
    if let Some(tag) = &params.tag {
        query = query.bind(serde_json::json!([tag]));
    }
    if let Some(search) = &params.q {
        let needle = format!("%{search}%");
        query = query.bind(needle.clone()).bind(needle.clone()).bind(needle);
    }
    debug_assert_eq!(plan.bind_count, count_question_binds(params));
    query
        .bind(plan.limit)
        .bind(plan.offset)
        .fetch_all(pool)
        .await
}

async fn execute_score_workbooks_query(
    pool: &PgPool,
    params: &ScoreWorkbookParams,
    sql: &str,
    bind_count: usize,
) -> Result<Vec<PgRow>, sqlx::Error> {
    let mut query = query(sql);
    if let Some(paper_id) = &params.paper_id {
        query = query.bind(paper_id);
    }
    if let Some(exam_session) = &params.exam_session {
        query = query.bind(exam_session);
    }
    debug_assert_eq!(bind_count, count_score_workbook_binds(params));
    query.fetch_all(pool).await
}

fn count_question_binds(params: &QuestionsParams) -> usize {
    usize::from(params.edition.is_some())
        + usize::from(params.paper_id.is_some())
        + usize::from(params.paper_type.is_some())
        + usize::from(params.category.is_some())
        + usize::from(params.min_avg_score.is_some())
        + usize::from(params.max_avg_score.is_some())
        + usize::from(params.tag.is_some())
        + params.q.as_ref().map(|_| 3).unwrap_or(0)
        + 2
}

fn count_score_workbook_binds(params: &ScoreWorkbookParams) -> usize {
    usize::from(params.paper_id.is_some()) + usize::from(params.exam_session.is_some())
}

fn map_paper_summary(row: PgRow) -> PaperSummary {
    PaperSummary {
        paper_id: row.get("paper_id"),
        edition: row.get("edition"),
        paper_type: row.get("paper_type"),
        title: row.get("title"),
        paper_tex_object_id: row.get("paper_tex_object_id"),
        source_pdf_object_id: row.get("source_pdf_object_id"),
        question_index: row.get::<Value, _>("question_index_json"),
        notes: row.get("notes"),
    }
}

fn map_paper_question_summary(row: PgRow) -> PaperQuestionSummary {
    PaperQuestionSummary {
        question_id: row.get("question_id"),
        paper_index: row.get("paper_index"),
        question_no: row.get("question_no"),
        category: row.get("category"),
        question_tex_object_id: row.get("question_tex_object_id"),
        answer_tex_object_id: row.get("answer_tex_object_id"),
        status: row.get("status"),
        tags: row.get::<Value, _>("tags_json"),
    }
}

fn map_question_summary(row: PgRow) -> QuestionSummary {
    QuestionSummary {
        question_id: row.get("question_id"),
        paper_id: row.get("paper_id"),
        paper_index: row.get("paper_index"),
        question_no: row.get("question_no"),
        category: row.get("category"),
        status: row.get("status"),
        search_text: row.get("search_text"),
        question_tex_object_id: row.get("question_tex_object_id"),
        answer_tex_object_id: row.get("answer_tex_object_id"),
        tags: row.get::<Value, _>("tags_json"),
        edition: row.get("edition"),
        paper_type: row.get("paper_type"),
        title: row.get("title"),
    }
}

fn map_question_asset(row: PgRow) -> QuestionAsset {
    QuestionAsset {
        asset_id: row.get("asset_id"),
        kind: row.get("kind"),
        object_id: row.get("object_id"),
        caption: row.get("caption"),
        sort_order: row.get("sort_order"),
    }
}

fn map_question_stat(row: PgRow) -> QuestionStat {
    QuestionStat {
        exam_session: row.get("exam_session"),
        source_workbook_id: row.get("source_workbook_id"),
        participant_count: row.get("participant_count"),
        avg_score: row.get("avg_score"),
        score_std: row.get("score_std"),
        full_mark_rate: row.get("full_mark_rate"),
        zero_score_rate: row.get("zero_score_rate"),
        max_score: row.get("max_score"),
        min_score: row.get("min_score"),
        stats_source: row.get("stats_source"),
        stats_version: row.get("stats_version"),
    }
}

fn map_difficulty_score(row: PgRow) -> DifficultyScore {
    DifficultyScore {
        exam_session: row.get("exam_session"),
        manual_level: row.get("manual_level"),
        derived_score: row.get("derived_score"),
        method: row.get("method"),
        method_version: row.get("method_version"),
        confidence: row.get("confidence"),
        feature_json: row.get::<Value, _>("feature_json"),
    }
}

fn map_score_workbook_summary(row: PgRow) -> ScoreWorkbookSummary {
    ScoreWorkbookSummary {
        workbook_id: row.get("workbook_id"),
        paper_id: row.get("paper_id"),
        exam_session: row.get("exam_session"),
        workbook_kind: row.get("workbook_kind"),
        workbook_object_id: row.get("workbook_object_id"),
        source_filename: row.get("source_filename"),
        mime_type: row.get("mime_type"),
        sheet_names: row.get::<Value, _>("sheet_names_json"),
        file_size: row.get("file_size"),
        sha256: row.get("sha256"),
        notes: row.get("notes"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        count_question_binds, count_score_workbook_binds, QuestionsParams, ScoreWorkbookParams,
    };

    #[test]
    fn question_query_normalizes_limit_offset_and_counts_binds() {
        let params = QuestionsParams {
            edition: Some("18".into()),
            paper_id: Some("CPHOS-18-REGULAR".into()),
            paper_type: Some("regular".into()),
            category: Some("theory".into()),
            has_assets: Some(true),
            has_answer: Some(false),
            min_avg_score: Some(1.5),
            max_avg_score: Some(4.5),
            tag: Some("mechanics".into()),
            q: Some("pendulum".into()),
            limit: Some(999),
            offset: Some(-10),
        };

        let query = params.build_query();
        assert_eq!(query.limit, 100);
        assert_eq!(query.offset, 0);
        assert_eq!(query.bind_count, count_question_binds(&params));
        assert!(query.sql.contains("q.tags_json @>"));
        assert!(query.sql.contains("EXISTS (SELECT 1 FROM question_assets"));
        assert!(query.sql.contains("q.answer_tex_object_id IS NULL"));
    }

    #[test]
    fn score_workbook_query_counts_optional_filters() {
        let params = ScoreWorkbookParams {
            paper_id: Some("CPHOS-18-REGULAR".into()),
            exam_session: None,
        };
        let (sql, bind_count) = params.build_query();
        assert!(sql.contains("FROM score_workbooks"));
        assert_eq!(bind_count, count_score_workbook_binds(&params));
    }
}
