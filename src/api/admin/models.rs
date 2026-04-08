use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::api::{
    papers::models::{validate_paper_filters, PaperDetail, PaperSummary, PapersParams},
    questions::{
        models::{QuestionDetail, QuestionSummary, QuestionsParams},
        queries::validate_question_filters,
    },
    shared::pagination::{normalize_limit, normalize_offset},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordState {
    Active,
    Deleted,
    All,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminQuestionsParams {
    pub(crate) state: Option<String>,
    pub(crate) paper_id: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) score_min: Option<i32>,
    pub(crate) score_max: Option<i32>,
    pub(crate) difficulty_tag: Option<String>,
    pub(crate) difficulty_min: Option<i32>,
    pub(crate) difficulty_max: Option<i32>,
    pub(crate) created_after: Option<String>,
    pub(crate) created_before: Option<String>,
    pub(crate) updated_after: Option<String>,
    pub(crate) updated_before: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminPapersParams {
    pub(crate) state: Option<String>,
    pub(crate) question_id: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) created_after: Option<String>,
    pub(crate) created_before: Option<String>,
    pub(crate) updated_after: Option<String>,
    pub(crate) updated_before: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminQuestionSummary {
    #[serde(flatten)]
    pub(crate) question: QuestionSummary,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
    pub(crate) is_deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminQuestionDetail {
    #[serde(flatten)]
    pub(crate) question: QuestionDetail,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
    pub(crate) is_deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminPaperSummary {
    #[serde(flatten)]
    pub(crate) paper: PaperSummary,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
    pub(crate) is_deleted: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminPaperDetail {
    #[serde(flatten)]
    pub(crate) paper: PaperDetail,
    pub(crate) deleted_at: Option<String>,
    pub(crate) deleted_by: Option<String>,
    pub(crate) is_deleted: bool,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(crate) struct GarbageCollectionRequest {}

#[derive(Debug, Serialize)]
pub(crate) struct GarbageCollectionResponse {
    pub(crate) dry_run: bool,
    pub(crate) deleted_questions: usize,
    pub(crate) deleted_papers: usize,
    pub(crate) deleted_objects: usize,
    pub(crate) freed_bytes: i64,
}

impl AdminQuestionsParams {
    pub(crate) fn normalized_limit(&self) -> i64 {
        normalize_limit(self.limit)
    }

    pub(crate) fn normalized_offset(&self) -> i64 {
        normalize_offset(self.offset)
    }

    pub(crate) fn state(&self) -> Result<RecordState> {
        normalize_record_state(self.state.as_deref())
    }

    pub(crate) fn validate_filters(&self) -> Result<RecordState> {
        let state = self.state()?;
        validate_question_filters(&self.as_question_params())?;
        Ok(state)
    }

    pub(crate) fn as_question_params(&self) -> QuestionsParams {
        QuestionsParams {
            paper_id: self.paper_id.clone(),
            category: self.category.clone(),
            tag: self.tag.clone(),
            score_min: self.score_min,
            score_max: self.score_max,
            difficulty_tag: self.difficulty_tag.clone(),
            difficulty_min: self.difficulty_min,
            difficulty_max: self.difficulty_max,
            created_after: self.created_after.clone(),
            created_before: self.created_before.clone(),
            updated_after: self.updated_after.clone(),
            updated_before: self.updated_before.clone(),
            q: self.q.clone(),
            limit: self.limit,
            offset: self.offset,
        }
    }
}

impl AdminPapersParams {
    pub(crate) fn normalized_limit(&self) -> i64 {
        normalize_limit(self.limit)
    }

    pub(crate) fn normalized_offset(&self) -> i64 {
        normalize_offset(self.offset)
    }

    pub(crate) fn state(&self) -> Result<RecordState> {
        normalize_record_state(self.state.as_deref())
    }

    pub(crate) fn validate_filters(&self) -> Result<RecordState> {
        let state = self.state()?;
        validate_paper_filters(&self.as_paper_params())?;
        Ok(state)
    }

    pub(crate) fn as_paper_params(&self) -> PapersParams {
        PapersParams {
            question_id: self.question_id.clone(),
            category: self.category.clone(),
            tag: self.tag.clone(),
            created_after: self.created_after.clone(),
            created_before: self.created_before.clone(),
            updated_after: self.updated_after.clone(),
            updated_before: self.updated_before.clone(),
            q: self.q.clone(),
            limit: self.limit,
            offset: self.offset,
        }
    }
}

pub(crate) fn normalize_record_state(raw: Option<&str>) -> Result<RecordState> {
    match raw.unwrap_or("all") {
        "active" => Ok(RecordState::Active),
        "deleted" => Ok(RecordState::Deleted),
        "all" => Ok(RecordState::All),
        other => Err(anyhow!(
            "state must be one of: active, deleted, all; got {other}"
        )),
    }
}

pub(crate) fn admin_question_summary(
    question: QuestionSummary,
    deleted_at: Option<String>,
    deleted_by: Option<String>,
) -> AdminQuestionSummary {
    let is_deleted = deleted_at.is_some();
    AdminQuestionSummary {
        question,
        deleted_at,
        deleted_by,
        is_deleted,
    }
}

pub(crate) fn admin_question_detail(
    question: QuestionDetail,
    deleted_at: Option<String>,
    deleted_by: Option<String>,
) -> AdminQuestionDetail {
    let is_deleted = deleted_at.is_some();
    AdminQuestionDetail {
        question,
        deleted_at,
        deleted_by,
        is_deleted,
    }
}

pub(crate) fn admin_paper_summary(
    paper: PaperSummary,
    deleted_at: Option<String>,
    deleted_by: Option<String>,
) -> AdminPaperSummary {
    let is_deleted = deleted_at.is_some();
    AdminPaperSummary {
        paper,
        deleted_at,
        deleted_by,
        is_deleted,
    }
}

pub(crate) fn admin_paper_detail(
    paper: PaperDetail,
    deleted_at: Option<String>,
    deleted_by: Option<String>,
) -> AdminPaperDetail {
    let is_deleted = deleted_at.is_some();
    AdminPaperDetail {
        paper,
        deleted_at,
        deleted_by,
        is_deleted,
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_record_state, RecordState};

    #[test]
    fn normalize_record_state_defaults_to_all() {
        assert_eq!(
            normalize_record_state(None).expect("default state should parse"),
            RecordState::All
        );
    }

    #[test]
    fn normalize_record_state_rejects_unknown_value() {
        let err = normalize_record_state(Some("oops")).expect_err("state should fail");
        assert!(err.to_string().contains("state must be one of"));
    }
}
