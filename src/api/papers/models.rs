use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use crate::api::questions::models::validate_question_category;

pub(crate) const PAPER_TYPES: [&str; 4] = ["regular", "semifinal", "final", "other"];

#[derive(Debug, Serialize)]
pub struct PaperSummary {
    pub(crate) paper_id: String,
    pub(crate) edition: Option<String>,
    pub(crate) paper_type: String,
    pub(crate) description: String,
    pub(crate) question_count: i64,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct PaperQuestionSummary {
    pub(crate) question_id: String,
    pub(crate) sort_order: i32,
    pub(crate) category: String,
    pub(crate) status: String,
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct PaperDetail {
    pub(crate) paper_id: String,
    pub(crate) edition: Option<String>,
    pub(crate) paper_type: String,
    pub(crate) description: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) questions: Vec<PaperQuestionSummary>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreatePaperRequest {
    pub(crate) edition: Option<String>,
    pub(crate) paper_type: String,
    pub(crate) description: String,
    pub(crate) question_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct PapersParams {
    pub(crate) question_id: Option<String>,
    pub(crate) paper_type: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) q: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PaperWriteResponse {
    pub(crate) paper_id: String,
    pub(crate) status: &'static str,
    pub(crate) question_count: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdatePaperRequest {
    #[serde(default)]
    pub(crate) edition: Option<Option<String>>,
    #[serde(default)]
    pub(crate) paper_type: Option<String>,
    #[serde(default)]
    pub(crate) description: Option<Option<String>>,
    #[serde(default)]
    pub(crate) question_ids: Option<Vec<String>>,
}

#[derive(Debug)]
pub(crate) struct NormalizedCreatePaperRequest {
    pub(crate) edition: Option<String>,
    pub(crate) paper_type: String,
    pub(crate) description: String,
    pub(crate) question_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct NormalizedPaperUpdate {
    pub(crate) edition: Option<Option<String>>,
    pub(crate) paper_type: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) question_ids: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PaperDeleteResponse {
    pub(crate) paper_id: String,
    pub(crate) status: &'static str,
}

pub(crate) fn validate_paper_type(paper_type: &str) -> Result<()> {
    if !PAPER_TYPES.contains(&paper_type) {
        bail!("paper_type must be one of: regular, semifinal, final, other");
    }
    Ok(())
}

impl CreatePaperRequest {
    pub(crate) fn normalize(self) -> Result<NormalizedCreatePaperRequest> {
        let edition = self.edition.and_then(normalize_optional_text);
        let paper_type = normalize_paper_type(&self.paper_type)?;
        let description = normalize_required_text("description", &self.description)?;
        let question_ids = normalize_question_ids(self.question_ids)?;

        Ok(NormalizedCreatePaperRequest {
            edition,
            paper_type,
            description,
            question_ids,
        })
    }
}

impl UpdatePaperRequest {
    pub(crate) fn normalize(self) -> Result<NormalizedPaperUpdate> {
        if self.edition.is_none()
            && self.paper_type.is_none()
            && self.description.is_none()
            && self.question_ids.is_none()
        {
            return Err(anyhow!(
                "request body must include at least one of: edition, paper_type, description, question_ids"
            ));
        }

        let edition = self
            .edition
            .map(|value| value.and_then(normalize_optional_text));
        let paper_type = self
            .paper_type
            .map(|value| normalize_paper_type(&value))
            .transpose()?;
        let description = self
            .description
            .map(|value| normalize_required_plaintext("description", value))
            .transpose()?;
        let question_ids = self.question_ids.map(normalize_question_ids).transpose()?;

        Ok(NormalizedPaperUpdate {
            edition,
            paper_type,
            description,
            question_ids,
        })
    }
}

impl PapersParams {
    pub(crate) fn normalized_limit(&self) -> i64 {
        self.limit.unwrap_or(20).clamp(1, 100)
    }

    pub(crate) fn normalized_offset(&self) -> i64 {
        self.offset.unwrap_or(0).max(0)
    }
}

fn normalize_paper_type(value: &str) -> Result<String> {
    let normalized = value.trim().to_string();
    validate_paper_type(&normalized)?;
    Ok(normalized)
}

fn normalize_required_text(field: &str, value: &str) -> Result<String> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(normalized)
}

fn normalize_optional_text(value: String) -> Option<String> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn normalize_required_plaintext(field: &str, value: Option<String>) -> Result<String> {
    let Some(text) = value else {
        bail!("{field} must not be null");
    };
    let trimmed = text.trim().to_string();
    if trimmed.is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(trimmed)
}

fn normalize_question_ids(question_ids: Vec<String>) -> Result<Vec<String>> {
    if question_ids.is_empty() {
        bail!("question_ids must not be empty");
    }

    let mut seen = HashSet::new();
    for question_id in &question_ids {
        if !seen.insert(question_id.clone()) {
            bail!("duplicate question_id in question_ids: {question_id}");
        }
    }

    Ok(question_ids)
}

pub(crate) fn validate_paper_filters(params: &PapersParams) -> Result<()> {
    if let Some(question_id) = &params.question_id {
        uuid::Uuid::parse_str(question_id)
            .map_err(|_| anyhow!("question_id must be a valid UUID"))?;
    }
    if let Some(paper_type) = &params.paper_type {
        validate_paper_type(paper_type)?;
    }
    if let Some(category) = &params.category {
        validate_question_category(category)
            .map_err(|_| anyhow!("category must be one of: none, T, E"))?;
    }
    if let Some(q) = &params.q {
        if q.trim().is_empty() {
            bail!("q must not be empty");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{CreatePaperRequest, UpdatePaperRequest};

    #[test]
    fn create_request_requires_non_empty_description() {
        let request = CreatePaperRequest {
            edition: Some(" 2026 ".into()),
            paper_type: " regular ".into(),
            description: "  热学决赛卷  ".into(),
            question_ids: vec!["q1".into()],
        };

        let normalized = request.normalize().expect("request should normalize");
        assert_eq!(normalized.edition.as_deref(), Some("2026"));
        assert_eq!(normalized.paper_type, "regular");
        assert_eq!(normalized.description, "热学决赛卷");

        let empty_request: CreatePaperRequest = serde_json::from_str(
            r#"{"edition":"2026","paper_type":"regular","description":" ","question_ids":["q1"]}"#,
        )
        .expect("json should parse");
        assert!(empty_request.normalize().is_err());
    }

    #[test]
    fn update_request_rejects_empty_or_null_description() {
        let empty_request: UpdatePaperRequest =
            serde_json::from_str(r#"{"description":""}"#).expect("json should parse");
        let null_request: UpdatePaperRequest =
            serde_json::from_str(r#"{"description":null}"#).expect("json should parse");

        assert!(empty_request.normalize().is_err());
        assert!(null_request.normalize().is_err());
    }
}
