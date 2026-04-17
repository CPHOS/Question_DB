use std::collections::{BTreeMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};

use crate::api::shared::utils::normalize_bundle_description;

pub(crate) const QUESTION_CATEGORIES: [&str; 3] = ["none", "T", "E"];
pub(crate) const QUESTION_STATUSES: [&str; 3] = ["none", "reviewed", "used"];

#[derive(Debug, Serialize)]
pub struct QuestionSourceRef {
    pub(crate) tex: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QuestionDifficulty {
    #[serde(flatten)]
    pub(crate) entries: BTreeMap<String, QuestionDifficultyValue>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DifficultyEditor {
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuestionDifficultyValue {
    pub(crate) score: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) notes: Option<String>,
    #[serde(default, skip_deserializing, skip_serializing_if = "Option::is_none")]
    pub(crate) updated_by: Option<DifficultyEditor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuestionAssetRef {
    pub(crate) path: String,
    pub(crate) file_kind: String,
    pub(crate) object_id: String,
    pub(crate) mime_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct QuestionSummary {
    pub(crate) question_id: String,
    pub(crate) source: QuestionSourceRef,
    pub(crate) category: String,
    pub(crate) status: String,
    pub(crate) description: String,
    pub(crate) score: Option<i32>,
    pub(crate) author: String,
    pub(crate) reviewers: Vec<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) difficulty: QuestionDifficulty,
    pub(crate) allow_auto_reviewer: bool,
    pub(crate) created_by: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Serialize)]
pub struct QuestionPaperRef {
    pub(crate) paper_id: String,
    pub(crate) description: String,
    pub(crate) title: String,
    pub(crate) subtitle: String,
    pub(crate) sort_order: i32,
}

#[derive(Debug, Serialize)]
pub struct QuestionDetail {
    pub(crate) question_id: String,
    pub(crate) tex_object_id: String,
    pub(crate) source: QuestionSourceRef,
    pub(crate) category: String,
    pub(crate) status: String,
    pub(crate) description: String,
    pub(crate) score: Option<i32>,
    pub(crate) author: String,
    pub(crate) reviewers: Vec<String>,
    pub(crate) tags: Vec<String>,
    pub(crate) difficulty: QuestionDifficulty,
    pub(crate) allow_auto_reviewer: bool,
    pub(crate) created_by: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) assets: Vec<QuestionAssetRef>,
    pub(crate) papers: Vec<QuestionPaperRef>,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionTagsResponse {
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionDifficultyTagsResponse {
    pub(crate) difficulty_tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum QuestionTagFilter {
    Tag { tag: String },
    And { children: Vec<QuestionTagFilter> },
    Or { children: Vec<QuestionTagFilter> },
    Not { child: Box<QuestionTagFilter> },
}

#[derive(Debug, Deserialize)]
pub(crate) struct QuestionsParams {
    pub(crate) paper_id: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) tag_filter: Option<QuestionTagFilter>,
    pub(crate) author: Option<String>,
    pub(crate) reviewer: Option<String>,
    pub(crate) assigned_reviewer_id: Option<String>,
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
#[serde(deny_unknown_fields)]
pub(crate) struct QuestionSearchRequest {
    pub(crate) paper_id: Option<String>,
    pub(crate) category: Option<String>,
    pub(crate) tag: Option<String>,
    pub(crate) tag_filter: Option<QuestionTagFilter>,
    pub(crate) author: Option<String>,
    pub(crate) reviewer: Option<String>,
    pub(crate) assigned_reviewer_id: Option<String>,
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
pub(crate) struct QuestionBundleRequest {
    pub(crate) question_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct CreateQuestionRequest {
    pub(crate) description: String,
    pub(crate) category: Option<String>,
    pub(crate) tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateDescriptionRequest {
    pub(crate) description: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateCategoryRequest {
    pub(crate) category: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateTagsRequest {
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateStatusRequest {
    pub(crate) status: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateAuthorRequest {
    pub(crate) author: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateReviewerNamesRequest {
    pub(crate) reviewers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CreateDifficultyRequest {
    pub(crate) algorithm_tag: String,
    pub(crate) score: i32,
    #[serde(default)]
    pub(crate) notes: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct UpdateDifficultyRequest {
    pub(crate) score: i32,
    #[serde(default)]
    pub(crate) notes: Option<String>,
}

#[derive(Debug)]
pub(crate) struct NormalizedCreateQuestionRequest {
    pub(crate) description: String,
    pub(crate) category: String,
    pub(crate) tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionImportResponse {
    pub(crate) question_id: String,
    pub(crate) file_name: String,
    pub(crate) imported_assets: usize,
    pub(crate) status: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionFileReplaceResponse {
    pub(crate) question_id: String,
    pub(crate) file_name: String,
    pub(crate) source_tex_path: String,
    pub(crate) imported_assets: usize,
    pub(crate) status: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct QuestionDeleteResponse {
    pub(crate) question_id: String,
    pub(crate) status: &'static str,
}

pub(crate) fn validate_question_category(category: &str) -> Result<()> {
    if !QUESTION_CATEGORIES.contains(&category) {
        bail!("category must be one of: none, T, E");
    }
    Ok(())
}

pub(crate) fn validate_question_status(status: &str) -> Result<()> {
    if !QUESTION_STATUSES.contains(&status) {
        bail!("status must be one of: none, reviewed, used");
    }
    Ok(())
}

impl CreateQuestionRequest {
    pub(crate) fn normalize(self) -> Result<NormalizedCreateQuestionRequest> {
        let description = normalize_required_plaintext_value("description", &self.description)?;
        let category = self
            .category
            .map(|value| normalize_category(&value))
            .transpose()?
            .unwrap_or_else(|| "none".to_string());
        let tags = self
            .tags
            .map(normalize_tags)
            .transpose()?
            .unwrap_or_default();

        Ok(NormalizedCreateQuestionRequest {
            description,
            category,
            tags,
        })
    }
}

impl UpdateDescriptionRequest {
    pub(crate) fn normalize(&self) -> Result<String> {
        normalize_required_plaintext_value("description", &self.description)
    }
}

impl UpdateCategoryRequest {
    pub(crate) fn normalize(&self) -> Result<String> {
        normalize_category(&self.category)
    }
}

impl UpdateTagsRequest {
    pub(crate) fn normalize(self) -> Result<Vec<String>> {
        normalize_tags(self.tags)
    }
}

impl UpdateStatusRequest {
    pub(crate) fn normalize(&self) -> Result<String> {
        normalize_status(&self.status)
    }
}

impl UpdateAuthorRequest {
    pub(crate) fn normalize(&self) -> Result<String> {
        let trimmed = self.author.trim().to_string();
        if trimmed.is_empty() {
            bail!("author must not be empty");
        }
        Ok(trimmed)
    }
}

impl UpdateReviewerNamesRequest {
    pub(crate) fn normalize(self) -> Result<Vec<String>> {
        let mut seen = HashSet::new();
        let mut normalized = Vec::with_capacity(self.reviewers.len());
        for name in &self.reviewers {
            let trimmed = name.trim().to_string();
            if !trimmed.is_empty() && seen.insert(trimmed.clone()) {
                normalized.push(trimmed);
            }
        }
        Ok(normalized)
    }
}

impl CreateDifficultyRequest {
    pub(crate) fn normalize(&self) -> Result<(String, i32, Option<String>)> {
        let tag = self.algorithm_tag.trim().to_string();
        if tag.is_empty() {
            bail!("algorithm_tag must not be empty");
        }
        if !(1..=10).contains(&self.score) {
            bail!("score must be between 1 and 10");
        }
        let notes = self.notes.as_ref().and_then(|n| {
            let trimmed = n.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });
        Ok((tag, self.score, notes))
    }
}

impl UpdateDifficultyRequest {
    pub(crate) fn normalize(&self) -> Result<(i32, Option<String>)> {
        if !(1..=10).contains(&self.score) {
            bail!("score must be between 1 and 10");
        }
        let notes = self.notes.as_ref().and_then(|n| {
            let trimmed = n.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });
        Ok((self.score, notes))
    }
}

impl QuestionBundleRequest {
    pub(crate) fn normalize(self) -> Result<Vec<String>> {
        normalize_bundle_ids("question_ids", self.question_ids)
    }
}

impl QuestionTagFilter {
    pub(crate) fn normalize(self) -> Result<Self> {
        let mut node_count = 0;
        self.normalize_inner(1, &mut node_count)
    }

    fn normalize_inner(self, depth: usize, node_count: &mut usize) -> Result<Self> {
        const MAX_TAG_FILTER_DEPTH: usize = 16;
        const MAX_TAG_FILTER_NODES: usize = 128;

        if depth > MAX_TAG_FILTER_DEPTH {
            bail!("tag_filter nesting must not exceed {MAX_TAG_FILTER_DEPTH} levels");
        }

        *node_count += 1;
        if *node_count > MAX_TAG_FILTER_NODES {
            bail!("tag_filter must not contain more than {MAX_TAG_FILTER_NODES} nodes");
        }

        match self {
            Self::Tag { tag } => {
                let tag = tag.trim().to_string();
                if tag.is_empty() {
                    bail!("tag_filter tag must not be empty");
                }
                Ok(Self::Tag { tag })
            }
            Self::And { children } => Ok(Self::And {
                children: normalize_tag_filter_children("and", children, depth, node_count)?,
            }),
            Self::Or { children } => Ok(Self::Or {
                children: normalize_tag_filter_children("or", children, depth, node_count)?,
            }),
            Self::Not { child } => Ok(Self::Not {
                child: Box::new(child.normalize_inner(depth + 1, node_count)?),
            }),
        }
    }
}

impl QuestionSearchRequest {
    pub(crate) fn normalize(self) -> Result<QuestionsParams> {
        Ok(QuestionsParams {
            paper_id: self.paper_id,
            category: self.category,
            tag: self.tag,
            tag_filter: self
                .tag_filter
                .map(QuestionTagFilter::normalize)
                .transpose()?,
            author: self.author,
            reviewer: self.reviewer,
            assigned_reviewer_id: self.assigned_reviewer_id,
            score_min: self.score_min,
            score_max: self.score_max,
            difficulty_tag: self.difficulty_tag,
            difficulty_min: self.difficulty_min,
            difficulty_max: self.difficulty_max,
            created_after: self.created_after,
            created_before: self.created_before,
            updated_after: self.updated_after,
            updated_before: self.updated_before,
            q: self.q,
            limit: self.limit,
            offset: self.offset,
        })
    }
}

fn normalize_category(value: &str) -> Result<String> {
    let normalized = value.trim().to_string();
    validate_question_category(&normalized)?;
    Ok(normalized)
}

fn normalize_status(value: &str) -> Result<String> {
    let normalized = value.trim().to_string();
    validate_question_status(&normalized)?;
    Ok(normalized)
}

fn normalize_required_plaintext_value(field: &str, value: &str) -> Result<String> {
    normalize_bundle_description(field, value)
}

fn normalize_tags(values: Vec<String>) -> Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::new();

    for value in values {
        let tag = value.trim().to_string();
        if tag.is_empty() {
            bail!("tags must not contain empty strings");
        }
        if seen.insert(tag.clone()) {
            normalized.push(tag);
        }
    }

    Ok(normalized)
}

fn normalize_tag_filter_children(
    operator: &str,
    children: Vec<QuestionTagFilter>,
    depth: usize,
    node_count: &mut usize,
) -> Result<Vec<QuestionTagFilter>> {
    if children.is_empty() {
        bail!("tag_filter {operator} must contain at least one child");
    }

    children
        .into_iter()
        .map(|child| child.normalize_inner(depth + 1, node_count))
        .collect()
}

fn normalize_bundle_ids(field_name: &str, ids: Vec<String>) -> Result<Vec<String>> {
    if ids.is_empty() {
        return Err(anyhow!("{field_name} must not be empty"));
    }

    let mut normalized = Vec::with_capacity(ids.len());
    let mut seen = HashSet::new();

    for raw_id in ids {
        let id = raw_id.trim().to_string();
        if id.is_empty() {
            bail!("{field_name} must not contain empty values");
        }
        uuid::Uuid::parse_str(&id).map_err(|_| anyhow!("invalid {field_name} entry: {id}"))?;
        if !seen.insert(id.clone()) {
            bail!("duplicate {field_name} entry: {id}");
        }
        normalized.push(id);
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_not_filter(levels: usize) -> QuestionTagFilter {
        let mut filter = QuestionTagFilter::Tag {
            tag: "mechanics".into(),
        };
        for _ in 0..levels {
            filter = QuestionTagFilter::Not {
                child: Box::new(filter),
            };
        }
        filter
    }

    #[test]
    fn create_request_normalizes_with_defaults() {
        let request = CreateQuestionRequest {
            description: "  demo note  ".into(),
            category: Some(" T ".into()),
            tags: Some(vec![" optics ".into(), "mechanics".into(), "optics".into()]),
        };

        let normalized = request.normalize().expect("request should normalize");
        assert_eq!(normalized.description, "demo note");
        assert_eq!(normalized.category, "T");
        assert_eq!(
            normalized.tags,
            vec!["optics".to_string(), "mechanics".to_string()]
        );
    }

    #[test]
    fn create_request_defaults_optional_fields() {
        let request = CreateQuestionRequest {
            description: "demo note".into(),
            category: None,
            tags: None,
        };

        let normalized = request.normalize().expect("request should normalize");
        assert_eq!(normalized.category, "none");
        assert!(normalized.tags.is_empty());
    }

    #[test]
    fn update_description_normalizes_whitespace() {
        let req = UpdateDescriptionRequest {
            description: "  hello world  ".into(),
        };
        assert_eq!(req.normalize().unwrap(), "hello world");
    }

    #[test]
    fn update_description_rejects_empty() {
        let req = UpdateDescriptionRequest {
            description: "   ".into(),
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn update_category_validates() {
        let req = UpdateCategoryRequest {
            category: " T ".into(),
        };
        assert_eq!(req.normalize().unwrap(), "T");

        let bad = UpdateCategoryRequest {
            category: "invalid".into(),
        };
        assert!(bad.normalize().is_err());
    }

    #[test]
    fn update_tags_deduplicates_and_trims() {
        let req = UpdateTagsRequest {
            tags: vec![" optics ".into(), "mechanics".into(), "optics".into()],
        };
        let tags = req.normalize().unwrap();
        assert_eq!(tags, vec!["optics".to_string(), "mechanics".to_string()]);
    }

    #[test]
    fn update_tags_rejects_empty_strings() {
        let req = UpdateTagsRequest {
            tags: vec!["good".into(), "  ".into()],
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn update_status_validates() {
        let req = UpdateStatusRequest {
            status: " reviewed ".into(),
        };
        assert_eq!(req.normalize().unwrap(), "reviewed");

        let bad = UpdateStatusRequest {
            status: "invalid".into(),
        };
        assert!(bad.normalize().is_err());
    }

    #[test]
    fn create_difficulty_normalizes() {
        let req = CreateDifficultyRequest {
            algorithm_tag: " human ".into(),
            score: 7,
            notes: Some("  calibrated  ".into()),
        };
        let (tag, score, notes) = req.normalize().unwrap();
        assert_eq!(tag, "human");
        assert_eq!(score, 7);
        assert_eq!(notes.as_deref(), Some("calibrated"));
    }

    #[test]
    fn create_difficulty_rejects_invalid_score() {
        let req = CreateDifficultyRequest {
            algorithm_tag: "human".into(),
            score: 11,
            notes: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn create_difficulty_rejects_empty_tag() {
        let req = CreateDifficultyRequest {
            algorithm_tag: "  ".into(),
            score: 5,
            notes: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn update_difficulty_normalizes() {
        let req = UpdateDifficultyRequest {
            score: 8,
            notes: Some("   ".into()),
        };
        let (score, notes) = req.normalize().unwrap();
        assert_eq!(score, 8);
        assert_eq!(notes, None); // whitespace-only becomes None
    }

    #[test]
    fn update_difficulty_rejects_invalid_score() {
        let req = UpdateDifficultyRequest {
            score: 0,
            notes: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn question_tag_filter_normalizes_nested_tags() {
        let normalized = QuestionTagFilter::Not {
            child: Box::new(QuestionTagFilter::And {
                children: vec![QuestionTagFilter::Tag {
                    tag: " mechanics ".into(),
                }],
            }),
        }
        .normalize()
        .expect("tag filter should normalize");

        assert_eq!(
            normalized,
            QuestionTagFilter::Not {
                child: Box::new(QuestionTagFilter::And {
                    children: vec![QuestionTagFilter::Tag {
                        tag: "mechanics".into(),
                    }],
                }),
            }
        );
    }

    #[test]
    fn question_tag_filter_rejects_excessive_depth() {
        let result = nested_not_filter(16).normalize();
        assert!(result.is_err());
    }

    #[test]
    fn question_tag_filter_rejects_excessive_nodes() {
        let result = QuestionTagFilter::And {
            children: (0..128)
                .map(|idx| QuestionTagFilter::Tag {
                    tag: format!("tag-{idx}"),
                })
                .collect(),
        }
        .normalize();
        assert!(result.is_err());
    }

    #[test]
    fn question_tag_filter_accepts_maximum_node_count() {
        let result = QuestionTagFilter::And {
            children: (0..127)
                .map(|idx| QuestionTagFilter::Tag {
                    tag: format!("tag-{idx}"),
                })
                .collect(),
        }
        .normalize();
        assert!(result.is_ok());
    }
}
