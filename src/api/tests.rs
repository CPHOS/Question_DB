#[cfg(test)]
mod tests {
    use crate::api::{
        papers::models::PapersParams,
        questions::models::{
            CreateDifficultyRequest, CreateQuestionRequest, QuestionsParams,
            UpdateCategoryRequest, UpdateDescriptionRequest, UpdateDifficultyRequest,
            UpdateStatusRequest, UpdateTagsRequest,
        },
    };

    // -----------------------------------------------------------------------
    // Question query plan tests
    // -----------------------------------------------------------------------

    #[test]
    fn question_query_normalizes_limit_offset_and_builds_sql() {
        let params = QuestionsParams {
            paper_id: Some("550e8400-e29b-41d4-a716-446655440000".into()),
            category: Some("none".into()),
            tag: Some("mechanics".into()),
            reviewer: None,
            assigned_reviewer_id: None,
            score_min: None,
            score_max: None,
            difficulty_tag: Some("human".into()),
            difficulty_min: Some(3),
            difficulty_max: Some(6),
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: Some("pendulum".into()),
            limit: Some(999),
            offset: Some(-10),
        };

        let plan = params.build_query();
        assert_eq!(plan.limit, 100);
        assert_eq!(plan.offset, 0);
        let sql = plan.builder.sql().to_owned();
        assert!(sql.contains("WHERE q.deleted_at IS NULL"));
        assert!(sql.contains("FROM question_tags qt"));
        assert!(sql.contains("FROM question_difficulties qd"));
        assert!(sql.contains("qd.algorithm_tag = "));
        assert!(sql.contains("qd.score >= "));
        assert!(sql.contains("qd.score <= "));
        assert!(sql.contains("FROM paper_questions pq"));
        assert!(sql.contains("JOIN papers p ON p.paper_id = pq.paper_id"));
        assert!(sql.contains("p.deleted_at IS NULL"));
        assert!(sql.contains("COALESCE(q.description, '') ILIKE"));
        assert!(sql.contains("COUNT(*) OVER() AS total_count"));
    }

    // -----------------------------------------------------------------------
    // Paper query plan tests
    // -----------------------------------------------------------------------

    #[test]
    fn paper_query_normalizes_limit_offset_and_builds_sql() {
        let params = PapersParams {
            question_id: Some("550e8400-e29b-41d4-a716-446655440000".into()),
            category: Some("E".into()),
            tag: Some("optics".into()),
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: Some("thermal".into()),
            limit: Some(999),
            offset: Some(-10),
        };

        let plan = params.build_query();
        assert_eq!(plan.limit, 100);
        assert_eq!(plan.offset, 0);
        let sql = plan.builder.sql().to_owned();
        assert!(sql.contains("WHERE p.deleted_at IS NULL"));
        assert!(sql.contains("FROM paper_questions pq"));
        assert!(sql.contains("q.deleted_at IS NULL"));
        assert!(sql.contains("JOIN question_tags qt"));
        assert!(sql.contains("CONCAT_WS(' ', p.description, p.title, p.subtitle"));
    }

    // -----------------------------------------------------------------------
    // CreateQuestionRequest normalization: positive cases
    // -----------------------------------------------------------------------

    #[test]
    fn create_question_normalizes_description_category_tags() {
        let req = CreateQuestionRequest {
            description: "  热学标定  ".into(),
            category: Some(" T ".into()),
            tags: Some(vec![" optics ".into(), "mechanics".into(), "optics".into()]),
        };
        let n = req.normalize().unwrap();
        assert_eq!(n.description, "热学标定");
        assert_eq!(n.category, "T");
        assert_eq!(n.tags, vec!["optics", "mechanics"]);
    }

    #[test]
    fn create_question_uses_defaults_when_optional_fields_missing() {
        let req = CreateQuestionRequest {
            description: "demo".into(),
            category: None,
            tags: None,
        };
        let n = req.normalize().unwrap();
        assert_eq!(n.category, "none");
        assert!(n.tags.is_empty());
    }

    // -----------------------------------------------------------------------
    // CreateQuestionRequest normalization: negative cases
    // -----------------------------------------------------------------------

    #[test]
    fn create_question_rejects_empty_description() {
        let req = CreateQuestionRequest {
            description: "   ".into(),
            category: None,
            tags: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn create_question_rejects_invalid_category() {
        let req = CreateQuestionRequest {
            description: "demo".into(),
            category: Some("X".into()),
            tags: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn create_question_rejects_empty_tag_elements() {
        let req = CreateQuestionRequest {
            description: "demo".into(),
            category: None,
            tags: Some(vec!["good".into(), "  ".into()]),
        };
        assert!(req.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // UpdateDescriptionRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn update_description_trims_whitespace() {
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
    fn update_description_rejects_control_characters() {
        let req = UpdateDescriptionRequest {
            description: "line1\x00line2".into(),
        };
        assert!(req.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // UpdateCategoryRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn update_category_normalizes_valid_values() {
        for val in &["none", " T ", "E"] {
            let req = UpdateCategoryRequest {
                category: val.to_string(),
            };
            assert!(req.normalize().is_ok());
        }
    }

    #[test]
    fn update_category_rejects_invalid_value() {
        let req = UpdateCategoryRequest {
            category: "X".into(),
        };
        assert!(req.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // UpdateTagsRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn update_tags_deduplicates_and_trims() {
        let req = UpdateTagsRequest {
            tags: vec![" a ".into(), "b".into(), "a".into()],
        };
        assert_eq!(req.normalize().unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn update_tags_rejects_empty_elements() {
        let req = UpdateTagsRequest {
            tags: vec!["good".into(), "".into()],
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn update_tags_accepts_empty_array() {
        let req = UpdateTagsRequest { tags: vec![] };
        assert_eq!(req.normalize().unwrap(), Vec::<String>::new());
    }

    // -----------------------------------------------------------------------
    // UpdateStatusRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn update_status_normalizes_valid_values() {
        for val in &["none", " reviewed ", "used"] {
            let req = UpdateStatusRequest {
                status: val.to_string(),
            };
            assert!(req.normalize().is_ok());
        }
    }

    #[test]
    fn update_status_rejects_invalid_value() {
        let req = UpdateStatusRequest {
            status: "draft".into(),
        };
        assert!(req.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // CreateDifficultyRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn create_difficulty_normalizes_valid_entry() {
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
    fn create_difficulty_strips_whitespace_only_notes() {
        let req = CreateDifficultyRequest {
            algorithm_tag: "ml".into(),
            score: 5,
            notes: Some("   ".into()),
        };
        let (_, _, notes) = req.normalize().unwrap();
        assert_eq!(notes, None);
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
    fn create_difficulty_rejects_score_below_range() {
        let req = CreateDifficultyRequest {
            algorithm_tag: "human".into(),
            score: 0,
            notes: None,
        };
        assert!(req.normalize().is_err());
    }

    #[test]
    fn create_difficulty_rejects_score_above_range() {
        let req = CreateDifficultyRequest {
            algorithm_tag: "human".into(),
            score: 11,
            notes: None,
        };
        assert!(req.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // UpdateDifficultyRequest: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn update_difficulty_normalizes_valid_entry() {
        let req = UpdateDifficultyRequest {
            score: 8,
            notes: Some("revised".into()),
        };
        let (score, notes) = req.normalize().unwrap();
        assert_eq!(score, 8);
        assert_eq!(notes.as_deref(), Some("revised"));
    }

    #[test]
    fn update_difficulty_rejects_score_out_of_range() {
        let req = UpdateDifficultyRequest {
            score: 0,
            notes: None,
        };
        assert!(req.normalize().is_err());

        let req2 = UpdateDifficultyRequest {
            score: 11,
            notes: None,
        };
        assert!(req2.normalize().is_err());
    }

    // -----------------------------------------------------------------------
    // JSON deserialization: deny_unknown_fields
    // -----------------------------------------------------------------------

    #[test]
    fn update_description_json_rejects_unknown_fields() {
        let result: Result<UpdateDescriptionRequest, _> =
            serde_json::from_str(r#"{"description":"ok","extra":1}"#);
        assert!(result.is_err());
    }

    #[test]
    fn update_category_json_rejects_unknown_fields() {
        let result: Result<UpdateCategoryRequest, _> =
            serde_json::from_str(r#"{"category":"T","extra":1}"#);
        assert!(result.is_err());
    }

    #[test]
    fn update_tags_json_rejects_unknown_fields() {
        let result: Result<UpdateTagsRequest, _> =
            serde_json::from_str(r#"{"tags":["a"],"extra":1}"#);
        assert!(result.is_err());
    }

    #[test]
    fn update_status_json_rejects_unknown_fields() {
        let result: Result<UpdateStatusRequest, _> =
            serde_json::from_str(r#"{"status":"none","extra":1}"#);
        assert!(result.is_err());
    }

    #[test]
    fn create_difficulty_json_rejects_unknown_fields() {
        let result: Result<CreateDifficultyRequest, _> =
            serde_json::from_str(r#"{"algorithm_tag":"h","score":5,"extra":1}"#);
        assert!(result.is_err());
    }

    #[test]
    fn update_difficulty_json_rejects_unknown_fields() {
        let result: Result<UpdateDifficultyRequest, _> =
            serde_json::from_str(r#"{"score":5,"extra":1}"#);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // JSON deserialization: valid payloads
    // -----------------------------------------------------------------------

    #[test]
    fn update_description_json_parses_valid() {
        let req: UpdateDescriptionRequest =
            serde_json::from_str(r#"{"description":"hello"}"#).unwrap();
        assert_eq!(req.description, "hello");
    }

    #[test]
    fn update_tags_json_parses_valid() {
        let req: UpdateTagsRequest =
            serde_json::from_str(r#"{"tags":["a","b"]}"#).unwrap();
        assert_eq!(req.tags, vec!["a", "b"]);
    }

    #[test]
    fn create_difficulty_json_parses_with_optional_notes() {
        let req: CreateDifficultyRequest =
            serde_json::from_str(r#"{"algorithm_tag":"human","score":7}"#).unwrap();
        assert_eq!(req.algorithm_tag, "human");
        assert_eq!(req.score, 7);
        assert_eq!(req.notes, None);
    }

    #[test]
    fn create_difficulty_json_parses_with_notes() {
        let req: CreateDifficultyRequest =
            serde_json::from_str(r#"{"algorithm_tag":"ml","score":5,"notes":"test"}"#).unwrap();
        assert_eq!(req.notes.as_deref(), Some("test"));
    }

    // -----------------------------------------------------------------------
    // Role permission helpers
    // -----------------------------------------------------------------------

    #[test]
    fn role_can_upload_question_for_user_and_above() {
        use crate::api::auth::models::Role;
        assert!(!Role::Viewer.can_upload_question());
        assert!(Role::User.can_upload_question());
        assert!(Role::Leader.can_upload_question());
        assert!(Role::Bot.can_upload_question());
        assert!(Role::Admin.can_upload_question());
    }

    #[test]
    fn role_can_create_paper_for_leader_and_above() {
        use crate::api::auth::models::Role;
        assert!(!Role::Viewer.can_create_paper());
        assert!(!Role::User.can_create_paper());
        assert!(Role::Leader.can_create_paper());
        assert!(Role::Bot.can_create_paper());
        assert!(Role::Admin.can_create_paper());
    }

    #[test]
    fn role_is_admin_or_bot_only_for_admin_and_bot() {
        use crate::api::auth::models::Role;
        assert!(!Role::Viewer.is_admin_or_bot());
        assert!(!Role::User.is_admin_or_bot());
        assert!(!Role::Leader.is_admin_or_bot());
        assert!(Role::Bot.is_admin_or_bot());
        assert!(Role::Admin.is_admin_or_bot());
    }

    #[test]
    fn role_is_leader_or_above() {
        use crate::api::auth::models::Role;
        assert!(!Role::Viewer.is_leader_or_above());
        assert!(!Role::User.is_leader_or_above());
        assert!(Role::Leader.is_leader_or_above());
        assert!(Role::Bot.is_leader_or_above());
        assert!(Role::Admin.is_leader_or_above());
    }

    // -----------------------------------------------------------------------
    // Question filter validation: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn question_filter_rejects_invalid_category() {
        use crate::api::questions::queries::validate_question_filters;
        let params = QuestionsParams {
            paper_id: None,
            category: Some("X".into()),
            tag: None,
            reviewer: None,
            assigned_reviewer_id: None,
            score_min: None,
            score_max: None,
            difficulty_tag: None,
            difficulty_min: None,
            difficulty_max: None,
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: None,
            limit: None,
            offset: None,
        };
        assert!(validate_question_filters(&params).is_err());
    }

    #[test]
    fn question_filter_rejects_difficulty_min_without_tag() {
        use crate::api::questions::queries::validate_question_filters;
        let params = QuestionsParams {
            paper_id: None,
            category: None,
            tag: None,
            reviewer: None,
            assigned_reviewer_id: None,
            score_min: None,
            score_max: None,
            difficulty_tag: None,
            difficulty_min: Some(3),
            difficulty_max: None,
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: None,
            limit: None,
            offset: None,
        };
        assert!(validate_question_filters(&params).is_err());
    }

    #[test]
    fn question_filter_accepts_valid_filters() {
        use crate::api::questions::queries::validate_question_filters;
        let params = QuestionsParams {
            paper_id: None,
            category: Some("T".into()),
            tag: Some("optics".into()),
            reviewer: None,
            assigned_reviewer_id: None,
            score_min: Some(0),
            score_max: Some(100),
            difficulty_tag: Some("human".into()),
            difficulty_min: Some(1),
            difficulty_max: Some(10),
            created_after: Some("2026-01-01".into()),
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: Some("test".into()),
            limit: None,
            offset: None,
        };
        assert!(validate_question_filters(&params).is_ok());
    }

    #[test]
    fn question_filter_rejects_inverted_score_range() {
        use crate::api::questions::queries::validate_question_filters;
        let params = QuestionsParams {
            paper_id: None,
            category: None,
            tag: None,
            reviewer: None,
            assigned_reviewer_id: None,
            score_min: Some(100),
            score_max: Some(1),
            difficulty_tag: None,
            difficulty_min: None,
            difficulty_max: None,
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: None,
            limit: None,
            offset: None,
        };
        assert!(validate_question_filters(&params).is_err());
    }

    // -----------------------------------------------------------------------
    // Paper filter validation: positive & negative
    // -----------------------------------------------------------------------

    #[test]
    fn paper_filter_rejects_invalid_category() {
        use crate::api::papers::models::validate_paper_filters;
        let params = PapersParams {
            question_id: None,
            category: Some("X".into()),
            tag: None,
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: None,
            limit: None,
            offset: None,
        };
        assert!(validate_paper_filters(&params).is_err());
    }

    #[test]
    fn paper_filter_accepts_valid_filters() {
        use crate::api::papers::models::validate_paper_filters;
        let params = PapersParams {
            question_id: Some("550e8400-e29b-41d4-a716-446655440000".into()),
            category: Some("T".into()),
            tag: Some("optics".into()),
            created_after: Some("2026-01-01".into()),
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: Some("test".into()),
            limit: None,
            offset: None,
        };
        assert!(validate_paper_filters(&params).is_ok());
    }

    #[test]
    fn paper_filter_rejects_invalid_uuid() {
        use crate::api::papers::models::validate_paper_filters;
        let params = PapersParams {
            question_id: Some("not-a-uuid".into()),
            category: None,
            tag: None,
            created_after: None,
            created_before: None,
            updated_after: None,
            updated_before: None,
            q: None,
            limit: None,
            offset: None,
        };
        assert!(validate_paper_filters(&params).is_err());
    }
}
