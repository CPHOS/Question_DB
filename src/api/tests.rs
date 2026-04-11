#[cfg(test)]
mod tests {
    use crate::api::{papers::models::PapersParams, questions::models::QuestionsParams};

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
}
