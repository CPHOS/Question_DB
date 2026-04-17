CREATE INDEX IF NOT EXISTS idx_question_tags_tag_question_id
    ON question_tags(tag, question_id);
