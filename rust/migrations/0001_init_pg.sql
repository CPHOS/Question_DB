-- PostgreSQL initial schema for Rust rewrite (phase 1)

CREATE TABLE IF NOT EXISTS objects (
    object_id UUID PRIMARY KEY,
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    mime_type TEXT,
    storage_class TEXT NOT NULL DEFAULT 'hot',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT,
    encryption TEXT NOT NULL DEFAULT 'sse'
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_objects_bucket_key ON objects(bucket, object_key);
CREATE UNIQUE INDEX IF NOT EXISTS ux_objects_hash_size ON objects(sha256, size_bytes);

CREATE TABLE IF NOT EXISTS papers (
    paper_id TEXT PRIMARY KEY,
    edition TEXT NOT NULL,
    paper_type TEXT NOT NULL,
    title TEXT NOT NULL,
    paper_tex_object_id UUID REFERENCES objects(object_id),
    source_pdf_object_id UUID REFERENCES objects(object_id),
    question_index_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS questions (
    question_id TEXT PRIMARY KEY,
    paper_id TEXT NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    paper_index INT NOT NULL,
    question_no TEXT,
    category TEXT NOT NULL,
    question_tex_object_id UUID REFERENCES objects(object_id),
    answer_tex_object_id UUID REFERENCES objects(object_id),
    search_text TEXT,
    status TEXT NOT NULL DEFAULT 'raw',
    tags_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (paper_id, paper_index)
);

CREATE INDEX IF NOT EXISTS idx_questions_paper_id ON questions(paper_id);
CREATE INDEX IF NOT EXISTS idx_questions_status ON questions(status);
