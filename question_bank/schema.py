from __future__ import annotations

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS papers (
        paper_id TEXT PRIMARY KEY,
        edition INTEGER NOT NULL,
        paper_type TEXT NOT NULL CHECK(paper_type IN ('regular', 'semifinal', 'final', 'other')),
        title TEXT NOT NULL,
        paper_latex_path TEXT NOT NULL,
        paper_latex_source TEXT NOT NULL DEFAULT '',
        source_pdf_path TEXT,
        question_index_json TEXT NOT NULL DEFAULT '[]',
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS questions (
        question_id TEXT PRIMARY KEY,
        paper_id TEXT NOT NULL,
        paper_index INTEGER NOT NULL,
        question_no TEXT NOT NULL,
        category TEXT NOT NULL CHECK(category IN ('theory', 'experiment')),
        latex_path TEXT NOT NULL,
        latex_source TEXT NOT NULL DEFAULT '',
        answer_latex_path TEXT,
        answer_latex_source TEXT,
        latex_anchor TEXT,
        search_text TEXT,
        status TEXT NOT NULL CHECK(status IN ('raw', 'reviewed', 'published')),
        tags_json TEXT NOT NULL DEFAULT '[]',
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
        UNIQUE (paper_id, paper_index),
        UNIQUE (paper_id, question_no)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS question_assets (
        asset_id TEXT PRIMARY KEY,
        question_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('statement_image', 'answer_image', 'figure')),
        file_path TEXT NOT NULL,
        sha256 TEXT NOT NULL,
        caption TEXT,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        FOREIGN KEY (question_id) REFERENCES questions(question_id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS score_workbooks (
        workbook_id TEXT PRIMARY KEY,
        paper_id TEXT NOT NULL,
        exam_session TEXT NOT NULL,
        workbook_kind TEXT NOT NULL,
        source_filename TEXT NOT NULL,
        file_path TEXT,
        mime_type TEXT NOT NULL,
        sheet_names_json TEXT NOT NULL DEFAULT '[]',
        file_size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        workbook_blob BLOB NOT NULL,
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (paper_id) REFERENCES papers(paper_id),
        UNIQUE (paper_id, exam_session, source_filename)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS question_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_id TEXT NOT NULL,
        exam_session TEXT NOT NULL,
        source_workbook_id TEXT,
        participant_count INTEGER NOT NULL,
        avg_score REAL NOT NULL,
        score_std REAL NOT NULL,
        full_mark_rate REAL NOT NULL,
        zero_score_rate REAL NOT NULL,
        max_score REAL NOT NULL,
        min_score REAL NOT NULL,
        stats_source TEXT NOT NULL,
        stats_version TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (question_id) REFERENCES questions(question_id),
        FOREIGN KEY (source_workbook_id) REFERENCES score_workbooks(workbook_id),
        UNIQUE (question_id, exam_session, stats_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS difficulty_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        question_id TEXT NOT NULL,
        exam_session TEXT,
        manual_level TEXT,
        derived_score REAL,
        method TEXT NOT NULL,
        method_version TEXT NOT NULL,
        confidence REAL,
        feature_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        FOREIGN KEY (question_id) REFERENCES questions(question_id),
        UNIQUE (question_id, exam_session, method, method_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS import_runs (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_label TEXT NOT NULL,
        bundle_path TEXT NOT NULL,
        dry_run INTEGER NOT NULL,
        status TEXT NOT NULL,
        item_count INTEGER NOT NULL DEFAULT 0,
        warning_count INTEGER NOT NULL DEFAULT 0,
        error_count INTEGER NOT NULL DEFAULT 0,
        details_json TEXT NOT NULL DEFAULT '{}',
        started_at TEXT NOT NULL,
        finished_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_questions_paper ON questions(paper_id)",
    "CREATE INDEX IF NOT EXISTS idx_question_stats_question ON question_stats(question_id)",
    "CREATE INDEX IF NOT EXISTS idx_question_assets_question ON question_assets(question_id)",
    "CREATE INDEX IF NOT EXISTS idx_score_workbooks_paper ON score_workbooks(paper_id)",
]

MIGRATION_COLUMNS = {
    "papers": {
        "paper_latex_source": "TEXT NOT NULL DEFAULT ''",
    },
    "questions": {
        "latex_source": "TEXT NOT NULL DEFAULT ''",
        "answer_latex_source": "TEXT",
    },
}
