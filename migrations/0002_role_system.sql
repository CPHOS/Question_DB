-- Migration: 5-role permission system with ownership tracking.

-- 1. Expand role enum: viewer, user, leader, bot, admin.
--    Drop existing role CHECK constraint (name varies by PG version).
DO $$
DECLARE
    _con TEXT;
BEGIN
    SELECT conname INTO _con
      FROM pg_constraint
     WHERE conrelid = 'users'::regclass
       AND contype = 'c'
       AND pg_get_constraintdef(oid) LIKE '%role%';
    IF _con IS NOT NULL THEN
        EXECUTE format('ALTER TABLE users DROP CONSTRAINT %I', _con);
    END IF;
END
$$;

-- Migrate existing editors to user (must run before adding new constraint).
UPDATE users SET role = 'user' WHERE role = 'editor';

-- Now add the new role constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = 'users'::regclass
           AND conname  = 'users_role_check'
    ) THEN
        ALTER TABLE users ADD CONSTRAINT users_role_check
            CHECK (role IN ('viewer', 'user', 'leader', 'bot', 'admin'));
    END IF;
END
$$;

-- 2. Leader expiry column.
ALTER TABLE users ADD COLUMN IF NOT EXISTS leader_expires_at TIMESTAMPTZ;

-- 3. Ownership tracking on questions and papers.
ALTER TABLE questions ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(user_id);
ALTER TABLE papers    ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(user_id);

-- 4. Difficulty tag ownership tracking.
ALTER TABLE question_difficulties ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(user_id);
ALTER TABLE question_difficulties ADD COLUMN IF NOT EXISTS updated_by UUID REFERENCES users(user_id);

-- 4a. Backfill NULL ownership to the first admin user (for old backups).
DO $$
DECLARE
    _admin_id UUID;
BEGIN
    SELECT user_id INTO _admin_id
      FROM users
     WHERE role = 'admin'
     ORDER BY created_at
     LIMIT 1;

    IF _admin_id IS NOT NULL THEN
        UPDATE questions SET created_by = _admin_id WHERE created_by IS NULL;
        UPDATE papers    SET created_by = _admin_id WHERE created_by IS NULL;
        UPDATE question_difficulties SET created_by = _admin_id WHERE created_by IS NULL;
        UPDATE question_difficulties SET updated_by = _admin_id WHERE updated_by IS NULL;
    END IF;
END
$$;

-- 5. Review assignment table (leader assigns user to review a question).
CREATE TABLE IF NOT EXISTS question_reviews (
    question_id UUID NOT NULL REFERENCES questions(question_id) ON DELETE CASCADE,
    reviewer_id UUID NOT NULL REFERENCES users(user_id),
    assigned_by UUID NOT NULL REFERENCES users(user_id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (question_id, reviewer_id)
);

CREATE INDEX IF NOT EXISTS idx_question_reviews_reviewer ON question_reviews(reviewer_id);
CREATE INDEX IF NOT EXISTS idx_questions_created_by ON questions(created_by);
CREATE INDEX IF NOT EXISTS idx_papers_created_by ON papers(created_by);

-- 6. Allow auto-reviewer flag on questions.
ALTER TABLE questions ADD COLUMN IF NOT EXISTS allow_auto_reviewer BOOLEAN NOT NULL DEFAULT false;
