-- Migration: bot accounts use long-lived opaque access tokens instead of passwords.

ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL;

ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_token_hash TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS bot_token_created_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_bot_token_hash
    ON users(bot_token_hash)
    WHERE bot_token_hash IS NOT NULL;

-- Existing bot accounts should no longer authenticate with passwords or refresh tokens.
UPDATE users
SET password_hash = NULL,
    bot_token_hash = NULL,
    bot_token_created_at = NULL
WHERE role = 'bot';

UPDATE refresh_tokens rt
SET revoked_at = COALESCE(rt.revoked_at, NOW())
FROM users u
WHERE rt.user_id = u.user_id
  AND u.role = 'bot'
  AND rt.revoked_at IS NULL;
