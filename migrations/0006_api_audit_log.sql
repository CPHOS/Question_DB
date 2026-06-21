-- API audit log: records every HTTP API call (method, path, status, duration,
-- caller identity, client IP) so administrators can review and export activity.
CREATE TABLE IF NOT EXISTS api_audit_log (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,
    query       TEXT,
    status_code INT NOT NULL,
    duration_ms INT NOT NULL,
    user_id     TEXT,
    username    TEXT,
    role        TEXT,
    client_ip   TEXT,
    user_agent  TEXT
);

CREATE INDEX IF NOT EXISTS api_audit_log_created_at_idx ON api_audit_log (created_at DESC);
CREATE INDEX IF NOT EXISTS api_audit_log_user_id_idx    ON api_audit_log (user_id);
CREATE INDEX IF NOT EXISTS api_audit_log_status_code_idx ON api_audit_log (status_code);
