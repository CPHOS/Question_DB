-- Migrate binary object storage from BYTEA blobs to filesystem-backed storage.
-- After this migration, new objects are written to disk and the `content` column
-- is left NULL.  Existing rows keep their BYTEA data until the application-level
-- auto-migration moves them to the filesystem on startup.

ALTER TABLE objects ADD COLUMN IF NOT EXISTS content_hash TEXT;
ALTER TABLE objects ADD COLUMN IF NOT EXISTS storage_path TEXT;
ALTER TABLE objects ALTER COLUMN content DROP NOT NULL;
