# v2.0.0 Legacy Cleanup

v1.2.0 引入了文件系统对象存储，但保留了 BYTEA 回退和 `.sql` restore 兼容。
v2.0.0 删除这些过渡代码。**前提：所有部署至少运行过一次 v1.2.0**。

## 1. 新增 SQL migration

`migrations/0006_drop_legacy_bytea.sql`:

```sql
-- 确认不存在未迁移的行
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM objects WHERE storage_path IS NULL LIMIT 1) THEN
    RAISE EXCEPTION 'unmigrated BYTEA objects exist — run v1.2.0 first';
  END IF;
END $$;

ALTER TABLE objects DROP COLUMN content;
ALTER TABLE objects ALTER COLUMN storage_path SET NOT NULL;
ALTER TABLE objects ALTER COLUMN content_hash SET NOT NULL;
```

## 2. 新增 schema_migrations 跟踪表

`migrations/0007_schema_migrations.sql`:

```sql
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Rust 端新增 `apply_pending_migrations(pool)` 函数：扫描 `migrations/*.sql`，
跳过 `schema_migrations` 中已存在的 version，执行后插入记录。
`main.rs` 启动时调用，restore 后也调用（替代当前的逐文件重跑）。

## 3. 删除 legacy Rust 代码

| 文件 | 位置 | 改动 |
|------|------|------|
| `src/api/shared/db.rs` | `fetch_object_bytes` | 删除 BYTEA fallback 分支（`content` 列已不存在），只读文件系统 |
| `src/api/shared/db.rs` | `fetch_object_meta` | `storage_path`/`content_hash` 改为非 `Option` |
| `src/api/shared/db.rs` | `migrate_legacy_objects` | 删除整个方法 |
| `src/api/shared/db.rs` | `insert_object_tx` | SQL 中删除 `content` 字段 |
| `src/main.rs` | L33-40 | 删除 `migrate_legacy_objects()` 调用 |
| `src/api/ops/quality.rs` | `object_blob_nonempty` | 删除 `octet_length(content)` legacy 分支 |
| `src/api/ops/database.rs` | `restore_database_backup` | 删除 legacy raw `.sql` restore 路径，只保留 tar.gz |
| `src/api/ops/database.rs` | `temp_restore_upload_path` | 删除 `.sql` 扩展名处理 |

## 4. 版本号

`Cargo.toml` → `version = "2.0.0"`。Major bump 因为：
- backup restore 不再接受旧 `.sql` 格式
- `objects` 表 schema 变化（DROP COLUMN content）
- 必须从 v1.2.0+ 升级，不可跳版本
