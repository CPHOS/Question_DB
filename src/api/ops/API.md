# Ops API

> 运维操作接口：数据导出、质量检查、数据库备份与恢复。批量打包接口见 [Questions API](../questions/API.md) 和 [Papers API](../papers/API.md)。

- 所有 Ops 接口需要 `admin` 角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

---

## Endpoints

### `GET /database/backup`

下载当前数据库的 plain SQL 备份文件。

- **认证**：`admin`
- **请求体**：无

**成功响应** `200`：

- **Content-Type**：`application/sql`
- **Header** 含 `content-disposition`（下载文件名）和 `content-length`
- **Body**：`pg_dump` 生成的 plain SQL，可按 [部署文档](../../../docs/DEPLOYMENT.md) 中的恢复方式导入

**说明**：

- 该接口直接返回下载文件，不写入 `QB_EXPORT_DIR`
- 备份包含 PostgreSQL 中的全部业务表和对象数据（包括 `objects` 表中的题目 zip / 试卷附件内容）
- 如果内置 `pg_dump` 与数据库 major version 不匹配，接口会返回具体错误提示；需要重建 API 镜像并对齐 PostgreSQL client 版本

---

### `POST /exports/run`

导出题目数据到文件。

- **认证**：`admin`
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `format` | `"jsonl"` \| `"csv"` | ✅ | — | 导出格式 |
| `public` | boolean | — | `false` | `true` 时不含 tex 源码 |
| `output_path` | string | — | 自动生成 | 相对于 `QB_EXPORT_DIR` 的路径 |

**路径安全规则**：

- `output_path` 必须为相对路径
- 不能包含 `..`（禁止目录逃逸）
- 最终文件写入 `QB_EXPORT_DIR` 下

```json
{
  "format": "jsonl",
  "public": false,
  "output_path": "exports/question_bank_internal.jsonl"
}
```

**导出内容**（只导出未软删除题目）：

| 字段 | JSONL | CSV | 说明 |
|---|:---:|:---:|---|
| question 基础字段 | ✅ | ✅ | question_id、category、status、description、score 等 |
| difficulty | ✅ | ✅ | 难度信息 |
| tags | ✅ | ✅ | 标签列表 |
| assets | ✅ | — | 资源文件引用（仅 JSONL） |
| tex_object_id | ✅ | — | tex 对象 ID（仅 JSONL） |
| tex_source | `public=false` 时 | — | tex 源码（仅 JSONL 且 `public=false`） |

**成功响应** `200`：

```json
{
  "format": "jsonl",
  "public": false,
  "output_path": "/absolute/path/to/exports/question_bank_internal.jsonl",
  "exported_questions": 42
}
```

---

### `POST /quality-checks/run`

运行数据质量检查。

- **认证**：`admin`
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `output_path` | string | — | 相对于 `QB_EXPORT_DIR` 的路径（同上安全规则） |

```json
{
  "output_path": "exports/quality_report.json"
}
```

**成功响应** `200`：

```json
{
  "output_path": "/absolute/path/to/exports/quality_report.json",
  "report": {
    "missing_tex_object": ["question-uuid-1"],
    "missing_tex_source": ["question-uuid-2"],
    "missing_asset_objects": [
      { "question_id": "uuid", "path": "assets/fig.png", "object_id": "uuid" }
    ],
    "empty_papers": ["paper-uuid-1"]
  }
}
```

**report 字段说明**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `missing_tex_object` | string[] | tex 对象记录缺失的题目 ID |
| `missing_tex_source` | string[] | tex 对象内容为空的题目 ID |
| `missing_asset_objects` | object[] | 资源对象缺失的条目 |
| `empty_papers` | string[] | 不含任何题目的试卷 ID |

---

### `POST /database/restore`

上传 plain SQL 备份并覆盖恢复当前数据库内容。

- **认证**：`admin`
- **Content-Type**：`multipart/form-data`
- **大小限制**：上传文件 ≤ 64 MiB

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `file` | binary (sql) | ✅ | 由 `GET /database/backup` 或 `pg_dump` 生成的 plain SQL 文件 |

**行为**：

- 先执行 `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`
- 再执行 `psql -v ON_ERROR_STOP=1 -f <uploaded.sql>` 导入上传文件
- 恢复流程与 [部署文档](../../../docs/DEPLOYMENT.md) 中“覆盖当前库”的恢复方法保持一致

**成功响应** `200`：

```json
{
  "file_name": "qb_backup.sql",
  "restored_bytes": 123456,
  "status": "restored"
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 缺少 `file` 字段 / 上传文件为空 / 文件超过 64 MiB |
| `500` | `psql` 恢复失败；响应里的 `error` 会尽量返回具体 stderr 提示。如果失败发生在清空 schema 之后，数据库可能已被部分覆盖 |
