# Ops API

> 运维操作接口：数据导出和质量检查。批量打包接口见 [Questions API](../questions/API.md) 和 [Papers API](../papers/API.md)。

- 所有 Ops 接口需要 `editor` 及以上角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

---

## Endpoints

### `POST /exports/run`

导出题目数据到文件。

- **认证**：`editor` 及以上
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

- **认证**：`editor` 及以上
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