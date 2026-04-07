# Papers API

> 试卷的增删改查、附录文件替换和批量打包接口。

- **`GET` 操作**：需要 `viewer` 及以上角色
- **`POST / PATCH / DELETE / PUT` 操作**：需要 `editor` 及以上角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

---

## 数据结构

### `PaperSummary`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "question_count": 5,
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z"
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `paper_id` | string(UUID) | 试卷 ID |
| `description` | string | 试卷描述 |
| `title` | string | 试卷标题 |
| `subtitle` | string | 试卷副标题 |
| `question_count` | int | 包含的题目数量 |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

### `PaperDetail`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z",
  "questions": [ /* QuestionSummary[] — 按 sort_order 排序 */ ]
}
```

`questions` 数组每个元素为完整的 `QuestionSummary`（含 source.tex、tags、difficulty 等全部字段）。

---

## Endpoints

### `GET /papers`

按条件分页查询试卷。

- **认证**：`viewer` 及以上
- **说明**：只返回未软删除试卷

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `question_id` | UUID | — | 按包含指定题目过滤 |
| `category` | `"none"` \| `"T"` \| `"E"` | — | 按包含题目的分类过滤 |
| `tag` | string | — | 按包含题目的标签过滤 |
| `q` | string | — | 关键词，匹配 `description`、`title`、`subtitle` |
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `PaperSummary[]`。

---

### `GET /papers/:paper_id`

返回试卷详情和按顺序展开的题目列表。

- **认证**：`viewer` 及以上
- **路径参数**：`paper_id` — UUID
- **说明**：只返回未软删除试卷；`questions` 中仅含未软删除题目

**成功响应** `200`：`PaperDetail` 对象。

**错误**：`404` — 试卷不存在或已软删除

---

### `POST /papers`

创建试卷。

- **认证**：`editor` 及以上
- **Content-Type**：`multipart/form-data`

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `description` | string | ✅ | 试卷描述，非空；需满足文件名安全规则（不能含 `/ \ : * ? " < > \|`，不能是 `.`/`..`，不能以 `.` 结尾） |
| `title` | string | ✅ | 试卷标题，非空，不允许控制字符 |
| `subtitle` | string | ✅ | 试卷副标题，非空，不允许控制字符 |
| `question_ids` | JSON string | ✅ | UUID 数组，非空、去重，如 `["uuid-1","uuid-2"]` |
| `file` | binary (zip) | — | 附录 zip 文件（可选）；若提供须为合法 zip 且 ≤ 20 MiB |

**题目约束**：

- 所有 `question_id` 必须存在且未软删除
- 所有题目的 `category` 必须同为 `T` 或同为 `E`
- 所有题目的 `status` 必须是 `reviewed` 或 `used`

**说明**：

- 题目按 `question_ids` 数组顺序写入关联
- 命题人和审题人由题目级别维护，组卷 bundle 时从题目中汇总

**成功响应** `200`：

```json
{
  "paper_id": "uuid",
  "file_name": "paper_appendix.zip",
  "status": "imported",
  "question_count": 5
}
```

`file_name` 在未上传附录时为 `null`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 参数校验失败 / zip 无效 / 题目 category 不一致 / 题目 status 不合规 |
| `404` | 有题目不存在或已软删除 |

**示例**：

```bash
curl -X POST http://127.0.0.1:8080/papers \
  -H "Authorization: Bearer <token>" \
  -F 'description=综合训练试卷 A' \
  -F 'title=综合训练 2026 A 卷' \
  -F 'subtitle=校内选拔 初版' \
  -F 'question_ids=["uuid-1","uuid-2"]' \
  -F 'file=@paper_appendix.zip;type=application/zip'
```

---

### `PATCH /papers/:paper_id`

部分更新试卷元数据和题目列表。

- **认证**：`editor` 及以上
- **Content-Type**：`application/json`
- **路径参数**：`paper_id` — UUID
- **说明**：至少提供一个字段；已软删除试卷返回 `404`

**请求体字段**（均为可选，但至少提供一个）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `description` | string | 试卷描述（不能为 null 或空），需满足文件名安全规则 |
| `title` | string | 试卷标题（不能为 null 或空） |
| `subtitle` | string | 试卷副标题（不能为 null 或空） |
| `question_ids` | string(UUID)[] | 题目列表，非空数组、去重；更新后按数组顺序重排 |

**行为**：

- 锁定试卷行，防止并发更新
- 对更新后的最终题目集合执行与创建相同的约束校验（category 一致性、status 合规性）
- 若更新了 `question_ids`，会删除旧关联并按新顺序重建

```json
{
  "title": "综合训练 2026 A 卷（修订）",
  "question_ids": ["uuid-3", "uuid-1", "uuid-2"]
}
```

**成功响应** `200`：更新后的 `PaperDetail`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 无可更新字段 / 参数校验失败 / 未知字段 / 题目约束不满足 |
| `404` | 试卷不存在或已软删除 / 有题目不存在 |

---

### `PUT /papers/:paper_id/file`

替换试卷的附录 zip 文件。

- **认证**：`editor` 及以上
- **Content-Type**：`multipart/form-data`
- **路径参数**：`paper_id` — UUID
- **大小限制**：≤ 20 MiB

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `file` | binary (zip) | ✅ | 新的附录 zip 文件，必须为合法 zip |

**行为**：

- 写入新的 appendix object
- 更新 `append_object_id`
- 删除旧的 appendix object（如果存在）
- 更新 `updated_at`

**成功响应** `200`：

```json
{
  "paper_id": "uuid",
  "file_name": "paper_appendix_v2.zip",
  "status": "replaced"
}
```

**错误**：`404` — 试卷不存在或已软删除

---

### `DELETE /papers/:paper_id`

软删除试卷。

- **认证**：`editor` 及以上
- **路径参数**：`paper_id` — UUID

**行为**：

- 设置 `deleted_at` / `deleted_by` / `updated_at`
- 不会立刻删除 appendix 文件对象，由管理员垃圾回收处理
- 已软删除试卷重复删除返回 `404`

**成功响应** `200`：

```json
{
  "paper_id": "uuid",
  "status": "deleted"
}
```

---

### `POST /papers/bundles`

批量打包下载试卷（含自动排版的 main.tex）。

- **认证**：`editor` 及以上
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `paper_ids` | string(UUID)[] | ✅ | 试卷 ID 列表，非空、去重、每项必须为有效 UUID |

```json
{
  "paper_ids": ["uuid-1", "uuid-2"]
}
```

**成功响应** `200`：

- **Content-Type**：`application/zip`
- **Header** 含 `content-disposition` 和 `content-length`

**ZIP 结构**：

```
manifest.json
综合训练试卷A_550e84/
  main.tex
  assets/
    fig1.png
    fig2.pdf
  append.zip
```

- `manifest.json`：试卷和题目清单元数据
- 每个试卷目录命名：`{description}_{uuid前6位}/`
- `main.tex`：基于内置 CPHOS-LaTeX 模板自动生成
  - 依据题目 `category` 选择理论 (`cphos.cls`) 或实验 (`cphos-e.cls`) 模板
  - 按试卷中的顺序注入题目 `\begin{problem}[score]...\end{problem}` 环境
  - `\includegraphics` 路径自动改写到合并后的 `assets/` 目录
  - `\label` / `\ref` / `\eqref` 等标签自动添加前缀（`p1-`、`p2-`…）防止跨题冲突
  - 命题人（`author`）从题目中按顺序汇总去重
  - 审题人（`reviewers`）从所有题目中收集去重
- `assets/`：所有题目的资源文件合并目录
- `append.zip`：试卷附录文件（如果存在）

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 列表为空 / 含无效 UUID / 有重复 |
| `404` | 有试卷不存在或已软删除 |