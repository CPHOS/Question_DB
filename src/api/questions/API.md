# Questions API

> 题目的增删改查、文件替换、审阅人管理和批量打包接口。

- **`GET` 操作和 `POST /questions/bundles`**：需要任意已认证角色（`viewer` 及以上）
- **`POST /questions`（上传）**：需要 `user` / `leader` / `bot` / `admin`
- **`PATCH /questions/:id`（更新）**：权限分级，详见各端点说明
- **`PUT /questions/:id/file`（替换文件）**：需要 Full 访问权限（owner / leader / bot / admin）
- **`DELETE /questions/:id`**：需要 `leader` / `bot` / `admin`
- **审阅人管理**：需要 `leader` / `bot` / `admin`
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

### 权限模型

题目操作使用三级访问控制：

| 访问等级 | 条件 | 允许操作 |
|---|---|---|
| **Full** | admin；leader/bot（题目非 `used`）；user 且为 owner | 所有字段更新、文件替换 |
| **ReviewerOnly** | user 且被分配为审阅人 | 仅更新 difficulty tags（合并模式） |
| **None** | viewer；user 且非 owner 非审阅人 | 只读 |

---

## 数据结构

### `QuestionSummary`

```json
{
  "question_id": "uuid",
  "source": { "tex": "\\begin{problem}[20]\n..." },
  "category": "T",
  "status": "reviewed",
  "description": "热学标定 gamma",
  "score": 20,
  "author": "张三",
  "reviewers": ["李四"],
  "tags": ["optics", "thermodynamics"],
  "difficulty": {
    "human": { "score": 7, "notes": "较难", "updated_by": { "user_id": "uuid", "username": "alice", "display_name": "Alice" } }
  },
  "created_by": "uuid or null",
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z",
  "allow_auto_reviewer": false
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `question_id` | string(UUID) | 题目 ID |
| `source.tex` | string | tex 源码内容 |
| `category` | `"none"` \| `"T"` \| `"E"` | 分类：无 / 理论 / 实验 |
| `status` | `"none"` \| `"reviewed"` \| `"used"` | 状态 |
| `description` | string | 题目描述（用于命名和搜索） |
| `score` | int \| null | 从 tex `\begin{problem}[N]` 自动提取的分值 |
| `author` | string | 命题人 |
| `reviewers` | string[] | 审题人列表 |
| `tags` | string[] | 标签列表 |
| `difficulty` | object | 难度评估，key 为 tag（如 `human`），value 含 `score`(1-10)、可选 `notes` 和可选 `updated_by`（编辑者信息） |
| `allow_auto_reviewer` | boolean | 是否允许审阅人在保存审阅编辑时自动将姓名添加到审题人列表，默认 `false` |
| `created_by` | string(UUID) \| null | 创建者的 user_id，历史数据可能为 null |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

### `DifficultyValue`

每个难度标签的值对象：

```json
{
  "score": 7,
  "notes": "较难",
  "updated_by": {
    "user_id": "uuid",
    "username": "alice",
    "display_name": "Alice"
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `score` | int (1-10) | 难度分数 |
| `notes` | string \| null | 难度备注，可选 |
| `updated_by` | object \| null | 最后编辑此难度标签的用户信息，只读，历史数据可能为 null |
| `updated_by.user_id` | string(UUID) | 编辑者 ID |
| `updated_by.username` | string | 编辑者用户名 |
| `updated_by.display_name` | string | 编辑者显示名 |

> **注意**：`updated_by` 为只读字段，由服务端自动填充。创建/更新难度时请勿传入此字段，否则返回 `400`。

### `QuestionDetail`

在 `QuestionSummary` 基础上增加：

| 字段 | 类型 | 说明 |
|---|---|---|
| `tex_object_id` | string(UUID) | tex 文件的对象存储 ID |
| `assets` | `QuestionAssetRef[]` | 关联的资源文件列表 |
| `papers` | `QuestionPaperRef[]` | 包含此题的试卷列表（仅未软删试卷） |

### `QuestionAssetRef`

```json
{
  "path": "assets/fig1.png",
  "file_kind": "asset",
  "object_id": "uuid",
  "mime_type": "image/png"
}
```

### `QuestionPaperRef`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "sort_order": 1
}
```

### `QuestionTagsResponse`

```json
{
  "tags": ["kinematics", "lenses", "mechanics", "optics"]
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `tags` | string[] | 所有未软删除题目的去重标签列表，按字典序升序返回 |

### `QuestionDifficultyTagsResponse`

```json
{
  "difficulty_tags": ["algo1", "human"]
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `difficulty_tags` | string[] | 所有未软删除题目的去重难度标签列表，按字典序升序返回。前端建议默认选中 `human` |

---

## Endpoints

### `GET /questions`

按条件分页查询题目。

- **认证**：`viewer` 及以上
- **说明**：只返回未软删除题目

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `paper_id` | UUID | — | 按所属试卷过滤 |
| `category` | `"none"` \| `"T"` \| `"E"` | — | 按分类过滤 |
| `tag` | string | — | 按标签过滤 |
| `reviewer` | string | — | 按审核人过滤，精确匹配 `reviewers` 数组中的某一项 |
| `assigned_reviewer_id` | UUID | — | 按审阅人管理中分配的 reviewer_id（UUID）过滤 |
| `score_min` | int (≥0) | — | 分值下限 |
| `score_max` | int (≥0) | — | 分值上限 |
| `difficulty_tag` | string | — | 难度 tag，如 `human` |
| `difficulty_min` | int (1-10) | — | 难度下限（需同时有 `difficulty_tag`） |
| `difficulty_max` | int (1-10) | — | 难度上限（需同时有 `difficulty_tag`） |
| `q` | string | — | 关键词，ILIKE 匹配 `description` |
| `created_after` | string(ISO 8601) | — | 创建时间下限（含），如 `2026-01-01` 或 `2026-01-01T00:00:00Z` |
| `created_before` | string(ISO 8601) | — | 创建时间上限（含） |
| `updated_after` | string(ISO 8601) | — | 更新时间下限（含） |
| `updated_before` | string(ISO 8601) | — | 更新时间上限（含） |
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `QuestionSummary[]`。

---

### `GET /questions/tags`

返回所有未软删除题目当前使用中的标签列表，适合前端做自动补全或候选选择。

- **认证**：`viewer` 及以上
- **说明**：只统计未软删除题目；自动去重；按字典序升序返回

**成功响应** `200`：`QuestionTagsResponse` 对象。

---

### `GET /questions/difficulty-tags`

返回所有未软删除题目当前使用中的难度标签（algorithm_tag）列表，适合前端做下拉选择。

- **认证**：`viewer` 及以上
- **说明**：只统计未软删除题目；自动去重；按字典序升序返回；前端建议默认选中 `human`

**成功响应** `200`：`QuestionDifficultyTagsResponse` 对象。

---

### `GET /questions/:question_id`

返回单个题目完整详情。

- **认证**：`viewer` 及以上
- **路径参数**：`question_id` — UUID
- **说明**：只返回未软删除题目

**成功响应** `200`：`QuestionDetail` 对象。

**错误**：`404` — 题目不存在或已软删除

---

### `POST /questions`

上传新题目（zip 包）。

- **认证**：`user` / `leader` / `bot` / `admin`（即 `can_upload_question` 能力）
- **Content-Type**：`multipart/form-data`
- **大小限制**：zip 文件 ≤ 20 MiB

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `file` | binary (zip) | ✅ | 题目 zip 文件 |
| `description` | string | ✅ | 题目描述，非空；不能含 `/ \ : * ? " < > \|`，不能是 `.`/`..`，不能以 `.` 结尾 |
| `difficulty` | JSON string | ✅ | 难度对象，必须至少含 `human` key，score 1-10 |
| `category` | string | — | `"none"` \| `"T"` \| `"E"`，默认 `"none"` |
| `tags` | JSON string | — | 字符串数组，默认 `[]`，会去重，不允许空字符串元素 |
| `status` | string | — | `"none"` \| `"reviewed"` \| `"used"`，默认 `"none"` |
| `author` | string | — | 命题人，默认 `""` |
| `reviewers` | JSON string | — | 字符串数组，默认 `[]`，会去重，不允许空字符串元素 |

**ZIP 结构要求**：

- 逻辑根目录必须恰好一个 `.tex` 文件
- 可选一个 `assets/` 目录（内含引用的图片等资源）
- 逻辑根目录的其他杂散文件会被忽略，不参与导入
- 除根目录 tex / 杂散文件外，其他被导入的非根目录文件都必须位于 `assets/` 下
- 若最外层是单一包裹目录，会自动剥离一层
- 拒绝路径穿越（`..`）和绝对路径
- 总解压体积 ≤ 64 MiB

**自动行为**：

- `score` 自动从 tex 中的 `\begin{problem}[<score>]` 提取（整数或 null）
- `score` 不支持通过 PATCH 手动修改

**成功响应** `200`：

```json
{
  "question_id": "uuid",
  "file_name": "question.zip",
  "imported_assets": 2,
  "status": "imported"
}
```

**错误**：`400` — zip 格式错误 / 缺少 tex 文件 / 参数校验失败

**示例**：

```bash
curl -X POST http://127.0.0.1:8080/questions \
  -H "Authorization: Bearer <token>" \
  -F 'file=@question.zip;type=application/zip' \
  -F 'description=热学标定 gamma' \
  -F 'difficulty={"human":{"score":7}}' \
  -F 'category=T' \
  -F 'tags=["optics","thermodynamics"]'
```

---

### `PATCH /questions/:question_id`

部分更新题目元数据。

- **认证**：需已认证；根据访问等级决定可更新字段
  - **Full 访问**（admin / leader / bot（非 used）/ owner）：可更新所有字段，`difficulty` 为整体替换
  - **ReviewerOnly 访问**（被分配的审阅人）：仅可更新 `difficulty`（合并模式）和 `delete_difficulty_tags`
- **Content-Type**：`application/json`
- **路径参数**：`question_id` — UUID
- **说明**：至少提供一个字段；已软删除题目返回 `404`；使用行锁保证并发安全

**请求体字段**（均为可选，但至少提供一个）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `category` | `"none"` \| `"T"` \| `"E"` | 分类（ReviewerOnly 不可用） |
| `description` | string | 题目描述（不能为 null 或空串），需满足文件名安全规则（ReviewerOnly 不可用） |
| `tags` | string[] | 标签列表，整体替换；空数组 `[]` 表示清空（ReviewerOnly 不可用） |
| `status` | `"none"` \| `"reviewed"` \| `"used"` | 状态（ReviewerOnly 不可用） |
| `difficulty` | object | Full：整体替换难度评估，必须至少含 `human`，score 1-10。ReviewerOnly：合并模式，只能修改自己创建的 tag 或作为最后编辑者的 `human` tag，可新增 tag（无需含 `human`） |
| `delete_difficulty_tags` | string[] | 要删除的难度标签列表；不能包含 `human`。ReviewerOnly 只能删除自己创建的 tag |
| `author` | string | 命题人（ReviewerOnly 不可用） |
| `reviewers` | string[] | 审题人列表，会去重（ReviewerOnly 不可用） |
| `allow_auto_reviewer` | boolean | 是否允许自动添加审阅人到审题人列表（仅 Full 访问可设置，ReviewerOnly 不可用） |

**审阅人合并模式说明**：

- 审阅人提交 `difficulty` 时，不会替换其他人的标签，仅更新/新增自己有权限的标签
- 可修改的标签：自己创建的标签，或 `human` 标签（如果自己是最后编辑者）
- 新增的标签会记录 `created_by` 和 `updated_by` 为当前用户
- 删除标签时同样仅允许删除自己创建的标签

```json
{
  "category": "T",
  "tags": ["optics"],
  "difficulty": {
    "human": { "score": 8 }
  },
  "delete_difficulty_tags": ["obsolete_algo"]
}
```

**成功响应** `200`：更新后的 `QuestionDetail`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 无可更新字段 / 参数校验失败 / 未知字段 |
| `403` | 无编辑权限 / 审阅人尝试修改不允许的字段或标签 |
| `404` | 题目不存在或已软删除 |

---

### `PUT /questions/:question_id/file`

替换题目的 zip 文件内容（tex 和 assets），不修改元数据。

- **认证**：需 Full 访问权限（admin / leader / bot（非 used）/ owner）
- **Content-Type**：`multipart/form-data`
- **路径参数**：`question_id` — UUID
- **大小限制**：zip 文件 ≤ 20 MiB

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `file` | binary (zip) | ✅ | 新的题目 zip 文件（结构要求同创建） |

**行为**：

- 删除旧的 tex / asset 文件对象
- 写入新文件对象
- 更新 `source_tex_path`
- 重新提取 `score`
- 更新 `updated_at`
- 保留所有原有元数据（category、tags、difficulty 等）

**成功响应** `200`：

```json
{
  "question_id": "uuid",
  "file_name": "question_v2.zip",
  "source_tex_path": "main.tex",
  "imported_assets": 3,
  "status": "replaced"
}
```

**错误**：`404` — 题目不存在或已软删除

---

### `DELETE /questions/:question_id`

软删除题目。

- **认证**：`leader` / `bot` / `admin`
- **路径参数**：`question_id` — UUID

**行为**：

- 设置 `deleted_at` / `deleted_by` / `updated_at`
- 不会立刻删除文件对象，由管理员垃圾回收处理
- 已软删除题目重复删除返回 `404`
- 若题目仍被未软删除试卷引用，返回 `409`
- `leader`/`bot` 不能删除 `status=used` 的题目，`admin` 可以

**成功响应** `200`：

```json
{
  "question_id": "uuid",
  "status": "deleted"
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `404` | 题目不存在或已软删除 |
| `409` | 题目仍被未软删除试卷引用 |

---

### `POST /questions/bundles`

批量打包下载题目原始文件。

- **认证**：`viewer` 及以上
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `question_ids` | string(UUID)[] | ✅ | 题目 ID 列表，非空、去重、每项必须为有效 UUID |

```json
{
  "question_ids": ["uuid-1", "uuid-2", "uuid-3"]
}
```

**成功响应** `200`：

- **Content-Type**：`application/zip`
- **Header** 含 `content-disposition`（下载文件名）和 `content-length`

**ZIP 结构**：

```
manifest.json
热学标定gamma_550e84/
  main.tex
  assets/
    fig1.png
```

- `manifest.json`：题目清单元数据
- 每题目录命名规则：`{description}_{uuid前6位}/`
- 目录内含原始 `.tex` 和 `assets/` 资源

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 列表为空 / 含空值 / 含无效 UUID / 有重复 |
| `404` | 有题目不存在或已软删除 |

---

## 审阅人管理

### `GET /questions/:question_id/reviewers`

获取题目的审阅人列表。

- **认证**：任意已认证角色
- **路径参数**：`question_id` — UUID

**成功响应** `200`：

```json
{
  "reviewers": [
    {
      "reviewer_id": "uuid",
      "username": "alice",
      "display_name": "Alice",
      "assigned_by": "uuid",
      "created_at": "2026-01-01T00:00:00.000Z"
    }
  ]
}
```

---

### `POST /questions/:question_id/reviewers`

分配审阅人到题目。

- **认证**：`leader` / `bot` / `admin`
- **路径参数**：`question_id` — UUID
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `reviewer_id` | string(UUID) | ✅ | 要分配的用户 ID，必须是 `user` 角色且账号启用 |

```json
{
  "reviewer_id": "uuid"
}
```

**行为**：

- 重复分配同一审阅人不会报错（幂等）
- 只有 `user` 角色的用户可以被分配为审阅人

**成功响应** `200`：更新后的审阅人列表（格式同 GET）。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | reviewer 账号已停用 / reviewer 角色不是 `user` |
| `403` | 当前用户不是 leader/bot/admin |
| `404` | 题目不存在 / reviewer 用户不存在 |

---

### `DELETE /questions/:question_id/reviewers/:reviewer_id`

移除题目的审阅人。

- **认证**：`leader` / `bot` / `admin`
- **路径参数**：`question_id` — UUID，`reviewer_id` — UUID

**成功响应** `200`：更新后的审阅人列表（格式同 GET）。

**错误**：

| 状态码 | 场景 |
|---|---|
| `403` | 当前用户不是 leader/bot/admin |
