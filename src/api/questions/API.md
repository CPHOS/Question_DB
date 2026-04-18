# Questions API

> 题目的增删改查、文件替换、审阅人管理、难度管理和批量打包接口。

所有请求需携带 `Authorization: Bearer <access_token>` 头。普通账号使用 JWT access token；bot 使用管理员签发的长期 access token。

### 权限模型（Trait-based）

题目写操作按功能拆分为独立端点，各角色可调用的 API 如下：

| 能力 | user | leader | reviewer (被分配) | admin/bot |
|---|---|---|---|---|
| 上传题目 | ✅ | ✅ | ✅ (作为 user) | ✅ |
| 修改 description | ✅ (自己的) | ✅ (非 used) | — | ✅ |
| 修改 category | ✅ (自己的) | ✅ (非 used) | — | ✅ |
| 修改 tags | ✅ (自己的) | ✅ (非 used) | ✅ (被分配的) | ✅ |
| 替换 file | ✅ (自己的) | ✅ (非 used) | — | ✅ |
| 修改 status | — | ✅ (非 used 题目, none/reviewed) | — | ✅ (任意) |
| 修改 author | — | — | — | ✅ |
| 修改 reviewer names | — | — | — | ✅ |
| 创建难度 | — | ✅ (非 used) | ✅ (被分配的) | ✅ |
| 修改难度 | — | ✅ (非 used) | ✅ (自己创建的) | ✅ |
| 删除难度 | — | ✅ (非 used) | ✅ (自己创建的) | ✅ |
| 管理 reviewers | — | ✅ | — | ✅ |
| 软删除 | — | ✅ (非 used) | — | ✅ |

**说明**：
- "自己的" 指 `created_by` 为当前用户的题目
- "非 used" 指 `status != 'used'` 的题目
- "被分配的" 指在 `question_reviews` 表中被 leader 分配为审阅人的题目（user 和 leader 均可被分配）
- reviewer 进行任意操作时，自动将其 display_name 加入 `questions.reviewers` 数组（去重）
- 替换文件时，后端仅更新题目文件、`source_tex_path`、从 TeX 提取出的 `score`、以及 `updated_at`
- 上传时，后端自动设置 difficulty 为空、status 为 `none`、author 为上传者 display_name、reviewers 为 `[]`
- 后端自动维护 `created_by`、`created_at`、`updated_at`
- 题目创建者（`created_by`）始终可修改自己题目的 description、category、tags 和 file，不受 status 限制

---

## 数据结构

### `QuestionSummary`

```json
{
  "question_id": "uuid",
  "source": { "tex": "problem.tex" },
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
| `source.tex` | string | tex 源码文件路径 |
| `category` | `"none"` \| `"T"` \| `"E"` | 分类 |
| `status` | `"none"` \| `"reviewed"` \| `"used"` | 状态 |
| `description` | string | 题目描述 |
| `score` | int \| null | 从 tex `\begin{problem}[N]` 自动提取的分值 |
| `author` | string | 命题人（上传 / 文件重置时自动设置为创建者 display_name） |
| `reviewers` | string[] | 审题人列表（reviewer 操作时自动追加） |
| `tags` | string[] | 标签列表 |
| `difficulty` | object | 难度评估，key 为 algorithm_tag，value 含 `score`(1-10)、可选 `notes` 和 `updated_by` |
| `allow_auto_reviewer` | boolean | 是否启用自动审阅人标记 |
| `created_by` | string(UUID) \| null | 创建者 user_id |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

### `QuestionDetail`

在 `QuestionSummary` 基础上增加 `tex_object_id`、`assets`、`papers`。

---

## Endpoints

### `GET /questions`

按条件分页查询题目。认证：`viewer` 及以上。

**Query 参数**：`paper_id`, `category`, `tag`, `author`, `reviewer`（支持逗号分隔多值，匹配任一）, `assigned_reviewer_id`, `score_min`, `score_max`, `difficulty_tag`, `difficulty_min`, `difficulty_max`, `q`, `created_after`, `created_before`, `updated_after`, `updated_before`, `limit` (1-100, 默认 20), `offset` (默认 0)。

- `tag` 仍用于简单单标签精确匹配
- 复杂标签组合查询请使用 `POST /questions/search`

**成功响应** `200`：分页包裹，`items` 为 `QuestionSummary[]`。

---

### `POST /questions/search`

按 JSON 逻辑树进行高级题目搜索。认证：`viewer` 及以上。

- **Content-Type**：`application/json`
- **说明**：除 `tag_filter` 外，其余字段与 `GET /questions` 的过滤字段一致；返回格式也一致

**请求体示例**：

```json
{
  "category": "T",
  "q": "pendulum",
  "limit": 20,
  "offset": 0,
  "tag_filter": {
    "type": "or",
    "children": [
      { "type": "tag", "tag": "mechanics" },
      {
        "type": "and",
        "children": [
          { "type": "tag", "tag": "contest" },
          {
            "type": "not",
            "child": { "type": "tag", "tag": "deprecated" }
          }
        ]
      }
    ]
  }
}
```

**`tag_filter` 节点结构**：

| `type` | 其他字段 | 说明 |
|---|---|---|
| `"tag"` | `tag: string` | 精确匹配单个标签 |
| `"and"` | `children: TagFilter[]` | 子条件全部满足 |
| `"or"` | `children: TagFilter[]` | 子条件任一满足 |
| `"not"` | `child: TagFilter` | 子条件不满足 |

**校验规则**：

- `tag_filter.tag` 会先 trim，不能为空
- `and` / `or` 的 `children` 不能为空数组
- 后端只接受固定 JSON 结构并把所有 tag 值作为 SQL 绑定参数处理，不接受原始 SQL / 表达式字符串

**成功响应** `200`：分页包裹，`items` 为 `QuestionSummary[]`。

---

### `GET /questions/tags`

返回未软删除题目的去重标签列表。认证：`viewer` 及以上。

---

### `GET /questions/difficulty-tags`

返回未软删除题目的去重难度标签列表。认证：`viewer` 及以上。

---

### `GET /questions/:question_id`

返回单个题目详情。认证：`viewer` 及以上。

---

### `POST /questions`

上传新题目（zip 包）。

- **认证**：`user` / `leader` / `bot` / `admin`
- **Content-Type**：`multipart/form-data`

**Multipart 字段**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `file` | binary (zip) | ✅ | 题目 zip 文件（≤ 20 MiB） |
| `description` | string | ✅ | 题目描述 |
| `category` | string | — | `"none"` \| `"T"` \| `"E"`，默认 `"none"` |
| `tags` | JSON string | — | 字符串数组，默认 `[]` |

**后端自动设置**：`difficulty` = 空、`status` = `"none"`、`author` = 上传者 display_name、`reviewers` = `[]`。

**成功响应** `200`：`{ "question_id", "file_name", "imported_assets", "status": "imported" }`

---

### `PUT /questions/:question_id/file`

替换题目的 zip 文件。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`multipart/form-data`
- **字段**：`file`（binary zip）

**后端行为**：仅替换题目文件，并更新 `source_tex_path`、从 TeX 提取出的 `score`、以及 `updated_at`；不会改动 `status`、`author`、`reviewers`、`difficulty`。

**成功响应** `200`：`{ "question_id", "file_name", "source_tex_path", "imported_assets", "status": "replaced" }`

---

### `PATCH /questions/:question_id/description`

更新题目描述。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "description": "string" }`

**成功响应** `200`：`QuestionDetail`

---

### `PATCH /questions/:question_id/category`

更新题目分类。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "category": "T" | "E" | "none" }`

**成功响应** `200`：`QuestionDetail`

---

### `PATCH /questions/:question_id/tags`

更新题目标签。

- **认证**：owner / leader（非 used）/ reviewer（被分配）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "tags": ["string", ...] }`
- **说明**：reviewer 操作时自动追加 display_name 到 `reviewers` 数组

**成功响应** `200`：`QuestionDetail`

---

### `PATCH /questions/:question_id/status`

更新题目状态。

- **认证**：leader（非 used 题目，只能设 `"none"` 或 `"reviewed"`）/ admin / bot（任意合法值）
- **Content-Type**：`application/json`
- **请求体**：`{ "status": "none" | "reviewed" | "used" }`

**成功响应** `200`：`QuestionDetail`

---

### `PATCH /questions/:question_id/author`

更新题目命题人。

- **认证**：admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "author": "string" }`

**成功响应** `200`：`QuestionDetail`

---

### `PATCH /questions/:question_id/reviewer-names`

更新题目审题人名称列表（`reviewers` 字符串数组）。

- **认证**：admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "reviewers": ["string", ...] }`
- **说明**：自动去重和 trim；允许设为空数组

**成功响应** `200`：`QuestionDetail`

---

### `POST /questions/:question_id/difficulties`

创建难度条目。

- **认证**：reviewer（被分配）/ leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "algorithm_tag": "string", "score": 1-10, "notes": "string (optional)" }`
- **说明**：`created_by` 自动设为当前用户；reviewer 操作时追加 display_name 到 `reviewers`

**成功响应** `200`：`QuestionDetail`

**错误**：`409` — algorithm_tag 已存在

---

### `PATCH /questions/:question_id/difficulties/:algorithm_tag`

更新难度条目。

- **认证**：reviewer（仅自己创建的）/ leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "score": 1-10, "notes": "string (optional)" }`
- **说明**：`updated_by` 自动更新为当前用户

**成功响应** `200`：`QuestionDetail`

**错误**：`404` — algorithm_tag 不存在；`403` — reviewer 试图修改他人创建的条目

---

### `DELETE /questions/:question_id/difficulties/:algorithm_tag`

删除难度条目。

- **认证**：reviewer（仅自己创建的）/ leader（非 used）/ admin / bot

**成功响应** `200`：`QuestionDetail`

**错误**：`404` — algorithm_tag 不存在；`403` — reviewer 试图删除他人创建的条目

---

### `POST /questions/:question_id/reviewers`

分配审阅人（写入 `question_reviews` 表）。目标用户必须为活跃的 `user` 或 `leader` 角色。

- **认证**：leader / bot / admin
- **Content-Type**：`application/json`
- **请求体**：`{ "reviewer_id": "uuid" }`

**成功响应** `200`：`{ "reviewers": [QuestionReviewer] }`

---

### `DELETE /questions/:question_id/reviewers/:reviewer_id`

移除审阅人。

- **认证**：leader / bot / admin

**成功响应** `200`：`{ "reviewers": [QuestionReviewer] }`

---

### `GET /questions/:question_id/reviewers`

列出已分配的审阅人。认证：`viewer` 及以上。

---

### `DELETE /questions/:question_id`

软删除题目。

- **认证**：leader（非 used）/ admin / bot

**前置检查**：题目不能被活跃试卷引用。

**成功响应** `200`：`{ "question_id", "status": "deleted" }`

---

### `POST /questions/bundles`

批量下载题目 zip 打包。认证：`viewer` 及以上。
