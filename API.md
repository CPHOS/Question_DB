# Question Bank API 文档

> 本文档完整描述后端所有 HTTP API 端点。前端开发者请据此对接。

## 目录

- [全局约定](#全局约定)
- [认证与权限](#认证与权限)
- [System — 系统](#system--系统)
- [Auth — 认证](#auth--认证)
- [Questions — 题目](#questions--题目)
- [Papers — 试卷](#papers--试卷)
- [Ops — 运维操作](#ops--运维操作)
- [Admin — 管理员](#admin--管理员)

---

## 全局约定

### Base URL

所有路径相对于服务根，例如 `http://localhost:8080`。

### 统一错误格式

```json
{
  "error": "错误描述文本"
}
```

| HTTP 状态码 | 含义 |
|---|---|
| `400` | 请求参数不合法 |
| `401` | 未认证（缺少 / 无效 / 过期的 access token） |
| `403` | 无权限（角色不满足要求） |
| `404` | 资源不存在（或已软删除） |
| `409` | 操作冲突（如删除仍被引用的题目、恢复未被删除的记录等） |
| `500` | 内部错误 |
| `503` | 服务不可用（数据库不可达） |

### 分页响应格式

所有列表接口使用统一分页包裹：

```json
{
  "items": [ ... ],
  "total": 42,
  "limit": 20,
  "offset": 0
}
```

- `limit` 默认 `20`，范围 `1..100`
- `offset` 默认 `0`，最小 `0`

### 未知字段策略

`PATCH` / `POST` 的 JSON 请求体启用了 **deny_unknown_fields**，传入未定义字段会返回 `400`。

---

## 认证与权限

### 认证方式

- **Access Token**：JWT (HS256)，有效期 **1800 秒（30 分钟）**
- **Refresh Token**：不透明 UUID 字符串，有效期 **7 天**，一次性消费（轮换）
- **传递方式**：`Authorization: Bearer <access_token>`
- **密码存储**：Argon2id

### 角色

| 角色 | 说明 |
|---|---|
| `viewer` | 只读 + bundle 下载（查询题目、试卷、下载 bundles） |
| `editor` | 读写 |
| `admin` | 全部权限 + ops 操作 + 用户管理 + 垃圾回收 |

### 权限矩阵

| 端点 | 公开 | viewer | editor | admin |
|---|:---:|:---:|:---:|:---:|
| `GET /health` | ✅ | ✅ | ✅ | ✅ |
| `POST /auth/login` | ✅ | — | — | — |
| `POST /auth/refresh` | ✅ | — | — | — |
| `GET /auth/me` | — | ✅ | ✅ | ✅ |
| `PATCH /auth/me/password` | — | ✅ | ✅ | ✅ |
| `POST /auth/logout` | — | ✅ | ✅ | ✅ |
| `GET /questions`、`GET /questions/tags`、`GET /papers` | — | ✅ | ✅ | ✅ |
| `GET /questions/:id`、`GET /papers/:id` | — | ✅ | ✅ | ✅ |
| `POST /questions/bundles`、`POST /papers/bundles` | — | ✅ | ✅ | ✅ |
| `POST/PATCH/PUT/DELETE` questions | — | — | ✅ | ✅ |
| `POST/PATCH/PUT/DELETE` papers | — | — | ✅ | ✅ |
| ops (exports / quality / db backup / db restore) | — | — | — | ✅ |
| `/admin/*` | — | — | — | ✅ |

### 初始账号

首次启动且 `users` 表为空时自动创建：

- 用户名：`admin`
- 密码：`changeme`
- 角色：`admin`

**请首次登录后立即修改密码。**

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `QB_JWT_SECRET` | `qb-dev-secret-change-me-in-production` | JWT 签名密钥，**生产必须修改** |

---

## System — 系统

### `GET /health`

健康检查，探测数据库连通性。无需认证。

**成功响应** `200`：

```json
{
  "status": "ok",
  "service": "qb_api_rust"
}
```

**数据库不可达** `503`：

```json
{
  "error": "database is unreachable"
}
```

---

## Auth — 认证

### `POST /auth/login`

用户名密码登录，获取 token 对。

- **认证**：无需
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `username` | string | ✅ | 用户名，不能为空 |
| `password` | string | ✅ | 密码，不能为空 |

```json
{
  "username": "admin",
  "password": "changeme"
}
```

**成功响应** `200`：

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000",
  "token_type": "Bearer",
  "expires_in": 1800
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 缺少 username 或 password |
| `401` | 用户名或密码错误 / 账号已停用 |

---

### `POST /auth/refresh`

使用 refresh token 换取新 token 对。旧 refresh token 消费后立即失效（轮换机制）。

- **认证**：无需
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `refresh_token` | string | ✅ | 之前获得的 refresh token UUID |

```json
{
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000"
}
```

**成功响应** `200`：格式同 login。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 缺少 refresh_token |
| `401` | refresh token 无效 / 已过期 / 已被消费 / 账号停用 |

---

### `POST /auth/logout`

撤销指定 refresh token。

- **认证**：`viewer` 及以上
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `refresh_token` | string | ✅ | 要撤销的 refresh token；传空字符串也会返回成功 |

```json
{
  "refresh_token": "550e8400-e29b-41d4-a716-446655440000"
}
```

**成功响应** `200`：

```json
{
  "message": "logged out"
}
```

---

### `GET /auth/me`

获取当前登录用户信息。

- **认证**：`viewer` 及以上

**成功响应** `200`：

```json
{
  "user_id": "uuid",
  "username": "admin",
  "display_name": "Administrator",
  "role": "admin",
  "is_active": true,
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z"
}
```

**`UserProfile` 字段说明**：

| 字段 | 类型 | 说明 |
|---|---|---|
| `user_id` | string(UUID) | 用户 ID |
| `username` | string | 用户名 |
| `display_name` | string | 显示名 |
| `role` | `"viewer"` \| `"editor"` \| `"admin"` | 角色 |
| `is_active` | boolean | 是否启用 |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

---

### `PATCH /auth/me/password`

修改当前用户密码。

- **认证**：`viewer` 及以上
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `old_password` | string | ✅ | 当前密码 |
| `new_password` | string | ✅ | 新密码，长度 ≥ 6 |

```json
{
  "old_password": "changeme",
  "new_password": "new-secure-password"
}
```

**成功响应** `200`：

```json
{
  "message": "password changed"
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 新密码少于 6 个字符 |
| `401` | 旧密码不正确 |
| `404` | 用户不存在 |

---

## Questions — 题目

### 数据结构

#### `QuestionSummary`

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
    "human": { "score": 7, "notes": "较难" }
  },
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z"
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
| `difficulty` | object | 难度评估，key 为 tag（如 `human`），value 含 `score`(1-10) 和可选 `notes` |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

#### `QuestionDetail`

在 `QuestionSummary` 基础上增加：

| 字段 | 类型 | 说明 |
|---|---|---|
| `tex_object_id` | string(UUID) | tex 文件的对象存储 ID |
| `assets` | `QuestionAssetRef[]` | 关联的资源文件列表 |
| `papers` | `QuestionPaperRef[]` | 包含此题的试卷列表（仅未软删试卷） |

#### `QuestionAssetRef`

```json
{
  "path": "assets/fig1.png",
  "file_kind": "asset",
  "object_id": "uuid",
  "mime_type": "image/png"
}
```

#### `QuestionPaperRef`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "sort_order": 1
}
```

#### `QuestionTagsResponse`

```json
{
  "tags": ["kinematics", "lenses", "mechanics", "optics"]
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `tags` | string[] | 所有未软删除题目的去重标签列表，按字典序升序返回 |

---

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
| `score_min` | int (≥0) | — | 分值下限 |
| `score_max` | int (≥0) | — | 分值上限 |
| `difficulty_tag` | string | — | 难度 tag，如 `human` |
| `difficulty_min` | int (1-10) | — | 难度下限（需同时有 `difficulty_tag`） |
| `difficulty_max` | int (1-10) | — | 难度上限（需同时有 `difficulty_tag`） |
| `q` | string | — | 关键词，ILIKE 匹配 `description` |
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

- **认证**：`editor` 及以上
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

- **认证**：`editor` 及以上
- **Content-Type**：`application/json`
- **路径参数**：`question_id` — UUID
- **说明**：至少提供一个字段；已软删除题目返回 `404`；使用行锁保证并发安全

**请求体字段**（均为可选，但至少提供一个）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `category` | `"none"` \| `"T"` \| `"E"` | 分类 |
| `description` | string | 题目描述（不能为 null 或空串），需满足文件名安全规则 |
| `tags` | string[] | 标签列表，整体替换；空数组 `[]` 表示清空 |
| `status` | `"none"` \| `"reviewed"` \| `"used"` | 状态 |
| `difficulty` | object | 整体替换难度评估；必须至少含 `human`；score 1-10；`notes` 若为空串会规范化为 null |
| `author` | string | 命题人 |
| `reviewers` | string[] | 审题人列表，会去重 |

```json
{
  "category": "T",
  "tags": ["optics"],
  "difficulty": {
    "human": { "score": 8 }
  }
}
```

**成功响应** `200`：更新后的 `QuestionDetail`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 无可更新字段 / 参数校验失败 / 未知字段 |
| `404` | 题目不存在或已软删除 |

---

### `PUT /questions/:question_id/file`

替换题目的 zip 文件内容（tex 和 assets），不修改元数据。

- **认证**：`editor` 及以上
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

- **认证**：`editor` 及以上
- **路径参数**：`question_id` — UUID

**行为**：

- 设置 `deleted_at` / `deleted_by` / `updated_at`
- 不会立刻删除文件对象，由管理员垃圾回收处理
- 已软删除题目重复删除返回 `404`
- 若题目仍被未软删除试卷引用，返回 `409`

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

## Papers — 试卷

### 数据结构

#### `PaperSummary`

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

#### `PaperDetail`

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

- **认证**：`viewer` 及以上
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

---

## Ops — 运维操作

所有 Ops 接口需要 `admin` 角色。

### `GET /database/backup`

下载当前数据库的 plain SQL 备份文件。

- **认证**：`admin`
- **请求体**：无

**成功响应** `200`：

- **Content-Type**：`application/sql`
- **Header** 含 `content-disposition`（下载文件名）和 `content-length`
- **Body**：`pg_dump` 生成的 plain SQL，可按 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) 中的恢复方式导入

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
- 恢复流程与 [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) 中“覆盖当前库”的恢复方法保持一致

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

---

## Admin — 管理员

所有 `/admin/*` 接口需要 `admin` 角色。

### 题目管理

#### `GET /admin/questions`

管理员视角查询题目，可查看软删除记录。

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `state` | `"active"` \| `"deleted"` \| `"all"` | `"all"` | 记录状态过滤 |
| 其他参数 | — | — | 同 `GET /questions` 的全部过滤参数 |

**成功响应** `200`：分页包裹，`items` 为 `AdminQuestionSummary[]`。

`AdminQuestionSummary` = `QuestionSummary` + 以下字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `deleted_at` | string \| null | 软删除时间 |
| `deleted_by` | string(UUID) \| null | 执行删除的用户 ID |
| `is_deleted` | boolean | 是否已软删除 |

---

#### `GET /admin/questions/:question_id`

管理员视角获取题目详情（含软删除记录）。

- **路径参数**：`question_id` — UUID

**成功响应** `200`：`AdminQuestionDetail` = `QuestionDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

#### `POST /admin/questions/:question_id/restore`

恢复已软删除的题目。

- **路径参数**：`question_id` — UUID
- **请求体**：无

**行为**：清空 `deleted_at` / `deleted_by`

**成功响应** `200`：恢复后的 `AdminQuestionDetail`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `404` | 题目不存在 |
| `409` | 题目未被软删除 |

---

### 试卷管理

#### `GET /admin/papers`

管理员视角查询试卷，可查看软删除记录。

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `state` | `"active"` \| `"deleted"` \| `"all"` | `"all"` | 记录状态过滤 |
| 其他参数 | — | — | 同 `GET /papers` 的全部过滤参数 |

**成功响应** `200`：分页包裹，`items` 为 `AdminPaperSummary[]`。

`AdminPaperSummary` = `PaperSummary` + `deleted_at` / `deleted_by` / `is_deleted`。

---

#### `GET /admin/papers/:paper_id`

管理员视角获取试卷详情（含软删除记录）。

- **路径参数**：`paper_id` — UUID

**成功响应** `200`：`AdminPaperDetail` = `PaperDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

#### `POST /admin/papers/:paper_id/restore`

恢复已软删除的试卷。

- **路径参数**：`paper_id` — UUID
- **请求体**：无

**行为**：

- 检查试卷必须已软删除
- 检查引用的所有题目不能有已软删除的
- 对题目集合重新校验创建约束（category 一致性、status 合规性）
- 清空 `deleted_at` / `deleted_by`

**成功响应** `200`：恢复后的 `AdminPaperDetail`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `404` | 试卷不存在 |
| `409` | 试卷未被软删除 / 引用的题目已被删除或不满足约束 |

---

### 垃圾回收

#### `POST /admin/garbage-collections/preview`

预演垃圾回收（dry run），不会真正提交。

- **请求体**：必须为空对象 `{}`（传任何额外字段返回 `400`）

**成功响应** `200`：

```json
{
  "dry_run": true,
  "deleted_questions": 13,
  "deleted_papers": 4,
  "deleted_objects": 45,
  "freed_bytes": 1711558
}
```

---

#### `POST /admin/garbage-collections/run`

真正执行垃圾回收（硬删除）。

- **请求体**：`{}`

**执行顺序**：

1. 硬删除所有已软删除试卷
2. 硬删除"已软删且不再被未软删试卷引用"的题目
3. 删除所有无任何引用的 objects（含关联的二进制数据）

**成功响应** `200`：格式同 preview，但 `dry_run: false`。

---

### 用户管理

#### `GET /admin/users`

分页列出所有用户。

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `UserProfile[]`。

---

#### `POST /admin/users`

创建新用户。

- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `username` | string | ✅ | — | 用户名，trim 后非空，唯一 |
| `password` | string | ✅ | — | 密码，长度 ≥ 6 |
| `display_name` | string | — | `""` | 显示名 |
| `role` | `"viewer"` \| `"editor"` \| `"admin"` | — | `"viewer"` | 角色 |

```json
{
  "username": "alice",
  "password": "secure-password",
  "display_name": "Alice",
  "role": "editor"
}
```

**成功响应** `200`：`UserProfile` 对象。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 参数校验失败 |
| `409` | 用户名已存在 |

---

#### `PATCH /admin/users/:user_id`

更新用户信息。

- **路径参数**：`user_id` — UUID
- **Content-Type**：`application/json`

**请求体**（至少提供一个字段）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `display_name` | string | 显示名 |
| `role` | `"viewer"` \| `"editor"` \| `"admin"` | 角色 |
| `is_active` | boolean | 是否启用 |

```json
{
  "role": "admin",
  "is_active": true
}
```

**特殊约束**：不允许管理员将自己设为 `is_active=false`。

**成功响应** `200`：更新后的 `UserProfile`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 无可更新字段 / 角色值无效 / 尝试停用自己 |
| `404` | 用户不存在 |

---

#### `DELETE /admin/users/:user_id`

停用用户（非硬删除）。

- **路径参数**：`user_id` — UUID

**行为**：

- 设置 `is_active = false`
- 撤销该用户的所有 refresh token
- 不允许停用自己

**成功响应** `200`：

```json
{
  "message": "user deactivated"
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 尝试删除自己 |
| `404` | 用户不存在 |

---

#### `POST /admin/users/:user_id/reset-password`

管理员重置指定用户密码。

- **路径参数**：`user_id` — UUID
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `new_password` | string | ✅ | 新密码，长度 ≥ 6 |

```json
{
  "new_password": "new-secure-password"
}
```

**行为**：

- 重置密码哈希
- 撤销该用户的所有 refresh token（强制重新登录）

**成功响应** `200`：

```json
{
  "message": "password reset"
}
```

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 密码长度不足 6 |
| `404` | 用户不存在 |
