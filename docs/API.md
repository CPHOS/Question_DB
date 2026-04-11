# Question Bank API 文档

> 本文档由 `scripts/build_api_doc.py` 自动生成，请勿手动编辑。
> 源文件位于各模块的 `src/api/<module>/API.md`。

## 全局约定

### Base URL

所有路径相对于服务根，例如 `http://localhost:8080`。

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


## 目录

- [System — 系统](#system-系统)
- [Auth — 认证](#auth-认证)
- [Questions — 题目](#questions-题目)
- [Papers — 试卷](#papers-试卷)
- [Ops — 运维操作](#ops-运维操作)
- [Admin — 管理员](#admin-管理员)


---

## System — 系统

> 系统级接口，包括健康检查和全局错误格式定义。

### 统一错误格式

所有接口在发生业务错误时返回：

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

---

### Endpoints

#### `GET /health`

健康检查，探测数据库连通性。

- **认证**：无需

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

#### `GET /version`

获取后端版本号（来自 Cargo.toml）。

- **认证**：无需

**成功响应** `200`：

```json
{
  "version": "0.1.2"
}
```


---

## Auth — 认证

> 认证和授权接口，基于 JWT access token + 不透明 refresh token。

### 概述

- **Access Token**：JWT (HS256)，有效期 **1800 秒（30 分钟）**
- **Refresh Token**：不透明 UUID 字符串，有效期 **7 天**，一次性消费（轮换）
- **传递方式**：`Authorization: Bearer <access_token>`
- **密码存储**：Argon2id
- **角色**：5 级角色体系，基于能力而非线性层级
  - `viewer`：只读 + bundle 下载
  - `user`：可上传题目，编辑自己创建的题目，可被分配为审阅人
  - `leader`：可创建题目和试卷，可编辑/删除非 used 状态的题目，可修改/删除自己创建的试卷，可分配审阅人，也可被分配为审阅人；有过期时间，过期后降级为 user
  - `bot`：与 admin 相同的数据操作权限（题目/试卷的完整读写），但无 ops 和用户管理权限；无过期时间，用于自动化程序
  - `admin`：全部权限 + ops + 用户管理 + 垃圾回收

### 权限矩阵

| 端点 | 公开 | viewer | user | leader | bot | admin |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `GET /health` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `POST /auth/login` | ✅ | — | — | — | — | — |
| `POST /auth/refresh` | ✅ | — | — | — | — | — |
| `GET /auth/me` | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PATCH /auth/me/password` | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `POST /auth/logout` | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `GET` questions/papers/tags | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `POST` bundles | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `POST /questions`（上传） | — | — | ✅ | ✅ | ✅ | ✅ |
| `PATCH /questions/:id`（更新） | — | — | ⚠️¹ | ⚠️³ | ✅ | ✅ |
| `DELETE /questions/:id` | — | — | — | ⚠️³ | ✅ | ✅ |
| `POST /papers`（创建） | — | — | — | ✅ | ✅ | ✅ |
| `PATCH/PUT/DELETE` papers | — | — | — | ⚠️² | ✅ | ✅ |
| 审阅人管理 | — | — | — | ✅ | ✅ | ✅ |
| `GET /users/search` | — | — | — | ✅ | ✅ | ✅ |
| ops (exports / quality / db) | — | — | — | — | — | ✅ |
| `/admin/*` | — | — | — | — | — | ✅ |

¹ user 只能编辑自己创建的题目（Full）或作为审阅人编辑难度标签（ReviewerOnly）
² leader 只能操作自己创建的试卷
³ leader 限于非 used 状态的题目；详见 Questions API

### 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `QB_JWT_SECRET` | `qb-dev-secret-change-me-in-production` | JWT 签名密钥，**生产必须修改** |

### 初始账号

首次启动且 `users` 表为空时自动创建：

- 用户名：`admin`
- 密码：`changeme`
- 角色：`admin`

**请首次登录后立即修改密码。**

---

### Endpoints

#### `POST /auth/login`

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

#### `POST /auth/refresh`

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

#### `POST /auth/logout`

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

#### `GET /auth/me`

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
  "leader_expires_at": null,
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
| `role` | `"viewer"` \| `"user"` \| `"leader"` \| `"bot"` \| `"admin"` | 角色 |
| `is_active` | boolean | 是否启用 |
| `leader_expires_at` | string(ISO 8601) \| null | Leader 角色过期时间，仅 leader 角色有值 |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

---

#### `PATCH /auth/me/password`

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

#### `GET /users/search`

按关键词搜索用户，用于审阅人分配时的用户查找。

- **认证**：`leader` 及以上
- **说明**：仅搜索已启用（`is_active=true`）的用户；按 `username` 和 `display_name` 进行 ILIKE 模糊匹配

**Query 参数**：

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `q` | string | ✅ | — | 搜索关键词，不能为空 |
| `limit` | int | — | `20` | 每页数量，范围 1-100 |
| `offset` | int | — | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `UserProfile[]`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 缺少 `q` 参数或为空 |
| `403` | 角色不满足 leader 及以上 |


---

## Questions — 题目

> 题目的增删改查、文件替换、审阅人管理、难度管理和批量打包接口。

所有请求需携带 `Authorization: Bearer <access_token>` 头。

#### 权限模型（Trait-based）

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
- 替换文件时，后端自动重置 difficulty（清空）、status（`none`）、author（创建者 display_name）、reviewers（`[]`）
- 上传时，后端自动设置 difficulty 为空、status 为 `none`、author 为上传者 display_name、reviewers 为 `[]`
- 后端自动维护 `created_by`、`created_at`、`updated_at`
- 题目创建者（`created_by`）始终可修改自己题目的 description、category、tags 和 file，不受 status 限制

---

### 数据结构

#### `QuestionSummary`

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

#### `QuestionDetail`

在 `QuestionSummary` 基础上增加 `tex_object_id`、`assets`、`papers`。

---

### Endpoints

#### `GET /questions`

按条件分页查询题目。认证：`viewer` 及以上。

**Query 参数**：`paper_id`, `category`, `tag`, `author`, `reviewer`（支持逗号分隔多值，匹配任一）, `assigned_reviewer_id`, `score_min`, `score_max`, `difficulty_tag`, `difficulty_min`, `difficulty_max`, `q`, `created_after`, `created_before`, `updated_after`, `updated_before`, `limit` (1-100, 默认 20), `offset` (默认 0)。

**成功响应** `200`：分页包裹，`items` 为 `QuestionSummary[]`。

---

#### `GET /questions/tags`

返回未软删除题目的去重标签列表。认证：`viewer` 及以上。

---

#### `GET /questions/difficulty-tags`

返回未软删除题目的去重难度标签列表。认证：`viewer` 及以上。

---

#### `GET /questions/:question_id`

返回单个题目详情。认证：`viewer` 及以上。

---

#### `POST /questions`

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

#### `PUT /questions/:question_id/file`

替换题目的 zip 文件。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`multipart/form-data`
- **字段**：`file`（binary zip）

**自动重置**：`difficulty`（清空所有条目）、`status` → `"none"`、`author` → 创建者当前 display_name、`reviewers` → `[]`。

**成功响应** `200`：`{ "question_id", "file_name", "source_tex_path", "imported_assets", "status": "replaced" }`

---

#### `PATCH /questions/:question_id/description`

更新题目描述。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "description": "string" }`

**成功响应** `200`：`QuestionDetail`

---

#### `PATCH /questions/:question_id/category`

更新题目分类。

- **认证**：owner / leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "category": "T" | "E" | "none" }`

**成功响应** `200`：`QuestionDetail`

---

#### `PATCH /questions/:question_id/tags`

更新题目标签。

- **认证**：owner / leader（非 used）/ reviewer（被分配）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "tags": ["string", ...] }`
- **说明**：reviewer 操作时自动追加 display_name 到 `reviewers` 数组

**成功响应** `200`：`QuestionDetail`

---

#### `PATCH /questions/:question_id/status`

更新题目状态。

- **认证**：leader（非 used 题目，只能设 `"none"` 或 `"reviewed"`）/ admin / bot（任意合法值）
- **Content-Type**：`application/json`
- **请求体**：`{ "status": "none" | "reviewed" | "used" }`

**成功响应** `200`：`QuestionDetail`

---

#### `PATCH /questions/:question_id/author`

更新题目命题人。

- **认证**：admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "author": "string" }`

**成功响应** `200`：`QuestionDetail`

---

#### `PATCH /questions/:question_id/reviewer-names`

更新题目审题人名称列表（`reviewers` 字符串数组）。

- **认证**：admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "reviewers": ["string", ...] }`
- **说明**：自动去重和 trim；允许设为空数组

**成功响应** `200`：`QuestionDetail`

---

#### `POST /questions/:question_id/difficulties`

创建难度条目。

- **认证**：reviewer（被分配）/ leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "algorithm_tag": "string", "score": 1-10, "notes": "string (optional)" }`
- **说明**：`created_by` 自动设为当前用户；reviewer 操作时追加 display_name 到 `reviewers`

**成功响应** `200`：`QuestionDetail`

**错误**：`409` — algorithm_tag 已存在

---

#### `PATCH /questions/:question_id/difficulties/:algorithm_tag`

更新难度条目。

- **认证**：reviewer（仅自己创建的）/ leader（非 used）/ admin / bot
- **Content-Type**：`application/json`
- **请求体**：`{ "score": 1-10, "notes": "string (optional)" }`
- **说明**：`updated_by` 自动更新为当前用户

**成功响应** `200`：`QuestionDetail`

**错误**：`404` — algorithm_tag 不存在；`403` — reviewer 试图修改他人创建的条目

---

#### `DELETE /questions/:question_id/difficulties/:algorithm_tag`

删除难度条目。

- **认证**：reviewer（仅自己创建的）/ leader（非 used）/ admin / bot

**成功响应** `200`：`QuestionDetail`

**错误**：`404` — algorithm_tag 不存在；`403` — reviewer 试图删除他人创建的条目

---

#### `POST /questions/:question_id/reviewers`

分配审阅人（写入 `question_reviews` 表）。目标用户必须为活跃的 `user` 或 `leader` 角色。

- **认证**：leader / bot / admin
- **Content-Type**：`application/json`
- **请求体**：`{ "reviewer_id": "uuid" }`

**成功响应** `200`：`{ "reviewers": [QuestionReviewer] }`

---

#### `DELETE /questions/:question_id/reviewers/:reviewer_id`

移除审阅人。

- **认证**：leader / bot / admin

**成功响应** `200`：`{ "reviewers": [QuestionReviewer] }`

---

#### `GET /questions/:question_id/reviewers`

列出已分配的审阅人。认证：`viewer` 及以上。

---

#### `DELETE /questions/:question_id`

软删除题目。

- **认证**：leader（非 used）/ admin / bot

**前置检查**：题目不能被活跃试卷引用。

**成功响应** `200`：`{ "question_id", "status": "deleted" }`

---

#### `POST /questions/bundles`

批量下载题目 zip 打包。认证：`viewer` 及以上。


---

## Papers — 试卷

> 试卷的增删改查、附录文件替换和批量打包接口。

- **`GET` 操作和 `POST /papers/bundles`**：需要任意已认证角色（`viewer` 及以上）
- **`POST /papers`（创建）**：需要 `leader` / `bot` / `admin`（即 `can_create_paper` 能力）
- **`PATCH / PUT / DELETE`（修改/删除）**：admin/bot 可操作任何试卷；leader 只能操作自己创建的试卷
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

---

### 数据结构

#### `PaperSummary`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "question_count": 5,
  "created_by": "uuid or null",
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
| `created_by` | string(UUID) \| null | 创建者的 user_id，历史数据可能为 null |
| `created_at` | string(ISO 8601) | 创建时间 |
| `updated_at` | string(ISO 8601) | 更新时间 |

#### `PaperDetail`

```json
{
  "paper_id": "uuid",
  "description": "综合训练试卷 A",
  "title": "综合训练 2026 A 卷",
  "subtitle": "校内选拔 初版",
  "created_by": "uuid or null",
  "created_at": "2026-01-01T00:00:00.000Z",
  "updated_at": "2026-01-01T00:00:00.000Z",
  "questions": [ /* QuestionSummary[] — 按 sort_order 排序 */ ]
}
```

`questions` 数组每个元素为完整的 `QuestionSummary`（含 source.tex、tags、difficulty 等全部字段）。

---

### Endpoints

#### `GET /papers`

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
| `created_after` | string(ISO 8601) | — | 创建时间下限（含），如 `2026-01-01` 或 `2026-01-01T00:00:00Z` |
| `created_before` | string(ISO 8601) | — | 创建时间上限（含） |
| `updated_after` | string(ISO 8601) | — | 更新时间下限（含） |
| `updated_before` | string(ISO 8601) | — | 更新时间上限（含） |
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `PaperSummary[]`。

---

#### `GET /papers/:paper_id`

返回试卷详情和按顺序展开的题目列表。

- **认证**：`viewer` 及以上
- **路径参数**：`paper_id` — UUID
- **说明**：只返回未软删除试卷；`questions` 中仅含未软删除题目

**成功响应** `200`：`PaperDetail` 对象。

**错误**：`404` — 试卷不存在或已软删除

---

#### `POST /papers`

创建试卷。

- **认证**：`leader` / `bot` / `admin`（即 `can_create_paper` 能力）
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

#### `PATCH /papers/:paper_id`

部分更新试卷元数据和题目列表。

- **认证**：admin/bot 可操作任何试卷；leader 只能操作自己创建的试卷
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

#### `PUT /papers/:paper_id/file`

替换试卷的附录 zip 文件。

- **认证**：admin/bot 可操作任何试卷；leader 只能操作自己创建的试卷
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

#### `DELETE /papers/:paper_id`

软删除试卷。

- **认证**：admin/bot 可操作任何试卷；leader 只能操作自己创建的试卷
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

#### `POST /papers/bundles`

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

> 运维操作接口：数据导出、质量检查、数据库备份与恢复。批量打包接口见 [Questions API](../src/api/questions/API.md) 和 [Papers API](../src/api/papers/API.md)。

- 所有 Ops 接口需要 `admin` 角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头

---

### Endpoints

#### `GET /database/backup`

下载当前数据库的 plain SQL 备份文件。

- **认证**：`admin`
- **请求体**：无

**成功响应** `200`：

- **Content-Type**：`application/sql`
- **Header** 含 `content-disposition`（下载文件名）和 `content-length`
- **Body**：`pg_dump` 生成的 plain SQL，可按 [部署文档](../docs/DEPLOYMENT.md) 中的恢复方式导入

**说明**：

- 该接口直接返回下载文件，不写入 `QB_EXPORT_DIR`
- 备份包含 PostgreSQL 中的全部业务表和对象数据（包括 `objects` 表中的题目 zip / 试卷附件内容）
- 如果内置 `pg_dump` 与数据库 major version 不匹配，接口会返回具体错误提示；需要重建 API 镜像并对齐 PostgreSQL client 版本

---

#### `POST /exports/run`

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

#### `POST /quality-checks/run`

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

#### `POST /database/restore`

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
- 恢复流程与 [部署文档](../docs/DEPLOYMENT.md) 中“覆盖当前库”的恢复方法保持一致

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

> 管理员接口：查看/恢复软删除数据、垃圾回收、用户管理。

- 所有 `/admin/*` 接口需要 `admin` 角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头
- `deleted_by` 返回执行删除操作的用户 UUID（鉴权上线前创建的记录该字段为 `null`）

---

### 题目管理

#### `GET /admin/questions`

管理员视角查询题目，可查看软删除记录。

- **认证**：`admin`

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

- **认证**：`admin`
- **路径参数**：`question_id` — UUID

**成功响应** `200`：`AdminQuestionDetail` = `QuestionDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

#### `POST /admin/questions/:question_id/restore`

恢复已软删除的题目。

- **认证**：`admin`
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

- **认证**：`admin`

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

- **认证**：`admin`
- **路径参数**：`paper_id` — UUID

**成功响应** `200`：`AdminPaperDetail` = `PaperDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

#### `POST /admin/papers/:paper_id/restore`

恢复已软删除的试卷。

- **认证**：`admin`
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

- **认证**：`admin`
- **Content-Type**：`application/json`
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

- **认证**：`admin`
- **Content-Type**：`application/json`
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

- **认证**：`admin`

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `UserProfile[]`。

---

#### `POST /admin/users`

创建新用户。

- **认证**：`admin`
- **Content-Type**：`application/json`

**请求体**：

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `username` | string | ✅ | — | 用户名，trim 后非空，唯一 |
| `password` | string | ✅ | — | 密码，长度 ≥ 6 |
| `display_name` | string | — | `""` | 显示名 |
| `role` | `"viewer"` \| `"user"` \| `"leader"` \| `"bot"` \| `"admin"` | — | `"viewer"` | 角色 |
| `leader_expires_at` | string(RFC 3339) | 条件必填 | — | Leader 角色过期时间；角色为 `leader` 时必填 |

```json
{
  "username": "alice",
  "password": "secure-password",
  "display_name": "Alice",
  "role": "leader",
  "leader_expires_at": "2026-12-31T23:59:59Z"
}
```

**成功响应** `200`：`UserProfile` 对象。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 参数校验失败 / leader 角色未提供 leader_expires_at |
| `409` | 用户名已存在 |

---

#### `PATCH /admin/users/:user_id`

更新用户信息。

- **认证**：`admin`
- **路径参数**：`user_id` — UUID
- **Content-Type**：`application/json`

**请求体**（至少提供一个字段）：

| 字段 | 类型 | 说明 |
|---|---|---|
| `display_name` | string | 显示名 |
| `role` | `"viewer"` \| `"user"` \| `"leader"` \| `"bot"` \| `"admin"` | 角色 |
| `is_active` | boolean | 是否启用 |
| `leader_expires_at` | string(RFC 3339) \| null | Leader 角色过期时间；设为 null 清除过期时间；设置 role 为 leader 时必须确保该字段有值 |

```json
{
  "role": "leader",
  "leader_expires_at": "2026-12-31T23:59:59Z",
  "is_active": true
}
```

**特殊约束**：

- 不允许管理员将自己设为 `is_active=false`
- 设置 `role` 为 `leader` 时，必须在当次请求或用户已有记录中提供 `leader_expires_at`

**成功响应** `200`：更新后的 `UserProfile`。

**错误**：

| 状态码 | 场景 |
|---|---|
| `400` | 无可更新字段 / 角色值无效 / 尝试停用自己 / leader 角色未提供 leader_expires_at |
| `404` | 用户不存在 |

---

#### `DELETE /admin/users/:user_id`

停用用户（非硬删除）。

- **认证**：`admin`
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

- **认证**：`admin`
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
