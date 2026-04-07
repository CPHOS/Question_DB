# Admin API

> 管理员接口：查看/恢复软删除数据、垃圾回收、用户管理。

- 所有 `/admin/*` 接口需要 `admin` 角色
- 所有请求需携带 `Authorization: Bearer <access_token>` 头
- `deleted_by` 返回执行删除操作的用户 UUID（鉴权上线前创建的记录该字段为 `null`）

---

## 题目管理

### `GET /admin/questions`

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

### `GET /admin/questions/:question_id`

管理员视角获取题目详情（含软删除记录）。

- **认证**：`admin`
- **路径参数**：`question_id` — UUID

**成功响应** `200`：`AdminQuestionDetail` = `QuestionDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

### `POST /admin/questions/:question_id/restore`

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

## 试卷管理

### `GET /admin/papers`

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

### `GET /admin/papers/:paper_id`

管理员视角获取试卷详情（含软删除记录）。

- **认证**：`admin`
- **路径参数**：`paper_id` — UUID

**成功响应** `200`：`AdminPaperDetail` = `PaperDetail` + `deleted_at` / `deleted_by` / `is_deleted`。

---

### `POST /admin/papers/:paper_id/restore`

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

## 垃圾回收

### `POST /admin/garbage-collections/preview`

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

### `POST /admin/garbage-collections/run`

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

## 用户管理

### `GET /admin/users`

分页列出所有用户。

- **认证**：`admin`

**Query 参数**：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `limit` | int | `20` | 每页数量，范围 1-100 |
| `offset` | int | `0` | 偏移量 |

**成功响应** `200`：分页包裹，`items` 为 `UserProfile[]`。

---

### `POST /admin/users`

创建新用户。

- **认证**：`admin`
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

### `PATCH /admin/users/:user_id`

更新用户信息。

- **认证**：`admin`
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

### `DELETE /admin/users/:user_id`

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

### `POST /admin/users/:user_id/reset-password`

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