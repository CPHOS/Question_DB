# Auth API

> 认证和授权接口。普通用户使用 JWT access token + 不透明 refresh token；bot 使用管理员签发的长期 access token。

## 概述

- **普通用户 Access Token**：JWT (HS256)，有效期 **1800 秒（30 分钟）**
- **Bot Access Token**：管理员生成的长期不透明 token，无过期时间，只会在重新签发或停用 bot 后失效
- **Refresh Token**：仅普通用户可用；不透明 UUID 字符串，有效期 **7 天**，一次性消费（轮换）
- **传递方式**：`Authorization: Bearer <access_token>`
- **密码存储**：Argon2id
- **角色**：5 级角色体系，基于能力而非线性层级
  - `viewer`：只读 + bundle 下载
  - `user`：可上传题目，编辑自己创建的题目，可被分配为审阅人
  - `leader`：可创建题目和试卷，可编辑/删除非 used 状态的题目，可修改/删除自己创建的试卷，可分配审阅人，也可被分配为审阅人；有过期时间，过期后降级为 user
  - `bot`：与 admin 相同的数据操作权限（题目/试卷的完整读写），但无 ops 和用户管理权限；不支持用户名密码登录，只能使用管理员签发的长期 access token
  - `admin`：全部权限 + ops + 用户管理 + 垃圾回收

## 权限矩阵

| 端点 | 公开 | viewer | user | leader | bot | admin |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `GET /health` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `POST /auth/login` | ✅ | — | — | — | — | — |
| `POST /auth/refresh` | ✅ | — | — | — | — | — |
| `GET /auth/me` | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `PATCH /auth/me/password` | — | ✅ | ✅ | ✅ | — | ✅ |
| `POST /auth/logout` | — | ✅ | ✅ | ✅ | ✅ | ✅ |
| `GET /questions`, `POST /questions/search`, `GET` questions/papers/tags | — | ✅ | ✅ | ✅ | ✅ | ✅ |
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

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `QB_JWT_SECRET` | `qb-dev-secret-change-me-in-production` | JWT 签名密钥，**生产必须修改** |

## 初始账号

首次启动且 `users` 表为空时自动创建：

- 用户名：`admin`
- 密码：`changeme`
- 角色：`admin`

**请首次登录后立即修改密码。**

Bot 账号不会自动生成；管理员创建或轮换 bot 账号时，接口会返回一次 access token，后端只保存其哈希值。

---

## Endpoints

### `POST /auth/login`

用户名密码登录，获取 token 对。仅适用于非 bot 账号。

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
| `401` | 用户名或密码错误 / 账号已停用 / bot 账号必须使用管理员签发的 access token |

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

### `PATCH /auth/me/password`

修改当前用户密码。bot 账号不支持该操作。

- **认证**：`viewer` / `user` / `leader` / `admin`
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

### `GET /users/search`

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
