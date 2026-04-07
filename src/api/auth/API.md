# Auth API

> 认证和授权接口，基于 JWT access token + 不透明 refresh token。

## 概述

- **Access Token**：JWT (HS256)，有效期 **1800 秒（30 分钟）**
- **Refresh Token**：不透明 UUID 字符串，有效期 **7 天**，一次性消费（轮换）
- **传递方式**：`Authorization: Bearer <access_token>`
- **密码存储**：Argon2id
- **角色**：`viewer`（只读）、`editor`（读写+ops）、`admin`（全部权限+用户管理）

## 权限矩阵

| 端点 | 公开 | viewer | editor | admin |
|---|:---:|:---:|:---:|:---:|
| `GET /health` | ✅ | ✅ | ✅ | ✅ |
| `POST /auth/login` | ✅ | — | — | — |
| `POST /auth/refresh` | ✅ | — | — | — |
| `GET /auth/me` | — | ✅ | ✅ | ✅ |
| `PATCH /auth/me/password` | — | ✅ | ✅ | ✅ |
| `POST /auth/logout` | — | ✅ | ✅ | ✅ |
| `GET /questions`、`GET /papers` | — | ✅ | ✅ | ✅ |
| `GET /questions/:id`、`GET /papers/:id` | — | ✅ | ✅ | ✅ |
| `POST/PATCH/PUT/DELETE` questions | — | — | ✅ | ✅ |
| `POST/PATCH/PUT/DELETE` papers | — | — | ✅ | ✅ |
| ops (bundles / exports / quality) | — | — | ✅ | ✅ |
| `/admin/*` | — | — | — | ✅ |

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

---

## Endpoints

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