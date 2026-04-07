//! Database queries for auth (users and refresh tokens).

use anyhow::{Context, Result};
use sqlx::{query, PgPool, Row};

use super::models::UserProfile;
use crate::api::shared::{
    error::{ConflictError, NotFoundError, ValidationError},
    pagination::{normalize_limit, normalize_offset},
};

// ---------------------------------------------------------------------------
// User queries
// ---------------------------------------------------------------------------

pub(crate) struct UserRow {
    pub(crate) user_id: String,
    pub(crate) username: String,
    #[allow(dead_code)]
    pub(crate) display_name: String,
    pub(crate) password_hash: String,
    pub(crate) role: String,
    pub(crate) is_active: bool,
}

pub(crate) async fn find_user_by_username(
    pool: &PgPool,
    username: &str,
) -> Result<Option<UserRow>> {
    let row = query(
        r#"SELECT user_id::text AS user_id, username, display_name,
                  password_hash, role, is_active
           FROM users WHERE username = $1"#,
    )
    .bind(username)
    .fetch_optional(pool)
    .await
    .context("find user by username failed")?;

    Ok(row.map(|r| UserRow {
        user_id: r.get("user_id"),
        username: r.get("username"),
        display_name: r.get("display_name"),
        password_hash: r.get("password_hash"),
        role: r.get("role"),
        is_active: r.get("is_active"),
    }))
}

pub(crate) async fn find_user_by_id(pool: &PgPool, user_id: &str) -> Result<Option<UserRow>> {
    let row = query(
        r#"SELECT user_id::text AS user_id, username, display_name,
                  password_hash, role, is_active
           FROM users WHERE user_id = $1::uuid"#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .context("find user by id failed")?;

    Ok(row.map(|r| UserRow {
        user_id: r.get("user_id"),
        username: r.get("username"),
        display_name: r.get("display_name"),
        password_hash: r.get("password_hash"),
        role: r.get("role"),
        is_active: r.get("is_active"),
    }))
}

pub(crate) async fn load_user_profile(pool: &PgPool, user_id: &str) -> Result<UserProfile> {
    let row = query(
        r#"SELECT user_id::text AS user_id, username, display_name,
                  role, is_active,
                  to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
                  to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at
           FROM users WHERE user_id = $1::uuid"#,
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await
    .context("load user profile failed")?
    .ok_or_else(|| NotFoundError(format!("user not found: {user_id}")))?;

    Ok(UserProfile {
        user_id: row.get("user_id"),
        username: row.get("username"),
        display_name: row.get("display_name"),
        role: row.get("role"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

pub(crate) async fn update_password(pool: &PgPool, user_id: &str, new_hash: &str) -> Result<()> {
    query("UPDATE users SET password_hash = $1, updated_at = NOW() WHERE user_id = $2::uuid")
        .bind(new_hash)
        .bind(user_id)
        .execute(pool)
        .await
        .context("update password failed")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Refresh token queries
// ---------------------------------------------------------------------------

pub(crate) async fn insert_refresh_token(
    pool: &PgPool,
    user_id: &str,
    token_hash: &str,
    expires_at: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    query(
        r#"INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
           VALUES ($1::uuid, $2, $3)"#,
    )
    .bind(user_id)
    .bind(token_hash)
    .bind(expires_at)
    .execute(pool)
    .await
    .context("insert refresh token failed")?;
    Ok(())
}

/// Consume a refresh token: find the active row matching the hash, revoke it,
/// and return the associated user_id. Returns None if not found or expired.
pub(crate) async fn consume_refresh_token(
    pool: &PgPool,
    token_hash: &str,
) -> Result<Option<String>> {
    let row = query(
        r#"UPDATE refresh_tokens
           SET revoked_at = NOW()
           WHERE token_hash = $1
             AND revoked_at IS NULL
             AND expires_at > NOW()
           RETURNING user_id::text AS user_id"#,
    )
    .bind(token_hash)
    .fetch_optional(pool)
    .await
    .context("consume refresh token failed")?;

    Ok(row.map(|r| r.get("user_id")))
}

/// Revoke all refresh tokens for a user.
pub(crate) async fn revoke_all_refresh_tokens(pool: &PgPool, user_id: &str) -> Result<()> {
    query(
        "UPDATE refresh_tokens SET revoked_at = NOW() WHERE user_id = $1::uuid AND revoked_at IS NULL",
    )
    .bind(user_id)
    .execute(pool)
    .await
    .context("revoke all refresh tokens failed")?;
    Ok(())
}

/// Revoke a single refresh token by hash.
pub(crate) async fn revoke_refresh_token(pool: &PgPool, token_hash: &str) -> Result<()> {
    query(
        "UPDATE refresh_tokens SET revoked_at = NOW() WHERE token_hash = $1 AND revoked_at IS NULL",
    )
    .bind(token_hash)
    .execute(pool)
    .await
    .context("revoke refresh token failed")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Admin user management queries
// ---------------------------------------------------------------------------

pub(crate) async fn create_user(
    pool: &PgPool,
    username: &str,
    display_name: &str,
    password_hash: &str,
    role: &str,
) -> Result<UserProfile> {
    // Check uniqueness explicitly for better error message
    let exists = query("SELECT 1 FROM users WHERE username = $1")
        .bind(username)
        .fetch_optional(pool)
        .await
        .context("check username uniqueness failed")?
        .is_some();
    if exists {
        return Err(ConflictError(format!("username already exists: {username}")).into());
    }

    let row = query(
        r#"INSERT INTO users (username, display_name, password_hash, role)
           VALUES ($1, $2, $3, $4)
           RETURNING user_id::text AS user_id,
                     username, display_name, role, is_active,
                     to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
                     to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at"#,
    )
    .bind(username)
    .bind(display_name)
    .bind(password_hash)
    .bind(role)
    .fetch_one(pool)
    .await
    .context("create user failed")?;

    Ok(UserProfile {
        user_id: row.get("user_id"),
        username: row.get("username"),
        display_name: row.get("display_name"),
        role: row.get("role"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

pub(crate) async fn list_users(
    pool: &PgPool,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<(Vec<UserProfile>, i64)> {
    let limit = normalize_limit(limit);
    let offset = normalize_offset(offset);

    let rows = query(
        r#"SELECT user_id::text AS user_id, username, display_name, role, is_active,
                  to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
                  to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at,
                  COUNT(*) OVER() AS total_count
           FROM users
           ORDER BY created_at ASC, user_id
           LIMIT $1 OFFSET $2"#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
    .context("list users failed")?;

    let total = rows
        .first()
        .map(|r| r.get::<i64, _>("total_count"))
        .unwrap_or(0);
    let users = rows
        .into_iter()
        .map(|r| UserProfile {
            user_id: r.get("user_id"),
            username: r.get("username"),
            display_name: r.get("display_name"),
            role: r.get("role"),
            is_active: r.get("is_active"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect();

    Ok((users, total))
}

pub(crate) async fn update_user(
    pool: &PgPool,
    user_id: &str,
    display_name: Option<&str>,
    role: Option<&str>,
    is_active: Option<bool>,
) -> Result<UserProfile> {
    // Verify user exists
    let exists = query("SELECT 1 FROM users WHERE user_id = $1::uuid")
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .context("check user exists failed")?
        .is_some();
    if !exists {
        return Err(NotFoundError(format!("user not found: {user_id}")).into());
    }

    if display_name.is_none() && role.is_none() && is_active.is_none() {
        return Err(ValidationError("at least one field must be provided".into()).into());
    }

    // Build dynamic UPDATE
    let mut sets = Vec::new();
    let mut param_idx = 1u32;

    // We'll build the query string manually with numbered params
    if display_name.is_some() {
        param_idx += 1;
        sets.push(format!("display_name = ${param_idx}"));
    }
    if role.is_some() {
        param_idx += 1;
        sets.push(format!("role = ${param_idx}"));
    }
    if is_active.is_some() {
        param_idx += 1;
        sets.push(format!("is_active = ${param_idx}"));
    }
    sets.push("updated_at = NOW()".to_string());

    let sql = format!(
        r#"UPDATE users SET {}
           WHERE user_id = $1::uuid
           RETURNING user_id::text AS user_id, username, display_name, role, is_active,
                     to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
                     to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at"#,
        sets.join(", ")
    );

    // Use sqlx QueryBuilder for safe binding
    use sqlx::{Postgres, QueryBuilder};
    let mut builder = QueryBuilder::<Postgres>::new("");
    builder.push(&sql);

    // Unfortunately, dynamic queries with variable bindings need a different approach.
    // Let's use a simpler strategy: always set all fields, using COALESCE to keep old values.
    let row = query(
        r#"UPDATE users SET
               display_name = COALESCE($2, display_name),
               role = COALESCE($3, role),
               is_active = COALESCE($4, is_active),
               updated_at = NOW()
           WHERE user_id = $1::uuid
           RETURNING user_id::text AS user_id, username, display_name, role, is_active,
                     to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS created_at,
                     to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS updated_at"#,
    )
    .bind(user_id)
    .bind(display_name)
    .bind(role)
    .bind(is_active)
    .fetch_one(pool)
    .await
    .context("update user failed")?;

    Ok(UserProfile {
        user_id: row.get("user_id"),
        username: row.get("username"),
        display_name: row.get("display_name"),
        role: row.get("role"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

pub(crate) async fn delete_user(pool: &PgPool, user_id: &str) -> Result<()> {
    let exists = query("SELECT 1 FROM users WHERE user_id = $1::uuid")
        .bind(user_id)
        .fetch_optional(pool)
        .await
        .context("check user exists failed")?
        .is_some();
    if !exists {
        return Err(NotFoundError(format!("user not found: {user_id}")).into());
    }

    // Deactivate rather than hard delete; revoke all tokens
    query("UPDATE users SET is_active = FALSE, updated_at = NOW() WHERE user_id = $1::uuid")
        .bind(user_id)
        .execute(pool)
        .await
        .context("deactivate user failed")?;

    revoke_all_refresh_tokens(pool, user_id).await?;
    Ok(())
}

/// Seed the initial admin user if no users exist. Called on startup.
pub async fn seed_admin_if_empty(pool: &PgPool, default_password_hash: &str) -> Result<bool> {
    let count: i64 = query("SELECT COUNT(*) AS cnt FROM users")
        .fetch_one(pool)
        .await
        .context("count users failed")?
        .get("cnt");

    if count > 0 {
        return Ok(false);
    }

    query(
        r#"INSERT INTO users (username, display_name, password_hash, role)
           VALUES ('admin', 'Administrator', $1, 'admin')"#,
    )
    .bind(default_password_hash)
    .execute(pool)
    .await
    .context("seed admin user failed")?;

    Ok(true)
}
