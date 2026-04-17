//! JWT access token and opaque refresh token utilities.

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::models::Role;

/// Access token lifetime in seconds.
pub(crate) const ACCESS_TOKEN_LIFETIME_SECS: i64 = 1800; // 30 min

/// Refresh token lifetime in seconds.
pub(crate) const REFRESH_TOKEN_LIFETIME_SECS: i64 = 7 * 24 * 3600; // 7 days

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub(crate) sub: String, // user_id
    pub(crate) username: String,
    pub(crate) display_name: String,
    pub(crate) role: String,
    /// Leader expiry timestamp (epoch seconds). Only present for leader role.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) leader_exp: Option<i64>,
    pub(crate) iat: i64,
    pub(crate) exp: i64,
}

/// Create a signed JWT access token.
pub(crate) fn create_access_token(
    user_id: &str,
    username: &str,
    display_name: &str,
    role: Role,
    leader_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    secret: &str,
) -> Result<String> {
    let now = Utc::now();
    let claims = Claims {
        sub: user_id.to_string(),
        username: username.to_string(),
        display_name: display_name.to_string(),
        role: role.as_str().to_string(),
        leader_exp: leader_expires_at.map(|t| t.timestamp()),
        iat: now.timestamp(),
        exp: (now + Duration::seconds(ACCESS_TOKEN_LIFETIME_SECS)).timestamp(),
    };
    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .context("failed to encode JWT")
}

/// Decode and validate a JWT access token, returning the claims.
pub(crate) fn decode_access_token(token: &str, secret: &str) -> Result<Claims> {
    let data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )
    .context("invalid or expired access token")?;
    Ok(data.claims)
}

/// Generate a new random opaque refresh token string.
pub(crate) fn generate_refresh_token() -> String {
    Uuid::new_v4().to_string()
}

/// Generate a long-lived opaque access token for bot users.
pub(crate) fn generate_bot_access_token() -> String {
    format!("qbt_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn hash_opaque_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// SHA-256 hash of a refresh token (stored in the database).
pub(crate) fn hash_refresh_token(token: &str) -> String {
    hash_opaque_token(token)
}

/// SHA-256 hash of a bot access token (stored in the database).
pub(crate) fn hash_bot_access_token(token: &str) -> String {
    hash_opaque_token(token)
}

/// Compute the expiration timestamp for a new refresh token.
pub(crate) fn refresh_token_expires_at() -> chrono::DateTime<Utc> {
    Utc::now() + Duration::seconds(REFRESH_TOKEN_LIFETIME_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn access_token_roundtrip() {
        let secret = "test-secret-key-at-least-32-bytes!";
        let token =
            create_access_token("abc-123", "alice", "Alice", Role::Admin, None, secret).unwrap();
        let claims = decode_access_token(&token, secret).unwrap();
        assert_eq!(claims.sub, "abc-123");
        assert_eq!(claims.username, "alice");
        assert_eq!(claims.display_name, "Alice");
        assert_eq!(claims.role, "admin");
        assert!(claims.leader_exp.is_none());
    }

    #[test]
    fn refresh_token_hash_is_deterministic() {
        let t = "some-random-token";
        assert_eq!(hash_refresh_token(t), hash_refresh_token(t));
    }

    #[test]
    fn bot_access_token_has_expected_prefix() {
        let token = generate_bot_access_token();
        assert!(token.starts_with("qbt_"));
        assert!(token.len() > 32);
        assert_eq!(hash_bot_access_token(&token), hash_bot_access_token(&token));
    }
}
