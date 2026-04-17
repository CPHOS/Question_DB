//! Auth data models.

use serde::{Deserialize, Deserializer, Serialize};

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Role {
    Viewer,
    User,
    Leader,
    Bot,
    Admin,
}

impl Role {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            "viewer" => Some(Self::Viewer),
            "user" => Some(Self::User),
            "leader" => Some(Self::Leader),
            "bot" => Some(Self::Bot),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::User => "user",
            Self::Leader => "leader",
            Self::Bot => "bot",
            Self::Admin => "admin",
        }
    }

    /// Can create (upload) questions.
    pub(crate) fn can_upload_question(self) -> bool {
        matches!(self, Self::User | Self::Leader | Self::Bot | Self::Admin)
    }

    /// Can create papers.
    pub(crate) fn can_create_paper(self) -> bool {
        matches!(self, Self::Leader | Self::Bot | Self::Admin)
    }

    /// Has leader-level privileges (modify/delete any non-used question, etc.).
    pub(crate) fn is_leader_or_above(self) -> bool {
        matches!(self, Self::Leader | Self::Bot | Self::Admin)
    }

    /// Full administrative access.
    pub(crate) fn is_admin(self) -> bool {
        matches!(self, Self::Admin)
    }

    /// Admin or Bot: unrestricted data access.
    pub(crate) fn is_admin_or_bot(self) -> bool {
        matches!(self, Self::Bot | Self::Admin)
    }
}

// ---------------------------------------------------------------------------
// Current user (extracted in middleware, injected into handlers)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct CurrentUser {
    pub(crate) user_id: String,
    #[allow(dead_code)]
    pub(crate) username: String,
    pub(crate) display_name: String,
    /// Effective role (leader downgraded to user if expired).
    pub(crate) role: Role,
}

// ---------------------------------------------------------------------------
// Request / response DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct LoginRequest {
    pub(crate) username: String,
    pub(crate) password: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct TokenResponse {
    pub(crate) access_token: String,
    pub(crate) refresh_token: String,
    pub(crate) token_type: &'static str,
    pub(crate) expires_in: i64,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RefreshRequest {
    pub(crate) refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ChangePasswordRequest {
    pub(crate) old_password: String,
    pub(crate) new_password: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct UserProfile {
    pub(crate) user_id: String,
    pub(crate) username: String,
    pub(crate) display_name: String,
    pub(crate) role: String,
    pub(crate) is_active: bool,
    pub(crate) leader_expires_at: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct MessageResponse {
    pub(crate) message: &'static str,
}

// ---------------------------------------------------------------------------
// Admin user management
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(crate) struct CreateUserRequest {
    pub(crate) username: String,
    pub(crate) password: Option<String>,
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) leader_expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateUserRequest {
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) is_active: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub(crate) leader_expires_at: Option<Option<String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResetPasswordRequest {
    pub(crate) new_password: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct AdminUserResponse {
    #[serde(flatten)]
    pub(crate) profile: UserProfile,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) token_type: Option<&'static str>,
}

impl AdminUserResponse {
    pub(crate) fn new(profile: UserProfile, access_token: Option<String>) -> Self {
        let token_type = access_token.as_ref().map(|_| "Bearer");
        Self {
            profile,
            access_token,
            token_type,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminUsersParams {
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UserSearchParams {
    pub(crate) q: Option<String>,
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}

/// Deserialize a double-option field so that JSON `null` maps to `Some(None)`
/// (explicit clear) and a missing key maps to `None` (no change).
fn deserialize_double_option<'de, D>(deserializer: D) -> Result<Option<Option<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Option::deserialize(deserializer)?))
}
