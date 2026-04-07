//! Auth data models.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Role {
    Viewer = 0,
    Editor = 1,
    Admin = 2,
}

impl Role {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            "viewer" => Some(Self::Viewer),
            "editor" => Some(Self::Editor),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Editor => "editor",
            Self::Admin => "admin",
        }
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
    pub(crate) role: Role,
}

impl CurrentUser {
    pub(crate) fn has_role(&self, minimum: Role) -> bool {
        self.role >= minimum
    }
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
    pub(crate) password: String,
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct UpdateUserRequest {
    pub(crate) display_name: Option<String>,
    pub(crate) role: Option<String>,
    pub(crate) is_active: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResetPasswordRequest {
    pub(crate) new_password: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct AdminUsersParams {
    pub(crate) limit: Option<i64>,
    pub(crate) offset: Option<i64>,
}
