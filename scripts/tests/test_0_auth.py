"""Authentication and authorization E2E tests.

Runs before all other tests to verify the auth system works correctly.
"""

from __future__ import annotations

from .session import parse_json


# ── Auth flow tests ──────────────────────────────────────────────


def test_health_is_public(api):
    """Health endpoint requires no authentication."""
    saved = api._access_token
    api.set_token(None)
    _, body, _ = api.get("/health")
    assert parse_json(body)["status"] == "ok"
    api.set_token(saved)


def test_login_admin(api):
    """Can login with the seeded admin account."""
    saved = api._access_token
    api.set_token(None)
    data = api.login("admin", "changeme")
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "Bearer"
    assert data["expires_in"] == 1800
    api.set_token(saved)


def test_login_bad_password(api):
    """Wrong password returns 401."""
    saved = api._access_token
    api.set_token(None)
    api._do(
        "POST", "/auth/login", expect=401,
        headers={"content-type": "application/json"},
        body=b'{"username":"admin","password":"wrong"}',
    )
    api.set_token(saved)


def test_login_nonexistent_user(api):
    """Non-existent user returns 401."""
    saved = api._access_token
    api.set_token(None)
    api._do(
        "POST", "/auth/login", expect=401,
        headers={"content-type": "application/json"},
        body=b'{"username":"nobody","password":"nope"}',
    )
    api.set_token(saved)


def test_me(api):
    """GET /auth/me returns current user profile."""
    _, body, _ = api.get("/auth/me")
    profile = parse_json(body)
    assert profile["username"] == "admin"
    assert profile["role"] == "admin"
    assert profile["is_active"] is True


def test_unauthenticated_requests_rejected(api):
    """Endpoints that require auth return 401 without a token."""
    saved = api._access_token
    api.set_token(None)
    api.get("/questions", expect=401)
    api.get("/admin/questions", expect=401)
    api.set_token(saved)


def test_refresh_token_flow(api):
    """Refresh token can be used to get a new token pair."""
    saved = api._access_token
    api.set_token(None)
    login_data = api.login("admin", "changeme")
    refresh_token = login_data["refresh_token"]

    # Use refresh token
    _, body, _ = api.post_json("/auth/refresh", {"refresh_token": refresh_token})
    new_data = parse_json(body)
    assert "access_token" in new_data
    assert "refresh_token" in new_data
    assert new_data["refresh_token"] != refresh_token  # token rotated

    # Old refresh token should be consumed (fails on reuse)
    api.set_token(new_data["access_token"])
    api.post_json("/auth/refresh", {"refresh_token": refresh_token}, expect=401)

    api.set_token(saved)


def test_logout(api):
    """Logout revokes the refresh token."""
    saved = api._access_token
    api.set_token(None)
    login_data = api.login("admin", "changeme")
    rt = login_data["refresh_token"]

    api.post_json("/auth/logout", {"refresh_token": rt})

    # Refresh should now fail
    api.post_json("/auth/refresh", {"refresh_token": rt}, expect=401)

    api.set_token(saved)


def test_change_password(api):
    """Can change own password, then login with new password."""
    # Create a temp user for this test
    _, body, _ = api.post_json("/admin/users", {
        "username": "pw_test_user",
        "password": "oldpass123",
        "role": "viewer",
    })
    user = parse_json(body)
    user_id = user["user_id"]

    # Login as temp user
    saved = api._access_token
    api.login("pw_test_user", "oldpass123")

    # Change password
    api.patch_json("/auth/me/password", {
        "old_password": "oldpass123",
        "new_password": "newpass456",
    })

    # Login with new password
    api.login("pw_test_user", "newpass456")

    # Old password should fail
    api.set_token(None)
    api._do(
        "POST", "/auth/login", expect=401,
        headers={"content-type": "application/json"},
        body=b'{"username":"pw_test_user","password":"oldpass123"}',
    )

    # Cleanup: re-login as admin, deactivate temp user
    api.login("admin", "changeme")
    api.delete(f"/admin/users/{user_id}")
    api.set_token(saved)


def test_admin_reset_password(api):
    """Admin can reset another user's password."""
    # Create a temp user
    _, body, _ = api.post_json("/admin/users", {
        "username": "pw_reset_user",
        "password": "original123",
        "role": "viewer",
    })
    user = parse_json(body)
    user_id = user["user_id"]

    # Reset password as admin
    _, body, _ = api.post_json(f"/admin/users/{user_id}/reset-password", {
        "new_password": "reset456",
    })
    msg = parse_json(body)
    assert msg["message"] == "password reset"

    # Old password should fail
    saved = api._access_token
    api.set_token(None)
    api._do(
        "POST", "/auth/login", expect=401,
        headers={"content-type": "application/json"},
        body=b'{"username":"pw_reset_user","password":"original123"}',
    )

    # New password should work
    api.login("pw_reset_user", "reset456")

    # Cleanup
    api.login("admin", "changeme")
    api.delete(f"/admin/users/{user_id}")
    api.set_token(saved)


def test_admin_reset_password_validation(api):
    """Reset password rejects short passwords and bad user IDs."""
    # Create a temp user
    _, body, _ = api.post_json("/admin/users", {
        "username": "pw_reset_val",
        "password": "valid12345",
        "role": "viewer",
    })
    user = parse_json(body)
    user_id = user["user_id"]

    # Too short
    api.post_json(f"/admin/users/{user_id}/reset-password", {
        "new_password": "ab",
    }, expect=400)

    # Non-existent user
    api.post_json(
        "/admin/users/00000000-0000-0000-0000-000000000000/reset-password",
        {"new_password": "abcdef"},
        expect=404,
    )

    # Cleanup
    api.delete(f"/admin/users/{user_id}")


# ── RBAC tests ───────────────────────────────────────────────────


def test_viewer_cannot_write(api):
    """Viewer role cannot create questions or access admin endpoints."""
    # Create a viewer user
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_viewer",
        "password": "viewer123",
        "role": "viewer",
    })
    viewer = parse_json(body)

    saved = api._access_token
    api.login("e2e_viewer", "viewer123")

    # Can read
    api.get("/questions")
    api.get("/papers")
    api.get("/auth/me")

    # Cannot write (403 Forbidden)
    api.post_json("/exports/run", {"format": "jsonl"}, expect=403)

    # Cannot access admin (403 Forbidden)
    api.get("/admin/questions", expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{viewer['user_id']}")


def test_editor_can_write_not_admin(api):
    """Editor role can do writes but not admin endpoints."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_editor",
        "password": "editor123",
        "role": "editor",
    })
    editor = parse_json(body)

    saved = api._access_token
    api.login("e2e_editor", "editor123")

    # Can read
    api.get("/questions")

    # Cannot access admin (403 Forbidden)
    api.get("/admin/questions", expect=403)
    api.get("/admin/users", expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{editor['user_id']}")


# ── Admin user management tests ──────────────────────────────────


def test_admin_create_and_list_users(api):
    """Admin can create users and list them."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_managed",
        "password": "managed123",
        "display_name": "Managed User",
        "role": "editor",
    })
    user = parse_json(body)
    assert user["username"] == "e2e_managed"
    assert user["display_name"] == "Managed User"
    assert user["role"] == "editor"
    assert user["is_active"] is True

    # List users and verify
    _, body, _ = api.get("/admin/users")
    users_data = parse_json(body)
    usernames = [u["username"] for u in users_data["items"]]
    assert "e2e_managed" in usernames

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_admin_update_user(api):
    """Admin can update user role and display name."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_update",
        "password": "update123",
        "role": "viewer",
    })
    user = parse_json(body)

    _, body, _ = api.patch_json(f"/admin/users/{user['user_id']}", {
        "role": "editor",
        "display_name": "Updated Name",
    })
    updated = parse_json(body)
    assert updated["role"] == "editor"
    assert updated["display_name"] == "Updated Name"

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_admin_deactivate_user(api):
    """Deactivated user cannot login."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_deactivate",
        "password": "deactivate123",
        "role": "viewer",
    })
    user = parse_json(body)

    # Deactivate
    api.delete(f"/admin/users/{user['user_id']}")

    # Login should fail
    saved = api._access_token
    api.set_token(None)
    api._do(
        "POST", "/auth/login", expect=401,
        headers={"content-type": "application/json"},
        body=f'{{"username":"e2e_deactivate","password":"deactivate123"}}'.encode(),
    )
    api.set_token(saved)


def test_admin_cannot_delete_self(api):
    """Admin cannot deactivate their own account."""
    _, body, _ = api.get("/auth/me")
    my_id = parse_json(body)["user_id"]
    api.delete(f"/admin/users/{my_id}", expect=400)


def test_create_user_duplicate_username(api):
    """Cannot create user with duplicate username."""
    _, body, _ = api.post_json("/admin/users", {
        "username": "e2e_dup",
        "password": "dup12345",
        "role": "viewer",
    })
    user = parse_json(body)

    api.post_json("/admin/users", {
        "username": "e2e_dup",
        "password": "dup12345",
        "role": "viewer",
    }, expect=409)

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_create_user_validation(api):
    """Validation errors for bad create-user payloads."""
    # Missing password
    api.post_json("/admin/users", {
        "username": "x",
        "password": "ab",
    }, expect=400)

    # Invalid role
    api.post_json("/admin/users", {
        "username": "x",
        "password": "abcdef",
        "role": "superadmin",
    }, expect=400)
