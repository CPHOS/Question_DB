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
    assert "leader_expires_at" in profile


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
    user = api.ensure_user({
        "username": "pw_test_user",
        "password": "oldpass123",
        "role": "viewer",
    })
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
    user = api.ensure_user({
        "username": "pw_reset_user",
        "password": "original123",
        "role": "viewer",
    })
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
    user = api.ensure_user({
        "username": "pw_reset_val",
        "password": "valid12345",
        "role": "viewer",
    })
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
    viewer = api.ensure_user({
        "username": "e2e_viewer",
        "password": "viewer123",
        "role": "viewer",
    })

    saved = api._access_token
    api.login("e2e_viewer", "viewer123")

    # Can read
    api.get("/questions")
    api.get("/papers")
    api.get("/auth/me")

    # Cannot upload questions (403 Forbidden)
    api.post_json("/exports/run", {"format": "jsonl"}, expect=403)

    # Cannot access admin (403 Forbidden)
    api.get("/admin/questions", expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{viewer['user_id']}")


def test_user_can_upload_not_admin(api):
    """User role can upload questions but not admin or ops endpoints."""
    user = api.ensure_user({
        "username": "e2e_user",
        "password": "user12345",
        "role": "user",
    })

    saved = api._access_token
    api.login("e2e_user", "user12345")

    # Can read
    api.get("/questions")

    # Cannot access admin (403 Forbidden)
    api.get("/admin/questions", expect=403)
    api.get("/admin/users", expect=403)
    api.post_json("/exports/run", {"format": "jsonl"}, expect=403)
    api.post_json("/quality-checks/run", {}, expect=403)
    api.get("/database/backup", expect=403)
    api.upload("/database/restore", expect=403)

    # Cannot create papers (403 Forbidden) — only leader/bot/admin can
    api.upload("/papers", fields={
        "description": "test",
        "title": "test",
        "subtitle": "test",
        "question_ids": "[]",
    }, expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{user['user_id']}")


def test_leader_expiry_downgrade(api):
    """Leader with expired leader_expires_at is downgraded to user."""
    # Create a leader with an already-expired timestamp
    leader = api.ensure_user({
        "username": "e2e_expired_leader",
        "password": "leader123",
        "role": "leader",
        "leader_expires_at": "2020-01-01T00:00:00Z",
    })
    assert leader["role"] == "leader"
    assert leader["leader_expires_at"] is not None

    saved = api._access_token
    api.login("e2e_expired_leader", "leader123")

    # Should be downgraded to user: cannot create papers
    api.upload("/papers", fields={
        "description": "test",
        "title": "test",
        "subtitle": "test",
        "question_ids": "[]",
    }, expect=403)

    # But can still read
    api.get("/questions")

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{leader['user_id']}")


# ── Admin user management tests ──────────────────────────────────


def test_admin_create_and_list_users(api):
    """Admin can create users and list them."""
    user = api.ensure_user({
        "username": "e2e_managed",
        "password": "managed123",
        "display_name": "Managed User",
        "role": "user",
    })
    assert user["username"] == "e2e_managed"
    assert user["display_name"] == "Managed User"
    assert user["role"] == "user"
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
    user = api.ensure_user({
        "username": "e2e_update",
        "password": "update123",
        "role": "viewer",
    })

    _, body, _ = api.patch_json(f"/admin/users/{user['user_id']}", {
        "role": "user",
        "display_name": "Updated Name",
    })
    updated = parse_json(body)
    assert updated["role"] == "user"
    assert updated["display_name"] == "Updated Name"

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_admin_deactivate_user(api):
    """Deactivated user cannot login."""
    user = api.ensure_user({
        "username": "e2e_deactivate",
        "password": "deactivate123",
        "role": "viewer",
    })

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
    user = api.ensure_user({
        "username": "e2e_dup",
        "password": "dup12345",
        "role": "viewer",
    })

    api.post_json("/admin/users", {
        "username": "e2e_dup",
        "password": "dup12345",
        "role": "viewer",
    }, expect=409)

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_create_leader_requires_expiry(api):
    """Creating a leader without leader_expires_at returns 400."""
    api.post_json("/admin/users", {
        "username": "e2e_leader_noexp",
        "password": "leader123",
        "role": "leader",
    }, expect=400)


def test_create_leader_with_expiry(api):
    """Creating a leader with leader_expires_at succeeds."""
    user = api.ensure_user({
        "username": "e2e_leader_ok",
        "password": "leader123",
        "role": "leader",
        "leader_expires_at": "2099-12-31T23:59:59Z",
    })
    assert user["role"] == "leader"
    assert user["leader_expires_at"] is not None

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_create_bot_user(api):
    """Creating a bot user succeeds."""
    user = api.ensure_user({
        "username": "e2e_bot",
        "password": "bot123456",
        "role": "bot",
    })
    assert user["role"] == "bot"
    assert user["leader_expires_at"] is None

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_update_user_to_leader_requires_expiry(api):
    """Updating a user to leader role without expiry returns 400."""
    user = api.ensure_user({
        "username": "e2e_upgrade_leader",
        "password": "upgrade123",
        "role": "viewer",
    })

    # Upgrade to leader without leader_expires_at → 400
    api.patch_json(f"/admin/users/{user['user_id']}", {
        "role": "leader",
    }, expect=400)

    # Upgrade to leader with leader_expires_at → success
    _, body, _ = api.patch_json(f"/admin/users/{user['user_id']}", {
        "role": "leader",
        "leader_expires_at": "2099-12-31T23:59:59Z",
    })
    updated = parse_json(body)
    assert updated["role"] == "leader"
    assert updated["leader_expires_at"] is not None

    # Cleanup
    api.delete(f"/admin/users/{user['user_id']}")


def test_invalid_role_rejected(api):
    """Invalid role values are rejected."""
    api.post_json("/admin/users", {
        "username": "e2e_badrole",
        "password": "badrole123",
        "role": "superadmin",
    }, expect=400)

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


# ── User search tests ────────────────────────────────────────────


def test_search_users_basic(api):
    """GET /users/search returns matching users by username/display_name."""
    # Create test users with distinctive names
    u1 = api.ensure_user({
        "username": "search_alice",
        "password": "alice12345",
        "display_name": "Alice Wonderland",
        "role": "user",
    })
    u2 = api.ensure_user({
        "username": "search_bob",
        "password": "bob1234567",
        "display_name": "Bob Builder",
        "role": "viewer",
    })

    # Search by username substring
    _, body, _ = api.get("/users/search?q=search_ali")
    data = parse_json(body)
    assert data["total"] >= 1
    usernames = [u["username"] for u in data["items"]]
    assert "search_alice" in usernames
    assert "search_bob" not in usernames

    # Search by display_name substring
    _, body, _ = api.get("/users/search?q=Builder")
    data = parse_json(body)
    assert data["total"] >= 1
    usernames = [u["username"] for u in data["items"]]
    assert "search_bob" in usernames

    # Search that matches both
    _, body, _ = api.get("/users/search?q=search_")
    data = parse_json(body)
    usernames = [u["username"] for u in data["items"]]
    assert "search_alice" in usernames
    assert "search_bob" in usernames

    # Cleanup
    api.delete(f"/admin/users/{u1['user_id']}")
    api.delete(f"/admin/users/{u2['user_id']}")


def test_search_users_pagination(api):
    """GET /users/search respects limit and offset."""
    _, body, _ = api.get("/users/search?q=admin&limit=1&offset=0")
    data = parse_json(body)
    assert data["limit"] == 1
    assert data["offset"] == 0
    assert len(data["items"]) <= 1


def test_search_users_empty_q_rejected(api):
    """GET /users/search rejects empty or missing q parameter."""
    api.get("/users/search?q=", expect=400)
    api.get("/users/search", expect=400)


def test_search_users_no_results(api):
    """GET /users/search returns empty items for non-matching query."""
    _, body, _ = api.get("/users/search?q=zzz_nonexistent_user_xyz")
    data = parse_json(body)
    assert data["total"] == 0
    assert data["items"] == []


def test_search_users_viewer_forbidden(api):
    """Viewer role cannot access GET /users/search."""
    viewer = api.ensure_user({
        "username": "search_viewer",
        "password": "viewer123",
        "role": "viewer",
    })

    saved = api._access_token
    api.login("search_viewer", "viewer123")
    api.get("/users/search?q=admin", expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{viewer['user_id']}")


def test_search_users_user_forbidden(api):
    """User role cannot access GET /users/search."""
    user = api.ensure_user({
        "username": "search_user_role",
        "password": "user123456",
        "role": "user",
    })

    saved = api._access_token
    api.login("search_user_role", "user123456")
    api.get("/users/search?q=admin", expect=403)

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{user['user_id']}")


def test_search_users_leader_allowed(api):
    """Leader role can access GET /users/search."""
    leader = api.ensure_user({
        "username": "search_leader",
        "password": "leader1234",
        "role": "leader",
        "leader_expires_at": "2099-12-31T23:59:59Z",
    })

    saved = api._access_token
    api.login("search_leader", "leader1234")
    _, body, _ = api.get("/users/search?q=admin")
    data = parse_json(body)
    assert data["total"] >= 1

    # Cleanup
    api.set_token(saved)
    api.delete(f"/admin/users/{leader['user_id']}")


def test_search_users_excludes_inactive(api):
    """GET /users/search does not return deactivated users."""
    user = api.ensure_user({
        "username": "search_inactive",
        "password": "inactive123",
        "display_name": "Inactive Search User",
        "role": "user",
    })

    # Deactivate the user
    api.delete(f"/admin/users/{user['user_id']}")

    # Search should not find the deactivated user
    _, body, _ = api.get("/users/search?q=search_inactive")
    data = parse_json(body)
    usernames = [u["username"] for u in data["items"]]
    assert "search_inactive" not in usernames
