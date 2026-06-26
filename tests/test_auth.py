"""Tests for auth — verify_token, JWTClaims enforcement, and AuthASGIApp."""

from __future__ import annotations

from typing import Any

import jwt as pyjwt
import pytest

from meadows.protocol import JWTRole, build_claims
from meadows.protocol.jwt import ALGORITHM, JWTClaims

from meadows.server.auth import AuthASGIApp, AuthError, verify_token

WRONG_SECRET = b"wrong-secret-but-long-enough-32-bytes!!"


# ---------------------------------------------------------------------------
# verify_token
# ---------------------------------------------------------------------------


class TestVerifyToken:
    def test_accepts_valid_token(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER), jwt_secret)
        claims = verify_token(token, jwt_secret)
        assert claims.sub == "user-alice"
        assert claims.is_user()
        assert claims.username == "alice"

    def test_accepts_valid_bot_token(self, jwt_secret: bytes):
        token = _mint(build_claims(name="echo", role=JWTRole.BOT), jwt_secret)
        claims = verify_token(token, jwt_secret)
        assert claims.sub == "bot-echo"
        assert claims.is_bot()
        assert claims.bot_name == "echo"

    def test_rejects_expired_token(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER, expires_in_seconds=-10), jwt_secret)
        with pytest.raises(AuthError):
            verify_token(token, jwt_secret)

    def test_rejects_wrong_secret(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER), WRONG_SECRET)
        with pytest.raises(AuthError):
            verify_token(token, jwt_secret)

    def test_rejects_malformed_token(self, jwt_secret: bytes):
        with pytest.raises(AuthError):
            verify_token("not.a.jwt", jwt_secret)

    def test_rejects_empty_token(self, jwt_secret: bytes):
        with pytest.raises(AuthError):
            verify_token("", jwt_secret)


# ---------------------------------------------------------------------------
# JWTClaims validation rules (the structure verify_token enforces)
# ---------------------------------------------------------------------------


class TestJWTClaimsValidation:
    def test_sub_must_have_user_prefix(self):
        with pytest.raises(Exception, match="sub must be prefixed"):
            JWTClaims(sub="alice", role=JWTRole.USER, exp=9999999999.0)

    def test_sub_must_have_bot_prefix(self):
        with pytest.raises(Exception, match="sub must be prefixed"):
            JWTClaims(sub="echo", role=JWTRole.BOT, bot_name="echo", exp=9999999999.0)

    def test_bot_requires_bot_name(self):
        with pytest.raises(Exception, match="bot_name is required"):
            JWTClaims(sub="bot-echo", role=JWTRole.BOT, exp=9999999999.0)

    def test_valid_user_claims_construct(self):
        claims = JWTClaims(sub="user-alice", role=JWTRole.USER, exp=9999999999.0)
        assert claims.is_user()
        assert claims.name() == "user-alice"

    def test_valid_bot_claims_construct(self):
        claims = JWTClaims(sub="bot-echo", role=JWTRole.BOT, bot_name="echo", exp=9999999999.0)
        assert claims.is_bot()
        assert claims.name() == "echo"


# ---------------------------------------------------------------------------
# AuthASGIApp
# ---------------------------------------------------------------------------


class RecordingApp:
    def __init__(self) -> None:
        self.called = False

    async def __call__(self, _scope: dict[str, Any], _receive: Any, send: Any) -> None:
        self.called = True
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


async def _call_http(
    app: AuthASGIApp,
    path: str,
    *,
    headers: tuple[tuple[bytes, bytes], ...] = (),
    scope_type: str = "http",
) -> tuple[int, bytes, bool]:
    scope: dict[str, Any] = {
        "type": scope_type,
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": list(headers),
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 0),
    }

    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    inner = RecordingApp()
    app.app = inner
    await app(scope, receive, send)

    status = next((m["status"] for m in sent if m["type"] == "http.response.start"), 0)
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return status, body, inner.called


class TestAuthASGIApp:
    async def test_valid_bearer_passes_through(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER), jwt_secret)
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, body, called = await _call_http(
            app, "/chat/anything", headers=[(b"authorization", f"Bearer {token}".encode())]
        )
        assert status == 200
        assert body == b"ok"
        assert called is True

    async def test_missing_bearer_returns_401(self, jwt_secret: bytes):
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, _body, called = await _call_http(app, "/chat/secret")
        assert status == 401
        assert called is False

    async def test_invalid_bearer_returns_401(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER), WRONG_SECRET)
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, _body, called = await _call_http(
            app, "/chat/secret", headers=[(b"authorization", f"Bearer {token}".encode())]
        )
        assert status == 401
        assert called is False

    async def test_socketio_path_bypasses_auth(self, jwt_secret: bytes):
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, _body, called = await _call_http(app, "/socket.io/? transport=websocket")
        assert status == 200
        assert called is True

    async def test_unprotected_path_bypasses_auth(self, jwt_secret: bytes):
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, _body, called = await _call_http(app, "/health")
        assert status == 200
        assert called is True

    async def test_non_http_scope_passes_through(self, jwt_secret: bytes):
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        _status, _body, called = await _call_http(app, "/chat/secret", scope_type="websocket")
        # websocket scope is not gated by the HTTP bearer check
        assert called is True

    async def test_non_bearer_scheme_rejected(self, jwt_secret: bytes):
        token = _mint(build_claims(name="alice", role=JWTRole.USER), jwt_secret)
        app = AuthASGIApp(RecordingApp(), jwt_secret=jwt_secret)
        status, _body, called = await _call_http(
            app, "/chat/secret", headers=[(b"authorization", f"Basic {token}".encode())]
        )
        assert status == 401
        assert called is False


def _mint(claims: JWTClaims, secret: bytes) -> str:
    return pyjwt.encode(claims.model_dump(exclude_none=True), secret, algorithm=ALGORITHM)
