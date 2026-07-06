"""Tests for the webhook endpoint — POST /r/{group_id}.

Two layers are tested:

1. ``TestHandleWebhook`` — unit tests on ``ChatNamespace.handle_webhook``
   directly (same pattern as ``test_namespace.py``).  Exercises business
   logic: validation, message construction, dispatch pipeline.

2. ``TestWebhookASGI`` — ASGI-level tests on ``MeadowServer`` (same pattern
   as ``test_auth.py::TestAuthASGIApp``).  Exercises HTTP routing, bearer
   extraction, body parsing, and JSON error responses.
"""

from __future__ import annotations

import json
from typing import Any

import jwt as pyjwt
import pytest

from meadows.protocol import EventName, JWTRole, MessageType, build_claims
from meadows.protocol.jwt import ALGORITHM

from meadows.server.app import MeadowServer
from meadows.server.hub import Hub
from meadows.server.namespace import MAX_WEBHOOK_CONTENT, WebhookError

WRONG_SECRET = b"wrong-secret-but-long-enough-32-bytes!!"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def started_hub(hub, fake_sio):  # noqa: ARG001
    """A Hub with hub.start() called — seeds the 'general' group.

    Also accepts the ``fake_sio`` fixture so the FakeSIO is wired onto the
    hub's AsyncServer for any test that uses this fixture.
    """
    await hub.start()
    return hub


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mint(claims: Any, secret: bytes) -> str:
    return pyjwt.encode(claims.model_dump(exclude_none=True), secret, algorithm=ALGORITHM)


def _make_server(hub: Hub) -> MeadowServer:
    """Build a MeadowServer around a test Hub (no uvicorn, no real sockets)."""
    return MeadowServer(hub)


async def _call_webhook(
    server: MeadowServer,
    path: str,
    body: bytes,
    *,
    token: str | None = None,
) -> tuple[int, dict]:
    """Send a synthetic ASGI HTTP request to the MeadowServer and return (status, json_body)."""
    headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/json")]
    if token is not None:
        headers.append((b"authorization", f"Bearer {token}".encode()))

    scope: dict[str, Any] = {
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": headers,
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 0),
    }

    body_sent = False

    async def receive() -> dict[str, Any]:
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list[dict[str, Any]] = []

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await server(scope, receive, send)

    status = next((m["status"] for m in sent if m["type"] == "http.response.start"), 0)
    raw_body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    data = json.loads(raw_body) if raw_body else {}
    return status, data


# ---------------------------------------------------------------------------
# Unit tests — ChatNamespace.handle_webhook directly
# ---------------------------------------------------------------------------


class TestHandleWebhook:
    """Unit tests for ChatNamespace.handle_webhook (business logic)."""

    async def test_user_message_broadcasts_and_persists(self, started_hub, fake_sio):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        msg_id = await hub.namespace.handle_webhook("general", claims, {"content": "hello from webhook"})

        assert isinstance(msg_id, str)
        assert len(msg_id) > 0

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["room"] == "general"
        assert msgs[0]["data"]["content"] == "hello from webhook"
        assert msgs[0]["data"]["type"] == "webhook"
        assert msgs[0]["data"]["user_id"] == "user-alice"
        assert msgs[0]["data"]["username"] == "alice"

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].content == "hello from webhook"
        assert persisted[0].type == MessageType.WEBHOOK

    async def test_bot_message_broadcasts_with_bot_name(self, started_hub, fake_sio):
        hub = started_hub
        claims = build_claims(name="echo", role=JWTRole.BOT)
        msg_id = await hub.namespace.handle_webhook("general", claims, {"content": "bot webhook"})

        assert isinstance(msg_id, str)

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["type"] == "webhook"
        assert msgs[0]["data"]["bot_name"] == "echo"
        assert msgs[0]["data"]["user_id"] == "bot-echo"
        assert "username" not in msgs[0]["data"]  # None values excluded by message_to_wire

    async def test_group_not_found_raises_404(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        with pytest.raises(WebhookError, match="group not found") as exc_info:
            await hub.namespace.handle_webhook("nonexistent", claims, {"content": "hi"})
        assert exc_info.value.status_code == 404

    async def test_empty_content_raises_400(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        with pytest.raises(WebhookError, match="content is required") as exc_info:
            await hub.namespace.handle_webhook("general", claims, {"content": ""})
        assert exc_info.value.status_code == 400

    async def test_missing_content_raises_400(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        with pytest.raises(WebhookError, match="content is required") as exc_info:
            await hub.namespace.handle_webhook("general", claims, {})
        assert exc_info.value.status_code == 400

    async def test_whitespace_only_content_raises_400(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        with pytest.raises(WebhookError, match="content is required"):
            await hub.namespace.handle_webhook("general", claims, {"content": "   "})

    async def test_content_too_large_raises_400(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        huge = "x" * (MAX_WEBHOOK_CONTENT + 1)
        with pytest.raises(WebhookError, match="content too large") as exc_info:
            await hub.namespace.handle_webhook("general", claims, {"content": huge})
        assert exc_info.value.status_code == 400

    async def test_content_at_max_length_succeeds(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        max_content = "x" * MAX_WEBHOOK_CONTENT
        msg_id = await hub.namespace.handle_webhook("general", claims, {"content": max_content})
        assert isinstance(msg_id, str)

    async def test_non_dict_data_raises_400(self, started_hub):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        with pytest.raises(WebhookError, match="content is required"):
            await hub.namespace.handle_webhook("general", claims, "not a dict")  # type: ignore[arg-type]

    async def test_strips_whitespace_from_content(self, started_hub, fake_sio):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        await hub.namespace.handle_webhook("general", claims, {"content": "  hello  "})

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["content"] == "hello"

    async def test_triggers_bot_routing(self, started_hub, fake_sio, bot_token):
        """Webhook messages go through _dispatch_message which routes @bot mentions."""
        hub = started_hub
        # Register a bot
        await hub.namespace.on_connect("sid-bot", {})
        await hub.namespace.on_authenticate("sid-bot", {"token": bot_token(name="echo")})
        await hub.namespace.on_register_bot("sid-bot", {"description": "echo bot", "commands": ["echo"]})
        fake_sio.emits.clear()

        claims = build_claims(name="alice", role=JWTRole.USER)
        await hub.namespace.handle_webhook("general", claims, {"content": "@echo hello"})

        # Should have a MESSAGE broadcast AND a BOT_COMMAND to the bot
        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["content"] == "@echo hello"

        bot_cmds = fake_sio.events(EventName.BOT_COMMAND.value)
        assert len(bot_cmds) == 1
        assert bot_cmds[0]["to"] == "sid-bot"
        assert bot_cmds[0]["data"]["command"] == "hello"

    async def test_triggers_pattern_evaluation(self, started_hub, fake_sio, bot_token):
        """Webhook messages go through _dispatch_message which evaluates patterns."""
        hub = started_hub
        # Register a bot with a pattern
        await hub.namespace.on_connect("sid-bot", {})
        await hub.namespace.on_authenticate("sid-bot", {"token": bot_token(name="echo")})
        await hub.namespace.on_register_pattern(
            "sid-bot",
            {"name": "urgent", "pattern": r"urgent|critical", "scope": "global"},
        )
        fake_sio.emits.clear()

        claims = build_claims(name="alice", role=JWTRole.USER)
        await hub.namespace.handle_webhook("general", claims, {"content": "this is urgent!"})

        matched = fake_sio.events(EventName.PATTERN_MATCHED.value)
        assert len(matched) == 1
        assert matched[0]["data"]["pattern_name"] == "urgent"
        assert matched[0]["to"] == "sid-bot"

    async def test_everyone_with_mention_all_permission(self, started_hub, fake_sio):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER, permissions=["mention-all"])
        await hub.namespace.handle_webhook("general", claims, {"content": "@everyone meeting time"})

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["is_everyone"] is True

    async def test_everyone_without_mention_all_permission(self, started_hub, fake_sio):
        hub = started_hub
        claims = build_claims(name="alice", role=JWTRole.USER)
        await hub.namespace.handle_webhook("general", claims, {"content": "@everyone wake up"})

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["is_everyone"] is False


# ---------------------------------------------------------------------------
# ASGI-level tests — MeadowServer HTTP routing
# ---------------------------------------------------------------------------


class TestWebhookASGI:
    """ASGI-level tests for POST /r/{group_id} on MeadowServer."""

    async def test_successful_post_returns_200(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "hello"}).encode(), token=user_token()
        )
        assert status == 200
        assert data["status"] == "ok"
        assert "message_id" in data

    async def test_missing_bearer_returns_401(self, started_hub):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "hello"}).encode()
        )
        assert status == 401
        assert data["error"] == "missing bearer token"

    async def test_invalid_bearer_returns_401(self, started_hub):
        server = _make_server(started_hub)
        bad_token = _mint(build_claims(name="alice", role=JWTRole.USER), WRONG_SECRET)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "hello"}).encode(), token=bad_token
        )
        assert status == 401
        assert data["error"] == "invalid token"

    async def test_expired_token_returns_401(self, started_hub):
        server = _make_server(started_hub)
        expired = _mint(
            build_claims(name="alice", role=JWTRole.USER, expires_in_seconds=-10),
            started_hub.jwt_secret,
        )
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "hello"}).encode(), token=expired
        )
        assert status == 401
        assert data["error"] == "invalid token"

    async def test_invalid_json_body_returns_400(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/general", b"not json{{{", token=user_token()
        )
        assert status == 400
        assert data["error"] == "invalid JSON body"

    async def test_empty_content_returns_400(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": ""}).encode(), token=user_token()
        )
        assert status == 400
        assert data["error"] == "content is required"

    async def test_group_not_found_returns_404(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/nonexistent", json.dumps({"content": "hello"}).encode(), token=user_token()
        )
        assert status == 404
        assert data["error"] == "group not found"

    async def test_content_too_large_returns_400(self, started_hub, user_token):
        server = _make_server(started_hub)
        huge = "x" * (MAX_WEBHOOK_CONTENT + 1)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": huge}).encode(), token=user_token()
        )
        assert status == 400
        assert data["error"] == "content too large"

    async def test_webhook_message_appears_in_broadcast(self, started_hub, fake_sio, user_token):
        server = _make_server(started_hub)
        status, _data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "webhook test"}).encode(), token=user_token()
        )
        assert status == 200

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["content"] == "webhook test"
        assert msgs[0]["data"]["type"] == "webhook"
        assert msgs[0]["data"]["user_id"] == "user-alice"
        assert msgs[0]["room"] == "general"

    async def test_webhook_persists_message(self, started_hub, user_token):
        server = _make_server(started_hub)
        await _call_webhook(
            server, "/r/general", json.dumps({"content": "persist me"}).encode(), token=user_token()
        )

        persisted = await started_hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].content == "persist me"
        assert persisted[0].type == MessageType.WEBHOOK

    async def test_group_id_is_lowercased(self, started_hub, fake_sio, user_token):
        server = _make_server(started_hub)
        status, _data = await _call_webhook(
            server, "/r/General", json.dumps({"content": "case test"}).encode(), token=user_token()
        )
        assert status == 200

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["group_id"] == "general"

    async def test_trailing_slash_in_path(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, _data = await _call_webhook(
            server, "/r/general/", json.dumps({"content": "slash test"}).encode(), token=user_token()
        )
        assert status == 200

    async def test_bot_token_works(self, started_hub, fake_sio, bot_token):
        server = _make_server(started_hub)
        status, _data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "from bot"}).encode(), token=bot_token()
        )
        assert status == 200

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["bot_name"] == "echo"
        assert msgs[0]["data"]["type"] == "webhook"

    async def test_non_webhook_paths_pass_through(self, started_hub):
        """Paths not starting with /r/ should not be intercepted by the webhook handler."""
        server = _make_server(started_hub)
        scope: dict[str, Any] = {
            "type": "http",
            "method": "GET",
            "path": "/health",
            "raw_path": b"/health",
            "query_string": b"",
            "headers": [],
            "scheme": "http",
            "server": ("test", 80),
            "client": ("test", 0),
        }
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await server(scope, receive, send)

        # The request should have passed through to the Socket.IO ASGI app,
        # which responds with its own response (not a webhook JSON error).
        # Verify no webhook-style JSON error was returned.
        json_error_starts = [
            m for m in sent
            if m["type"] == "http.response.start" and m.get("status") in (401, 400, 404)
        ]
        # Socket.IO ASGI app may return 400 for unknown Engine.IO requests,
        # but the content-type won't be application/json like our webhook errors.
        for m in json_error_starts:
            headers = dict(m.get("headers", []))
            assert headers.get(b"content-type") != b"application/json"

    async def test_message_id_in_response_matches_persisted(self, started_hub, user_token):
        server = _make_server(started_hub)
        status, data = await _call_webhook(
            server, "/r/general", json.dumps({"content": "id check"}).encode(), token=user_token()
        )
        assert status == 200
        message_id = data["message_id"]

        persisted = await started_hub.persistence.load_group("general")
        assert persisted[0].id == message_id
