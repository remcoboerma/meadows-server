"""Tests for ChatNamespace — auth, message broadcast, typing, bot registration.

Uses a real Hub whose AsyncServer has its emit/enter_room/leave_room wired to
a FakeSIO. No live sockets. All client-bound frames go through the chokepoint
(hub.emit_frame), so the FakeSIO records what would have hit the wire.
"""

from __future__ import annotations


from meadows.protocol import EventName, JWTRole, Message, MessageType, build_claims

from meadows.server.namespace import GENERAL_GROUP

WRONG_SECRET = b"wrong-secret-but-long-enough-32-bytes!!"


def _wire(content: str = "hello", *, user_id: str = "user-alice", group_id: str = "general") -> dict:
    return Message(type=MessageType.USER, user_id=user_id, group_id=group_id, content=content).model_dump(
        exclude_none=True
    )


class TestConnect:
    async def test_on_connect_creates_unauthenticated_session(self, hub):
        await hub.namespace.on_connect("sid-1", {})
        session = hub.user_sessions["sid-1"]
        assert session["authenticated"] is False
        assert session["claims"] is None
        assert session["group_ids"] == set()


class TestAuthenticate:
    async def test_good_user_token_authenticates(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        session = hub.user_sessions["sid-1"]
        assert session["authenticated"] is True
        assert session["claims"].sub == "user-alice"

        authd = fake_sio.events(EventName.AUTHENTICATED.value)
        assert len(authd) == 1
        assert authd[0]["to"] == "sid-1"
        assert authd[0]["data"]["user_id"] == "user-alice"
        assert authd[0]["namespace"] == "/chat"

    async def test_authenticate_joins_general_group(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        joined = fake_sio.events(EventName.JOINED_GROUP.value)
        assert len(joined) == 1
        assert joined[0]["data"]["group_id"] == GENERAL_GROUP
        assert ("sid-1", "general", "/chat") in fake_sio.rooms_entered
        # members is now keyed by user_id (from JWT sub), not by sid
        assert "user-alice" in hub.groups[GENERAL_GROUP].members

    async def test_good_bot_token_authenticates(self, hub, fake_sio, bot_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": bot_token()})

        session = hub.user_sessions["sid-1"]
        assert session["authenticated"] is True
        assert session["claims"].is_bot()

        bot_authd = fake_sio.events(EventName.BOT_AUTHENTICATED.value)
        assert len(bot_authd) == 1
        assert bot_authd[0]["data"]["bot_name"] == "echo"
        assert "echo" in hub.bot_registry

    async def test_bad_token_emits_auth_error(self, hub, fake_sio, mint):
        await hub.namespace.on_connect("sid-1", {})
        bad = mint(build_claims(name="alice", role=JWTRole.USER), secret=WRONG_SECRET)
        await hub.namespace.on_authenticate("sid-1", {"token": bad})

        session = hub.user_sessions["sid-1"]
        assert session["authenticated"] is False
        assert len(fake_sio.events(EventName.AUTH_ERROR.value)) == 1
        assert fake_sio.events(EventName.AUTHENTICATED.value) == []

    async def test_missing_token_emits_auth_error(self, hub, fake_sio):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {})

        assert hub.user_sessions["sid-1"]["authenticated"] is False
        assert len(fake_sio.events(EventName.AUTH_ERROR.value)) == 1


class TestMessage:
    async def test_message_broadcasts_via_chokepoint_and_persists(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", _wire("hello"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["room"] == "general"
        assert msgs[0]["data"]["content"] == "hello"
        assert msgs[0]["data"]["group_id"] == "general"
        assert msgs[0]["data"]["type"] == "user"
        assert msgs[0]["data"]["user_id"] == "user-alice"

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].content == "hello"

    async def test_message_unauthenticated_is_rejected(self, hub, fake_sio):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_message("sid-1", _wire("hello"))

        assert fake_sio.events(EventName.MESSAGE.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1
        assert await hub.persistence.load_group("general") == []

    async def test_message_overrides_client_identity(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        # Client self-asserts a different identity; server must override it.
        await hub.namespace.on_message("sid-1", _wire("hi", user_id="user-attacker"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["user_id"] == "user-alice"
        assert msgs[0]["data"]["username"] == "alice"

    async def test_bot_response_broadcasts_as_bot_message(self, hub, fake_sio, bot_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": bot_token()})

        await hub.namespace.on_bot_response("sid-1", _wire("pong", user_id="bot-echo"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["type"] == "bot"
        assert msgs[0]["data"]["bot_name"] == "echo"

        persisted = await hub.persistence.load_group("general")
        assert persisted[0].type == MessageType.BOT

    async def test_bot_response_from_non_bot_rejected(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_bot_response("sid-1", _wire("pong"))

        assert fake_sio.events(EventName.MESSAGE.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1


class TestBotRateLimit:
    async def test_bot_response_allowed_under_limit(self, hub, fake_sio, bot_token):
        await hub.namespace.on_connect("sid-bot", {})
        await hub.namespace.on_authenticate("sid-bot", {"token": bot_token(name="echo")})

        await hub.namespace.on_bot_response("sid-bot", _wire("pong", user_id="bot-echo"))

        assert len(fake_sio.events(EventName.MESSAGE.value)) == 1
        assert fake_sio.events(EventName.RATE_LIMITED.value) == []

    async def test_bot_response_blocked_at_limit(self, hub, fake_sio, bot_token):
        from meadows.server.namespace import RATE_LIMIT_MAX_MESSAGES

        await hub.namespace.on_connect("sid-bot", {})
        await hub.namespace.on_authenticate("sid-bot", {"token": bot_token(name="echo")})

        # Exhaust the sliding window
        for _ in range(RATE_LIMIT_MAX_MESSAGES):
            await hub.namespace.on_bot_response("sid-bot", _wire("msg", user_id="bot-echo"))
        fake_sio.emits.clear()

        # Next one should be rate-limited
        await hub.namespace.on_bot_response("sid-bot", _wire("over", user_id="bot-echo"))

        assert fake_sio.events(EventName.MESSAGE.value) == []
        rate_events = fake_sio.events(EventName.RATE_LIMITED.value)
        assert len(rate_events) == 1
        assert rate_events[0]["data"]["bot_name"] == "echo"

    async def test_rate_limit_state_cleared_on_disconnect(self, hub, fake_sio, bot_token):
        from meadows.server.namespace import RATE_LIMIT_MAX_MESSAGES

        await hub.namespace.on_connect("sid-bot", {})
        await hub.namespace.on_authenticate("sid-bot", {"token": bot_token(name="echo")})

        # Exhaust the window
        for _ in range(RATE_LIMIT_MAX_MESSAGES):
            await hub.namespace.on_bot_response("sid-bot", _wire("msg", user_id="bot-echo"))

        # Disconnect — clears rate limit state
        await hub.namespace.on_disconnect("sid-bot")

        # Reconnect
        await hub.namespace.on_connect("sid-bot-2", {})
        await hub.namespace.on_authenticate("sid-bot-2", {"token": bot_token(name="echo")})
        fake_sio.emits.clear()

        # Should be allowed again
        await hub.namespace.on_bot_response("sid-bot-2", _wire("fresh", user_id="bot-echo"))

        assert len(fake_sio.events(EventName.MESSAGE.value)) == 1

    async def test_user_message_not_rate_limited(self, hub, fake_sio, user_token):
        """Only bot_response is rate-limited, not user messages."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        for _ in range(50):
            await hub.namespace.on_message("sid-1", _wire("spam"))

        assert len(fake_sio.events(EventName.MESSAGE.value)) == 50
        assert fake_sio.events(EventName.RATE_LIMITED.value) == []


class TestTyping:
    async def test_typing_broadcasts_user_typing(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_typing("sid-1", {"group_id": "general"})

        typing = fake_sio.events(EventName.USER_TYPING.value)
        assert len(typing) == 1
        assert typing[0]["room"] == "general"
        assert typing[0]["data"]["user_id"] == "user-alice"

    async def test_typing_is_rate_limited(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_typing("sid-1", {"group_id": "general"})
        await hub.namespace.on_typing("sid-1", {"group_id": "general"})

        assert len(fake_sio.events(EventName.USER_TYPING.value)) == 1

    async def test_typing_unauthenticated_rejected(self, hub, fake_sio):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_typing("sid-1", {"group_id": "general"})

        assert fake_sio.events(EventName.USER_TYPING.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1


class TestRegisterBot:
    async def test_register_bot_stores_and_emits(self, hub, fake_sio, bot_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": bot_token(name="greeter")})

        await hub.namespace.on_register_bot(
            "sid-1", {"description": "A greeter bot", "commands": [], "context_limit": 30}
        )

        assert "greeter" in hub.bot_registry
        assert hub.bot_registry["greeter"]["description"] == "A greeter bot"
        registered = fake_sio.events(EventName.BOT_REGISTERED.value)
        assert len(registered) == 1
        assert registered[0]["data"]["bot_name"] == "greeter"

    async def test_register_bot_rejects_user(self, hub, fake_sio, user_token):
        """BUSINESS RULE: only bots may register_bot. A user token is rejected."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_register_bot("sid-1", {"bot_name": "greeter"})

        assert "greeter" not in hub.bot_registry
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_register_bot_missing_fields_defaults(self, hub, fake_sio, bot_token):
        """Missing description/commands default to empty, not an error."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": bot_token(name="echo")})

        await hub.namespace.on_register_bot("sid-1", {})

        assert "echo" in hub.bot_registry
        assert hub.bot_registry["echo"]["description"] == ""
        assert hub.bot_registry["echo"]["commands"] == []
        assert fake_sio.events(EventName.BOT_REGISTERED.value)


class TestDisconnect:
    async def test_disconnect_clears_session_and_leaves_groups(self, hub, fake_sio, user_token):
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})
        assert "sid-1" in hub.user_sessions

        await hub.namespace.on_disconnect("sid-1")

        assert "sid-1" not in hub.user_sessions
        assert ("sid-1", "general", "/chat") in fake_sio.rooms_left
        # members is keyed by user_id, not sid
        assert "user-alice" not in hub.groups[GENERAL_GROUP].members
