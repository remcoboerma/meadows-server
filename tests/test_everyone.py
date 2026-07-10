"""Unit tests for @everyone permission gate and mention handling.

BUSINESS RULE (MEADOWS §3.3 line 73): @everyone is core — "zonder reacties,
mentions en replies is er geen interactie." @everyone/@all sets
is_everyone=True on the message BEFORE broadcast so clients can style it.
The sender must have the 'mention-all' permission — this is the only
permission-gated notification type (monolith sioserver.py:2282-2286).
Without the permission, the message is still sent but is_everyone stays
False (no glow, no ntfy push).

These tests verify the behavior validated via Playwright:
1. User WITH mention-all permission: @everyone sets is_everyone=True.
2. User WITHOUT mention-all permission: is_everyone stays False.
3. @all also triggers the gate (not just @everyone).
4. Normal @username mentions do NOT set is_everyone.
5. Message without @everyone has is_everyone=False.
"""

from __future__ import annotations

from meadows.protocol import EventName, Message, MessageType


async def _auth_user(hub, user_token, sid: str = "sid-1", **kw) -> None:
    """Helper: connect + authenticate a user session.

    BUSINESS RULE (MEADOWS §3.1 line 54): auth is event-based — on_connect
    creates an unauthenticated session, on_authenticate with a valid JWT
    transitions it to authenticated. Tests must go through both steps.

    The **kw allows passing permissions= to test the mention-all gate.
    """
    await hub.namespace.on_connect(sid, {})
    await hub.namespace.on_authenticate(sid, {"token": user_token(**kw)})


def _wire(content: str = "hello", *, user_id: str = "user-alice") -> dict:
    """Build a message wire dict for on_message.

    BUSINESS RULE (MEADOWS §3.2 line 64): the envelope is the closed set of
    fields the system contracts on. This helper constructs a valid user
    message so tests exercise the handler without socket plumbing.
    """
    return Message(type=MessageType.USER, user_id=user_id, group_id="general", content=content).model_dump(
        exclude_none=True
    )


class TestEveryonePermissionGate:
    """Tests for @everyone — the permission-gated mention verified via Playwright."""

    async def test_everyone_with_permission_sets_is_everyone_true(
        self, hub, fake_sio, user_token
    ):
        """Alice (with mention-all) sends @everyone → is_everyone=True in broadcast + JSONL."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("@everyone wake up!"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["is_everyone"] is True
        assert msgs[0]["data"]["content"] == "@everyone wake up!"

        persisted = await hub.persistence.load_group("general")
        assert persisted[0].is_everyone is True

    async def test_everyone_without_permission_keeps_is_everyone_false(
        self, hub, fake_sio, user_token
    ):
        """User without mention-all permission: message sent but is_everyone=False."""
        await _auth_user(hub, user_token, permissions=[])

        await hub.namespace.on_message("sid-1", _wire("@everyone wake up!"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 1
        assert msgs[0]["data"]["is_everyone"] is False

        persisted = await hub.persistence.load_group("general")
        assert persisted[0].is_everyone is False

    async def test_all_alias_triggers_is_everyone(self, hub, fake_sio, user_token):
        """@all is an alias for @everyone — both trigger the gate."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("@all hands meeting"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["is_everyone"] is True

    async def test_normal_mention_does_not_set_is_everyone(self, hub, fake_sio, user_token):
        """@alice is a normal mention, not @everyone — is_everyone stays False."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("Hey @alice did you see this?"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["is_everyone"] is False

    async def test_plain_message_has_is_everyone_false(self, hub, fake_sio, user_token):
        """A message without any @mention has is_everyone=False."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("just chatting"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["is_everyone"] is False

    async def test_everyone_word_boundary_not_triggered(self, hub, fake_sio, user_token):
        """@everyoneelse should NOT trigger is_everyone (word boundary check)."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("@everyoneelse test"))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["is_everyone"] is False

    async def test_everyone_with_permission_persists_is_everyone(self, hub, fake_sio, user_token):  # noqa: ARG002
        """is_everyone=True is persisted to JSONL so it survives reload."""
        await _auth_user(hub, user_token, permissions=["mention-all"])

        await hub.namespace.on_message("sid-1", _wire("@everyone persistent test"))

        persisted = await hub.persistence.load_group("general")
        assert persisted[0].is_everyone is True
        assert persisted[0].content == "@everyone persistent test"
