"""Unit tests for reaction handling in ChatNamespace.

BUSINESS RULE (MEADOWS §3.3 line 73): reactions are core server machinery —
"zonder reacties, mentions en replies is er geen interactie; dat ís het
systeem." A reaction is a type='reaction' message with emoji +
target_message_id. Toggle semantics: clicking the same emoji again
removes it (sets removed=True).

These tests verify the behavior validated via Playwright:
1. Adding a reaction emits REACTION_ADDED and persists to JSONL.
2. Toggling an existing reaction emits REACTION_TOGGLED and marks removed.
3. Removing a reaction explicitly emits REACTION_REMOVED.
4. Missing emoji or target_message_id emits ERROR.
5. Unauthenticated calls are rejected.
"""

from __future__ import annotations

from meadows.protocol import EventName, Message, MessageType


def _wire(content: str = "hello", *, user_id: str = "user-alice", group_id: str = "general") -> dict:
    """Build a message wire dict for on_message.

    BUSINESS RULE (MEADOWS §3.2 line 64): the envelope is the closed set of
    fields the system contracts on. This helper constructs a valid user
    message so tests exercise the handler without socket plumbing.
    """
    return Message(
        type=MessageType.USER, user_id=user_id, group_id=group_id, content=content
    ).model_dump(exclude_none=True)


async def _auth_user(hub, user_token, sid: str = "sid-1") -> None:
    """Helper: connect + authenticate a user session.

    BUSINESS RULE (MEADOWS §3.1 line 54): the hub is an object with explicit
    lifecycle. on_connect creates an unauthenticated session; on_authenticate
    with a valid JWT transitions it to authenticated. Tests must go through
    both steps to exercise the real auth gate.
    """
    await hub.namespace.on_connect(sid, {})
    await hub.namespace.on_authenticate(sid, {"token": user_token()})


async def _send_message(hub, fake_sio, sid: str = "sid-1") -> str:
    """Helper: send a user message and return its id.

    BUSINESS RULE (monolith sioserver.py:1513): the sender DOES receive their
    own message back (no skip_sid). The FakeSIO records the broadcast; we
    extract the message id from the emitted frame to use as a reaction target.
    """
    await hub.namespace.on_message(sid, _wire("target message"))
    for e in reversed(fake_sio.emits):
        if e["event"] == EventName.MESSAGE.value:
            return e["data"]["id"]
    raise AssertionError("No MESSAGE event found")


class TestAddReaction:
    """Tests for on_add_reaction — the + button behavior verified via Playwright."""

    async def test_add_reaction_emits_reaction_added_and_persists(self, hub, fake_sio, user_token):
        """Clicking 👍 in the emoji popup emits REACTION_ADDED + stores in JSONL."""
        await _auth_user(hub, user_token)
        msg_id = await _send_message(hub, fake_sio)

        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍",
            "target_message_id": msg_id,
            "group_id": "general",
        })

        added = fake_sio.events(EventName.REACTION_ADDED.value)
        assert len(added) == 1
        assert added[0]["room"] == "general"
        assert added[0]["data"]["type"] == MessageType.REACTION.value
        assert added[0]["data"]["emoji"] == "👍"
        assert added[0]["data"]["target_message_id"] == msg_id
        assert added[0]["data"]["user_id"] == "user-alice"

        persisted = await hub.persistence.load_group("general")
        reactions = [m for m in persisted if m.type == MessageType.REACTION]
        assert len(reactions) == 1
        assert reactions[0].emoji == "👍"
        assert reactions[0].removed is False

    async def test_toggle_reaction_emits_reaction_toggled_and_marks_removed(
        self, hub, fake_sio, user_token
    ):
        """Clicking the same emoji again toggles the reaction off (Playwright verified)."""
        await _auth_user(hub, user_token)
        msg_id = await _send_message(hub, fake_sio)

        # First click: add
        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍",
            "target_message_id": msg_id,
            "group_id": "general",
        })
        assert len(fake_sio.events(EventName.REACTION_ADDED.value)) == 1

        # Second click: toggle off
        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍",
            "target_message_id": msg_id,
            "group_id": "general",
        })

        toggled = fake_sio.events(EventName.REACTION_TOGGLED.value)
        assert len(toggled) == 1
        assert toggled[0]["data"]["removed"] is True
        assert toggled[0]["data"]["emoji"] == "👍"

        persisted = await hub.persistence.load_group("general")
        reactions = [m for m in persisted if m.type == MessageType.REACTION]
        assert len(reactions) == 1
        assert reactions[0].removed is True

    async def test_add_reaction_missing_emoji_emits_error(self, hub, fake_sio, user_token):
        """BUSINESS RULE: missing emoji is a protocol violation — server emits ERROR, never crashes."""
        await _auth_user(hub, user_token)
        msg_id = await _send_message(hub, fake_sio)

        await hub.namespace.on_add_reaction("sid-1", {
            "target_message_id": msg_id,
            "group_id": "general",
        })

        assert fake_sio.events(EventName.REACTION_ADDED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_add_reaction_missing_target_emits_error(self, hub, fake_sio, user_token):
        """BUSINESS RULE: missing target_message_id is a protocol violation — server emits ERROR."""
        await _auth_user(hub, user_token)

        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍",
            "group_id": "general",
        })

        assert fake_sio.events(EventName.REACTION_ADDED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_add_reaction_unauthenticated_rejected(self, hub, fake_sio):
        """BUSINESS RULE (MEADOWS §3.1 line 54): unauthenticated sessions cannot
        emit — the auth gate rejects before any domain logic runs."""
        await hub.namespace.on_connect("sid-1", {})

        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍",
            "target_message_id": "some-id",
            "group_id": "general",
        })

        assert fake_sio.events(EventName.REACTION_ADDED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_different_users_can_react_with_same_emoji(self, hub, fake_sio, user_token):
        """Two users reacting with the same emoji produces two distinct reaction messages."""
        await _auth_user(hub, user_token, sid="sid-1")
        await hub.namespace.on_message("sid-1", _wire("hello"))
        msg_id = await _send_message(hub, fake_sio)

        # Second user
        await hub.namespace.on_connect("sid-2", {})
        await hub.namespace.on_authenticate("sid-2", {"token": user_token(name="bob")})

        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "👍", "target_message_id": msg_id, "group_id": "general",
        })
        await hub.namespace.on_add_reaction("sid-2", {
            "emoji": "👍", "target_message_id": msg_id, "group_id": "general",
        })

        added = fake_sio.events(EventName.REACTION_ADDED.value)
        assert len(added) == 2
        assert added[0]["data"]["user_id"] == "user-alice"
        assert added[1]["data"]["user_id"] == "user-bob"


class TestRemoveReaction:
    """Tests for on_remove_reaction — explicit removal (distinct from toggle-off)."""

    async def test_remove_reaction_emits_removed_and_marks_persisted(
        self, hub, fake_sio, user_token
    ):
        await _auth_user(hub, user_token)
        msg_id = await _send_message(hub, fake_sio)

        await hub.namespace.on_add_reaction("sid-1", {
            "emoji": "❤️", "target_message_id": msg_id, "group_id": "general",
        })

        await hub.namespace.on_remove_reaction("sid-1", {
            "emoji": "❤️", "target_message_id": msg_id, "group_id": "general",
        })

        removed = fake_sio.events(EventName.REACTION_REMOVED.value)
        assert len(removed) == 1
        assert removed[0]["data"]["target_message_id"] == msg_id
        assert removed[0]["data"]["emoji"] == "❤️"

        persisted = await hub.persistence.load_group("general")
        reactions = [m for m in persisted if m.type == MessageType.REACTION]
        assert reactions[0].removed is True

    async def test_remove_nonexistent_reaction_still_emits(self, hub, fake_sio, user_token):
        """Removing a reaction that doesn't exist still emits REACTION_REMOVED."""
        await _auth_user(hub, user_token)

        await hub.namespace.on_remove_reaction("sid-1", {
            "emoji": "😂", "target_message_id": "nonexistent", "group_id": "general",
        })

        removed = fake_sio.events(EventName.REACTION_REMOVED.value)
        assert len(removed) == 1
