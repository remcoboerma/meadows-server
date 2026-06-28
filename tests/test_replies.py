"""Unit tests for reply (quoted_message) handling in ChatNamespace.

BUSINESS RULE (MEADOWS §3.3 line 73): replies are core — "zonder reacties,
mentions en replies is er geen interactie." A reply is a user message with
a `quoted_message` field referencing the original message. The server
passes this through as part of the envelope (MEADOWS §3.2 line 64:
quoted_message is a pass-through field the system carries but does not
interpret).

These tests verify the behavior validated via Playwright:
1. A message with quoted_message broadcasts the reply context.
2. The persisted message includes the quoted_message.
3. The quoted_message shape matches the protocol (id, author, content, timestamp).
"""

from __future__ import annotations

from meadows.protocol import EventName, Message, MessageType


async def _auth_user(hub, user_token, sid: str = "sid-1") -> None:
    """Helper: connect + authenticate a user session.

    BUSINESS RULE (MEADOWS §3.1 line 54): auth is event-based — on_connect
    creates an unauthenticated session, on_authenticate with a valid JWT
    transitions it to authenticated. Tests must go through both steps.
    """
    await hub.namespace.on_connect(sid, {})
    await hub.namespace.on_authenticate(sid, {"token": user_token()})


def _wire_with_reply(content: str, quoted: dict, *, user_id: str = "user-alice") -> dict:
    """Build a message wire dict with a quoted_message (reply context).

    BUSINESS RULE (MEADOWS §3.2 line 64-65): quoted_message is a pass-through
    field — the system carries it but does not interpret it. The server
    constructs the broadcast from the envelope shape, not from the reply content.
    """
    msg = Message(type=MessageType.USER, user_id=user_id, group_id="general", content=content)
    wire = msg.model_dump(exclude_none=True)
    wire["quoted_message"] = quoted
    return wire


class TestReplies:
    """Tests for reply context — the ⤴️ Reply button behavior verified via Playwright."""

    async def test_reply_broadcasts_with_quoted_message(self, hub, fake_sio, user_token):
        """Clicking ⤴️ Reply and sending includes quoted_message in the broadcast."""
        await _auth_user(hub, user_token)

        # First, send a target message
        target = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="original")
        target_wire = target.model_dump(exclude_none=True)
        await hub.namespace.on_message("sid-1", target_wire)

        original_id = fake_sio.events(EventName.MESSAGE.value)[0]["data"]["id"]

        # Now send a reply
        quoted = {
            "id": original_id,
            "author": "alice",
            "content": "original",
            "timestamp": target.timestamp,
        }
        await hub.namespace.on_message("sid-1", _wire_with_reply("replying to you", quoted))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert len(msgs) == 2
        reply = msgs[1]
        assert reply["data"]["content"] == "replying to you"
        assert reply["data"]["quoted_message"]["id"] == original_id
        assert reply["data"]["quoted_message"]["author"] == "alice"
        assert reply["data"]["quoted_message"]["content"] == "original"

    async def test_reply_persists_with_quoted_message(self, hub, fake_sio, user_token):
        """The quoted_message survives to disk (JSONL) so reload shows reply context."""
        await _auth_user(hub, user_token)

        target = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hello")
        await hub.namespace.on_message("sid-1", target.model_dump(exclude_none=True))
        original_id = fake_sio.events(EventName.MESSAGE.value)[0]["data"]["id"]

        quoted = {
            "id": original_id,
            "author": "alice",
            "content": "hello",
            "timestamp": target.timestamp,
        }
        await hub.namespace.on_message("sid-1", _wire_with_reply("reply body", quoted))

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 2
        reply = persisted[1]
        assert reply.content == "reply body"
        assert reply.quoted_message is not None
        assert reply.quoted_message.id == original_id
        assert reply.quoted_message.author == "alice"
        assert reply.quoted_message.content == "hello"

    async def test_reply_to_bot_message_includes_bot_name(self, hub, fake_sio, user_token):
        """Replying to a bot message carries bot_name in the quoted_message."""
        await _auth_user(hub, user_token)

        # Simulate a bot message the user is replying to
        quoted = {
            "id": "some-bot-msg-id",
            "author": "echo (bot)",
            "content": "Pong!",
            "timestamp": "2026-01-01T00:00:00.000000",
            "bot_name": "echo",
        }
        await hub.namespace.on_message("sid-1", _wire_with_reply("thanks bot", quoted))

        msgs = fake_sio.events(EventName.MESSAGE.value)
        assert msgs[0]["data"]["quoted_message"]["bot_name"] == "echo"

    async def test_message_without_reply_has_no_quoted_message(self, hub, fake_sio, user_token):
        """A plain message has no quoted_message field."""
        await _auth_user(hub, user_token)

        msg = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="plain")
        await hub.namespace.on_message("sid-1", msg.model_dump(exclude_none=True))

        broadcast = fake_sio.events(EventName.MESSAGE.value)[0]
        assert broadcast["data"].get("quoted_message") is None or "quoted_message" not in broadcast["data"]
