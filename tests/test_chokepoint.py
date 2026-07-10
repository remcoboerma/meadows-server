"""Tests for the chokepoint — validate_frame and the message_to_wire re-export.

Every client-bound frame must pass validate_frame. Message-carrying events
round-trip through the protocol envelope; system events are a closed set;
unknown events and non-dict data are rejected.
"""

from __future__ import annotations

import pytest

from meadows.protocol import EventName, Message, MessageType
from meadows.protocol.codec import message_to_wire as codec_message_to_wire

from meadows.server.chokepoint import MESSAGE_EVENTS, message_to_wire, validate_frame


def _valid_message_wire() -> dict:
    return Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hi").model_dump(
        exclude_none=True
    )


class TestValidateFrameMessages:
    def test_accepts_valid_message_frame(self):
        validate_frame(EventName.MESSAGE, _valid_message_wire())

    def test_accepts_valid_bot_response_frame_deprecated(self):
        """BOT_RESPONSE is deprecated but still validated for backward compat."""
        wire = Message(
            type=MessageType.BOT,
            user_id="bot-echo",
            bot_name="echo",
            group_id="general",
            content="pong",
        ).model_dump(exclude_none=True)
        validate_frame(EventName.BOT_RESPONSE, wire)

    def test_rejects_missing_required_field(self):
        with pytest.raises(ValueError, match="invalid Message frame"):
            validate_frame(EventName.MESSAGE, {"type": "user", "user_id": "u"})

    def test_rejects_extra_field(self):
        wire = _valid_message_wire()
        wire["bogus_field"] = "no"
        with pytest.raises(ValueError, match="invalid Message frame"):
            validate_frame(EventName.MESSAGE, wire)

    def test_rejects_wrong_type_value(self):
        with pytest.raises(ValueError, match="invalid Message frame"):
            validate_frame(
                EventName.MESSAGE,
                {"type": "bogus", "user_id": "u", "group_id": "g", "content": "c"},
            )

    def test_rejects_non_dict_data(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_frame(EventName.MESSAGE, ["not", "a", "dict"])

    def test_accepts_event_name_enum_or_string(self):
        wire = _valid_message_wire()
        validate_frame("message", wire)
        validate_frame(EventName.MESSAGE, wire)


class TestValidateFrameSystemEvents:
    def test_accepts_known_system_event_with_dict(self):
        validate_frame(EventName.USER_TYPING, {"group_id": "general", "user_id": "u"})

    def test_accepts_authenticated_event(self):
        validate_frame(EventName.AUTHENTICATED, {"user_id": "user-alice"})

    def test_rejects_unknown_event(self):
        with pytest.raises(ValueError, match="unknown event"):
            validate_frame("totally-made-up", {"a": 1})

    def test_rejects_non_dict_for_system_event(self):
        with pytest.raises(ValueError, match="must be a dict"):
            validate_frame(EventName.USER_TYPING, 42)

    def test_message_events_set_is_contracted(self):
        assert EventName.MESSAGE in MESSAGE_EVENTS
        # BOT_RESPONSE is deprecated but kept in MESSAGE_EVENTS for backward compat
        assert EventName.BOT_RESPONSE in MESSAGE_EVENTS
        assert EventName.USER_TYPING not in MESSAGE_EVENTS


class TestMessageToWireReexport:
    def test_matches_codec(self):
        msg = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hi")
        assert message_to_wire(msg) == codec_message_to_wire(msg)

    def test_round_trips_through_validate_frame(self):
        msg = Message(
            type=MessageType.BOT,
            user_id="bot-echo",
            bot_name="echo",
            group_id="general",
            content="pong",
        )
        wire = message_to_wire(msg)
        validate_frame(EventName.MESSAGE, wire)
