"""The client edge — single chokepoint emit.

Every client-bound frame passes through ``validate_frame`` before it reaches
the wire. This is the "client edge" from MEADOWS-migration-intent.md section
3.4. The monolith had ~100 scattered ``self.emit(...)`` call sites with no
central validation; this is the consolidation.

Rules:
  - Message-carrying events (MESSAGE, BOT_RESPONSE [deprecated]): the data must round-trip
    through the protocol envelope (meadows.protocol.codec.message_from_wire).
  - Other known events (closed set in EventName): accepted as long as data is
    a dict. Their payload shapes are not yet formalised in the protocol
    package, so the chokepoint only asserts the event is contracted.
  - Unknown event names: rejected.

The peer edge (server-to-server) does not exist yet.
"""

from __future__ import annotations

from typing import Any

from meadows.protocol import EventName, Message
from meadows.protocol.codec import message_from_wire, message_to_wire as _codec_message_to_wire

# Events whose payload is a Message envelope.
MESSAGE_EVENTS: frozenset[EventName] = frozenset({EventName.MESSAGE, EventName.BOT_RESPONSE})


def _event_name(event: EventName | str) -> str:
    return event.value if isinstance(event, EventName) else str(event)


def validate_frame(event: EventName | str, data: Any) -> None:
    """Raise ``ValueError`` if ``(event, data)`` is not a valid protocol frame."""
    name = _event_name(event)
    try:
        ev = EventName(name)
    except ValueError:
        raise ValueError(f"unknown event: {name!r}") from None

    if not isinstance(data, dict):
        raise ValueError(f"data for {name!r} must be a dict, got {type(data).__name__}")

    if ev in MESSAGE_EVENTS:
        try:
            message_from_wire(data)
        except Exception as exc:  # pydantic ValidationError or codec errors
            raise ValueError(f"invalid Message frame for {name!r}: {exc}") from exc


def message_to_wire(msg: Message) -> dict[str, Any]:
    """Serialize a Message to its on-the-wire dict (delegates to the protocol codec)."""
    return _codec_message_to_wire(msg)


__all__ = ["MESSAGE_EVENTS", "message_to_wire", "validate_frame"]
