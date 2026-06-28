"""Tests for the Hub — the server-as-object.

The hard invariant: state lives on the instance, not in module globals. Two
Hub instances must not share state. emit_frame is the chokepoint: it
validates against the protocol before anything hits the wire.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import socketio

from meadows.protocol import EventName, Message, MessageType

from meadows.server.groups import GroupState
from meadows.server.hub import Hub
from meadows.server.namespace import ChatNamespace
from meadows.server.persistence import JSONLPersistence


class TestHubConstruction:
    def test_stores_config_on_instance(self, hub: Hub, jwt_secret: bytes, messages_dir: Path):
        assert hub.jwt_secret == jwt_secret
        assert hub.messages_dir == messages_dir
        assert hub.cors_origins == "*"

    def test_state_attrs_are_instance_dicts(self, hub: Hub):
        for attr in ("user_sessions", "bot_registry", "groups", "pattern_registry"):
            assert getattr(hub, attr) == {}
            assert isinstance(getattr(hub, attr), dict)

    def test_sio_is_async_server(self, hub: Hub):
        assert isinstance(hub.sio, socketio.AsyncServer)

    def test_namespace_registered_and_back_ref(self, hub: Hub):
        assert isinstance(hub.namespace, ChatNamespace)
        assert hub.namespace.hub is hub
        assert hub.namespace.server is hub.sio

    def test_persistence_wired(self, hub: Hub):
        assert isinstance(hub.persistence, JSONLPersistence)
        assert hub.persistence.messages_dir == hub.messages_dir

    def test_state_is_not_module_global(self):
        import meadows.server.hub as hub_mod

        # The module must not carry the per-instance state attrs.
        for attr in ("user_sessions", "bot_registry", "groups", "sio"):
            assert not hasattr(hub_mod, attr), f"Hub module must not expose {attr!r}"


class TestHubStateIsolation:
    def test_two_hubs_have_distinct_state(self, jwt_secret: bytes, messages_dir: Path):
        a = Hub(jwt_secret=jwt_secret, messages_dir=messages_dir / "a")
        b = Hub(jwt_secret=jwt_secret, messages_dir=messages_dir / "b")

        a.user_sessions["sid-1"] = {"authenticated": True}
        a.bot_registry["echo"] = {}
        a.groups["general"] = GroupState(group_id="general")

        assert b.user_sessions == {}
        assert b.bot_registry == {}
        assert b.groups == {}
        assert a.user_sessions is not b.user_sessions
        assert a.sio is not b.sio

    def test_mutating_one_does_not_leak(self, jwt_secret: bytes, messages_dir: Path):
        a = Hub(jwt_secret=jwt_secret, messages_dir=messages_dir / "a")
        b = Hub(jwt_secret=jwt_secret, messages_dir=messages_dir / "b")

        a.pattern_registry["scope"] = [{"entry": 1}]
        assert b.pattern_registry == {}


class TestHubLifecycle:
    async def test_start_seeds_general_and_discovers_groups(self, jwt_secret: bytes, tmp_path: Path):
        """BUSINESS RULE: groups are discovered from JSONL files on disk.

        The messages_dir is created in JSONLPersistence.__init__ (once, at
        construction time). Hub.start() then scans for *.jsonl files and
        creates GroupState entries for each. "general" is always seeded
        even if no general.jsonl exists yet (fresh install).
        """
        msgs_dir = tmp_path / "fresh" / "msgs"
        # Simulate pre-existing groups on disk
        msgs_dir.mkdir(parents=True)
        msg_line = '{"id":"1","type":"user","user_id":"u","group_id":"g","content":"hi"}\n'
        (msgs_dir / "general.jsonl").write_text(msg_line)
        (msgs_dir / "testgroep.jsonl").write_text(msg_line)
        # Deleted group should be skipped
        (msgs_dir / "oldgroup.jsonl.deleted").write_text("ignored\n")

        hub = Hub(jwt_secret=jwt_secret, messages_dir=msgs_dir)
        await hub.start()

        assert "general" in hub.groups
        assert "testgroep" in hub.groups
        assert "oldgroup" not in hub.groups
        assert isinstance(hub.groups["general"], GroupState)
        assert isinstance(hub.groups["testgroep"], GroupState)

    async def test_start_is_idempotent(self, hub: Hub):
        await hub.start()
        await hub.start()
        assert "general" in hub.groups

    async def test_stop_is_awaitable(self, hub: Hub):
        await hub.stop()  # no-op for Sprint 1, but must be awaitable


class TestHubEmitFrame:
    async def test_valid_message_emits(self, hub: Hub, fake_sio):
        msg = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hi")
        wire = msg.model_dump(exclude_none=True)
        await hub.emit_frame(EventName.MESSAGE, wire, room="general")

        assert len(fake_sio.emits) == 1
        e = fake_sio.emits[0]
        assert e["event"] == EventName.MESSAGE.value
        assert e["data"] == wire
        assert e["room"] == "general"
        assert e["namespace"] == "/chat"

    async def test_invalid_message_raises_and_does_not_emit(self, hub: Hub, fake_sio):
        with pytest.raises(ValueError, match="invalid Message frame"):
            await hub.emit_frame(EventName.MESSAGE, {"type": "user", "user_id": "u"}, room="general")
        assert fake_sio.emits == []

    async def test_unknown_event_rejected(self, hub: Hub, fake_sio):
        with pytest.raises(ValueError, match="unknown event"):
            await hub.emit_frame("not-a-real-event", {"a": 1})
        assert fake_sio.emits == []

    async def test_non_dict_data_rejected(self, hub: Hub, fake_sio):
        with pytest.raises(ValueError, match="must be a dict"):
            await hub.emit_frame(EventName.USER_TYPING, "not a dict")
        assert fake_sio.emits == []

    async def test_system_event_accepted(self, hub: Hub, fake_sio):
        await hub.emit_frame(
            EventName.USER_TYPING,
            {"group_id": "general", "user_id": "user-alice"},
            room="general",
        )
        assert len(fake_sio.emits) == 1
        assert fake_sio.emits[0]["event"] == EventName.USER_TYPING.value

    async def test_sid_targeting_uses_to(self, hub: Hub, fake_sio):
        msg = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hi")
        await hub.emit_frame(EventName.MESSAGE, msg.model_dump(exclude_none=True), sid="sid-1")
        assert fake_sio.emits[0]["to"] == "sid-1"
        assert fake_sio.emits[0]["room"] is None
