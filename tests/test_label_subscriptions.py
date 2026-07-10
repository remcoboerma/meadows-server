"""Tests for label subscriptions — registration, dedup, cascade, RPC routing.

Uses the same FakeSIO + Hub fixture pattern as other test modules.
"""

from __future__ import annotations

from meadows.protocol import EventName, Message, MessageType

from meadows.server.namespace import GENERAL_GROUP


async def _authenticate_bot(hub, fake_sio, bot_token, sid="bot-1"):
    await hub.namespace.on_connect(sid, {})
    await hub.namespace.on_authenticate(sid, {"token": bot_token()})
    fake_sio.emits.clear()


async def _authenticate_user(hub, fake_sio, user_token, sid="user-1"):
    await hub.namespace.on_connect(sid, {})
    await hub.namespace.on_authenticate(sid, {"token": user_token()})
    fake_sio.emits.clear()


class TestLabelSubscriptionRegistration:
    async def test_register_and_ack(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {"name": "all-labels", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        registered = fake_sio.events(EventName.LABEL_SUBSCRIPTION_REGISTERED.value)
        assert len(registered) == 1
        assert registered[0]["data"]["name"] == "all-labels"
        assert registered[0]["to"] == "bot-1"
        assert "*" in hub.label_subscriptions
        assert hub.label_subscriptions["*"][0]["name"] == "all-labels"

    async def test_unregister_and_ack(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "sub1", "predicate": {}, "scope": "global"}
        )
        fake_sio.emits.clear()

        await hub.namespace.on_unregister_label_subscription("bot-1", {"name": "sub1"})

        unreg = fake_sio.events(EventName.LABEL_SUBSCRIPTION_UNREGISTERED.value)
        assert len(unreg) == 1
        assert unreg[0]["data"]["name"] == "sub1"
        assert hub.label_subscriptions.get("*", []) == []

    async def test_auth_required(self, hub, fake_sio):
        await hub.namespace.on_connect("anon", {})
        await hub.namespace.on_register_label_subscription("anon", {"name": "x", "predicate": {}})

        assert fake_sio.events(EventName.LABEL_SUBSCRIPTION_REGISTERED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_bot_only_restriction(self, hub, fake_sio, user_token):
        await _authenticate_user(hub, fake_sio, user_token)
        await hub.namespace.on_register_label_subscription(
            "user-1", {"name": "sub", "predicate": {}, "scope": "global"}
        )

        assert fake_sio.events(EventName.LABEL_SUBSCRIPTION_REGISTERED.value) == []
        errors = fake_sio.events(EventName.ERROR.value)
        assert len(errors) == 1
        assert "only bots" in errors[0]["data"]["error"]

    async def test_missing_name_rejected(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription("bot-1", {"predicate": {}})

        assert fake_sio.events(EventName.LABEL_SUBSCRIPTION_REGISTERED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_invalid_deliver_mode_rejected(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "sub", "predicate": {}, "deliver": "invalid"}
        )

        assert fake_sio.events(EventName.LABEL_SUBSCRIPTION_REGISTERED.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_room_scoped_subscription(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "room-sub", "predicate": {}, "scope": "room", "group_id": "dev"}
        )

        assert "dev" in hub.label_subscriptions
        assert hub.label_subscriptions["dev"][0]["name"] == "room-sub"

    async def test_default_scope_is_room_with_general(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "sub", "predicate": {}}
        )

        assert GENERAL_GROUP in hub.label_subscriptions


class TestLabelEvaluation:
    async def test_empty_predicate_matches_all_labels(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.3): empty predicate matches everything."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["meadows", "room:general", "1.0.0"]],
                "target_msg_id": "msg-1",
                "applied_by": "server",
            },
        )

        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1
        assert assigned[0]["to"] == "bot-1"
        assert assigned[0]["data"]["subscription_name"] == "all"
        assert len(assigned[0]["data"]["labels"]) == 1

    async def test_regex_match_on_origin(self, hub, fake_sio, bot_token):
        """Predicate with regex_match on origin field."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {
                "name": "meadows-only",
                "predicate": {"regex_match": [{"var": "origin"}, "^meadows$"]},
                "scope": "global",
                "deliver": "label_only",
            },
        )
        fake_sio.emits.clear()

        # Matching label
        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["meadows", "room:general", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1

        # Non-matching label
        fake_sio.emits.clear()
        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["other-bot", "sentiment", "1.0.0"]], "target_msg_id": "msg-2", "applied_by": "server"},
        )
        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []

    async def test_deliver_label_only(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {"name": "lo", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )

        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1

    async def test_deliver_message_only_no_label_event(self, hub, fake_sio, bot_token):
        """With deliver='message_only', LABEL_ASSIGNED should NOT be emitted (TODO: message delivery)."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {"name": "mo", "predicate": {}, "scope": "global", "deliver": "message_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )

        # message_only: no LABEL_ASSIGNED (message delivery is TODO)
        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []

    async def test_deliver_both_emits_label(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {"name": "both", "predicate": {}, "scope": "global", "deliver": "both"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )

        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1


class TestAutoRoomLabel:
    async def test_auto_room_label_on_user_message(self, hub, fake_sio, bot_token, user_token):
        """BUSINESS RULE (§2.1): every non-RPC message gets a room label."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {
                "name": "room-general",
                "predicate": {"==": [{"var": "label"}, "room:general"]},
                "scope": "global",
                "deliver": "label_only",
            },
        )
        fake_sio.emits.clear()

        # Send a user message
        await _authenticate_user(hub, fake_sio, user_token, sid="user-sid")
        from meadows.protocol import Message, MessageType

        msg = Message(type=MessageType.USER, user_id="user-alice", group_id="general", content="hi")
        from meadows.server.chokepoint import message_to_wire

        await hub.namespace.on_message("user-sid", message_to_wire(msg))

        # The bot should have received LABEL_ASSIGNED with the auto-room-label
        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1
        lbl = assigned[0]["data"]["labels"][0]
        assert lbl["origin"] == "meadows"
        assert lbl["label"] == "room:general"
        assert lbl["semver"] == "1.0.0"

    async def test_rpc_messages_not_room_broadcast(self, hub, fake_sio, bot_token, user_token):
        """BUSINESS RULE (§2.10): RPC messages are NOT room-broadcast."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await _authenticate_user(hub, fake_sio, user_token, sid="user-sid")

        msg = Message(
            type=MessageType.RPC_REQUEST,
            user_id="user-alice",
            group_id="general",
            content='{"method": "ping"}',
        )
        from meadows.server.chokepoint import message_to_wire

        await hub.namespace.on_message("user-sid", message_to_wire(msg))

        # No MESSAGE broadcast to room
        messages = fake_sio.events(EventName.MESSAGE.value)
        assert messages == []

        # But persisted
        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].type == MessageType.RPC_REQUEST

    async def test_rpc_response_not_room_broadcast(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)

        msg = Message(
            type=MessageType.RPC_RESPONSE,
            user_id="bot-echo",
            bot_name="echo",
            group_id="general",
            content='{"result": "pong"}',
        )
        from meadows.server.chokepoint import message_to_wire

        await hub.namespace.on_message("bot-1", message_to_wire(msg))

        messages = fake_sio.events(EventName.MESSAGE.value)
        assert messages == []

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].type == MessageType.RPC_RESPONSE


class TestLabelDedup:
    async def test_same_key_dropped(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.5): same (origin, label, semver, message_id) = duplicate."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        data = {
            "labels": [["meadows", "sentiment", "1.0.0"]],
            "target_msg_id": "msg-1",
            "applied_by": "server",
        }
        await hub.namespace.on_label_assigned("bot-1", data)
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1

        # Same key again — should be deduped
        fake_sio.emits.clear()
        await hub.namespace.on_label_assigned("bot-1", data)
        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []

    async def test_metadata_excluded_from_dedup_key(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.2): metadata is NOT part of the dedup key."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        # First: no metadata
        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["meadows", "sentiment", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1

        # Same key but different metadata — still duplicate
        fake_sio.emits.clear()
        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["meadows", "sentiment", "1.0.0", {"score": 0.9}]],
                "target_msg_id": "msg-1",
                "applied_by": "server",
            },
        )
        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []

    async def test_different_message_id_is_not_duplicate(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1

        fake_sio.emits.clear()
        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-2", "applied_by": "server"},
        )
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1

    async def test_different_semver_is_not_duplicate(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "1.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )
        fake_sio.emits.clear()
        await hub.namespace.on_label_assigned(
            "bot-1",
            {"labels": [["m", "l", "2.0.0"]], "target_msg_id": "msg-1", "applied_by": "server"},
        )
        assert len(fake_sio.events(EventName.LABEL_ASSIGNED.value)) == 1


class TestCascade:
    async def test_bot_produces_label_triggers_evaluation(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.5): bot-produced labels enter the same pipeline."""
        await _authenticate_bot(hub, fake_sio, bot_token, sid="bot-1")

        # Register a subscription that matches sentiment labels
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {
                "name": "sentiment-watcher",
                "predicate": {"==": [{"var": "label"}, "sentiment"]},
                "scope": "global",
                "deliver": "label_only",
            },
        )
        fake_sio.emits.clear()

        # Bot produces a label via on_label_assigned
        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["bot-analyzer", "sentiment", "1.0.0", {"score": 0.5}]],
                "target_msg_id": "msg-42",
                "applied_by": "bot-analyzer",
            },
        )

        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1
        assert assigned[0]["data"]["subscription_name"] == "sentiment-watcher"
        assert assigned[0]["data"]["target_msg_id"] == "msg-42"
        assert assigned[0]["data"]["applied_by"] == "bot-analyzer"

    async def test_multiple_labels_some_match(self, hub, fake_sio, bot_token):
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1",
            {
                "name": "room-only",
                "predicate": {"regex_match": [{"var": "label"}, "^room:"]},
                "scope": "global",
                "deliver": "label_only",
            },
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [
                    ["meadows", "room:general", "1.0.0"],
                    ["bot-x", "sentiment", "1.0.0"],
                ],
                "target_msg_id": "msg-1",
                "applied_by": "server",
            },
        )

        assigned = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(assigned) == 1
        # Only the room label should match
        assert len(assigned[0]["data"]["labels"]) == 1
        assert assigned[0]["data"]["labels"][0]["label"] == "room:general"


class TestMetadataValidation:
    async def test_oversized_metadata_rejected(self, hub, fake_sio, bot_token):
        from meadows.protocol.labels import MAX_LABEL_METADATA_LENGTH

        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        huge = "x" * (MAX_LABEL_METADATA_LENGTH + 1)
        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["m", "l", "1.0.0", {"data": huge}]],
                "target_msg_id": "msg-1",
                "applied_by": "server",
            },
        )

        errors = fake_sio.events(EventName.ERROR.value)
        assert len(errors) == 1
        assert "too large" in errors[0]["data"]["error"]
        # No labels should be delivered
        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []

    async def test_short_labels_skipped(self, hub, fake_sio, bot_token):
        """Labels with fewer than 3 elements are silently skipped."""
        await _authenticate_bot(hub, fake_sio, bot_token)
        await hub.namespace.on_register_label_subscription(
            "bot-1", {"name": "all", "predicate": {}, "scope": "global", "deliver": "label_only"},
        )
        fake_sio.emits.clear()

        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["m", "l"]],  # only 2 elements
                "target_msg_id": "msg-1",
                "applied_by": "server",
            },
        )

        assert fake_sio.events(EventName.LABEL_ASSIGNED.value) == []


class TestDedupIndex:
    async def test_hub_has_dedup_index(self, hub):
        assert hub.label_dedup is not None

    async def test_dedup_close(self, hub):
        """LabelDedupIndex.close() should not raise."""
        hub.label_dedup.close()

    def test_dedup_add_returns_true_for_new(self, tmp_path):
        from meadows.server.label_dedup import LabelDedupIndex

        idx = LabelDedupIndex(tmp_path / "cache")
        assert idx.add("m", "l", "1.0.0", "msg-1") is True
        assert idx.add("m", "l", "1.0.0", "msg-1") is False
        idx.close()

    def test_dedup_contains(self, tmp_path):
        from meadows.server.label_dedup import LabelDedupIndex

        idx = LabelDedupIndex(tmp_path / "cache")
        assert idx.contains("m", "l", "1.0.0", "msg-1") is False
        idx.add("m", "l", "1.0.0", "msg-1")
        assert idx.contains("m", "l", "1.0.0", "msg-1") is True
        idx.close()


class TestLabelPersistence:
    async def test_label_assigned_persisted_in_jsonl(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.9): LABEL_ASSIGNED events are stored as
        separate records in the group JSONL.  The server never merges
        MESSAGE and LABEL_ASSIGNED — they are distinct records.
        """
        await _authenticate_bot(hub, fake_sio, bot_token, sid="bot-1")

        # First store a message so the group exists
        msg = Message(type=MessageType.USER, user_id="user-1", group_id="general", content="hello")
        await hub.persistence.store("general", msg)

        # Bot produces a label on that message
        await hub.namespace.on_label_assigned(
            "bot-1",
            {
                "labels": [["bot-sentiment", "sentiment", "1.0.0", {"score": -0.9, "tone": "angry"}]],
                "target_msg_id": msg.id,
                "applied_by": "bot-sentiment",
            },
        )

        # Load raw JSONL lines — should have 2 records: 1 MESSAGE + 1 LABEL_ASSIGNED
        import json
        path = hub.persistence._path("general")
        lines = [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]
        assert len(lines) == 2

        label_record = json.loads(lines[1])
        assert label_record["event"] == "label_assigned"
        assert label_record["target_msg_id"] == msg.id
        assert label_record["applied_by"] == "bot-sentiment"
        assert len(label_record["labels"]) == 1
        assert label_record["labels"][0]["origin"] == "bot-sentiment"
        assert label_record["labels"][0]["metadata"] == {"score": -0.9, "tone": "angry"}

    async def test_auto_room_label_not_persisted(self, hub, fake_sio, user_token):
        """BUSINESS RULE (§2.9): auto-room-labels are NOT persisted.
        The room is already in the filename.
        """
        await _authenticate_user(hub, fake_sio, user_token, sid="user-1")

        msg = Message(type=MessageType.USER, user_id="user-1", group_id="general", content="hi")
        await hub.namespace._dispatch_message(msg)

        # JSONL should have only the MESSAGE record, no LABEL_ASSIGNED
        import json
        path = hub.persistence._path("general")
        lines = [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]
        assert len(lines) == 1
        record = json.loads(lines[0])
        assert record["type"] == "user"

    async def test_duplicate_label_not_persisted_twice(self, hub, fake_sio, bot_token):
        """BUSINESS RULE (§2.5): dedup prevents duplicate persistence."""
        await _authenticate_bot(hub, fake_sio, bot_token, sid="bot-1")

        msg = Message(type=MessageType.USER, user_id="user-1", group_id="general", content="hello")
        await hub.persistence.store("general", msg)

        label_data = {
            "labels": [["bot-x", "sentiment", "1.0.0"]],
            "target_msg_id": msg.id,
            "applied_by": "bot-x",
        }

        # First call — persisted
        await hub.namespace.on_label_assigned("bot-1", label_data)
        # Second call — deduped, NOT persisted again
        await hub.namespace.on_label_assigned("bot-1", label_data)

        path = hub.persistence._path("general")
        lines = [ln.strip() for ln in path.read_text().splitlines() if ln.strip()]
        # 1 MESSAGE + 1 LABEL_ASSIGNED (not 2)
        assert len(lines) == 2
