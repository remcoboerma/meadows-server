"""Tests for form submission handling and metadata sanitization.

BUSINESS RULE (MEADOWS-forms-intent §2.9): form submissions are treated
as normal messages that route via label subscriptions. No room broadcast.
BUSINESS RULE (MEADOWS-forms-intent §2.4): metadata['meadows'] is the
protocol-protected namespace. Unknown keys are stripped.
"""

from __future__ import annotations

from meadows.protocol import EventName, Label, Message, MessageType


class TestOnFormSubmission:
    """Tests for the on_form_submission handler."""

    async def test_form_submission_persists(self, hub, fake_sio, user_token):  # noqa: ARG002
        """BUSINESS RULE (§2.9): form submissions are persisted in JSONL."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {"mood": "goed", "stemming": "8"},
            "reply_to": "original-msg-id",
            "answer_label": ["bot-daily", "checkin-resp", "1.0.0"],
            "group_id": "general",
        })

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        msg = persisted[0]
        assert msg.type == MessageType.FORM_SUBMISSION
        assert "goed" in msg.content
        assert msg.user_id == "user-alice"

    async def test_form_submission_does_not_room_broadcast(self, hub, fake_sio, user_token):
        """BUSINESS RULE (§2.9): form submissions are NOT room-broadcast (like RPC)."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {"mood": "goed"},
            "reply_to": "",
            "answer_label": ["bot-daily", "checkin-resp", "1.0.0"],
        })

        # No MESSAGE event should be emitted (no room broadcast)
        msg_events = fake_sio.events(EventName.MESSAGE.value)
        assert len(msg_events) == 0

    async def test_form_submission_evaluates_label_subscriptions(self, hub, fake_sio, user_token, bot_token):
        """BUSINESS RULE (§2.9): form submissions evaluate label subscriptions."""
        # Register a bot with a label subscription
        await hub.namespace.on_connect("bot-sid", {})
        await hub.namespace.on_authenticate("bot-sid", {"token": bot_token(name="daily")})

        # Register a label subscription that matches the answer_label
        await hub.namespace.on_register_label_subscription("bot-sid", {
            "name": "checkin-listener",
            "predicate": {
                "and": [
                    {"==": [{"var": "origin"}, "bot-daily"]},
                    {"==": [{"var": "label"}, "checkin-resp"]},
                ]
            },
            "scope": "global",
            "deliver": "both",
        })
        fake_sio.emits.clear()

        # User submits a form
        await hub.namespace.on_connect("user-sid", {})
        await hub.namespace.on_authenticate("user-sid", {"token": user_token()})

        await hub.namespace.on_form_submission("user-sid", {
            "form_data": {"mood": "goed"},
            "answer_label": ["bot-daily", "checkin-resp", "1.0.0"],
        })

        # The bot should receive the submission via label subscription
        label_events = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        assert len(label_events) >= 1
        assert label_events[0]["to"] == "bot-sid"
        assert label_events[0]["data"]["subscription_name"] == "checkin-listener"

    async def test_form_submission_unauthenticated_rejected(self, hub, fake_sio):
        """Form submission from unauthenticated session is rejected."""
        await hub.namespace.on_connect("sid-1", {})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {"mood": "goed"},
        })

        assert fake_sio.events(EventName.MESSAGE.value) == []
        assert len(fake_sio.events(EventName.ERROR.value)) == 1

    async def test_form_submission_with_empty_data(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Form submission with empty form_data still persists."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {},
        })

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        assert persisted[0].type == MessageType.FORM_SUBMISSION
        assert persisted[0].content == "Formulier ingevuld"

    async def test_form_submission_carries_answer_label(self, hub, fake_sio, user_token):  # noqa: ARG002
        """The answer_label from event data is placed as a label on the message."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {"x": "y"},
            "answer_label": ["bot-a", "my-resp", "1.0.0"],
        })

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        msg = persisted[0]
        assert len(msg.labels) == 1
        assert msg.labels[0] == Label("bot-a", "my-resp", "1.0.0")

    async def test_form_submission_response_in_metadata(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Response data is placed in metadata['meadows']['form_handling']['response']."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_form_submission("sid-1", {
            "form_data": {"dag": "goed", "stemming": "8"},
            "answer_label": ["bot-a", "resp", "1.0.0"],
        })

        persisted = await hub.persistence.load_group("general")
        msg = persisted[0]
        assert msg.metadata["meadows"]["form_handling"]["response"] == {"dag": "goed", "stemming": "8"}


class TestMetadataSanitization:
    """BUSINESS RULE (MEADOWS-forms-intent §2.4): the server strips unknown
    keys from metadata['meadows'] before persisting or routing.
    """

    async def test_unknown_meadows_keys_removed(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Unknown keys under metadata['meadows'] are stripped."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", {
            "type": "user",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "test",
            "metadata": {
                "meadows": {
                    "form_handling": {"answer_label": ["a", "b", "1.0.0"]},
                    "evil_extension": {"data": "should be removed"},
                }
            },
        })

        persisted = await hub.persistence.load_group("general")
        assert len(persisted) == 1
        # Known key preserved
        assert "form_handling" in persisted[0].metadata.get("meadows", {})
        # Unknown key removed
        assert "evil_extension" not in persisted[0].metadata.get("meadows", {})

    async def test_domain_metadata_untouched(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Domain metadata (non-meadows keys) is never modified."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", {
            "type": "user",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "test",
            "metadata": {
                "meadows": {"form_handling": {"x": 1}},
                "custom_domain": {"key": "value"},
                "another_key": {"nested": True},
            },
        })

        persisted = await hub.persistence.load_group("general")
        msg = persisted[0]
        assert msg.metadata["custom_domain"] == {"key": "value"}
        assert msg.metadata["another_key"] == {"nested": True}

    async def test_known_meadows_key_preserved(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Known key 'form_handling' under meadows is preserved."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", {
            "type": "user",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "test",
            "metadata": {
                "meadows": {
                    "form_handling": {
                        "answer_label": ["a", "b", "1.0.0"],
                        "form": "<form></form>",
                    }
                }
            },
        })

        persisted = await hub.persistence.load_group("general")
        fh = persisted[0].metadata["meadows"]["form_handling"]
        assert fh["answer_label"] == ["a", "b", "1.0.0"]
        assert fh["form"] == "<form></form>"

    async def test_all_meadows_keys_unknown_removes_entirely(self, hub, fake_sio, user_token):  # noqa: ARG002
        """If all keys under meadows are unknown, the meadows key is removed entirely."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", {
            "type": "user",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "test",
            "metadata": {
                "meadows": {
                    "unknown_key": "should be gone",
                },
                "domain_key": "keep",
            },
        })

        persisted = await hub.persistence.load_group("general")
        msg = persisted[0]
        assert "meadows" not in msg.metadata
        assert msg.metadata["domain_key"] == "keep"

    async def test_no_meadows_key_no_crash(self, hub, fake_sio, user_token):  # noqa: ARG002
        """No meadows key in metadata is fine — nothing to sanitize."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        await hub.namespace.on_message("sid-1", {
            "type": "user",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "test",
            "metadata": {"domain_only": True},
        })

        persisted = await hub.persistence.load_group("general")
        assert persisted[0].metadata["domain_only"] is True

    async def test_sanitization_on_form_submission(self, hub, fake_sio, user_token):  # noqa: ARG002
        """Metadata sanitization also runs on form submissions."""
        await hub.namespace.on_connect("sid-1", {})
        await hub.namespace.on_authenticate("sid-1", {"token": user_token()})

        # on_form_submission builds its own metadata['meadows'], so unknown
        # keys wouldn't normally be there. But the sanitization runs anyway.
        # Test via the on_message path with a FORM_SUBMISSION type.
        await hub.namespace.on_message("sid-1", {
            "type": "form_submission",
            "user_id": "user-alice",
            "group_id": "general",
            "content": "submitted",
            "metadata": {
                "meadows": {
                    "form_handling": {"response": {"x": 1}},
                    "rogue": {"data": "gone"},
                }
            },
        })

        persisted = await hub.persistence.load_group("general")
        msg = persisted[0]
        assert "rogue" not in msg.metadata.get("meadows", {})
        assert "form_handling" in msg.metadata["meadows"]


class TestFormIntegration:
    """End-to-end form flow: bot sends form → user submits → bots receive."""

    async def test_bot_sends_form_user_submits_bots_receive(self, hub, fake_sio, bot_token, user_token):
        """Full flow: bot sends form with interactive-form label, user submits,
        subscriber bot receives via label subscription.
        """
        # Setup: two bots — one sends the form, one subscribes to responses
        await hub.namespace.on_connect("bot-former-sid", {})
        await hub.namespace.on_authenticate("bot-former-sid", {"token": bot_token(name="daily")})

        await hub.namespace.on_connect("bot-listener-sid", {})
        await hub.namespace.on_authenticate("bot-listener-sid", {"token": bot_token(name="archiver")})

        # Listener subscribes to checkin responses
        await hub.namespace.on_register_label_subscription("bot-listener-sid", {
            "name": "archive-checkins",
            "predicate": {
                "and": [
                    {"==": [{"var": "origin"}, "bot-daily"]},
                    {"==": [{"var": "label"}, "checkin-resp"]},
                ]
            },
            "scope": "global",
            "deliver": "label_only",
        })

        # Former sends a form message (this would normally be via on_message)
        form_msg = Message(
            type=MessageType.BOT,
            user_id="bot-daily",
            bot_name="daily",
            group_id="general",
            content="Hoe was je dag?",
            labels=[Label("meadows", "interactive-form", "1.0.0")],
            metadata={
                "meadows": {
                    "form_handling": {
                        "answer_label": ["bot-daily", "checkin-resp", "1.0.0"],
                        "form": "<form><input name='mood'></form>",
                    }
                }
            },
        )
        from meadows.server.chokepoint import message_to_wire
        wire = message_to_wire(form_msg)
        await hub.namespace.on_message("bot-former-sid", wire)
        fake_sio.emits.clear()

        # User submits the form
        await hub.namespace.on_connect("user-sid", {})
        await hub.namespace.on_authenticate("user-sid", {"token": user_token()})

        await hub.namespace.on_form_submission("user-sid", {
            "form_data": {"mood": "goed"},
            "answer_label": ["bot-daily", "checkin-resp", "1.0.0"],
        })

        # Listener bot should receive the submission via label subscription
        label_events = fake_sio.events(EventName.LABEL_ASSIGNED.value)
        matching = [e for e in label_events if e.get("to") == "bot-listener-sid"]
        assert len(matching) >= 1
        assert matching[0]["data"]["subscription_name"] == "archive-checkins"

        # The submission should be persisted
        persisted = await hub.persistence.load_group("general")
        submissions = [m for m in persisted if m.type == MessageType.FORM_SUBMISSION]
        assert len(submissions) == 1
        assert "goed" in submissions[0].content
