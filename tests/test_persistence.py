"""Tests for JSONLPersistence — store/load round-trip, recency, missing groups."""

from __future__ import annotations

from pathlib import Path


from meadows.protocol import Message, MessageType

from meadows.server.persistence import JSONLPersistence


def _msg(content: str, *, group_id: str = "general", user_id: str = "user-alice") -> Message:
    return Message(type=MessageType.USER, user_id=user_id, group_id=group_id, content=content)


class TestStoreLoadRoundTrip:
    async def test_store_then_load(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        msg = _msg("hello")
        await store.store("general", msg)

        loaded = await store.load_group("general")
        assert len(loaded) == 1
        assert loaded[0].content == "hello"
        assert loaded[0].type == MessageType.USER
        assert loaded[0].user_id == "user-alice"
        assert loaded[0].group_id == "general"
        assert loaded[0].id == msg.id

    async def test_store_appends_to_existing_file(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        await store.store("general", _msg("first"))
        await store.store("general", _msg("second"))

        loaded = await store.load_group("general")
        assert [m.content for m in loaded] == ["first", "second"]

    async def test_separate_files_per_group(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        await store.store("general", _msg("g", group_id="general"))
        await store.store("random", _msg("r", group_id="random"))

        assert len(await store.load_group("general")) == 1
        assert len(await store.load_group("random")) == 1
        assert (messages_dir / "general.jsonl").exists()
        assert (messages_dir / "random.jsonl").exists()


class TestLoadRecency:
    async def test_load_returns_most_recent_n(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        for i in range(10):
            await store.store("general", _msg(f"msg-{i}"))

        loaded = await store.load_group("general", limit=3)
        assert [m.content for m in loaded] == ["msg-7", "msg-8", "msg-9"]

    async def test_load_limit_larger_than_count_returns_all(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        await store.store("general", _msg("only"))
        loaded = await store.load_group("general", limit=50)
        assert len(loaded) == 1


class TestMissingGroup:
    async def test_missing_group_returns_empty(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        assert await store.load_group("nope") == []

    async def test_missing_directory_returns_empty(self, tmp_path: Path):
        store = JSONLPersistence(tmp_path / "does-not-exist")
        assert await store.load_group("general") == []


class TestStoreCreatesDirectory:
    async def test_store_creates_missing_dir(self, tmp_path: Path):
        target = tmp_path / "nested" / "store"
        store = JSONLPersistence(target)
        assert not target.exists()
        await store.store("general", _msg("hi"))
        assert target.is_dir()
        assert (target / "general.jsonl").exists()


class TestBotMessagePersistence:
    async def test_bot_message_round_trips(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        msg = Message(
            type=MessageType.BOT,
            user_id="bot-echo",
            bot_name="echo",
            group_id="general",
            content="pong",
        )
        await store.store("general", msg)
        loaded = await store.load_group("general")
        assert loaded[0].type == MessageType.BOT
        assert loaded[0].bot_name == "echo"
        assert loaded[0].content == "pong"


class TestUnparseableLines:
    async def test_skips_corrupt_lines(self, messages_dir: Path):
        store = JSONLPersistence(messages_dir)
        await store.store("general", _msg("good"))
        # corrupt the file with a bad line
        with (messages_dir / "general.jsonl").open("a", encoding="utf-8") as fh:
            fh.write("{not valid json\n")
        await store.store("general", _msg("good-2"))

        loaded = await store.load_group("general")
        contents = [m.content for m in loaded]
        assert "good" in contents
        assert "good-2" in contents
        assert len(loaded) == 2
