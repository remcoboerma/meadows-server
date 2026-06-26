"""Shared fixtures and helpers for meadows-server tests.

Tests never bind a real socket. The FakeSIO wires instance attributes onto a
real Hub's AsyncServer so the ChatNamespace (which calls self.server.emit /
enter_room / leave_room) hits the recorder instead of a live transport.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import jwt as pyjwt
import pytest

from meadows.protocol import JWTRole, build_claims
from meadows.protocol.jwt import ALGORITHM

from meadows.server.hub import Hub

TEST_SECRET = b"test-secret-key-that-is-long-enough-32b!"
OTHER_SECRET = b"a-completely-different-secret-32-bytes!"


class FakeSIO:
    """Records emits / room ops in place of a live AsyncServer."""

    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []
        self.rooms_entered: list[tuple[str, str, str | None]] = []
        self.rooms_left: list[tuple[str, str, str | None]] = []

    async def emit(
        self,
        event: Any,
        data: Any = None,
        to: str | None = None,
        room: str | None = None,
        namespace: str | None = None,
        **_kw: Any,
    ) -> None:
        self.emits.append({"event": event, "data": data, "room": room, "to": to, "namespace": namespace})

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.rooms_entered.append((sid, room, namespace))

    async def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.rooms_left.append((sid, room, namespace))

    def events(self, name: str) -> list[dict[str, Any]]:
        return [e for e in self.emits if e["event"] == name]


@pytest.fixture
def jwt_secret() -> bytes:
    return TEST_SECRET


@pytest.fixture
def messages_dir(tmp_path: Path) -> Path:
    d = tmp_path / "messages"
    d.mkdir()
    return d


@pytest.fixture
def hub(jwt_secret: bytes, messages_dir: Path) -> Hub:
    return Hub(jwt_secret=jwt_secret, messages_dir=messages_dir)


@pytest.fixture
def fake_sio(hub: Hub) -> FakeSIO:
    """Wire a FakeSIO onto the hub's AsyncServer (instance attrs shadow)."""
    fake = FakeSIO()
    hub.sio.emit = fake.emit  # type: ignore[method-assign]
    hub.sio.enter_room = fake.enter_room  # type: ignore[method-assign]
    hub.sio.leave_room = fake.leave_room  # type: ignore[method-assign]
    return fake


@pytest.fixture
def mint(jwt_secret: bytes):
    def _mint(claims: Any, *, secret: bytes | None = None) -> str:
        return pyjwt.encode(
            claims.model_dump(exclude_none=True),
            secret if secret is not None else jwt_secret,
            algorithm=ALGORITHM,
        )

    return _mint


@pytest.fixture
def user_token(mint):
    def _make(name: str = "alice", **kw: Any) -> str:
        return mint(build_claims(name=name, role=JWTRole.USER, **kw))

    return _make


@pytest.fixture
def bot_token(mint):
    def _make(name: str = "echo", **kw: Any) -> str:
        return mint(build_claims(name=name, role=JWTRole.BOT, **kw))

    return _make
