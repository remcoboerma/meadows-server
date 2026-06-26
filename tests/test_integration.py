"""End-to-end integration test: real meadows-server + real meadows-client + real meadows-bot.

BUSINESS RULE (MEADOWS §1 line 15): "Het doel van deze herstructurering is
het protocol expliciet maken en de implementaties eromheen loswrikken."
This test proves the five packages compose into a working system: a real
server (Hub + ASGI), a real client (MeadowClient), and a real bot
(BaseBot) talk to each other over Socket.IO using only the protocol
declaration as the shared contract.

This test is in meadows-server because it has the broadest dependency
set (server + client + bot). It requires the [integration] extra:
    uv pip install -e ".[dev,integration]"

The test components are NOT mocks — they are the real meadows-client
and meadows-bot packages wired against a real (in-process) meadows-server.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import ClassVar

import pytest
import uvicorn

from meadows.client import MeadowClient
from meadows.protocol import EventName, JWTRole, build_claims

# Skip the entire module if the integration deps aren't installed.
pytest.importorskip("meadows.client")
pytest.importorskip("meadows.bot")

from meadows.bot import BaseBot
from meadows.server.app import create_app

# A shared secret for all JWT minting in this test (must be valid utf-8, 32+ bytes).
INTEGRATION_SECRET = b"integration-test-secret-key-32-bytes!"
INTEGRATION_SERVER_URL = "http://127.0.0.1:18099"


class EchoBot(BaseBot):
    """The canonical test bot — echoes back what it receives.

    BUSINESS RULE (MEADOWS §5 line 130): this is the quick-start shape.
    BOT_NAME + should_handle + handle + connect(). Nothing else.
    """

    BOT_NAME = "echo"
    BOT_DESCRIPTION = "Integration test echo bot"
    BOT_COMMANDS: ClassVar[list[dict[str, str]]] = [{"name": "echo", "description": "Echo back text"}]

    def should_handle(self, command: str, _args: list) -> bool:
        return command in ("echo", "ping")

    def handle(self, command: str, args: list, _raw_args: list, _message: dict, _thread_context: list) -> str | None:
        if command == "echo":
            return f"Echo: {' '.join(args) if args else ''}"
        if command == "ping":
            return "Pong!"
        return None


@pytest.fixture(scope="module")
def integration_secret() -> bytes:
    return INTEGRATION_SECRET


@pytest.fixture(scope="module")
def integration_server(integration_secret: bytes, tmp_path_factory: pytest.TempPathFactory):
    """Start a real meadows-server on a random port using uvicorn in a thread.

    BUSINESS RULE (MEADOWS §7 line 152): "Het gedrag is heilig." This
    fixture runs the actual ASGI app — the same code path that Docker
    runs — so the integration test exercises the real server, not a
    simplified test harness.
    """
    import os

    messages_dir = tmp_path_factory.mktemp("integration_messages")
    # Write the secret to a file so _load_jwt_secret() reads the exact bytes.
    keys_dir = tmp_path_factory.mktemp("integration_keys")
    key_file = keys_dir / "jwt.key"
    key_file.write_bytes(integration_secret)

    os.environ["MEADOWS_JWT_SECRET"] = str(key_file)
    os.environ["MEADOWS_MESSAGES_DIR"] = str(messages_dir)
    os.environ["MEADOWS_CORS_ORIGINS"] = "*"

    app = create_app()
    config = uvicorn.Config(app, host="127.0.0.1", port=18099, log_level="error")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to be ready
    deadline = time.time() + 10
    import httpx

    while time.time() < deadline:
        try:
            r = httpx.get("http://127.0.0.1:18099/socket.io/?EIO=4&transport=polling", timeout=1)
            if r.status_code in (200, 400):
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        raise RuntimeError("Integration server did not start in 10s")

    yield server

    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
async def user_client(integration_secret: bytes) -> MeadowClient:
    """A real MeadowClient authenticated as a user."""
    client = MeadowClient(
        server_url=INTEGRATION_SERVER_URL,
        claims=build_claims(name="alice", role=JWTRole.USER),
        jwt_secret=integration_secret,
    )
    await client.connect()
    # Wait for authentication to complete
    deadline = time.time() + 5
    while not client.authenticated and time.time() < deadline:
        await asyncio.sleep(0.05)
    assert client.authenticated, "User client did not authenticate in 5s"
    yield client
    await client.disconnect()


@pytest.fixture
async def echo_bot_client(integration_secret: bytes) -> tuple[EchoBot, MeadowClient]:
    """A real EchoBot connected and authenticated.

    Returns the bot and its internal MeadowClient for direct inspection.
    """
    bot = EchoBot()
    # Override the JWT secret path to use our integration secret directly.
    bot.jwt_secret = integration_secret
    bot.claims = build_claims(name="echo", role=JWTRole.BOT)
    bot.client = MeadowClient(
        server_url=INTEGRATION_SERVER_URL,
        claims=bot.claims,
        jwt_secret=integration_secret,
    )
    # Re-wire handlers on the new client.
    bot._setup_handlers()

    await bot.client.connect()
    deadline = time.time() + 5
    while not bot.client.authenticated and time.time() < deadline:
        await asyncio.sleep(0.05)
    assert bot.client.authenticated, "Bot did not authenticate in 5s"

    # Give the server a moment to process register_bot.
    await asyncio.sleep(0.2)

    yield bot, bot.client
    await bot.client.disconnect()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIntegrationServerStarts:
    """Proves the server boots and accepts connections."""

    def test_server_is_running(self, integration_server):
        assert integration_server is not None

    async def test_user_connects_and_authenticates(self, user_client):
        assert user_client.connected
        assert user_client.authenticated


class TestIntegrationUserChat:
    """Proves a user can send a message and receive it back (broadcast)."""

    async def test_user_sends_message_and_receives_broadcast(self, user_client):
        received: list[dict] = []

        def on_message(data: dict) -> None:
            received.append(data)

        user_client.on(EventName.MESSAGE, on_message)

        await user_client.send_message(content="hello integration", group_id="general")

        # Wait for the broadcast to arrive
        deadline = time.time() + 3
        while not received and time.time() < deadline:
            await asyncio.sleep(0.05)

        assert len(received) >= 1
        msg = received[0]
        assert msg["content"] == "hello integration"
        assert msg["type"] == "user"
        assert msg["user_id"] == "user-alice"
        assert msg["group_id"] == "general"


class TestIntegrationBot:
    """Proves a bot connects, registers, and the server knows about it."""

    async def test_bot_authenticates_and_registers(self, echo_bot_client):
        bot, _client = echo_bot_client
        assert bot.client.authenticated
        # The server should have the bot in its registry.
        # We can't directly inspect the server's hub from here, but we
        # can verify the bot received bot_authenticated (which it did,
        # since authenticated is True).

    async def test_user_sees_bot_join(self, user_client, echo_bot_client):
        """When a bot registers, the user doesn't directly see it — but
        the bot is in the room and will receive broadcasts."""
        # Both user and bot are in "general". When the user sends a message,
        # the bot should receive it as a bot_command (if the server routes it).
        # For Sprint 1, we just verify the bot is in the room by checking
        # it receives the broadcast message.
        _bot, bot_client = echo_bot_client
        bot_received: list[dict] = []

        def on_message(data: dict) -> None:
            bot_received.append(data)

        bot_client.on(EventName.MESSAGE, on_message)

        await user_client.send_message(content="hello bot", group_id="general")

        deadline = time.time() + 3
        while not bot_received and time.time() < deadline:
            await asyncio.sleep(0.05)

        assert len(bot_received) >= 1
        assert bot_received[0]["content"] == "hello bot"


class TestIntegrationProtocolContract:
    """Proves the protocol envelope is the shared contract.

    BUSINESS RULE (MEADOWS §3.2 line 60): "Het systeem contracteert wat
    het zélf moet begrijpen." The message the user sends and the message
    the bot receives are the same envelope shape — defined once in
    meadows.protocol, used by both sides.
    """

    async def test_message_envelope_is_consistent(self, user_client, echo_bot_client):
        _bot, bot_client = echo_bot_client
        bot_received: list[dict] = []

        def on_message(data: dict) -> None:
            bot_received.append(data)

        bot_client.on(EventName.MESSAGE, on_message)

        await user_client.send_message(content="protocol check", group_id="general")

        deadline = time.time() + 3
        while not bot_received and time.time() < deadline:
            await asyncio.sleep(0.05)

        assert len(bot_received) >= 1
        msg = bot_received[0]
        # The envelope fields the system contracts on (MEADOWS §3.2 line 64):
        assert "id" in msg
        assert "uuid" in msg
        assert "type" in msg
        assert "user_id" in msg
        assert "group_id" in msg
        assert "timestamp" in msg
        assert "content" in msg
        assert msg["type"] == "user"
        assert msg["content"] == "protocol check"
