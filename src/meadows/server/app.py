"""MeadowServer — the ASGI entrypoint composing Hub + AuthASGIApp.

``create_app()`` builds a Hub from environment configuration, wraps the
Socket.IO ASGI app in the auth middleware, and returns a callable ASGI app.
A module-level ``app`` is exposed for ``uvicorn meadows.server.app:app``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import socketio

from meadows.server.auth import AuthASGIApp
from meadows.server.hub import Hub


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _load_jwt_secret() -> bytes:
    """Load the JWT secret from a file path, or fall back to the literal bytes.

    If ``MEADOWS_JWT_SECRET`` points at an existing file, its bytes are used
    (the shared-keys convention). Otherwise the value is treated as a literal
    secret string — a dev convenience so a single env var suffices.
    """
    value = _env("MEADOWS_JWT_SECRET", "./shared_keys/jwt.key")
    path = Path(value)
    if path.is_file():
        return path.read_bytes()
    return value.encode("utf-8")


class MeadowServer:
    """ASGI app composing Hub + AuthASGIApp."""

    def __init__(self, hub: Hub) -> None:
        self.hub = hub
        socketio_asgi = socketio.ASGIApp(hub.sio)
        self.asgi = AuthASGIApp(socketio_asgi, jwt_secret=hub.jwt_secret)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        await self.asgi(scope, receive, send)


def create_app() -> MeadowServer:
    """Build the ASGI app from environment configuration.

    The Hub is started eagerly (hub.start()) so group discovery from JSONL
    files happens before uvicorn accepts connections. This avoids the
    complexity of ASGI lifespan event interception (which conflicts with
    socketio.ASGIApp's own lifespan handling).
    """
    import asyncio

    messages_dir = Path(_env("MEADOWS_MESSAGES_DIR", "./messages"))
    hub = Hub(
        jwt_secret=_load_jwt_secret(),
        messages_dir=messages_dir,
        cors_origins=_env("MEADOWS_CORS_ORIGINS", "*"),
        ntfy_prefs_path=messages_dir.parent / "ntfy_prefs.json",
    )
    # Run hub.start() synchronously before serving — discovers groups from disk.
    asyncio.run(hub.start())
    return MeadowServer(hub)


app = create_app()


__all__ = ["MeadowServer", "app", "create_app"]
