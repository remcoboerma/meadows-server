"""MeadowServer — the ASGI entrypoint composing Hub + AuthASGIApp.

``create_app()`` builds a Hub from environment configuration, wraps the
Socket.IO ASGI app in the auth middleware, and returns a callable ASGI app.
A module-level ``app`` is exposed for ``uvicorn meadows.server.app:app``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import socketio

from meadows.server.auth import AuthASGIApp, AuthError, _extract_bearer, _send_json, verify_token
from meadows.server.hub import Hub
from meadows.server.namespace import WebhookError


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


async def _read_body(receive: Any) -> bytes:
    """Read the full HTTP request body from the ASGI receive callable."""
    body_parts: list[bytes] = []
    while True:
        message = await receive()
        if message["type"] == "http.request":
            body_parts.append(message.get("body", b""))
            if not message.get("more_body", False):
                break
    return b"".join(body_parts)


class MeadowServer:
    """ASGI app composing Hub + AuthASGIApp."""

    def __init__(self, hub: Hub) -> None:
        self.hub = hub
        socketio_asgi = socketio.ASGIApp(hub.sio)
        self.asgi = AuthASGIApp(socketio_asgi, jwt_secret=hub.jwt_secret)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = scope.get("path", "")

        if scope.get("type") == "http" and path.startswith("/r/"):
            await self._handle_webhook(scope, receive, send)
            return

        await self.asgi(scope, receive, send)

    async def _handle_webhook(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """Handle POST /r/{group_id} — inject a message via HTTP transport.

        Reuses the same JWT verification and JSON response helpers as the
        AuthASGIApp middleware, and delegates to ChatNamespace.handle_webhook
        so the message goes through the same pipeline as Socket.IO messages.
        """
        if scope.get("method", "").upper() != "POST":
            await _send_json(send, 405, {"error": "method not allowed"})
            return

        group_id = scope.get("path", "/r/")[3:].strip("/").lower()

        token = _extract_bearer(scope)
        if token is None:
            await _send_json(send, 401, {"error": "missing bearer token"})
            return
        try:
            claims = verify_token(token, self.hub.jwt_secret)
        except AuthError:
            await _send_json(send, 401, {"error": "invalid token"})
            return

        try:
            body = await _read_body(receive)
            data = json.loads(body)
        except (json.JSONDecodeError, Exception):
            await _send_json(send, 400, {"error": "invalid JSON body"})
            return

        try:
            message_id = await self.hub.namespace.handle_webhook(group_id, claims, data)
        except WebhookError as exc:
            await _send_json(send, exc.status_code, {"error": exc.message})
            return

        await _send_json(send, 200, {"status": "ok", "message_id": message_id})


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
