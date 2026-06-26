"""JWT verification — the auth declaration enforced.

Tokens are decoded with pyjwt (signature + exp checked) and then the claim
structure is enforced via ``meadows.protocol.jwt.JWTClaims``. This is the
single verify path: the ChatNamespace ``on_authenticate`` handler and the
AuthASGIApp HTTP middleware both go through ``verify_token``.

The monolith had three divergent mint shapes (sioserver.py:268, :3060,
bots/base.py:121); JWTClaims is the consolidation, and verify_token is where
it is enforced on the server side.
"""

from __future__ import annotations

from typing import Any, Iterable

import jwt as pyjwt
from meadows.protocol.jwt import ALGORITHM, JWTClaims


class AuthError(Exception):
    """Raised when a JWT cannot be verified or its claims are invalid."""


def verify_token(token: str, secret: bytes) -> JWTClaims:
    """Verify a JWT and return the validated claims.

    pyjwt checks signature and expiry; JWTClaims.model_validate enforces the
    MEADOWS claim structure (sub prefix, role, bot_name-for-bots, ...).
    """
    try:
        payload = pyjwt.decode(token, secret, algorithms=[ALGORITHM])
    except pyjwt.PyJWTError as exc:
        raise AuthError(f"invalid token: {exc}") from exc
    try:
        return JWTClaims.model_validate(payload)
    except Exception as exc:
        raise AuthError(f"invalid claims: {exc}") from exc


def _extract_bearer(scope: dict[str, Any]) -> str | None:
    for key, value in scope.get("headers", []):
        if key == b"authorization":
            text = value.decode("latin-1")
            scheme, _, rest = text.partition(" ")
            if scheme.lower() == "bearer" and rest.strip():
                return rest.strip()
    return None


async def _send_json(send: Any, status: int, body: dict[str, Any]) -> None:
    import json

    payload = json.dumps(body).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": payload})


class AuthASGIApp:
    """ASGI middleware that gates HTTP routes behind a verified JWT.

    MEADOWS auth is event-based: a client connects to the /chat Socket.IO
    namespace and emits ``authenticate`` with a JWT, which ChatNamespace
    verifies via :func:`verify_token`. This middleware is the HTTP-level
    companion — any non-Engine.IO route whose path starts with one of
    ``protected_prefixes`` must carry a valid ``Authorization: Bearer <jwt>``.

    Socket.IO Engine.IO traffic (``/socket.io``) always passes through; its
    auth happens in the namespace handler.
    """

    def __init__(
        self,
        app: Any,
        *,
        jwt_secret: bytes,
        protected_prefixes: Iterable[str] = ("/chat",),
    ) -> None:
        self.app = app
        self.jwt_secret = jwt_secret
        self.protected_prefixes = tuple(protected_prefixes)

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path.startswith("/socket.io"):
            await self.app(scope, receive, send)
            return

        if not any(path.startswith(prefix) for prefix in self.protected_prefixes):
            await self.app(scope, receive, send)
            return

        token = _extract_bearer(scope)
        if token is None:
            await _send_json(send, 401, {"error": "missing bearer token"})
            return
        try:
            verify_token(token, self.jwt_secret)
        except AuthError:
            await _send_json(send, 401, {"error": "invalid token"})
            return

        await self.app(scope, receive, send)


__all__ = ["AuthASGIApp", "AuthError", "verify_token"]
