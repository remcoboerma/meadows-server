"""Hub — the MEADOWS coordination hub, as an object.

This is the server-as-object (MEADOWS-migration-intent.md section 3.4). All
mutable state lives on the instance — ``sio``, ``user_sessions``,
``bot_registry``, ``groups``, ``pattern_registry`` — never in module globals.
Someone can instantiate ``Hub()``, wrap it, run it in another process.

The monolith (sioserver.py) kept these as module-level globals; that is the
anti-pattern this class replaces.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import socketio

from meadows.protocol import EventName

from meadows.server.chokepoint import validate_frame
from meadows.server.groups import GroupState
from meadows.server.namespace import GENERAL_GROUP, ChatNamespace
from meadows.server.persistence import JSONLPersistence


class Hub:
    """MEADOWS coordination hub — the server-as-object.

    State lives on the instance, not in module globals. Someone can
    instantiate this, wrap it, run it in another process.
    """

    NAMESPACE = "/chat"

    def __init__(
        self,
        *,
        jwt_secret: bytes,
        messages_dir: Path,
        cors_origins: str = "*",
    ) -> None:
        self.jwt_secret = jwt_secret
        self.messages_dir = Path(messages_dir)
        self.cors_origins = cors_origins

        self.sio: socketio.AsyncServer = socketio.AsyncServer(
            async_mode="asgi",
            cors_allowed_origins=cors_origins,
            logger=False,
        )
        self.user_sessions: dict[str, dict[str, Any]] = {}
        self.bot_registry: dict[str, dict[str, Any]] = {}
        self.groups: dict[str, GroupState] = {}
        self.pattern_registry: dict[str, list[dict[str, Any]]] = {}

        self.persistence = JSONLPersistence(self.messages_dir)
        self.namespace = ChatNamespace(self.NAMESPACE, hub=self)
        self.sio.register_namespace(self.namespace)

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Prepare the hub for serving: ensure the message store and seed groups."""
        self.messages_dir.mkdir(parents=True, exist_ok=True)
        self.groups.setdefault(GENERAL_GROUP, GroupState(group_id=GENERAL_GROUP))

    async def stop(self) -> None:
        """Tear down hub state.

        The ASGI/transport lifecycle is owned by uvicorn; this only clears
        hub-level bookkeeping so a stopped Hub can be discarded cleanly.
        """

    # -- the client edge (chokepoint) -------------------------------------

    async def emit_frame(
        self,
        event: EventName | str,
        data: dict[str, Any],
        *,
        room: str | None = None,
        sid: str | None = None,
    ) -> None:
        """The chokepoint — validate against the protocol, then emit.

        Every client-bound frame passes through here. Validation runs before
        any bytes hit the wire; an invalid frame raises ``ValueError`` and is
        never emitted.
        """
        validate_frame(event, data)
        kwargs: dict[str, Any] = {"namespace": self.NAMESPACE}
        if sid is not None:
            kwargs["to"] = sid
        elif room is not None:
            kwargs["room"] = room
        await self.sio.emit(event, data, **kwargs)


__all__ = ["Hub"]
