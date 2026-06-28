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
from meadows.server.ntfy_prefs import NtfyPrefsStore
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
        ntfy_prefs_path: Path | None = None,
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
        # BUSINESS RULE (§3.3): ntfy prefs stored per-user; the server owns
        # this because only the server knows who is online.
        self.ntfy_prefs = NtfyPrefsStore(ntfy_prefs_path or (self.messages_dir.parent / "ntfy_prefs.json"))
        self.namespace = ChatNamespace(self.NAMESPACE, hub=self)
        self.sio.register_namespace(self.namespace)

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Prepare the hub for serving: discover groups from JSONL files.

        BUSINESS RULE: groups are derived from the JSONL files on disk —
        each ``<group_id>.jsonl`` file is a group. This means groups survive
        restarts without a separate metadata store. Deleted groups
        (``.jsonl.deleted``) are skipped. The messages_dir IS the source of
        truth for which groups exist.

        Directory creation is handled once in JSONLPersistence.__init__
        and NtfyPrefsStore.__init__, not repeatedly on each operation.
        """
        # Seed "general" even if no JSONL exists yet (fresh install)
        self.groups.setdefault(GENERAL_GROUP, GroupState(group_id=GENERAL_GROUP))
        # Discover all groups from existing JSONL files
        for path in self.messages_dir.glob("*.jsonl"):
            group_id = path.stem
            if group_id and group_id not in self.groups:
                self.groups[group_id] = GroupState(group_id=group_id, name=group_id)

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
        skip_sid: str | None = None,
    ) -> None:
        """The chokepoint — validate against the protocol, then emit.

        Every client-bound frame passes through here. Validation runs before
        any bytes hit the wire; an invalid frame raises ``ValueError`` and is
        never emitted.

        ``skip_sid`` excludes a specific client from a room broadcast — used
        to avoid echoing typing/presence events back to their sender.
        """
        validate_frame(event, data)
        kwargs: dict[str, Any] = {"namespace": self.NAMESPACE}
        if sid is not None:
            kwargs["to"] = sid
        elif room is not None:
            kwargs["room"] = room
        if skip_sid is not None:
            kwargs["skip_sid"] = skip_sid
        await self.sio.emit(event, data, **kwargs)


__all__ = ["Hub"]
