"""ChatNamespace — the Socket.IO /chat namespace handler.

Auth is event-based: the client connects, then emits ``authenticate`` with a
JWT (see meadows-client). Until authenticated, a session is rejected.

Hard invariant (section 3.4): every client-bound frame — message traffic AND
control events (AUTHENTICATED, JOINED_GROUP, AUTH_ERROR, ...) — goes through
``hub.emit_frame``, the single chokepoint. The namespace never calls
``self.emit`` directly for client-bound traffic; it goes through the Hub so
``validate_frame`` runs first.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import socketio

from meadows.protocol import EventName, Message, MessageType
from meadows.protocol.codec import message_from_wire

from meadows.server.auth import AuthError, verify_token
from meadows.server.chokepoint import message_to_wire
from meadows.server.groups import GroupState

if TYPE_CHECKING:
    from meadows.server.hub import Hub

GENERAL_GROUP = "general"
TYPING_COOLDOWN_SECONDS = 1.0


class ChatNamespace(socketio.AsyncNamespace):
    """Socket.IO /chat namespace handler.

    The namespace holds a back-reference to the Hub; all mutable state lives
    on the Hub instance (user_sessions, bot_registry, groups), not in module
    globals.
    """

    def __init__(self, namespace: str, *, hub: Hub) -> None:
        super().__init__(namespace)
        self.hub = hub
        self._typing_last: dict[str, float] = {}

    # -- connect / disconnect ---------------------------------------------

    async def on_connect(self, sid: str, _environ: dict) -> None:
        self.hub.user_sessions[sid] = {"authenticated": False, "claims": None, "group_ids": set()}

    async def on_disconnect(self, sid: str) -> None:
        session = self.hub.user_sessions.pop(sid, None)
        if session:
            for group_id in list(session.get("group_ids", set())):
                await self._leave_group(sid, group_id)
        self._typing_last.pop(sid, None)

    # -- auth -------------------------------------------------------------

    async def on_authenticate(self, sid: str, data: dict) -> None:
        token = data.get("token") if isinstance(data, dict) else None
        if not token:
            await self.hub.emit_frame(EventName.AUTH_ERROR, {"error": "missing token"}, sid=sid)
            return
        try:
            claims = verify_token(token, self.hub.jwt_secret)
        except AuthError:
            await self.hub.emit_frame(EventName.AUTH_ERROR, {"error": "invalid token"}, sid=sid)
            return

        session = self.hub.user_sessions.setdefault(sid, {"authenticated": False, "claims": None, "group_ids": set()})
        session["authenticated"] = True
        session["claims"] = claims

        if claims.is_bot():
            self.hub.bot_registry[claims.bot_name or claims.sub] = {"sid": sid, "claims": claims}
            await self.hub.emit_frame(EventName.BOT_AUTHENTICATED, {"bot_name": claims.bot_name}, sid=sid)
        else:
            await self.hub.emit_frame(
                EventName.AUTHENTICATED,
                {"user_id": claims.sub, "username": claims.username},
                sid=sid,
            )

        await self._join_group(sid, GENERAL_GROUP)

    # -- chat -------------------------------------------------------------

    async def on_message(self, sid: str, data: dict) -> None:
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        msg = self._build_message(data, claims, MessageType.USER if claims.is_user() else MessageType.BOT)
        await self._dispatch_message(msg)

    async def on_bot_response(self, sid: str, data: dict) -> None:
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        if not claims.is_bot():
            await self.hub.emit_frame(EventName.ERROR, {"error": "only bots may emit bot_response"}, sid=sid)
            return
        msg = self._build_message(data, claims, MessageType.BOT)
        await self._dispatch_message(msg)

    async def on_typing(self, sid: str, data: dict) -> None:
        session = await self._require_auth(sid)
        if session is None:
            return
        now = time.monotonic()
        if now - self._typing_last.get(sid, 0.0) < TYPING_COOLDOWN_SECONDS:
            return
        self._typing_last[sid] = now
        group_id = data.get("group_id", GENERAL_GROUP) if isinstance(data, dict) else GENERAL_GROUP
        claims = session["claims"]
        await self.hub.emit_frame(
            EventName.USER_TYPING,
            {"group_id": group_id, "user_id": claims.sub, "username": claims.name()},
            room=group_id,
            skip_sid=sid,
        )

    # -- bot registration -------------------------------------------------

    async def on_register_bot(self, sid: str, data: dict) -> None:
        """Register bot metadata (description, commands, context_limit).

        BUSINESS RULE (MEADOWS §2 line 41): the bot's identity comes from
        the verified JWT claims, NOT from the payload. The bot cannot
        self-assert a different name. The monolith enforced this at
        sioserver.py:1751-1764 by disconnecting on mismatch; here we
        simply ignore any payload bot_name and use claims.bot_name.
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        if not claims.is_bot():
            await self.hub.emit_frame(EventName.ERROR, {"error": "only bots may register_bot"}, sid=sid)
            return
        bot_name = claims.bot_name or claims.sub
        description = data.get("description", "") if isinstance(data, dict) else ""
        commands = data.get("commands", []) if isinstance(data, dict) else []
        context_limit = data.get("context_limit", 30) if isinstance(data, dict) else 30
        self.hub.bot_registry[bot_name] = {
            "sid": sid,
            "claims": claims,
            "description": description,
            "commands": commands,
            "context_limit": context_limit,
        }
        await self.hub.emit_frame(EventName.BOT_REGISTERED, {"bot_name": bot_name}, sid=sid)

    # -- helpers ----------------------------------------------------------

    async def _require_auth(self, sid: str) -> dict | None:
        session = self.hub.user_sessions.get(sid)
        if session is None or not session.get("authenticated"):
            await self.hub.emit_frame(EventName.ERROR, {"error": "not authenticated"}, sid=sid)
            return None
        return session

    async def _join_group(self, sid: str, group_id: str) -> None:
        await self.enter_room(sid, group_id)
        state = self.hub.groups.setdefault(group_id, GroupState(group_id=group_id))
        state.members.add(sid)
        session = self.hub.user_sessions.get(sid)
        if session is not None:
            session.setdefault("group_ids", set()).add(group_id)
        await self.hub.emit_frame(EventName.JOINED_GROUP, {"group_id": group_id}, sid=sid)

    async def _leave_group(self, sid: str, group_id: str) -> None:
        await self.leave_room(sid, group_id)
        state = self.hub.groups.get(group_id)
        if state is not None:
            state.members.discard(sid)

    def _build_message(self, data: dict, claims: Any, msg_type: MessageType) -> Message:
        payload = dict(data) if isinstance(data, dict) else {}
        # Trust the server-verified identity, never the client's self-assertion.
        payload["type"] = msg_type
        payload["user_id"] = claims.sub
        if claims.is_user():
            payload["username"] = claims.username
        else:
            payload["bot_name"] = claims.bot_name
        return message_from_wire(payload)

    async def _dispatch_message(self, msg: Message) -> None:
        wire = message_to_wire(msg)
        await self.hub.emit_frame(EventName.MESSAGE, wire, room=msg.group_id)
        await self.hub.persistence.store(msg.group_id, msg)


__all__ = ["GENERAL_GROUP", "ChatNamespace"]
