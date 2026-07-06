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

import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import jwt as pyjwt
import socketio

from meadows.protocol import EventName, Message, MessageType, build_claims, parse_everyone
from meadows.protocol.codec import message_from_wire
from meadows.protocol.jwt import ALGORITHM, JWTRole, JWTClaims
from meadows.protocol.permissions import AVAILABLE_PERMISSIONS

from meadows.server.auth import AuthError, verify_token
from meadows.server.chokepoint import message_to_wire
from meadows.server.groups import GroupState

if TYPE_CHECKING:
    from meadows.server.hub import Hub

GENERAL_GROUP = "general"
TYPING_COOLDOWN_SECONDS = 1.0
MAX_PATTERNS_PER_BOT = 50
MAX_PATTERN_LENGTH = 512
MAX_WEBHOOK_CONTENT = 100_000
_GROUP_ID_RE = re.compile(r"^[a-z0-9_-]{1,32}$")


class WebhookError(Exception):
    """Raised by handle_webhook so the ASGI layer can send the right HTTP status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.message = message


def _parse_expiry(expiry_str: str) -> float:
    """Parse an expiry string like '30d', '1w', '3h', '1y' into seconds.

    BUSINESS RULE: matches the monolith's tasks.py:297-310 _parse_expiry
    so CLI-generated and server-generated JWTs use the same format.
    """
    multipliers = {"d": 86400, "h": 3600, "w": 604800, "m": 2592000, "y": 31536000}
    if expiry_str and expiry_str[-1] in multipliers:
        try:
            return float(expiry_str[:-1]) * multipliers[expiry_str[-1]]
        except ValueError:
            pass
    try:
        return float(expiry_str)
    except ValueError:
        return 30 * 86400  # default 30 days


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
                await self._leave_group(sid, group_id, session=session)
        self._typing_last.pop(sid, None)

    # -- auth -------------------------------------------------------------

    async def on_authenticate(self, sid: str, data: dict) -> None:
        """Verify JWT, set session state, join general, send initial data.

        BUSINESS RULE (MEADOWS §3.2): the JWT claim structure is contracted
        in the protocol. The server enforces it here via verify_token(),
        which delegates to JWTClaims.model_validate. Identity (user_id,
        bot_name, role, permissions) comes from the verified JWT, never
        from the client's self-assertion.

        After auth, the client receives:
        - AUTHENTICATED/BOT_AUTHENTICATED (identity + permissions + groups + bots)
        - JOINED_GROUP (general group with display history)
        - GROUP_LIST (all available groups)
        - BOT_LIST (all registered bots for discovery)
        - MY_PERMISSIONS (RBAC surface)
        """
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

        # Add user/bot to general group members
        general = self.hub.groups.setdefault(GENERAL_GROUP, GroupState(group_id=GENERAL_GROUP))
        general.members[claims.sub] = {"username": claims.name()}

        if claims.is_bot():
            self.hub.bot_registry[claims.bot_name or claims.sub] = {"sid": sid, "claims": claims}
            await self.hub.emit_frame(
                EventName.BOT_AUTHENTICATED,
                {"bot_name": claims.bot_name, "groups": list(session.get("group_ids", set()))},
                sid=sid,
            )
        else:
            groups_list = [g.simplify() for g in self.hub.groups.values()]
            bots_list = [
                {"name": name, "description": info.get("description", ""), "commands": info.get("commands", [])}
                for name, info in self.hub.bot_registry.items()
            ]
            await self.hub.emit_frame(
                EventName.AUTHENTICATED,
                {
                    "user_id": claims.sub,
                    "username": claims.username,
                    "groups": groups_list,
                    "bots": bots_list,
                    "permissions": claims.permissions,
                    "available_permissions": AVAILABLE_PERMISSIONS,
                },
                sid=sid,
            )
            # Send permissions and group/bot lists
            await self.hub.emit_frame(
                EventName.MY_PERMISSIONS,
                {"permissions": claims.permissions, "available_permissions": AVAILABLE_PERMISSIONS},
                sid=sid,
            )
            await self.hub.emit_frame(
                EventName.GROUP_LIST,
                {"groups": groups_list},
                sid=sid,
            )
            await self.hub.emit_frame(
                EventName.BOT_LIST,
                {"bots": bots_list},
                sid=sid,
            )

        # Join general group (loads display history)
        await self._join_group(sid, GENERAL_GROUP)

    # -- chat -------------------------------------------------------------

    async def on_message(self, sid: str, data: dict) -> None:
        """Handle a user message: broadcast, persist, route to bots, evaluate patterns.

        BUSINESS RULE (MEADOWS §3.3): patterns and @bot routing are core
        server machinery, not bot features. The server evaluates registered
        regexes on every message and routes @bot mentions to the named bot.

        BUSINESS RULE (monolith sioserver.py:2282-2286): @everyone/@all sets
        is_everyone=True on the message BEFORE broadcast so clients can style
        it. The sender must have 'mention-all' permission — this is the only
        permission-gated notification type. Without the permission, the
        message is still sent but is_everyone stays False (no glow, no ntfy).
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        msg = self._build_message(data, claims, MessageType.USER if claims.is_user() else MessageType.BOT)
        if parse_everyone(msg.content) and "mention-all" in claims.permissions:
            msg.is_everyone = True
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

    # -- message removal --------------------------------------------------

    async def on_remove_message(self, sid: str, data: dict) -> None:
        """Mark a message as removed (strikethrough, not deletion).

        BUSINESS RULE (MEADOWS §3.3 line 73): message management is core —
        without it there is no interaction hygiene. The `removed` flag is
        the system's own state on a message, so it belongs in the server's
        persistence layer, not in a bot.

        BUSINESS RULE: messages are retained for audit; only the `removed`
        flag is set. The monolith did this at sioserver.py:1886-1959.
        Removed messages are excluded from bot thread context (a bot
        shouldn't see a deleted message as conversation history) but
        remain in the JSONL for audit/recovery.
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        message_id = data.get("message_id") if isinstance(data, dict) else None
        group_id = data.get("group_id") if isinstance(data, dict) else None
        if not message_id or not group_id:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing message_id or group_id"}, sid=sid)
            return
        found = await self.hub.persistence.mark_removed(group_id, message_id)
        if not found:
            await self.hub.emit_frame(EventName.ERROR, {"error": "message not found"}, sid=sid)
            return
        await self.hub.emit_frame(
            EventName.MESSAGE_REMOVED,
            {"message_id": message_id, "group_id": group_id},
            room=group_id,
        )

    # -- group lifecycle --------------------------------------------------

    async def on_create_group(self, sid: str, data: dict) -> None:
        """Create a new group and auto-join all connected bots.

        BUSINESS RULE (MEADOWS §3.3 line 73): group lifecycle is core —
        groups are the routing substrate for all interaction (messages,
        reactions, mentions, patterns all scope to a group). This is not
        a bot feature; it's the server's job to manage rooms.

        BUSINESS RULE (monolith sioserver.py:1137-1164): all connected bots
        MUST automatically join newly created groups. Users see bots as
        group members immediately upon creation. This is because bots are
        "volwaardige deelnemers" (full participants) in the MEADOWS model
        — a group without its bots is a broken group.

        SECURITY (CWE-22): group_id is validated against `^[a-z0-9_-]{1,32}$`
        to prevent path traversal via the JSONL filename. The monolith
        did this at sioserver.py:1096.
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        group_id = (data.get("group_id", "") if isinstance(data, dict) else "").strip().lower().replace(" ", "-")
        if not _GROUP_ID_RE.match(group_id):
            await self.hub.emit_frame(
                EventName.ERROR,
                {"error": "group name can only contain letters, numbers, hyphens, underscores (max 32 chars)"},
                sid=sid,
            )
            return
        if group_id in self.hub.groups:
            await self.hub.emit_frame(EventName.ERROR, {"error": "group already exists"}, sid=sid)
            return
        state = GroupState(
            group_id=group_id,
            name=data.get("name", group_id) if isinstance(data, dict) else group_id,
            description=data.get("description", "") if isinstance(data, dict) else "",
            created_by=claims.sub,
        )
        state.members[claims.sub] = {"username": claims.name()}
        self.hub.groups[group_id] = state
        await self.enter_room(sid, group_id)
        session.setdefault("group_ids", set()).add(group_id)

        # Auto-join all connected bots
        for bot_name, bot_info in self.hub.bot_registry.items():
            bot_sid = bot_info.get("sid")
            if bot_sid:
                await self.enter_room(bot_sid, group_id)
                bot_session = self.hub.user_sessions.get(bot_sid, {})
                bot_session.setdefault("group_ids", set()).add(group_id)
                state.members[f"bot-{bot_name}"] = {"username": f"bot-{bot_name}"}
                await self.hub.emit_frame(
                    EventName.JOINED_GROUP,
                    {"group_id": group_id, "members": state.safe_members(), "thread": []},
                    sid=bot_sid,
                )

        # Broadcast group creation to all clients
        groups_list = [g.simplify() for g in self.hub.groups.values()]
        await self.hub.emit_frame(EventName.GROUP_CREATED, state.simplify())
        await self.hub.emit_frame(EventName.GROUP_LIST, {"groups": groups_list})

    async def on_list_groups(self, sid: str, data: dict) -> None:
        """Send the full group list to the requesting client."""
        del data  # unused: no parameters for list_groups
        session = await self._require_auth(sid)
        if session is None:
            return
        groups_list = [g.simplify() for g in self.hub.groups.values()]
        await self.hub.emit_frame(EventName.GROUP_LIST, {"groups": groups_list}, sid=sid)

    async def on_join_group(self, sid: str, data: dict) -> None:
        """Join a group: enter room, load display history, notify others."""
        session = await self._require_auth(sid)
        if session is None:
            return
        group_id = data.get("group_id") if isinstance(data, dict) else None
        if not group_id or group_id not in self.hub.groups:
            await self.hub.emit_frame(EventName.ERROR, {"error": "group not found"}, sid=sid)
            return
        await self._join_group(sid, group_id)

    async def on_leave_group(self, sid: str, data: dict) -> None:
        """Leave a group: exit room, notify others."""
        session = await self._require_auth(sid)
        if session is None:
            return
        group_id = data.get("group_id") if isinstance(data, dict) else None
        if not group_id:
            return
        claims = session["claims"]
        await self._leave_group(sid, group_id)
        await self.hub.emit_frame(EventName.LEFT_GROUP, {"group_id": group_id}, sid=sid)
        state = self.hub.groups.get(group_id)
        if state:
            await self.hub.emit_frame(
                EventName.MEMBERS_UPDATED,
                {"group_id": group_id, "members": state.safe_members()},
                room=group_id,
            )
        await self.hub.emit_frame(
            EventName.USER_LEFT,
            {"user_id": claims.sub, "group_id": group_id},
            room=group_id,
            skip_sid=sid,
        )

    async def on_delete_group(self, sid: str, data: dict) -> None:
        """Delete a group: remove state, rename message file, notify members."""
        session = await self._require_auth(sid)
        if session is None:
            return
        group_id = data.get("group_id") if isinstance(data, dict) else None
        if not group_id or group_id not in self.hub.groups:
            await self.hub.emit_frame(EventName.ERROR, {"error": "group not found"}, sid=sid)
            return
        if group_id == GENERAL_GROUP:
            await self.hub.emit_frame(EventName.ERROR, {"error": "cannot delete general group"}, sid=sid)
            return
        # Rename message file to .deleted (audit trail)
        msg_path = self.hub.persistence._path(group_id)
        if msg_path.exists():
            deleted_path = msg_path.with_suffix(".jsonl.deleted")
            msg_path.rename(deleted_path)
        del self.hub.groups[group_id]
        await self.hub.emit_frame(EventName.GROUP_DELETED, {"group_id": group_id}, room=group_id)
        groups_list = [g.simplify() for g in self.hub.groups.values()]
        await self.hub.emit_frame(EventName.GROUP_LIST, {"groups": groups_list})

    # -- reactions --------------------------------------------------------

    async def on_add_reaction(self, sid: str, data: dict) -> None:
        """Toggle a reaction on a message (add or remove if already exists).

        BUSINESS RULE (MEADOWS §3.3 line 73): reactions are core — "zonder
        reacties, mentions en replies is er geen interactie; dat ís het
        systeem." A reaction is a `type='reaction'` message with `emoji` +
        `target_message_id`. The code is generic (no domain knowledge of
        what the emoji means). This generieke interactiemechaniek hoort in
        de kern van de server, niet in een bot.

        BUSINESS RULE: the server persists reactions as type=reaction
        messages in JSONL, so they survive reload. Toggle semantics:
        clicking the same emoji again removes it (sets removed=True).
        This matches the monolith at sioserver.py:2301-2378.

        BUSINESS RULE (MEADOWS §3.2): the reaction *berichtvorm*
        (reaction_added event, is_everyone flag) is contracted in
        meadows.protocol. The *meaning* of a 👍 vs 👎 is opaque to the
        system — only bots and humans interpret it.
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        emoji = data.get("emoji") if isinstance(data, dict) else None
        target_message_id = data.get("target_message_id") if isinstance(data, dict) else None
        group_id = data.get("group_id", GENERAL_GROUP) if isinstance(data, dict) else GENERAL_GROUP
        if not emoji or not target_message_id:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing emoji or target_message_id"}, sid=sid)
            return

        # Check if this user already reacted with this emoji on this message
        existing = await self._find_reaction(group_id, target_message_id, claims.sub, emoji)
        if existing:
            # Toggle: mark as removed
            await self.hub.persistence.mark_removed(group_id, existing["id"])
            reaction_msg = {
                **existing,
                "removed": True,
            }
            await self.hub.emit_frame(
                EventName.REACTION_TOGGLED,
                reaction_msg,
                room=group_id,
            )
        else:
            # Add new reaction
            msg = Message(
                type=MessageType.REACTION,
                user_id=claims.sub,
                username=claims.name(),
                group_id=group_id,
                emoji=emoji,
                target_message_id=target_message_id,
            )
            wire = message_to_wire(msg)
            await self.hub.emit_frame(EventName.REACTION_ADDED, wire, room=group_id)
            await self.hub.persistence.store(group_id, msg)

    async def on_remove_reaction(self, sid: str, data: dict) -> None:
        """Explicitly remove a reaction (distinct from toggle-off)."""
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        emoji = data.get("emoji") if isinstance(data, dict) else None
        target_message_id = data.get("target_message_id") if isinstance(data, dict) else None
        group_id = data.get("group_id", GENERAL_GROUP) if isinstance(data, dict) else GENERAL_GROUP
        if not emoji or not target_message_id:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing emoji or target_message_id"}, sid=sid)
            return
        existing = await self._find_reaction(group_id, target_message_id, claims.sub, emoji)
        if existing:
            await self.hub.persistence.mark_removed(group_id, existing["id"])
        await self.hub.emit_frame(
            EventName.REACTION_REMOVED,
            {"target_message_id": target_message_id, "emoji": emoji, "user_id": claims.sub, "group_id": group_id},
            room=group_id,
        )

    async def _find_reaction(self, group_id: str, target_message_id: str, user_id: str, emoji: str) -> dict | None:
        """Find an existing reaction by (target, user, emoji) in the JSONL."""
        history = await self.hub.persistence.load_display_history(group_id)
        for msg in reversed(history):
            if (
                msg.get("type") == MessageType.REACTION.value
                and msg.get("target_message_id") == target_message_id
                and msg.get("user_id") == user_id
                and msg.get("emoji") == emoji
                and not msg.get("removed", False)
            ):
                return msg
        return None

    # -- patterns ---------------------------------------------------------

    async def on_register_pattern(self, sid: str, data: dict) -> None:
        """Register a regex pattern for server-side evaluation.

        BUSINESS RULE (MEADOWS §3.3 line 74): patterns are core — "de server
        evalueert geregistreerde regex-patterns op elk bericht en stuurt
        pattern_matched naar de registrerende bot. Dat is generieke
        routing-machinerie, geen domein." The server holds no domain
        knowledge of any pattern's meaning — it's a generic regex matcher
        that routes matches to the bot that registered them.

        BUSINESS RULE (MEADOWS §3.2): the pattern_matched *event* and its
        envelope (pattern_name, matched_text, original_message_id, sender,
        group_id, timestamp) are contracted in meadows.protocol. The
        *meaning* of "urgent" or "critical" is opaque to the system —
        only the registering bot interprets it.

        BUSINESS RULE: patterns are per-bot, per-scope (room or global).
        The server enforces MAX_PATTERNS_PER_BOT (50) and MAX_PATTERN_LENGTH
        (512) to prevent resource exhaustion. Patterns are re-registered
        by the bot on reconnect (the bot SDK replays them after auth —
        see meadows-bot base.py on_bot_authenticated).
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        if not claims.is_bot():
            await self.hub.emit_frame(EventName.ERROR, {"error": "only bots may register patterns"}, sid=sid)
            return
        name = data.get("name", "") if isinstance(data, dict) else ""
        pattern_str = data.get("pattern", "") if isinstance(data, dict) else ""
        scope = data.get("scope", "room") if isinstance(data, dict) else "room"
        group_id = data.get("group_id") if isinstance(data, dict) else None
        if not name or not pattern_str:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing name or pattern"}, sid=sid)
            return
        if scope not in ("room", "global"):
            await self.hub.emit_frame(EventName.ERROR, {"error": "scope must be 'room' or 'global'"}, sid=sid)
            return
        if len(pattern_str) > MAX_PATTERN_LENGTH:
            await self.hub.emit_frame(EventName.ERROR, {"error": "pattern too long"}, sid=sid)
            return
        storage_key = "*" if scope == "global" else (group_id or "*")
        entries = self.hub.pattern_registry.setdefault(storage_key, [])
        if len(entries) >= MAX_PATTERNS_PER_BOT:
            await self.hub.emit_frame(EventName.ERROR, {"error": "pattern limit reached"}, sid=sid)
            return
        try:
            compiled = re.compile(pattern_str, re.IGNORECASE)
        except re.error as exc:
            await self.hub.emit_frame(EventName.ERROR, {"error": f"invalid regex: {exc}"}, sid=sid)
            return
        entries.append(
            {
                "name": name,
                "pattern": pattern_str,
                "compiled": compiled,
                "bot_id": claims.bot_name or claims.sub,
                "bot_sid": sid,
                "scope": scope,
                "group_id": storage_key,
                "registered_at": time.time(),
            }
        )
        await self.hub.emit_frame(EventName.PATTERN_REGISTERED, {"name": name}, sid=sid)

    async def on_unregister_pattern(self, sid: str, data: dict) -> None:
        """Remove a previously registered pattern by name."""
        session = await self._require_auth(sid)
        if session is None:
            return
        name = data.get("name", "") if isinstance(data, dict) else ""
        for storage_key, entries in self.hub.pattern_registry.items():
            self.hub.pattern_registry[storage_key] = [e for e in entries if e["name"] != name]
        await self.hub.emit_frame(EventName.PATTERN_UNREGISTERED, {"name": name}, sid=sid)

    async def _evaluate_patterns(self, group_id: str, msg: Message) -> None:
        """Evaluate all registered patterns against a message.

        BUSINESS RULE: the server runs re.search on each message's content
        against every registered pattern. On match, emit PATTERN_MATCHED
        to the registering bot. This is generic routing machinery, not
        domain logic (§3.3 line 74).
        """
        content = msg.content or ""
        # Check group-scoped patterns
        for storage_key in (group_id, "*"):
            entries = self.hub.pattern_registry.get(storage_key, [])
            for entry in entries:
                match = entry["compiled"].search(content)
                if match:
                    await self.hub.emit_frame(
                        EventName.PATTERN_MATCHED,
                        {
                            "pattern_name": entry["name"],
                            "matched_text": match.group(0),
                            "original_message_id": msg.id,
                            "sender": msg.user_id or msg.bot_name or "unknown",
                            "group_id": group_id,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                        sid=entry["bot_sid"],
                    )

    # -- JWT invite -------------------------------------------------------

    async def on_request_user_jwt(self, sid: str, data: dict) -> None:
        """Mint a JWT for a new user (requires 'user-invite' permission).

        BUSINESS RULE (MEADOWS §3.2 line 64): the JWT-claimstructuur is
        contracted in the protocol — `sub` prefix `user-`/`bot-`, `role`,
        `bot_name`, `permissions`, `exp`. The server enforces this via
        build_claims() which goes through JWTClaims validators.

        BUSINESS RULE (MEADOWS §3.2): permissions are the RBAC surface.
        'user-invite' is contracted in meadows.protocol.permissions.
        A user without this permission cannot mint tokens — the server
        is the sole minting authority, never the client.

        BUSINESS RULE (monolith tasks.py:356-361): "Permissions are
        intentionally NOT exposed in the public API of this function.
        Only the CLI/task layer should construct permissions, preventing
        clients from granting themselves arbitrary permissions via API
        calls." The server enforces this: the requesting user's own
        permissions are checked, and the new token's permissions are
        validated against AVAILABLE_PERMISSIONS.
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        if "user-invite" not in claims.permissions:
            await self.hub.emit_frame(EventName.ERROR, {"error": "permission denied: user-invite required"}, sid=sid)
            return
        username = data.get("username", "") if isinstance(data, dict) else ""
        if not username:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing username"}, sid=sid)
            return
        perms = data.get("permissions", []) if isinstance(data, dict) else []
        expiry_str = data.get("expiry", "30d") if isinstance(data, dict) else "30d"
        expires_in_seconds = _parse_expiry(expiry_str)
        new_claims = build_claims(
            name=username, role=JWTRole.USER, permissions=perms, expires_in_seconds=expires_in_seconds
        )
        token = pyjwt.encode(new_claims.model_dump(exclude_none=True), self.hub.jwt_secret, algorithm=ALGORITHM)
        await self.hub.emit_frame(
            EventName.USER_JWT_GENERATED,
            {"token": token, "username": username},
            sid=sid,
        )

    async def on_request_bot_jwt(self, sid: str, data: dict) -> None:
        """Mint a JWT for a new bot (requires 'bot-invite' permission).

        BUSINESS RULE: same as on_request_user_jwt but for bots. The
        'bot-invite' permission is contracted in meadows.protocol.
        Bot tokens use `sub` prefix `bot-` and carry `bot_name` — the
        server enforces this via build_claims() (§3.2 line 64).
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        if "bot-invite" not in claims.permissions:
            await self.hub.emit_frame(EventName.ERROR, {"error": "permission denied: bot-invite required"}, sid=sid)
            return
        bot_name = data.get("bot_name", "") if isinstance(data, dict) else ""
        if not bot_name:
            await self.hub.emit_frame(EventName.ERROR, {"error": "missing bot_name"}, sid=sid)
            return
        perms = data.get("permissions", []) if isinstance(data, dict) else []
        expiry_str = data.get("expiry", "1y") if isinstance(data, dict) else "1y"
        expires_in_seconds = _parse_expiry(expiry_str)
        new_claims = build_claims(
            name=bot_name, role=JWTRole.BOT, permissions=perms, expires_in_seconds=expires_in_seconds
        )
        token = pyjwt.encode(new_claims.model_dump(exclude_none=True), self.hub.jwt_secret, algorithm=ALGORITHM)
        await self.hub.emit_frame(
            EventName.BOT_JWT_GENERATED,
            {"token": token, "bot_name": bot_name},
            sid=sid,
        )

    # -- ntfy prefs -------------------------------------------------------

    async def on_get_ntfy_prefs(self, sid: str, data: dict) -> None:
        """Return the user's ntfy notification preferences.

        BUSINESS RULE (MEADOWS §3.3 line 75): ntfy sits in the server because
        "alleen de server weet wie online is." The server pushes ntfy
        notifications to offline users who were mentioned/replied-to/
        @everyone'd. The prefs (server URL, topic, auth token, enabled flag)
        are stored per-user on the server — the server is the only party
        that both knows presence AND can push.

        BUSINESS RULE (MEADOWS §3.3 line 75-76): "Dit kán naar een bot
        verschuiven, maar alléén als een bot de presence-informatie krijgt
        die nu alleen de server heeft." That's the observe-hook path,
        postponed to a later iteration. For now, ntfy stays core.
        """
        del data  # unused: prefs are keyed by session identity
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        prefs = self.hub.ntfy_prefs.get(claims.sub)
        await self.hub.emit_frame(EventName.NTFY_PREFS, prefs, sid=sid)

    async def on_save_ntfy_prefs(self, sid: str, data: dict) -> None:
        """Save the user's ntfy notification preferences.

        BUSINESS RULE: same as on_get_ntfy_prefs — the server owns ntfy
        prefs because the server is the party that pushes notifications.
        Prefs are stored keyed by the JWT `sub` claim (e.g. `user-alice`),
        never by sid (which changes on reconnect).
        """
        session = await self._require_auth(sid)
        if session is None:
            return
        claims = session["claims"]
        prefs = data if isinstance(data, dict) else {}
        self.hub.ntfy_prefs.set(claims.sub, prefs)
        await self.hub.emit_frame(EventName.NTFY_PREFS_SAVED, {"success": True}, sid=sid)

    # -- bot discovery & fetch --------------------------------------------

    async def on_bot_list_bots(self, sid: str, data: dict) -> None:
        """Send the list of registered bots to the requesting client."""
        del data  # unused: no parameters
        session = await self._require_auth(sid)
        if session is None:
            return
        bots_list = [
            {
                "name": name,
                "description": info.get("description", ""),
                "commands": info.get("commands", []),
                "context_limit": info.get("context_limit", 30),
            }
            for name, info in self.hub.bot_registry.items()
        ]
        await self.hub.emit_frame(EventName.BOT_LIST, {"bots": bots_list}, sid=sid)

    async def on_fetch_messages(self, sid: str, data: dict) -> None:
        """Fetch specific messages by ID from a group's history."""
        session = await self._require_auth(sid)
        if session is None:
            return
        message_ids = data.get("message_ids", []) if isinstance(data, dict) else []
        group_id = data.get("group_id", GENERAL_GROUP) if isinstance(data, dict) else GENERAL_GROUP
        request_id = data.get("request_id", "") if isinstance(data, dict) else ""
        messages = await self.hub.persistence.load_by_ids(group_id, message_ids)
        await self.hub.emit_frame(
            EventName.FETCH_MESSAGES_RESULT,
            {"request_id": request_id, "messages": messages},
            sid=sid,
        )

    # -- link tracking (optional, no-op) ----------------------------------

    async def on_link_click(self, sid: str, data: dict) -> None:
        """No-op for the PoC. Link tracking is optional and disabled by default.

        BUSINESS RULE (MEADOWS §3.3): link-tracking stays opaque to the core.
        The monolith had it as a server feature; in MEADOWS it's a future
        bot/feature, not core. We accept the event so the UI doesn't error.
        """
        del sid, data  # unused: no-op

    # -- bot registration -------------------------------------------------

    async def on_register_bot(self, sid: str, data: dict) -> None:
        """Register bot metadata (description, commands, context_limit).

        BUSINESS RULE (MEADOWS §2 line 41): the bot's identity comes from
        the verified JWT claims, NOT from the payload.
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
        """Join a group: enter room, add to members, send history + notify room.

        BUSINESS RULE (monolith sioserver.py:1290-1292): the joiner receives
        ALL display history (not limited to thread context). "Load ALL
        messages for display (not limited to DEFAULT_THREAD_SIZE). Thread
        context limit only applies to bots, not chat display." This is
        because the UI needs to show the full conversation, while bots
        only need the last N messages for LLM context.

        BUSINESS RULE: the room receives MEMBERS_UPDATED and USER_JOINED
        so other clients know someone joined. USER_JOINED uses skip_sid
        — the joiner already knows they joined; only others need the
        notification. This is the presence model: join/leave events are
        the implicit presence surface (there is no explicit 'presence'
        event — MEADOWS §3.2 collapses presence into join/leave).

        BUSINESS RULE: members are keyed by user_id (from JWT `sub`),
        not by Socket.IO sid. The sid changes on reconnect; the user_id
        is stable. This matches the monolith's member dict at
        sioserver.py:1128-1135.
        """
        await self.enter_room(sid, group_id)
        state = self.hub.groups.setdefault(group_id, GroupState(group_id=group_id))
        session = self.hub.user_sessions.get(sid)
        if session is not None:
            claims = session.get("claims")
            if claims:
                state.members[claims.sub] = {"username": claims.name()}
            session.setdefault("group_ids", set()).add(group_id)

        # Load display history (ALL messages, not limited)
        thread = await self.hub.persistence.load_display_history(group_id)
        await self.hub.emit_frame(
            EventName.JOINED_GROUP,
            {"group_id": group_id, "members": state.safe_members(), "thread": thread},
            sid=sid,
        )
        # Notify the room about the new member
        await self.hub.emit_frame(
            EventName.MEMBERS_UPDATED,
            {"group_id": group_id, "members": state.safe_members()},
            room=group_id,
        )
        if session is not None and session.get("claims"):
            claims = session["claims"]
            await self.hub.emit_frame(
                EventName.USER_JOINED,
                {"user_id": claims.sub, "group_id": group_id},
                room=group_id,
                skip_sid=sid,
            )

    async def _leave_group(self, sid: str, group_id: str, *, session: dict | None = None) -> None:
        """Leave a group: exit room, remove from members.

        Accepts an optional ``session`` param so on_disconnect can pass
        the session before it's popped from user_sessions.
        """
        await self.leave_room(sid, group_id)
        state = self.hub.groups.get(group_id)
        if state is not None:
            sess = session or self.hub.user_sessions.get(sid)
            if sess and sess.get("claims"):
                state.members.pop(sess["claims"].sub, None)
        sess = session or self.hub.user_sessions.get(sid)
        if sess is not None:
            sess.get("group_ids", set()).discard(group_id)

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
        """Broadcast a message, persist it, route @bot mentions, evaluate patterns.

        BUSINESS RULE (MEADOWS §3.3 line 73-74): reactions, mentions, replies,
        @everyone, and patterns are core server machinery — they live in
        meadows-server, not in bots. The server is the routing hub: it
        evaluates registered regexes on every message (PATTERN_MATCHED) and
        parses @botname from content to route as BOT_COMMAND.

        BUSINESS RULE (monolith sioserver.py:1513): the sender DOES receive
        their own message back (no skip_sid). The client deduplicates by
        message ID. This is standard chat behavior — the optimistic-UI
        add on send is confirmed by the server round-trip. This is different
        from typing/presence events (user_typing, user_joined, user_left)
        where skip_sid excludes the sender — those are "something happened
        to someone else" notifications, not "your action was confirmed."
        """
        wire = message_to_wire(msg)
        await self.hub.emit_frame(EventName.MESSAGE, wire, room=msg.group_id)
        await self.hub.persistence.store(msg.group_id, msg)

        # Route @bot mentions
        await self._route_bot_commands(msg)

        # Evaluate registered patterns
        await self._evaluate_patterns(msg.group_id, msg)

    async def handle_webhook(self, group_id: str, claims: JWTClaims, data: dict) -> str:
        """Handle an inbound webhook message over HTTP transport.

        This is the HTTP equivalent of ``on_message`` — it builds a
        ``MessageType.WEBHOOK`` message and feeds it into the same
        ``_dispatch_message`` pipeline (broadcast, persist, @bot routing,
        pattern evaluation). The only difference is the transport: HTTP
        POST instead of a Socket.IO event.

        BUSINESS RULE: any valid JWT (user or bot) may use the webhook.
        Transport is not restricted — the webhook is an alternative
        delivery mechanism, not a privileged one.

        BUSINESS RULE: @everyone is supported if the JWT carries the
        ``mention-all`` permission, matching ``on_message`` behaviour.

        Returns the message id on success. Raises ``WebhookError`` on
        validation failure so the ASGI layer can send the right HTTP status.
        """
        if group_id not in self.hub.groups:
            raise WebhookError(404, "group not found")

        content = (data.get("content") if isinstance(data, dict) else "") or ""
        content = content.strip()
        if not content:
            raise WebhookError(400, "content is required")
        if len(content) > MAX_WEBHOOK_CONTENT:
            raise WebhookError(400, "content too large")

        msg = Message(
            type=MessageType.WEBHOOK,
            user_id=claims.sub,
            username=claims.username if claims.is_user() else None,
            bot_name=claims.bot_name if claims.is_bot() else None,
            group_id=group_id,
            content=content,
        )
        if parse_everyone(msg.content) and "mention-all" in claims.permissions:
            msg.is_everyone = True

        await self._dispatch_message(msg)
        return msg.id

    async def _route_bot_commands(self, msg: Message) -> None:
        """Parse @botname from message content and route as BOT_COMMAND.

        BUSINESS RULE (MEADOWS §3.3 line 73): @bot routing is core —
        the server parses `@botname command args` from message content
        and emits BOT_COMMAND to the named bot. This is generieke
        routing-machinerie: the server doesn't know what the bot does
        with the command, it just routes it. The bot's should_handle/
        handle decides whether and how to respond.

        BUSINESS RULE (MEADOWS §5 line 130): the bot author never writes
        this routing. The server constructs the BOT_COMMAND payload with
        command, args, raw_args, the original message, and thread_context
        (last N messages where N = the bot's registered context_limit,
        default 30). The bot SDK's on_bot_command receives it and calls
        the author's should_handle/handle.

        BUSINESS RULE (monolith bots/base.py:42): BOT_CONTEXT_LIMIT=30
        is the thread context window. Bots see the last 30 messages as
        conversation context — enough for an LLM to understand the flow,
        not so much that token costs explode. The bot can override this
        via its BOT_CONTEXT_LIMIT class attribute.
        """
        content = msg.content or ""
        match = re.match(r"@(\w+)\s+(.*)", content)
        if not match:
            return
        bot_name = match.group(1)
        if bot_name not in self.hub.bot_registry:
            return
        bot_info = self.hub.bot_registry[bot_name]
        bot_sid = bot_info.get("sid")
        if not bot_sid:
            return
        rest = match.group(2).strip()
        parts = rest.split(None, 1)
        command = parts[0] if parts else ""
        args_str = parts[1] if len(parts) > 1 else ""
        args = args_str.split() if args_str else []
        context_limit = bot_info.get("context_limit", 30)
        thread_context = await self.hub.persistence.load_thread_context(msg.group_id, context_limit)
        await self.hub.emit_frame(
            EventName.BOT_COMMAND,
            {
                "command": command,
                "args": args,
                "raw_args": [args_str] if args_str else [],
                "message": message_to_wire(msg),
                "thread_context": thread_context,
            },
            sid=bot_sid,
        )


__all__ = ["GENERAL_GROUP", "ChatNamespace", "WebhookError"]
