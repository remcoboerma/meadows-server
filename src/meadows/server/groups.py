"""Group state — in-memory membership and metadata.

Message history lives in JSONLPersistence (on disk), not in memory. The
server is the source of truth for routing and presence, not for replay;
fetch-on-join reads from disk. See MEADOWS-migration-intent.md section 3.4.

BUSINESS RULE (MEADOWS §3.3): reactions, mentions, replies, @everyone are
core — they live in the server, not in bots. Group membership is the
routing substrate for all of them.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class GroupState:
    """In-memory group state: metadata + membership.

    `members` maps user_id (from JWT sub) to a dict with display info.
    History is not kept here — it is append-only on disk via JSONLPersistence.

    BUSINESS RULE: the server tracks who is in which group so it can:
    - route messages to the right room (Socket.IO room = group_id)
    - broadcast presence (user_joined/user_left/members_updated)
    - know who is online for ntfy (§3.3: "alleen de server weet wie online is")
    """

    group_id: str
    name: str = ""
    description: str = ""
    created_by: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    members: dict[str, dict] = field(default_factory=dict)

    def simplify(self) -> dict:
        """Frontend-safe group summary (no emails, no internal state).

        BUSINESS RULE: privacy — the frontend gets aggregate info, not
        raw member details. Matches the monolith's simplify_group()
        at sioserver.py:1204-1210.
        """
        return {
            "id": self.group_id,
            "name": self.name or self.group_id,
            "description": self.description,
            "member_count": len(self.members),
        }

    def safe_members(self) -> list[dict]:
        """Frontend-safe member list (user_id + username, NO email).

        BUSINESS RULE (§3.3): the server knows who is online; the frontend
        needs to display member lists. Emails are never sent to other
        clients — the monolith stripped them at sioserver.py:1510.
        """
        return [{"user_id": uid, "username": info.get("username", uid)} for uid, info in self.members.items()]


__all__ = ["GroupState"]
