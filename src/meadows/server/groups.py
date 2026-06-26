"""Group state — in-memory membership.

Message history lives in JSONLPersistence (on disk), not in memory. The
server is the source of truth for routing and presence, not for replay;
fetch-on-join reads from disk. See MEADOWS-migration-intent.md section 3.4.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GroupState:
    """In-memory group membership.

    `members` holds the Socket.IO sids currently in the group. History is
    not kept here — it is append-only on disk.
    """

    group_id: str
    members: set[str] = field(default_factory=set)


__all__ = ["GroupState"]
