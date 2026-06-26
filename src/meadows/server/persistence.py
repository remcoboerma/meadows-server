"""JSONL message store — append-only, one file per group.

Each line is the wire form of a Message (see meadows.protocol.codec).
The store is the source of truth for replay; the in-memory GroupState
only tracks live membership.

Synchronous file I/O wrapped in async methods is acceptable for the PoC:
JSONL append is fast and the volumes are small. A future iteration may
swap in aiofiles without changing the surface.
"""

from __future__ import annotations

import json
from pathlib import Path

from meadows.protocol import Message
from meadows.protocol.codec import message_from_wire, message_to_wire


def _safe_filename(group_id: str) -> str:
    """Reduce a group id to a filesystem-safe filename component."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in group_id) or "_"


class JSONLPersistence:
    """Append-only JSONL message store: ``<messages_dir>/<group_id>.jsonl``."""

    def __init__(self, messages_dir: Path) -> None:
        self.messages_dir = Path(messages_dir)

    def _path(self, group_id: str) -> Path:
        return self.messages_dir / f"{_safe_filename(group_id)}.jsonl"

    async def store(self, group_id: str, msg: Message) -> None:
        """Append a message as one JSON line to the group's file."""
        self.messages_dir.mkdir(parents=True, exist_ok=True)
        line = json.dumps(message_to_wire(msg), separators=(",", ":"))
        with self._path(group_id).open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    async def load_group(self, group_id: str, limit: int = 50) -> list[Message]:
        """Load the most recent ``limit`` messages for a group.

        Missing group -> empty list. Unparseable lines are skipped (forward-
        compat with protocol additions written by a newer server).
        """
        path = self._path(group_id)
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        recent = lines[-limit:] if limit and limit > 0 else lines
        messages: list[Message] = []
        for line in recent:
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(message_from_wire(json.loads(line)))
            except Exception:
                continue
        return messages


__all__ = ["JSONLPersistence"]
