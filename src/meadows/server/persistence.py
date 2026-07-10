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
from typing import Any

from endow import Service

from meadows.protocol import Message
from meadows.protocol.codec import message_from_wire, message_to_wire


def _safe_filename(group_id: str) -> str:
    """Reduce a group id to a filesystem-safe filename component.

    SECURITY (CWE-22): prevents path traversal via group_id. The monolith
    validated at sioserver.py:1096 with `^[a-z0-9_-]{1,32}$`; this is the
    filesystem-level backstop.
    """
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in group_id) or "_"


class JSONLPersistence(Service):
    """Append-only JSONL message store: ``<messages_dir>/<group_id>.jsonl``."""

    messages_dir: Path

    def __init__(self, messages_dir: Path | None = None) -> None:
        if messages_dir is not None:
            self.messages_dir = Path(messages_dir)
            self.messages_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, group_id: str) -> Path:
        return self.messages_dir / f"{_safe_filename(group_id)}.jsonl"

    async def store(self, group_id: str, msg: Message) -> None:
        """Append a message as one JSON line to the group's file."""
        line = json.dumps(message_to_wire(msg), separators=(",", ":"))
        with self._path(group_id).open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    async def store_label_assigned(self, group_id: str, label_data: dict[str, Any]) -> None:
        """Append a LABEL_ASSIGNED record to the group's JSONL.

        BUSINESS RULE (MEADOWS-labeling-intent §2.9): LABEL_ASSIGNED
        events are stored as separate records in the group JSONL.  The
        server never merges MESSAGE and LABEL_ASSIGNED — they are
        distinct records.  FETCH_MESSAGES returns both.
        """
        line = json.dumps(label_data, separators=(",", ":"))
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

    async def load_display_history(self, group_id: str) -> list[dict]:
        """Load ALL messages for display (not limited to thread context).

        BUSINESS RULE: the monolith distinguished display history (all
        messages, for the chat UI) from thread context (last N, for bots).
        See sioserver.py:1290-1292: "Load ALL messages for display (not
        limited to DEFAULT_THREAD_SIZE). Thread context limit only applies
        to bots, not chat display."
        """
        path = self._path(group_id)
        if not path.exists():
            return []
        messages: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return messages

    async def load_thread_context(self, group_id: str, limit: int = 30) -> list[dict]:
        """Load the last N messages as raw dicts (for bot thread context).

        BUSINESS RULE (monolith base.py:42): BOT_CONTEXT_LIMIT=30 — bots
        receive the last 30 messages as context when handling a command.
        """
        path = self._path(group_id)
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        recent = lines[-limit:] if limit and limit > 0 else lines
        context: list[dict] = []
        for line in recent:
            line = line.strip()
            if not line:
                continue
            try:
                context.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return context

    async def mark_removed(self, group_id: str, message_id: str) -> bool:
        """Mark a message as removed (set removed=True in the JSONL).

        BUSINESS RULE: messages are not deleted, only marked removed —
        the strikethrough effect is a display concern, but the data is
        retained for audit. Matches monolith sioserver.py:1886-1959.

        Returns True if the message was found and marked, False otherwise.
        """
        path = self._path(group_id)
        if not path.exists():
            return False
        lines = path.read_text(encoding="utf-8").splitlines()
        found = False
        updated: list[str] = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("id") == message_id:
                    msg["removed"] = True
                    found = True
                updated.append(json.dumps(msg, separators=(",", ":")))
            except json.JSONDecodeError:
                updated.append(line)
        if found:
            with path.open("w", encoding="utf-8") as fh:
                fh.write("\n".join(updated) + "\n")
        return found

    async def load_by_ids(self, group_id: str, message_ids: list[str]) -> list[dict]:
        """Load specific messages by ID from a group's JSONL.

        Used by the fetch_messages event so bots can retrieve prior context
        (e.g. the message a reply references).
        """
        path = self._path(group_id)
        if not path.exists():
            return []
        want = set(message_ids)
        found: list[dict] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
                if msg.get("id") in want:
                    found.append(msg)
            except json.JSONDecodeError:
                continue
        return found


__all__ = ["JSONLPersistence"]
