"""ntfy preferences storage — per-user notification settings.

BUSINESS RULE (MEADOWS §3.3 line 75): ntfy stays core for now because
"alleen de server weet wie online is." The server pushes ntfy
notifications to offline users who were mentioned/replied-to/@everyone'd.
This module stores the per-user ntfy configuration (server URL, topic,
auth token, enabled flag).

The monolith stored these in a single JSON file at
webchat_users/ntfy_prefs.json keyed by `user-{username}` (sioserver.py:383-407).
This module follows the same pattern but as a class so the Hub owns it
as instance state (not a module global — §3.1).
"""

from __future__ import annotations

import json
from pathlib import Path

from endow import Service

DEFAULT_PREFS: dict = {
    "enabled": False,
    "server": "",
    "topic": "",
    "token": "",
}


class NtfyPrefsStore(Service):
    """Per-user ntfy preferences, stored as a single JSON file.

    The file is keyed by user_id (the JWT `sub` claim, e.g. `user-alice`).
    Missing users return DEFAULT_PREFS. The store is lazy: the file is
    created on first save, not on construction.
    """

    prefs_path: Path

    def __init__(self, prefs_path: Path | None = None) -> None:
        if prefs_path is not None:
            self.prefs_path = Path(prefs_path)
            self.prefs_path.parent.mkdir(parents=True, exist_ok=True)

    def _load_all(self) -> dict[str, dict]:
        if not self.prefs_path.exists():
            return {}
        try:
            return json.loads(self.prefs_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}

    def get(self, user_id: str) -> dict:
        """Return prefs for a user, or DEFAULT_PREFS if none saved."""
        return self._load_all().get(user_id, dict(DEFAULT_PREFS))

    def set(self, user_id: str, prefs: dict) -> None:
        """Save prefs for a user."""
        all_prefs = self._load_all()
        all_prefs[user_id] = {**DEFAULT_PREFS, **prefs}
        self.prefs_path.write_text(json.dumps(all_prefs, indent=2), encoding="utf-8")


__all__ = ["DEFAULT_PREFS", "NtfyPrefsStore"]
