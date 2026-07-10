"""Label deduplication index — SQLite-backed via diskcache.

BUSINESS RULE (MEADOWS-labeling-intent §2.5): the dedup-key is
(origin, label, semver, message_id).  Metadata is NOT part of the key.
Two labels with the same key but different metadata are duplicates.
If you want different metadata, bump the semver.

BUSINESS RULE (§7): the index is a cache, not the source of truth.
Labels are stored in JSONL.  If the index corrupts, delete the
directory and let the server rebuild from JSONL records.
"""

from __future__ import annotations

from pathlib import Path

import diskcache


class LabelDedupIndex:
    """SQLite-backed dedup cache keyed by (origin, label, semver, message_id)."""

    def __init__(self, cache_dir: Path) -> None:
        self._cache = diskcache.Cache(str(cache_dir))

    def contains(self, origin: str, label: str, semver: str, message_id: str) -> bool:
        """Return True if this exact dedup key has been seen before."""
        key = (origin, label, semver, message_id)
        return key in self._cache

    def add(self, origin: str, label: str, semver: str, message_id: str) -> bool:
        """Atomically check-and-add.  Returns True if NEW (added), False if duplicate."""
        key = (origin, label, semver, message_id)
        return self._cache.add(key, True)

    def close(self) -> None:
        """Release the underlying SQLite connection."""
        self._cache.close()
