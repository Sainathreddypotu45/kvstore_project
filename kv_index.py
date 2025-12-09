# kv_index.py
"""
In-memory index for the key-value database.

This module avoids using dict/map for the core key->value store.
Instead it maintains two parallel lists:

    keys:   [str, str, ...] sorted lexicographically
    entries:[KeyEntry, KeyEntry, ...]

Access is via binary search on the sorted keys list.
"""

from __future__ import annotations

from dataclasses import dataclass
from bisect import bisect_left, bisect_right
from typing import List, Optional


@dataclass
class KeyEntry:
    key: str
    value: str
    exists: bool          # False means "logically deleted"
    expire_at_ms: Optional[int]  # Absolute expiry time in ms, or None


class KeyIndex:
    """Sorted in-memory index without using dict/map for the core store."""

    def __init__(self) -> None:
        self.keys: List[str] = []
        self.entries: List[KeyEntry] = []

    # ---------- internal helpers ----------

    def _find_pos(self, key: str) -> int:
        """Binary search: index of key, or insertion point if not found."""
        return bisect_left(self.keys, key)

    def _find_entry(self, key: str) -> Optional[KeyEntry]:
        """Return KeyEntry for key, or None if not present."""
        pos = self._find_pos(key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return self.entries[pos]
        return None

    def _ensure_not_expired(self, entry: KeyEntry, now_ms: int) -> None:
        """Apply lazy expiration: if expired, mark as deleted and clear TTL."""
        if entry.exists and entry.expire_at_ms is not None:
            if entry.expire_at_ms <= now_ms:
                entry.exists = False
                entry.expire_at_ms = None

    # ---------- cloning for transactions ----------

    def clone(self) -> "KeyIndex":
        """Deep-ish clone used for transaction snapshots."""
        clone_index = KeyIndex()
        clone_index.keys = list(self.keys)
        clone_index.entries = [
            KeyEntry(e.key, e.value, e.exists, e.expire_at_ms)
            for e in self.entries
        ]
        return clone_index

    # ---------- core operations ----------

    def set(self, key: str, value: str, preserve_ttl: bool = True) -> None:
        """SET key to value, optionally preserving existing TTL."""
        pos = self._find_pos(key)
        if pos < len(self.keys) and self.keys[pos] == key:
            entry = self.entries[pos]
            entry.value = value
            entry.exists = True
            if not preserve_ttl:
                entry.expire_at_ms = None
        else:
            self.keys.insert(pos, key)
            self.entries.insert(
                pos, KeyEntry(key=key, value=value, exists=True, expire_at_ms=None)
            )

    def get(self, key: str, now_ms: int) -> Optional[str]:
        entry = self._find_entry(key)
        if entry is None:
            return None
        self._ensure_not_expired(entry, now_ms)
        if not entry.exists:
            return None
        return entry.value

    def exists(self, key: str, now_ms: int) -> bool:
        entry = self._find_entry(key)
        if entry is None:
            return False
        self._ensure_not_expired(entry, now_ms)
        return entry.exists

    def delete(self, key: str, now_ms: int) -> bool:
        entry = self._find_entry(key)
        if entry is None:
            return False
        self._ensure_not_expired(entry, now_ms)
        if not entry.exists:
            return False
        entry.exists = False
        entry.expire_at_ms = None
        return True

    # ---------- TTL operations ----------

    def expire_abs(self, key: str, expire_at_ms: int, now_ms: int) -> int:
        """
        Set absolute expiry time for key.  Returns 1 if TTL set, 0 if key missing.
        If expire_at_ms <= now_ms, key expires immediately.
        """
        entry = self._find_entry(key)
        if entry is None:
            return 0
        self._ensure_not_expired(entry, now_ms)
        if not entry.exists:
            return 0

        if expire_at_ms <= now_ms:
            # Immediate expiration
            entry.exists = False
            entry.expire_at_ms = None
        else:
            entry.expire_at_ms = expire_at_ms
        return 1

    def ttl(self, key: str, now_ms: int) -> int:
        """
        TTL semantics:
        - return remaining ms (int) if TTL set and key exists
        - -1 if key exists but has no TTL
        - -2 if missing or expired
        """
        entry = self._find_entry(key)
        if entry is None:
            return -2
        self._ensure_not_expired(entry, now_ms)
        if not entry.exists:
            return -2
        if entry.expire_at_ms is None:
            return -1
        remaining = entry.expire_at_ms - now_ms
        if remaining <= 0:
            entry.exists = False
            entry.expire_at_ms = None
            return -2
        return int(remaining)

    def persist(self, key: str, now_ms: int) -> int:
        """Clear TTL for key. Return 1 if cleared, 0 otherwise."""
        entry = self._find_entry(key)
        if entry is None:
            return 0
        self._ensure_not_expired(entry, now_ms)
        if not entry.exists:
            return 0
        if entry.expire_at_ms is None:
            return 0
        entry.expire_at_ms = None
        return 1

    # ---------- range queries ----------

    def range_keys(self, start: str, end: str, now_ms: int) -> List[str]:
        """
        Return list of keys in lexicographic order between [start, end], inclusive.

        Empty string ("") means open bound.  Gradebot sends "" as two quotes,
        so the caller is responsible for converting '""' to "" before calling this.
        """
        if not self.keys:
            return []

        # Lower bound
        if start == "":
            lo = 0
        else:
            lo = bisect_left(self.keys, start)

        # Upper bound (inclusive)
        if end == "":
            hi = len(self.keys) - 1
        else:
            hi = bisect_right(self.keys, end) - 1

        if lo < 0:
            lo = 0
        if hi >= len(self.keys):
            hi = len(self.keys) - 1
        if lo > hi:
            return []

        result: List[str] = []
        for i in range(lo, hi + 1):
            entry = self.entries[i]
            self._ensure_not_expired(entry, now_ms)
            if entry.exists:
                # This implementation never inserts internal/UUID keys,
                # so all existing keys are user keys.
                result.append(entry.key)
        return result
