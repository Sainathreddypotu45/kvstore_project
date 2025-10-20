"""
In-memory index for the KV store (no dict/map) with explicit typed entries.
Stores Entry(key, value) pairs and enforces last-write-wins.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Entry:
    """A single key–value record in memory."""
    key: str
    value: str


class KVList:
    """
    Minimal in-memory index using a Python list of Entry objects.

    """

    def __init__(self) -> None:
        """Initialize an empty list of Entry objects."""
        self._items: List[Entry] = []

    def set(self, key: str, value: str) -> None:
        """
        Insert or overwrite the value for `key`.

        """
        for item in self._items:
            if item.key == key:
                item.value = value
                return
        self._items.append(Entry(key=key, value=value))

    def get(self, key: str) -> Optional[str]:
        """
        Return the most recent value for `key`, or None if missing.

        Parameters
        ----------
        key : str
            The key to retrieve.

        Returns
        -------
        Optional[str]
            The value if present; otherwise None.
        """
        for i in range(len(self._items) - 1, -1, -1):
            if self._items[i].key == key:
                return self._items[i].value
        return None
