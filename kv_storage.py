# kv_storage.py
"""
Simple append-only log storage for the key-value database.

Each record is written as a single JSON line.  On startup, the log
is replayed to rebuild the in-memory index.
"""

from __future__ import annotations

import json
import os
from typing import Dict, Iterator, Any


class LogStorage:
    """Append-only JSON-lines log for persistence."""

    def __init__(self, path: str = "data.db") -> None:
        self.path = path
        # Ensure the file exists
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8"):
                pass

    def append(self, record: Dict[str, Any]) -> None:
        """Append a single record to the log."""
        # Open in text append mode; newline-delimited JSON
        with open(self.path, "a", encoding="utf-8") as f:
            json.dump(record, f, separators=(",", ":"))
            f.write("\n")
            f.flush()

    def records(self) -> Iterator[Dict[str, Any]]:
        """Iterate over all records in the log."""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    # Skip corrupted lines (should not happen in this project)
                    continue
                if isinstance(rec, dict):
                    yield rec
