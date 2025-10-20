

from __future__ import annotations

import os
from typing import TextIO

from kv_index import KVList

DATA_FILE: str = "data.db"


def fsync_file(fh: TextIO) -> None:
    
    try:
        fh.flush()
        os.fsync(fh.fileno())
    except (OSError, AttributeError):
        return


def replay_log(kv: KVList, path: str = DATA_FILE) -> None:
    
    if not os.path.exists(path):
        return

    try:
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line: str = raw.rstrip("\n")
                if not line:
                    continue
                parts = line.split(" ", 2)
                if len(parts) == 3 and parts[0] == "SET":
                    _, key, value = parts
                    # Defensive: ensure no-space key as required by CLI
                    if key and (" " not in key):
                        kv.set(key, value)
    except (OSError, UnicodeError):
        # Corrupt/unreadable log: skip replay silently.
        return
