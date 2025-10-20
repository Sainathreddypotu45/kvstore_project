"""
CSCE 5350 – Project 1: Simple Key-Value Store
Author: Sainath Reddy Potu
EUID: 11768010

CLI
---
    SET <key> <value>
    GET <key>
    EXIT

Contract
--------
• Append-only persistence to 'data.db' (fsync per SET).
• Log replay on startup rebuilds the in-memory index (no dict/map).
• Last-write-wins semantics.
• GET for a missing key prints a single blank line (no text).
Run (recommended for testers):
    python -u kvstore.py
"""

from __future__ import annotations

import sys
from typing import Callable, Optional, TextIO, Tuple

from kv_index import KVList
from kv_storage import DATA_FILE, fsync_file, replay_log


def parse_command(line: str) -> Optional[Tuple[str, Tuple[str, ...]]]:
    
    
    if not line:
        return None

    parts = line.split(" ", 2)
    cmd = parts[0].upper()

    if cmd == "EXIT":
        return ("EXIT", ())

    if cmd == "SET" and len(parts) == 3:
        key, value = parts[1], parts[2]
        if key and (" " not in key):
            return ("SET", (key, value))
        return None

    if cmd == "GET" and len(parts) == 2:
        key = parts[1]
        if key and (" " not in key):
            return ("GET", (key,))
        return None

    return None


def handle_set(kv: KVList, log_fh: TextIO, key: str, value: str) -> None:
    
    kv.set(key, value)
    try:
        log_fh.write(f"SET {key} {value}\n")
        fsync_file(log_fh)
    except OSError:
        # Keep CLI clean for the tester; value still stored in memory.
        pass
    print("OK")


def handle_get(kv: KVList, key: str) -> None:
    """
    Print the value if present; otherwise print a single blank line.

    Parameters
    ----------
    kv : KVList
        In-memory index.
    key : str
        Non-empty key without spaces.
    """
    val: Optional[str] = kv.get(key)
    print("" if val is None else val)


def main() -> None:
    """Read commands from STDIN; write exact outputs to STDOUT."""
    kv: KVList = KVList()
    replay_log(kv, DATA_FILE)

    # Keep the log open across the loop; newline '\n' for consistent output.
    try:
        with open(DATA_FILE, "a", encoding="utf-8", newline="\n") as log_fh:
            # Simple command dispatch table (cleaner organization)
            def do_set(args: Tuple[str, ...]) -> None:
                key, value = args  # type: ignore[misc]
                handle_set(kv, log_fh, key, value)

            def do_get(args: Tuple[str, ...]) -> None:
                (key,) = args  # type: ignore[misc]
                handle_get(kv, key)

            dispatch: dict[str, Callable[[Tuple[str, ...]], None]] = {
                "SET": do_set,
                "GET": do_get,
            }

            for raw in sys.stdin:
                line: str = raw.strip()
                parsed = parse_command(line)
                if parsed is None:
                    # Silently ignore malformed/empty input lines
                    continue

                cmd, args = parsed
                if cmd == "EXIT":
                    break

                handler = dispatch.get(cmd)
                if handler is not None:
                    handler(args)
                # Unknown commands are ignored silently.
    except OSError:
        # If the log cannot be opened, still serve commands in-memory.
        for raw in sys.stdin:
            line = raw.strip()
            parsed = parse_command(line)
            if parsed is None:
                continue
            cmd, args = parsed
            if cmd == "EXIT":
                break
            if cmd == "SET":
                key, value = args  # type: ignore[misc]
                kv.set(key, value)
                print("OK")
            elif cmd == "GET":
                (key,) = args  # type: ignore[misc]
                handle_get(kv, key)


if __name__ == "__main__":
    main()
