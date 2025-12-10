KV Store Project — Persistent Key-Value Database (Project-2)
Overview

This project is the second phase of CSCE 5350 — Build Your Own Database at the University of North Texas. It extends the basic append-only key-value store from Project-1 and introduces advanced database features including range queries, multi-key operations, TTL-based expiration, and transactional writes. The database persists all changes using an append-only log file (data.db), and state is reconstructed on restart by replaying the log.

Features Implemented

Basic commands: SET, GET, DEL, EXISTS
Multi-key commands: MSET, MGET
Lexicographic range scan: RANGE
Expiration and TTL semantics: EXPIRE, TTL, PERSIST
Transactions with atomic commit and abort: BEGIN, COMMIT, ABORT
Durable log-based storage model
Crash-safe replay on restart
Lazy expiration for TTL keys
All functionality is implemented in Python using a custom log index and in-memory data structures.

Supported Commands
Basic Operations

SET <key> <value>
GET <key>
DEL <key>
EXISTS <key>
EXIT

Example:

SET name Sainath
OK
GET name
Sainath
EXISTS name
1
DEL name
1
EXISTS name
0
EXIT

Multi-Key Operations

MSET k1 v1 k2 v2
MGET k1 k2 k3

Example:

MSET a 10 b 20
OK
MGET a b c
10
20
nil

Range Scan (Lexicographic)

Returns keys from start to end (inclusive), in sorted order.

Example:

SET a 1
OK
SET b 2
OK
SET c 3
OK
RANGE a c
a
b
c
END

Expiration and TTL

EXPIRE <key> <seconds>
TTL <key>
PERSIST <key>

TTL return values:
positive number = seconds remaining
-1 = key exists with no TTL
-2 = key missing or expired

Example:

SET temp 99
OK
EXPIRE temp 100
1
TTL temp
98
PERSIST temp
1
TTL temp
-1

TTL cleanup uses lazy expiration: expired keys are removed only when accessed.

Transactions

Transactions support atomic writes with commit/abort.

BEGIN
COMMIT
ABORT

BEGIN starts transactional staging.
READ returns staged values first (read-your-writes).
ABORT discards staged writes.
COMMIT writes changes to data.db.

Example:

BEGIN
SET x 10
OK
GET x
10
ABORT
OK
GET x
nil

Persistence and Storage Architecture

The database uses a single append-only log file.
Every write, delete, expiration update, and commit event is appended.
On restart, the log file is replayed in order to rebuild in-memory state.
This ensures crash consistency, atomic committed transactions, and simplicity without complex checkpointing.

Project Structure

kvstore_project/
│
├─ kvstore.py (CLI and command dispatcher)
├─ kv_index.py (in-memory index, TTL, range scan)
├─ kv_storage.py (append-only log implementation)
├─ data.db (persistent log file)
└─ gradebot/ (local gradebot binary)

Running the Program

python kvstore.py

Example session:

SET name Sainath
OK
GET name
Sainath
EXIT

Testing with Gradebot

gradebot project-2 --dir="." --run="python -u kvstore.py"

Project-2 tests include:
DeleteExists
MSET/MGET
TTL expiration
RANGE lexicographic ordering
Transaction commit/abort
Code quality

To tag and push the final submission:
git tag project-2
git push origin project-2
git push origin main

Author

Sainath Reddy Potu
CSCE 5350 — University of North Texas
EUID: 11768010

Notes

Tested on Windows 11 using Python 3.14.
Passed Project-2 Gradebot with >90%.
Implements lazy expiration for simplicity and performance.
Inspired by log-structured storage systems.

This README documents the complete Project-2 implementation, covering features, usage, storage architecture, and testing. It replaces the Project-1 overview with the advanced feature set implemented in this phase.