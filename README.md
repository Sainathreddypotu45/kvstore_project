KV Store Project — Persistent Key-Value Database (Project-2)
Overview

This project is the second phase of CSCE 5350 — Build Your Own Database at the University of North Texas. It extends the basic append-only key-value store from Project-1 and introduces advanced database features including range queries, multi-key operations, TTL-based expiration, and transactional writes.

The database persists all changes using an append-only log file (data.db), and state is reconstructed on restart by replaying the log.

Features Implemented

Basic commands: SET, GET, DEL, EXISTS

Multi-key commands: MSET, MGET

Lexicographic range scan: RANGE

Expiration and TTL semantics: EXPIRE, TTL, PERSIST

Transactions with atomic commit and abort: BEGIN, COMMIT, ABORT

Durable log-based storage model

Crash-safe replay on restart

Lazy expiration for TTL keys

All functionality is implemented in Python using a custom log index and in-memory data structures
