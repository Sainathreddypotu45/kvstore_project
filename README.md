# KV Store Project 1 — Persistent Key-Value Database

## Overview
This project implements a simple append-only key–value store that supports:
- `SET <key> <value>`
- `GET <key>`
- `EXIT`

All data is persisted in an append-only file (`data.db`) and restored upon restart by replaying the log.

## Example Usage
```bash
$ python kvstore.py
SET name Sainath
OK
GET name
Sainath
EXIT
