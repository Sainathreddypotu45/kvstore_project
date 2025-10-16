
import sys
import os

DATA_FILE = "data.db"

class KVList:
    
    def __init__(self):
        self.items = []  # list of [key, value]

    def set(self, key, value):
        # Replace if key exists, else append
        for pair in self.items:
            if pair[0] == key:
                pair[1] = value
                return
        self.items.append([key, value])

    def get(self, key):
        # Linear search from end for last-write-wins
        for i in range(len(self.items) - 1, -1, -1):
            if self.items[i][0] == key:
                return self.items[i][1]
        return None

def fsync_file(fh):
    fh.flush()
    os.fsync(fh.fileno())

def replay_log(kv, path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            parts = line.strip().split(" ", 2)
            if len(parts) == 3 and parts[0] == "SET":
                _, key, value = parts
                kv.set(key, value)

def main():
    kv = KVList()
    replay_log(kv, DATA_FILE)
    log = open(DATA_FILE, "a", encoding="utf-8")

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        parts = line.split(" ", 2)
        cmd = parts[0].upper()

        if cmd == "EXIT":
            break
        elif cmd == "SET" and len(parts) == 3:
            key, value = parts[1], parts[2]
            kv.set(key, value)
            log.write(f"SET {key} {value}\n")
            fsync_file(log)
            print("OK")
        elif cmd == "GET" and len(parts) == 2:
            key = parts[1]
            val = kv.get(key)
            print(val if val is not None else "NULL")

    log.close()

if __name__ == "__main__":
    main()