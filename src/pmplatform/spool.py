import json
import sqlite3
import time
from pathlib import Path

from .model import RawRecord, canonical


class Spool:
    """Raw capture and pending Kafka delivery survive process restarts."""
    def __init__(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS frames (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, sent INTEGER DEFAULT 0)")
        self.db.execute("CREATE TABLE IF NOT EXISTS epochs (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp INTEGER)")
        self.db.commit()

    def epoch(self):
        cursor = self.db.execute("INSERT INTO epochs(timestamp) VALUES (?)", (time.time_ns(),))
        self.db.commit()
        return cursor.lastrowid

    def append(self, payload, **fields):
        cursor = self.db.execute("INSERT INTO frames(payload) VALUES ('')")
        offset = cursor.lastrowid
        raw = RawRecord.capture(payload, offset=offset, partition=0, **fields)
        self.db.execute("UPDATE frames SET payload=? WHERE id=?", (canonical(raw.model_dump(mode="json")).decode(), offset))
        self.db.commit()
        return raw

    def pending(self, limit=100):
        return [RawRecord.model_validate(json.loads(row[0])) for row in self.db.execute(
            "SELECT payload FROM frames WHERE sent=0 ORDER BY id LIMIT ?", (limit,))]

    def acknowledge(self, offset):
        self.db.execute("UPDATE frames SET sent=1 WHERE id=?", (offset,))
        self.db.commit()

    def close(self):
        self.db.close()
