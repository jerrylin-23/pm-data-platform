import json
import sqlite3
import time
from pathlib import Path

from .model import canonical


class Subscriptions:
    def __init__(self, path, maximum=200):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS subscriptions
            (key TEXT PRIMARY KEY, venue TEXT, market TEXT, action TEXT, payload TEXT)""")
        self.db.execute("CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, key TEXT, payload TEXT)")
        self.db.commit()
        self.maximum = maximum

    def list(self):
        return [json.loads(row[0]) for row in self.db.execute(
            "SELECT payload FROM subscriptions WHERE action='track' ORDER BY key")]

    def update(self, markets, action="track"):
        if action not in ("track", "untrack"):
            raise ValueError("invalid_subscription_action")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            saved = self.list()
            current = {(m["venue"], m["market_id"]) for m in saved}
            changes = []
            if action == "track":
                current.update((m["venue"], m["market_id"]) for m in markets)
                if len(current) > self.maximum:
                    raise ValueError(f"Tracking limit is {self.maximum} markets.")
                replacements = {(m["venue"], m["market_id"]) for m in markets}
                new_keys = {(m["venue"], m["instrument_id"]) for m in markets}
                changes.extend((m, "untrack") for m in saved if (m["venue"], m["market_id"]) in replacements
                               and (m["venue"], m["instrument_id"]) not in new_keys)
            changes.extend((m, action) for m in markets)
            records = []
            for market, record_action in changes:
                key = f"{market['venue']}:{market['instrument_id']}"
                payload = {**market, "action": record_action, "added_at_ns": time.time_ns()}
                body = canonical(payload).decode()
                self.db.execute("INSERT OR REPLACE INTO subscriptions VALUES (?, ?, ?, ?, ?)",
                                (key, market["venue"], market["market_id"], record_action, body))
                self.db.execute("INSERT INTO outbox(key,payload) VALUES (?,?)", (key, body))
                if record_action == action:
                    records.append(payload)
            self.db.commit()
            return records
        except BaseException:
            self.db.rollback()
            raise

    def remove(self, identifier):
        matches = [m for m in self.list() if identifier in (m["instrument_id"], m["market_id"],
                                                          f"{m['venue']}:{m['instrument_id']}")]
        if not matches:
            raise ValueError("Tracked market not found.")
        return self.update(matches, "untrack")

    def publish(self, producer):
        rows = list(self.db.execute("SELECT id,key,payload FROM outbox ORDER BY id"))
        for identifier, key, payload in rows:
            producer.send("control.subscriptions", key, payload.encode())
            self.db.execute("DELETE FROM outbox WHERE id=?", (identifier,))
            self.db.commit()
        return len(rows)

    def close(self):
        self.db.close()

    def restore(self, records):
        with self.db:
            for key, market in records.items():
                row = self.db.execute("SELECT payload FROM subscriptions WHERE key=?", (key,)).fetchone()
                if row and json.loads(row[0])["added_at_ns"] > market["added_at_ns"]:
                    continue
                self.db.execute("INSERT OR REPLACE INTO subscriptions VALUES (?, ?, ?, ?, ?)",
                                (key, market["venue"], market["market_id"], market["action"], canonical(market).decode()))
