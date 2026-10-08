"""Parquet files with a durable offset ledger and atomic file publication."""
import hashlib
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .model import canonical, iso_ns

SCHEMA = pa.schema([
    ("kind", pa.string()), ("topic", pa.string()), ("partition", pa.int32()),
    ("offset", pa.int64()), ("subindex", pa.int32()), ("venue", pa.string()),
    ("recv_ts_ns", pa.int64()), ("payload_json", pa.string()),
])


class Archive:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / "index.sqlite", timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("""CREATE TABLE IF NOT EXISTS records (
            kind TEXT, topic TEXT, part INTEGER, off INTEGER, sub INTEGER,
            venue TEXT, ts INTEGER, file TEXT, payload_sha TEXT,
            PRIMARY KEY(kind, topic, part, off, sub))""")
        self.db.commit()

    def close(self):
        self.db.close()

    def append(self, kind, items):
        entries = []
        for item in items:
            data = item.model_dump(mode="json") if hasattr(item, "model_dump") else item
            if kind == "raw":
                topic, part, offset, sub = data["topic"], data["partition"], data["offset"], 0
            else:
                topic, part, offset, sub = (f"md.events/{data['source_topic']}", data["source_partition"],
                                             data["source_offset"], data["subevent_index"])
            entries.append((kind, topic, part, offset, sub, data))
        return self.write(entries)

    def write(self, entries):
        self.db.execute("BEGIN IMMEDIATE")
        count = 0
        try:
            groups = defaultdict(list)
            pending = {}
            for kind, topic, part, offset, sub, data in entries:
                identity = (kind, topic, part, offset, sub)
                body = canonical(data)
                digest = hashlib.sha256(body).hexdigest()
                row = self.db.execute(
                    "SELECT payload_sha FROM records WHERE kind=? AND topic=? AND part=? AND off=? AND sub=?",
                    identity).fetchone()
                if row or identity in pending:
                    existing = row[0] if row else pending[identity]
                    if existing != digest:
                        raise ValueError("archive_offset_payload_conflict")
                    continue
                pending[identity] = digest
                hour = iso_ns(data["recv_ts_ns"])[:13].replace("T", "/")
                groups[kind, data["venue"], hour].append((identity, data["recv_ts_ns"], body, digest))
            for (kind, venue, hour), rows in sorted(groups.items()):
                rows.sort(key=lambda row: row[0])
                digest = hashlib.sha256(b"\n".join(canonical(list(r[0])) + b":" + r[2] for r in rows)).hexdigest()
                relative = Path(kind) / venue / hour / f"{digest}.parquet"
                path = self.root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix(".tmp")
                table = pa.Table.from_pylist([
                    {"kind": identity[0], "topic": identity[1], "partition": identity[2],
                     "offset": identity[3], "subindex": identity[4], "venue": venue,
                     "recv_ts_ns": ts, "payload_json": body.decode()}
                    for identity, ts, body, _ in rows], schema=SCHEMA)
                pq.write_table(table, temporary, compression="zstd", use_dictionary=False,
                               write_statistics=True, version="2.6")
                with temporary.open("rb") as f:
                    os.fsync(f.fileno())
                os.replace(temporary, path)
                directory = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                for identity, ts, _, body_sha in rows:
                    self.db.execute("INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                    (*identity, venue, ts, str(relative), body_sha))
                    count += 1
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return count

    def rows(self, kind, venue=None):
        import json
        query, params = "SELECT DISTINCT file FROM records WHERE kind=?", [kind]
        if venue:
            query += " AND venue=?"
            params.append(venue)
        files = [self.root / row[0] for row in self.db.execute(query + " ORDER BY file", params)]
        rows = []
        for path in files:
            rows.extend(pq.ParquetFile(path).read().to_pylist())
        rows.sort(key=lambda r: (r["topic"], r["partition"], r["offset"], r["subindex"]))
        return [json.loads(row["payload_json"]) for row in rows]

    def count(self, kind):
        return self.db.execute("SELECT COUNT(*) FROM records WHERE kind=?", (kind,)).fetchone()[0]
