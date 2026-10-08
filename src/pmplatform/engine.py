"""Generic state machine around the native book. No venue protocol logic."""
from dataclasses import dataclass, field

from . import _book
from .model import Event


def native_levels(event):
    return [_book.Level(_book.Side.BID if level.side == "bid" else _book.Side.ASK,
                        level.price_e4, level.size_e6, getattr(_book.Operation, level.operation)) for level in event.levels]


@dataclass
class InstrumentBook:
    book: _book.Book = field(default_factory=_book.Book)
    status: str = "awaiting_snapshot"
    epoch: int = -1
    version: int = 0
    last_event_id: str = ""
    recv_ts_ns: int = 0
    snapshot_ts_ns: int = 0
    venue_ts_ns: int | None = None
    continuity: str = "unverified"
    highwater: dict[str, list[int]] = field(default_factory=dict)
    identity: dict = field(default_factory=dict)

    def clone(self):
        return InstrumentBook(self.book.clone(), self.status, self.epoch, self.version, self.last_event_id,
                              self.recv_ts_ns, self.snapshot_ts_ns, self.venue_ts_ns, self.continuity,
                              {k: v.copy() for k, v in self.highwater.items()}, self.identity.copy())

    def apply(self, event: Event):
        cursor_key = f"{event.source_topic}:{event.source_partition}"
        cursor = [event.source_offset, event.subevent_index]
        if cursor <= self.highwater.get(cursor_key, [-1, -1]):
            return None, []
        if event.connection_epoch < self.epoch:
            return None, [{"reason": "old_epoch", "event_id": event.event_id, "key": event.key}]
        violations = []
        self.identity = {"key": event.key, "venue": event.venue, "market_id": event.market_id,
                         "instrument_id": event.instrument_id, "outcome": event.outcome, "data_origin": event.data_origin}
        if event.connection_epoch > self.epoch:
            self.epoch = event.connection_epoch
            if self.status != "closed":
                self.status = "awaiting_snapshot"
            self.venue_ts_ns = None
            self.continuity = "unverified"
        if event.recv_ts_ns < self.recv_ts_ns:
            violations.append({"reason": "receive_time_regression", "event_id": event.event_id, "key": event.key})
        if event.venue_ts_ns is not None and self.venue_ts_ns is not None and event.venue_ts_ns < self.venue_ts_ns:
            violations.append({"reason": "venue_time_regression", "event_id": event.event_id, "key": event.key})
        self.highwater[cursor_key] = cursor
        self.recv_ts_ns = event.recv_ts_ns
        self.last_event_id = event.event_id
        self.version += 1
        if event.venue_ts_ns is not None:
            self.venue_ts_ns = event.venue_ts_ns
        try:
            if event.event_kind == "snapshot":
                if self.status == "closed":
                    raise ValueError("closed_book")
                self.book.snapshot(native_levels(event))
                self.status = "live"
                self.snapshot_ts_ns = event.recv_ts_ns
                self.continuity = event.continuity
            elif event.event_kind == "delta":
                if self.status != "live":
                    raise ValueError("delta_without_live_snapshot")
                self.book.update(native_levels(event))
            elif event.event_kind == "status" and event.status == "closed":
                self.status = "closed"
            elif event.event_kind == "control":
                if event.status in ("disconnect", "sequence_gap", "sequence_regression", "timeout", "metadata_changed", "parser_error"):
                    if self.status != "closed":
                        self.status = "stale"
                    self.continuity = "broken"
                    violations.append({"reason": event.status, "event_id": event.event_id, "key": event.key})
                elif event.status == "resync":
                    if self.status != "closed":
                        self.status = "resyncing"
                elif event.status == "untrack":
                    self.status = "untracked"
            if self.status == "live":
                for expected, actual, empty in ((event.expected_bid_e4, self.book.best_bid(), 0),
                                                 (event.expected_ask_e4, self.book.best_ask(), 10_000)):
                    if expected is not None and expected != (actual if actual is not None else empty):
                        raise ValueError("reported_top_mismatch")
        except (ValueError, OverflowError) as error:
            if self.status != "closed":
                self.status = "stale"
            self.continuity = "broken"
            violations.append({"reason": str(error), "event_id": event.event_id, "key": event.key})
        if any(v["reason"] in ("receive_time_regression", "venue_time_regression") for v in violations):
            if self.status != "closed":
                self.status = "stale"
            self.continuity = "broken"
        return self.state(), violations

    def state(self, depth=10):
        def levels(side):
            return [{"price_e4": p, "size_e6": q} for p, q in self.book.depth(side, depth)]
        return {**self.identity, "schema_version": 1, "status": self.status, "epoch": self.epoch,
                "version": self.version, "event_id": self.last_event_id, "recv_ts_ns": self.recv_ts_ns,
                "snapshot_ts_ns": self.snapshot_ts_ns, "venue_ts_ns": self.venue_ts_ns,
                "continuity": self.continuity, "bids": levels(_book.Side.BID), "asks": levels(_book.Side.ASK)}

    def checkpoint(self):
        return {"state": self.state(10001), "highwater": {k: v.copy() for k, v in self.highwater.items()}}

    @classmethod
    def restore(cls, checkpoint):
        data = checkpoint["state"]
        result = cls()
        result.book.snapshot([_book.Level(side, level["price_e4"], level["size_e6"])
                              for side, name in ((_book.Side.BID, "bids"), (_book.Side.ASK, "asks"))
                              for level in data[name]])
        result.status, result.epoch, result.version = data["status"], data["epoch"], data["version"]
        result.last_event_id, result.recv_ts_ns = data["event_id"], data["recv_ts_ns"]
        result.snapshot_ts_ns, result.venue_ts_ns = data["snapshot_ts_ns"], data["venue_ts_ns"]
        result.continuity = data["continuity"]
        result.highwater = {k: v.copy() for k, v in checkpoint["highwater"].items()}
        result.identity = {k: data[k] for k in ("key", "venue", "market_id", "instrument_id", "outcome", "data_origin")}
        return result


class BookEngine:
    def __init__(self):
        self.books: dict[str, InstrumentBook] = {}

    def apply(self, event):
        return self.books.setdefault(event.key, InstrumentBook()).apply(event)

    def stage(self, events):
        working = BookEngine()
        working.books = self.books.copy()
        for key in {e.key for e in events}:
            working.books[key] = self.books[key].clone() if key in self.books else InstrumentBook()
        states, violations = [], []
        for event in events:
            state, errors = working.apply(event)
            if state is not None:
                states.append(state)
            violations.extend(errors)
        return working, states, violations

    def checkpoint(self):
        return {key: book.checkpoint() for key, book in sorted(self.books.items())}

    @classmethod
    def restore(cls, data):
        result = cls()
        result.books = {key: InstrumentBook.restore(checkpoint) for key, checkpoint in data.items()}
        return result
