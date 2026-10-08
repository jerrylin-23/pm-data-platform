import hashlib
from dataclasses import dataclass

import yaml

from .model import canonical


@dataclass(frozen=True)
class Pair:
    name: str
    left: str
    right: str
    invert_right: bool = False
    fee_e4: int = 0
    threshold_e4: int = 100


def load_pairs(path):
    with open(path) as stream:
        data = yaml.safe_load(stream) or {}
    pairs = [Pair(**p) for p in data.get("pairs", [])]
    if any(p.fee_e4 < 0 or p.threshold_e4 < 0 for p in pairs):
        raise ValueError("negative_fee_or_threshold")
    return pairs


def quote(state, invert=False):
    if not state["bids"] or not state["asks"]:
        return None
    bid, ask = state["bids"][0], state["asks"][0]
    if invert:
        return ({"price_e4": 10_000 - ask["price_e4"], "size_e6": ask["size_e6"]},
                {"price_e4": 10_000 - bid["price_e4"], "size_e6": bid["size_e6"]})
    return bid, ask


class GapDetector:
    def __init__(self, pairs, stale_ns=15_000_000_000, snapshot_ns=120_000_000_000):
        self.pairs = pairs
        self.stale_ns, self.snapshot_ns = stale_ns, snapshot_ns
        self.states = {}

    def apply(self, state):
        self.states[state["key"]] = state
        now = state["recv_ts_ns"]
        alerts = []
        for pair in self.pairs:
            if state["key"] not in (pair.left, pair.right):
                continue
            left, right = self.states.get(pair.left), self.states.get(pair.right)
            if not left or not right:
                continue
            reference_time = max(now, left["recv_ts_ns"], right["recv_ts_ns"])
            if any(s["status"] != "live" or s["continuity"] == "broken" or
                   not 0 <= reference_time - s["recv_ts_ns"] <= self.stale_ns or
                   not 0 <= reference_time - s["snapshot_ts_ns"] <= self.snapshot_ns for s in (left, right)):
                continue
            lquote, rquote = quote(left), quote(right, pair.invert_right)
            if not lquote or not rquote:
                continue
            for buy_key, sell_key, ask, bid in ((pair.left, pair.right, lquote[1], rquote[0]),
                                               (pair.right, pair.left, rquote[1], lquote[0])):
                gap = bid["price_e4"] - ask["price_e4"] - pair.fee_e4
                if gap < pair.threshold_e4:
                    continue
                alert = {"schema_version": 1, "pair": pair.name, "buy_quote": buy_key, "sell_quote": sell_key,
                         "ask_e4": ask["price_e4"], "bid_e4": bid["price_e4"], "gap_e4": gap,
                         "displayed_size_e6": min(ask["size_e6"], bid["size_e6"]),
                         "fee_e4": pair.fee_e4, "threshold_e4": pair.threshold_e4,
                         "recv_ts_ns": now, "source_event_ids": [left["event_id"], right["event_id"]],
                         "book_versions": [left["version"], right["version"]],
                         "continuity": [left["continuity"], right["continuity"]],
                         "data_origin": [left["data_origin"], right["data_origin"]],
                         "config_sha256": hashlib.sha256(canonical(vars(pair))).hexdigest(),
                         "invert_right": pair.invert_right}
                alert["alert_id"] = hashlib.sha256(canonical(alert)).hexdigest()
                alerts.append(alert)
        return alerts
