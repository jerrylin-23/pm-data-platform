"""Venue-specific translation. Downstream book logic uses only generic events."""
import json
from collections import defaultdict
from copy import deepcopy

from .model import Event, Level, RawRecord, parse_ns, scaled


def timestamp_ms(value):
    return None if value is None else scaled(str(value), 0) * 1_000_000


def optional_price(value):
    return None if value in (None, "") else scaled(value, 4)


class Normalizer:
    def __init__(self):
        self.sequences: dict[str, int] = {}
        self.members: dict[str, list[str]] = {}
        self.captures: dict[str, int] = {}

    def clone(self):
        return deepcopy(self)

    def checkpoint(self):
        return {"sequences": self.sequences.copy(), "members": deepcopy(self.members), "captures": self.captures.copy()}

    @classmethod
    def restore(cls, data):
        result = cls()
        result.sequences = data["sequences"].copy()
        result.members = deepcopy(data["members"])
        result.captures = data.get("captures", {}).copy()
        return result

    def apply(self, raw: RawRecord) -> list[Event]:
        capture_scope = f"{raw.connection_id}:{raw.connection_epoch}"
        if raw.counter <= self.captures.get(capture_scope, -1):
            return []
        payload = json.loads(raw.payload, parse_float=str)
        messages = payload if isinstance(payload, list) else [payload]
        events = []
        for message in messages:
            if not isinstance(message, dict):
                raise ValueError("message_object_required")
            if "_control" in message:
                for instrument in sorted(raw.instruments):
                    events.append(self._event(raw, instrument, "control", status=message["_control"]))
            elif raw.venue == "polymarket":
                events.extend(self._polymarket(raw, message))
            else:
                events.extend(self._kalshi(raw, message))
        self.captures[capture_scope] = raw.counter
        return [event.model_copy(update={"subevent_index": i}) for i, event in enumerate(events)]

    def apply_safe(self, raw):
        candidate = self.clone()
        try:
            return candidate, candidate.apply(raw), []
        except (ValueError, KeyError, TypeError) as error:
            violation = {"reason": "parser_error", "error_type": type(error).__name__,
                         "source_topic": raw.topic, "source_partition": raw.partition,
                         "source_offset": raw.offset, "recv_ts_ns": raw.recv_ts_ns}
            controls = [self._event(raw, asset, "control", status="parser_error", continuity="broken")
                        .model_copy(update={"subevent_index": i}) for i, asset in enumerate(sorted(raw.instruments))]
            return self, controls, [violation]

    def _event(self, raw, instrument, kind, **extra):
        metadata = raw.instruments.get(instrument, {})
        return Event(
            venue=raw.venue, data_origin=raw.data_origin, market_id=str(metadata.get("market_id", instrument)),
            instrument_id=instrument, outcome=metadata.get("outcome", "yes"), event_kind=kind,
            source_topic=raw.topic, source_partition=raw.partition, source_offset=raw.offset,
            subevent_index=0, connection_epoch=raw.connection_epoch, connection_counter=raw.counter,
            recv_ts_ns=raw.recv_ts_ns, **extra,
        )

    def _polymarket(self, raw, message):
        kind = message.get("event_type")
        instrument = str(message.get("asset_id", ""))
        ts = timestamp_ms(message.get("timestamp"))
        if kind == "book":
            levels = [Level(side=side, price_e4=scaled(level["price"], 4), size_e6=scaled(level["size"], 6))
                      for side, field in (("bid", "bids"), ("ask", "asks")) for level in message[field]]
            return [self._event(raw, instrument, "snapshot", levels=levels, venue_ts_ns=ts)]
        if kind == "price_change":
            groups = defaultdict(dict)
            expected = {}
            for change in message["price_changes"]:
                instrument = str(change["asset_id"])
                if change["side"] not in ("BUY", "SELL"):
                    raise ValueError("invalid_side")
                side = "bid" if change["side"] == "BUY" else "ask"
                price = scaled(change["price"], 4)
                groups[instrument][side, price] = Level(
                    side=side, price_e4=price, size_e6=scaled(change["size"], 6))
                expected[instrument] = (optional_price(change.get("best_bid")),
                                        optional_price(change.get("best_ask")))
            return [self._event(raw, asset, "delta", levels=list(groups[asset].values()),
                                venue_ts_ns=ts, expected_bid_e4=expected[asset][0],
                                expected_ask_e4=expected[asset][1]) for asset in sorted(groups)]
        if kind == "last_trade_price":
            return [self._event(raw, instrument, "trade", trade_price_e4=scaled(message["price"], 4),
                                trade_size_e6=None if message.get("size") is None
                                else scaled(message["size"], 6), venue_ts_ns=ts)]
        if kind == "market_resolved":
            return [self._event(raw, str(asset), "status", status="closed", venue_ts_ns=ts)
                    for asset in message.get("assets_ids", message.get("asset_ids", []))]
        if kind == "tick_size_change":
            return [self._event(raw, instrument, "control", status="metadata_changed", venue_ts_ns=ts)]
        if kind == "best_bid_ask":
            # This notification can precede its depth delta. Validate tops inside price_change instead.
            return []
        return []

    def _kalshi(self, raw, message):
        kind = message.get("type")
        if kind not in ("orderbook_snapshot", "orderbook_delta", "trade", "ok", "unsubscribed"):
            return []
        data = message.get("msg", {})
        instrument = data.get("market_ticker", "")
        sid = str(message.get("sid", ""))
        scope = f"{raw.connection_id}:{raw.connection_epoch}:{sid}"
        seq = message.get("seq")
        events = []
        continuity = "unverified"
        if kind != "trade" and seq is not None:
            if isinstance(seq, bool) or not isinstance(seq, int) or seq < 0:
                raise ValueError("invalid_sequence")
            old = self.sequences.get(scope)
            members = sorted(set(self.members.get(scope, [])) | set(raw.instruments) | ({instrument} if instrument else set()))
            self.members[scope] = members
            if old is not None and seq <= old:
                return [self._event(raw, asset, "control", status="sequence_regression",
                                    continuity="broken", subscription_id=sid, venue_seq=seq)
                        for asset in members]
            if old is not None and seq != old + 1:
                events.extend(self._event(raw, asset, "control", status="sequence_gap",
                                          continuity="broken", subscription_id=sid, venue_seq=seq)
                              for asset in members)
            self.sequences[scope] = seq
            continuity = "checked"
        if kind in ("ok", "unsubscribed"):
            return events
        if data.get("ts_ms") is not None:
            ts = timestamp_ms(data["ts_ms"])
        elif data.get("ts"):
            ts = parse_ns(data["ts"])
        else:
            ts = timestamp_ms(message.get("sending_ts_ms"))
        common = {"venue_ts_ns": ts, "venue_seq": seq, "subscription_id": sid, "continuity": continuity}
        if kind == "orderbook_snapshot":
            levels = []
            for field, side in (("yes_dollars_fp", "bid"), ("no_dollars_fp", "ask")):
                for price, quantity in data[field]:
                    p = scaled(price, 4)
                    levels.append(Level(side=side, price_e4=p if side == "bid" else 10_000 - p,
                                        size_e6=scaled(quantity, 6)))
            events.append(self._event(raw, instrument, "snapshot", levels=levels, **common))
        elif kind == "orderbook_delta":
            if data["side"] not in ("yes", "no"):
                raise ValueError("invalid_side")
            side = "bid" if data["side"] == "yes" else "ask"
            price = scaled(data["price_dollars"], 4)
            level = Level(side=side, price_e4=price if side == "bid" else 10_000 - price,
                          size_e6=scaled(data["delta_fp"], 6), operation="ADD_SIZE")
            events.append(self._event(raw, instrument, "delta", levels=[level], **common))
        else:
            events.append(self._event(raw, instrument, "trade", trade_price_e4=scaled(data["yes_price_dollars"], 4),
                                      trade_size_e6=scaled(data["count_fp"], 6), **common))
        return events
