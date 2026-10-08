from pmplatform.engine import BookEngine
from pmplatform.model import RawRecord, canonical
from pmplatform.normalize import Normalizer


def raw(message, venue="polymarket", offset=0, **extra):
    return RawRecord.capture(canonical(message), venue=venue, topic=f"raw.{venue}", partition=0,
                             offset=offset, recv_ts_ns=1_000_000_000 + offset, connection_epoch=1,
                             connection_id="connection", counter=offset, **extra)


def test_multi_asset_batch_and_absolute_size():
    message = {"event_type": "price_change", "timestamp": "1000", "price_changes": [
        {"asset_id": "2", "price": "0.4", "size": "7.123456", "side": "BUY"},
        {"asset_id": "1", "price": "0.6", "size": "0", "side": "SELL"}]}
    events = Normalizer().apply(raw(message))
    assert [e.instrument_id for e in events] == ["1", "2"]
    assert events[1].levels[0].size_e6 == 7_123_456
    assert events[1].levels[0].operation == "SET_SIZE"
    assert events[1].recv_ts_ns == 1_000_000_000


def test_scope_gap_invalidates_all_members():
    normalizer = Normalizer()
    snapshot = {"type": "orderbook_snapshot", "sid": 2, "seq": 1,
                "msg": {"market_ticker": "A", "yes_dollars_fp": [["0.4", "10.00"]],
                        "no_dollars_fp": [["0.3", "20.00"]]}}
    events = normalizer.apply(raw(snapshot, "kalshi", instruments={"A": {}, "B": {}}))
    assert events[0].levels[1].price_e4 == 7000
    delta = {"type": "orderbook_delta", "sid": 2, "seq": 3,
             "msg": {"market_ticker": "B", "side": "yes", "price_dollars": "0.4", "delta_fp": "-5.00"}}
    events = normalizer.apply(raw(delta, "kalshi", 1, instruments={"A": {}, "B": {}}))
    assert [e.instrument_id for e in events[:2]] == ["A", "B"]
    assert all(e.status == "sequence_gap" for e in events[:2])
    assert events[-1].levels[0].operation == "ADD_SIZE"
    assert events[-1].levels[0].size_e6 == -5_000_000


def test_interleaved_markets_do_not_make_false_sequence_gaps():
    n = Normalizer()
    for offset, ticker in enumerate(["A", "B", "A", "B"]):
        message = {"type": "orderbook_snapshot", "sid": 9, "seq": offset + 1,
                   "msg": {"market_ticker": ticker, "yes_dollars_fp": [], "no_dollars_fp": []}}
        assert len(n.apply(raw(message, "kalshi", offset, instruments={"A": {}, "B": {}}))) == 1


def test_republished_capture_deduplicates_across_broker_offsets():
    n = Normalizer()
    message = {"event_type": "book", "asset_id": "1", "bids": [], "asks": []}
    first = raw(message)
    assert len(n.apply(first)) == 1
    restored = Normalizer.restore(n.checkpoint())
    assert restored.apply(first.model_copy(update={"offset": 100})) == []


def test_command_response_advances_sequence_and_new_members_are_protected():
    n = Normalizer()
    snapshot = {"type": "orderbook_snapshot", "sid": 9, "seq": 1,
                "msg": {"market_ticker": "A", "yes_dollars_fp": [], "no_dollars_fp": []}}
    n.apply(raw(snapshot, "kalshi", instruments={"A": {}}))
    assert n.apply(raw({"type": "ok", "sid": 9, "seq": 2, "msg": {}}, "kalshi", 1)) == []
    snapshot["seq"] = 3
    assert len(n.apply(raw(snapshot, "kalshi", 2, instruments={"A": {}, "B": {}}))) == 1
    snapshot["seq"] = 5
    events = n.apply(raw(snapshot, "kalshi", 3, instruments={"A": {}, "B": {}}))
    assert {e.instrument_id for e in events if e.status == "sequence_gap"} == {"A", "B"}


def test_top_notification_can_arrive_before_its_depth_delta():
    normalizer, engine = Normalizer(), BookEngine()
    snapshot = {"event_type": "book", "asset_id": "1", "timestamp": "1000",
                "bids": [{"price": "0.21", "size": "10"}], "asks": [{"price": "0.32", "size": "10"}]}
    engine.apply(normalizer.apply(raw(snapshot))[0])
    notification = raw({"event_type": "best_bid_ask", "asset_id": "1", "timestamp": "1001",
                        "best_bid": "0.22", "best_ask": "0.32"}, offset=1)
    assert b"best_bid_ask" in notification.payload
    assert normalizer.apply(notification) == []
    assert engine.books["polymarket:1"].state()["bids"][0]["price_e4"] == 2100
    delta = {"event_type": "price_change", "timestamp": "1001", "price_changes": [
        {"asset_id": "1", "price": "0.22", "size": "36", "side": "BUY", "best_bid": "0.22", "best_ask": "0.32"}]}
    state, errors = engine.apply(normalizer.apply(raw(delta, offset=2))[0])
    assert not errors and state["status"] == "live"
    assert state["bids"][0] == {"price_e4": 2200, "size_e6": 36000000}


def test_reported_top_in_depth_delta_still_detects_mismatch():
    normalizer, engine = Normalizer(), BookEngine()
    snapshot = {"event_type": "book", "asset_id": "1", "timestamp": "1000",
                "bids": [{"price": "0.21", "size": "10"}], "asks": [{"price": "0.32", "size": "10"}]}
    engine.apply(normalizer.apply(raw(snapshot))[0])
    delta = {"event_type": "price_change", "timestamp": "1001", "price_changes": [
        {"asset_id": "1", "price": "0.22", "size": "36", "side": "BUY", "best_bid": "0.23", "best_ask": "0.32"}]}
    state, errors = engine.apply(normalizer.apply(raw(delta, offset=1))[0])
    assert state["status"] == "stale"
    assert [error["reason"] for error in errors] == ["reported_top_mismatch"]
