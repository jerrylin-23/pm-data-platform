from pmplatform.engine import BookEngine
from pmplatform.gaps import GapDetector, Pair
from pmplatform.model import Level


def pair_states(event):
    engine = BookEngine()
    a = event().model_copy(update={"instrument_id": "a"})
    b = event(offset=1, levels=[Level(side="bid", price_e4=7000, size_e6=1000),
                               Level(side="ask", price_e4=8000, size_e6=2000)]).model_copy(update={"instrument_id": "b"})
    return engine.apply(a)[0], engine.apply(b)[0]


def test_gap_has_lineage_and_stale_books_are_excluded(event):
    a, b = pair_states(event)
    detector = GapDetector([Pair("pair", "test:a", "test:b", fee_e4=100, threshold_e4=500)])
    assert detector.apply(a) == []
    alert = detector.apply(b)[0]
    assert alert["gap_e4"] == 900
    assert alert["source_event_ids"] == [a["event_id"], b["event_id"]]
    assert len(alert["config_sha256"]) == 64
    assert detector.apply({**b, "status": "stale"}) == []
    assert detector.apply({**b, "recv_ts_ns": a["recv_ts_ns"] + 16_000_000_000}) == []


def test_inverted_pair_uses_opposite_quote_sides(event):
    a, b = pair_states(event)
    detector = GapDetector([Pair("pair", "test:a", "test:b", invert_right=True, threshold_e4=0)])
    detector.apply(a)
    alerts = detector.apply(b)
    assert alerts[0]["buy_quote"] == "test:b"
    assert alerts[0]["ask_e4"] == 3000
