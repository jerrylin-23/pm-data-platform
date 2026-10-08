import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from pmplatform import _book
from pmplatform.engine import BookEngine
from pmplatform.model import Level


def native(side, price, size, operation="SET_SIZE"):
    return _book.Level(getattr(_book.Side, side), price, size, getattr(_book.Operation, operation))


@given(st.dictionaries(st.integers(0, 4999), st.integers(1, 10**12), max_size=40),
       st.dictionaries(st.integers(5001, 10000), st.integers(1, 10**12), max_size=40),
       st.lists(st.tuples(st.booleans(), st.integers(0, 4999), st.integers(0, 10**12)), max_size=80))
@settings(max_examples=100)
def test_native_matches_reference(bids, asks, changes):
    book = _book.Book()
    book.snapshot([native("BID", p, q) for p, q in bids.items()] +
                  [native("ASK", p, q) for p, q in asks.items()])
    bids, asks = bids.copy(), asks.copy()
    for is_bid, price, size in changes:
        side = "BID" if is_bid else "ASK"
        price = price if is_bid else price + 5001
        target = bids if is_bid else asks
        book.update([native(side, price, size)])
        if size:
            target[price] = size
        else:
            target.pop(price, None)
    assert book.depth(_book.Side.BID, 10001) == sorted(bids.items(), reverse=True)
    assert book.depth(_book.Side.ASK, 10001) == sorted(asks.items())
    final = _book.Book()
    final.snapshot([native("BID", p, q) for p, q in bids.items()] +
                   [native("ASK", p, q) for p, q in asks.items()])
    assert final.depth(_book.Side.BID, 10001) == book.depth(_book.Side.BID, 10001)


@pytest.mark.parametrize("levels,error", [
    ([native("BID", 6000, 1)], "crossed_book"),
    ([native("BID", 4000, -101, "ADD_SIZE")], "negative_result"),
    ([native("BID", 4000, -1)], "negative_size"),
    ([native("BID", 4000, 1), native("BID", 4000, 2)], "duplicate_level"),
    ([native("BID", 10001, 1)], "price_out_of_range"),
    ([native("ASK", 6000, 1), native("BID", 7000, 1)], "crossed_book"),
])
def test_invalid_batch_is_atomic(levels, error):
    b = _book.Book()
    b.snapshot([native("BID", 4000, 100), native("ASK", 6000, 200)])
    with pytest.raises(ValueError, match=error):
        b.update(levels)
    assert b.depth(_book.Side.BID, 10001) == [(4000, 100)]
    assert b.depth(_book.Side.ASK, 10001) == [(6000, 200)]


def test_snapshot_replaces_and_invalid_snapshot_preserves():
    b = _book.Book()
    b.snapshot([native("BID", 10, 100)])
    with pytest.raises(ValueError):
        b.snapshot([native("BID", 7000, 100), native("ASK", 6000, 100)])
    assert b.best_bid() == 10
    b.snapshot([native("ASK", 9000, 100)])
    assert b.best_bid() is None
    b.snapshot([])
    assert b.best_ask() is None


def test_signed_change_and_overflow():
    b = _book.Book()
    b.snapshot([native("BID", 1, (1 << 63) - 1)])
    with pytest.raises(OverflowError):
        b.update([native("BID", 1, 1, "ADD_SIZE")])
    b.update([native("BID", 1, -((1 << 63) - 1), "ADD_SIZE")])
    assert b.best_bid() is None
    with pytest.raises(ValueError):
        b.update([native("BID", 1, -(1 << 63), "ADD_SIZE")])


def test_epoch_duplicate_checkpoint_and_transaction_abort(event):
    engine = BookEngine()
    engine.apply(event())
    change = event("delta", offset=1, levels=[Level(side="bid", price_e4=4000, size_e6=10, operation="ADD_SIZE")])
    working, states, errors = engine.stage([change])
    assert not errors
    assert engine.books[change.key].book.quantity(_book.Side.BID, 4000) == 100_000_000
    assert working.books[change.key].book.quantity(_book.Side.BID, 4000) == 100_000_010
    engine = BookEngine.restore(working.checkpoint())
    assert engine.apply(change) == (None, [])
    state, errors = engine.apply(event("delta", offset=2, epoch=2, levels=[]))
    assert state["status"] == "stale"
    assert errors
    state, errors = engine.apply(event("snapshot", offset=3, epoch=2))
    assert state["status"] == "live"
    assert engine.apply(event("snapshot", offset=4, epoch=1))[0] is None


def test_trade_does_not_modify_book(event):
    engine = BookEngine()
    engine.apply(event())
    before = engine.books["test:i"].book.depth(_book.Side.BID, 10)
    engine.apply(event("trade", offset=1, levels=[], trade_price_e4=5000, trade_size_e6=10_000_000))
    assert engine.books["test:i"].book.depth(_book.Side.BID, 10) == before


def test_time_regression_excludes_book_until_fresh_snapshot(event):
    engine = BookEngine()
    engine.apply(event())
    change = event("trade", offset=1, levels=[], trade_price_e4=5000, trade_size_e6=1)
    state, errors = engine.apply(change.model_copy(update={"recv_ts_ns": 1}))
    assert state["status"] == "stale"
    assert state["continuity"] == "broken"
    assert errors[0]["reason"] == "receive_time_regression"
    state, _ = engine.apply(event(offset=2))
    assert state["status"] == "live"
