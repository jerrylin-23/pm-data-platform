import hashlib

import backtrader as bt
import pandas as pd
import pytest

from pmplatform.archive import Archive
from pmplatform.exporter import export_events
from pmplatform.model import Level
from pmplatform.replay import import_events, merge_partitions, replay


def capture(event):
    return [event(), event("delta", offset=1, levels=[Level(side="bid", price_e4=4000, size_e6=30_000_000)]),
            event("trade", offset=2, levels=[], trade_price_e4=5000, trade_size_e6=7_123_456),
            event("trade", offset=3, levels=[], trade_price_e4=5500, trade_size_e6=2_000_000)]


def test_archive_idempotency_and_conflict(tmp_path, event):
    archive = Archive(tmp_path)
    events = capture(event)
    assert archive.append("events", events) == 4
    assert archive.append("events", events) == 0
    assert archive.count("events") == 4
    assert archive.rows("events")[2] == events[2].model_dump(mode="json")
    with pytest.raises(ValueError, match="conflict"):
        archive.append("events", [events[0].model_copy(update={"recv_ts_ns": 0})])
    archive.close()


@pytest.mark.parametrize("fmt", ["jsonl", "csv", "parquet", "ohlcv-csv", "l2-csv"])
def test_deterministic_exports(fmt, tmp_path, event):
    events = capture(event)
    a, b = tmp_path / f"a.{fmt}", tmp_path / f"b.{fmt}"
    first = export_events(events, a, fmt)
    second = export_events(events, b, fmt)
    assert a.read_bytes() == b.read_bytes()
    assert first == second
    assert first["sha256"] == hashlib.sha256(a.read_bytes()).hexdigest()


def test_replay_warmup_jsonl_roundtrip(tmp_path, event):
    events = capture(event)
    start = events[1].recv_ts_ns
    original = replay(events, start=start)
    assert original.sha256 == replay(events, start=start).sha256
    path = tmp_path / "events.jsonl"
    export_events(events, path, "jsonl", start=start)
    imported, initial = import_events(path)
    roundtrip = replay(imported, checkpoint=initial)
    assert roundtrip.sha256 == original.sha256
    with pytest.raises(ValueError, match="missing_start_state"):
        replay(events[1:])


def test_full_window_and_reexport_preserve_start_checkpoint(tmp_path, event):
    events = capture(event)
    path = tmp_path / "full.jsonl"
    export_events(events, path, "jsonl")
    imported, initial = import_events(path)
    assert initial == {}
    assert replay(imported, checkpoint=initial).sha256 == replay(events).sha256
    window = tmp_path / "window.jsonl"
    export_events(events, window, "jsonl", start=events[1].recv_ts_ns)
    imported, initial = import_events(window)
    copied = tmp_path / "copied.jsonl"
    manifest = export_events(imported, copied, "jsonl", checkpoint=initial)
    assert copied.read_bytes() == window.read_bytes()
    assert manifest["book_state_sha256"] == replay(events, start=events[1].recv_ts_ns).sha256


def test_partition_order_survives_clock_regression(event):
    events = [event(offset=0), event(offset=1).model_copy(update={"recv_ts_ns": 0})]
    assert [e.source_offset for e in merge_partitions(events)] == [0, 1]


def test_window_rejects_regression_that_would_lose_replay_context(event):
    events = [event(), event(offset=1).model_copy(update={"recv_ts_ns": 3_000_000_000}),
              event(offset=2).model_copy(update={"recv_ts_ns": 1_500_000_000}),
              event(offset=3).model_copy(update={"recv_ts_ns": 4_000_000_000})]
    assert replay(events).violations
    with pytest.raises(ValueError, match="window_start_crossed_by_clock_regression"):
        replay(events, start=2_000_000_000)


def test_ohlcv_loads_in_pandas_and_backtrader(tmp_path, event):
    path = tmp_path / "bars.csv"
    export_events(capture(event), path, "ohlcv-csv", interval="1s")
    frame = pd.read_csv(path, index_col="datetime", parse_dates=True)
    assert float(frame.iloc[0]["volume"]) == 9.123456
    cerebro = bt.Cerebro()
    cerebro.adddata(bt.feeds.PandasData(dataname=frame))
    assert cerebro.run()
