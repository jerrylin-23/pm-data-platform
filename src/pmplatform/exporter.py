import csv
import hashlib
import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .model import canonical, decimal_text, iso_ns
from .replay import replay

EVENT_COLUMNS = ["event_id", "venue", "market_id", "instrument_id", "outcome", "event_kind", "batch_index",
                 "side", "operation", "price", "size", "recv_ts_ns", "recv_iso", "venue_ts_ns"]


def csv_bytes(rows, fields):
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def flat_events(events):
    rows = []
    for event in events:
        base = {"event_id": event.event_id, "venue": event.venue, "market_id": event.market_id,
                "instrument_id": event.instrument_id, "outcome": event.outcome, "event_kind": event.event_kind,
                "recv_ts_ns": event.recv_ts_ns, "recv_iso": iso_ns(event.recv_ts_ns),
                "venue_ts_ns": event.venue_ts_ns}
        if event.levels:
            rows.extend({**base, "batch_index": i, "side": level.side, "operation": level.operation,
                         "price": decimal_text(level.price_e4, 4), "size": decimal_text(level.size_e6, 6)}
                        for i, level in enumerate(event.levels))
        else:
            rows.append({**base, "batch_index": 0, "side": "", "operation": "",
                         "price": "" if event.trade_price_e4 is None else decimal_text(event.trade_price_e4, 4),
                         "size": "" if event.trade_size_e6 is None else decimal_text(event.trade_size_e6, 6)})
    return rows


def bars(events, interval_ns):
    result = {}
    for event in events:
        if event.event_kind != "trade" or event.trade_size_e6 is None:
            continue
        timestamp = event.venue_ts_ns if event.venue_ts_ns is not None else event.recv_ts_ns
        bucket = timestamp // interval_ns * interval_ns
        price, size = event.trade_price_e4, event.trade_size_e6
        key = event.key, bucket
        if key not in result:
            result[key] = [price, price, price, price, size, event]
        else:
            row = result[key]
            row[1], row[2], row[3], row[4] = max(row[1], price), min(row[2], price), price, row[4] + size
    return [{"datetime": iso_ns(bucket), "open": decimal_text(row[0], 4),
             "high": decimal_text(row[1], 4), "low": decimal_text(row[2], 4),
             "close": decimal_text(row[3], 4), "volume": decimal_text(row[4], 6), "openinterest": 0}
            for (_, bucket), row in sorted(result.items(), key=lambda r: (r[0][1], r[0][0]))]


def export_events(all_events, path, fmt, start=0, end=(1 << 63) - 1, venue=None, market=None,
                  interval="1m", every_events=1, checkpoint=None):
    path = Path(path)
    if every_events < 1:
        raise ValueError("every_events_must_be_positive")
    all_events = [e for e in all_events if (venue is None or e.venue == venue)
                  and (market is None or e.market_id == market or e.instrument_id == market)]
    checkpoint = {k: data for k, data in (checkpoint or {}).items()
                  if (venue is None or data["state"]["venue"] == venue)
                  and (market is None or market in (data["state"]["market_id"], data["state"]["instrument_id"]))}
    result = replay(all_events, start, end, checkpoint=checkpoint)
    events = result.selected_events
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "jsonl":
        path.write_bytes(b"".join(canonical(e.model_dump(mode="json")) + b"\n" for e in events))
        rows = len(events)
    elif fmt in ("csv", "parquet"):
        flat = flat_events(events)
        rows = len(flat)
        if fmt == "csv":
            path.write_bytes(csv_bytes(flat, EVENT_COLUMNS))
        else:
            schema = pa.schema([(name, pa.int64() if name in ("recv_ts_ns", "venue_ts_ns", "batch_index")
                                 else pa.string()) for name in EVENT_COLUMNS])
            pq.write_table(pa.Table.from_pylist(flat, schema=schema), path, compression="zstd",
                           use_dictionary=False, version="2.6")
    elif fmt == "ohlcv-csv":
        intervals = {"1s": 1_000_000_000, "1m": 60_000_000_000, "1h": 3_600_000_000_000}
        if interval not in intervals:
            raise ValueError("unsupported_bar_interval")
        if len({e.key for e in events if e.event_kind == "trade"}) > 1:
            raise ValueError("OHLCV_export_requires_one_instrument")
        data = bars(events, intervals[interval])
        rows = len(data)
        path.write_bytes(csv_bytes(data, ["datetime", "open", "high", "low", "close", "volume", "openinterest"]))
    elif fmt == "l2-csv":
        data = []
        for i, state in enumerate(result.states):
            if i % every_events or state["status"] != "live":
                continue
            for name, side in (("bids", "bid"), ("asks", "ask")):
                for level, entry in enumerate(state[name], 1):
                    data.append({"event_id": state["event_id"], "instrument_id": state["instrument_id"],
                                 "recv_ts_ns": state["recv_ts_ns"], "recv_iso": iso_ns(state["recv_ts_ns"]),
                                 "side": side, "level": level, "price": decimal_text(entry["price_e4"], 4),
                                 "size": decimal_text(entry["size_e6"], 6)})
        rows = len(data)
        path.write_bytes(csv_bytes(data, ["event_id", "instrument_id", "recv_ts_ns", "recv_iso", "side", "level", "price", "size"]))
    else:
        raise ValueError("unsupported_export_format")
    manifest = {"schema_version": 1, "format": fmt, "venue": venue, "market": market,
                "from_ns": start, "to_ns": end, "row_count": rows,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "book_state_sha256": result.sha256,
                "initial_checkpoint": result.initial_checkpoint,
                "bar_interval": interval if fmt == "ohlcv-csv" else None,
                "every_events": every_events if fmt == "l2-csv" else None}
    path.with_name(path.name + ".manifest.json").write_bytes(canonical(manifest) + b"\n")
    return manifest
