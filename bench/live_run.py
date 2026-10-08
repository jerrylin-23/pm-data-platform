"""Isolated local live benchmark. It never reads or writes the main app data."""
import argparse
import asyncio
import hashlib
import json
import os
import platform
import resource
import time
from collections import Counter
from pathlib import Path

import httpx
import uvicorn
import websockets

from pmplatform.engine import BookEngine
from pmplatform.model import canonical
from pmplatform.resolver import Resolver
from pmplatform.server import Runtime, make_app


def summary(values):
    ordered = sorted(values)
    if not ordered:
        return {"samples": 0}
    return {"samples": len(ordered), "mean": sum(ordered) / len(ordered),
            **{f"p{p}": ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p / 100))]
               for p in (50, 95, 99)}, "max": ordered[-1]}


def state_hash(states):
    return hashlib.sha256(canonical(states)).hexdigest()


def disk(root):
    files = [p for p in root.rglob("*") if p.is_file()]
    return {"files": len(files), "logical_bytes": sum(p.stat().st_size for p in files),
            "allocated_bytes": sum(p.stat().st_blocks * 512 for p in files)}


def choose_markets():
    cases = (("mlb-tb-nyy-2026-10-07", "sports"),
             ("elon-musk-of-tweets-october-2-october-9-2026", "ranges"),
             ("bitcoin-up-or-down-on-october-8-2026", "up_down"))
    markets, sources = [], []
    resolver = Resolver()
    for slug, kind in cases:
        url = f"https://polymarket.com/event/{slug}"
        available = resolver.resolve(url)
        unique = {m["market_id"]: m for m in available}
        ordered = sorted(unique.values(), key=lambda m: float(m["volume_24h"]), reverse=True)
        if kind == "sports":
            selected = [m for group in ("moneyline", "spreads", "totals")
                        for m in [x for x in ordered if x["market_type"] == group][:2]]
        else:
            selected = ordered[:2]
        ids = {m["market_id"] for m in selected}
        markets.extend(m for m in available if m["market_id"] in ids)
        sources.append({"url": url, "open_markets_at_lookup": len(unique),
                        "selected_markets": len(ids), "format": kind})
    return markets, sources


async def live(args):
    root = Path(args.root)
    if root.exists() and any(root.iterdir()):
        raise ValueError("Benchmark root must be empty.")
    root.mkdir(parents=True, exist_ok=True)
    os.environ.pop("KALSHI_KEY_ID", None)
    markets, sources = await asyncio.to_thread(choose_markets)
    app = make_app(root, demo=False, pairs_path=root / "no-pairs.yml")
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning"))
    launched = time.perf_counter()
    serving = asyncio.create_task(server.serve())
    while not server.started:
        if serving.done():
            await serving
            raise RuntimeError("Benchmark server did not start.")
        await asyncio.sleep(.01)
    runtime = app.state.runtime
    empty_start_seconds = time.perf_counter() - launched
    clean = {"archive_raw_rows": runtime.archive.count("raw"),
             "archive_event_rows": runtime.archive.count("events"),
             "saved_instruments": len(runtime.subscriptions.list()), "books": len(runtime.states)}
    assert not any(clean.values()) and not runtime.demo
    counters, violations, payload_types = Counter(), Counter(), Counter()
    process_ms, capture_to_processed_ms, stage_ms = [], [], []
    api_ms, websocket_ms, observed_book_ms, samples, versions = [], [], [], [], {}
    original_stage = BookEngine.stage

    def timed_stage(engine, events):
        began = time.perf_counter_ns()
        result = original_stage(engine, events)
        stage_ms.append((time.perf_counter_ns() - began) / 1e6)
        counters["normalized_events"] += len(events)
        counters["book_states"] += len(result[1])
        violations.update(error["reason"] for error in result[2])
        return result

    BookEngine.stage = timed_stage
    collector = next(c for c in runtime.collectors if c.venue == "polymarket")
    original_sink = collector.sink

    async def timed_sink(raw):
        began = time.perf_counter_ns()
        await original_sink(raw)
        finished = time.time_ns()
        process_ms.append((time.perf_counter_ns() - began) / 1e6)
        capture_to_processed_ms.append((finished - raw.recv_ts_ns) / 1e6)
        counters["raw_frames"] += 1
        counters["payload_bytes"] += len(raw.payload)
        message = json.loads(raw.payload)
        for item in message if isinstance(message, list) else [message]:
            payload_types[item.get("event_type", item.get("_control", "other"))] += 1

    collector.sink = timed_sink
    expected = len(markets)
    runtime.subscriptions.update(markets)
    first_snapshots = None
    began = time.monotonic()
    started_ns = time.time_ns()
    cpu_before = resource.getrusage(resource.RUSAGE_SELF)

    async def watch_view():
        async with websockets.connect(f"ws://127.0.0.1:{args.port}/ws") as socket:
            async for payload in socket:
                received = time.time_ns()
                view = json.loads(payload)
                websocket_ms.append((received - view["server_ts_ns"]) / 1e6)
                for book in view["books"]:
                    cursor = book["epoch"], book["version"]
                    if book["status"] == "live" and cursor != versions.get(book["key"]):
                        observed_book_ms.append((received - book["recv_ts_ns"]) / 1e6)
                    versions[book["key"]] = cursor

    watcher = asyncio.create_task(watch_view())
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            while time.monotonic() - began < args.seconds:
                requested = time.perf_counter_ns()
                try:
                    response = await client.get(f"http://127.0.0.1:{args.port}/api/state")
                    response.raise_for_status()
                    state = response.json()
                    api_ms.append((time.perf_counter_ns() - requested) / 1e6)
                    statuses = dict(Counter(book["status"] for book in state["books"]))
                    snapshots = sum(b.get("snapshot_ts_ns", 0) >= started_ns for b in state["books"])
                    if first_snapshots is None and snapshots == expected:
                        first_snapshots = time.monotonic() - began
                    samples.append({"elapsed_seconds": time.monotonic() - began, "statuses": statuses,
                                    "expected_instruments": expected, "snapshot_books": snapshots,
                                    "api_ms": api_ms[-1]})
                except httpx.HTTPError as error:
                    counters["api_errors"] += 1
                    samples.append({"elapsed_seconds": time.monotonic() - began,
                                    "api_error": type(error).__name__})
                await asyncio.sleep(1)
    finally:
        elapsed = time.monotonic() - began
        cpu_after = resource.getrusage(resource.RUSAGE_SELF)
        watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)
        server.should_exit = True
        await serving
        BookEngine.stage = original_stage
    report = {"checked_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
              "data_origin": "live", "pipeline": "local durable capture, no Kafka", "initial_state": clean,
              "sources": sources, "elapsed_seconds": elapsed, "tracked_markets": expected // 2,
              "native_books": expected, "first_all_snapshots_seconds": first_snapshots,
              "counts": dict(counters), "payload_types": dict(payload_types), "violations": dict(violations),
              "raw_frames_per_second": counters["raw_frames"] / elapsed,
              "book_states_per_second": counters["book_states"] / elapsed,
              "process_ms": summary(process_ms), "capture_to_processed_ms": summary(capture_to_processed_ms),
              "book_stage_ms": summary(stage_ms), "api_response_ms": summary(api_ms),
              "server_snapshot_to_ws_observer_ms": summary(websocket_ms),
              "observed_latest_book_receive_to_view_ms": summary(observed_book_ms),
              "samples": samples, "disk": disk(root), "final_state_sha256": state_hash(runtime.states),
              "cpu_seconds": (cpu_after.ru_utime + cpu_after.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime),
              "cpu_percent_one_core": 100 * (cpu_after.ru_utime + cpu_after.ru_stime - cpu_before.ru_utime - cpu_before.ru_stime) / elapsed,
              "peak_process_rss_bytes": cpu_after.ru_maxrss if platform.system() == "Darwin" else cpu_after.ru_maxrss * 1024,
              "empty_app_start_seconds_after_imports": empty_start_seconds,
              "limitations": ["Main dashboard and desktop remained active.",
                              "Stage and processing timings include benchmark probes.",
                              "View latency samples cover only latest changed live books seen by the observer.",
                              "Record totals include shutdown control records.",
                              "Source arrival rate is observed load, not maximum capacity."]}
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k not in ("samples", "sources", "payload_types")}, indent=2), flush=True)


def recover(args):
    before_disk = disk(Path(args.root))
    began = time.perf_counter()
    runtime = Runtime(args.root, demo=False, pairs_path=Path(args.root) / "no-pairs.yml")
    elapsed = time.perf_counter() - began
    result = {"seconds": elapsed, "raw_rows": runtime.archive.count("raw"),
              "event_rows": runtime.archive.count("events"), "books": len(runtime.states),
              "state_sha256": state_hash(runtime.states), "disk_before_recovery": before_disk,
              "recovery_raw_frames_per_second": runtime.archive.count("raw") / elapsed}
    asyncio.run(runtime.close())
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--port", type=int, default=18768)
    parser.add_argument("--recover", action="store_true")
    options = parser.parse_args()
    recover(options) if options.recover else asyncio.run(live(options))
