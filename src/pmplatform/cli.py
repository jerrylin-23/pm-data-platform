import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

from .archive import Archive
from .model import Event, RawRecord, canonical, parse_ns
from .replay import import_events, merge_partitions, normalized_raw, replay


def source_events(args):
    if getattr(args, "input", None):
        return import_events(args.input)
    archive = Archive(args.archive)
    try:
        if getattr(args, "raw", False):
            records = [RawRecord.model_validate(data) for data in archive.rows("raw")]
            return list(normalized_raw(records)), {}
        events = [Event.model_validate(data) for data in archive.rows("events")]
        return list(merge_partitions(events)), {}
    finally:
        archive.close()


def window(parser):
    parser.add_argument("--from", dest="start", default="0", help="UTC ISO timestamp or nanoseconds, inclusive")
    parser.add_argument("--to", dest="end", default=str((1 << 63) - 1), help="UTC ISO timestamp or nanoseconds, exclusive")


def parser():
    root = argparse.ArgumentParser(prog="pm", description="Public market data, C++ books, and repeatable replay")
    root.add_argument("--data-dir", default=os.getenv("PM_DATA_DIR", "data"))
    root.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Create Kafka topics")
    demo = commands.add_parser("demo", help="Create an explicitly synthetic fixture and archive")
    demo.add_argument("--seconds", type=int, default=600)
    demo.add_argument("--out", default="data/demo")
    record = commands.add_parser("capture", help="Record tracked live markets for a bounded duration")
    record.add_argument("--seconds", type=int, default=60)
    for name in ("serve", "stack"):
        serve = commands.add_parser(name, help="Start dashboard" if name == "serve" else "Start all Kafka workers and dashboard")
        serve.add_argument("--host", default="127.0.0.1")
        serve.add_argument("--port", type=int, default=8000)
        serve.add_argument("--demo", action="store_true", default=os.getenv("PM_DEMO") == "1")
        serve.add_argument("--pairs", default="pairs.yml")
    worker = commands.add_parser("worker")
    worker.add_argument("service", choices=["normalizer", "builder", "archiver", "gaps"])
    worker.add_argument("--metrics-port", type=int, default=0)
    worker.add_argument("--pairs", default="pairs.yml")
    worker.add_argument("--worker-id")
    track = commands.add_parser("track")
    tracking = track.add_subparsers(dest="track_command", required=True)
    add = tracking.add_parser("add")
    add.add_argument("url")
    add.add_argument("--all", action="store_true")
    add.add_argument("--market", action="append")
    search = tracking.add_parser("search")
    search.add_argument("keyword")
    search.add_argument("--venue", choices=["polymarket", "kalshi"])
    tracking.add_parser("list")
    remove = tracking.add_parser("remove")
    remove.add_argument("identifier")
    printing = commands.add_parser("book", help="Print the latest top of book")
    printing.add_argument("--key")
    replay_parser = commands.add_parser("replay")
    replay_parser.add_argument("--archive", default="data/demo/archive")
    replay_parser.add_argument("--input")
    replay_parser.add_argument("--raw", action="store_true")
    replay_parser.add_argument("--speed", default="max")
    replay_parser.add_argument("--out", default="data/replay/book.state.jsonl")
    window(replay_parser)
    exporting = commands.add_parser("export")
    exporting.add_argument("--archive", default="data/demo/archive")
    exporting.add_argument("--input")
    exporting.add_argument("--venue")
    exporting.add_argument("--market")
    exporting.add_argument("--format", required=True, choices=["jsonl", "csv", "parquet", "ohlcv-csv", "l2-csv"])
    exporting.add_argument("--out", required=True)
    exporting.add_argument("--interval", choices=["1s", "1m", "1h"], default="1m")
    exporting.add_argument("--every-events", type=int, default=1)
    window(exporting)
    return root


def output(data):
    print(json.dumps(data, indent=2, ensure_ascii=False))


def track(args):
    from .resolver import Resolver
    from .subscriptions import Subscriptions
    store = Subscriptions(Path(args.data_dir) / "subscriptions.sqlite", int(os.getenv("PM_MAX_MARKETS", "200")))
    try:
        if args.track_command == "search":
            output(Resolver().search(args.keyword, args.venue))
            return
        if args.track_command == "list":
            output(store.list())
            return
        if args.track_command == "remove":
            result = store.remove(args.identifier)
        else:
            markets = Resolver().resolve(args.url)
            if args.market:
                markets = [m for m in markets if m["market_id"] in args.market]
                if not markets:
                    raise ValueError("No selected open markets.")
            elif len({m["market_id"] for m in markets}) > 1 and not args.all:
                output({"markets": markets, "message": "Choose --market ID, or --all, to track this event."})
                return
            result = store.update(markets)
        if args.bootstrap:
            from .kafka import Sender
            store.publish(Sender(args.bootstrap))
        output(result)
    finally:
        store.close()


async def serve(args):
    import uvicorn

    from .metrics import serve_metrics
    from .server import make_app
    processes, logs = [], []
    root = Path(args.data_dir)
    root.mkdir(parents=True, exist_ok=True)
    bootstrap = args.bootstrap if args.command == "stack" else None
    if args.command == "stack":
        from .kafka import initialize
        if args.demo:
            import yaml

            from .demo import DEMO_PAIR
            from .gaps import load_pairs
            pairs = load_pairs(args.pairs) if Path(args.pairs).exists() else []
            if DEMO_PAIR not in pairs:
                pairs.append(DEMO_PAIR)
            args.pairs = str(root / "demo-pairs.yml")
            Path(args.pairs).write_text(yaml.safe_dump({"pairs": [vars(pair) for pair in pairs]}))
        bootstrap = bootstrap or "localhost:19092"
        initialize(bootstrap)
        (root / "logs").mkdir(exist_ok=True)
        for index, name in enumerate(("normalizer", "builder", "archiver", "gaps"), 1):
            log = (root / "logs" / f"{name}.log").open("a")
            logs.append(log)
            command = [sys.executable, "-m", "pmplatform", "--data-dir", str(root), "--bootstrap", bootstrap,
                       "worker", name, "--metrics-port", str(9100 + index), "--pairs", args.pairs]
            processes.append((command, subprocess.Popen(command, stdout=log, stderr=log), log))
    serve_metrics(9100)
    server = uvicorn.Server(uvicorn.Config(make_app(root, args.demo, bootstrap, args.pairs),
                                          host=args.host, port=args.port, log_level="info"))
    async def supervise():
        while not server.should_exit:
            for i, (command, process, log) in enumerate(processes):
                if process.poll() is not None:
                    print(f"Restarting {command[command.index('worker') + 1]} worker after exit {process.returncode}.", flush=True)
                    processes[i] = command, subprocess.Popen(command, stdout=log, stderr=log), log
            await asyncio.sleep(2)
    supervisor = asyncio.create_task(supervise())
    try:
        await server.serve()
    finally:
        supervisor.cancel()
        for _, process, _ in processes:
            process.terminate()
        for _, process, _ in processes:
            try:
                await asyncio.to_thread(process.wait, 5)
            except subprocess.TimeoutExpired:
                process.kill()
        for log in logs:
            log.close()


def main():
    args = parser().parse_args()
    try:
        if args.command == "track":
            track(args)
        elif args.command == "init":
            from .kafka import initialize
            initialize(args.bootstrap or "localhost:19092")
            output({"topics": "ready"})
        elif args.command == "worker":
            from . import kafka
            from .gaps import load_pairs
            from .metrics import serve_metrics
            serve_metrics(args.metrics_port)
            stop = threading.Event()
            signal.signal(signal.SIGTERM, lambda *_: stop.set())
            signal.signal(signal.SIGINT, lambda *_: stop.set())
            bootstrap = args.bootstrap or "localhost:19092"
            if args.service == "archiver":
                kafka.run_archiver(bootstrap, Path(args.data_dir) / "archive", stop)
            elif args.service == "gaps":
                kafka.run_gap_detector(bootstrap, load_pairs(args.pairs), stop)
            else:
                getattr(kafka, f"run_{'builder' if args.service == 'builder' else 'normalizer'}")(
                    bootstrap, args.worker_id or f"{args.service}-1", stop)
        elif args.command in ("serve", "stack"):
            asyncio.run(serve(args))
        elif args.command == "capture":
            from .server import Runtime
            async def capture():
                runtime = Runtime(args.data_dir)
                await runtime.start()
                try:
                    await asyncio.sleep(args.seconds)
                    output({"raw_rows": runtime.archive.count("raw"), "normalized_rows": runtime.archive.count("events"),
                            "books": len(runtime.states), "violations": runtime.violations})
                finally:
                    await runtime.close()
            asyncio.run(capture())
        elif args.command == "demo":
            from .demo import fixture
            root = Path(args.out)
            root.mkdir(parents=True, exist_ok=True)
            records = list(fixture(args.seconds))
            events = list(normalized_raw(records))
            result = replay(events)
            archive = Archive(root / "archive")
            try:
                raw_added, events_added = archive.append("raw", records), archive.append("events", events)
            finally:
                archive.close()
            (root / "raw.jsonl").write_bytes(b"".join(canonical(r.model_dump(mode="json")) + b"\n" for r in records))
            (root / "book.state.jsonl").write_bytes(result.output)
            summary = {"data_origin": "synthetic", "raw_rows": len(records), "events": len(events),
                       "book_state_sha256": result.sha256, "raw_rows_added": raw_added, "event_rows_added": events_added}
            (root / "manifest.json").write_bytes(canonical(summary) + b"\n")
            output(summary)
        elif args.command == "replay":
            events, checkpoint = source_events(args)
            result = replay(events, parse_ns(args.start), parse_ns(args.end), args.speed, checkpoint)
            path = Path(args.out)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(result.output)
            output({"states": len(result.states), "sha256": result.sha256, "violations": len(result.violations), "out": str(path)})
        elif args.command == "export":
            from .exporter import export_events
            events, checkpoint = source_events(args)
            output(export_events(events, args.out, args.format, parse_ns(args.start), parse_ns(args.end),
                                 args.venue, args.market, args.interval, args.every_events, checkpoint))
        elif args.command == "book":
            from .kafka import latest
            books = latest(args.bootstrap or "localhost:19092", "book.state")
            output(books.get(args.key, {}) if args.key else books)
    except (ValueError, FileNotFoundError) as error:
        print(f"Error: {error}", file=sys.stderr)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()
