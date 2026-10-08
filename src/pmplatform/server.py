import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .archive import Archive
from .demo import DEMO_PAIR, frame_pair
from .engine import BookEngine
from .gaps import GapDetector, load_pairs
from .ingest import Collector
from .metrics import MESSAGES
from .model import RawRecord, canonical
from .normalize import Normalizer
from .replay import merge_partitions
from .resolver import Resolver
from .spool import Spool
from .subscriptions import Subscriptions


class TrackRequest(BaseModel):
    url: str = Field(max_length=1000)
    market_ids: list[str] | None = None
    all_markets: bool = False


class Runtime:
    def __init__(self, root, demo=False, bootstrap=None, pairs_path="pairs.yml"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.demo, self.bootstrap = demo, bootstrap
        self.archive = Archive(self.root / "archive")
        self.subscriptions = Subscriptions(self.root / "subscriptions.sqlite", int(os.getenv("PM_MAX_MARKETS", "200")))
        self.normalizer, self.engine = Normalizer(), BookEngine()
        self.states, self.alerts, self.violations = {}, [], []
        pairs = load_pairs(pairs_path) if Path(pairs_path).exists() else []
        if demo and DEMO_PAIR not in pairs:
            pairs.append(DEMO_PAIR)
        self.detector = GapDetector(pairs)
        self.stop = asyncio.Event()
        self.collectors = []
        self.tasks = []
        if not bootstrap:
            for raw in merge_partitions([RawRecord.model_validate(data) for data in self.archive.rows("raw")], raw=True):
                self.process(raw, recovery=True)

    def process(self, raw, recovery=False):
        if not recovery:
            self.archive.append("raw", [raw])
        candidate, events, parser_errors = self.normalizer.apply_safe(raw)
        self.violations.extend(parser_errors)
        working, states, errors = self.engine.stage(events)
        self.archive.append("events", events)
        self.normalizer, self.engine = candidate, working
        self.violations.extend(errors)
        for state in states:
            self.states[state["key"]] = state
            self.alerts.extend(self.detector.apply(state))
            if state["status"] == "stale":
                for collector in self.collectors:
                    if state["venue"] == collector.venue:
                        collector.request_resync(state["instrument_id"], state["recv_ts_ns"])
        self.alerts, self.violations = self.alerts[-100:], self.violations[-100:]

    async def consume(self, raw):
        if self.bootstrap:
            from .kafka import Sender
            if not hasattr(self, "sender"):
                self.sender = Sender(self.bootstrap)
            # Producer send is blocking; run it in a worker thread.
            await asyncio.to_thread(self.sender.send, raw.topic, raw.connection_id, canonical(raw.model_dump(mode="json")))
        else:
            self.process(raw)

    async def tracked(self):
        return self.subscriptions.list()

    async def start(self):
        if self.bootstrap:
            await self.sync_subscriptions()
            self.launch(self.control_loop(), "subscription control")
        for venue in ("polymarket", "kalshi"):
            if venue == "kalshi" and not (os.getenv("KALSHI_KEY_ID") and os.getenv("KALSHI_PRIVATE_KEY_PATH")):
                continue
            collector = Collector(venue, self.root, self.tracked, self.consume, self.stop)
            self.collectors.append(collector)
            self.launch(collector.run(), f"{venue} capture")
        if self.demo:
            self.launch(self.feed_demo(), "synthetic feed")
        if self.bootstrap:
            self.launch(self.broker_view(), "broker view")

    def launch(self, coroutine, name):
        task = asyncio.create_task(coroutine, name=name)
        def finished(completed):
            if not completed.cancelled() and completed.exception() is not None:
                error_type = type(completed.exception()).__name__
                self.violations.append({"reason": f"service_failed:{name}:{error_type}"})
                print(f"{name} stopped: {error_type}", flush=True)
        task.add_done_callback(finished)
        self.tasks.append(task)

    async def feed_demo(self):
        index = 0
        spool = Spool(self.root / "spool-demo.sqlite")
        epoch = spool.epoch()
        try:
            while pending := spool.pending(1000):
                for raw in pending:
                    await self.consume(raw)
                    spool.acknowledge(raw.offset)
            while not self.stop.is_set():
                for sample in frame_pair(index, time.time_ns(), epoch):
                    raw = spool.append(sample.payload, venue=sample.venue, data_origin="synthetic",
                                       topic=sample.topic if self.bootstrap else f"demo.{sample.topic}",
                                       recv_ts_ns=sample.recv_ts_ns, connection_epoch=epoch,
                                       connection_id=sample.connection_id, counter=sample.counter,
                                       instruments=sample.instruments)
                    await self.consume(raw)
                    spool.acknowledge(raw.offset)
                    MESSAGES.labels(sample.venue).inc()
                index += 1
                await asyncio.sleep(.5)
        finally:
            spool.close()

    async def sync_subscriptions(self):
        from .kafka import Sender, latest
        def publish():
            store = Subscriptions(self.root / "subscriptions.sqlite")
            try:
                store.publish(Sender(self.bootstrap))
            finally:
                store.close()
        await asyncio.to_thread(publish)
        records = await asyncio.to_thread(latest, self.bootstrap, "control.subscriptions")
        self.subscriptions.restore(records)
        resync = await asyncio.to_thread(latest, self.bootstrap, "control.resync")
        if not hasattr(self, "seen_resync"):
            self.seen_resync = set()
        for record in resync.values():
            if record["event_id"] in self.seen_resync:
                continue
            self.seen_resync.add(record["event_id"])
            for collector in self.collectors:
                if collector.venue == record["venue"]:
                    collector.request_resync(record["instrument_id"], record["recv_ts_ns"])

    async def control_loop(self):
        while not self.stop.is_set():
            await asyncio.sleep(2)
            await self.sync_subscriptions()

    async def broker_view(self):
        from .kafka import consumer, latest, topic_name
        self.states = await asyncio.to_thread(latest, self.bootstrap, "book.state")
        reader = consumer(self.bootstrap, f"pm-dashboard-{uuid.uuid4()}")
        reader.subscribe([topic_name(t) for t in ("book.history", "alerts.gaps", "dq.violations")])
        try:
            while not self.stop.is_set():
                message = await asyncio.to_thread(reader.poll, .5)
                if message is None or message.error():
                    continue
                data = json.loads(message.value())
                if message.topic() == topic_name("book.history"):
                    existing = self.states.get(data["key"])
                    if not existing or (data["epoch"], data["version"]) >= (existing["epoch"], existing["version"]):
                        self.states[data["key"]] = data
                elif message.topic() == topic_name("alerts.gaps"):
                    self.alerts = (self.alerts + [data])[-100:]
                else:
                    self.violations = (self.violations + [data])[-100:]
        finally:
            reader.close()

    def snapshot(self):
        tracked = self.subscriptions.list()
        by_key = {f"{m['venue']}:{m['instrument_id']}": m for m in tracked}
        states = [{**state, "title": by_key.get(key, {}).get("title", "Synthetic rate decision" if
                   state["data_origin"] == "synthetic" else state["market_id"]),
                   **{field: by_key.get(key, {})[field] for field in
                      ("parent_event_id", "event_title", "event_url", "market_label", "market_order", "market_type", "outcome_labels")
                      if field in by_key.get(key, {})},
                   "source_url": by_key.get(key, {}).get("source_url")}
                  for key, state in sorted(self.states.items())
                  if self.demo or state["data_origin"] != "synthetic"]
        alerts = [alert for alert in self.alerts if self.demo or "synthetic" not in alert["data_origin"]]
        return {"mode": "kafka" if self.bootstrap else "local", "demo": self.demo,
                "books": states, "tracked": tracked, "alerts": alerts[-30:],
                "violations": self.violations[-30:], "kalshi_ready": bool(os.getenv("KALSHI_KEY_ID") and os.getenv("KALSHI_PRIVATE_KEY_PATH")),
                "server_ts_ns": time.time_ns()}

    async def close(self):
        self.stop.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.archive.close()
        self.subscriptions.close()


def make_app(root, demo=False, bootstrap=None, pairs_path="pairs.yml"):
    @asynccontextmanager
    async def lifespan(app):
        app.state.runtime = Runtime(root, demo, bootstrap, pairs_path)
        await app.state.runtime.start()
        try:
            yield
        finally:
            await app.state.runtime.close()

    app = FastAPI(title="Prediction Market Data Platform", lifespan=lifespan)
    static = Path(__file__).parent / "static"
    app.mount("/assets", StaticFiles(directory=static), name="assets")

    @app.get("/")
    async def index():
        return FileResponse(static / "index.html")

    @app.get("/api/state")
    async def state():
        return app.state.runtime.snapshot()

    @app.get("/api/search")
    async def search(q: str, venue: str | None = None):
        if venue not in (None, "polymarket", "kalshi"):
            raise HTTPException(400, "Invalid venue.")
        try:
            return await asyncio.to_thread(Resolver().search, q, venue)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.post("/api/resolve")
    async def resolve(body: TrackRequest):
        try:
            return {"markets": await asyncio.to_thread(Resolver().resolve, body.url)}
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        except Exception as error:
            raise HTTPException(502, f"Venue lookup failed: {type(error).__name__}.") from error

    @app.post("/api/track")
    async def track(body: TrackRequest):
        markets = (await resolve(body))["markets"]
        if body.market_ids is not None:
            markets = [m for m in markets if m["market_id"] in body.market_ids]
        elif len({m["market_id"] for m in markets}) > 1 and not body.all_markets:
            raise HTTPException(400, "Select markets from this event, or choose Track all.")
        if not markets:
            raise HTTPException(400, "No selected open markets.")
        runtime = app.state.runtime
        try:
            result = runtime.subscriptions.update(markets)
            if runtime.bootstrap:
                from .kafka import Sender
                # Use a separate SQLite connection in the publication thread.
                def publish():
                    store = Subscriptions(runtime.root / "subscriptions.sqlite")
                    try:
                        store.publish(Sender(runtime.bootstrap))
                    finally:
                        store.close()
                await asyncio.to_thread(publish)
            return {"tracked": result}
        except ValueError as error:
            raise HTTPException(400, str(error)) from error

    @app.delete("/api/track/{identifier:path}")
    async def untrack(identifier: str):
        runtime = app.state.runtime
        try:
            result = runtime.subscriptions.remove(identifier)
            if runtime.bootstrap:
                from .kafka import Sender
                def publish():
                    store = Subscriptions(runtime.root / "subscriptions.sqlite")
                    try:
                        store.publish(Sender(runtime.bootstrap))
                    finally:
                        store.close()
                await asyncio.to_thread(publish)
            return {"untracked": result}
        except ValueError as error:
            raise HTTPException(404, str(error)) from error

    @app.websocket("/ws")
    async def websocket(socket: WebSocket):
        await socket.accept()
        try:
            while True:
                await socket.send_json(app.state.runtime.snapshot())
                await asyncio.sleep(.5)
        except (WebSocketDisconnect, RuntimeError):
            pass

    return app
