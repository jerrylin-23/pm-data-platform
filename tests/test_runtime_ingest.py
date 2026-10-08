import asyncio
import base64
import json

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from pmplatform.ingest import Collector, auth_headers
from pmplatform.server import Runtime
from pmplatform.spool import Spool


def test_signing_uses_exact_websocket_path(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    path = tmp_path / "test-key.pem"
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                      serialization.NoEncryption()))
    headers = auth_headers("test-id", path, 123456)
    key.public_key().verify(base64.b64decode(headers["KALSHI-ACCESS-SIGNATURE"]),
                            b"123456GET/trade-api/ws/v2",
                            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                            hashes.SHA256())


def test_spool_survives_failed_delivery_and_epoch_restart(tmp_path):
    path = tmp_path / "spool.sqlite"
    spool = Spool(path)
    epoch = spool.epoch()
    raw = spool.append(b'{"invalid":', venue="polymarket", topic="raw.polymarket", recv_ts_ns=1,
                       connection_id="c", connection_epoch=epoch, counter=1, instruments={})
    spool.close()
    spool = Spool(path)
    assert spool.pending() == [raw]
    assert spool.epoch() > epoch
    spool.acknowledge(raw.offset)
    assert spool.pending() == []
    spool.close()


def test_collector_spools_before_sink_and_preserves_exact_bytes(tmp_path, monkeypatch):
    from pmplatform import ingest
    payload = b'[{ "event_type": "book", "asset_id":"1", "bids":[], "asks":[] }]'
    stop = asyncio.Event()
    class Socket:
        commands = []
        async def send(self, data):
            self.commands.append(data)
        async def recv(self):
            return payload
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
    socket = Socket()
    monkeypatch.setattr(ingest.websockets, "connect", lambda *a, **kw: socket)
    async def markets():
        return [{"venue": "polymarket", "instrument_id": "1", "market_id": "m", "outcome": "yes"}]
    async def run():
        collector = None
        async def sink(raw):
            assert collector.spool.pending()[-1] == raw
            assert raw.payload == payload
            stop.set()
        collector = Collector("polymarket", tmp_path, markets, sink, stop)
        await collector.stream(0, markets)
        assert collector.spool.pending() == []
        assert json.loads(socket.commands[0])["assets_ids"] == ["1"]
        collector.spool.close()
    asyncio.run(run())


def test_synthetic_runtime_advances_and_recovers_without_offset_conflict(tmp_path):
    async def run_feed(runtime):
        versions = {k: s["version"] for k, s in runtime.states.items()}
        task = asyncio.create_task(runtime.feed_demo())
        try:
            async with asyncio.timeout(10):
                while not runtime.states or any(s["version"] < max(2, versions.get(k, 0) + 1)
                                                for k, s in runtime.states.items()):
                    await asyncio.sleep(.05)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    first = Runtime(tmp_path, demo=True)
    asyncio.run(run_feed(first))
    versions = {k: s["version"] for k, s in first.states.items()}
    assert all(v >= 2 for v in versions.values())
    asyncio.run(first.close())
    second = Runtime(tmp_path, demo=True)
    asyncio.run(run_feed(second))
    assert all(s["version"] > versions[k] for k, s in second.states.items())
    assert all(s["data_origin"] == "synthetic" for s in second.states.values())
    asyncio.run(second.close())


def test_pending_send_recovers_without_process_restart(tmp_path):
    async def run():
        failed = False
        delivered = []
        async def sink(raw):
            nonlocal failed
            if not failed:
                failed = True
                raise ConnectionError("test outage")
            delivered.append(raw.counter)
        collector = Collector("polymarket", tmp_path, None, sink, asyncio.Event())
        try:
            await collector.capture(b'first', 1, 'c', 1, {})
        except ConnectionError:
            pass
        assert len(collector.spool.pending()) == 1
        await collector.capture(b'second', 1, 'c', 2, {})
        assert delivered == [1, 2]
        assert collector.spool.pending() == []
        collector.spool.close()
    asyncio.run(run())


def test_pending_local_delivery_allows_other_tasks_to_run(tmp_path):
    async def run():
        delivered, observed = [], []
        async def sink(raw):
            delivered.append(raw.counter)
        async def observe():
            observed.extend(delivered)
        collector = Collector("polymarket", tmp_path, None, sink, asyncio.Event())
        for counter in range(1, 4):
            collector.spool.append(b'{}', venue="polymarket", topic="raw.polymarket", recv_ts_ns=counter,
                                   connection_id="c", connection_epoch=1, counter=counter, instruments={})
        observer = asyncio.create_task(observe())
        await collector.deliver_pending()
        await observer
        assert observed == [1]
        assert delivered == [1, 2, 3]
        assert collector.spool.pending() == []
        collector.spool.close()
    asyncio.run(run())


def test_snapshot_preserves_event_and_side_labels(tmp_path):
    runtime = Runtime(tmp_path)
    metadata = {"venue": "polymarket", "market_id": "m", "instrument_id": "123", "outcome": "yes",
                "title": "Count in range?", "parent_event_id": "e", "event_title": "Tweet count",
                "event_url": "https://polymarket.com/event/tweets", "market_label": "220-239",
                "market_order": 11, "market_type": "totals", "outcome_labels": {"yes": "Over", "no": "Under"}}
    runtime.subscriptions.update([metadata])
    runtime.states["polymarket:123"] = {"key": "polymarket:123", "venue": "polymarket",
                                       "market_id": "m", "instrument_id": "123", "data_origin": "live",
                                       "event_id": "source-record-id"}
    snapshot = runtime.snapshot()["books"][0]
    for field in ("parent_event_id", "event_title", "event_url", "market_label", "market_order", "market_type", "outcome_labels"):
        assert snapshot[field] == metadata[field]
    assert snapshot["event_id"] == "source-record-id"
    asyncio.run(runtime.close())


def test_live_mode_hides_saved_synthetic_books_and_alerts(tmp_path):
    runtime = Runtime(tmp_path, demo=False)
    runtime.states = {"sample": {"market_id": "sample", "data_origin": "synthetic"},
                      "real": {"market_id": "real", "data_origin": "live"}}
    runtime.alerts = [{"pair": "sample", "data_origin": ["synthetic", "synthetic"]},
                      {"pair": "mixed", "data_origin": ["live", "synthetic"]},
                      {"pair": "real", "data_origin": ["live", "live"]}]
    assert [b["market_id"] for b in runtime.snapshot()["books"]] == ["real"]
    assert [a["pair"] for a in runtime.snapshot()["alerts"]] == ["real"]
    runtime.demo = True
    assert len(runtime.snapshot()["books"]) == 2
    assert len(runtime.snapshot()["alerts"]) == 3
    asyncio.run(runtime.close())


def test_resync_waits_for_initial_snapshot_and_uses_new_connection(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from pmplatform import ingest

    clock, sockets, records = [0.0], [], []
    monkeypatch.setattr(ingest, "time", SimpleNamespace(monotonic=lambda: clock[0], time_ns=ingest.time.time_ns))
    stop = asyncio.Event()
    class Socket:
        def __init__(self):
            self.sent, self.received = [], 0
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        async def send(self, data):
            self.sent.append(data)
        async def recv(self):
            self.received += 1
            clock[0] += 1
            if len(sockets) == 2:
                stop.set()
            return b'{"event_type":"book","asset_id":"1","bids":[],"asks":[]}'
    def connect(*args, **kwargs):
        socket = Socket()
        sockets.append(socket)
        return socket
    monkeypatch.setattr(ingest.websockets, "connect", connect)
    async def markets():
        return [{"venue": "polymarket", "instrument_id": "1", "market_id": "m", "outcome": "yes"}]
    async def run():
        async def sink(raw):
            records.append(raw)
            if raw.counter == 1 and len(sockets) == 1:
                collector.request_resync("1", raw.recv_ts_ns)
        collector = Collector("polymarket", tmp_path, markets, sink, stop)
        await collector.stream(0, markets)
        collector.spool.close()
    asyncio.run(run())
    assert len(sockets) == 2
    assert sockets[0].received == 5
    assert sum(json.loads(raw.payload).get("_control") == "resync" for raw in records) == 1
    assert records[-1].connection_epoch > records[0].connection_epoch
    assert all(len([command for command in socket.sent if command != "PING"]) == 1 for socket in sockets)


def test_resync_ignores_requests_from_previous_subscription(tmp_path):
    collector = Collector("polymarket", tmp_path, None, None, asyncio.Event())
    collector.subscription_started_ns["1"] = 100
    collector.request_resync("1", 99)
    assert not collector.resync_keys
    collector.request_resync("1", 100)
    assert collector.resync_keys == {"1"}
    collector.spool.close()
