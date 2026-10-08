import asyncio
import json
import os
import random
import time
import uuid
from contextlib import suppress

import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .metrics import MESSAGES, RESYNCS
from .model import canonical
from .spool import Spool

POLY_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
KALSHI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"


def auth_headers(key_id, private_key_path, timestamp_ms=None):
    if not key_id or not private_key_path:
        raise ValueError("Kalshi streaming requires KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH.")
    with open(private_key_path, "rb") as stream:
        key = serialization.load_pem_private_key(stream.read(), password=None)
    timestamp = str(timestamp_ms if timestamp_ms is not None else time.time_ns() // 1_000_000)
    signature = key.sign((timestamp + "GET/trade-api/ws/v2").encode(),
                         padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
                         hashes.SHA256())
    import base64
    return {"KALSHI-ACCESS-KEY": key_id, "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode()}


class Collector:
    def __init__(self, venue, root, markets, sink, stop, batch_size=100, max_snapshot_age=60):
        self.venue, self.root, self.markets, self.sink, self.stop = venue, root, markets, sink, stop
        self.batch_size, self.max_snapshot_age = batch_size, max_snapshot_age
        self.spool = Spool(root / f"spool-{venue}.sqlite")
        self.tasks = {}
        self.resync_keys = set()
        self.subscription_started_ns = {}
        self.delivery_lock = asyncio.Lock()

    def request_resync(self, instrument_id, recv_ts_ns):
        if recv_ts_ns >= self.subscription_started_ns.get(instrument_id, 0):
            self.resync_keys.add(instrument_id)

    async def deliver_pending(self):
        async with self.delivery_lock:
            while pending := self.spool.pending(1000):
                for raw in pending:
                    await self.sink(raw)
                    self.spool.acknowledge(raw.offset)
                    # Local delivery can complete without yielding while live frames queue up.
                    await asyncio.sleep(0)

    async def capture(self, payload, epoch, connection_id, counter, instruments):
        self.spool.append(payload, venue=self.venue, topic=f"raw.{self.venue}", recv_ts_ns=time.time_ns(),
                          connection_epoch=epoch, connection_id=connection_id, counter=counter,
                          instruments=instruments)
        await self.deliver_pending()
        MESSAGES.labels(self.venue).inc()

    async def run(self):
        while not self.stop.is_set():
            try:
                await self.deliver_pending()
                break
            except Exception as error:
                print(f"{self.venue} pending delivery: {type(error).__name__}", flush=True)
                await asyncio.sleep(1)
        try:
            while not self.stop.is_set():
                markets = await self.markets()
                instruments = {m["instrument_id"]: m for m in markets if m["venue"] == self.venue}
                # Sorted chunks enforce the connection cap. Moved instruments receive a new snapshot.
                shards = {}
                for index, (asset, metadata) in enumerate(sorted(instruments.items())):
                    shard = index // self.batch_size
                    shards.setdefault(shard, {})[asset] = metadata
                for shard in set(self.tasks) - set(shards):
                    self.tasks.pop(shard).cancel()
                for shard in shards:
                    if shard not in self.tasks or self.tasks[shard].done():
                        self.tasks[shard] = asyncio.create_task(self.stream(shard, self.markets))
                await asyncio.sleep(1)
        finally:
            tasks = list(self.tasks.values())
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.spool.close()

    async def stream(self, shard, markets_provider):
        attempt = 0
        while not self.stop.is_set():
            epoch = self.spool.epoch()
            connection_id = str(uuid.uuid4())
            counter, subscribed, sid = 0, {}, None
            async def record(payload, members=None):
                nonlocal counter
                counter += 1
                await self.capture(payload, epoch, connection_id, counter, members if members is not None else subscribed)
            try:
                await self.deliver_pending()
                url = POLY_WS if self.venue == "polymarket" else KALSHI_WS
                headers = None if self.venue == "polymarket" else auth_headers(
                    os.getenv("KALSHI_KEY_ID"), os.getenv("KALSHI_PRIVATE_KEY_PATH"))
                async with websockets.connect(url, additional_headers=headers, open_timeout=20,
                                              ping_interval=20, ping_timeout=20, max_queue=4096,
                                              max_size=8 * 1024 * 1024) as socket:
                    last_ping, last_snapshot, last_data = 0.0, time.monotonic(), time.monotonic()
                    command_id = 1
                    while not self.stop.is_set():
                        markets = [m for m in await markets_provider() if m["venue"] == self.venue]
                        markets.sort(key=lambda m: m["instrument_id"])
                        desired = {m["instrument_id"]: m for m in markets[shard * self.batch_size:(shard + 1) * self.batch_size]}
                        add, remove = set(desired) - set(subscribed), set(subscribed) - set(desired)
                        if self.venue == "polymarket":
                            if remove:
                                await record(canonical({"_control": "untrack"}), {a: subscribed[a] for a in sorted(remove)})
                                await socket.send(json.dumps({"assets_ids": sorted(remove), "operation": "unsubscribe"}))
                            if add:
                                frame = {"assets_ids": sorted(add), "custom_feature_enabled": True}
                                frame["operation" if subscribed else "type"] = "subscribe" if subscribed else "market"
                                await socket.send(json.dumps(frame))
                                last_snapshot = time.monotonic()
                                self.resync_keys.difference_update(add)
                                self.subscription_started_ns.update({asset: time.time_ns() for asset in add})
                        elif add or remove:
                            if subscribed:
                                if remove:
                                    await record(canonical({"_control": "untrack"}), {a: subscribed[a] for a in sorted(remove)})
                                subscribed = {a: m for a, m in subscribed.items() if a not in remove}
                                # Reconnect both channels together. Each channel has its own subscription ID.
                                raise ValueError("subscription_membership_changed")
                            command_id += 1
                            await socket.send(json.dumps({"id": command_id, "cmd": "subscribe", "params":
                                                          {"channels": ["orderbook_delta", "trade"], "market_tickers": sorted(desired)}}))
                        subscribed = desired
                        now = time.monotonic()
                        if self.venue == "polymarket" and now - last_ping >= 10:
                            await socket.send("PING")
                            last_ping = now
                        resync = bool(set(subscribed) & self.resync_keys) and now - last_snapshot >= 5
                        if subscribed and (resync or now - last_snapshot >= self.max_snapshot_age or now - last_data >= 15):
                            if now - last_data >= 15:
                                await record(canonical({"_control": "timeout"}))
                            await record(canonical({"_control": "resync"}))
                            RESYNCS.labels(self.venue).inc()
                            self.resync_keys.difference_update(subscribed)
                            if self.venue == "polymarket":
                                # A new connection gives snapshots a new epoch and drains old queued updates.
                                break
                            elif sid is not None:
                                command_id += 1
                                await socket.send(json.dumps({"id": command_id, "cmd": "update_subscription", "params":
                                                              {"sids": [sid], "market_tickers": sorted(subscribed), "action": "get_snapshot"}}))
                            last_snapshot, last_data = now, now
                        try:
                            payload = await asyncio.wait_for(socket.recv(), timeout=1)
                        except TimeoutError:
                            continue
                        payload = payload.encode() if isinstance(payload, str) else payload
                        if payload in (b"PONG", b"PING"):
                            continue
                        # The spool write occurs before parsing, including subscription acknowledgements.
                        await record(payload)
                        last_data = time.monotonic()
                        attempt = 0
                        if self.venue == "kalshi":
                            message = json.loads(payload)
                            if message.get("type") == "error":
                                raise ValueError("venue_command_error")
                            if message.get("type") == "subscribed" and message["msg"].get("channel") == "orderbook_delta":
                                sid = message["msg"]["sid"]
            except asyncio.CancelledError:
                if subscribed:
                    with suppress(Exception):
                        remaining = {m["instrument_id"] for m in await markets_provider() if m["venue"] == self.venue}
                        removed = set(subscribed) - remaining
                        if removed:
                            await record(canonical({"_control": "untrack"}), {a: subscribed[a] for a in sorted(removed)})
                        if set(subscribed) & remaining:
                            await record(canonical({"_control": "disconnect"}),
                                         {a: subscribed[a] for a in sorted(set(subscribed) & remaining)})
                raise
            except Exception as error:
                if subscribed:
                    with suppress(Exception):
                        await record(canonical({"_control": "disconnect"}))
                # Do not log authentication headers, key IDs, payloads, or exception text.
                print(f"{self.venue} reconnect: {type(error).__name__}", flush=True)
                await asyncio.sleep(min(30, .5 * 2 ** min(attempt, 6)) * random.uniform(.75, 1.25))
                attempt += 1
