import hashlib
import heapq
import json
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .engine import BookEngine
from .model import Event, RawRecord, canonical
from .normalize import Normalizer


def merge_partitions(records, raw=False):
    groups = defaultdict(list)
    for record in records:
        key = (record.topic, record.partition) if raw else (record.source_topic, record.source_partition)
        cursor = (record.offset, 0) if raw else (record.source_offset, record.subevent_index)
        groups[key].append((cursor, record))
    heap = []
    streams = {}
    for key, values in groups.items():
        values.sort(key=lambda v: v[0])
        streams[key] = iter(values)
        cursor, record = next(streams[key])
        heapq.heappush(heap, (record.recv_ts_ns, key, cursor, record))
    while heap:
        _, key, _, record = heapq.heappop(heap)
        yield record
        following = next(streams[key], None)
        if following:
            cursor, record = following
            heapq.heappush(heap, (record.recv_ts_ns, key, cursor, record))


def normalized_raw(records):
    normalizer = Normalizer()
    for raw in merge_partitions(records, raw=True):
        normalizer, events, _ = normalizer.apply_safe(raw)
        yield from events


@dataclass
class ReplayResult:
    states: list[dict]
    violations: list[dict]
    engine: BookEngine
    initial_checkpoint: dict
    selected_events: list[Event]

    @property
    def output(self):
        return b"".join(canonical(state) + b"\n" for state in self.states)

    @property
    def sha256(self):
        return hashlib.sha256(self.output).hexdigest()


def replay(events, start=0, end=(1 << 63) - 1, speed="max", checkpoint=None):
    if end <= start:
        raise ValueError("end_must_follow_start")
    multiplier = None if speed == "max" else float(str(speed).rstrip("x"))
    if multiplier is not None and multiplier <= 0:
        raise ValueError("speed_must_be_positive")
    engine = BookEngine.restore(checkpoint) if checkpoint else BookEngine()
    states, violations, selected = [], [], []
    initial = None
    last_ts = None
    closed_partitions = set()
    entered_partitions = set()
    for event in events:
        partition = event.source_topic, event.source_partition
        if partition in closed_partitions:
            continue
        if event.recv_ts_ns >= end:
            closed_partitions.add(partition)
            continue
        if event.recv_ts_ns < start and partition in entered_partitions:
            raise ValueError("window_start_crossed_by_clock_regression")
        if event.recv_ts_ns >= start:
            entered_partitions.add(partition)
        if event.recv_ts_ns >= start and initial is None:
            initial = engine.checkpoint()
        if event.event_kind == "delta" and event.key not in engine.books:
            raise ValueError(f"missing_start_state:{event.key}")
        if event.recv_ts_ns >= start:
            if multiplier is not None and last_ts is not None:
                time.sleep(min(60, max(0, event.recv_ts_ns - last_ts) / 1e9 / multiplier))
            last_ts = event.recv_ts_ns
            selected.append(event)
        state, errors = engine.apply(event)
        if event.recv_ts_ns >= start:
            if state is not None:
                states.append(state)
            violations.extend(errors)
    return ReplayResult(states, violations, engine, initial if initial is not None else engine.checkpoint(), selected)


def read_jsonl(path, raw=False):
    cls = RawRecord if raw else Event
    with Path(path).open() as stream:
        return [cls.model_validate_json(line) for line in stream if line.strip()]


def import_events(path):
    path = Path(path)
    manifest_path = path.with_name(path.name + ".manifest.json")
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if manifest.get("sha256") and hashlib.sha256(path.read_bytes()).hexdigest() != manifest["sha256"]:
        raise ValueError("export_checksum_mismatch")
    return read_jsonl(path), manifest.get("initial_checkpoint", {})
