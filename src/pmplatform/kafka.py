import json
import os
import time
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from .archive import Archive
from .engine import BookEngine, InstrumentBook
from .metrics import ALERT_LATENCY, BOOK_GAPS, LAG, VIOLATIONS
from .model import Event, RawRecord, canonical
from .normalize import Normalizer


def topic_name(name):
    prefix = os.getenv("PM_TOPIC_NAMESPACE", "")
    return name if prefix and name.startswith(prefix) else prefix + name


DAY = 86_400_000
TOPICS = {
    "raw.polymarket": (4, "delete", 7 * DAY), "raw.kalshi": (4, "delete", 7 * DAY),
    "md.events": (4, "delete", 7 * DAY), "book.state": (4, "compact", -1),
    "book.history": (4, "delete", 7 * DAY), "book.checkpoints": (4, "compact", -1),
    "normalizer.checkpoints": (1, "compact", -1), "alerts.gaps": (4, "delete", 30 * DAY),
    "gap.checkpoints": (1, "compact", -1),
    "dq.violations": (4, "delete", 30 * DAY), "control.subscriptions": (1, "compact", -1),
    "control.resync": (1, "compact", -1),
}


def initialize(bootstrap):
    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    topics = [NewTopic(topic_name(name), num_partitions=partitions, replication_factor=1,
                       config={"cleanup.policy": policy, "retention.ms": str(retention)})
              for name, (partitions, policy, retention) in TOPICS.items()]
    for _, future in admin.create_topics(topics, request_timeout=30).items():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def consumer(bootstrap, group, **extra):
    return Consumer({"bootstrap.servers": bootstrap, "broker.address.family": "v4", "group.id": topic_name(group),
                     "auto.offset.reset": "earliest", "enable.auto.commit": False,
                     "isolation.level": "read_committed", "enable.partition.eof": True,
                     "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000, **extra})


class Sender:
    def __init__(self, bootstrap, transaction_id=None):
        config = {"bootstrap.servers": bootstrap, "broker.address.family": "v4", "enable.idempotence": True, "acks": "all",
                  "delivery.timeout.ms": 30_000}
        if transaction_id:
            config["transactional.id"] = topic_name(transaction_id)
        self.producer = Producer(config)
        if transaction_id:
            self.producer.init_transactions(30)

    def send(self, topic, key, value):
        errors = []
        self.producer.produce(topic_name(topic), key=key.encode(), value=value,
                              on_delivery=lambda error, _: errors.append(error) if error else None)
        if self.producer.flush(30):
            raise TimeoutError("producer_delivery_timeout")
        if errors:
            raise KafkaException(errors[0])


def latest(bootstrap, topic, timeout=30):
    topic = topic_name(topic)
    reader = consumer(bootstrap, f"snapshot-{uuid.uuid4()}")
    metadata = reader.list_topics(topic, timeout=timeout)
    partitions = [TopicPartition(topic, p, 0) for p in metadata.topics[topic].partitions]
    reader.assign(partitions)
    completed, result = set(), {}
    deadline = time.monotonic() + timeout
    try:
        while len(completed) < len(partitions):
            if time.monotonic() > deadline:
                raise TimeoutError(f"checkpoint_snapshot_timeout:{topic}")
            message = reader.poll(.5)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    completed.add(message.partition())
                    continue
                raise KafkaException(message.error())
            key = message.key().decode()
            if message.value() is None:
                result.pop(key, None)
            else:
                result[key] = json.loads(message.value())
        return result
    finally:
        reader.close()


def messages_batch(client, maximum=100):
    first = client.poll(.5)
    if first is None:
        return []
    messages = [first] + client.consume(maximum - 1, timeout=.05)
    result = []
    for message in messages:
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            raise KafkaException(message.error())
        result.append(message)
    return result


def offsets_for(messages):
    result = {}
    for m in messages:
        result[m.topic(), m.partition()] = max(result.get((m.topic(), m.partition()), -1), m.offset() + 1)
    return [TopicPartition(topic, part, offset) for (topic, part), offset in result.items()]


def atomic_publish(producer, client, messages, outputs):
    """False means the staged objects must be discarded. Ambiguous errors terminate the worker."""
    producer.begin_transaction()
    errors = []
    try:
        for topic, key, payload in outputs:
            while True:
                try:
                    producer.produce(topic_name(topic), key=key.encode(), value=canonical(payload),
                                     on_delivery=lambda error, _: errors.append(error) if error else None)
                    break
                except BufferError:
                    producer.poll(.1)
        if producer.flush(30):
            raise TimeoutError("transaction_delivery_timeout")
        if errors:
            raise KafkaException(errors[0])
        producer.send_offsets_to_transaction(offsets_for(messages), client.consumer_group_metadata(), 30)
        producer.commit_transaction(30)
        return True
    except KafkaException as error:
        if not error.args[0].txn_requires_abort():
            # A retryable commit result can be ambiguous. Restart and restore committed broker state.
            raise
        producer.abort_transaction(30)
        rewind = {}
        for m in messages:
            rewind[m.topic(), m.partition()] = min(rewind.get((m.topic(), m.partition()), m.offset()), m.offset())
        for (topic, partition), offset in rewind.items():
            client.seek(TopicPartition(topic, partition, offset))
        return False


def update_lag(client, group):
    assigned = client.assignment()
    if not assigned:
        return
    # Called only after offsets commit. Use cached watermarks so metrics cannot block the data path.
    for position in client.position(assigned):
        if position.offset < 0:
            continue
        _, high = client.get_watermark_offsets(position, cached=True)
        if high is not None:
            LAG.labels(group, position.topic, str(position.partition)).set(max(0, high - position.offset))


def run_normalizer(bootstrap, worker_id="normalizer-1", stop=None):
    group = "pm-normalizer"
    client = consumer(bootstrap, group)
    producer = Sender(bootstrap, f"pm-{worker_id}").producer
    normalizers = {}

    def assigned(c, partitions):
        checkpoints = latest(bootstrap, "normalizer.checkpoints")
        normalizers.clear()
        for partition in partitions:
            key = f"{partition.topic}:{partition.partition}"
            normalizers[key] = Normalizer.restore(checkpoints[key]) if key in checkpoints else Normalizer()
        c.assign(partitions)

    client.subscribe([topic_name("raw.polymarket"), topic_name("raw.kalshi")], on_assign=assigned)
    try:
        while stop is None or not stop.is_set():
            messages = messages_batch(client)
            if not messages:
                continue
            staged = {key: n.clone() for key, n in normalizers.items()}
            outputs = []
            for message in messages:
                key = f"{message.topic()}:{message.partition()}"
                raw = RawRecord.model_validate_json(message.value()).model_copy(update={
                    "topic": message.topic(), "partition": message.partition(), "offset": message.offset()})
                staged[key], events, parser_errors = staged[key].apply_safe(raw)
                outputs.extend(("dq.violations", key, error) for error in parser_errors)
                for event in events:
                    outputs.append(("md.events", event.key, event.model_dump(mode="json")))
            for key, normalizer in staged.items():
                outputs.append(("normalizer.checkpoints", key, normalizer.checkpoint()))
            if atomic_publish(producer, client, messages, outputs):
                normalizers = staged
                update_lag(client, group)
    finally:
        client.close()


def run_builder(bootstrap, worker_id="builder-1", stop=None):
    group = "pm-builder"
    client = consumer(bootstrap, group)
    producer = Sender(bootstrap, f"pm-{worker_id}").producer
    engine = BookEngine()

    def assigned(c, partitions):
        nonlocal engine
        checkpoints = latest(bootstrap, "book.checkpoints")
        allowed = {(p.topic, p.partition) for p in partitions}
        engine = BookEngine()
        for key, data in checkpoints.items():
            if (data["input_topic"], data["input_partition"]) in allowed:
                engine.books[key] = InstrumentBook.restore(data["book"])
        c.assign(partitions)

    client.subscribe([topic_name("md.events")], on_assign=assigned)
    try:
        while stop is None or not stop.is_set():
            messages = messages_batch(client)
            if not messages:
                continue
            events = [Event.model_validate_json(m.value()) for m in messages]
            working, states, violations = engine.stage(events)
            outputs = [(topic, s["key"], s) for s in states for topic in ("book.state", "book.history")]
            for violation in violations:
                VIOLATIONS.labels(violation["reason"]).inc()
                if violation["reason"] in ("sequence_gap", "sequence_regression"):
                    BOOK_GAPS.labels(working.books[violation["key"]].identity["venue"]).inc()
                outputs.append(("dq.violations", violation["key"], violation))
            last_per_key = {event.key: (event, message) for event, message in zip(events, messages)}
            for event, message in last_per_key.values():
                book = working.books[event.key]
                outputs.append(("book.checkpoints", event.key, {"book": book.checkpoint(),
                                "input_topic": message.topic(), "input_partition": message.partition(),
                                "input_offset": message.offset()}))
                if book.status == "stale":
                    outputs.append(("control.resync", event.key, {"venue": event.venue,
                                    "instrument_id": event.instrument_id, "event_id": event.event_id,
                                    "recv_ts_ns": event.recv_ts_ns}))
            if atomic_publish(producer, client, messages, outputs):
                engine = working
                update_lag(client, group)
    finally:
        client.close()


def run_archiver(bootstrap, root, stop=None):
    client = consumer(bootstrap, "pm-archiver")
    archive = Archive(root)
    client.subscribe([topic_name(t) for t in ("raw.polymarket", "raw.kalshi", "md.events")])
    try:
        while stop is None or not stop.is_set():
            messages = messages_batch(client, 500)
            if not messages:
                continue
            entries = []
            for m in messages:
                kind = "raw" if m.topic().endswith(("raw.polymarket", "raw.kalshi")) else "events"
                data = json.loads(m.value())
                if kind == "raw":
                    data.update(topic=m.topic(), partition=m.partition(), offset=m.offset())
                entries.append((kind, m.topic(), m.partition(), m.offset(), 0, data))
            archive.write(entries)
            client.commit(offsets=offsets_for(messages), asynchronous=False)
            update_lag(client, "pm-archiver")
    finally:
        archive.close()
        client.close()


def run_gap_detector(bootstrap, pairs, stop=None):
    from .gaps import GapDetector
    client = consumer(bootstrap, "pm-gaps")
    producer = Sender(bootstrap, "pm-gaps-1").producer
    detector = GapDetector(pairs)
    checkpoint = latest(bootstrap, "gap.checkpoints").get("pm-gaps", {})
    detector.states = checkpoint.get("states", {})
    client.subscribe([topic_name("book.history")])
    try:
        while stop is None or not stop.is_set():
            messages = messages_batch(client)
            if not messages:
                continue
            previous = detector.states.copy()
            outputs = []
            for message in messages:
                state = json.loads(message.value())
                for alert in detector.apply(state):
                    outputs.append(("alerts.gaps", alert["alert_id"], alert))
            outputs.append(("gap.checkpoints", "pm-gaps", {"states": detector.states}))
            if not atomic_publish(producer, client, messages, outputs):
                detector.states = previous
            else:
                for topic, _, alert in outputs:
                    if topic == "alerts.gaps":
                        ALERT_LATENCY.observe(max(0, time.time_ns() - alert["recv_ts_ns"]) / 1e9)
                update_lag(client, "pm-gaps")
    finally:
        client.close()
