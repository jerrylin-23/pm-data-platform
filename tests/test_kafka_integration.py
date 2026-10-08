"""Runs only against the disposable broker created by this project."""
import os
import subprocess
import sys
import time
import uuid

import pytest
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from pmplatform.archive import Archive
from pmplatform.demo import fixture
from pmplatform.engine import BookEngine
from pmplatform.kafka import TOPICS, Sender, atomic_publish, consumer, initialize, latest, topic_name
from pmplatform.model import RawRecord, canonical
from pmplatform.replay import normalized_raw, replay

pytestmark = pytest.mark.skipif(os.getenv("PM_KAFKA_TESTS") != "1", reason="Set PM_KAFKA_TESTS=1 for local broker tests")


@pytest.fixture(autouse=True)
def remove_own_test_topics(monkeypatch):
    yield
    prefix = os.getenv("PM_TOPIC_NAMESPACE", "")
    if prefix.startswith(("it-", "abort-")):
        admin = AdminClient({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
                             "broker.address.family": "v4"})
        existing = admin.list_topics(timeout=10).topics
        for future in admin.delete_topics([prefix + name for name in TOPICS if prefix + name in existing]).values():
            future.result(30)


def wait_for(predicate, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.3)
    raise AssertionError("Kafka pipeline did not reach expected state")


def test_real_transactions_duplicate_capture_and_worker_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("PM_TOPIC_NAMESPACE", f"it-{uuid.uuid4().hex[:10]}-")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    initialize(bootstrap)
    processes, logs = {}, {}
    def start(service):
        log = (tmp_path / f"{service}.log").open("a")
        logs[service] = log
        command = [sys.executable, "-m", "pmplatform", "--data-dir", str(tmp_path), "--bootstrap", bootstrap,
                   "worker", service]
        processes[service] = subprocess.Popen(command, stdout=log, stderr=log)
    try:
        for service in ("normalizer", "builder", "archiver"):
            start(service)
        sender = Sender(bootstrap)
        records = list(fixture(12))
        for raw in records[:12]:
            sender.send(raw.topic, raw.connection_id, canonical(raw.model_dump(mode="json")))
        wait_for(lambda: len(latest(bootstrap, "book.state")) >= 2)
        # Kill only the worker created in this test, then restart it with the same transactional ID.
        processes["builder"].kill()
        processes["builder"].wait(5)
        logs["builder"].close()
        start("builder")
        for raw in records[12:]:
            sender.send(raw.topic, raw.connection_id, canonical(raw.model_dump(mode="json")))
        wait_for(lambda: all(state["version"] >= 12 for state in latest(bootstrap, "book.state").values()))
        before = latest(bootstrap, "book.state")
        sender.send(records[-1].topic, records[-1].connection_id, canonical(records[-1].model_dump(mode="json")))
        time.sleep(2)
        assert latest(bootstrap, "book.state") == before
        for service in ("normalizer", "archiver"):
            processes[service].kill()
            processes[service].wait(5)
            logs[service].close()
            start(service)
            sender.send(records[-1].topic, records[-1].connection_id, canonical(records[-1].model_dump(mode="json")))
        archive_path = tmp_path / "archive"
        def archived():
            archive = Archive(archive_path)
            try:
                return archive.count("raw") >= len(records) + 3 and archive.count("events") >= len(records)
            finally:
                archive.close()
        wait_for(archived)
        archive = Archive(archive_path)
        try:
            raw_records = [RawRecord.model_validate(data) for data in archive.rows("raw")]
            clean = replay(list(normalized_raw(raw_records)))
            assert {key: book.state() for key, book in clean.engine.books.items()} == before
            assert archive.count("events") == 24
            assert clean.sha256 == replay(list(normalized_raw(raw_records))).sha256
        finally:
            archive.close()
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(5)
                except subprocess.TimeoutExpired:
                    process.kill()
        for log in logs.values():
            log.close()


def test_aborted_output_is_hidden_and_input_is_rewound(monkeypatch):
    monkeypatch.setenv("PM_TOPIC_NAMESPACE", f"abort-{uuid.uuid4().hex[:10]}-")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
    initialize(bootstrap)
    raw = next(fixture(1))
    Sender(bootstrap).send(raw.topic, raw.connection_id, canonical(raw.model_dump(mode="json")))
    client = consumer(bootstrap, "abort-test")
    client.subscribe([topic_name(raw.topic)])
    def received():
        message = client.poll(.3)
        return message if message is not None and not message.error() else None
    message = wait_for(received)
    raw = raw.model_copy(update={"topic": message.topic(), "partition": message.partition(), "offset": message.offset()})
    engine = BookEngine()
    staged, states, errors = engine.stage(list(normalized_raw([raw])))
    assert not errors
    real = Sender(bootstrap, "abort-producer").producer
    class ForcedAbort:
        def __getattr__(self, name):
            return getattr(real, name)
        def commit_transaction(self, timeout):
            raise KafkaException(KafkaError(KafkaError._STATE, "injected test abort", False, False, True))
    outputs = [("book.state", s["key"], s) for s in states]
    try:
        assert not atomic_publish(ForcedAbort(), client, [message], outputs)
        assert latest(bootstrap, "book.state") == {}
        assert engine.books == {}
        retried = wait_for(received)
        assert retried.offset() == message.offset()
        assert atomic_publish(real, client, [retried], outputs)
        engine = staged
        assert latest(bootstrap, "book.state") == {k: b.state() for k, b in engine.books.items()}
    finally:
        client.close()
