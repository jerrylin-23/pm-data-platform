"""Short synthetic spike through all four workers and the local Kafka API."""
import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient
from hdrh.histogram import HdrHistogram

from pmplatform.demo import frame_pair
from pmplatform.kafka import TOPICS, Sender, consumer, initialize, topic_name
from pmplatform.model import canonical


def run(bootstrap, speed, seconds):
    os.environ['PM_TOPIC_NAMESPACE'] = f'bench-{uuid.uuid4().hex[:10]}-'
    initialize(bootstrap)
    root = Path(tempfile.mkdtemp(prefix='pm-bench-'))
    pairs = root / 'pairs.yml'
    pairs.write_text('pairs:\n  - name: Synthetic benchmark\n    left: polymarket:demo-yes\n    right: kalshi:DEMO-RATE\n    fee_e4: 50\n    threshold_e4: 100\n')
    processes, streams = [], []
    reader = consumer(bootstrap, 'bench-reader')
    reader.subscribe([topic_name('book.history'), topic_name('alerts.gaps')])
    probe = consumer(bootstrap, 'pm-normalizer')
    producer = Sender(bootstrap).producer
    raw_topics = [topic_name('raw.polymarket'), topic_name('raw.kalshi')]
    partitions = [TopicPartition(t, p) for t in raw_topics for p in range(4)]
    try:
        for name in ('normalizer', 'builder', 'archiver', 'gaps'):
            log = (root / f'{name}.log').open('a')
            streams.append(log)
            processes.append(subprocess.Popen([sys.executable, '-m', 'pmplatform', '--data-dir', str(root),
                                              '--bootstrap', bootstrap, 'worker', name, '--pairs', str(pairs)],
                                             stdout=log, stderr=log))
        deadline = time.monotonic() + 30
        while not reader.assignment():
            reader.poll(.1)
            if time.monotonic() > deadline:
                raise TimeoutError('benchmark_reader_assignment')
        # Allow worker groups to complete their first assignment before timing.
        time.sleep(2)
        count = seconds * 100
        latencies = HdrHistogram(1, 120_000_000, 3)
        observed, alerts, peak_lag = 0, 0, 0
        started, sent_done, last_lag_sample = time.monotonic(), None, 0
        index = 0
        deadline = started + seconds + 90
        while observed < count * 2:
            now = time.monotonic()
            due = index < count and (speed == 'max' or now - started >= index * .01 / float(speed))
            if due:
                for raw in frame_pair(index, time.time_ns()):
                    while True:
                        try:
                            producer.produce(topic_name(raw.topic), key=raw.connection_id.encode(),
                                             value=canonical(raw.model_dump(mode='json')))
                            break
                        except BufferError:
                            producer.poll(.001)
                index += 1
                producer.poll(0)
                if index == count:
                    if producer.flush(30):
                        raise TimeoutError('benchmark_delivery_timeout')
                    sent_done = time.monotonic()
            message = reader.poll(0 if due else .001)
            if message is not None and not message.error():
                state = json.loads(message.value())
                if message.topic() == topic_name('book.history'):
                    observed += 1
                else:
                    alerts += 1
                    latencies.record_value(max(1, (time.time_ns() - state['recv_ts_ns']) // 1000))
            if now - last_lag_sample >= .1:
                committed = probe.committed(partitions, timeout=5)
                lag = 0
                for partition in committed:
                    _, high = probe.get_watermark_offsets(partition, timeout=5)
                    lag += max(0, high - max(partition.offset, 0))
                peak_lag = max(peak_lag, lag)
                last_lag_sample = now
            if any(p.poll() is not None for p in processes):
                raise RuntimeError(f'worker_failed: logs in {root}')
            if time.monotonic() > deadline:
                raise TimeoutError(f'benchmark_drain_timeout: {observed}/{count * 2}, logs in {root}')
        drained = time.monotonic()
        # Read remaining alert transactions without extending the measured book drain interval.
        quiet_until = time.monotonic() + 2
        while time.monotonic() < quiet_until:
            message = reader.poll(.05)
            if message is not None and not message.error() and message.topic() == topic_name('alerts.gaps'):
                state = json.loads(message.value())
                alerts += 1
                latencies.record_value(max(1, (time.time_ns() - state['recv_ts_ns']) // 1000))
        return {'speed': speed, 'synthetic_base_raw_frames_per_second': 200, 'raw_frames': count * 2,
                'book_states': observed, 'alerts_observed': alerts,
                'book_states_per_second': observed / (drained - started), 'run_seconds': drained - started,
                'receive_to_alert_observed_p50_ms': latencies.get_value_at_percentile(50) / 1000,
                'receive_to_alert_observed_p95_ms': latencies.get_value_at_percentile(95) / 1000,
                'receive_to_alert_observed_p99_ms': latencies.get_value_at_percentile(99) / 1000,
                'receive_to_alert_observed_p99_9_ms': latencies.get_value_at_percentile(99.9) / 1000,
                'peak_normalizer_lag_sampled_100ms': peak_lag,
                'seconds_to_drain_after_last_send': max(0, drained - sent_done), 'logs': str(root)}
    finally:
        reader.close()
        probe.close()
        for process in processes:
            process.terminate()
        for process in processes:
            try:
                process.wait(5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(5)
        for stream in streams:
            stream.close()
        admin = AdminClient({'bootstrap.servers': bootstrap, 'broker.address.family': 'v4'})
        for future in admin.delete_topics([topic_name(name) for name in TOPICS]).values():
            future.result(30)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap', default='localhost:19092')
    parser.add_argument('--seconds', type=int, default=5)
    parser.add_argument('--out', default='bench/kafka-measurements.json')
    args = parser.parse_args()
    result = {'data_origin': 'synthetic', 'method': 'Local Redpanda, four workers. Receive timestamp set before enqueue. Alert latency includes observer delivery. Lag sampled every 100 ms; measured peak is a lower bound.',
              'runs': [run(args.bootstrap, speed, args.seconds) for speed in ('1', '10', 'max')]}
    Path(args.out).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
