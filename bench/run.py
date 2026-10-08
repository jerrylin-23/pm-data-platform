"""Repeatable local microbenchmarks. These are not Kafka or venue latency measurements."""
import argparse
import hashlib
import json
import platform
import time
from pathlib import Path

from hdrh.histogram import HdrHistogram

from pmplatform import _book
from pmplatform.demo import DEMO_PAIR, fixture
from pmplatform.engine import BookEngine
from pmplatform.gaps import GapDetector
from pmplatform.model import canonical
from pmplatform.normalize import Normalizer
from pmplatform.replay import merge_partitions


class Reference:
    """Plain dict baseline, with atomic validation and sorted top ten levels."""
    def __init__(self, levels):
        self.bids = {p: q for side, p, q in levels if side == 'bid'}
        self.asks = {p: q for side, p, q in levels if side == 'ask'}

    def update(self, side, price, size):
        if not 0 <= price <= 10000 or not 0 <= size < (1 << 63):
            raise ValueError('invalid_level')
        entries = self.bids if side == 'bid' else self.asks
        old = entries.get(price)
        if size:
            entries[price] = size
        else:
            entries.pop(price, None)
        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            if old is None:
                entries.pop(price, None)
            else:
                entries[price] = old
            raise ValueError('crossed_book')

    def depth(self):
        return [[(p, entries[p]) for p in sorted(entries, reverse=reverse)[:10]]
                for entries, reverse in ((self.bids, True), (self.asks, False))]


def timing(operation, count):
    histogram = HdrHistogram(1, 60_000_000_000, 3)
    begin = time.perf_counter_ns()
    for i in range(count):
        started = time.perf_counter_ns()
        operation(i)
        histogram.record_value(max(1, time.perf_counter_ns() - started))
    elapsed = time.perf_counter_ns() - begin
    return {'operations': count, 'seconds': elapsed / 1e9, 'operations_per_second': count / (elapsed / 1e9),
            'p50_us': histogram.get_value_at_percentile(50) / 1000,
            'p95_us': histogram.get_value_at_percentile(95) / 1000,
            'p99_us': histogram.get_value_at_percentile(99) / 1000,
            'p99_9_us': histogram.get_value_at_percentile(99.9) / 1000}


def books(count, levels):
    initial = [(side, (4000 - i if side == 'bid' else 6000 + i), (i + 1) * 1_000_000)
               for side in ('bid', 'ask') for i in range(levels)]
    changes = [('bid' if i % 2 == 0 else 'ask',
                4000 - i % levels if i % 2 == 0 else 6000 + i % levels,
                (i % 97 + 1) * 1_000_000) for i in range(count)]
    native = _book.Book()
    native.snapshot([_book.Level(_book.Side.BID if s == 'bid' else _book.Side.ASK, p, q) for s, p, q in initial])
    batches = [[_book.Level(_book.Side.BID if s == 'bid' else _book.Side.ASK, p, q)] for s, p, q in changes]
    def cpp(i):
        native.update(batches[i])
        native.depth(_book.Side.BID, 10)
        native.depth(_book.Side.ASK, 10)
    reference = Reference(initial)
    def python(i):
        reference.update(*changes[i])
        reference.depth()
    cpp_result, python_result = timing(cpp, count), timing(python, count)
    assert reference.depth() == [native.depth(_book.Side.BID, 10), native.depth(_book.Side.ASK, 10)]
    return {'levels_per_side': levels, 'cpp_binding': cpp_result, 'python_dict': python_result,
            'throughput_ratio': cpp_result['operations_per_second'] / python_result['operations_per_second']}


def pipeline(seconds):
    records = list(merge_partitions(list(fixture(seconds)), raw=True))
    normalizer, engine, gaps = Normalizer(), BookEngine(), GapDetector([DEMO_PAIR])
    digest, counts = hashlib.sha256(), {'events': 0, 'alerts': 0}
    def apply(i):
        nonlocal normalizer, engine
        normalizer, events, errors = normalizer.apply_safe(records[i])
        assert not errors
        engine, states, violations = engine.stage(events)
        assert not violations
        for state in states:
            digest.update(canonical(state) + b'\n')
            counts['alerts'] += len(gaps.apply(state))
        counts['events'] += len(events)
    return {**timing(apply, len(records)), **counts, 'state_sha256': digest.hexdigest(), 'data_origin': 'synthetic'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operations', type=int, default=20000)
    parser.add_argument('--seconds', type=int, default=600)
    parser.add_argument('--out', default='bench/measurements.json')
    args = parser.parse_args()
    result = {'method': 'Single process; prepared one-level SET update plus top ten on each side. Native binding included.',
              'pipeline_method': 'Raw JSON decode, validation, normalization, staged C++ book, gap check, canonical state encoding. Storage and Kafka excluded.',
              'system': {'platform': platform.platform(), 'machine': platform.machine(), 'python': platform.python_version()},
              'books': [books(args.operations, depth) for depth in (10, 100, 1000)], 'pipeline': pipeline(args.seconds)}
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
