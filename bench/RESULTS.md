# Measured results

Measured on 2026-10-07. Apple M4, 10 CPU cores, 16 GiB memory, macOS 26.5.2, Python 3.12.13, C++20 with Apple Clang 17. Redpanda uses one CPU and 1 GiB in a local Linux container. The desktop, demo, and metrics services were active. These are local measurements.

## C++ binding and Python reference

Each run applies 200,000 prepared one-level SET updates. Each update validates the book and reads the best ten levels on both sides. The C++ result includes the Python binding call. Preparation and allocation of input batches happen before the timer. The Python reference uses dictionaries, atomic rollback on a crossed book, and sorted depth. Final depth must match. Histogram bookkeeping is included in elapsed throughput.

- 10 levels per side: C++ 299,961 updates/s; Python 379,591 updates/s; ratio 0.79x. C++ p50 1.167 us, p99 10.335 us, p99.9 103.743 us.
- 100 levels per side: C++ 533,903 updates/s; Python 219,890 updates/s; ratio 2.43x. C++ p50 1.167 us, p99 2.917 us, p99.9 16.959 us.
- 1000 levels per side: C++ 491,300 updates/s; Python 34,016 updates/s; ratio 14.44x. C++ p50 1.167 us, p99 3.501 us, p99.9 20.847 us.

The C++ binding was slower than the dictionary reference at ten levels in this run. Its advantage increased with depth. This comparison does not include JSON, Kafka, network, or disk costs. It does not establish a general speedup over all Python book structures.

## In-process pipeline

The 1,200-frame synthetic replay ran at 7,120 frames/s. Per-frame p50 was 115.391 us, p99 396.031 us, and p99.9 593.919 us. This includes raw JSON decode, schema validation, normalization, staged C++ state, gap checks, and canonical state encoding. It excludes storage and Kafka.

Replay state SHA-256: `f17d590bb04b358e86da81ba20b46cae4b6422e5b1fd3da106c771bfc3c8c099`.

## Kafka spike test

The input contains 1,000 synthetic snapshots across two books. The base rate is 200 raw frames/s. Each run has a fresh topic namespace and all four workers. Input timestamps are set before producer enqueue. Throughput includes the time to drain book history. Alert latency is measured at a separate consumer, so it includes alert delivery and observer polling. It is an upper bound on publication latency. Lag is sampled every 100 ms for the normalizer group. Its measured peak is a lower bound.

- 1 speed: 165 book states/s; 909 observed alerts. Receive-to-alert observation p50 379.391 ms, p99 1496.063 ms, p99.9 1678.335 ms. Peak sampled normalizer lag 20 raw frames. Book drain after the last send: 1.070 s.
- 10 speed: 860 book states/s; 500 observed alerts. Receive-to-alert observation p50 1446.911 ms, p99 1689.599 ms, p99.9 1693.695 ms. Peak sampled normalizer lag 138 raw frames. Book drain after the last send: 0.662 s.
- max speed: 878 book states/s; 500 observed alerts. Receive-to-alert observation p50 1662.975 ms, p99 1692.671 ms, p99.9 1701.887 ms. Peak sampled normalizer lag 511 raw frames. Book drain after the last send: 1.088 s.

These short runs use full synthetic snapshots. They do not measure a busy venue hour, hundreds of markets, or the maximum sustainable production rate. The number of alerts depends on which fresh quotes meet the threshold and how updates from separate partitions arrive.

## Reproduce

```sh
uv run python bench/run.py --operations 200000
uv run python bench/kafka_run.py
```

Raw measurement files are `measurements.json` and `kafka-measurements.json`. `live-snapshot-check.json` records a separate live REST comparison. A real busy-window test remains open.
