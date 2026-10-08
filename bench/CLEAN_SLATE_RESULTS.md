# Fresh benchmark results

Checked on 2026-10-07 21:21 America/Toronto.

The C++ book is fast. Disk records and live book recovery need work. This run is a local measurement on your computer.

## Main results

- C++ update plus top-ten depth at 1,000 levels per side: **234,227 updates/s**. Median of three runs. Each run used 200,000 updates. Per-update p99 was 13.671 microseconds.
- In-memory pipeline replay: **3,614 frames/s**, median of three 1,200-frame runs. This excludes disk and Kafka.
- Kafka burst: **1,271 book states/s** for 2,000 input frames. Receive-to-alert observation p99 was 2,326.5 ms. All four workers ran against a separate empty broker.
- Live source: **70.5 frames/s** and **127.2 book states/s** across 16 native books and 8 markets. This is the arrival rate observed during 120.4 seconds, not a maximum capacity test.
- Durable capture-to-processing latency: p50 **4.25 ms**, p95 **10.13 ms**, p99 **27.06 ms**.
- Local page response: p50 **30.0 ms**, p95 **158.3 ms**, p99 **692.4 ms**. There were 0 request errors in 107 requests.
- CPU: **46.2% of one core**. Peak process memory: **138.3 MiB**.
- Disk after the live run: **194.6 MiB** of logical data in **15,946 files**. Allocated disk space was 241.8 MiB.
- Fresh-process recovery: **9.28 seconds** to replay 8,486 raw records and recover 16 books. Final state hash and raw/event counts matched the capture.

## Live book availability

All native books received an initial snapshot within 2.26 seconds. Books were live in **81.5% of individual sampled book checks**. All 16 books were live together in 76 of 107 samples.

The guards reported 40 top-price mismatches and 1094 subsequent updates without a live snapshot. Recovery held these books out of price and gap calculations. The 16 disconnect records at the end came from stopping the test instance. The mismatch records need review before changing the guards. Some recovery also comes from the existing periodic snapshot policy.

Latest changed live books seen by the page observer had receive-to-view p50 101.4 ms and p99 824.9 ms. These samples cover the latest version seen at each page update. They do not measure every source event. The page sends snapshots at a nominal 500 ms interval.

## Storage

Capture spool: 110.6 MiB. Raw Parquet: 38.6 MiB in 8,486 files. Event Parquet: 38.9 MiB in 7,457 files. Other databases: 6.6 MiB.

A simple size/time calculation gives about 136.3 GiB per day at this same load. This is an extrapolation from two minutes. It includes initial files, assumes the same activity continues, and excludes cleanup. It is not a measured daily volume.

The next useful changes are to batch Parquet writes, reduce repeated subscription metadata in capture records, and inspect the captured top-price mismatches. A longer run can then test sustained load and retention.

## Method and limits

The test root started with zero records, zero subscriptions, and zero books. The benchmark broker started with zero application topics. Each broker run used its own topic namespace and new worker state. No main-dashboard records were reset. The main dashboard and desktop remained active. System caches were left as normal.

The host was Apple M4 with 10 CPU cores and 16 GiB memory. Native compute ran on macOS with Python 3.12.13. The separate Redpanda v26.1.14 broker used one CPU and 1 GiB memory in Docker. The C++ comparison includes the Python binding, validation, depth conversion, and histogram bookkeeping. Final depth matched the Python dictionary reference in every run. Ratios of median throughput were 1.17x at 10 levels, 2.07x at 100 levels, and 12.00x at 1,000 levels.

Compute replay and Kafka spikes used explicitly synthetic controlled input. The isolated live run used public Polymarket capture with demo mode disabled. Live timings include benchmark probes. The live stage timing includes book cloning, the native book, and output state creation. Processing timing also includes parsing, validation, and archive writes. Capture-to-processing timing includes the durable spool write. Kafka alert timing includes consumer observation. Sampled lag is a lower bound. The burst is short and does not establish a sustained production rate.

One frame can change several books. Book-state counts include trade and control events. Startup time after Python imports was 0.038 seconds; this excludes dependency import time and market lookup. Record totals include shutdown control records. Live percentiles use sorted observed samples. Compute and Kafka use three-significant-digit HDR histograms.

## Reproduce

Use `bench/run.py` three times with `--operations 200000`. Run `bench/kafka_run.py --bootstrap localhost:29092 --seconds 10` against a separate empty broker. Run `bench/live_run.py --root NEW_EMPTY_DIRECTORY --seconds 120 --out live.json`, then use its `--recover` option in a new process. The saved source links identify the exact live events used. Market availability can change between runs.

Evidence files: `compute.json`, `compute-run-1.json`, `compute-run-2.json`, `compute-run-3.json`, `kafka.json`, `live.json`, `recovery.json`, `disk-breakdown.json`, `live-samples.csv`, and `run-context.json`.

Recovery state SHA-256: `b4447735a544002540f0ca21a59d7dfb52da95bd59e31ebf5693d2d1a2d00bb8`.
