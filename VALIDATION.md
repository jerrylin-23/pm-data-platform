# Build checks

Checked on 2026-10-07 on Apple M4 with Python 3.12.13.

## Passed

- GitHub CI: the [first public build](https://github.com/jerrylin-23/pm-data-platform/actions/runs/37716064788) passed on a Linux runner. It built the Python package and dashboard, checked the C++ book with sanitizers, ran Ruff, and ran the test set with real Kafka integration enabled.
- Full test set: **95 passed**. The main run passed 93 tests. A separate run passed the two real Kafka integration tests.
- Python checks: Ruff passed for source, tests, and benchmark scripts.
- Dashboard: TypeScript checks and the Vite production build passed.
- Container: the application image built with its native C++ module and pinned Python dependencies.
- Local install: `compose.local.yml` started one container with no Kafka. Live Polymarket YES and NO books received data. After container removal and recreation, both saved subscriptions and all 391 prior archive rows remained. All prior Parquet file hashes matched. The published port accepts only local connections. See `bench/local-install-check.json`.
- Event choices: the tweet-count example resolved to 21 open ranges and 42 native instrument books. One event card and 21 chart series appeared. The selected range and its YES/NO depth controls worked. A single tracked range used the paired chart. The layout had no horizontal overflow at 471 and 1280 pixels. See `bench/multi-choice-ui-check.json`.
- Event persistence: all 21 choice labels and 42 subscriptions survived local container recreation. All 734 prior archive rows and 464 Parquet file hashes remained. Event removal left no saved choices or displayed books in the separate test app. See `bench/multi-choice-local-check.json`.
- Event recovery: all 42 books were live at each five-second sample during a 95-second check. New connections supplied fresh snapshots. Recovery requests from earlier subscriptions were ignored. The source timestamp checks remained active. See `bench/multi-choice-live-check.json`.
- Live mode: the running dashboard has synthetic capture disabled. No sample books, demo banner, or synthetic gap alerts are shown. An offline regression test checks retained synthetic records in both live and demo modes.
- Dynamic formats: captured public metadata covers binary, ranges, named choices, Up/Down, head-to-head sports, mixed sports types, three-way sports, and a 194-market sports event. Tests preserve native token IDs and outcome labels. A search regression keeps all 106 instrument records for 53 choices. Unknown type names and repeated short labels have regression checks. See `tests/fixtures/polymarket-formats.json`.
- Dynamic live lookup: eight event links resolved with current venue counts. A fresh local check received 30 native books for 15 selected markets across six events. Each book preserved its native outcome labels. The large sports event returned 171 open markets during this check. See `bench/dynamic-formats-final-live-check.json`.
- Dynamic UI: moneyline, spreads, totals, and three-way draw controls worked. One market showed its two native outcomes. Multiple choices shared one chart within their market type. Choice changes kept chart history; type changes reset it. The page had no horizontal overflow at 471 and 1280 pixels. No browser console errors were recorded. See `bench/dynamic-formats-ui-check.json`.
- Local response: pending capture delivery gives other tasks time to run between records. A regression test checks this behavior without timing assumptions. All 30 native books were live at each of 15 samples over about 30 seconds. Requests took at most 0.483 seconds and source updates continued. This short check does not establish sustained busy-window capacity. See `bench/dynamic-local-response-check.json`.
- Fresh benchmark: new books, an empty capture directory, and a separate empty broker were used. Three 200,000-update compute trials matched the Python reference. A two-minute live run received 8,486 frames across 16 books. API requests had no errors. Fresh-process replay recovered the same state hash and record counts in 9.28 seconds. Live book checks were valid in 81.5% of samples; top-price mismatches triggered recovery. Records used 194.6 MiB in 15,946 files. These are measured limits, not production-capacity claims. See `bench/CLEAN_SLATE_RESULTS.md` and `bench/clean-slate-summary.json`.
- Feed checks: sports order book tokens returned live REST depth where the distinct position IDs returned no book. Re-tracking replaces the old IDs and publishes their removal. A captured top-price notification arrived before its matching depth delta. Regression tests keep the book live for this order and retain the mismatch guard for reported top prices inside depth updates.
- C++: native checks passed on macOS. AddressSanitizer and UndefinedBehaviorSanitizer checks passed inside the Linux application image.
- Live Polymarket: two instrument books matched the complete depth of fresh REST snapshots. No book update occurred during either matching request. See `bench/live-snapshot-check.json`.
- Kafka recovery: the test killed and restarted the normalizer, book worker, and archiver. Final books matched raw replay. Archive event counts stayed correct.
- Kafka abort: output from an aborted transaction stayed hidden. The worker retried the same input offset. Committed output matched staged book state.
- Archive idempotence: a second run on the 1,200-frame demo added zero raw rows and zero event rows.
- Replay: full JSONL export and re-import produced the same state hash. A partial window retained its initial checkpoint. Re-export also retained that state.
- Exports: JSONL, CSV, Parquet, OHLCV CSV, and L2 CSV were byte-identical on repeated test runs. Trade bars loaded into pandas and Backtrader.
- Live tracking: removal took effect without a process restart. Re-addition received a fresh live snapshot. See `bench/live-tracking-check.json`.
- Source fixtures: real Polymarket capture and three explicitly synthetic protocol fixtures have stored hashes.
- C++ property tests compare native depth with a Python dictionary reference. Sequence-drop tests hold the book stale until the next snapshot.

The CLI demo and JSONL round trip produced 1,200 book states with this SHA-256:

```text
f17d590bb04b358e86da81ba20b46cae4b6422e5b1fd3da106c771bfc3c8c099
```

## Not yet checked

Authenticated live Kalshi capture, real one-hour busy-window performance, broker failure recovery, a full ingest-container kill test, and production deployment remain open. Full Windows and Linux host installation remain untested. GitHub CI checks the Linux build and tests on each push and pull request.

The real Polymarket fixture covers a requested 60-second capture. The ten-minute Kalshi fixtures are synthetic. They are not evidence of a live Kalshi connection.
