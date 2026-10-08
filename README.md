# Prediction market data platform

A working market data platform with a C++20 order book. Python manages live connections, Kafka transactions, storage, replay, and the API. A React dashboard shows depth, data quality, subscriptions, and price gaps.

**Status: research prototype.** A two-minute live benchmark had valid book checks in 81.5% of samples and stored 194.6 MiB. Quote recovery and storage retention need further work before production use. See the [fresh benchmark results](bench/CLEAN_SLATE_RESULTS.md) and [validation limits](VALIDATION.md).

This project reads market data. It has no order submission or wallet code. The book stores total quantity at each price level. Public feeds do not supply individual queue positions.

The code uses the [MIT license](LICENSE). Captured market data remains subject to the venue's terms.

Build and test status: [GitHub CI](https://github.com/jerrylin-23/pm-data-platform/actions/workflows/ci.yml).

## Run on your computer

Install Docker with Compose. Download and extract this project. Run this command from its directory:

```sh
docker compose -f compose.local.yml up --build -d
```

Open [the dashboard](http://127.0.0.1:8765). Use **Track a market** to add a live Polymarket feed. This feed needs no API key. The C++ book and dashboard run in one container. This mode does not need Kafka, Python, Node, or a compiler installed on the host.

Each user has their own saved markets and records. Docker keeps these in the `local-data` volume. The dashboard accepts connections only from the same computer. Internet access is needed for live data. Capture stops when the computer sleeps or Docker stops. The app starts again when Docker starts, unless you have stopped it.

Stop the app with:

```sh
docker compose -f compose.local.yml down
```

This command keeps saved data. Run the start command again to use it. Use `PM_HTTP_PORT=8766` in `.env` if port 8765 is in use. Set `PM_DEMO=1` in `.env` for synthetic demo books. Kalshi needs a separate key setup, described below.

## Start the full Docker system

Start the full demo from this directory:

```sh
PM_DEMO=1 docker compose --profile platform up --build -d
```

Open [the dashboard](http://127.0.0.1:8765). The demo has two synthetic books and one synthetic pair. Each book and alert has a source label. Use **Track a market** to add a real public Polymarket feed.

Other local pages:

- [Redpanda Console](http://127.0.0.1:8081)
- [Grafana](http://127.0.0.1:3000/d/pm-data-platform)
- [Prometheus](http://127.0.0.1:9090)

Use `PM_HTTP_PORT=8766` if port 8765 is in use. Use `PM_METRICS_PORT=19100` if port 9100 is in use. Remove `PM_DEMO=1` for an empty live dashboard. Stop the project with `docker compose --profile platform down`. The normal stop keeps the data volumes.

## Start on the host

Use Python 3.12, a C++20 compiler, Node 22, Docker, and `uv`. The Python and dashboard dependency locks are included. The built dashboard is also included.

```sh
uv sync --frozen --extra dev
cd dashboard
npm ci
npm run build
cd ..
docker compose up -d redpanda console prometheus grafana
uv run pm --bootstrap localhost:19092 stack --demo --port 8765
```

`stack` starts the normalizer, book worker, archiver, gap worker, capture service, and dashboard. It restarts a worker that exits. Worker logs go in `data/logs`. One stack owns one set of topics. Use `PM_TOPIC_NAMESPACE` to give separate test or demo stacks their own topics.

For a demo without Kafka:

```sh
uv run pm serve --demo --port 8765
```

The local mode uses the same normalizer and native book. It stores raw and normalized records on disk. Pending capture delivery gives page updates time to run between records. Kafka transactions apply to `stack` and Kafka workers.

## Track markets

Use the dashboard search box. You can enter a keyword or a Polymarket or Kalshi HTTPS link. An event with several markets shows a selection list. Saved subscriptions survive a restart.

Events group related market choices in one card. Count ranges, named winners, teams, and thresholds use the choice labels from the venue. Search results let you track selected choices or all open choices. Closed markets are excluded.

The number of choices, their names, and sports market types come from Polymarket data. Sports events show type buttons for the groups returned by the venue. These can include moneyline, spreads, totals, and player markets. A new type uses its venue name without a code change. Each type has its own chart. Named sides such as team names and Over/Under appear in the price cards, chart labels, depth controls, and source details. Repeated short choice labels use the full market question.

For example, paste `https://polymarket.com/event/elon-musk-of-tweets-october-2-october-9-2026`. Track all open ranges, then select **220-239** to view its YES and NO book. The event chart overlays each tracked range's YES midpoint on the same time and probability axes. It does not adjust prices to make their sum equal 100%.

A single tracked choice uses the paired chart. Named binary sides such as Up and Down keep their venue labels. Use the side buttons to change the depth view. If the feed supplies one side, the other uses opposite quotes and has a Calculated label. The chart keeps the last 60 updates in the current session. A stale quote or a book without both a bid and an ask leaves a gap.

Each choice keeps its own native book. The app supports market formats exposed by the Polymarket and Kalshi order book feeds. It does not connect to other betting sites. Older saved subscriptions still work; track the same link again to add event labels.

Polymarket subscriptions prefer `clobTokenIds` for the order book feed. When re-tracking changes a market's instrument IDs, the app replaces the saved pair and removes the old IDs from capture. Retained archive books do not add extra outcomes to the active view. Search results keep all returned choices. The tracking limit still applies.

The CLI supports the same actions:

```sh
uv run pm --bootstrap localhost:19092 track search bitcoin --venue polymarket
uv run pm --bootstrap localhost:19092 track add 'https://polymarket.com/market/will-bitcoin-reach-105k-in-october-2026'
uv run pm track list
uv run pm --bootstrap localhost:19092 track remove 5170728
uv run pm --bootstrap localhost:19092 book
```

The example market was open during the build on 2026-10-07. Search for a current market if it has closed. For a multi-market event, add `--market ID` for each selected market, or use `--all`.

The resolver accepts only the exact Polymarket and Kalshi hosts, including their `www` forms. It queries the official API. It does not fetch the pasted page. The default limit is 200 distinct markets. Polymarket YES and NO tokens count as one market. Connections use batches of at most 100 instruments.

Kalshi search filters the first 1,000 open events. The response states this limit. A direct link provides a more specific lookup.

## Kalshi access

Kalshi requires a signed WebSocket connection. The code uses RSA-PSS with SHA-256. The signing path and signature have an offline test.

For a host run, copy `.env.example` to `.env`. Set `KALSHI_KEY_ID` and `KALSHI_PRIVATE_KEY_PATH`. The second value is the path to your private PEM file. Keep that file outside source control. Load the environment before you start the CLI:

```sh
set -a
source .env
set +a
uv run pm stack --port 8765
```

The CLI does not load `.env` by itself. Docker Compose uses `.env` for its settings. The default container setup does not mount a private key. Use the host run for authenticated Kalshi capture, or add a private read-only key mount and the two environment values to the container configuration.

No Kalshi account key was available for this build. The live Kalshi connection is implemented but has not had an authenticated live test.

## Record, replay, and export

Create a fixed ten-minute synthetic dataset:

```sh
uv run pm demo --seconds 600 --out data/demo
uv run pm replay --archive data/demo/archive --raw --out data/replay/raw.jsonl
uv run pm replay --archive data/demo/archive --raw --out data/replay/again.jsonl
```

The two output files must have the same SHA-256. The demo command can run again on the same directory. Its second run adds zero archive rows.

For a bounded live capture, first save subscriptions with `track add`, then run:

```sh
uv run pm capture --seconds 60
uv run pm replay --archive data/archive --raw --out data/replay/live.jsonl
```

Replay accepts UTC timestamps or integer nanoseconds. `--from` is inclusive. `--to` is exclusive. Use `--speed 1x`, `--speed 10x`, or `--speed max`. Time in the book logic comes from records. A replay speed changes only the wait between records.

Replay preserves order within each source partition. It merges partitions by receive time and a stable tie break. A clock regression cannot move a record ahead of an earlier offset in its partition. For a time window, replay first builds the required starting state. It fails if a delta has no starting snapshot. At the end boundary, it stops each partition at its first excluded record.

A window that crosses backward through its start is rejected. Such a window cannot use one initial checkpoint without losing replay context. Replay the full capture, or choose a start before that clock change.

Supported exports:

```sh
uv run pm export --archive data/demo/archive --format jsonl --out data/export/events.jsonl
uv run pm export --archive data/demo/archive --format csv --out data/export/events.csv
uv run pm export --archive data/demo/archive --format parquet --out data/export/events.parquet
uv run pm export --archive data/demo/archive --format l2-csv --every-events 10 --out data/export/depth.csv
uv run pm export --archive data/archive --venue kalshi --market MARKET-TICKER --format ohlcv-csv --interval 1m --out data/export/bars.csv
uv run pm replay --input data/export/events.jsonl --out data/replay/roundtrip.jsonl
```

Add `--from`, `--to`, `--venue`, and `--market` to select data. The market filter accepts a market ID or instrument ID. OHLCV needs one instrument. Intervals are `1s`, `1m`, and `1h`. The snapshot demo contains no trades, so its OHLCV file has a header only. Trade fixtures verify bar loading in pandas and Backtrader.

Each export has a `.manifest.json` file. It records the schema, range, row count, SHA-256, and initial book checkpoint. Keep the JSONL manifest with the file for a window that starts after a snapshot. CSV and Parquet have one row per changed level and a batch index. JSONL preserves the complete event envelope.

Prices use four decimal places. Quantities use six. File formatting uses integer division. It does not print floating point prices. Deterministic Parquet output is tested with the pinned Arrow version.

## Book and delivery rules

`cpp/book.hpp` is the native core. It has 10,001 price slots per side and a bitmap index for occupied levels. It validates a complete batch before applying it. A crossed batch rolls back. A snapshot replaces the full book in one step.

Prices are integer probability units from 0 to 10,000. Quantities are signed 64-bit integers with six decimal places. Polymarket updates set a level's total quantity. Kalshi deltas add a signed change. Kalshi NO bids map to YES asks at `10000 - price`.

Raw WebSocket market frames go to a durable SQLite spool before JSON parsing. Protocol ping and pong messages are excluded. Pending sends survive a restart. A stable connection ID, epoch, and counter allow the normalizer to ignore a republished capture even when it gets a new Kafka offset.

Raw Kafka records use the connection ID as the key. This preserves one connection's sequence and handles frames with several assets. Normalized events use `venue:instrument_id` as the key. The book uses only generic events.

The normalizer and book worker stage their state. They publish results, full checkpoints, and next input offsets in one Kafka transaction. Consumers use `read_committed`. An abort discards staged state and rewinds input. A worker with an ambiguous transaction result exits, then restores committed checkpoints on restart.

The archiver writes Parquet, syncs the file, then commits its SQLite index before it commits Kafka offsets. Repeated offsets do not add rows. A changed payload at the same offset is an error. An unindexed file from an interrupted write is not visible to archive readers.

A missing snapshot, sequence gap, invalid quantity, crossed book, reported top mismatch, backward timestamp, disconnect, or quiet-feed timeout prevents a book from supplying gap alerts. Recovery requires a fresh snapshot. Closed books stay closed across a connection change.

Kalshi sequence checks use the connection, epoch, and subscription ID. Command responses with a sequence also advance that scope. A gap invalidates every known member of the scope. Market selection changes reconnect both Kalshi channels together. Polymarket subscriptions change on the existing connection.

Polymarket has no documented sequence guarantee for this feed. Its continuity label is `unverified`. The adapter checks top prices included in each depth-update batch. Separate top-price notifications can arrive before their depth update. The raw archive retains these notifications; they do not change book state. The adapter does not calculate the venue's undocumented book hash. It opens a new connection for a fresh snapshot every 60 seconds and after 15 seconds without market data. Recovery requests wait at least five seconds after the previous subscription. Requests from an older subscription cannot interrupt the new connection. A quiet book can therefore enter recovery even when the network is healthy.

## Topics and metrics

The topic set includes `raw.polymarket`, `raw.kalshi`, `md.events`, `book.state`, `book.history`, `book.checkpoints`, `normalizer.checkpoints`, `gap.checkpoints`, `alerts.gaps`, `dq.violations`, `control.subscriptions`, and `control.resync`.

Raw and event history retain seven days. Alerts and data quality events retain 30 days. State, checkpoints, and controls are compacted. `book.history` is separate from compacted state so the gap worker can read each update.

Prometheus collects message counts, approximate cached consumer lag, sequence failures, resyncs, data quality counts, and receive-to-alert latency. The host stack exposes ports 9100 through 9104. `grafana/dashboard.json` contains the dashboard.

## Tests and measurements

```sh
uv run pytest
uv run ruff check src tests bench
docker compose up -d redpanda
PM_KAFKA_TESTS=1 uv run pytest tests/test_kafka_integration.py
uv run python bench/run.py --operations 200000
uv run python bench/kafka_run.py
```

The broker tests create separate topics and remove those topics at the end. They kill only workers they start. They check normalizer, book worker, and archiver recovery. A forced transaction abort checks that output stays hidden and input is retried.

Golden fixtures include a real 60-second Polymarket capture and synthetic ten-minute fixtures for both venues. A separate synthetic Kalshi fixture has signed deltas. Its tests remove each possible delta in the first minute and check that levels stay fixed until the next snapshot.

See [bench/RESULTS.md](bench/RESULTS.md) for measured results and their limits. See [VALIDATION.md](VALIDATION.md) for the build checks.

The fresh benchmark on 2026-10-07 used empty test state and a separate broker. See [bench/CLEAN_SLATE_RESULTS.md](bench/CLEAN_SLATE_RESULTS.md) for compute, live capture, disk use, and recovery results. The live run found storage growth and quote-recovery limits. `bench/live_run.py` repeats this check with a new empty directory and can measure recovery in a new process.

## Work still needed

- Test Kalshi with a real key and save a real ten-minute capture. Confirm subscription sequence scope against that session.
- Record a busy hour from the venues. The current spike test uses synthetic snapshots across two books.
- Add 20 to 30 reviewed market pairs. `pairs.yml` starts empty. The demo pair is added only when demo mode is selected.
- Extend process recovery tests to broker loss, ingest process kills, and multiple worker assignments. The current stack uses one process per service and stable transaction IDs.
- Add retention for delivered spool rows, checkpoint connection maps, and archive files. Current archive queries load a selected kind into memory. Large archives need a streaming query path.
- Add verified venue hash checks if the venue publishes a stable hash specification. Add NautilusTrader and hftbacktest exports after the main path is complete.

## Sources

The implementation uses current official documentation checked on 2026-10-07:

- [Polymarket real-time data](https://docs.polymarket.com/market-data/realtime-data)
- [Polymarket market discovery](https://docs.polymarket.com/market-data/discover-markets)
- [Polymarket markets and events](https://docs.polymarket.com/concepts/markets-events)
- [Polymarket sports market types](https://docs.polymarket.com/api-reference/sports/get-valid-sports-market-types)
- [Polymarket WebSocket protocol notes](https://github.com/Polymarket/agent-skills/blob/main/websocket.md)
- [Kalshi WebSocket quick start](https://docs.kalshi.com/getting_started/quick_start_websockets)
- [Kalshi order book updates](https://docs.kalshi.com/websockets/orderbook-updates)
- [Kalshi fixed point migration](https://docs.kalshi.com/getting_started/fixed_point_migration)
- [Kalshi order book responses](https://docs.kalshi.com/getting_started/orderbook_responses)
- [Redpanda local setup](https://docs.redpanda.com/streaming/26.1/get-started/quick-start/)
