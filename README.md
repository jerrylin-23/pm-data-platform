# Prediction market data platform

Live Polymarket order books with a C++20 core and a React dashboard. Python handles capture, storage, and replay. The app reads market data and does not place orders.

## Run locally

Install Docker with Compose, then run:

```sh
git clone https://github.com/jerrylin-23/pm-data-platform.git
cd pm-data-platform
docker compose -f compose.local.yml up --build -d
```

Open [localhost:8765](http://127.0.0.1:8765). Use **Track a market** to search or paste a Polymarket market or event link. Public Polymarket data needs no API key.

Saved markets and captured data stay in a local Docker volume. Stop the app with:

```sh
docker compose -f compose.local.yml down
```

Stopping keeps your data. Capture stops when the computer sleeps or Docker stops.

## Features

- C++20 price-level books with snapshot and delta updates.
- Paired YES/NO depth and shared price charts.
- Binary markets, count ranges, named choices, and sports groups from venue data.
- Raw capture, deterministic replay, and JSONL, CSV, and Parquet exports.
- Optional Kafka pipeline with Redpanda, Prometheus, and Grafana.

## Development

Use Python 3.12, a C++20 compiler, and `uv`. The built dashboard is included.

```sh
uv sync --frozen --extra dev
uv run pm serve --port 8765
```

For tests and checks:

```sh
uv run pytest
uv run ruff check src tests bench
```

Use `uv run pm --help` for capture, replay, and export commands. Frontend source is in `dashboard/`.

## Status

Research prototype. Quote recovery and storage retention need further work. A Kalshi adapter is included, but its authenticated live connection has not been tested.

[Benchmarks](bench/CLEAN_SLATE_RESULTS.md) · [Validation](VALIDATION.md) · [CI](https://github.com/jerrylin-23/pm-data-platform/actions/workflows/ci.yml) · [MIT license](LICENSE)
