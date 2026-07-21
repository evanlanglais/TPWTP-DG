# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TPWTP-DG (Theme Park Wait Time Predictor - Data Gatherer) is a Go service that polls the [themeparks.wiki REST API](https://api.themeparks.wiki/v1) every 5 minutes to collect ride wait times from a US-park allowlist, storing them in PostgreSQL.

Its mandate is to keep a durable record that two readers leverage from the **same** data: **inspection** (an existing API service / frontend reading "now" = the latest row) and a **future ML predictor** (reading the same rows as-of past time `t` for training). Two invariants make that possible and must not be eroded by new features:

- **I1 — Full replayability:** every source response is persisted verbatim to `raw_observations` *before* projection into `wait_times` (raw is never the thing discarded).
- **I2 — Point-in-time correctness:** every derived output is either PIT-correct (value at `T` uses only data ≤ `T`, safe as an at-`t` feature) or explicitly inspection-only. Each `Metric` declares this via `Temporal()`.

`docs/ARCHITECTURE.md` is the canonical design doc (domain of ownership, invariants, PIT classification); read it before changing the pipeline's shape.

## Commands

- **Build:** `go build ./cmd/collector`
- **Run:** `DATABASE_URL=postgres://... go run ./cmd/collector`
- **Run with Docker:** `docker compose up`
- **Tidy deps:** `go mod tidy`
- **Vet:** `go vet ./...`

There are no test scripts configured yet.

## Architecture

### Data Flow

1. On startup, the collector syncs reference data: fetches all destinations and parks from `GET /v1/destinations`, then fetches children for each park from `GET /v1/entity/{id}/children` to populate attractions (and, from the children's locations, computes each destination's weather centroid)
2. Every 5 minutes (configurable via `POLL_INTERVAL`), it fans out `GET /v1/entity/{parkId}/live` for all parks — one goroutine per park, bounded only by the API semaphore (`API_CONCURRENCY`), not a worker pool
3. Each `/live` response is written verbatim to `raw_observations` (I1) **before** projection
4. Live data is filtered to `entityType == "ATTRACTION"` and bulk-inserted into the `wait_times` table, each row carrying a `source_observation_id` lineage pointer
5. New attractions discovered during live polling are auto-upserted
6. After each poll, derived processing runs (velocity/trends, transitions, and park-level metrics); each metric declares a PIT contract via `Temporal()`
7. Materialized views for daily stats are refreshed hourly
8. **If `WEATHER_API_KEY` is set**, a sibling weather poll runs on its own ticker (`WEATHER_POLL_INTERVAL`): one OpenWeatherMap fetch per destination centroid → written verbatim to `raw_observations` (I1, `source_type=openweather_onecall`) **before** projection → current conditions into `weather_observations` and the next ~6h hourly forecast into `weather_forecasts`, projected to each park in the destination. It feeds no `Metric`, so it does not trigger Layer 2.

### Key Packages

- **`cmd/collector/main.go`** — Entry point, config loading, signal handling, graceful shutdown
- **`internal/api/`** — HTTP client for themeparks.wiki (semaphore-based concurrency limiting) and Go struct types matching API responses
- **`internal/weather/`** — HTTP client + types for the OpenWeatherMap One Call 3.0 source (mirrors `internal/api`'s raw-carrier shape for I1); opt-in via `WEATHER_API_KEY`
- **`internal/collector/`** — Poll orchestrator: startup sync, ticker loop, worker pool, fan-out/fan-in, triggers Layer 2 processing
- **`internal/processor/`** — Layer 2 derived data: Metric interface, velocity computation, status transition detection
- **`internal/store/`** — PostgreSQL data access (pgx/v5): upserts, COPY-based bulk insert, partition management (Layer 1 in `postgres.go`, Layer 2 in `l2_store.go`)
- **`internal/config/`** — Environment variable configuration
- **`migrations/`** — Embedded SQL migrations with simple tracking table

### Database Schema (PostgreSQL)

- **`raw_observations`** — Verbatim source responses (I1): source_type, observed_at, received_at, payload (jsonb), metadata; partitioned by month on received_at
- **`destinations`** — Theme park resort groups (id, name, slug, latitude/longitude weather centroid)
- **`parks`** — Individual parks (id, destination_id, name, timezone)
- **`attractions`** — Rides/attractions (id, park_id, name, external_id)
- **`wait_times`** — Projected time-series data, partitioned by month (attraction_id, timestamp, status, standby_wait, …, source_observation_id lineage)
- **`weather_observations`** — Projected current conditions from the weather source, park-keyed, partitioned by month (**PIT-correct**; source_observation_id lineage)
- **`weather_forecasts`** — Projected bi-temporal forecast rows (issued_at + valid_at + horizon_minutes + precip_prob …), partitioned by month on issued_at (**PIT-correct only as-of issued_at**; never overwritten with the later actual)

#### Derived (`l2_`/`l3_` prefixes are historical names, not structural layers)

- **`l2_wait_velocity`** — Per-attraction velocity, EMA, and trend per poll tick (PIT-correct)
- **`l2_status_transitions`** — Records when rides change status (OPERATING→DOWN, etc.) with duration (PIT-correct)
- **`l3_park_crowd_pressure`** — Park-level wait-weighted velocity EMA (PIT-correct)
- **`l3_correlation_pairs`** — Pairwise Pearson r between rides' velocity series over a trailing window (PIT-correct)
- **`l3_transition_impact`** — Closure/opening crowd redistribution; **inspection-only** (embeds event+30min, ≥30min lag to use as a feature)
- **`l2_daily_attraction_stats` / `l2_daily_park_stats`** — Materialized views of daily aggregates; **inspection-only** (all-history)
- **`l2_processing_cursors`** — Tracks last-processed timestamp + logic_version per metric for incremental processing

See `docs/ARCHITECTURE.md` §11 for the point-in-time classification.

### Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `POLL_INTERVAL` | `5m` | How often to poll wait times |
| `API_CONCURRENCY` | `15` | Max concurrent API requests |
| `API_TIMEOUT` | `10s` | Per-request timeout |
| `LOG_LEVEL` | `info` | Logging verbosity (debug/info/warn/error) |
| `WEATHER_API_KEY` | (unset) | OpenWeatherMap One Call 3.0 key. **Presence enables the weather source**; unset = disabled |
| `WEATHER_POLL_INTERVAL` | `30m` | How often to poll weather (only when enabled) |
