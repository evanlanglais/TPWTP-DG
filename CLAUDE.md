# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

TPWTP-DG (Theme Park Wait Time Predictor - Data Gatherer) is a Go service that polls the [themeparks.wiki REST API](https://api.themeparks.wiki/v1) every 5 minutes to collect real-time ride wait times from all theme parks worldwide, storing them in PostgreSQL.

## Commands

- **Build:** `go build ./cmd/collector`
- **Run:** `DATABASE_URL=postgres://... go run ./cmd/collector`
- **Run with Docker:** `docker compose up`
- **Tidy deps:** `go mod tidy`
- **Vet:** `go vet ./...`

There are no test scripts configured yet.

## Architecture

### Data Flow

1. On startup, the collector syncs reference data: fetches all destinations and parks from `GET /v1/destinations`, then fetches children for each park from `GET /v1/entity/{id}/children` to populate attractions
2. Every 5 minutes (configurable via `POLL_INTERVAL`), it fans out `GET /v1/entity/{parkId}/live` for all parks using a bounded worker pool
3. Live data is filtered to `entityType == "ATTRACTION"` and bulk-inserted into the `wait_times` table (Layer 1)
4. New attractions discovered during live polling are auto-upserted
5. After each poll, Layer 2 processing runs: computes velocity/trends and detects status transitions from the raw data
6. Materialized views for daily stats are refreshed hourly

### Key Packages

- **`cmd/collector/main.go`** — Entry point, config loading, signal handling, graceful shutdown
- **`internal/api/`** — HTTP client for themeparks.wiki (semaphore-based concurrency limiting) and Go struct types matching API responses
- **`internal/collector/`** — Poll orchestrator: startup sync, ticker loop, worker pool, fan-out/fan-in, triggers Layer 2 processing
- **`internal/processor/`** — Layer 2 derived data: Metric interface, velocity computation, status transition detection
- **`internal/store/`** — PostgreSQL data access (pgx/v5): upserts, COPY-based bulk insert, partition management (Layer 1 in `postgres.go`, Layer 2 in `l2_store.go`)
- **`internal/config/`** — Environment variable configuration
- **`migrations/`** — Embedded SQL migrations with simple tracking table

### Database Schema (PostgreSQL)

- **`destinations`** — Theme park resort groups (id, name, slug)
- **`parks`** — Individual parks (id, destination_id, name, timezone)
- **`attractions`** — Rides/attractions (id, park_id, name, external_id)
- **`wait_times`** — Time-series data, partitioned by month (attraction_id, timestamp, status, standby_wait, etc.)

#### Layer 2 (Derived — `l2_` prefix)

- **`l2_wait_velocity`** — Per-attraction velocity, EMA, and trend per poll tick (partitioned by month)
- **`l2_status_transitions`** — Records when rides change status (OPERATING→DOWN, etc.) with duration
- **`l2_daily_attraction_stats`** — Materialized view: min/max/avg/median/p90 wait per attraction per day
- **`l2_daily_park_stats`** — Materialized view: park-level daily aggregates
- **`l2_processing_cursors`** — Tracks last-processed timestamp per metric for incremental processing

### Configuration

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL connection string |
| `POLL_INTERVAL` | `5m` | How often to poll wait times |
| `API_CONCURRENCY` | `15` | Max concurrent API requests |
| `API_TIMEOUT` | `10s` | Per-request timeout |
| `LOG_LEVEL` | `info` | Logging verbosity (debug/info/warn/error) |
