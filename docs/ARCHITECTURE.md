# TPWTP-DG Architecture

**Status:** Describes the system as it is built today. Every mechanism named here maps to real
code; where the code has rough edges, this doc says so rather than describing an intended future.

---

## 1. Purpose

TPWTP-DG has one job: **keep a durable, honest historical record of theme-park ride wait times,
in a form that two different readers can leverage from the *same* data.**

1. **Inspection** — an existing API service reads these tables to serve wait-time data to a
   frontend. It wants the current state: "now" = the latest row.
2. **A future predictor** (not built here) will learn to forecast near-term wait-time *movement*.
   It wants the historical state *as it was knowable at each past moment* — the same rows, read
   as-of time `t` for training and at the tail for inference.

The whole design goal is that those two readers consume **one** record and **one** set of derived
signals — no duplicate pipelines, no separate "ML tables." Section 3 states the two invariants
that make that possible; if a future feature would break them, it doesn't belong here.

---

## 2. Domain of ownership

This component owns exactly three things, and deliberately not a fourth:

- **Observation** — polling the source and writing down what was seen (raw + projected).
- **The record** — the durable, replayable source of truth (`raw_observations`, `wait_times`).
- **Causal derivation** — signals computable from the past only (velocity, transitions, crowd
  pressure, …), served identically to both readers.

It does **not** own labels, feature-assembly, model training, or inference. Those belong to the
separate ML component, which reads this component's record and its point-in-time signals. The
boundary is simple: **causal-and-shared lives here; future-dependent-and-model-specific lives
there.** Anything computable from data at-or-before its own timestamp is a candidate to live here
once and be read by everyone; anything that needs the future (a label) or is model-specific (a
training matrix) does not.

---

## 3. Invariants

Two non-negotiable properties. They are what let one record serve both readers; guard them.

**I1 — Full replayability of raw sources.** Every source response is persisted verbatim
(`raw_observations`, §6) *before* it is projected into domain tables. We keep the raw body,
`observed_at` (when the fact was true upstream) and `received_at` (when we wrote it). *Rationale:*
a feature we haven't thought of yet can only be built later if the raw signal still exists;
projection is lossy and irreversible, so we never let the raw bytes be the thing we discard.

**I2 — Point-in-time correctness of processed data.** Every derived output is either
**PIT-correct** — its value stamped at time `T` uses only data at-or-before `T`, so it is safe to
read as an at-`t` feature — or it is explicitly labeled **inspection-only** with the lag after
which it becomes safe. *Rationale:* a signal that secretly peeks at the future inflates a model in
training and collapses in production; the only defense is to make each signal's temporal contract
explicit and enforced in code (§7). Every metric declares its contract; §11 is the current
classification.

---

## 4. Scope

- **Sources:**
  - [themeparks.wiki REST API](https://api.themeparks.wiki/v1) — wait times (always on).
  - [OpenWeatherMap One Call 3.0](https://openweathermap.org/api/one-call-3) — weather,
    **opt-in**: polled only when `WEATHER_API_KEY` is set, so existing deployments are
    unaffected. It is the second source the §9 seam anticipated. See §6.
- **Coverage:** US parks only. The set is a hardcoded allowlist of destination slugs in
  `internal/collector/collector.go` (`usDestinations`). Anything not on the list is skipped on
  sync and pruned from the DB if previously stored.
- **Cadence:** wait times every 5 minutes (`POLL_INTERVAL`); weather on its own slower ticker
  (`WEATHER_POLL_INTERVAL`, default 30 min) because forecasts refresh ~hourly and its free-tier
  call budget is finite.

---

## 5. How it works

One Go binary (`cmd/collector`). Postgres is the entire substrate — no queue, no scheduler, no
second service. The lifecycle:

```
startup
  ├─ load config, connect pool, run embedded SQL migrations
  ├─ syncReferenceData   destinations → parks → attractions (+ timezones)
  └─ syncSchedules       park open/close windows
        │
        ▼
loop (every 5 min)                          (every 24 h)
  pollAllParks                              re-sync reference data + schedules
    ├─ one goroutine per park ─┐
    │    pollPark:             │  (concurrency bounded only by the
    │      GET /entity/{id}/live │   API semaphore, API_CONCURRENCY=15)
    │      write RAW response → raw_observations   (I1: before projection)
    │      project ATTRACTION rows → wait_times    (stamped with source lineage)
    │      filter: park-closed, then unchanged (dedup)
    │      COPY-insert into wait_times
    └─ wait for all parks ─────┘
    └─ RunAfterPoll  ← synchronous derived processing, then hourly view refresh
```

Key files: `cmd/collector/main.go` (wiring), `internal/collector/collector.go`
(`Run` / `pollAllParks` / `pollPark`), `internal/processor/processor.go` (derivation),
`internal/store/*.go` (all SQL), `migrations/*.sql` (schema).

**Each poll captures twice:** first the raw `/live` response is written verbatim to
`raw_observations` (I1), then each live `ATTRACTION` entity is projected into a `wait_times` row
(status, standby wait, single-rider wait, return-time window) carrying a `source_observation_id`
pointer back to the raw row. New attractions seen during a poll are auto-discovered and upserted
first.

**Two filters** run before the `wait_times` insert (they shrink the *projection*, never the raw
record):
- *park-closed:* when the park's schedule says it's closed, drop `OPERATING` rows (the API often
  reports stale "operating" values off-hours). Keeps DOWN/CLOSED/etc.
- *dedup:* drop non-`OPERATING` rows identical to the last one seen for that ride. A ride that
  stays DOWN for hours produces **one** `wait_times` row, not dozens.

---

## 6. The data model, by purpose

The `l2_`/`l3_` table prefixes are historical names, not layers with structural meaning. What
matters is what each table is *for*.

### The raw record — `raw_observations`
The replayable substrate (I1). One row per source fetch: `source_type`, `observed_at`,
`received_at`, `payload` (the verbatim JSON body), `metadata` (park_id or destination_id, http
status, latency, and on a failed fetch the error / non-JSON body). Range-partitioned by month on
`received_at`, partitions created lazily in Go. `source_type` now discriminates two sources
(`themeparks_wiki_live`, `openweather_onecall`) writing through this one path; a source-registry
table remains the seam if a third is added (§9). Each response self-identifies its subject inside
`payload` (a `/live` response its park, a weather response its coordinate).

### The projected record — `wait_times`
The load-bearing time series for inspection. One row per `(attraction_id, timestamp)`; columns:
`status`, `standby_wait`, `single_rider_wait`, `return_time_state/start/end`, and
`source_observation_id` (lineage → `raw_observations`). Range-partitioned by month on `timestamp`,
partitions created lazily in Go (`internal/store/postgres.go`, `ensurePartition`).

Two properties to know before querying it:
- **Timestamps are per-park poll wall-clock**, captured once per park per poll — *not* snapped to
  a 5-minute grid; the interval between a ride's rows jitters around 5 min.
- **Closed-ride rows are sparse** (a consequence of dedup): reconstruct downtime from
  `l2_status_transitions`, not from row presence.

### The weather record — `weather_observations` / `weather_forecasts`
The second source's projection. Weather is fetched **once per destination** (a resort's parks are
within a few km and share weather) at the destination centroid, then projected to each of that
destination's parks — so both tables stay park-keyed like `wait_times` for a trivial ML join.
Both carry a `source_observation_id` back to the one `raw_observations` row per destination per
poll. Range-partitioned by month, partitions created lazily in Go.
- **`weather_observations`** — current conditions (temp, feels-like, precip mm, humidity, wind,
  cloud, condition code/name), stamped at the observation's valid `timestamp`. **PIT-correct**
  (§11): value at `T` uses only data ≤ `T`. Serves `temp_f`, `precip_mm_now`, "currently raining".
- **`weather_forecasts`** — **bi-temporal**. Each row carries `issued_at` (when the forecast
  became knowable = our fetch time, the as-of key) and `valid_at` (the future time it describes),
  plus `horizon_minutes` and `precip_prob`. The projection stores the next ~6 h of hourly forecast
  each poll; the raw body keeps the full response. **PIT-correct only when read as-of `issued_at`**
  (§11); partitioned on `issued_at`.

### Reference & schedule
`destinations` (now also carries a weather `latitude`/`longitude` centroid, computed from its
attraction locations on sync), `parks` (carries `timezone`), `attractions` (`external_id`) —
upserted on sync, pruned to the US allowlist. `park_schedules` holds open/close windows and drives
the park-closed filter. That filter **fails open**: a park with no cached schedule is treated as
always open. Likewise a destination with no located attractions keeps NULL coords and is simply
skipped by the weather poll.

### Derived analytics
Computed after each poll from `wait_times` (§7). Every one is read by the frontend today (all
load-bearing for inspection); the column that matters for the ML future is whether it is
**point-in-time correct** (I2 / §11):

| Output | What it is | PIT-correct? |
|---|---|---|
| `l2_wait_velocity` | per-ride wait delta, EMA, rising/falling/stable trend | ✅ per-tick, causal |
| `l2_status_transitions` | status-change events with held-duration | ✅ predecessor is strictly earlier |
| `l3_park_crowd_pressure` | park-level wait-weighted velocity EMA | ✅ per `(park, timestamp)` |
| `l3_correlation_pairs` | pairwise Pearson r between rides' velocity series | ✅ trailing 2h window, stamped at `computed_at` |
| `l3_transition_impact` | how a closure/opening redistributed crowds (absorption/drain) | ⚠️ inspection-only — embeds `event+30min`; safe as a feature only with ≥30 min lag |
| `l2_daily_attraction_stats` / `l2_daily_park_stats` | materialized views: per-day min/max/avg/median/p90 wait | ⚠️ inspection-only — whole-day / all-history aggregate |

The two ⚠️ rows are legitimate *inspection* signals; they are simply not safe to feed a model at
their own timestamp. The ML computes its own as-of baselines from the ✅ substrate instead of
reusing them.

---

## 7. The derivation engine

Every derived output is a `Metric` (`internal/processor/processor.go`):

```go
type Metric interface {
    Name() string                                    // cursor key
    Version() int                                    // bump to force re-backfill
    OutputTables() []string
    Temporal() TemporalContract                      // I2: PIT-correct, or inspection-only + lag
    Process(ctx, since, now time.Time) error
}
```

The engine (`processMetric`) is small and is the genuinely reusable core:

- **Incremental.** Each metric has a cursor row (`l2_processing_cursors`) tracking the last
  timestamp it processed. Normal operation advances it one poll at a time.
- **Backfilling.** With no cursor, it starts from the earliest `wait_time` and walks forward in
  **1-hour chunks**, saving the cursor after each chunk so progress survives a crash.
- **Versioned & replayable.** Each metric carries a `logic_version`. Bump it and the engine
  truncates that metric's tables and re-backfills from scratch — because every derived value is a
  pure function of `wait_times`, the record can always be recomputed.
- **Self-declaring temporal contract.** `Temporal()` makes each output's PIT status explicit in
  code (I2); it is logged at startup so the quarantine in §11 is visible, not folklore.

Materialized views (`l2_daily_*`) are refreshed at most hourly via `REFRESH ... CONCURRENTLY`;
they are not `Metric`s, so their inspection-only status is recorded in §11 and the migration.

---

## 8. Design tenets

The commitments that keep this system small. Hold to them:

- **One binary, one Postgres.** No orchestrator, no message queue, no second language.
- **The raw record is sacred and self-sufficient.** Raw is persisted before projection (I1); all
  derived data is recomputable from it; nothing derived is a source of truth.
- **Derivations are versioned and declare their temporal contract** (I2).
- **Scope is deliberately narrow.** One source, US parks, one cadence.

---

## 9. Extensibility

The seams the current design already has — stated as seams, not roadmaps:

- **A new derived output** = implement `Metric` (including its `Temporal()` contract) and add a
  migration for its table. The engine handles scheduling, backfill, and versioning for free.
- **A new source** = a new `source_type` in `raw_observations` writing through the same raw path;
  a source-registry table is the point at which that would be formalized. *Now exercised:* the
  weather source (`openweather_onecall`, §6) is a sibling poll on its own ticker, reusing the raw
  path unchanged and projecting into its own park-keyed tables.
- **More parks** = add slugs to the allowlist.
- **Growth over time** = monthly partitioning on the two record tables is the scaling seam.

---

## 10. Known rough edges

Honest debts in the current build. None are hidden; none are pretending to be features:

- **Captured but unused:** `single_rider_wait` and the return-time fields are stored but no metric
  reads them. Free vs paid return-time is collapsed in the *projection* (the raw body keeps both).
- **Park-closed filter fails open** — parks without cached schedule data are never filtered.
- **Correlation scheduling is fragile:** it runs on in-memory hour-boundary bookkeeping that can
  double-run or skip an hour across restarts/backfill.
- **L2→L3 dependency is implicit.** Metrics run in a fixed slice order with independent cursors;
  the fact that crowd_pressure/correlation/crowd_flow depend on `l2_wait_velocity` is not enforced
  by a watermark.
- **No retention.** Partitions and the unpartitioned event tables grow without bound; the daily
  views full-recompute hourly, so refresh cost rises with total history.
- **No tests.**

---

## 11. Point-in-time classification (I2)

The per-output temporal contract, mirrored by each metric's `Temporal()` and enforced by
convention:

- **PIT-correct (safe as an at-`t` feature, shared by both readers):** `l2_wait_velocity`,
  `l2_status_transitions`, `l3_park_crowd_pressure`, `l3_correlation_pairs`, `weather_observations`.
  - `weather_observations` is a raw-source projection (like `wait_times`, not a `Metric`): it is
    stamped at the observation's valid `timestamp`, so its value uses only data ≤ that time.
- **Conditionally PIT-correct — depends on the read key:**
  - `weather_forecasts` — **safe as an at-`t` feature only when read as-of `issued_at`** (the time
    the forecast became knowable), *never* `valid_at`. A forecast for `t+60` is legal at `t`
    *because* it was issued at `issued_at ≤ t`. Keying an as-of join on `valid_at` instead is the
    same trap as `l3_transition_impact` below (stamped at one time, value from another). A forecast
    row is **never overwritten with the later actual** — the two must coexist; the actual at a
    forecast's `valid_at` is future-dependent (a label) and belongs to the ML component (§2).
- **Inspection-only (not safe as an at-`t` feature):**
  - `l3_transition_impact` — its row is stamped at `transition_time` but is computed from a
    snapshot at `event + 30min` (`crowd_flow.go`); safe only when read with a **≥30 min lag**.
  - `l2_daily_attraction_stats` / `l2_daily_park_stats` — a day-`D` row aggregates the whole of
    day `D` (and the views recompute over all history), so a naive historical-baseline join leaks
    future data. The ML computes an as-of/expanding-window baseline from the PIT substrate itself.

This classification is the resolution of what used to be an open question ("is this signal
load-bearing?"). It is: **everything is load-bearing for inspection; the axis that matters going
forward is PIT-safety, and it is now declared, not guessed.**
