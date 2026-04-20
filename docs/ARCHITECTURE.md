# TPWTP-DG — Data Pipeline Architecture

**Status:** Living design. Refined iteratively. Sections not yet implemented are tracked in §11 Migration Trajectory.

**Scope:** The backend data pipeline — how external signals are observed, persisted, transformed, and exposed via Postgres. Does not cover HTTP API surface, frontend consumers, or ML/prediction work beyond treating them as downstream readers.

---

## 1. Design Tenets

Four properties govern every decision in this document. When a proposed change violates one of them, the proposal is wrong, not the tenet.

1. **Straightforward** — the whole pipeline is understandable from this document plus a quick code tour. No clever metaprogramming, no hidden control flow.
2. **Lightweight** — in-Go framework only. No external orchestrators, no second language for transforms, no distributed-compute engines. One binary, one Postgres.
3. **Self-documenting** — processors declare their dependencies, inputs, outputs, schemas, and versions in code. The pipeline's shape is readable by walking the registry; no out-of-band diagrams required.
4. **Reusable and decoupled** — the framework has zero domain knowledge. Domain code has zero framework internals. Adding a new processor touches one module plus a registration call; nothing else.

## 2. The Model

The pipeline is a **directed acyclic graph of processors** reading from an immutable foundation table, `raw_telemetry`.

```
                  ┌──────────────────────┐
external  ───►    │    raw_telemetry     │    ◄── every observer writes here
sources           │ (one row = one obs.) │
                  └──────────┬───────────┘
                             │
                   ┌─────────┴─────────┐
                   ▼                   ▼
            ┌──────────────┐    ┌──────────────┐
            │  processor A │    │  processor B │
            └──────┬───────┘    └──────┬───────┘
                   │                   │
                   ▼                   ▼
            ┌──────────────┐    ┌──────────────┐
            │  processor C │◄───┤  processor D │
            └──────────────┘    └──────────────┘
```

Every node except `raw_telemetry` is a processor. Every edge is a declared dependency. The graph is built at startup by walking processor registrations; cycles are rejected.

**DAG depth is an emergent property, not a structural primitive.** If you want to call something "deep in the graph," you can compute its topological depth from the dependency list. That number has no prescriptive meaning. A processor that writes aggregates may sit next to a processor that writes signals may sit next to a processor that writes features — all at whatever depth their upstreams place them. Their semantic purpose is documented in their own contract, not encoded in a layer number.

Legacy table prefixes (`l2_`, `l3_`) in the current code are **historical names**; they do not carry structural meaning under this architecture and will not be renamed.

## 3. The Foundation: `raw_telemetry`

A single table, written to by any observer from any source. Source-specific structure lives inside JSONB fields discriminated by `source_type`.

### 3.1 Schema

```sql
CREATE TABLE raw_telemetry (
    telemetry_id     uuid        NOT NULL,
    source_type      text        NOT NULL REFERENCES telemetry_source_types(source_type),
    source_version   text        NOT NULL,
    observed_at      timestamptz NOT NULL,   -- when the fact was true upstream
    received_at      timestamptz NOT NULL,   -- when we wrote this row (partition key)
    subject          jsonb       NOT NULL,   -- source-specific identity of what's observed
    payload          jsonb,                  -- raw body; null on failed fetches
    metadata         jsonb,                  -- delivery/ops info
    PRIMARY KEY (telemetry_id, received_at)
) PARTITION BY RANGE (received_at);
```

### 3.2 Field rationale

- **`telemetry_id`** — stable identity. Referenced by lineage columns on downstream rows.
- **`source_type`** — discriminator; determines how `subject`, `payload`, and `metadata` are interpreted. Examples: `themeparks_wiki_live`, `user_wait_submission`, `noaa_weather_observation`.
- **`source_version`** — the source's schema version. Bumped when upstream structure changes. Old rows remain valid under old versions.
- **`observed_at` vs `received_at`** — bitemporal. Equal for pure polling. Diverge for retroactive submissions, backfilled feeds, etc. The divergence is signal.
- **`subject`** — JSONB identity. `{"park_id": "..."}`, `{"attraction_id": "...", "user_hash": "..."}`, `{"station_id": "KMCO"}`. Shape is per-source-type.
- **`payload`** — raw body as received. Nullable (failed fetch, no-body source).
- **`metadata`** — delivery-channel info: `{"http_status": 200, "latency_ms": 312}`, `{"error": "timeout"}`, `{"mq_offset": 12345}`. Kept separate from payload so source semantics and delivery semantics don't tangle.

### 3.3 Source registry

A companion table documents and gates what's allowed in `source_type`:

```sql
CREATE TABLE telemetry_source_types (
    source_type      text PRIMARY KEY,
    display_name     text        NOT NULL,
    description      text        NOT NULL,
    current_version  text        NOT NULL,
    subject_schema   jsonb,              -- optional; shape documentation
    payload_schema   jsonb,              -- optional; shape documentation
    active           boolean     NOT NULL DEFAULT true,
    created_at       timestamptz NOT NULL DEFAULT now()
);
```

Adding a new source requires adding a row here first. The FK on `raw_telemetry.source_type` enforces it.

### 3.4 Observer pattern

An **observer** is any component that writes to `raw_telemetry`. Observers differ in how they acquire data:

| Delivery | Example | Shape |
|---|---|---|
| Polling | themeparks.wiki live | Ticker + HTTP client |
| Pull on demand | Weather fetch on park open | Event-driven HTTP client |
| Push HTTP | User submission form | HTTP handler |
| Push queue | Partner feed | MQ consumer |

Observers share the write API (`InsertRawTelemetry`) and nothing else. No observer interface is formalized until two concrete observers exist — at that point, the shared shape will be extracted. Today there is one, and it calls the write function directly.

## 4. Processors

A processor is a named, versioned, contracted transformation from declared upstream data to a managed output table. Processors are the **only** primitive above the foundation table.

### 4.1 Interface (conceptual)

```go
// Processor is the contract implemented by every domain module.
// The framework orchestrates processors; it never interprets their data.
type Processor interface {
    // Identity, stable across restarts. Used as the registry key and
    // as the dependency reference by other processors.
    ID() string

    // IDs of upstreams this processor reads from. May be other processor
    // IDs or the sentinel "raw_telemetry". Returned list is immutable.
    DependsOn() []string

    // Cursor shape per upstream. Framework stores and advances these.
    CursorTypes() map[string]CursorType

    // Algorithm version. Bumping invalidates prior output; framework
    // handles replay per policy.
    AlgorithmVersion() string

    // Materialize reads upstream inputs at the given cursor positions,
    // computes output, and writes it. Returns advanced cursors.
    // Must be idempotent: same inputs + same cursors → same writes.
    Materialize(ctx context.Context, cursors Cursors) (Cursors, error)
}
```

Optional capabilities are signaled by additional interfaces the framework type-asserts against:

```go
// Partitioned processors manage their own output partition scheme.
type Partitioned interface {
    PartitionKeyFor(t time.Time) string
    EnsurePartition(ctx context.Context, t time.Time) error
}

// Scheduled processors override the default eager-on-readiness policy.
type Scheduled interface {
    Schedule() ScheduleSpec // cron, fixed-interval, upstream-triggered, on-demand
}

// ReadyGated processors override the default "all upstreams at watermark" check.
type ReadyGated interface {
    Ready(watermarks map[string]time.Time) bool
}
```

This keeps the required interface small (four methods) and makes everything else opt-in. A processor with no special needs implements only `Processor`.

### 4.2 What a processor owns

- Its ID.
- Its output table's schema and any DDL it requires (declared in SQL migrations; processor code asserts the table exists).
- The math: filters, transforms, aggregations, windowed computations, whatever it does.
- Idempotency discipline (stable PKs, upserts, or per-partition replace semantics).

### 4.3 What a processor does NOT own

- DAG resolution, cycle detection.
- Cursor storage or reads.
- Watermark computation.
- Scheduling decisions.
- Retry, backoff, or concurrency limits.
- Lineage bookkeeping (beyond populating a lineage column on output rows).

All of that is the framework.

## 5. The Framework

A small in-Go layer — single package, no dependencies beyond pgx and the standard library — that provides:

### 5.1 Registry and DAG

Processors register themselves at startup:

```go
registry.Register(&wait_times.Projector{...})
registry.Register(&wait_velocity.Processor{...})
registry.Register(&crowd_pressure.Processor{...})
```

The framework walks the registry, reads each processor's `DependsOn()`, builds the DAG, and rejects cycles. The resulting graph is the source of truth for:
- topological ordering on startup / replay
- visualization (`debug_processors` view or CLI command)
- operational introspection (what's stale, what's ready, what's blocked)

### 5.2 Cursor store

A single table stores cursors for all processors:

```sql
CREATE TABLE processor_cursors (
    processor_id       text         NOT NULL,
    dependency_id      text         NOT NULL,
    cursor_type        text         NOT NULL,  -- 'timestamp' | 'sequence' | 'partition_key' | 'version'
    cursor_position    jsonb        NOT NULL,
    algorithm_version  text         NOT NULL,
    last_advanced_at   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (processor_id, dependency_id)
);
```

One row per (processor, upstream). Polymorphic `cursor_position` (JSONB) supports different cursor shapes without schema changes. This generalizes the current `l2_processing_cursors`.

### 5.3 Watermark and readiness

Each processor publishes a **watermark**: "I have output through time T and believe no more upstream data will arrive with observed_at < T." The framework computes watermarks by combining:
- the processor's current cursor position
- a configurable safety buffer

Default readiness predicate: "I can advance up to the min of my upstreams' watermarks." Processors that want custom semantics implement `ReadyGated`.

### 5.4 Scheduling

Default policy: **eager on readiness.** When any upstream's watermark advances and this processor becomes ready to move further, the framework triggers materialization. Processors that want cron-style, upstream-triggered, or on-demand policies implement `Scheduled`.

The scheduler is a single goroutine walking the DAG on a tick (say, every 30s). Simple, observable, cheap.

### 5.5 Partition management

A helper, not a mandate:

```go
type PartitionManager struct { /* ... */ }
func (pm *PartitionManager) Ensure(ctx context.Context, table string, t time.Time, scheme PartitionScheme) error
```

Processors that implement `Partitioned` use this helper (or their own equivalent). Processors that write to non-partitioned tables ignore it. The framework does not assume partitioning.

### 5.6 Lineage

When a processor writes derived rows, it records the telemetry id(s) that drove each row. Simple one-to-many is a nullable `source_telemetry_id uuid` column on the output table. Many-to-many (one derived row summarizes many telemetry rows) uses a sidecar `<table>_sources` table.

The framework provides no magic here — it offers a helper to set the column and a convention for the sidecar. The column's presence is the contract; population is the processor's responsibility.

### 5.7 What the framework does NOT do

- Parse any specific source payload.
- Compute any specific metric.
- Know what a wait time, park, attraction, or crowd is.
- Impose a schema on any processor's output.
- Require a minimum number of columns or a naming convention on output tables.
- Provide a UI.
- Orchestrate cross-process work (it's an in-binary library, not a service).

## 6. Modules

A **module** is a Go package that contains one or more processors for a specific domain concept. Modules are the unit of addition, change, and ownership.

A module typically contains:
- A `processor.go` with the `Processor` implementation
- A `schema.sql` (or migration file reference) for the output table
- A `compute.go` with the domain math, separate from framework integration
- Optional: partition/schedule specializations, helpers

Example module layout:

```
internal/
  pipeline/                    # framework (reusable, domain-free)
    registry.go
    cursors.go
    watermark.go
    scheduler.go
    partition.go
  observers/
    themeparks_live/           # observer module
      observer.go
  modules/
    wait_times/                # projects raw_telemetry → wait_times
      processor.go
      projector.go             # the extraction logic
      filters.go               # park-closed, dedup, entity-type filter
    wait_velocity/             # wait_times → wait_velocity
      processor.go
      compute.go               # EMA, trend, velocity math
    crowd_pressure/
      processor.go
      compute.go
    daily_attraction_stats/
      processor.go
      compute.go
```

Key property: **adding `modules/weather_observations/` requires no changes to `pipeline/` or any other module.** It registers itself, declares its dependencies (`raw_telemetry`), and operates.

## 7. Cursors and Watermarks in Practice

### 7.1 Cursor types

The framework supports any cursor type whose position fits in JSONB. Common:

- **timestamp**: `{"max_observed_at": "2026-04-20T14:30:00Z"}` — default for event-time sources
- **sequence**: `{"max_id": 12345}` — monotonic insertion id
- **partition_key**: `{"last_materialized": "2026-W16"}` — for consuming partitioned upstreams
- **version**: `{"seen_version": "v3"}` — for slowly-changing reference data

Processors with multiple upstreams may use different cursor types for different dependencies. The framework does not require uniformity.

### 7.2 Watermark semantics

For a polled upstream: watermark = `last_successful_poll_start - safety_buffer`. For an event-driven upstream: watermark is harder; start conservative, relax as operational confidence grows.

A downstream processor's effective cursor target is `min(watermark(dep) for dep in upstreams)`. This is the classic watermark-join pattern from stream processing — kept intentionally simple here.

### 7.3 Late data

Default: pessimistic. Safety buffers keep late data out of the ready window. If late data becomes common, individual processors can implement `ReadyGated` with custom logic (e.g., "re-materialize the affected partition when retrograde data arrives"). The framework doesn't solve this universally.

## 8. Lineage

Every derived row carries a pointer back to the `raw_telemetry` row(s) that produced it.

Simple case (one-to-one or many-to-one):

```sql
ALTER TABLE wait_times
    ADD COLUMN source_telemetry_id uuid;
```

Nullable, no FK (partitioned tables + FKs are awkward), no index by default (add if queried).

Multi-source case (e.g., an aggregate summarizing many telemetry rows):

```sql
CREATE TABLE daily_attraction_stats_sources (
    stats_row_key   /* pk of the stats row */,
    telemetry_id    uuid,
    PRIMARY KEY (stats_row_key, telemetry_id)
);
```

When a processor's output can't be traced to a single telemetry row, it populates the sidecar instead of (or in addition to) the direct column.

Algorithm version is recorded on every derived row so consumers can see which methodology produced which row.

## 9. Operational Concerns

### 9.1 Idempotency

All writes are idempotent:
- Observer writes use client-generated `telemetry_id`. Duplicate delivery collides on PK.
- Processor writes are keyed by stable identifiers (`(attraction_id, timestamp)`, partition keys, etc.). Re-materialization over the same cursor range upserts or partition-replaces.

### 9.2 Failures

- **Observer failure**: logged, skipped (observer's own retry policy). `raw_telemetry` row lost; no downstream effect beyond a gap.
- **Source unreachable**: observer writes a `raw_telemetry` row with `payload = null` and `metadata.error` populated. Downstream processors branch on payload-null.
- **Processor failure**: cursor does not advance. Framework retries on next scheduler tick. No partial output is committed.
- **Algorithm bug discovered**: bump `AlgorithmVersion()`, framework resets cursor and replays. Output is either truncated and rewritten, or written alongside the old version (processor's choice).

### 9.3 Replay

Replaying a processor means: reset its cursors to the beginning of its upstream's available data, clear its output table (or its current-version rows), and let the framework re-run it. The framework offers a `Replay(processorID)` helper. Because processors are pure functions of inputs at cursor positions, replay is deterministic.

### 9.4 Retention

Per-processor. `raw_telemetry` retention is the most consequential: once a telemetry row is dropped, any processor that depends on it cannot replay past that point. Recommended: keep `raw_telemetry` hot 90 days; archive or drop beyond that once derived tables are stable.

Retention policies are a module concern, not a framework one. The framework provides pruning helpers; modules decide when to use them.

## 10. Self-Documentation

A core tenet is that the pipeline's shape is visible from code. Concrete mechanisms:

- Processors declare all metadata (ID, deps, cursors, version) on the interface. A CLI command `tpwtp-dg describe processors` prints the full registry.
- A generated `processor_catalog` table, populated at startup from registry metadata, lets operators `SELECT` their way through the DAG.
- Each module has a `README.md` with its contract: what it reads, what it writes, the gist of its math, its algorithm version history. Short. Hand-written. Living.
- The DAG can be rendered to Graphviz / Mermaid by a built-in helper; no external tooling required.

The goal: no hidden dependencies, no "you have to read this file to understand that one," no reliance on institutional memory.

## 11. Operations Portal

A thin HTTP surface inside the main binary for humans (browser) and tools (curl, scripts) to **inspect pipeline state and take administrative actions against it.** The portal serves operators — the people who run the system day-to-day.

### 11.1 Intent

Two jobs, co-located because they naturally co-occur in operator workflows:

1. **Inspection** — "what is the pipeline doing right now?" Answered without SQL, log greps, or recompiles.
2. **Administration** — "change what the pipeline is doing." Trigger a replay, reset a cursor, pause a processor, activate a source.

These are bundled in one portal because an operator about to take an admin action always wants to see current state first. Separating read and write into two UIs forces context switching and duplicates navigation — a pattern widely rejected by analogous systems (Airflow, Dagster, Temporal, RabbitMQ, Kubernetes dashboards).

Everything the portal displays is pulled live from the registry, cursor store, scheduler, and observer registry. Nothing is duplicated. Every write action routes through a framework operations API so ad-hoc state-mutation stays out of the handlers.

Time-series metrics, dashboards, and alerting are **not** the portal's job — see §12 for the Prometheus surface that handles those.

### 11.2 Design tenets applied

- **Thin** — standard library only (`net/http`, `html/template`, `embed`). No web framework, no JS bundler, no templating DSL beyond Go's.
- **Self-documenting** — the portal reads the same registry that code reads. Adding a processor makes it appear in the portal automatically. There is no portal-side configuration of "what to show."
- **Decoupled** — modules have zero knowledge of the portal. The framework exposes read accessors the portal composes.
- **Reusable** — the framework accessors are general. The portal is just one consumer; a CLI `describe` command or external dashboard could consume the same data.

### 11.3 What it exposes (inspection)

#### About observers
- List of running observers, their source_type, delivery mode (poll / push / etc.), start time.
- Per-observer activity: timestamp of last successful write, recent error count, rolling write rate (rows/min, bytes/min).
- Gaps: time since last write per source, flagged if exceeds expected cadence.

#### About the DAG / pipeline
- The full DAG rendered as:
  - Mermaid text (copy-pasteable, renders in GitHub/Notion/Obsidian)
  - an inline-rendered HTML diagram via a single embedded client-side Mermaid script
  - a flat adjacency list (for programmatic consumers)
- Topological depth of each node (emergent property, for visualization only — not a structural claim).
- Graph-wide health: counts of ready / blocked / stalled / erroring processors.
- Recent scheduler ticks: timestamp, processors triggered, total duration.

#### About processors
From the Processor interface and runtime state:
- ID, dependencies, cursor types, algorithm version (from interface methods — always current).
- Current cursor position per dependency (pretty-printed by cursor type).
- Current watermark (derived from cursors + safety buffer).
- Readiness state: `ready` / `blocked_by: [dep_id@cursor vs upstream watermark]` / `paused`.
- Last successful materialization: timestamp, duration, rows written.
- Recent errors (last N — configurable, default 10): timestamp, error message, cursor state at time of error.
- Algorithm version history (if the processor has bumped versions during this binary's lifetime).

#### About sources
- Registered `telemetry_source_types`: type, description, current version, active flag.
- Per-source volume: rows/hour, bytes/hour, error rate (from `metadata->>'error' IS NOT NULL`).
- Age of most recent row per source (gap detection).

#### About cursors
- Full dump of the `processor_cursors` table (useful for replay debugging).
- Grouped by processor for easy scanning.

### 11.4 What it supports (administrative actions)

Administrative actions are modifications to pipeline state. Each action is:
- **Explicit** — invoked through a dedicated endpoint, not a side effect.
- **Idempotent where possible** (e.g., "pause" is idempotent; "trigger-run-now" is not).
- **Audit-logged** — every action records who (principal), when, what, target, parameters, and pre/post state.
- **Framework-mediated** — handlers call into a small operations API on the framework, not into processor internals.

Core actions (MVP set):

| Action | Target | Effect |
|---|---|---|
| Trigger materialization | Processor | Run the processor once, now, regardless of schedule |
| Pause | Processor | Skip the processor in scheduler walks until resumed |
| Resume | Processor | Undo pause |
| Reset cursor | Processor + dependency | Rewind cursor to a specified position (or zero) |
| Replay | Processor | Reset cursors + truncate output (or mark old-version) + re-materialize from start |
| Pause observer | Observer | Stop producing new telemetry rows |
| Resume observer | Observer | Undo pause |
| Toggle source active | source_type | Flip `l1_source_types.active` (prevents new writes for that source) |
| Bump algorithm version | Processor | Declare the current in-memory algorithm version has changed mid-run (rarely needed; usually handled at code level) |

Deferred (not MVP — land once the MVP pattern is stable):
- Backfill a specific cursor range for a processor (as opposed to full replay).
- Schedule-override ("pause scheduler entirely for N minutes").
- Telemetry redrive (re-emit an L1 row to force downstream re-materialization of that specific row).
- Bulk actions (replay N processors in dependency order).

Each action has clear failure modes documented with the action — e.g., "replay fails if any downstream processor is not paused," surfaced in the portal as a confirmation-dialog gate before the action executes.

### 11.5 Endpoints

HTML by default (content negotiation via `Accept` promotes to JSON; explicit `/api/...` prefix returns JSON unconditionally). Read endpoints are `GET`; admin endpoints are `POST` with a short JSON body and require auth in non-localhost deployments (§11.8).

**Read (inspection):**

| Path | Purpose |
|---|---|
| `GET /` | Dashboard: health summary, DAG overview, recent activity |
| `GET /observers` | Observer list |
| `GET /observers/{id}` | Observer detail |
| `GET /processors` | Processor list + DAG-depth column |
| `GET /processors/{id}` | Processor detail: deps, cursors, watermarks, recent runs, recent errors |
| `GET /sources` | Source-type registry + live volume per source |
| `GET /sources/{type}` | Per-source drill-down: subject shape, recent telemetry sample, volume chart |
| `GET /dag` | DAG visualization (Mermaid + inline-rendered) |
| `GET /cursors` | Full cursor table, grouped by processor |
| `GET /runs` | Recent materialization history across the DAG |
| `GET /audit` | Administrative action history |
| `GET /healthz` | Liveness probe — binary is up, scheduler goroutine running |
| `GET /readyz` | Readiness probe — DB reachable, no processors stuck beyond a threshold |
| `GET /api/*` | Same read endpoints, JSON-only |

**Write (administration):**

| Path | Purpose |
|---|---|
| `POST /processors/{id}/trigger` | Run this processor once, now |
| `POST /processors/{id}/pause` | Skip this processor in scheduler walks |
| `POST /processors/{id}/resume` | Undo pause |
| `POST /processors/{id}/cursors/{dep}/reset` | Rewind cursor to a specified position (body: `{"position": ...}` or `{"to_zero": true}`) |
| `POST /processors/{id}/replay` | Reset cursors + truncate (or version-mark) output + re-run |
| `POST /observers/{id}/pause` | Stop producing telemetry for this observer |
| `POST /observers/{id}/resume` | Undo pause |
| `POST /sources/{type}/toggle-active` | Flip the `active` flag in the source registry |

Each write endpoint:
- Returns the pre-action state, the accepted action, and the post-action state in the response body.
- Writes an `audit_actions` row before acting and updates it with the outcome after.
- Is idempotent where the action permits (pause is; trigger is not — callers with retry semantics must handle that).

### 11.6 How it's wired

A new package `internal/portal/` with:

- `Server` struct holding references to the registry, cursor store, scheduler run-history, and observer registry.
- Handlers registered on a `http.ServeMux`.
- HTML templates embedded via `embed.FS`.
- A small shared template layout with a nav and status ribbon.
- JSON responses share the same source structs as HTML templates — no schema drift.

The framework grows a few read accessors the portal consumes:

```go
// in internal/pipeline/
func (r *Registry) Processors() []ProcessorInfo
func (r *Registry) DAG() DAGSnapshot
func (s *Scheduler) RecentRuns(limit int) []RunRecord
func (cs *CursorStore) All(ctx context.Context) ([]CursorSnapshot, error)

// in internal/observers/
func (or *ObserverRegistry) Observers() []ObserverInfo
func (or *ObserverRegistry) Activity(id string) ObserverActivity
```

Types (`ProcessorInfo`, `RunRecord`, `CursorSnapshot`, `ObserverInfo`) are plain data — no behavior, no interface back to the framework. The portal composes them into views.

The scheduler maintains a small ring buffer of recent run records in memory (say, last 500). For deeper history, runs could later be persisted to a `processor_runs` table; MVP keeps it in-memory.

The portal depends on an **operations API** on the framework for write actions:

```go
// in internal/pipeline/ops.go
type Operations struct { /* ... */ }

func (o *Operations) TriggerProcessor(ctx context.Context, id string, actor string) (RunRecord, error)
func (o *Operations) PauseProcessor(ctx context.Context, id string, actor string) error
func (o *Operations) ResumeProcessor(ctx context.Context, id string, actor string) error
func (o *Operations) ResetCursor(ctx context.Context, id, dep string, pos CursorPosition, actor string) error
func (o *Operations) ReplayProcessor(ctx context.Context, id string, actor string) (RunRecord, error)
func (o *Operations) PauseObserver(ctx context.Context, id string, actor string) error
func (o *Operations) ResumeObserver(ctx context.Context, id string, actor string) error
func (o *Operations) ToggleSourceActive(ctx context.Context, sourceType string, actor string) error
```

Every method takes an `actor` (authenticated principal or `"local-admin"` when unauthenticated). Every method emits an audit record. The operations API is the only way mutation happens in the framework; the portal, a CLI, or a future API-key-driven automation client are all equal consumers of the same surface.

### 11.7 Audit log

Every administrative action produces a row:

```sql
CREATE TABLE audit_actions (
    action_id     uuid        PRIMARY KEY,
    actor         text        NOT NULL,    -- 'local-admin' | username | token-id
    requested_at  timestamptz NOT NULL DEFAULT now(),
    completed_at  timestamptz,
    action_type   text        NOT NULL,    -- 'trigger' | 'pause' | 'reset_cursor' | 'replay' | ...
    target_kind   text        NOT NULL,    -- 'processor' | 'observer' | 'source_type'
    target_id     text        NOT NULL,
    params        jsonb,                   -- action-specific inputs
    pre_state     jsonb,                   -- captured before the action
    post_state    jsonb,                   -- captured after the action
    status        text        NOT NULL,    -- 'succeeded' | 'failed'
    error         text
);
```

Visible in the portal at `/audit`. Retained indefinitely (low volume). This is the authoritative record of "what operators did to the system" — useful for post-incident review and for answering "why does cursor X look weird?"

### 11.8 Configuration

| Var | Default | Description |
|---|---|---|
| `PORTAL_ENABLED` | `true` | Toggle the portal entirely |
| `PORTAL_ADDR` | `:8080` | Bind address |
| `PORTAL_READ_ONLY` | `false` | If true, admin endpoints 403 — useful in prod until auth is set up |
| `PORTAL_AUTH_MODE` | `none` | `none` \| `local-only` \| `bearer-token` \| `reverse-proxy` |
| `PORTAL_BEARER_TOKEN` | (unset) | Required when `PORTAL_AUTH_MODE=bearer-token` |

### 11.9 Access and security

Access control is tiered by deployment context:

- **Development / localhost**: `PORTAL_AUTH_MODE=none`. Read and write open. Actor recorded as `local-admin`. Acceptable because the trust boundary is the loopback interface.
- **Internal network**: `PORTAL_AUTH_MODE=local-only` restricts admin endpoints to requests from private IP ranges (RFC 1918), reads stay open. Or `reverse-proxy` trusts an upstream-injected `X-Auth-User` header and delegates auth to the proxy.
- **Public exposure**: `PORTAL_AUTH_MODE=bearer-token` with a shared secret (and TLS termination upstream). Admin endpoints require the token; reads may or may not, per config. This is the simplest self-contained auth; if more is needed (multi-user, SSO), put a proper reverse proxy in front.

Regardless of mode:
- Admin endpoints 403 if `PORTAL_READ_ONLY=true`. This is the panic-button for production deploys until auth is verified.
- No secrets surfaced: no DB URLs, no API keys, no environment dumps.
- Raw telemetry payload sampling on `/sources/{type}` is an explicit click-through, size-capped, and passes through a configurable redaction hook.
- The audit log records every admin action with actor identity, so even when `local-admin` is the actor, the principal's IP / auth context is captured from request metadata.

### 11.10 What it is NOT

- **Not a metrics system.** Time-series, histograms, counters, fleet aggregates — those live in the Prometheus exposition described in §12. Portal and metrics both read from the same accessors; don't double-source truth.
- **Not alerting.** Alerting runs on the metrics surface (Alertmanager, Grafana alerts). Portal is interactive, not proactive.
- **Not a dashboard builder.** Views are Go templates edited in the repo; there is no point-and-click dashboard composer.
- **Not a tracing / APM surface.** If request/job tracing is wanted, add OTel alongside metrics — not inside the portal.
- **Not a permanent log viewer.** Recent run history and audit log are structured and durable; raw application logs stay in slog output.

## 12. Metrics Emission

An independent surface for time-series metrics, consumed by Prometheus scrapers and rendered in Grafana (or any compatible tool).

### 12.1 Intent

Portal answers "what is this specific processor / observer / source doing right now, and can I change it?" Metrics answer "how is the fleet behaving over time, and when should I be paged?" Different consumers, different shapes, different cadences.

### 12.2 What is emitted

Prometheus text-exposition format at `GET /metrics`. Scraped on the scraper's schedule.

**Processor metrics** (labeled by `processor_id`, `algorithm_version`):
- `pipeline_processor_ready` (gauge 0/1) — currently ready to advance
- `pipeline_processor_paused` (gauge 0/1)
- `pipeline_processor_cursor_lag_seconds` (gauge, labeled by `dependency_id`) — now − cursor position
- `pipeline_processor_materialize_duration_seconds` (histogram)
- `pipeline_processor_materialize_rows_total` (counter)
- `pipeline_processor_materialize_errors_total` (counter, labeled by `error_class`)
- `pipeline_processor_algorithm_version_info` (gauge=1 with label) — for grouping in dashboards

**Observer metrics** (labeled by `observer_id`, `source_type`):
- `telemetry_observer_writes_total` (counter)
- `telemetry_observer_write_errors_total` (counter, labeled by `error_class`)
- `telemetry_observer_last_write_timestamp_seconds` (gauge) — for staleness alerts
- `telemetry_observer_payload_bytes` (histogram)

**Source metrics** (labeled by `source_type`):
- `telemetry_source_rows_total` (counter) — rows landed in `raw_telemetry`
- `telemetry_source_payload_null_total` (counter) — failed fetches
- `telemetry_source_active` (gauge 0/1) — from the registry

**Framework metrics:**
- `pipeline_scheduler_tick_duration_seconds` (histogram)
- `pipeline_scheduler_processors_triggered_total` (counter)
- `pipeline_dag_size` (gauge)
- `pipeline_admin_actions_total` (counter, labeled by `action_type`, `status`)

**Build info** (one gauge with version labels) so Grafana can slice by deploy.

### 12.3 How it's wired

A new package `internal/metrics/` with:
- Prometheus `Registry` (standard `prometheus/client_golang`).
- A collector that pulls from the same framework accessors as the portal — `registry.Processors()`, `cursorStore.All()`, `observerRegistry.Observers()`, etc. No duplicated state.
- Handler mounted at `GET /metrics`. Typical convention is to serve it on a separate internal port from the main application (or in this case the portal), but for a thin single-binary service, co-locating on the portal port is acceptable. Configurable.

Metrics population happens on scrape, not continuously. No background goroutine; the collector reads live state when Prometheus asks.

### 12.4 Configuration

| Var | Default | Description |
|---|---|---|
| `METRICS_ENABLED` | `true` | Toggle the `/metrics` endpoint |
| `METRICS_PATH` | `/metrics` | Path on the portal listener |
| `METRICS_ADDR` | (unset) | If set, serve metrics on a separate listener instead |

### 12.5 What is NOT emitted

- Per-entity current state that's better rendered in the portal (don't emit one metric per cursor position per processor — use labels for dimensions that fit Prometheus's model).
- Secrets or identifiers that aren't bounded-cardinality (no per-attraction-id label; those explode cardinality).
- Raw payloads, subjects, or anything user-content-bearing.

### 12.6 Relationship to tracing

OTel tracing is orthogonal to both portal and metrics and is not in scope for this document. If added, instrumentation lives at the framework level (one span per materialization) and emits to an OTel collector; portal and metrics do not change.

## 13. Current State → Target State

### 13.1 Current state (as of 2026-04-20)

- Polling loop in `internal/collector/collector.go` fetches, filters (entityType, park-closed, dedup), projects, and writes `wait_times` in one pass.
- No `raw_telemetry` table; API response bytes discarded after `json.Decode`.
- Processors (`l2_wait_velocity`, `l2_status_transitions`, `l3_park_crowd_pressure`, etc.) exist but run synchronously via `Processor.RunAfterPoll` with ad-hoc cursor handling in `l2_processing_cursors`.
- Output table names carry legacy `l2_`/`l3_` prefixes — **these stay as-is**, interpreted as historical names.

### 13.2 Target state

- `raw_telemetry` table exists; every observer writes to it before domain projection.
- `pipeline` package provides the `Processor` interface, registry, cursor store, watermark/readiness evaluation, scheduler, and partition/lineage helpers.
- Existing processors refactored **in place** to implement `Processor`. Their output tables keep their current names and schemas (with the addition of `source_telemetry_id` and `algorithm_version` columns).
- The collector becomes one `Observer` implementation, not a monolithic fetch-filter-project-write loop.
- Filters move out of the observer and into whatever processor owns the `wait_times` projection.

### 13.3 Migration trajectory

High-level sequencing. Each phase is non-breaking; phase plans live in separate documents.

- **Phase 1 — `raw_telemetry` foundation.** Add the table, source registry, and dual-write from the current poller. Add `source_telemetry_id` nullable column on `wait_times`. No processor changes.
- **Phase 2 — Separate projection from ingestion.** Extract the L1 → `wait_times` projection from the collector into its own processor. Filters move with it.
- **Phase 3 — Minimal framework skeleton.** Introduce the `pipeline` package: `Processor` interface, `registry`, polymorphic `processor_cursors` table. One existing processor (simplest: `wait_velocity`) migrates as the first implementer. Keeps running alongside non-migrated processors.
- **Phase 4 — Operations portal (read-only).** Introduce `internal/portal/` with inspection endpoints (`/processors`, `/dag`, `/cursors`, `/sources`, `/observers`, `/healthz`). Admin endpoints stubbed behind `PORTAL_READ_ONLY=true`. Portal makes migrating the remaining processors in Phase 5 much easier to verify.
- **Phase 5 — Migrate remaining processors.** Each existing processor becomes a `Processor` implementation. `l2_processing_cursors` retired in favor of `processor_cursors`. No table renames. Portal views validate each migration.
- **Phase 6 — Operations portal (admin actions).** Land the write surface: `Operations` API on the framework, admin endpoints, `audit_actions` table. Start with `local-only` auth mode. Write actions validated by replaying stubborn processors during development.
- **Phase 7 — Metrics emission.** Introduce `internal/metrics/` with Prometheus exposition. Standing up a Grafana dashboard on top is a deployment concern, not a code concern; the emission itself is a small addition.
- **Phase 8 — Second observer.** Add a new source (weather or user submissions). First real test of observer generality.
- **Phase 9+** — Additional processors, additional observers, DAG-aware CLI tooling, retention automation, portal auth hardening (bearer token, reverse-proxy trust), OTel tracing if requested.

Phases may reorder, split, or overlap as we learn. Updates to this trajectory happen here, in this document.

## 14. Pseudocode

Go-flavored, not compilable. One connected example showing framework + observer + two processors.

### 12.1 The framework (shape, not implementation)

```go
package pipeline

type Processor interface {
    ID() string
    DependsOn() []string
    CursorTypes() map[string]CursorType
    AlgorithmVersion() string
    Materialize(ctx context.Context, cursors Cursors) (Cursors, error)
}

type Registry struct { /* ... */ }
func (r *Registry) Register(p Processor)
func (r *Registry) DAG() *DAG

type Scheduler struct {
    registry *Registry
    store    *CursorStore
    tick     time.Duration
}
func (s *Scheduler) Run(ctx context.Context) error // eager-on-readiness loop

// helpers modules may use
func (pm *PartitionManager) Ensure(ctx context.Context, table string, t time.Time) error
```

### 12.2 An observer (writes `raw_telemetry`)

```go
package themeparks_live

func (o *Observer) PollPark(ctx context.Context, parkID string) {
    id := uuid.New()
    resp, raw, err := o.client.GetEntityLiveWithRaw(ctx, parkID)

    subject, _ := json.Marshal(map[string]string{"park_id": parkID})
    metadata := buildMetadata(raw, err)

    tel := store.RawTelemetry{
        TelemetryID:   id,
        SourceType:    "themeparks_wiki_live",
        SourceVersion: "v1",
        ObservedAt:    raw.FetchedAt,
        ReceivedAt:    raw.FetchedAt,
        Subject:       subject,
        Payload:       raw.Body, // nil on transport failure
        Metadata:      metadata,
    }
    if writeErr := o.store.InsertRawTelemetry(ctx, tel); writeErr != nil {
        slog.Error("telemetry write failed", "err", writeErr)
    }
}
```

### 12.3 A projecting processor (`raw_telemetry` → `wait_times`)

```go
package wait_times

func (p *Projector) ID() string              { return "wait_times" }
func (p *Projector) DependsOn() []string     { return []string{"raw_telemetry"} }
func (p *Projector) CursorTypes() map[string]pipeline.CursorType {
    return map[string]pipeline.CursorType{"raw_telemetry": pipeline.CursorTimestamp}
}
func (p *Projector) AlgorithmVersion() string { return "v3" }

func (p *Projector) Materialize(ctx context.Context, cursors pipeline.Cursors) (pipeline.Cursors, error) {
    lastObserved := cursors["raw_telemetry"].Timestamp

    rows, err := p.store.ReadRawTelemetry(ctx, store.ReadTelemetryArgs{
        SourceType:    "themeparks_wiki_live",
        ObservedAfter: lastObserved,
        Limit:         5000,
    })
    if err != nil { return cursors, err }

    var out []store.WaitTime
    var maxObserved time.Time
    for _, tel := range rows {
        parsed, ok := decodeLivePayload(tel.Payload)
        if !ok { continue }
        parkID := jsonGet(tel.Subject, "park_id")

        projected := project(parsed, parkID, tel.TelemetryID, tel.ObservedAt)
        projected = applyFilters(projected, parkID, tel.ObservedAt) // park-closed, dedup, etc.

        out = append(out, projected...)
        if tel.ObservedAt.After(maxObserved) { maxObserved = tel.ObservedAt }
    }

    if err := p.store.InsertWaitTimes(ctx, out); err != nil {
        return cursors, err
    }
    cursors["raw_telemetry"] = pipeline.Cursor{Type: pipeline.CursorTimestamp, Timestamp: maxObserved}
    return cursors, nil
}
```

### 12.4 A metric processor (`wait_times` → `wait_velocity`)

```go
package wait_velocity

func (p *Processor) ID() string              { return "wait_velocity" }
func (p *Processor) DependsOn() []string     { return []string{"wait_times"} }
func (p *Processor) CursorTypes() map[string]pipeline.CursorType {
    return map[string]pipeline.CursorType{"wait_times": pipeline.CursorTimestamp}
}
func (p *Processor) AlgorithmVersion() string { return "v2" }

func (p *Processor) Materialize(ctx context.Context, cursors pipeline.Cursors) (pipeline.Cursors, error) {
    from := cursors["wait_times"].Timestamp

    // Internal detail: we reach back 1h for EMA continuity. The framework
    // does not need to know this; lookback is a property of the math.
    rows, err := p.store.ReadWaitTimesForVelocity(ctx, from, 1*time.Hour)
    if err != nil { return cursors, err }

    velocities := computeVelocitiesAndEMA(rows)
    if err := p.store.InsertWaitVelocities(ctx, velocities); err != nil {
        return cursors, err
    }
    cursors["wait_times"] = pipeline.Cursor{Type: pipeline.CursorTimestamp, Timestamp: rows.MaxTimestamp()}
    return cursors, nil
}
```

### 12.5 A multi-upstream processor (`wait_times` + `park_schedules` → `crowd_pressure`)

```go
package crowd_pressure

func (p *Processor) ID() string          { return "crowd_pressure" }
func (p *Processor) DependsOn() []string { return []string{"wait_times", "park_schedules"} }

func (p *Processor) Materialize(ctx context.Context, cursors pipeline.Cursors) (pipeline.Cursors, error) {
    // Framework already advanced cursors only as far as min(upstream watermarks).
    wtFrom := cursors["wait_times"].Timestamp
    schedFrom := cursors["park_schedules"].Timestamp

    waits, _ := p.store.ReadWaitTimesAfter(ctx, wtFrom)
    scheds, _ := p.store.ReadSchedulesAfter(ctx, schedFrom)

    rows := computeCrowdPressure(waits, scheds)
    if err := p.store.InsertCrowdPressure(ctx, rows); err != nil {
        return cursors, err
    }

    target := rows.MaxTimestamp()
    cursors["wait_times"]     = pipeline.Cursor{Type: pipeline.CursorTimestamp, Timestamp: target}
    cursors["park_schedules"] = pipeline.Cursor{Type: pipeline.CursorTimestamp, Timestamp: target}
    return cursors, nil
}
```

### 12.6 Startup

```go
func main() {
    store := store.New(pool)
    registry := pipeline.NewRegistry()

    registry.Register(wait_times.NewProjector(store))
    registry.Register(wait_velocity.NewProcessor(store))
    registry.Register(status_transitions.NewProcessor(store))
    registry.Register(crowd_pressure.NewProcessor(store))
    registry.Register(daily_attraction_stats.NewProcessor(store))

    // Observers are independent; they don't implement Processor.
    observer := themeparks_live.NewObserver(client, store)
    go observer.Run(ctx, cfg.PollInterval)

    // The scheduler walks the DAG on each tick, advancing processors that are ready.
    sched := pipeline.NewScheduler(registry, store, 30*time.Second)
    if err := sched.Run(ctx); err != nil {
        log.Fatal(err)
    }
}
```

## 15. Open Decisions

Tracked here to preserve context between design sessions. Each resolves or gets removed.

- [ ] **Observer interface** extracted in Phase 5 (when second observer arrives) or earlier? Current plan: extract when there are two concrete examples.
- [ ] **Cursor storage shape**: one row per `(processor, dep)` with JSONB position (current plan) vs one row per processor with JSONB-of-all-deps. Row-per-dep is easier to query and index.
- [ ] **Algorithm version co-existence**: on version bump, truncate old output or keep both versions side-by-side? Current plan: truncate by default; processors can opt into versioned retention.
- [ ] **Late-data handling**: pessimistic watermark + safety buffer (current plan) vs partition-invalidation on retrograde data (more accurate, more complex).
- [ ] **Scheduler granularity**: one tick walks the whole DAG (simple) vs per-processor goroutines (more concurrent). Start with the walker; split only if latency demands it.
- [ ] **`raw_telemetry` retention policy** — needed before L1 volume matters. 90d hot is a placeholder.
- [ ] **Describe/catalog surface**: CLI command, generated table, or both?
- [ ] **Lineage for multi-telemetry derived rows**: sidecar table pattern vs `uuid[]` array column. Array is simpler; sidecar is more index-friendly for reverse queries.
- [ ] **Portal run history**: in-memory ring buffer (simplest, lost on restart) vs persisted `processor_runs` table (durable, queryable, extra write per materialization). Start with in-memory; promote if durability becomes necessary.
- [ ] **Portal DAG rendering**: inline Mermaid via a single embedded client-side script (zero server deps, requires JS in browser) vs server-side Graphviz (adds `dot` binary dependency, works without JS). Lean Mermaid — standalone HTML file consumers can also render it.
- [ ] **Portal admin-action confirmation**: in-browser confirmation dialog for destructive actions (replay, reset-cursor) vs relying purely on audit log after the fact. Lean toward browser confirmation for interactive use; API callers handle their own safety.
- [ ] **Portal + metrics on same listener or separate ports**: convention says separate (distinct security zones) but thin self-hosted services often co-locate. Start co-located; split via `METRICS_ADDR` if an operational need emerges.
- [ ] **Audit log retention**: indefinite (useful history but unbounded growth) vs. time-capped. Volume is low; indefinite is fine until it isn't.
- [ ] **Tracing (OTel) scope**: not in this document's scope. Decide separately if/when request-level tracing is wanted — if yes, it's a framework-level concern (one span per materialization) and slots in alongside metrics.

---

*End of document. Refine in place; phase plans draw from specific sections.*