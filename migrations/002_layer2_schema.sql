-- Layer 2: Derived metrics computed from raw wait_times data

-- Cursor tracking for incremental processing
CREATE TABLE IF NOT EXISTS l2_processing_cursors (
    metric_name TEXT PRIMARY KEY,
    last_processed TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Wait velocity: trend direction and rate of change per attraction per poll
CREATE TABLE IF NOT EXISTS l2_wait_velocity (
    attraction_id UUID        NOT NULL REFERENCES attractions(id),
    timestamp     TIMESTAMPTZ NOT NULL,
    velocity      REAL,
    velocity_ema  REAL,
    trend         TEXT        NOT NULL,
    sample_count  SMALLINT    NOT NULL DEFAULT 1,
    PRIMARY KEY (attraction_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_l2_velocity_trend
    ON l2_wait_velocity (trend, timestamp);

-- Status transitions: records when a ride changes operating status
CREATE TABLE IF NOT EXISTS l2_status_transitions (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attraction_id   UUID        NOT NULL REFERENCES attractions(id),
    transition_time TIMESTAMPTZ NOT NULL,
    previous_status TEXT        NOT NULL,
    new_status      TEXT        NOT NULL,
    duration_seconds INT
);

CREATE INDEX IF NOT EXISTS idx_l2_transitions_attraction_time
    ON l2_status_transitions (attraction_id, transition_time);

CREATE INDEX IF NOT EXISTS idx_l2_transitions_time
    ON l2_status_transitions (transition_time);

-- Daily attraction stats (materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS l2_daily_attraction_stats AS
SELECT
    a.id AS attraction_id,
    a.park_id,
    date_trunc('day', wt.timestamp AT TIME ZONE p.timezone)::date AS day,
    count(*) AS sample_count,
    count(*) FILTER (WHERE wt.status = 'OPERATING') AS operating_samples,
    count(*) FILTER (WHERE wt.status = 'DOWN') AS down_samples,
    min(wt.standby_wait) FILTER (WHERE wt.status = 'OPERATING') AS min_wait,
    max(wt.standby_wait) FILTER (WHERE wt.status = 'OPERATING') AS max_wait,
    avg(wt.standby_wait) FILTER (WHERE wt.status = 'OPERATING')::REAL AS avg_wait,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY wt.standby_wait)
        FILTER (WHERE wt.status = 'OPERATING')::REAL AS median_wait,
    percentile_cont(0.9) WITHIN GROUP (ORDER BY wt.standby_wait)
        FILTER (WHERE wt.status = 'OPERATING')::REAL AS p90_wait,
    min(wt.timestamp) FILTER (WHERE wt.status = 'OPERATING') AS first_operating,
    max(wt.timestamp) FILTER (WHERE wt.status = 'OPERATING') AS last_operating
FROM wait_times wt
JOIN attractions a ON a.id = wt.attraction_id
JOIN parks p ON p.id = a.park_id
WHERE wt.standby_wait IS NOT NULL OR wt.status != 'OPERATING'
GROUP BY a.id, a.park_id, date_trunc('day', wt.timestamp AT TIME ZONE p.timezone)::date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_l2_daily_attraction_stats_pk
    ON l2_daily_attraction_stats (attraction_id, day);

-- Daily park stats (materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS l2_daily_park_stats AS
SELECT
    das.park_id,
    das.day,
    count(*) AS attractions_reporting,
    count(*) FILTER (WHERE das.operating_samples > 0) AS attractions_operating,
    avg(das.avg_wait)::REAL AS avg_wait,
    max(das.max_wait) AS peak_wait,
    sum(das.sample_count) AS total_samples
FROM l2_daily_attraction_stats das
GROUP BY das.park_id, das.day;

CREATE UNIQUE INDEX IF NOT EXISTS idx_l2_daily_park_stats_pk
    ON l2_daily_park_stats (park_id, day);
