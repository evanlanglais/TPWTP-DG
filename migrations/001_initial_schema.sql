-- Static reference data (refreshed on startup + periodically)
CREATE TABLE IF NOT EXISTS destinations (
    id   UUID PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parks (
    id             UUID PRIMARY KEY,
    destination_id UUID NOT NULL REFERENCES destinations(id),
    name           TEXT NOT NULL,
    timezone       TEXT NOT NULL DEFAULT 'UTC'
);

CREATE TABLE IF NOT EXISTS attractions (
    id          UUID PRIMARY KEY,
    park_id     UUID NOT NULL REFERENCES parks(id),
    name        TEXT NOT NULL,
    external_id TEXT
);

-- Time-series data (partitioned by month)
CREATE TABLE IF NOT EXISTS wait_times (
    attraction_id     UUID        NOT NULL REFERENCES attractions(id),
    timestamp         TIMESTAMPTZ NOT NULL,
    status            TEXT        NOT NULL,
    standby_wait      INT,
    single_rider_wait INT,
    return_time_state TEXT,
    return_time_start TIMESTAMPTZ,
    return_time_end   TIMESTAMPTZ,
    PRIMARY KEY (attraction_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_wait_times_timestamp
    ON wait_times (timestamp);

CREATE INDEX IF NOT EXISTS idx_wait_times_attraction_status
    ON wait_times (attraction_id, status, timestamp);
