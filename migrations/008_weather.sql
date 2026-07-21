-- Weather: a second data source (OpenWeatherMap One Call 3.0), honoring the same
-- invariants as the wait-time source. Raw responses land verbatim in raw_observations
-- (source_type = 'openweather_onecall') BEFORE these projections (I1); no schema change
-- is needed there since source_type already discriminates. See docs/ARCHITECTURE.md §6/§11.
--
-- Weather is fetched once per DESTINATION (parks in a resort are within a few km and
-- share weather) and projected to each park in that destination, so both tables stay
-- park-keyed like wait_times for a trivial ML join.

-- Destination coordinates: the centroid of a destination's attraction locations,
-- populated during reference sync. Nullable with no default = instant metadata-only
-- change (no rewrite); a destination with no located attractions stays NULL and is
-- skipped by the weather poll (fail-safe).
ALTER TABLE destinations ADD COLUMN IF NOT EXISTS latitude  DOUBLE PRECISION;
ALTER TABLE destinations ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- weather_observations — CURRENT conditions. PIT-correct (I2): the row is stamped at
-- the observation's valid `timestamp`, and its value uses only data at-or-before that
-- time, so it is safe to read as an at-t feature. Serves temp_f / precip_mm_now /
-- "currently raining". Monthly partitions (by timestamp) created lazily in Go.
CREATE TABLE IF NOT EXISTS weather_observations (
    park_id       UUID        NOT NULL REFERENCES parks(id),
    timestamp     TIMESTAMPTZ NOT NULL,   -- observation valid time (PIT stamp)
    temp_c        REAL,
    feels_like_c  REAL,
    precip_mm     REAL,                   -- precipitation in the last hour (rain + snow)
    humidity      INT,                    -- percent
    wind_speed_ms REAL,
    cloud_pct     INT,
    weather_code  INT,                    -- OWM condition id (encodes rain/snow/clear)
    weather_main  TEXT,                   -- e.g. 'Rain', 'Clear'
    source_observation_id UUID,           -- lineage -> raw_observations (I1)
    PRIMARY KEY (park_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_weather_obs_park_ts
    ON weather_observations (park_id, timestamp);

-- weather_forecasts — bi-temporal. PIT-correct ONLY when read as-of `issued_at`
-- (invariant I2). issued_at = when the forecast became knowable (our fetch time) and is
-- the as-of/PIT key; valid_at = the future time it describes. A forecast for t+60 is a
-- legal at-t feature *because* it was issued at issued_at <= t. An as-of query MUST key on
-- issued_at, never valid_at (keying on valid_at is the l3_transition_impact trap:
-- stamped-at-one-time, value-from-another). A forecast row is NEVER overwritten with the
-- later actual — both must coexist. The actual weather at valid_at is future-dependent and
-- belongs to the ML component, not here. Monthly partitions (by issued_at) created in Go.
CREATE TABLE IF NOT EXISTS weather_forecasts (
    park_id         UUID        NOT NULL REFERENCES parks(id),
    issued_at       TIMESTAMPTZ NOT NULL,  -- when knowable (fetch time) = as-of/PIT key
    valid_at        TIMESTAMPTZ NOT NULL,  -- the future time described
    horizon_minutes INT         NOT NULL,  -- valid_at - issued_at, in minutes
    temp_c          REAL,
    precip_mm       REAL,
    precip_prob     REAL,                  -- probability of precipitation, 0..1 (OWM pop)
    weather_code    INT,
    weather_main    TEXT,
    source_observation_id UUID,            -- lineage -> raw_observations (I1)
    PRIMARY KEY (park_id, issued_at, valid_at)
) PARTITION BY RANGE (issued_at);

-- Serves the ML's as-of query: latest forecast for a target valid_at where issued_at <= t.
CREATE INDEX IF NOT EXISTS idx_weather_fc_park_valid
    ON weather_forecasts (park_id, valid_at, issued_at);
