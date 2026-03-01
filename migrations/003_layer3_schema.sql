-- Layer 3: Park-level derived metrics computed from Layer 2 data

-- Park crowd pressure: wait-weighted velocity EMA aggregated per park per poll
CREATE TABLE IF NOT EXISTS l3_park_crowd_pressure (
    park_id         UUID        NOT NULL REFERENCES parks(id),
    timestamp       TIMESTAMPTZ NOT NULL,
    pressure_score  REAL        NOT NULL,
    pressure_ema    REAL        NOT NULL,
    trend           TEXT        NOT NULL,
    operating_count SMALLINT    NOT NULL,
    avg_wait        REAL        NOT NULL,
    PRIMARY KEY (park_id, timestamp)
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_l3_pressure_trend
    ON l3_park_crowd_pressure (park_id, trend, timestamp);

-- Correlation pairs: pairwise Pearson r between attractions' velocity EMA series
CREATE TABLE IF NOT EXISTS l3_correlation_pairs (
    park_id       UUID        NOT NULL REFERENCES parks(id),
    attraction_a  UUID        NOT NULL REFERENCES attractions(id),
    attraction_b  UUID        NOT NULL REFERENCES attractions(id),
    computed_at   TIMESTAMPTZ NOT NULL,
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    correlation   REAL        NOT NULL,
    sample_count  SMALLINT    NOT NULL,
    PRIMARY KEY (park_id, attraction_a, attraction_b, computed_at)
);

CREATE INDEX IF NOT EXISTS idx_l3_correlation_computed
    ON l3_correlation_pairs (computed_at);

CREATE INDEX IF NOT EXISTS idx_l3_correlation_park_computed
    ON l3_correlation_pairs (park_id, computed_at);

-- Closure impact: how closing one ride affects nearby rides' wait times
CREATE TABLE IF NOT EXISTS l3_closure_impact (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transition_id       BIGINT       NOT NULL,
    closed_attraction   UUID         NOT NULL REFERENCES attractions(id),
    impacted_attraction UUID         NOT NULL REFERENCES attractions(id),
    park_id             UUID         NOT NULL REFERENCES parks(id),
    closure_time        TIMESTAMPTZ  NOT NULL,
    velocity_before     REAL,
    velocity_after      REAL,
    velocity_delta      REAL,
    wait_before         INT,
    wait_after          INT,
    wait_delta          INT,
    absorption_score    REAL
);

CREATE INDEX IF NOT EXISTS idx_l3_closure_transition
    ON l3_closure_impact (transition_id);

CREATE INDEX IF NOT EXISTS idx_l3_closure_park_time
    ON l3_closure_impact (park_id, closure_time);

CREATE INDEX IF NOT EXISTS idx_l3_closure_attraction_time
    ON l3_closure_impact (closed_attraction, closure_time);
