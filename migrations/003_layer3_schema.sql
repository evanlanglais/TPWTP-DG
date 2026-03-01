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

-- Transition impact: how ride closures and openings affect surrounding rides
-- event_type: 'CLOSURE' (OPERATING→non-OPERATING) or 'OPENING' (non-OPERATING→OPERATING)
-- baseline_delta: velocity change over 30 min BEFORE the event (pre-existing trend)
-- excess_delta: velocity_delta - baseline_delta (portion attributable to the event)
-- Only rows where excess_delta is in the expected direction are stored:
--   closures: excess_delta > 0 (ride accelerated beyond pre-existing trend)
--   openings: excess_delta < 0 (ride decelerated beyond pre-existing trend)
-- absorption_score: for closures, normalized share of excess_delta (sums to 1.0)
-- drain_score: for openings, normalized share of |excess_delta| (sums to 1.0)
CREATE TABLE IF NOT EXISTS l3_transition_impact (
    id                      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transition_id           BIGINT       NOT NULL,
    event_type              TEXT         NOT NULL,
    transitioned_attraction UUID         NOT NULL REFERENCES attractions(id),
    impacted_attraction     UUID         NOT NULL REFERENCES attractions(id),
    park_id                 UUID         NOT NULL REFERENCES parks(id),
    transition_time         TIMESTAMPTZ  NOT NULL,
    velocity_before         REAL,
    velocity_after          REAL,
    velocity_delta          REAL,
    baseline_delta          REAL,
    excess_delta            REAL,
    wait_before             INT,
    wait_after              INT,
    wait_delta              INT,
    absorption_score        REAL,
    drain_score             REAL
);

CREATE INDEX IF NOT EXISTS idx_l3_transition_transition_id
    ON l3_transition_impact (transition_id);

CREATE INDEX IF NOT EXISTS idx_l3_transition_park_time
    ON l3_transition_impact (park_id, transition_time);

CREATE INDEX IF NOT EXISTS idx_l3_transition_attraction_time
    ON l3_transition_impact (transitioned_attraction, transition_time);

CREATE INDEX IF NOT EXISTS idx_l3_transition_event_type
    ON l3_transition_impact (event_type, transition_time);
