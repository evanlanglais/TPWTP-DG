-- Raw observations: verbatim source responses, persisted BEFORE projection.
-- Invariant I1 (full replayability): a feature we haven't thought of yet can
-- only be built later if the raw signal still exists. See docs/ARCHITECTURE.md §3.
--
-- Monthly partitions (by received_at) are created lazily in Go, mirroring
-- wait_times — there is no declarative partitioning or partition manager here.
CREATE TABLE IF NOT EXISTS raw_observations (
    observation_id UUID        NOT NULL,
    source_type    TEXT        NOT NULL,   -- e.g. 'themeparks_wiki_live'
    observed_at    TIMESTAMPTZ NOT NULL,   -- when the fact was true upstream
    received_at    TIMESTAMPTZ NOT NULL,   -- when we wrote this row (partition key)
    payload        JSONB,                  -- verbatim response body; NULL on failed fetch
    metadata       JSONB,                  -- delivery info: {http_status, latency_ms, ...}
    PRIMARY KEY (observation_id, received_at)
) PARTITION BY RANGE (received_at);

CREATE INDEX IF NOT EXISTS idx_raw_observations_source_received
    ON raw_observations (source_type, received_at);
