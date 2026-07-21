-- Lineage: link each projected wait_times row back to the raw_observations row
-- it was projected from (invariant I1 traceability).
--
-- Nullable with NO default is a metadata-only change (instant, no partition
-- rewrite). Old rows keep NULL; only rows written after this migration carry a
-- pointer. A volatile default would rewrite every partition and is forbidden.
ALTER TABLE wait_times
    ADD COLUMN IF NOT EXISTS source_observation_id UUID;
