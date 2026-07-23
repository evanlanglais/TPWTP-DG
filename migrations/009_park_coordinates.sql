-- Weather is now fetched per PARK-CLUSTER, not per destination: parks are grouped at
-- ~2km (OpenWeather's grid resolution) and one call serves each cluster, then projected
-- to every park in it. This supersedes the destination-centroid approach in migration
-- 008 — the coordinate belongs on `parks` so co-located parks can be clustered and
-- spread-out resorts (Walt Disney World, Universal Orlando) fetched separately.
--
-- Idempotent and convergent regardless of prior state: adds the park columns if missing
-- and drops the now-unused destination columns if present. 008's weather_observations /
-- weather_forecasts tables are unchanged (their schema was already correct).

-- Park coordinates: the centroid of a park's attraction locations, populated during
-- reference sync and used to cluster parks into fetch locations. Nullable with no default
-- = instant metadata-only change (no rewrite); a park with no located attractions stays
-- NULL and is skipped by the weather poll (fail-safe).
ALTER TABLE parks ADD COLUMN IF NOT EXISTS latitude  DOUBLE PRECISION;
ALTER TABLE parks ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION;

-- Drop the superseded destination coordinates (added by 008, never read now).
ALTER TABLE destinations DROP COLUMN IF EXISTS latitude;
ALTER TABLE destinations DROP COLUMN IF EXISTS longitude;
