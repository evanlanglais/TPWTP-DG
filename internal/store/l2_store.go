package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// WaitTimeDelta is the raw per-interval change data returned by the LAG() query.
type WaitTimeDelta struct {
	AttractionID string
	Timestamp    time.Time
	Status       string
	StandbyWait  *int
	PrevWait     *int
	Delta        *int
	SampleCount  int16
}

// WaitVelocity is a computed velocity record ready for insertion.
type WaitVelocity struct {
	AttractionID string
	Timestamp    time.Time
	Velocity     *float32
	VelocityEMA  *float32
	Trend        string
	SampleCount  int16
}

// GetEarliestWaitTime returns the earliest timestamp in the wait_times table.
// Returns zero time if the table is empty.
func (s *Store) GetEarliestWaitTime(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx,
		"SELECT min(timestamp) FROM wait_times",
	).Scan(&t)
	if err != nil {
		return time.Time{}, fmt.Errorf("getting earliest wait time: %w", err)
	}
	return t, nil
}

// GetProcessingCursor returns the last-processed timestamp and logic version for
// the given metric. Returns (zero time, 0, nil) if no cursor exists yet.
func (s *Store) GetProcessingCursor(ctx context.Context, metricName string) (time.Time, int, error) {
	var t time.Time
	var version int
	err := s.pool.QueryRow(ctx,
		"SELECT last_processed, logic_version FROM l2_processing_cursors WHERE metric_name = $1",
		metricName,
	).Scan(&t, &version)
	if err == pgx.ErrNoRows {
		return time.Time{}, 0, nil
	}
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("getting cursor for %s: %w", metricName, err)
	}
	return t, version, nil
}

// SetProcessingCursor upserts the cursor timestamp and logic version for a metric.
func (s *Store) SetProcessingCursor(ctx context.Context, metricName string, t time.Time, version int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO l2_processing_cursors (metric_name, last_processed, logic_version, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (metric_name) DO UPDATE SET
			last_processed = EXCLUDED.last_processed,
			logic_version = EXCLUDED.logic_version,
			updated_at = now()
	`, metricName, t, version)
	if err != nil {
		return fmt.Errorf("setting cursor for %s: %w", metricName, err)
	}
	return nil
}

// DeleteProcessingCursor removes the cursor row for a metric, used when
// logic version changes to trigger a full re-backfill.
func (s *Store) DeleteProcessingCursor(ctx context.Context, metricName string) error {
	_, err := s.pool.Exec(ctx,
		"DELETE FROM l2_processing_cursors WHERE metric_name = $1",
		metricName,
	)
	if err != nil {
		return fmt.Errorf("deleting cursor for %s: %w", metricName, err)
	}
	return nil
}

// TruncateTables truncates each of the given tables. Used to purge stale
// derived data when a metric's logic version changes.
func (s *Store) TruncateTables(ctx context.Context, tables []string) error {
	for _, table := range tables {
		sql := fmt.Sprintf("TRUNCATE TABLE %s", table)
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("truncating table %s: %w", table, err)
		}
		slog.Info("truncated table for metric re-backfill", "table", table)
	}
	return nil
}

// GetRecentWaitTimeDeltas returns per-attraction wait time deltas for the given
// time window using a LAG() window function. Each row includes the current and
// previous standby wait plus a running count of non-null observations.
func (s *Store) GetRecentWaitTimeDeltas(ctx context.Context, since, until time.Time) ([]WaitTimeDelta, error) {
	lookback := since.Add(-30 * time.Minute)
	rows, err := s.pool.Query(ctx, `
		WITH windowed AS (
			SELECT
				attraction_id,
				timestamp,
				status,
				standby_wait,
				LAG(standby_wait) OVER (
					PARTITION BY attraction_id ORDER BY timestamp
				) AS prev_wait,
				ROW_NUMBER() OVER (
					PARTITION BY attraction_id ORDER BY timestamp
				)::SMALLINT AS sample_count
			FROM wait_times
			WHERE timestamp > $1
			  AND timestamp <= $2
		)
		SELECT
			attraction_id,
			timestamp,
			status,
			standby_wait,
			prev_wait,
			CASE WHEN standby_wait IS NOT NULL AND prev_wait IS NOT NULL
				THEN standby_wait - prev_wait
			END AS delta,
			sample_count
		FROM windowed
		WHERE timestamp > $3
		  AND timestamp <= $2
		ORDER BY attraction_id, timestamp
	`, lookback, until, since)
	if err != nil {
		return nil, fmt.Errorf("querying wait time deltas: %w", err)
	}
	defer rows.Close()

	var deltas []WaitTimeDelta
	for rows.Next() {
		var d WaitTimeDelta
		if err := rows.Scan(
			&d.AttractionID, &d.Timestamp, &d.Status,
			&d.StandbyWait, &d.PrevWait, &d.Delta, &d.SampleCount,
		); err != nil {
			return nil, fmt.Errorf("scanning wait time delta: %w", err)
		}
		deltas = append(deltas, d)
	}
	return deltas, rows.Err()
}

// InsertWaitVelocities bulk-inserts velocity records using COPY.
func (s *Store) InsertWaitVelocities(ctx context.Context, velocities []WaitVelocity) (int, error) {
	if len(velocities) == 0 {
		return 0, nil
	}

	// Ensure partitions exist
	for _, v := range velocities {
		if err := s.ensureL2VelocityPartition(ctx, v.Timestamp); err != nil {
			return 0, err
		}
	}

	copyRows := make([][]any, len(velocities))
	for i, v := range velocities {
		copyRows[i] = []any{
			v.AttractionID,
			v.Timestamp,
			v.Velocity,
			v.VelocityEMA,
			v.Trend,
			v.SampleCount,
		}
	}

	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"l2_wait_velocity"},
		[]string{"attraction_id", "timestamp", "velocity", "velocity_ema", "trend", "sample_count"},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk inserting wait velocities: %w", err)
	}
	return int(n), nil
}

// ensureL2VelocityPartition creates a monthly partition for l2_wait_velocity.
func (s *Store) ensureL2VelocityPartition(ctx context.Context, t time.Time) error {
	year, month, _ := t.UTC().Date()
	key := fmt.Sprintf("l2v_%d_%02d", year, month)

	if _, ok := s.partitions.Load(key); ok {
		return nil
	}

	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partName := fmt.Sprintf("l2_wait_velocity_%d_%02d", year, month)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF l2_wait_velocity
		FOR VALUES FROM ('%s') TO ('%s')
	`, partName, start.Format(time.DateOnly), end.Format(time.DateOnly))

	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating partition %s: %w", partName, err)
	}
	slog.Info("ensured L2 velocity partition", "name", partName)
	s.partitions.Store(key, true)
	return nil
}

// DetectAndInsertStatusTransitions finds status changes in wait_times between
// since and until, and inserts them into l2_status_transitions.
// Uses a LATERAL join to find each row's actual predecessor, which handles
// gaps from deduplication of unchanged non-OPERATING rows.
// Returns the number of transitions inserted.
func (s *Store) DetectAndInsertStatusTransitions(ctx context.Context, since, until time.Time) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH current_rows AS (
			SELECT attraction_id, timestamp, status
			FROM wait_times
			WHERE timestamp > $1
			  AND timestamp <= $2
		)
		INSERT INTO l2_status_transitions (attraction_id, transition_time, previous_status, new_status, duration_seconds)
		SELECT
			cr.attraction_id,
			cr.timestamp,
			prev.status,
			cr.status,
			EXTRACT(EPOCH FROM cr.timestamp - prev.timestamp)::INT
		FROM current_rows cr
		CROSS JOIN LATERAL (
			SELECT w.status, w.timestamp
			FROM wait_times w
			WHERE w.attraction_id = cr.attraction_id
			  AND w.timestamp < cr.timestamp
			ORDER BY w.timestamp DESC
			LIMIT 1
		) prev
		WHERE prev.status != cr.status
	`, since, until)
	if err != nil {
		return 0, fmt.Errorf("detecting status transitions: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// RefreshMaterializedView concurrently refreshes the named materialized view.
func (s *Store) RefreshMaterializedView(ctx context.Context, viewName string) error {
	sql := fmt.Sprintf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s", viewName)
	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("refreshing materialized view %s: %w", viewName, err)
	}
	return nil
}
