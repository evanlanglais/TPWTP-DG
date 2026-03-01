package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// VelocitySnapshot is a per-attraction velocity + wait snapshot grouped by park.
type VelocitySnapshot struct {
	ParkID       string
	AttractionID string
	Timestamp    time.Time
	VelocityEMA  float32
	StandbyWait  int
	Status       string
}

// ParkCrowdPressure is a computed park-level crowd pressure record.
type ParkCrowdPressure struct {
	ParkID         string
	Timestamp      time.Time
	PressureScore  float32
	PressureEMA    float32
	Trend          string
	OperatingCount int16
	AvgWait        float32
}

// CorrelationPair is a pairwise correlation between two attractions.
type CorrelationPair struct {
	ParkID      string
	AttractionA string
	AttractionB string
	ComputedAt  time.Time
	WindowStart time.Time
	WindowEnd   time.Time
	Correlation float32
	SampleCount int16
}

// TimestampedValue is a single (timestamp, value) pair for time-series data.
type TimestampedValue struct {
	Timestamp time.Time
	Value     float32
}

// TransitionImpact records how one ride's closure or opening affected another ride.
type TransitionImpact struct {
	TransitionID           int64
	EventType              string // "CLOSURE" or "OPENING"
	TransitionedAttraction string
	ImpactedAttraction     string
	ParkID                 string
	TransitionTime         time.Time
	VelocityBefore         *float32
	VelocityAfter          *float32
	VelocityDelta          *float32
	BaselineDelta          *float32
	ExcessDelta            *float32
	WaitBefore             *int
	WaitAfter              *int
	WaitDelta              *int
	AbsorptionScore        *float32
	DrainScore             *float32
}

// StatusTransitionRecord is a transition event from l2_status_transitions.
type StatusTransitionRecord struct {
	ID             int64
	AttractionID   string
	ParkID         string
	TransitionTime time.Time
	PreviousStatus string
	NewStatus      string
}

// AttractionSnapshot is a point-in-time velocity + wait for a single attraction.
type AttractionSnapshot struct {
	AttractionID string
	VelocityEMA  float32
	StandbyWait  int
}

// GetParkVelocitySnapshots returns velocity EMA + wait data grouped by park
// for the given time window by joining l2_wait_velocity with wait_times.
func (s *Store) GetParkVelocitySnapshots(ctx context.Context, since, until time.Time) ([]VelocitySnapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			a.park_id,
			v.attraction_id,
			v.timestamp,
			v.velocity_ema,
			COALESCE(wt.standby_wait, 0),
			wt.status
		FROM l2_wait_velocity v
		JOIN attractions a ON a.id = v.attraction_id
		JOIN wait_times wt ON wt.attraction_id = v.attraction_id
			AND wt.timestamp = v.timestamp
		WHERE v.timestamp > $1
		  AND v.timestamp <= $2
		  AND wt.status = 'OPERATING'
		  AND v.velocity_ema IS NOT NULL
		ORDER BY a.park_id, v.timestamp, v.attraction_id
	`, since, until)
	if err != nil {
		return nil, fmt.Errorf("querying park velocity snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []VelocitySnapshot
	for rows.Next() {
		var s VelocitySnapshot
		if err := rows.Scan(
			&s.ParkID, &s.AttractionID, &s.Timestamp,
			&s.VelocityEMA, &s.StandbyWait, &s.Status,
		); err != nil {
			return nil, fmt.Errorf("scanning velocity snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}
	return snapshots, rows.Err()
}

// InsertParkCrowdPressures bulk-inserts crowd pressure records using COPY.
func (s *Store) InsertParkCrowdPressures(ctx context.Context, pressures []ParkCrowdPressure) (int, error) {
	if len(pressures) == 0 {
		return 0, nil
	}

	for _, p := range pressures {
		if err := s.ensureL3PressurePartition(ctx, p.Timestamp); err != nil {
			return 0, err
		}
	}

	copyRows := make([][]any, len(pressures))
	for i, p := range pressures {
		copyRows[i] = []any{
			p.ParkID,
			p.Timestamp,
			p.PressureScore,
			p.PressureEMA,
			p.Trend,
			p.OperatingCount,
			p.AvgWait,
		}
	}

	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"l3_park_crowd_pressure"},
		[]string{"park_id", "timestamp", "pressure_score", "pressure_ema", "trend", "operating_count", "avg_wait"},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk inserting crowd pressures: %w", err)
	}
	return int(n), nil
}

// ensureL3PressurePartition creates a monthly partition for l3_park_crowd_pressure.
func (s *Store) ensureL3PressurePartition(ctx context.Context, t time.Time) error {
	year, month, _ := t.UTC().Date()
	key := fmt.Sprintf("l3p_%d_%02d", year, month)

	if _, ok := s.partitions.Load(key); ok {
		return nil
	}

	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partName := fmt.Sprintf("l3_park_crowd_pressure_%d_%02d", year, month)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF l3_park_crowd_pressure
		FOR VALUES FROM ('%s') TO ('%s')
	`, partName, start.Format(time.DateOnly), end.Format(time.DateOnly))

	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating partition %s: %w", partName, err)
	}
	slog.Info("ensured L3 pressure partition", "name", partName)
	s.partitions.Store(key, true)
	return nil
}

// GetVelocityEMAWindow returns velocity EMA time-series data for all attractions
// in a park within the given time window, keyed by attraction ID.
func (s *Store) GetVelocityEMAWindow(ctx context.Context, parkID string, start, end time.Time) (map[string][]TimestampedValue, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT v.attraction_id, v.timestamp, v.velocity_ema
		FROM l2_wait_velocity v
		JOIN attractions a ON a.id = v.attraction_id
		WHERE a.park_id = $1
		  AND v.timestamp > $2
		  AND v.timestamp <= $3
		  AND v.velocity_ema IS NOT NULL
		ORDER BY v.attraction_id, v.timestamp
	`, parkID, start, end)
	if err != nil {
		return nil, fmt.Errorf("querying velocity EMA window: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]TimestampedValue)
	for rows.Next() {
		var attrID string
		var tv TimestampedValue
		if err := rows.Scan(&attrID, &tv.Timestamp, &tv.Value); err != nil {
			return nil, fmt.Errorf("scanning velocity EMA: %w", err)
		}
		result[attrID] = append(result[attrID], tv)
	}
	return result, rows.Err()
}

// InsertCorrelationPairs batch-inserts correlation pair records.
func (s *Store) InsertCorrelationPairs(ctx context.Context, pairs []CorrelationPair) (int, error) {
	if len(pairs) == 0 {
		return 0, nil
	}

	copyRows := make([][]any, len(pairs))
	for i, p := range pairs {
		copyRows[i] = []any{
			p.ParkID,
			p.AttractionA,
			p.AttractionB,
			p.ComputedAt,
			p.WindowStart,
			p.WindowEnd,
			p.Correlation,
			p.SampleCount,
		}
	}

	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"l3_correlation_pairs"},
		[]string{"park_id", "attraction_a", "attraction_b", "computed_at", "window_start", "window_end", "correlation", "sample_count"},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk inserting correlation pairs: %w", err)
	}
	return int(n), nil
}

// GetRecentClosures returns OPERATING→non-OPERATING transitions from l2_status_transitions
// within the given time window, joined with attraction data to get park_id.
func (s *Store) GetRecentClosures(ctx context.Context, since, until time.Time) ([]StatusTransitionRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.id, st.attraction_id, a.park_id, st.transition_time,
			   st.previous_status, st.new_status
		FROM l2_status_transitions st
		JOIN attractions a ON a.id = st.attraction_id
		WHERE st.transition_time > $1
		  AND st.transition_time <= $2
		  AND st.previous_status = 'OPERATING'
		  AND st.new_status != 'OPERATING'
		ORDER BY st.transition_time
	`, since, until)
	if err != nil {
		return nil, fmt.Errorf("querying recent closures: %w", err)
	}
	defer rows.Close()

	var transitions []StatusTransitionRecord
	for rows.Next() {
		var t StatusTransitionRecord
		if err := rows.Scan(
			&t.ID, &t.AttractionID, &t.ParkID, &t.TransitionTime,
			&t.PreviousStatus, &t.NewStatus,
		); err != nil {
			return nil, fmt.Errorf("scanning closure: %w", err)
		}
		transitions = append(transitions, t)
	}
	return transitions, rows.Err()
}

// GetRecentOpenings returns non-OPERATING→OPERATING transitions from l2_status_transitions
// within the given time window, joined with attraction data to get park_id.
func (s *Store) GetRecentOpenings(ctx context.Context, since, until time.Time) ([]StatusTransitionRecord, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.id, st.attraction_id, a.park_id, st.transition_time,
			   st.previous_status, st.new_status
		FROM l2_status_transitions st
		JOIN attractions a ON a.id = st.attraction_id
		WHERE st.transition_time > $1
		  AND st.transition_time <= $2
		  AND st.previous_status != 'OPERATING'
		  AND st.new_status = 'OPERATING'
		ORDER BY st.transition_time
	`, since, until)
	if err != nil {
		return nil, fmt.Errorf("querying recent openings: %w", err)
	}
	defer rows.Close()

	var transitions []StatusTransitionRecord
	for rows.Next() {
		var t StatusTransitionRecord
		if err := rows.Scan(
			&t.ID, &t.AttractionID, &t.ParkID, &t.TransitionTime,
			&t.PreviousStatus, &t.NewStatus,
		); err != nil {
			return nil, fmt.Errorf("scanning opening: %w", err)
		}
		transitions = append(transitions, t)
	}
	return transitions, rows.Err()
}

// GetVelocityAndWaitAtTime returns a snapshot of velocity EMA + wait time for all
// operating attractions in a park at a specific point in time (within tolerance).
func (s *Store) GetVelocityAndWaitAtTime(ctx context.Context, parkID string, target time.Time, tolerance time.Duration) ([]AttractionSnapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (v.attraction_id)
			v.attraction_id,
			v.velocity_ema,
			COALESCE(wt.standby_wait, 0)
		FROM l2_wait_velocity v
		JOIN attractions a ON a.id = v.attraction_id
		JOIN wait_times wt ON wt.attraction_id = v.attraction_id
			AND wt.timestamp = v.timestamp
		WHERE a.park_id = $1
		  AND v.timestamp >= $2
		  AND v.timestamp <= $3
		  AND v.velocity_ema IS NOT NULL
		  AND wt.status = 'OPERATING'
		ORDER BY v.attraction_id, ABS(EXTRACT(EPOCH FROM v.timestamp - $4))
	`, parkID, target.Add(-tolerance), target.Add(tolerance), target)
	if err != nil {
		return nil, fmt.Errorf("querying velocity/wait at time: %w", err)
	}
	defer rows.Close()

	var snapshots []AttractionSnapshot
	for rows.Next() {
		var s AttractionSnapshot
		if err := rows.Scan(&s.AttractionID, &s.VelocityEMA, &s.StandbyWait); err != nil {
			return nil, fmt.Errorf("scanning attraction snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}
	return snapshots, rows.Err()
}

// InsertTransitionImpacts batch-inserts transition impact records (closures and openings).
func (s *Store) InsertTransitionImpacts(ctx context.Context, impacts []TransitionImpact) (int, error) {
	if len(impacts) == 0 {
		return 0, nil
	}

	copyRows := make([][]any, len(impacts))
	for i, imp := range impacts {
		copyRows[i] = []any{
			imp.TransitionID,
			imp.EventType,
			imp.TransitionedAttraction,
			imp.ImpactedAttraction,
			imp.ParkID,
			imp.TransitionTime,
			imp.VelocityBefore,
			imp.VelocityAfter,
			imp.VelocityDelta,
			imp.BaselineDelta,
			imp.ExcessDelta,
			imp.WaitBefore,
			imp.WaitAfter,
			imp.WaitDelta,
			imp.AbsorptionScore,
			imp.DrainScore,
		}
	}

	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"l3_transition_impact"},
		[]string{
			"transition_id", "event_type", "transitioned_attraction", "impacted_attraction",
			"park_id", "transition_time",
			"velocity_before", "velocity_after", "velocity_delta",
			"baseline_delta", "excess_delta",
			"wait_before", "wait_after", "wait_delta",
			"absorption_score", "drain_score",
		},
		pgx.CopyFromRows(copyRows),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk inserting transition impacts: %w", err)
	}
	return int(n), nil
}

// GetParkIDsWithVelocityData returns distinct park IDs that have velocity data
// in the given time window.
func (s *Store) GetParkIDsWithVelocityData(ctx context.Context, since, until time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT a.park_id
		FROM l2_wait_velocity v
		JOIN attractions a ON a.id = v.attraction_id
		WHERE v.timestamp > $1
		  AND v.timestamp <= $2
		  AND v.velocity_ema IS NOT NULL
	`, since, until)
	if err != nil {
		return nil, fmt.Errorf("querying parks with velocity data: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning park ID: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
