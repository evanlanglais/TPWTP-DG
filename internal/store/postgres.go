package store

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Destination is the DB representation of a theme park destination.
type Destination struct {
	ID   string
	Name string
	Slug string
}

// Park is the DB representation of a theme park.
type Park struct {
	ID            string
	DestinationID string
	Name          string
	Timezone      string
}

// Attraction is the DB representation of a ride/attraction.
type Attraction struct {
	ID         string
	ParkID     string
	Name       string
	ExternalID string
}

// WaitTime is a single wait time observation.
type WaitTime struct {
	AttractionID    string
	Timestamp       time.Time
	Status          string
	StandbyWait     *int
	SingleRiderWait *int
	ReturnTimeState *string
	ReturnTimeStart *time.Time
	ReturnTimeEnd   *time.Time
	// SourceObservationID links this projected row back to the raw_observations
	// row it came from (invariant I1). Nil for rows written before lineage existed.
	SourceObservationID *string
}

// Store wraps a pgx connection pool and provides data access methods.
type Store struct {
	pool       *pgxpool.Pool
	partitions sync.Map // tracks which monthly partitions have been created
}

// New creates a new Store with the given connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// UpsertDestinations inserts or updates destination records.
func (s *Store) UpsertDestinations(ctx context.Context, dests []Destination) error {
	batch := &pgx.Batch{}
	for _, d := range dests {
		batch.Queue(`
			INSERT INTO destinations (id, name, slug)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, slug = EXCLUDED.slug
		`, d.ID, d.Name, d.Slug)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range dests {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upserting destination: %w", err)
		}
	}
	return nil
}

// UpsertParks inserts or updates park records.
func (s *Store) UpsertParks(ctx context.Context, parks []Park) error {
	batch := &pgx.Batch{}
	for _, p := range parks {
		batch.Queue(`
			INSERT INTO parks (id, destination_id, name, timezone)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE SET
				destination_id = EXCLUDED.destination_id,
				name = EXCLUDED.name,
				timezone = EXCLUDED.timezone
		`, p.ID, p.DestinationID, p.Name, p.Timezone)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range parks {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upserting park: %w", err)
		}
	}
	return nil
}

// UpsertAttractions inserts or updates attraction records.
func (s *Store) UpsertAttractions(ctx context.Context, attrs []Attraction) error {
	if len(attrs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, a := range attrs {
		batch.Queue(`
			INSERT INTO attractions (id, park_id, name, external_id)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO UPDATE SET
				park_id = EXCLUDED.park_id,
				name = EXCLUDED.name,
				external_id = EXCLUDED.external_id
		`, a.ID, a.ParkID, a.Name, a.ExternalID)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range attrs {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upserting attraction: %w", err)
		}
	}
	return nil
}

// InsertWaitTimes bulk-inserts wait time observations.
// It ensures the required monthly partition exists before inserting.
func (s *Store) InsertWaitTimes(ctx context.Context, wts []WaitTime) (int, error) {
	if len(wts) == 0 {
		return 0, nil
	}

	// Ensure partitions exist for all timestamps in this batch
	for _, wt := range wts {
		if err := s.ensurePartition(ctx, wt.Timestamp); err != nil {
			return 0, err
		}
	}

	// Use COPY for bulk insert
	rows := make([][]any, len(wts))
	for i, wt := range wts {
		rows[i] = []any{
			wt.AttractionID,
			wt.Timestamp,
			wt.Status,
			wt.StandbyWait,
			wt.SingleRiderWait,
			wt.ReturnTimeState,
			wt.ReturnTimeStart,
			wt.ReturnTimeEnd,
			wt.SourceObservationID,
		}
	}

	copyCount, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"wait_times"},
		[]string{
			"attraction_id", "timestamp", "status",
			"standby_wait", "single_rider_wait",
			"return_time_state", "return_time_start", "return_time_end",
			"source_observation_id",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("bulk inserting wait times: %w", err)
	}
	return int(copyCount), nil
}

// ensurePartition creates a monthly partition if it doesn't already exist.
func (s *Store) ensurePartition(ctx context.Context, t time.Time) error {
	year, month, _ := t.Date()
	key := fmt.Sprintf("%d_%02d", year, month)

	if _, ok := s.partitions.Load(key); ok {
		return nil
	}

	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partName := fmt.Sprintf("wait_times_%s", key)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF wait_times
		FOR VALUES FROM ('%s') TO ('%s')
	`, partName, start.Format(time.DateOnly), end.Format(time.DateOnly))

	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating partition %s: %w", partName, err)
	}
	slog.Info("ensured partition", "name", partName)
	s.partitions.Store(key, true)
	return nil
}

// UpdateParkTimezone updates the timezone for a single park.
func (s *Store) UpdateParkTimezone(ctx context.Context, parkID, timezone string) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE parks SET timezone = $1 WHERE id = $2", timezone, parkID)
	if err != nil {
		return fmt.Errorf("updating park timezone: %w", err)
	}
	return nil
}

// GetParkIDs returns all park IDs in the database.
func (s *Store) GetParkIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, "SELECT id FROM parks")
	if err != nil {
		return nil, fmt.Errorf("querying park IDs: %w", err)
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

// PruneUntrackedParks removes all data for parks not in the given set of destination IDs.
// Deletes cascade: wait_times → attractions → parks → orphaned destinations.
func (s *Store) PruneUntrackedParks(ctx context.Context, trackedDestIDs []string) error {
	if len(trackedDestIDs) == 0 {
		return nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning prune transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Delete wait_times for attractions in untracked parks
	_, err = tx.Exec(ctx, `
		DELETE FROM wait_times
		WHERE attraction_id IN (
			SELECT a.id FROM attractions a
			JOIN parks p ON a.park_id = p.id
			WHERE p.destination_id != ALL($1::uuid[])
		)
	`, trackedDestIDs)
	if err != nil {
		return fmt.Errorf("pruning wait_times: %w", err)
	}

	// Delete attractions in untracked parks
	_, err = tx.Exec(ctx, `
		DELETE FROM attractions
		WHERE park_id IN (
			SELECT id FROM parks WHERE destination_id != ALL($1::uuid[])
		)
	`, trackedDestIDs)
	if err != nil {
		return fmt.Errorf("pruning attractions: %w", err)
	}

	// Delete untracked parks
	_, err = tx.Exec(ctx, `
		DELETE FROM parks WHERE destination_id != ALL($1::uuid[])
	`, trackedDestIDs)
	if err != nil {
		return fmt.Errorf("pruning parks: %w", err)
	}

	// Delete untracked destinations
	_, err = tx.Exec(ctx, `
		DELETE FROM destinations WHERE id != ALL($1::uuid[])
	`, trackedDestIDs)
	if err != nil {
		return fmt.Errorf("pruning destinations: %w", err)
	}

	return tx.Commit(ctx)
}

// GetKnownAttractionIDs returns a set of all attraction IDs in the database for a given park.
func (s *Store) GetKnownAttractionIDs(ctx context.Context, parkID string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT id FROM attractions WHERE park_id = $1", parkID)
	if err != nil {
		return nil, fmt.Errorf("querying attraction IDs: %w", err)
	}
	defer rows.Close()

	ids := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scanning attraction ID: %w", err)
		}
		ids[id] = true
	}
	return ids, rows.Err()
}

// ReadWaitTimesWindow returns raw wait_times rows in (since, until], ordered by
// timestamp. Read-only; used by the legacy backfill to adopt historical rows into
// raw_observations without modifying the originals.
func (s *Store) ReadWaitTimesWindow(ctx context.Context, since, until time.Time) ([]WaitTime, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT attraction_id, timestamp, status, standby_wait, single_rider_wait,
		       return_time_state, return_time_start, return_time_end
		FROM wait_times
		WHERE timestamp > $1 AND timestamp <= $2
		ORDER BY timestamp
	`, since, until)
	if err != nil {
		return nil, fmt.Errorf("querying wait_times window: %w", err)
	}
	defer rows.Close()

	var out []WaitTime
	for rows.Next() {
		var wt WaitTime
		if err := rows.Scan(&wt.AttractionID, &wt.Timestamp, &wt.Status,
			&wt.StandbyWait, &wt.SingleRiderWait,
			&wt.ReturnTimeState, &wt.ReturnTimeStart, &wt.ReturnTimeEnd); err != nil {
			return nil, fmt.Errorf("scanning wait_time: %w", err)
		}
		out = append(out, wt)
	}
	return out, rows.Err()
}
