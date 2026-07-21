package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// RawObservation is a verbatim source response, persisted before projection
// (invariant I1). Payload/Metadata are json.RawMessage so pgx's JSON codec
// targets the jsonb columns (a bare []byte would be treated as bytea).
type RawObservation struct {
	ObservationID string
	SourceType    string
	ObservedAt    time.Time
	ReceivedAt    time.Time
	Payload       json.RawMessage // verbatim body; nil on failed fetch → SQL NULL
	Metadata      json.RawMessage
}

// InsertRawObservation writes a single raw observation, ensuring its monthly
// partition exists first. Called once per park per poll from the collector.
func (s *Store) InsertRawObservation(ctx context.Context, obs RawObservation) error {
	if err := s.ensureRawObservationPartition(ctx, obs.ReceivedAt); err != nil {
		return err
	}

	// Pass nil (not an empty slice) for absent JSON so the column is NULL.
	var payload, metadata any
	if len(obs.Payload) > 0 {
		payload = obs.Payload
	}
	if len(obs.Metadata) > 0 {
		metadata = obs.Metadata
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO raw_observations
			(observation_id, source_type, observed_at, received_at, payload, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, obs.ObservationID, obs.SourceType, obs.ObservedAt, obs.ReceivedAt, payload, metadata)
	if err != nil {
		return fmt.Errorf("inserting raw observation: %w", err)
	}
	return nil
}

// InsertRawObservationBatch inserts many raw observations idempotently
// (ON CONFLICT DO NOTHING), ensuring partitions first. For single-threaded
// backfill use — CopyFrom cannot express ON CONFLICT and aborts on any PK clash.
// Returns the number of rows actually inserted (conflicts count as 0).
func (s *Store) InsertRawObservationBatch(ctx context.Context, obs []RawObservation) (int, error) {
	if len(obs) == 0 {
		return 0, nil
	}
	for _, o := range obs {
		if err := s.ensureRawObservationPartition(ctx, o.ReceivedAt); err != nil {
			return 0, err
		}
	}

	batch := &pgx.Batch{}
	for _, o := range obs {
		var payload, metadata any
		if len(o.Payload) > 0 {
			payload = o.Payload
		}
		if len(o.Metadata) > 0 {
			metadata = o.Metadata
		}
		batch.Queue(`
			INSERT INTO raw_observations
				(observation_id, source_type, observed_at, received_at, payload, metadata)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (observation_id, received_at) DO NOTHING
		`, o.ObservationID, o.SourceType, o.ObservedAt, o.ReceivedAt, payload, metadata)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	inserted := 0
	for range obs {
		ct, err := br.Exec()
		if err != nil {
			return inserted, fmt.Errorf("batch inserting raw observation: %w", err)
		}
		inserted += int(ct.RowsAffected())
	}
	return inserted, nil
}

// ensureRawObservationPartition creates a monthly partition (keyed on received_at)
// if it doesn't already exist. Mirrors ensurePartition in postgres.go, using the
// "ro_" cache-key prefix to avoid colliding with other tables' partition keys.
func (s *Store) ensureRawObservationPartition(ctx context.Context, t time.Time) error {
	year, month, _ := t.UTC().Date()
	key := fmt.Sprintf("ro_%d_%02d", year, month)

	if _, ok := s.partitions.Load(key); ok {
		return nil
	}

	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partName := fmt.Sprintf("raw_observations_%d_%02d", year, month)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF raw_observations
		FOR VALUES FROM ('%s') TO ('%s')
	`, partName, start.Format(time.DateOnly), end.Format(time.DateOnly))

	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating partition %s: %w", partName, err)
	}
	slog.Info("ensured partition", "name", partName)
	s.partitions.Store(key, true)
	return nil
}
