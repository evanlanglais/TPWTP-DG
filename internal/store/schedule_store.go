package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ParkScheduleEntry is the DB representation of a park schedule window.
type ParkScheduleEntry struct {
	ParkID      string
	Date        time.Time
	Type        string
	OpeningTime time.Time
	ClosingTime time.Time
}

// ParkOperatingStatus represents a park's current operating state.
type ParkOperatingStatus struct {
	ParkID       string
	IsOpen       bool
	ScheduleType string
	OpeningTime  time.Time
	ClosingTime  time.Time
}

// UpsertParkSchedules replaces all schedule entries for a park with the given entries.
func (s *Store) UpsertParkSchedules(ctx context.Context, parkID string, entries []ParkScheduleEntry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning schedule upsert transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, "DELETE FROM park_schedules WHERE park_id = $1", parkID); err != nil {
		return fmt.Errorf("deleting existing schedules for park %s: %w", parkID, err)
	}

	if len(entries) == 0 {
		return tx.Commit(ctx)
	}

	rows := make([][]any, len(entries))
	for i, e := range entries {
		rows[i] = []any{e.ParkID, e.Date, e.Type, e.OpeningTime, e.ClosingTime}
	}

	_, err = tx.CopyFrom(ctx,
		pgx.Identifier{"park_schedules"},
		[]string{"park_id", "date", "type", "opening_time", "closing_time"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("inserting schedules for park %s: %w", parkID, err)
	}

	return tx.Commit(ctx)
}

// GetParkScheduleWindows returns all schedule entries for a park.
func (s *Store) GetParkScheduleWindows(ctx context.Context, parkID string) ([]ParkScheduleEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT park_id, date, type, opening_time, closing_time
		FROM park_schedules
		WHERE park_id = $1
		ORDER BY opening_time
	`, parkID)
	if err != nil {
		return nil, fmt.Errorf("querying schedule windows for park %s: %w", parkID, err)
	}
	defer rows.Close()

	var entries []ParkScheduleEntry
	for rows.Next() {
		var e ParkScheduleEntry
		if err := rows.Scan(&e.ParkID, &e.Date, &e.Type, &e.OpeningTime, &e.ClosingTime); err != nil {
			return nil, fmt.Errorf("scanning schedule entry: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// GetCurrentParkStatuses returns the current operating status for all parks at the given time.
// Parks with no matching schedule window are not included in the result (treat as closed).
func (s *Store) GetCurrentParkStatuses(ctx context.Context, t time.Time) (map[string]ParkOperatingStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (ps.park_id)
			ps.park_id, ps.type, ps.opening_time, ps.closing_time
		FROM park_schedules ps
		WHERE ps.opening_time <= $1 AND ps.closing_time > $1
			AND ps.type IN ('OPERATING', 'TICKETED_EVENT', 'EXTRA_HOURS')
		ORDER BY ps.park_id, ps.opening_time
	`, t)
	if err != nil {
		return nil, fmt.Errorf("querying current park statuses: %w", err)
	}
	defer rows.Close()

	statuses := make(map[string]ParkOperatingStatus)
	for rows.Next() {
		var s ParkOperatingStatus
		if err := rows.Scan(&s.ParkID, &s.ScheduleType, &s.OpeningTime, &s.ClosingTime); err != nil {
			return nil, fmt.Errorf("scanning park status: %w", err)
		}
		s.IsOpen = true
		statuses[s.ParkID] = s
	}
	return statuses, rows.Err()
}
