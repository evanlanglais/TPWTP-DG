package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// TransitionsMetric detects when rides change operating status (e.g.,
// OPERATING → DOWN) and records the transitions.
type TransitionsMetric struct {
	store *store.Store
}

// NewTransitionsMetric creates a new TransitionsMetric.
func NewTransitionsMetric(s *store.Store) *TransitionsMetric {
	return &TransitionsMetric{store: s}
}

func (t *TransitionsMetric) Name() string           { return "transitions" }
func (t *TransitionsMetric) Version() int            { return 1 }
func (t *TransitionsMetric) OutputTables() []string   { return []string{"l2_status_transitions"} }

// Temporal: each transition is stamped at its own time and derived from a
// strictly-earlier predecessor row, so it uses only data <= t.
func (t *TransitionsMetric) Temporal() TemporalContract { return TemporalContract{PointInTime: true} }

func (t *TransitionsMetric) Process(ctx context.Context, since, now time.Time) error {
	n, err := t.store.DetectAndInsertStatusTransitions(ctx, since, now)
	if err != nil {
		return err
	}

	if n > 0 {
		slog.Debug("status transitions detected", "count", n)
	}
	return nil
}
