package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// Metric is the interface that all Layer 2 metrics must implement.
type Metric interface {
	// Name returns the unique identifier for this metric (used as cursor key).
	Name() string
	// Process computes the metric for data between since and now.
	Process(ctx context.Context, since time.Time, now time.Time) error
}

// materializedViews lists the views to refresh periodically.
var materializedViews = []string{
	"l2_daily_attraction_stats",
	"l2_daily_park_stats",
}

// Processor orchestrates Layer 2 metric computation after each poll cycle.
type Processor struct {
	store       *store.Store
	metrics     []Metric
	lastRefresh time.Time
}

// New creates a Processor with the standard set of metrics.
func New(s *store.Store) *Processor {
	p := &Processor{store: s}
	p.metrics = []Metric{
		NewVelocityMetric(s),
		NewTransitionsMetric(s),
	}
	return p
}

// RunAfterPoll processes all metrics incrementally, then refreshes
// materialized views hourly.
func (p *Processor) RunAfterPoll(ctx context.Context, pollTimestamp time.Time) {
	start := time.Now()

	for _, m := range p.metrics {
		if err := p.processMetric(ctx, m, pollTimestamp); err != nil {
			slog.Warn("layer 2 metric failed",
				"metric", m.Name(),
				"error", err,
			)
		}
	}

	// Refresh materialized views hourly
	if time.Since(p.lastRefresh) >= time.Hour {
		p.refreshViews(ctx)
		p.lastRefresh = time.Now()
	}

	slog.Info("layer 2 processing complete",
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// backfillChunkSize is the time window processed per iteration during backfill.
const backfillChunkSize = 1 * time.Hour

// processMetric reads the cursor, runs the metric, and updates the cursor.
// When no cursor exists, it backfills from the earliest wait_time in 1-hour chunks.
func (p *Processor) processMetric(ctx context.Context, m Metric, now time.Time) error {
	since, err := p.store.GetProcessingCursor(ctx, m.Name())
	if err != nil {
		return err
	}

	if since.IsZero() {
		earliest, err := p.store.GetEarliestWaitTime(ctx)
		if err != nil {
			return err
		}
		if earliest.IsZero() {
			return nil // no data yet
		}
		since = earliest.Add(-1 * time.Minute)
		slog.Info("backfilling layer 2 metric",
			"metric", m.Name(),
			"from", since.Format(time.RFC3339),
		)
	}

	// Process in chunks to avoid overwhelming queries on large backlogs
	for since.Before(now) {
		chunkEnd := since.Add(backfillChunkSize)
		if chunkEnd.After(now) {
			chunkEnd = now
		}

		if err := m.Process(ctx, since, chunkEnd); err != nil {
			return err
		}

		since = chunkEnd
		// Save cursor after each chunk so progress isn't lost on failure
		if err := p.store.SetProcessingCursor(ctx, m.Name(), since); err != nil {
			return err
		}
	}

	return nil
}

// refreshViews refreshes all Layer 2 materialized views.
func (p *Processor) refreshViews(ctx context.Context) {
	for _, view := range materializedViews {
		start := time.Now()
		if err := p.store.RefreshMaterializedView(ctx, view); err != nil {
			slog.Error("failed to refresh materialized view",
				"view", view,
				"error", err,
			)
			continue
		}
		slog.Info("refreshed materialized view",
			"view", view,
			"duration", time.Since(start).Round(time.Millisecond),
		)
	}
}
