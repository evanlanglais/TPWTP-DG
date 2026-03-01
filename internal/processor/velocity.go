package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

const (
	// EMA alpha for ~30-minute window with 5-min intervals (6 samples).
	// alpha = 2 / (N+1) where N=6 → ~0.286
	emaAlpha = 0.286

	// Velocity threshold: |delta| <= stableThreshold is STABLE.
	stableThreshold = 2.0
)

// VelocityMetric computes wait time velocity (rate of change) and trend
// direction for each attraction per poll interval.
type VelocityMetric struct {
	store *store.Store
}

// NewVelocityMetric creates a new VelocityMetric.
func NewVelocityMetric(s *store.Store) *VelocityMetric {
	return &VelocityMetric{store: s}
}

func (v *VelocityMetric) Name() string           { return "velocity" }
func (v *VelocityMetric) Version() int            { return 1 }
func (v *VelocityMetric) OutputTables() []string   { return []string{"l2_wait_velocity"} }

func (v *VelocityMetric) Process(ctx context.Context, since, now time.Time) error {
	deltas, err := v.store.GetRecentWaitTimeDeltas(ctx, since, now)
	if err != nil {
		return err
	}

	if len(deltas) == 0 {
		return nil
	}

	// Group deltas by attraction to compute EMA across the window
	type attractionState struct {
		ema *float32
	}
	emaState := make(map[string]*attractionState)

	var velocities []store.WaitVelocity

	for _, d := range deltas {
		// Skip non-operating attractions and rows without a computable delta.
		// Closed/down status is already captured by l2_status_transitions,
		// and first-observations (no prior data point) carry no velocity info.
		if d.Status != "OPERATING" || d.Delta == nil {
			continue
		}

		st, ok := emaState[d.AttractionID]
		if !ok {
			st = &attractionState{}
			emaState[d.AttractionID] = st
		}

		delta := float32(*d.Delta)

		// Update EMA
		if st.ema == nil {
			st.ema = &delta
		} else {
			ema := float32(emaAlpha)*delta + float32(1-emaAlpha)*(*st.ema)
			st.ema = &ema
		}

		// Classify trend based on EMA
		var trend string
		switch {
		case *st.ema > stableThreshold:
			trend = "RISING"
		case *st.ema < -stableThreshold:
			trend = "FALLING"
		default:
			trend = "STABLE"
		}

		velocities = append(velocities, store.WaitVelocity{
			AttractionID: d.AttractionID,
			Timestamp:    d.Timestamp,
			Velocity:     &delta,
			VelocityEMA:  st.ema,
			Trend:        trend,
			SampleCount:  d.SampleCount,
		})
	}

	inserted, err := v.store.InsertWaitVelocities(ctx, velocities)
	if err != nil {
		return err
	}

	slog.Debug("velocity metric processed",
		"records", inserted,
		"window", now.Sub(since).Round(time.Second),
	)
	return nil
}
