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

func (v *VelocityMetric) Name() string { return "velocity" }

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
		st, ok := emaState[d.AttractionID]
		if !ok {
			st = &attractionState{}
			emaState[d.AttractionID] = st
		}

		vel := store.WaitVelocity{
			AttractionID: d.AttractionID,
			Timestamp:    d.Timestamp,
			SampleCount:  d.SampleCount,
		}

		// Classify trend
		switch {
		case d.Status != "OPERATING":
			vel.Trend = "CLOSED"
		case d.Delta != nil:
			delta := float32(*d.Delta)
			vel.Velocity = &delta

			// Update EMA
			if st.ema == nil {
				st.ema = &delta
			} else {
				ema := float32(emaAlpha)*delta + float32(1-emaAlpha)*(*st.ema)
				st.ema = &ema
			}
			vel.VelocityEMA = st.ema

			// Classify based on EMA
			switch {
			case *st.ema > stableThreshold:
				vel.Trend = "RISING"
			case *st.ema < -stableThreshold:
				vel.Trend = "FALLING"
			default:
				vel.Trend = "STABLE"
			}
		default:
			vel.Trend = "UNKNOWN"
		}

		velocities = append(velocities, vel)
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
