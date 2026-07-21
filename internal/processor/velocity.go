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
	store    *store.Store
	emaState map[string]*float32 // attractionID → last EMA value
}

// NewVelocityMetric creates a new VelocityMetric.
func NewVelocityMetric(s *store.Store) *VelocityMetric {
	return &VelocityMetric{
		store:    s,
		emaState: make(map[string]*float32),
	}
}

func (v *VelocityMetric) Name() string           { return "velocity" }
func (v *VelocityMetric) Version() int            { return 3 }
func (v *VelocityMetric) OutputTables() []string   { return []string{"l2_wait_velocity"} }

// Temporal: velocity[t] = wait[t] - wait[t-1] and its EMA use only data <= t.
func (v *VelocityMetric) Temporal() TemporalContract { return TemporalContract{PointInTime: true} }

func (v *VelocityMetric) Process(ctx context.Context, since, now time.Time) error {
	deltas, err := v.store.GetRecentWaitTimeDeltas(ctx, since, now)
	if err != nil {
		return err
	}

	if len(deltas) == 0 {
		return nil
	}

	// Seed EMA state from DB for attractions we haven't seen this process lifetime
	missing := make(map[string]bool)
	for _, d := range deltas {
		if _, ok := v.emaState[d.AttractionID]; !ok {
			missing[d.AttractionID] = true
		}
	}
	if len(missing) > 0 {
		ids := make([]string, 0, len(missing))
		for id := range missing {
			ids = append(ids, id)
		}
		seeds, err := v.store.GetLatestVelocityEMAs(ctx, ids, since)
		if err != nil {
			return err
		}
		for id, ema := range seeds {
			e := ema
			v.emaState[id] = &e
		}
	}

	var velocities []store.WaitVelocity

	for _, d := range deltas {
		// Skip non-operating attractions and rows without a computable delta.
		// Closed/down status is already captured by l2_status_transitions,
		// and first-observations (no prior data point) carry no velocity info.
		if d.Status != "OPERATING" || d.Delta == nil {
			continue
		}

		delta := float32(*d.Delta)

		// Update EMA, carrying state across Process() calls
		prev := v.emaState[d.AttractionID]
		var ema float32
		if prev == nil {
			ema = delta
		} else {
			ema = float32(emaAlpha)*delta + float32(1-emaAlpha)*(*prev)
		}
		v.emaState[d.AttractionID] = &ema

		// Classify trend based on EMA
		var trend string
		switch {
		case ema > stableThreshold:
			trend = "RISING"
		case ema < -stableThreshold:
			trend = "FALLING"
		default:
			trend = "STABLE"
		}

		velocities = append(velocities, store.WaitVelocity{
			AttractionID: d.AttractionID,
			Timestamp:    d.Timestamp,
			Velocity:     &delta,
			VelocityEMA:  &ema,
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
