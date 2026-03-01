package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// CrowdPressureMetric computes a park-level crowd pressure score by
// aggregating per-attraction velocity EMA weighted by wait time.
type CrowdPressureMetric struct {
	store    *store.Store
	emaState map[string]*float32 // parkID → last pressure EMA
}

// NewCrowdPressureMetric creates a new CrowdPressureMetric.
func NewCrowdPressureMetric(s *store.Store) *CrowdPressureMetric {
	return &CrowdPressureMetric{
		store:    s,
		emaState: make(map[string]*float32),
	}
}

func (c *CrowdPressureMetric) Name() string           { return "crowd_pressure" }
func (c *CrowdPressureMetric) Version() int            { return 3 }
func (c *CrowdPressureMetric) OutputTables() []string   { return []string{"l3_park_crowd_pressure"} }

func (c *CrowdPressureMetric) Process(ctx context.Context, since, now time.Time) error {
	snapshots, err := c.store.GetParkVelocitySnapshots(ctx, since, now)
	if err != nil {
		return err
	}

	if len(snapshots) == 0 {
		return nil
	}

	// Seed EMA state from DB for parks we haven't seen this process lifetime
	hasMissing := false
	for _, s := range snapshots {
		if _, ok := c.emaState[s.ParkID]; !ok {
			hasMissing = true
			break
		}
	}
	if hasMissing {
		seeds, err := c.store.GetLatestPressureEMAs(ctx, since)
		if err != nil {
			return err
		}
		for id, ema := range seeds {
			if _, ok := c.emaState[id]; !ok {
				e := ema
				c.emaState[id] = &e
			}
		}
	}

	// Group snapshots by (parkID, timestamp)
	type groupKey struct {
		parkID    string
		timestamp time.Time
	}
	type groupData struct {
		weightedSum float64
		weightSum   float64
		waitSum     float64
		count       int16
	}
	groups := make(map[groupKey]*groupData)
	// Maintain insertion order
	var keys []groupKey

	for _, s := range snapshots {
		k := groupKey{parkID: s.ParkID, timestamp: s.Timestamp}
		g, ok := groups[k]
		if !ok {
			g = &groupData{}
			groups[k] = g
			keys = append(keys, k)
		}

		weight := float64(s.StandbyWait)
		if weight < 1 {
			weight = 1
		}
		g.weightedSum += float64(s.VelocityEMA) * weight
		g.weightSum += weight
		g.waitSum += float64(s.StandbyWait)
		g.count++
	}

	var pressures []store.ParkCrowdPressure

	for _, k := range keys {
		g := groups[k]

		pressureScore := float32(g.weightedSum / g.weightSum)

		// Update EMA
		prev, hasPrev := c.emaState[k.parkID]
		var pressureEMA float32
		if !hasPrev || prev == nil {
			pressureEMA = pressureScore
		} else {
			pressureEMA = float32(emaAlpha)*pressureScore + float32(1-emaAlpha)*(*prev)
		}
		ema := pressureEMA
		c.emaState[k.parkID] = &ema

		// Classify trend
		var trend string
		switch {
		case pressureEMA > stableThreshold:
			trend = "RISING"
		case pressureEMA < -stableThreshold:
			trend = "FALLING"
		default:
			trend = "STABLE"
		}

		avgWait := float32(g.waitSum / float64(g.count))

		pressures = append(pressures, store.ParkCrowdPressure{
			ParkID:         k.parkID,
			Timestamp:      k.timestamp,
			PressureScore:  pressureScore,
			PressureEMA:    pressureEMA,
			Trend:          trend,
			OperatingCount: g.count,
			AvgWait:        avgWait,
		})
	}

	inserted, err := c.store.InsertParkCrowdPressures(ctx, pressures)
	if err != nil {
		return err
	}

	slog.Debug("crowd pressure metric processed",
		"records", inserted,
		"window", now.Sub(since).Round(time.Second),
	)
	return nil
}
