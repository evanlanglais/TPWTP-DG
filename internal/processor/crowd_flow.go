package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

const (
	// crowdFlowLag is the delay before processing closures, to ensure
	// post-closure data exists for impact measurement.
	crowdFlowLag = 15 * time.Minute

	// snapshotTolerance is the time window around a target timestamp
	// when looking for velocity/wait snapshots.
	snapshotTolerance = 3 * time.Minute
)

// CrowdFlowMetric measures how ride closures redistribute crowds by
// comparing velocity and wait times before and after each closure event.
type CrowdFlowMetric struct {
	store *store.Store
}

// NewCrowdFlowMetric creates a new CrowdFlowMetric.
func NewCrowdFlowMetric(s *store.Store) *CrowdFlowMetric {
	return &CrowdFlowMetric{store: s}
}

func (f *CrowdFlowMetric) Name() string { return "crowd_flow" }

func (f *CrowdFlowMetric) Process(ctx context.Context, since, now time.Time) error {
	// Apply lag: only process closures where we have 15 min of post-closure data
	laggedEnd := now.Add(-crowdFlowLag)
	if !laggedEnd.After(since) {
		return nil // not enough elapsed time yet
	}

	closures, err := f.store.GetRecentClosures(ctx, since, laggedEnd)
	if err != nil {
		return err
	}

	if len(closures) == 0 {
		return nil
	}

	var allImpacts []store.ClosureImpact

	for _, closure := range closures {
		// Get snapshots at closure time and 15 min after
		before, err := f.store.GetVelocityAndWaitAtTime(
			ctx, closure.ParkID, closure.TransitionTime, snapshotTolerance,
		)
		if err != nil {
			return err
		}

		after, err := f.store.GetVelocityAndWaitAtTime(
			ctx, closure.ParkID, closure.TransitionTime.Add(crowdFlowLag), snapshotTolerance,
		)
		if err != nil {
			return err
		}

		impacts := f.computeImpacts(closure, before, after)
		allImpacts = append(allImpacts, impacts...)
	}

	inserted, err := f.store.InsertClosureImpacts(ctx, allImpacts)
	if err != nil {
		return err
	}

	if inserted > 0 {
		slog.Debug("crowd flow metric processed",
			"closures", len(closures),
			"impacts", inserted,
		)
	}
	return nil
}

// computeImpacts calculates velocity and wait deltas for each ride affected
// by a closure, and assigns absorption scores.
func (f *CrowdFlowMetric) computeImpacts(
	closure store.StatusTransitionRecord,
	before, after []store.AttractionSnapshot,
) []store.ClosureImpact {
	// Index "before" snapshots by attraction ID
	beforeMap := make(map[string]store.AttractionSnapshot, len(before))
	for _, s := range before {
		beforeMap[s.AttractionID] = s
	}

	// Index "after" snapshots by attraction ID
	afterMap := make(map[string]store.AttractionSnapshot, len(after))
	for _, s := range after {
		afterMap[s.AttractionID] = s
	}

	// Compute deltas for attractions present in both snapshots
	type impactData struct {
		attrID        string
		velBefore     float32
		velAfter      float32
		velDelta      float32
		waitBefore    int
		waitAfter     int
		waitDelta     int
		posVelDelta   float32 // max(velDelta, 0) for absorption score
	}

	var impacts []impactData
	var totalPosVelDelta float32

	for attrID, b := range beforeMap {
		// Skip the closed attraction itself
		if attrID == closure.AttractionID {
			continue
		}

		a, ok := afterMap[attrID]
		if !ok {
			continue
		}

		velDelta := a.VelocityEMA - b.VelocityEMA
		waitDelta := a.StandbyWait - b.StandbyWait

		posVel := velDelta
		if posVel < 0 {
			posVel = 0
		}
		totalPosVelDelta += posVel

		impacts = append(impacts, impactData{
			attrID:      attrID,
			velBefore:   b.VelocityEMA,
			velAfter:    a.VelocityEMA,
			velDelta:    velDelta,
			waitBefore:  b.StandbyWait,
			waitAfter:   a.StandbyWait,
			waitDelta:   waitDelta,
			posVelDelta: posVel,
		})
	}

	if len(impacts) == 0 {
		return nil
	}

	result := make([]store.ClosureImpact, len(impacts))
	for i, imp := range impacts {
		velBefore := imp.velBefore
		velAfter := imp.velAfter
		velDelta := imp.velDelta
		waitBefore := imp.waitBefore
		waitAfter := imp.waitAfter
		waitDelta := imp.waitDelta

		var absorptionScore *float32
		if totalPosVelDelta > 0 {
			score := imp.posVelDelta / totalPosVelDelta
			absorptionScore = &score
		}

		result[i] = store.ClosureImpact{
			TransitionID:       closure.ID,
			ClosedAttraction:   closure.AttractionID,
			ImpactedAttraction: imp.attrID,
			ParkID:             closure.ParkID,
			ClosureTime:        closure.TransitionTime,
			VelocityBefore:     &velBefore,
			VelocityAfter:      &velAfter,
			VelocityDelta:      &velDelta,
			WaitBefore:         &waitBefore,
			WaitAfter:          &waitAfter,
			WaitDelta:          &waitDelta,
			AbsorptionScore:    absorptionScore,
		}
	}

	return result
}
