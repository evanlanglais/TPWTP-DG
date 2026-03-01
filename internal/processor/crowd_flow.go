package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

const (
	// crowdFlowLag is the delay before processing transitions, to ensure
	// post-transition data exists for impact measurement.
	crowdFlowLag = 30 * time.Minute

	// snapshotTolerance is the time window around a target timestamp
	// when looking for velocity/wait snapshots.
	snapshotTolerance = 3 * time.Minute
)

// CrowdFlowMetric measures how ride closures and openings redistribute crowds
// by comparing velocity and wait times before and after each transition event.
//
// Three snapshots are taken per event:
//   - event - 30min (pre-baseline)
//   - event time (before)
//   - event + 30min (after)
//
// baseline_delta = vel_at_event - vel_at_30min_before (pre-existing trend)
// velocity_delta = vel_at_30min_after - vel_at_event (observed change)
// excess_delta = velocity_delta - baseline_delta (attributable to event)
//
// Only impacts where excess_delta is in the expected direction are stored:
//   - Closures: excess_delta > 0 → absorption_score
//   - Openings: excess_delta < 0 → drain_score
//
// Backfill note: The transition query window is shifted back by crowdFlowLag
// so that cursor advancement (which happens in 1-hour chunks) doesn't create
// gaps. We query transitions in (since-lag, now-lag] — the cursor advances
// to now, and the next chunk starts at (now-lag, ...) which is contiguous.
type CrowdFlowMetric struct {
	store *store.Store
}

// NewCrowdFlowMetric creates a new CrowdFlowMetric.
func NewCrowdFlowMetric(s *store.Store) *CrowdFlowMetric {
	return &CrowdFlowMetric{store: s}
}

func (f *CrowdFlowMetric) Name() string           { return "crowd_flow" }
func (f *CrowdFlowMetric) Version() int            { return 3 }
func (f *CrowdFlowMetric) OutputTables() []string   { return []string{"l3_transition_impact"} }

func (f *CrowdFlowMetric) Process(ctx context.Context, since, now time.Time) error {
	// Shift the query window back by the lag so cursor advancement doesn't
	// create gaps. The cursor advances to `now`, so the next chunk will query
	// from (now - lag, ...) — contiguous with this chunk's (since - lag, now - lag].
	queryStart := since.Add(-crowdFlowLag)
	queryEnd := now.Add(-crowdFlowLag)

	// Process closures
	closureCount, closureImpacts, err := f.processEvents(ctx, queryStart, queryEnd, "CLOSURE")
	if err != nil {
		return err
	}

	// Process openings
	openingCount, openingImpacts, err := f.processEvents(ctx, queryStart, queryEnd, "OPENING")
	if err != nil {
		return err
	}

	// Batch insert all impacts together
	allImpacts := append(closureImpacts, openingImpacts...)
	inserted, err := f.store.InsertTransitionImpacts(ctx, allImpacts)
	if err != nil {
		return err
	}

	if inserted > 0 {
		slog.Debug("crowd flow metric processed",
			"closures", closureCount,
			"openings", openingCount,
			"impacts", inserted,
		)
	}
	return nil
}

// processEvents handles transitions of the given event type.
func (f *CrowdFlowMetric) processEvents(ctx context.Context, since, laggedEnd time.Time, eventType string) (int, []store.TransitionImpact, error) {
	var transitions []store.StatusTransitionRecord
	var err error

	if eventType == "CLOSURE" {
		transitions, err = f.store.GetRecentClosures(ctx, since, laggedEnd)
	} else {
		transitions, err = f.store.GetRecentOpenings(ctx, since, laggedEnd)
	}
	if err != nil {
		return 0, nil, err
	}

	var allImpacts []store.TransitionImpact
	for _, tr := range transitions {
		// Three snapshots: 30 min before, at event, 30 min after
		preBaseline, err := f.store.GetVelocityAndWaitAtTime(
			ctx, tr.ParkID, tr.TransitionTime.Add(-crowdFlowLag), snapshotTolerance,
		)
		if err != nil {
			return 0, nil, err
		}

		before, err := f.store.GetVelocityAndWaitAtTime(
			ctx, tr.ParkID, tr.TransitionTime, snapshotTolerance,
		)
		if err != nil {
			return 0, nil, err
		}

		after, err := f.store.GetVelocityAndWaitAtTime(
			ctx, tr.ParkID, tr.TransitionTime.Add(crowdFlowLag), snapshotTolerance,
		)
		if err != nil {
			return 0, nil, err
		}

		impacts := f.computeImpacts(tr, eventType, preBaseline, before, after)
		allImpacts = append(allImpacts, impacts...)
	}

	return len(transitions), allImpacts, nil
}

// impactDelta holds the computed three-snapshot deltas for a single impacted attraction.
type impactDelta struct {
	attrID        string
	velBefore     float32
	velAfter      float32
	velDelta      float32
	baselineDelta float32
	excessDelta   float32
	waitBefore    int
	waitAfter     int
	waitDelta     int
}

// computeImpacts calculates baseline-adjusted velocity deltas for each ride,
// filters to only those in the expected direction, and assigns scores.
func (f *CrowdFlowMetric) computeImpacts(
	transition store.StatusTransitionRecord,
	eventType string,
	preBaseline, before, after []store.AttractionSnapshot,
) []store.TransitionImpact {
	preMap := make(map[string]store.AttractionSnapshot, len(preBaseline))
	for _, s := range preBaseline {
		preMap[s.AttractionID] = s
	}
	beforeMap := make(map[string]store.AttractionSnapshot, len(before))
	for _, s := range before {
		beforeMap[s.AttractionID] = s
	}
	afterMap := make(map[string]store.AttractionSnapshot, len(after))
	for _, s := range after {
		afterMap[s.AttractionID] = s
	}

	// Compute deltas for attractions present in both before and after snapshots
	var deltas []impactDelta
	for attrID, b := range beforeMap {
		if attrID == transition.AttractionID {
			continue
		}

		a, ok := afterMap[attrID]
		if !ok {
			continue
		}

		velDelta := a.VelocityEMA - b.VelocityEMA

		// Compute baseline: how was this ride's velocity already changing?
		var baselineDelta float32
		if pre, ok := preMap[attrID]; ok {
			baselineDelta = b.VelocityEMA - pre.VelocityEMA
		}
		// If no pre-baseline snapshot, baseline is 0 (assume stable)

		excessDelta := velDelta - baselineDelta

		// Filter: only keep impacts in the expected direction
		if eventType == "CLOSURE" && excessDelta <= 0 {
			continue // ride didn't accelerate beyond pre-existing trend
		}
		if eventType == "OPENING" && excessDelta >= 0 {
			continue // ride didn't decelerate beyond pre-existing trend
		}

		deltas = append(deltas, impactDelta{
			attrID:        attrID,
			velBefore:     b.VelocityEMA,
			velAfter:      a.VelocityEMA,
			velDelta:      velDelta,
			baselineDelta: baselineDelta,
			excessDelta:   excessDelta,
			waitBefore:    b.StandbyWait,
			waitAfter:     a.StandbyWait,
			waitDelta:     a.StandbyWait - b.StandbyWait,
		})
	}

	if len(deltas) == 0 {
		return nil
	}

	// Compute normalization denominator from excess deltas
	var totalExcess float32
	for _, d := range deltas {
		if eventType == "CLOSURE" {
			totalExcess += d.excessDelta // all positive after filtering
		} else {
			totalExcess += -d.excessDelta // invert negatives to get positive sum
		}
	}

	result := make([]store.TransitionImpact, len(deltas))
	for i, d := range deltas {
		velBefore := d.velBefore
		velAfter := d.velAfter
		velDelta := d.velDelta
		baselineDelta := d.baselineDelta
		excessDelta := d.excessDelta
		waitBefore := d.waitBefore
		waitAfter := d.waitAfter
		waitDelta := d.waitDelta

		imp := store.TransitionImpact{
			TransitionID:           transition.ID,
			EventType:              eventType,
			TransitionedAttraction: transition.AttractionID,
			ImpactedAttraction:     d.attrID,
			ParkID:                 transition.ParkID,
			TransitionTime:         transition.TransitionTime,
			VelocityBefore:         &velBefore,
			VelocityAfter:          &velAfter,
			VelocityDelta:          &velDelta,
			BaselineDelta:          &baselineDelta,
			ExcessDelta:            &excessDelta,
			WaitBefore:             &waitBefore,
			WaitAfter:              &waitAfter,
			WaitDelta:              &waitDelta,
		}

		if totalExcess > 0 {
			if eventType == "CLOSURE" {
				score := d.excessDelta / totalExcess
				imp.AbsorptionScore = &score
			} else {
				score := -d.excessDelta / totalExcess
				imp.DrainScore = &score
			}
		}

		result[i] = imp
	}

	return result
}
