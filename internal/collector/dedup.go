package collector

import (
	"sync"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// lastObservation holds the fields we compare to detect unchanged non-operating rows.
type lastObservation struct {
	Status          string
	StandbyWait     *int
	SingleRiderWait *int
	ReturnTimeState *string
	ReturnTimeStart *time.Time
	ReturnTimeEnd   *time.Time
}

func ptrIntEqual(a, b *int) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptrStringEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func ptrTimeEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}

func (o *lastObservation) matches(wt *store.WaitTime) bool {
	return o.Status == wt.Status &&
		ptrIntEqual(o.StandbyWait, wt.StandbyWait) &&
		ptrIntEqual(o.SingleRiderWait, wt.SingleRiderWait) &&
		ptrStringEqual(o.ReturnTimeState, wt.ReturnTimeState) &&
		ptrTimeEqual(o.ReturnTimeStart, wt.ReturnTimeStart) &&
		ptrTimeEqual(o.ReturnTimeEnd, wt.ReturnTimeEnd)
}

func observationFrom(wt *store.WaitTime) *lastObservation {
	return &lastObservation{
		Status:          wt.Status,
		StandbyWait:     wt.StandbyWait,
		SingleRiderWait: wt.SingleRiderWait,
		ReturnTimeState: wt.ReturnTimeState,
		ReturnTimeStart: wt.ReturnTimeStart,
		ReturnTimeEnd:   wt.ReturnTimeEnd,
	}
}

// filterUnchanged removes non-OPERATING rows that are identical to the last
// observation for that attraction. OPERATING rows are always kept.
// Returns the filtered slice and the number of rows skipped.
func filterUnchanged(waitTimes []store.WaitTime, lastSeen map[string]*lastObservation, mu *sync.Mutex) ([]store.WaitTime, int) {
	kept := make([]store.WaitTime, 0, len(waitTimes))
	skipped := 0

	mu.Lock()
	defer mu.Unlock()

	for i := range waitTimes {
		wt := &waitTimes[i]

		if wt.Status == "OPERATING" {
			kept = append(kept, *wt)
			lastSeen[wt.AttractionID] = observationFrom(wt)
			continue
		}

		prev, exists := lastSeen[wt.AttractionID]
		if exists && prev.matches(wt) {
			skipped++
			continue
		}

		kept = append(kept, *wt)
		lastSeen[wt.AttractionID] = observationFrom(wt)
	}

	return kept, skipped
}
