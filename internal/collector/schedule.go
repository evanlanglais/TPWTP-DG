package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// scheduleWindow represents an open/close time window for a park.
type scheduleWindow struct {
	Open  time.Time
	Close time.Time
}

// openScheduleTypes are the schedule entry types that count as "park is open".
var openScheduleTypes = map[string]bool{
	"OPERATING":      true,
	"TICKETED_EVENT": true,
	"EXTRA_HOURS":    true,
}

// syncSchedules fetches schedule data for all parks, stores it in the DB,
// and rebuilds the in-memory schedule cache.
func (c *Collector) syncSchedules(ctx context.Context) {
	slog.Info("syncing park schedules")
	start := time.Now()

	parkIDs, err := c.store.GetParkIDs(ctx)
	if err != nil {
		slog.Error("failed to get park IDs for schedule sync", "error", err)
		return
	}

	type parkResult struct {
		parkID  string
		windows []scheduleWindow
	}

	results := make(chan parkResult, len(parkIDs))
	var wg sync.WaitGroup

	for _, pid := range parkIDs {
		wg.Add(1)
		go func(parkID string) {
			defer wg.Done()

			resp, err := c.client.GetEntitySchedule(ctx, parkID)
			if err != nil {
				slog.Error("failed to fetch schedule", "park_id", parkID, "error", err)
				return
			}

			var entries []store.ParkScheduleEntry
			var windows []scheduleWindow

			for _, se := range resp.Schedule {
				date, err := time.Parse(time.DateOnly, se.Date)
				if err != nil {
					slog.Warn("skipping schedule entry with invalid date",
						"park_id", parkID, "date", se.Date, "error", err)
					continue
				}

				entries = append(entries, store.ParkScheduleEntry{
					ParkID:      parkID,
					Date:        date,
					Type:        se.Type,
					OpeningTime: se.OpeningTime,
					ClosingTime: se.ClosingTime,
				})

				if openScheduleTypes[se.Type] {
					windows = append(windows, scheduleWindow{
						Open:  se.OpeningTime,
						Close: se.ClosingTime,
					})
				}
			}

			if err := c.store.UpsertParkSchedules(ctx, parkID, entries); err != nil {
				slog.Error("failed to upsert park schedules", "park_id", parkID, "error", err)
				return
			}

			results <- parkResult{parkID: parkID, windows: windows}
		}(pid)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	newCache := make(map[string][]scheduleWindow)
	var parksSynced int
	for pr := range results {
		newCache[pr.parkID] = pr.windows
		parksSynced++
	}

	// Swap cache under write lock
	c.scheduleMu.Lock()
	c.scheduleCache = newCache
	c.scheduleMu.Unlock()

	slog.Info("park schedule sync complete",
		"parks_synced", parksSynced,
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// isParkOpen checks whether the park is currently open at time t.
// Returns true if t falls within any open schedule window, or if no schedule data exists (fail-open).
func (c *Collector) isParkOpen(parkID string, t time.Time) bool {
	c.scheduleMu.RLock()
	windows, exists := c.scheduleCache[parkID]
	c.scheduleMu.RUnlock()

	// Fail-open: no schedule data means treat as always open
	if !exists || len(windows) == 0 {
		return true
	}

	for _, w := range windows {
		// open <= t < close
		if !t.Before(w.Open) && t.Before(w.Close) {
			return true
		}
	}
	return false
}

// filterParkClosed removes wait time entries with status "OPERATING" (park is closed,
// so OPERATING data is stale/false). Non-OPERATING statuses (DOWN, CLOSED, REFURBISHMENT)
// are kept for maintenance visibility.
// Returns the filtered slice and the number of entries removed.
func filterParkClosed(waitTimes []store.WaitTime) ([]store.WaitTime, int) {
	kept := make([]store.WaitTime, 0, len(waitTimes))
	filtered := 0
	for _, wt := range waitTimes {
		if wt.Status == "OPERATING" {
			filtered++
			continue
		}
		kept = append(kept, wt)
	}
	return kept, filtered
}
