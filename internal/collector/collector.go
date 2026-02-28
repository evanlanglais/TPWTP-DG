package collector

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/api"
	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// Collector polls the themeparks.wiki API and writes wait times to the store.
type Collector struct {
	client   *api.Client
	store    *store.Store
	interval time.Duration
}

// New creates a new Collector.
func New(client *api.Client, store *store.Store, interval time.Duration) *Collector {
	return &Collector{
		client:   client,
		store:    store,
		interval: interval,
	}
}

// Run starts the collector loop. It blocks until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) error {
	// Sync reference data on startup
	if err := c.syncReferenceData(ctx); err != nil {
		return err
	}

	// Run first poll immediately
	c.pollAllParks(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Refresh reference data daily
	refreshTicker := time.NewTicker(24 * time.Hour)
	defer refreshTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("collector shutting down")
			return nil
		case <-ticker.C:
			c.pollAllParks(ctx)
		case <-refreshTicker.C:
			if err := c.syncReferenceData(ctx); err != nil {
				slog.Error("daily reference data refresh failed", "error", err)
			}
		}
	}
}

// syncReferenceData fetches destinations, parks, and attractions from the API
// and upserts them into the database.
func (c *Collector) syncReferenceData(ctx context.Context) error {
	slog.Info("syncing reference data from API")
	start := time.Now()

	// Fetch all destinations
	destResp, err := c.client.GetDestinations(ctx)
	if err != nil {
		return err
	}

	var destinations []store.Destination
	var parks []store.Park
	var parkIDs []string

	for _, d := range destResp.Destinations {
		destinations = append(destinations, store.Destination{
			ID:   d.ID,
			Name: d.Name,
			Slug: d.Slug,
		})
		for _, p := range d.Parks {
			parkIDs = append(parkIDs, p.ID)
			// Timezone will be filled in when we fetch children
			parks = append(parks, store.Park{
				ID:            p.ID,
				DestinationID: d.ID,
				Name:          p.Name,
				Timezone:      "UTC",
			})
		}
	}

	// Upsert destinations first (parks reference them)
	if err := c.store.UpsertDestinations(ctx, destinations); err != nil {
		return err
	}
	slog.Info("synced destinations", "count", len(destinations))

	// Upsert parks
	if err := c.store.UpsertParks(ctx, parks); err != nil {
		return err
	}
	slog.Info("synced parks", "count", len(parks))

	// Fetch children for each park to get attractions and timezones
	type parkChildren struct {
		parkID   string
		timezone string
		attrs    []store.Attraction
	}

	results := make(chan parkChildren, len(parkIDs))
	var wg sync.WaitGroup

	for _, pid := range parkIDs {
		wg.Add(1)
		go func(parkID string) {
			defer wg.Done()
			resp, err := c.client.GetEntityChildren(ctx, parkID)
			if err != nil {
				slog.Error("failed to fetch children", "park_id", parkID, "error", err)
				return
			}

			var attrs []store.Attraction
			for _, child := range resp.Children {
				if child.EntityType == "ATTRACTION" {
					attrs = append(attrs, store.Attraction{
						ID:         child.ID,
						ParkID:     parkID,
						Name:       child.Name,
						ExternalID: child.ExternalID,
					})
				}
			}

			tz := resp.Timezone
			if tz == "" {
				tz = "UTC"
			}

			results <- parkChildren{
				parkID:   parkID,
				timezone: tz,
				attrs:    attrs,
			}
		}(pid)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalAttractions int
	var parkUpdates []store.Park
	for pc := range results {
		if len(pc.attrs) > 0 {
			if err := c.store.UpsertAttractions(ctx, pc.attrs); err != nil {
				slog.Error("failed to upsert attractions", "park_id", pc.parkID, "error", err)
				continue
			}
			totalAttractions += len(pc.attrs)
		}
		// Collect timezone updates
		parkUpdates = append(parkUpdates, store.Park{
			ID:       pc.parkID,
			Timezone: pc.timezone,
		})
	}

	// Update park timezones (do a targeted update since we only have id + timezone here)
	for _, pu := range parkUpdates {
		if err := c.store.UpdateParkTimezone(ctx, pu.ID, pu.Timezone); err != nil {
			slog.Error("failed to update park timezone", "park_id", pu.ID, "error", err)
		}
	}

	slog.Info("reference data sync complete",
		"destinations", len(destinations),
		"parks", len(parks),
		"attractions", totalAttractions,
		"duration", time.Since(start).Round(time.Millisecond),
	)
	return nil
}

// pollAllParks fetches live data for every park and inserts wait times.
func (c *Collector) pollAllParks(ctx context.Context) {
	start := time.Now()

	parkIDs, err := c.store.GetParkIDs(ctx)
	if err != nil {
		slog.Error("failed to get park IDs", "error", err)
		return
	}

	var (
		wg             sync.WaitGroup
		totalWaitTimes atomic.Int64
		errorCount     atomic.Int64
		parksPolled    atomic.Int64
	)

	for _, pid := range parkIDs {
		wg.Add(1)
		go func(parkID string) {
			defer wg.Done()
			n, err := c.pollPark(ctx, parkID)
			if err != nil {
				slog.Error("failed to poll park", "park_id", parkID, "error", err)
				errorCount.Add(1)
				return
			}
			parksPolled.Add(1)
			totalWaitTimes.Add(int64(n))
		}(pid)
	}

	wg.Wait()

	slog.Info("poll complete",
		"parks_polled", parksPolled.Load(),
		"wait_times_recorded", totalWaitTimes.Load(),
		"errors", errorCount.Load(),
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// pollPark fetches live data for a single park and inserts wait times.
// Returns the number of wait time records inserted.
func (c *Collector) pollPark(ctx context.Context, parkID string) (int, error) {
	resp, err := c.client.GetEntityLive(ctx, parkID)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC()

	// Get known attractions to detect new ones
	knownAttrs, err := c.store.GetKnownAttractionIDs(ctx, parkID)
	if err != nil {
		slog.Warn("could not load known attractions, will skip auto-discovery", "park_id", parkID, "error", err)
		knownAttrs = make(map[string]bool)
	}

	var waitTimes []store.WaitTime
	var newAttrs []store.Attraction

	for _, entity := range resp.LiveData {
		if entity.EntityType != "ATTRACTION" {
			continue
		}

		// Auto-discover new attractions
		if !knownAttrs[entity.ID] {
			newAttrs = append(newAttrs, store.Attraction{
				ID:         entity.ID,
				ParkID:     parkID,
				Name:       entity.Name,
				ExternalID: entity.ExternalID,
			})
		}

		wt := store.WaitTime{
			AttractionID: entity.ID,
			Timestamp:    now,
			Status:       entity.Status,
		}

		if entity.Queue != nil {
			if entity.Queue.Standby != nil {
				wt.StandbyWait = entity.Queue.Standby.WaitTime
			}
			if entity.Queue.SingleRider != nil {
				wt.SingleRiderWait = entity.Queue.SingleRider.WaitTime
			}

			// Check both free and paid return time
			rt := entity.Queue.ReturnTime
			if rt == nil {
				rt = entity.Queue.PaidReturnTime
			}
			if rt != nil {
				wt.ReturnTimeState = &rt.State
				if rt.ReturnStart != nil {
					if t, err := time.Parse(time.RFC3339, *rt.ReturnStart); err == nil {
						wt.ReturnTimeStart = &t
					}
				}
				if rt.ReturnEnd != nil {
					if t, err := time.Parse(time.RFC3339, *rt.ReturnEnd); err == nil {
						wt.ReturnTimeEnd = &t
					}
				}
			}
		}

		waitTimes = append(waitTimes, wt)
	}

	// Upsert newly discovered attractions before inserting wait times
	if len(newAttrs) > 0 {
		if err := c.store.UpsertAttractions(ctx, newAttrs); err != nil {
			slog.Error("failed to upsert new attractions", "park_id", parkID, "count", len(newAttrs), "error", err)
		} else {
			slog.Info("discovered new attractions", "park_id", parkID, "count", len(newAttrs))
		}
	}

	inserted, err := c.store.InsertWaitTimes(ctx, waitTimes)
	if err != nil {
		return 0, err
	}
	return inserted, nil
}
