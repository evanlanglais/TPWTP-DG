package collector

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/api"
	"github.com/evanlanglais/tpwtp-dg/internal/processor"
	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

// usDestinations is the set of destination slugs for US-based parks.
var usDestinations = map[string]bool{
	// Disney
	"waltdisneyworldresort": true,
	"disneylandresort":     true,
	// Universal
	"universalorlando": true,
	"universalstudios": true, // Hollywood
	// SeaWorld / Busch Gardens
	"seaworldorlandoresort":   true,
	"seaworldsandiego":        true,
	"seaworldsanantonio":      true,
	"buschgardenstampa":       true,
	"buschgardenswillamsburg": true,
	// Cedar Fair
	"cedar-point":        true,
	"knotts-berry-farm":  true,
	"kings-island":       true,
	"kings-dominion":     true,
	"carowinds":          true,
	"great-america":      true,
	"dorney-park":        true,
	"valleyfair":         true,
	"michigans-adventure": true,
	"worlds-of-fun":      true,
	// Six Flags
	"six-flags-magic-mountain":    true,
	"six-flags-great-adventure":   true,
	"six-flags-great-america":     true,
	"six-flags-over-texas":        true,
	"six-flags-over-georgia":      true,
	"six-flags-fiesta-texas":      true,
	"six-flags-new-england":       true,
	"six-flags-america":           true,
	"six-flags-discovery-kingdom": true,
	"six-flags-st-louis":          true,
	"six-flags-great-escape":      true,
	"six-flags-darien-lake":       true,
	"six-flags-frontier-city":     true,
	"six-flags-white-water-atlanta": true,
	// Water parks
	"hurricane-harbor-oklahoma-city": true,
	"hurricane-harbor-rockford":      true,
	"hurricane-harbor-splashtown":    true,
	"hurricane-harbor-concord":       true,
	"hurricane-harbor-phoenix":       true,
	"schlitterbahn-nb":               true,
	"schlitterbahn-gv":               true,
	// Independent / Other
	"dollywood":        true,
	"hersheypark":      true,
	"silverdollarcity": true,
	"kennywood":        true,
	"knoebels":         true,
	"legolandcaliforniaresort":              true,
	"legolandorlandoresort":                 true,
	"legolandnewyorkdestination":            true,
	"peppapigthemeparkfloridadestination":   true,
}

// Collector polls the themeparks.wiki API and writes wait times to the store.
type Collector struct {
	client    *api.Client
	store     *store.Store
	processor *processor.Processor
	interval  time.Duration

	lastSeenMu sync.Mutex
	lastSeen   map[string]*lastObservation

	scheduleMu    sync.RWMutex
	scheduleCache map[string][]scheduleWindow
}

// New creates a new Collector.
func New(client *api.Client, store *store.Store, interval time.Duration) *Collector {
	return &Collector{
		client:        client,
		store:         store,
		processor:     processor.New(store),
		interval:      interval,
		lastSeen:      make(map[string]*lastObservation),
		scheduleCache: make(map[string][]scheduleWindow),
	}
}

// Run starts the collector loop. It blocks until ctx is cancelled.
func (c *Collector) Run(ctx context.Context) error {
	// Sync reference data on startup
	if err := c.syncReferenceData(ctx); err != nil {
		return err
	}

	// Sync park schedules (for park-closed filtering)
	c.syncSchedules(ctx)

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
			c.syncSchedules(ctx)
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
		if !usDestinations[d.Slug] {
			continue
		}
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

	// Prune any previously-tracked non-US data from the database
	destIDs := make([]string, len(destinations))
	for i, d := range destinations {
		destIDs[i] = d.ID
	}
	if err := c.store.PruneUntrackedParks(ctx, destIDs); err != nil {
		slog.Error("failed to prune untracked parks", "error", err)
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

	pollTimestamp := time.Now().UTC()

	slog.Info("poll complete",
		"parks_polled", parksPolled.Load(),
		"wait_times_recorded", totalWaitTimes.Load(),
		"errors", errorCount.Load(),
		"duration", time.Since(start).Round(time.Millisecond),
	)

	// Run Layer 2 derived data processing
	c.processor.RunAfterPoll(ctx, pollTimestamp)
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

	// Filter out OPERATING rides when park is closed
	if !c.isParkOpen(parkID, now) {
		var parkClosedFiltered int
		waitTimes, parkClosedFiltered = filterParkClosed(waitTimes)
		if parkClosedFiltered > 0 {
			slog.Debug("filtered operating rides during park closure",
				"park_id", parkID, "filtered", parkClosedFiltered)
		}
	}

	// Filter out unchanged non-OPERATING rows to reduce storage
	var skipped int
	waitTimes, skipped = filterUnchanged(waitTimes, c.lastSeen, &c.lastSeenMu)
	if skipped > 0 {
		slog.Debug("skipped unchanged non-operating rows", "park_id", parkID, "skipped", skipped)
	}

	inserted, err := c.store.InsertWaitTimes(ctx, waitTimes)
	if err != nil {
		return 0, err
	}
	return inserted, nil
}
