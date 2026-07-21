package collector

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/api"
	"github.com/evanlanglais/tpwtp-dg/internal/processor"
	"github.com/evanlanglais/tpwtp-dg/internal/store"
	"github.com/evanlanglais/tpwtp-dg/internal/weather"
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

	// weather is nil when no WEATHER_API_KEY is configured (weather source disabled).
	weather         *weather.Client
	weatherInterval time.Duration

	lastSeenMu sync.Mutex
	lastSeen   map[string]*lastObservation

	scheduleMu    sync.RWMutex
	scheduleCache map[string][]scheduleWindow
}

// New creates a new Collector. weatherClient may be nil to disable the weather source.
func New(client *api.Client, weatherClient *weather.Client, store *store.Store, interval, weatherInterval time.Duration) *Collector {
	return &Collector{
		client:          client,
		store:           store,
		processor:       processor.New(store),
		interval:        interval,
		weather:         weatherClient,
		weatherInterval: weatherInterval,
		lastSeen:        make(map[string]*lastObservation),
		scheduleCache:   make(map[string][]scheduleWindow),
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

	// Weather runs as a sibling source on its own slower cadence, only when configured.
	// A nil channel blocks forever, so the select case never fires when disabled.
	var weatherC <-chan time.Time
	if c.weather != nil {
		c.pollAllWeather(ctx) // first weather poll immediately
		weatherTicker := time.NewTicker(c.weatherInterval)
		defer weatherTicker.Stop()
		weatherC = weatherTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("collector shutting down")
			return nil
		case <-ticker.C:
			c.pollAllParks(ctx)
		case <-weatherC:
			c.pollAllWeather(ctx)
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
	parkToDest := make(map[string]string) // park ID -> destination ID (for weather centroids)

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
			parkToDest[p.ID] = d.ID
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

	// Fetch children for each park to get attractions, timezones, and locations.
	// latSum/lonSum/locCount accumulate this park's located attractions so a
	// destination centroid (the weather fetch coordinate) can be computed.
	type parkChildren struct {
		parkID   string
		timezone string
		attrs    []store.Attraction
		latSum   float64
		lonSum   float64
		locCount int
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
			var latSum, lonSum float64
			var locCount int
			for _, child := range resp.Children {
				if child.EntityType == "ATTRACTION" {
					attrs = append(attrs, store.Attraction{
						ID:         child.ID,
						ParkID:     parkID,
						Name:       child.Name,
						ExternalID: child.ExternalID,
					})
					if child.Location != nil {
						latSum += child.Location.Latitude
						lonSum += child.Location.Longitude
						locCount++
					}
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
				latSum:   latSum,
				lonSum:   lonSum,
				locCount: locCount,
			}
		}(pid)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// destCoord accumulates located-attraction coordinates per destination.
	type destCoord struct {
		latSum, lonSum float64
		count          int
	}

	var totalAttractions int
	var parkUpdates []store.Park
	destCoords := make(map[string]*destCoord)
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
		// Accumulate this park's located attractions into its destination's centroid.
		if pc.locCount > 0 {
			if destID := parkToDest[pc.parkID]; destID != "" {
				acc := destCoords[destID]
				if acc == nil {
					acc = &destCoord{}
					destCoords[destID] = acc
				}
				acc.latSum += pc.latSum
				acc.lonSum += pc.lonSum
				acc.count += pc.locCount
			}
		}
	}

	// Update park timezones (do a targeted update since we only have id + timezone here)
	for _, pu := range parkUpdates {
		if err := c.store.UpdateParkTimezone(ctx, pu.ID, pu.Timezone); err != nil {
			slog.Error("failed to update park timezone", "park_id", pu.ID, "error", err)
		}
	}

	// Store each destination's weather centroid (mean of its attraction locations).
	// Destinations with no located attractions keep NULL coords and are skipped by the
	// weather poll (fail-safe).
	for destID, acc := range destCoords {
		lat := acc.latSum / float64(acc.count)
		lon := acc.lonSum / float64(acc.count)
		if err := c.store.UpdateDestinationCoordinates(ctx, destID, lat, lon); err != nil {
			slog.Error("failed to update destination coordinates", "destination_id", destID, "error", err)
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
	res, err := c.client.GetEntityLiveWithRaw(ctx, parkID)
	now := time.Now().UTC()

	// Persist the raw response before projection (invariant I1). A failed fetch is
	// recorded too — an absent-payload row marks a gap in the series.
	observationID := newUUID()
	if werr := c.writeRawObservation(ctx, parkID, observationID, now, res, err); werr != nil {
		slog.Error("failed to write raw observation", "park_id", parkID, "error", werr)
		observationID = "" // don't stamp a dangling lineage pointer
	}

	if err != nil {
		return 0, err
	}
	resp := res.Response

	// Get known attractions to detect new ones
	knownAttrs, err := c.store.GetKnownAttractionIDs(ctx, parkID)
	if err != nil {
		slog.Warn("could not load known attractions, will skip auto-discovery", "park_id", parkID, "error", err)
		knownAttrs = make(map[string]bool)
	}

	var waitTimes []store.WaitTime
	var newAttrs []store.Attraction

	// Lineage pointer shared by every row projected from this poll's raw response.
	var obsIDPtr *string
	if observationID != "" {
		obsIDPtr = &observationID
	}

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
			AttractionID:        entity.ID,
			Timestamp:           now,
			Status:              entity.Status,
			SourceObservationID: obsIDPtr,
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

// writeRawObservation persists the verbatim source response (invariant I1). res may
// be nil (transport error) and its body may be non-JSON (an error page); both cases
// are handled so the jsonb payload only ever receives valid JSON.
func (c *Collector) writeRawObservation(ctx context.Context, parkID, observationID string, now time.Time, res *api.LiveResult, fetchErr error) error {
	meta := map[string]any{"park_id": parkID}
	var payload json.RawMessage

	if res != nil {
		meta["http_status"] = res.HTTPStatus
		meta["latency_ms"] = res.LatencyMS
		switch {
		case len(res.Raw) == 0:
			// nothing to store
		case json.Valid(res.Raw):
			payload = res.Raw
			if res.Response != nil {
				meta["entity_count"] = len(res.Response.LiveData)
			}
		default:
			// Non-JSON body: keep a bounded copy as a string so payload stays valid jsonb.
			body := string(res.Raw)
			if len(body) > 2000 {
				body = body[:2000]
			}
			meta["raw_body"] = body
		}
	}
	if fetchErr != nil {
		meta["error"] = fetchErr.Error()
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling raw observation metadata: %w", err)
	}

	return c.store.InsertRawObservation(ctx, store.RawObservation{
		ObservationID: observationID,
		SourceType:    "themeparks_wiki_live",
		ObservedAt:    now,
		ReceivedAt:    now,
		Payload:       payload,
		Metadata:      metaJSON,
	})
}

// pollAllWeather fetches weather for every destination with coordinates and projects
// it to that destination's parks. Runs on its own (slower) ticker; it feeds no Metric,
// so it does NOT trigger Layer 2 processing.
func (c *Collector) pollAllWeather(ctx context.Context) {
	start := time.Now()

	targets, err := c.store.GetWeatherTargets(ctx)
	if err != nil {
		slog.Error("failed to get weather targets", "error", err)
		return
	}
	if len(targets) == 0 {
		slog.Warn("no destinations have coordinates; skipping weather poll")
		return
	}

	var (
		wg          sync.WaitGroup
		totalObs    atomic.Int64
		errorCount  atomic.Int64
		destsPolled atomic.Int64
	)

	for _, t := range targets {
		wg.Add(1)
		go func(target store.WeatherTarget) {
			defer wg.Done()
			n, err := c.pollDestinationWeather(ctx, target)
			if err != nil {
				slog.Error("failed to poll weather", "destination_id", target.DestinationID, "error", err)
				errorCount.Add(1)
				return
			}
			destsPolled.Add(1)
			totalObs.Add(int64(n))
		}(t)
	}

	wg.Wait()

	slog.Info("weather poll complete",
		"destinations_polled", destsPolled.Load(),
		"observations_recorded", totalObs.Load(),
		"errors", errorCount.Load(),
		"duration", time.Since(start).Round(time.Millisecond),
	)
}

// pollDestinationWeather fetches weather for one destination and projects it. Mirrors
// pollPark: the raw response is persisted BEFORE projection and even on a failed fetch
// (invariant I1). Current conditions become PIT-correct weather_observations; the next
// ~6h of hourly forecast become bi-temporal weather_forecasts (issued_at = now). Every
// projected row is written for each park in the destination. Returns the number of
// current-conditions rows inserted.
func (c *Collector) pollDestinationWeather(ctx context.Context, target store.WeatherTarget) (int, error) {
	res, err := c.weather.FetchWithRaw(ctx, target.Lat, target.Lon)
	now := time.Now().UTC()

	// Persist raw before projection (I1). A failed fetch is recorded too (gap marker).
	observationID := newUUID()
	if werr := c.writeRawWeatherObservation(ctx, target, observationID, now, res, err); werr != nil {
		slog.Error("failed to write raw weather observation", "destination_id", target.DestinationID, "error", werr)
		observationID = "" // don't stamp a dangling lineage pointer
	}

	if err != nil {
		return 0, err
	}
	resp := res.Response
	if resp == nil {
		return 0, nil
	}

	// Lineage pointer shared by every row projected from this poll's raw response.
	var obsIDPtr *string
	if observationID != "" {
		obsIDPtr = &observationID
	}

	var observations []store.WeatherObservation
	var forecasts []store.WeatherForecast

	// Current conditions — stamped at the observation's valid time (PIT-correct).
	if resp.Current != nil {
		obsTime := time.Unix(resp.Current.Dt, 0).UTC()
		for _, parkID := range target.ParkIDs {
			observations = append(observations, store.WeatherObservation{
				ParkID:              parkID,
				Timestamp:           obsTime,
				TempC:               resp.Current.Temp,
				FeelsLikeC:          resp.Current.FeelsLike,
				PrecipMM:            weather.PrecipMM(resp.Current.Rain, resp.Current.Snow),
				Humidity:            resp.Current.Humidity,
				WindSpeedMS:         resp.Current.WindSpeed,
				CloudPct:            resp.Current.Clouds,
				WeatherCode:         weather.CondCode(resp.Current.Weather),
				WeatherMain:         weather.CondMain(resp.Current.Weather),
				SourceObservationID: obsIDPtr,
			})
		}
	}

	// Forecast — hourly entries strictly in the future, up to +6h. issued_at = now is
	// the PIT/as-of key; valid_at is the future time each entry describes.
	horizonCap := now.Add(6 * time.Hour)
	for _, h := range resp.Hourly {
		validAt := time.Unix(h.Dt, 0).UTC()
		if !validAt.After(now) || validAt.After(horizonCap) {
			continue
		}
		horizonMin := int(validAt.Sub(now).Round(time.Minute) / time.Minute)
		for _, parkID := range target.ParkIDs {
			forecasts = append(forecasts, store.WeatherForecast{
				ParkID:              parkID,
				IssuedAt:            now,
				ValidAt:             validAt,
				HorizonMinutes:      horizonMin,
				TempC:               h.Temp,
				PrecipMM:            weather.PrecipMM(h.Rain, h.Snow),
				PrecipProb:          h.Pop,
				WeatherCode:         weather.CondCode(h.Weather),
				WeatherMain:         weather.CondMain(h.Weather),
				SourceObservationID: obsIDPtr,
			})
		}
	}

	insertedObs, err := c.store.InsertWeatherObservations(ctx, observations)
	if err != nil {
		return 0, err
	}
	if _, err := c.store.InsertWeatherForecasts(ctx, forecasts); err != nil {
		return insertedObs, err
	}
	return insertedObs, nil
}

// writeRawWeatherObservation persists the verbatim weather response (invariant I1),
// mirroring writeRawObservation. res may be nil (transport error) and its body may be
// non-JSON; both are handled so the jsonb payload only ever receives valid JSON.
func (c *Collector) writeRawWeatherObservation(ctx context.Context, target store.WeatherTarget, observationID string, now time.Time, res *weather.Result, fetchErr error) error {
	meta := map[string]any{
		"destination_id": target.DestinationID,
		"lat":            target.Lat,
		"lon":            target.Lon,
	}
	var payload json.RawMessage

	if res != nil {
		meta["http_status"] = res.HTTPStatus
		meta["latency_ms"] = res.LatencyMS
		switch {
		case len(res.Raw) == 0:
			// nothing to store
		case json.Valid(res.Raw):
			payload = res.Raw
		default:
			// Non-JSON body: keep a bounded copy as a string so payload stays valid jsonb.
			body := string(res.Raw)
			if len(body) > 2000 {
				body = body[:2000]
			}
			meta["raw_body"] = body
		}
	}
	if fetchErr != nil {
		meta["error"] = fetchErr.Error()
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshaling raw weather observation metadata: %w", err)
	}

	return c.store.InsertRawObservation(ctx, store.RawObservation{
		ObservationID: observationID,
		SourceType:    "openweather_onecall",
		ObservedAt:    now,
		ReceivedAt:    now,
		Payload:       payload,
		Metadata:      metaJSON,
	})
}

// newUUID returns a random RFC-4122 v4 UUID string (crypto/rand; no dependency).
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
