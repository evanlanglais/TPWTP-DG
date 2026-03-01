package processor

import (
	"context"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/evanlanglais/tpwtp-dg/internal/store"
)

const (
	// correlationWindow is the lookback window for computing correlations.
	correlationWindow = 2 * time.Hour

	// minCorrelationSamples is the minimum number of aligned data points
	// required to compute a meaningful correlation.
	minCorrelationSamples = 6

	// minAbsCorrelation is the minimum |r| to store a correlation pair.
	minAbsCorrelation = 0.3
)

// CorrelationMetric computes pairwise Pearson correlations between attractions'
// velocity EMA series within each park. Runs hourly.
type CorrelationMetric struct {
	store    *store.Store
	lastHour int // tracks the last hour boundary we computed for
}

// NewCorrelationMetric creates a new CorrelationMetric.
func NewCorrelationMetric(s *store.Store) *CorrelationMetric {
	return &CorrelationMetric{store: s, lastHour: -1}
}

func (c *CorrelationMetric) Name() string           { return "correlation" }
func (c *CorrelationMetric) Version() int            { return 2 }
func (c *CorrelationMetric) OutputTables() []string   { return []string{"l3_correlation_pairs"} }

func (c *CorrelationMetric) Process(ctx context.Context, since, now time.Time) error {
	// Only compute on hour boundaries
	currentHour := now.Truncate(time.Hour).Hour()
	sinceHour := since.Truncate(time.Hour).Hour()

	// Skip if since and now are in the same hour (no new hour boundary crossed)
	if now.Truncate(time.Hour).Equal(since.Truncate(time.Hour)) {
		return nil
	}

	// Avoid recomputing for the same hour
	if currentHour == c.lastHour && sinceHour != currentHour {
		return nil
	}
	c.lastHour = currentHour

	// Get all parks that have velocity data
	parkIDs, err := c.store.GetParkIDsWithVelocityData(ctx, now.Add(-correlationWindow), now)
	if err != nil {
		return err
	}

	var totalPairs int
	windowStart := now.Add(-correlationWindow)

	for _, parkID := range parkIDs {
		series, err := c.store.GetVelocityEMAWindow(ctx, parkID, windowStart, now)
		if err != nil {
			return err
		}

		pairs := c.computeParkCorrelations(parkID, series, now, windowStart)
		if len(pairs) == 0 {
			continue
		}

		n, err := c.store.InsertCorrelationPairs(ctx, pairs)
		if err != nil {
			return err
		}
		totalPairs += n
	}

	slog.Debug("correlation metric processed",
		"pairs", totalPairs,
		"parks", len(parkIDs),
	)
	return nil
}

// computeParkCorrelations computes pairwise Pearson r for all attractions in a park.
func (c *CorrelationMetric) computeParkCorrelations(
	parkID string,
	series map[string][]store.TimestampedValue,
	computedAt, windowStart time.Time,
) []store.CorrelationPair {
	// Collect all unique timestamps across all attractions
	tsSet := make(map[time.Time]bool)
	for _, values := range series {
		for _, v := range values {
			tsSet[v.Timestamp] = true
		}
	}

	// Sort timestamps for consistent alignment
	timestamps := make([]time.Time, 0, len(tsSet))
	for ts := range tsSet {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i].Before(timestamps[j])
	})

	// Build timestamp index
	tsIndex := make(map[time.Time]int, len(timestamps))
	for i, ts := range timestamps {
		tsIndex[ts] = i
	}

	// Build aligned series: map attraction -> values indexed by timestamp position
	type alignedSeries struct {
		attrID string
		values []float64 // indexed by timestamp position, NaN if missing
	}

	var aligned []alignedSeries
	for attrID, values := range series {
		if len(values) < minCorrelationSamples {
			continue
		}

		as := alignedSeries{
			attrID: attrID,
			values: make([]float64, len(timestamps)),
		}
		for i := range as.values {
			as.values[i] = math.NaN()
		}
		for _, v := range values {
			if idx, ok := tsIndex[v.Timestamp]; ok {
				as.values[idx] = float64(v.Value)
			}
		}
		aligned = append(aligned, as)
	}

	// Sort by attrID for canonical ordering (attraction_a < attraction_b)
	sort.Slice(aligned, func(i, j int) bool {
		return aligned[i].attrID < aligned[j].attrID
	})

	var pairs []store.CorrelationPair

	for i := 0; i < len(aligned); i++ {
		for j := i + 1; j < len(aligned); j++ {
			r, n := pearson(aligned[i].values, aligned[j].values)
			if n < minCorrelationSamples {
				continue
			}
			if math.Abs(r) < minAbsCorrelation {
				continue
			}

			pairs = append(pairs, store.CorrelationPair{
				ParkID:      parkID,
				AttractionA: aligned[i].attrID,
				AttractionB: aligned[j].attrID,
				ComputedAt:  computedAt,
				WindowStart: windowStart,
				WindowEnd:   computedAt,
				Correlation: float32(r),
				SampleCount: int16(n),
			})
		}
	}

	return pairs
}

// pearson computes the Pearson correlation coefficient between two series.
// NaN values in either series are excluded (pairwise deletion).
// Returns the correlation and the number of valid pairs used.
func pearson(x, y []float64) (float64, int) {
	n := len(x)
	if n != len(y) || n == 0 {
		return 0, 0
	}

	var sumX, sumY, sumXX, sumYY, sumXY float64
	var count int

	for i := 0; i < n; i++ {
		if math.IsNaN(x[i]) || math.IsNaN(y[i]) {
			continue
		}
		sumX += x[i]
		sumY += y[i]
		sumXX += x[i] * x[i]
		sumYY += y[i] * y[i]
		sumXY += x[i] * y[i]
		count++
	}

	if count < 2 {
		return 0, count
	}

	cf := float64(count)
	num := cf*sumXY - sumX*sumY
	den := math.Sqrt((cf*sumXX - sumX*sumX) * (cf*sumYY - sumY*sumY))

	if den == 0 {
		return 0, count
	}

	return num / den, count
}
