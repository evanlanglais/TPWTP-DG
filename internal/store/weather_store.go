package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// WeatherObservation is a projected current-conditions row (PIT-correct: stamped at
// Timestamp, the observation's valid time). Mirrors WaitTime's shape.
type WeatherObservation struct {
	ParkID      string
	Timestamp   time.Time
	TempC       float64
	FeelsLikeC  float64
	PrecipMM    float64
	Humidity    int
	WindSpeedMS float64
	CloudPct    int
	WeatherCode int
	WeatherMain string
	// SourceObservationID links this projected row back to the raw_observations row
	// it came from (invariant I1). Nil if the raw write failed.
	SourceObservationID *string
}

// WeatherForecast is a projected bi-temporal forecast row. It is PIT-correct only when
// read as-of IssuedAt (the time the forecast became knowable); ValidAt is the future
// time it describes. Never overwrite a row with a later actual — see migration 008.
type WeatherForecast struct {
	ParkID              string
	IssuedAt            time.Time
	ValidAt             time.Time
	HorizonMinutes      int
	TempC               float64
	PrecipMM            float64
	PrecipProb          float64
	WeatherCode         int
	WeatherMain         string
	SourceObservationID *string
}

// WeatherPark is a park with a known coordinate. The collector clusters these into
// fetch locations (parks within ~2km share one weather call).
type WeatherPark struct {
	ParkID string
	Lat    float64
	Lon    float64
}

// UpdateParkCoordinates sets a park's weather centroid.
func (s *Store) UpdateParkCoordinates(ctx context.Context, parkID string, lat, lon float64) error {
	_, err := s.pool.Exec(ctx,
		"UPDATE parks SET latitude = $1, longitude = $2 WHERE id = $3", lat, lon, parkID)
	if err != nil {
		return fmt.Errorf("updating park coordinates: %w", err)
	}
	return nil
}

// GetWeatherParks returns every park that has coordinates. Parks without coordinates are
// skipped (fail-safe). The collector clusters the result into fetch locations.
func (s *Store) GetWeatherParks(ctx context.Context) ([]WeatherPark, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, latitude, longitude
		FROM parks
		WHERE latitude IS NOT NULL AND longitude IS NOT NULL
		ORDER BY id
	`)
	if err != nil {
		return nil, fmt.Errorf("querying weather parks: %w", err)
	}
	defer rows.Close()

	var out []WeatherPark
	for rows.Next() {
		var wp WeatherPark
		if err := rows.Scan(&wp.ParkID, &wp.Lat, &wp.Lon); err != nil {
			return nil, fmt.Errorf("scanning weather park: %w", err)
		}
		out = append(out, wp)
	}
	return out, rows.Err()
}

// InsertWeatherObservations inserts current-conditions rows idempotently, ensuring the
// required monthly partitions (by timestamp) first. It uses batched INSERT ... ON
// CONFLICT DO NOTHING rather than COPY: `timestamp` is the upstream observation time
// (current.dt), which only advances every few minutes, so consecutive polls can re-read
// the same value — a duplicate that COPY would abort on. DO NOTHING makes a repeated
// read a no-op (one row per distinct upstream observation). Returns rows inserted.
func (s *Store) InsertWeatherObservations(ctx context.Context, obs []WeatherObservation) (int, error) {
	if len(obs) == 0 {
		return 0, nil
	}
	for _, o := range obs {
		if err := s.ensureWeatherObsPartition(ctx, o.Timestamp); err != nil {
			return 0, err
		}
	}

	batch := &pgx.Batch{}
	for _, o := range obs {
		batch.Queue(`
			INSERT INTO weather_observations
				(park_id, timestamp, temp_c, feels_like_c, precip_mm,
				 humidity, wind_speed_ms, cloud_pct, weather_code, weather_main,
				 source_observation_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			ON CONFLICT (park_id, timestamp) DO NOTHING
		`, o.ParkID, o.Timestamp, o.TempC, o.FeelsLikeC, o.PrecipMM,
			o.Humidity, o.WindSpeedMS, o.CloudPct, o.WeatherCode, o.WeatherMain,
			o.SourceObservationID)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	inserted := 0
	for range obs {
		ct, err := br.Exec()
		if err != nil {
			return inserted, fmt.Errorf("inserting weather observation: %w", err)
		}
		inserted += int(ct.RowsAffected())
	}
	return inserted, nil
}

// InsertWeatherForecasts inserts forecast rows idempotently, ensuring the required
// monthly partitions (by issued_at) first. Batched INSERT ... ON CONFLICT DO NOTHING
// (consistent with observations); each poll's issued_at makes rows distinct, and DO
// NOTHING guards against a re-run of the same poll. Returns rows inserted.
func (s *Store) InsertWeatherForecasts(ctx context.Context, fcs []WeatherForecast) (int, error) {
	if len(fcs) == 0 {
		return 0, nil
	}
	for _, f := range fcs {
		if err := s.ensureWeatherForecastPartition(ctx, f.IssuedAt); err != nil {
			return 0, err
		}
	}

	batch := &pgx.Batch{}
	for _, f := range fcs {
		batch.Queue(`
			INSERT INTO weather_forecasts
				(park_id, issued_at, valid_at, horizon_minutes,
				 temp_c, precip_mm, precip_prob, weather_code, weather_main,
				 source_observation_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (park_id, issued_at, valid_at) DO NOTHING
		`, f.ParkID, f.IssuedAt, f.ValidAt, f.HorizonMinutes,
			f.TempC, f.PrecipMM, f.PrecipProb, f.WeatherCode, f.WeatherMain,
			f.SourceObservationID)
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	inserted := 0
	for range fcs {
		ct, err := br.Exec()
		if err != nil {
			return inserted, fmt.Errorf("inserting weather forecast: %w", err)
		}
		inserted += int(ct.RowsAffected())
	}
	return inserted, nil
}

// ensureWeatherObsPartition creates a monthly weather_observations partition (by
// timestamp) if it doesn't already exist. Mirrors ensurePartition; uses the "wxo_"
// cache-key prefix to avoid colliding with other tables in the shared partition map.
func (s *Store) ensureWeatherObsPartition(ctx context.Context, t time.Time) error {
	return s.ensureMonthlyPartition(ctx, "weather_observations", "wxo_", t)
}

// ensureWeatherForecastPartition creates a monthly weather_forecasts partition (by
// issued_at) if it doesn't already exist. Uses the "wxf_" cache-key prefix.
func (s *Store) ensureWeatherForecastPartition(ctx context.Context, t time.Time) error {
	return s.ensureMonthlyPartition(ctx, "weather_forecasts", "wxf_", t)
}

// ensureMonthlyPartition is the shared helper behind the weather partition managers:
// it creates `<table>_<year>_<month>` covering t's month if absent, memoizing on
// prefix+key so each table gets an independent cache namespace in s.partitions.
func (s *Store) ensureMonthlyPartition(ctx context.Context, table, cachePrefix string, t time.Time) error {
	year, month, _ := t.UTC().Date()
	key := fmt.Sprintf("%s%d_%02d", cachePrefix, year, month)
	if _, ok := s.partitions.Load(key); ok {
		return nil
	}

	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	partName := fmt.Sprintf("%s_%d_%02d", table, year, month)

	sql := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s
		PARTITION OF %s
		FOR VALUES FROM ('%s') TO ('%s')
	`, partName, table, start.Format(time.DateOnly), end.Format(time.DateOnly))

	if _, err := s.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("creating partition %s: %w", partName, err)
	}
	slog.Info("ensured partition", "name", partName)
	s.partitions.Store(key, true)
	return nil
}
