package config

import (
	"fmt"
	"os"
	"time"
)

type Config struct {
	DatabaseURL    string
	PollInterval   time.Duration
	APIConcurrency int
	APITimeout     time.Duration
	LogLevel       string

	// WeatherAPIKey enables the weather source when set (OpenWeatherMap One Call 3.0).
	// Empty = weather polling disabled; existing deployments are unaffected.
	WeatherAPIKey string
	// WeatherPollInterval is the (slower) cadence for weather polling.
	WeatherPollInterval time.Duration
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		PollInterval:        5 * time.Minute,
		APIConcurrency:      15,
		APITimeout:          10 * time.Second,
		LogLevel:            "info",
		WeatherAPIKey:       os.Getenv("WEATHER_API_KEY"),
		WeatherPollInterval: 30 * time.Minute,
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid POLL_INTERVAL %q: %w", v, err)
		}
		cfg.PollInterval = d
	}

	if v := os.Getenv("API_CONCURRENCY"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err != nil || n < 1 {
			return nil, fmt.Errorf("invalid API_CONCURRENCY %q", v)
		}
		cfg.APIConcurrency = n
	}

	if v := os.Getenv("API_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid API_TIMEOUT %q: %w", v, err)
		}
		cfg.APITimeout = d
	}

	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}

	if v := os.Getenv("WEATHER_POLL_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid WEATHER_POLL_INTERVAL %q: %w", v, err)
		}
		cfg.WeatherPollInterval = d
	}

	return cfg, nil
}
