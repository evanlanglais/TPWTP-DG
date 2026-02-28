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
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:    os.Getenv("DATABASE_URL"),
		PollInterval:   5 * time.Minute,
		APIConcurrency: 15,
		APITimeout:     10 * time.Second,
		LogLevel:       "info",
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

	return cfg, nil
}
