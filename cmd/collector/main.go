package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/evanlanglais/tpwtp-dg/internal/api"
	"github.com/evanlanglais/tpwtp-dg/internal/collector"
	"github.com/evanlanglais/tpwtp-dg/internal/config"
	"github.com/evanlanglais/tpwtp-dg/internal/store"
	"github.com/evanlanglais/tpwtp-dg/migrations"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Set up structured logging
	var level slog.Level
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	// Context with signal handling for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Connect to PostgreSQL
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	slog.Info("connected to database")

	// Run migrations
	if err := migrations.Run(ctx, pool); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}
	slog.Info("migrations complete")

	// Initialize components
	client := api.NewClient(cfg.APIConcurrency, cfg.APITimeout)
	db := store.New(pool)
	coll := collector.New(client, db, cfg.PollInterval)

	slog.Info("starting collector",
		"poll_interval", cfg.PollInterval,
		"api_concurrency", cfg.APIConcurrency,
	)

	if err := coll.Run(ctx); err != nil {
		slog.Error("collector error", "error", err)
		os.Exit(1)
	}

	slog.Info("shutdown complete")
}
