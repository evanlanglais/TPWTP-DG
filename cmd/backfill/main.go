// Command backfill is a one-shot, resumable, out-of-band tool that adopts already
// collected wait_times rows into raw_observations as a reduced-fidelity legacy
// source (invariant I1: preserve prior data for replay). It only READS wait_times
// and only WRITES raw_observations — the originals are never modified. Safe to run
// against a live database and safe to re-run (idempotent via deterministic IDs).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/evanlanglais/tpwtp-dg/internal/config"
	"github.com/evanlanglais/tpwtp-dg/internal/store"
	"github.com/evanlanglais/tpwtp-dg/migrations"
)

const (
	backfillMetric   = "backfill_legacy_raw_observations"
	backfillVersion  = 1
	backfillChunk    = 1 * time.Hour
	backfillThrottle = 50 * time.Millisecond
	legacySourceType = "legacy_wait_times_projection"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

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

	// Ensure raw_observations exists (migration 006).
	if err := migrations.Run(ctx, pool); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	if err := run(ctx, store.New(pool)); err != nil {
		slog.Error("backfill failed", "error", err)
		os.Exit(1)
	}
	slog.Info("legacy raw-observation backfill complete")
}

// run walks wait_times forward in fixed windows, adopting each window into
// raw_observations and advancing a cursor after every window so a restart resumes.
func run(ctx context.Context, db *store.Store) error {
	since, _, err := db.GetProcessingCursor(ctx, backfillMetric)
	if err != nil {
		return err
	}
	if since.IsZero() {
		earliest, err := db.GetEarliestWaitTime(ctx)
		if err != nil {
			return err
		}
		if earliest.IsZero() {
			slog.Info("no wait_times to adopt")
			return nil
		}
		since = earliest.Add(-time.Second)
	}

	end := time.Now().UTC()
	var seen, adopted int

	for since.Before(end) {
		if ctx.Err() != nil {
			slog.Info("interrupted; progress saved", "cursor", since.Format(time.RFC3339))
			return nil
		}

		chunkEnd := since.Add(backfillChunk)
		if chunkEnd.After(end) {
			chunkEnd = end
		}

		wts, err := db.ReadWaitTimesWindow(ctx, since, chunkEnd)
		if err != nil {
			return err
		}
		if len(wts) > 0 {
			obs := make([]store.RawObservation, 0, len(wts))
			for _, wt := range wts {
				o, err := toRawObservation(wt)
				if err != nil {
					return err
				}
				obs = append(obs, o)
			}
			n, err := db.InsertRawObservationBatch(ctx, obs)
			if err != nil {
				return err
			}
			seen += len(wts)
			adopted += n
		}

		since = chunkEnd
		if err := db.SetProcessingCursor(ctx, backfillMetric, since, backfillVersion); err != nil {
			return err
		}
		slog.Info("adopted window",
			"through", since.Format(time.RFC3339),
			"rows_seen", seen,
			"rows_inserted", adopted,
		)
		time.Sleep(backfillThrottle)
	}
	return nil
}

// toRawObservation reconstructs a reduced-fidelity raw observation from a projected
// wait_times row. observed_at == received_at (no upstream time was retained), and the
// metadata records exactly what fidelity was lost.
func toRawObservation(wt store.WaitTime) (store.RawObservation, error) {
	queue := map[string]any{}
	if wt.StandbyWait != nil {
		queue["STANDBY"] = map[string]any{"waitTime": *wt.StandbyWait}
	}
	if wt.SingleRiderWait != nil {
		queue["SINGLE_RIDER"] = map[string]any{"waitTime": *wt.SingleRiderWait}
	}
	if wt.ReturnTimeState != nil {
		rt := map[string]any{"state": *wt.ReturnTimeState, "kind": "unknown"}
		if wt.ReturnTimeStart != nil {
			rt["returnStart"] = wt.ReturnTimeStart.Format(time.RFC3339)
		}
		if wt.ReturnTimeEnd != nil {
			rt["returnEnd"] = wt.ReturnTimeEnd.Format(time.RFC3339)
		}
		queue["RETURN_TIME"] = rt
	}

	payload := map[string]any{
		"_reconstructed": true,
		"id":             wt.AttractionID,
		"status":         wt.Status,
	}
	if len(queue) > 0 {
		payload["queue"] = queue
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return store.RawObservation{}, err
	}

	meta := map[string]any{
		"legacy":        true,
		"reconstructed": true,
		"fidelity":      "reduced",
		"attraction_id": wt.AttractionID,
		"known_gaps":    []string{"no_observed_at_fidelity", "return_time_free_paid_collapsed", "no_full_queue_object"},
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return store.RawObservation{}, err
	}

	return store.RawObservation{
		ObservationID: deterministicUUID(wt.AttractionID + "|" + wt.Timestamp.UTC().Format(time.RFC3339Nano)),
		SourceType:    legacySourceType,
		ObservedAt:    wt.Timestamp,
		ReceivedAt:    wt.Timestamp,
		Payload:       payloadJSON,
		Metadata:      metaJSON,
	}, nil
}

// deterministicUUID derives a stable RFC-4122 v5-style UUID string from a key, so
// re-running the backfill collides on the primary key (ON CONFLICT DO NOTHING).
func deterministicUUID(key string) string {
	sum := sha256.Sum256([]byte(key))
	var b [16]byte
	copy(b[:], sum[:16])
	b[6] = (b[6] & 0x0f) | 0x50 // version 5 (name-based)
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
