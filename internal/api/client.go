package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const baseURL = "https://api.themeparks.wiki/v1"

// Client is a rate-limited HTTP client for the themeparks.wiki API.
type Client struct {
	http *http.Client
	sem  chan struct{}
}

// NewClient creates a new API client with the given concurrency limit and per-request timeout.
func NewClient(concurrency int, timeout time.Duration) *Client {
	return &Client{
		http: &http.Client{Timeout: timeout},
		sem:  make(chan struct{}, concurrency),
	}
}

func (c *Client) acquire(ctx context.Context) error {
	select {
	case c.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) release() {
	<-c.sem
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()

	url := baseURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request for %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("fetching %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d for %s", resp.StatusCode, path)
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decoding response from %s: %w", path, err)
	}
	return nil
}

// GetDestinations returns all destinations and their parks.
func (c *Client) GetDestinations(ctx context.Context) (*DestinationsResponse, error) {
	slog.Debug("fetching destinations")
	var resp DestinationsResponse
	if err := c.get(ctx, "/destinations", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetEntityChildren returns all child entities for the given park/entity ID.
func (c *Client) GetEntityChildren(ctx context.Context, entityID string) (*EntityChildrenResponse, error) {
	slog.Debug("fetching children", "entity_id", entityID)
	var resp EntityChildrenResponse
	if err := c.get(ctx, "/entity/"+entityID+"/children", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetEntityLive returns live queue/status data for all entities in the given park.
func (c *Client) GetEntityLive(ctx context.Context, entityID string) (*EntityLiveResponse, error) {
	slog.Debug("fetching live data", "entity_id", entityID)
	var resp EntityLiveResponse
	if err := c.get(ctx, "/entity/"+entityID+"/live", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// LiveResult carries the decoded live response plus the verbatim response body
// and delivery metadata needed to persist a raw_observations row (invariant I1).
type LiveResult struct {
	Response   *EntityLiveResponse // nil on non-200 or parse error
	Raw        json.RawMessage     // verbatim response body
	HTTPStatus int
	LatencyMS  int64
}

// GetEntityLiveWithRaw fetches live data and returns the verbatim body alongside
// the decoded response. Unlike GetEntityLive it does NOT discard the bytes, so the
// caller can persist them for full replayability. It returns a non-nil *LiveResult
// even on non-200 or parse error (with Response == nil) so the caller can record a
// failed-fetch observation as a signal.
func (c *Client) GetEntityLiveWithRaw(ctx context.Context, entityID string) (*LiveResult, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()

	path := "/entity/" + entityID + "/live"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request for %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	res := &LiveResult{
		Raw:        json.RawMessage(body),
		HTTPStatus: resp.StatusCode,
		LatencyMS:  time.Since(start).Milliseconds(),
	}
	if readErr != nil {
		return res, fmt.Errorf("reading response from %s: %w", path, readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return res, fmt.Errorf("unexpected status %d for %s", resp.StatusCode, path)
	}

	var decoded EntityLiveResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return res, fmt.Errorf("decoding response from %s: %w", path, err)
	}
	res.Response = &decoded
	return res, nil
}

// GetEntitySchedule returns schedule data for the given entity (typically a park).
func (c *Client) GetEntitySchedule(ctx context.Context, entityID string) (*EntityScheduleResponse, error) {
	slog.Debug("fetching schedule", "entity_id", entityID)
	var resp EntityScheduleResponse
	if err := c.get(ctx, "/entity/"+entityID+"/schedule", &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
