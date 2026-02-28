package api

import (
	"context"
	"encoding/json"
	"fmt"
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
