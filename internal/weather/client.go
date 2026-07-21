package weather

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const baseURL = "https://api.openweathermap.org/data/3.0/onecall"

// Client is a rate-limited HTTP client for the OpenWeatherMap One Call 3.0 API.
// It mirrors internal/api.Client (buffered-channel semaphore + per-request timeout)
// but targets a different host and carries the API key.
type Client struct {
	http   *http.Client
	sem    chan struct{}
	apiKey string
}

// NewClient creates a weather client with the given concurrency limit and per-request
// timeout. apiKey is required by OWM; presence of a key is what enables weather polling.
func NewClient(apiKey string, concurrency int, timeout time.Duration) *Client {
	return &Client{
		http:   &http.Client{Timeout: timeout},
		sem:    make(chan struct{}, concurrency),
		apiKey: apiKey,
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

// Result carries the decoded response plus the verbatim body and delivery metadata
// needed to persist a raw_observations row (invariant I1).
type Result struct {
	Response   *OneCallResponse // nil on non-200 or parse error
	Raw        json.RawMessage  // verbatim response body
	HTTPStatus int
	LatencyMS  int64
}

// FetchWithRaw fetches current + hourly weather for a coordinate and returns the
// verbatim body alongside the decoded response. Like api.GetEntityLiveWithRaw it does
// NOT discard the bytes (so the caller can persist them for full replayability), and it
// returns a non-nil *Result even on non-200 or parse error (Response == nil) so the
// caller can record a failed-fetch observation as a signal.
func (c *Client) FetchWithRaw(ctx context.Context, lat, lon float64) (*Result, error) {
	if err := c.acquire(ctx); err != nil {
		return nil, err
	}
	defer c.release()

	q := url.Values{}
	q.Set("lat", strconv.FormatFloat(lat, 'f', -1, 64))
	q.Set("lon", strconv.FormatFloat(lon, 'f', -1, 64))
	q.Set("units", "metric")
	q.Set("exclude", "minutely,daily,alerts")
	q.Set("appid", c.apiKey)
	reqURL := baseURL + "?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("creating weather request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching weather: %w", err)
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	res := &Result{
		Raw:        json.RawMessage(body),
		HTTPStatus: resp.StatusCode,
		LatencyMS:  time.Since(start).Milliseconds(),
	}
	if readErr != nil {
		return res, fmt.Errorf("reading weather response: %w", readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return res, fmt.Errorf("unexpected weather status %d", resp.StatusCode)
	}

	var decoded OneCallResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return res, fmt.Errorf("decoding weather response: %w", err)
	}
	res.Response = &decoded
	return res, nil
}
