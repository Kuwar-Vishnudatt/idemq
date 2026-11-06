// Package client is the reference worker SDK: a thin HTTP client plus a
// Worker loop that claims a job, runs a user-supplied handler, keeps the
// lease alive with background heartbeats while the handler runs, and
// acks/nacks based on the result. Any process that links this package (or
// just speaks the same HTTP protocol in another language) is a valid,
// independent worker — that's the distribution model: N worker processes,
// one durable broker, safety enforced by server-side fencing tokens rather
// than by trusting workers to behave.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

var ErrFenced = errors.New("client: stale lease (job reclaimed or already finalized)")
var ErrNoJob = errors.New("client: no job available")

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

type Job struct {
	ID          string          `json:"id"`
	Queue       string          `json:"queue"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LeaseToken  int64           `json:"lease_token"`
}

type EnqueueOpts struct {
	IdempotencyKey string
	Delay          time.Duration
	MaxAttempts    int
	Priority       int
}

func (c *Client) Enqueue(ctx context.Context, queue string, payload any, idempotencyKey string) (id string, duplicate bool, err error) {
	return c.EnqueueWithOpts(ctx, queue, payload, EnqueueOpts{IdempotencyKey: idempotencyKey})
}

func (c *Client) EnqueueWithOpts(ctx context.Context, queue string, payload any, opts EnqueueOpts) (id string, duplicate bool, err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", false, err
	}
	req := map[string]any{
		"payload":         json.RawMessage(body),
		"idempotency_key": opts.IdempotencyKey,
		"delay_ms":        opts.Delay.Milliseconds(),
		"max_attempts":    opts.MaxAttempts,
		"priority":        opts.Priority,
	}
	var resp struct {
		ID        string `json:"id"`
		Duplicate bool   `json:"duplicate"`
	}
	if err := c.do(ctx, "POST", fmt.Sprintf("/v1/queues/%s/enqueue", queue), req, http.StatusCreated, &resp); err != nil {
		return "", false, err
	}
	return resp.ID, resp.Duplicate, nil
}

func (c *Client) ListDead(ctx context.Context, queue string) ([]Job, error) {
	var jobs []Job
	err := c.do(ctx, "GET", fmt.Sprintf("/v1/queues/%s/dlq", queue), nil, http.StatusOK, &jobs)
	return jobs, err
}

func (c *Client) RequeueDead(ctx context.Context, jobID string) error {
	return c.do(ctx, "POST", fmt.Sprintf("/v1/jobs/%s/requeue", jobID), nil, http.StatusOK, nil)
}

type Stats struct {
	Pending   int `json:"Pending"`
	InFlight  int `json:"InFlight"`
	Completed int `json:"Completed"`
	Dead      int `json:"Dead"`
}

func (c *Client) Stats(ctx context.Context, queue string) (Stats, error) {
	var s Stats
	err := c.do(ctx, "GET", fmt.Sprintf("/v1/queues/%s/stats", queue), nil, http.StatusOK, &s)
	return s, err
}

func (c *Client) Claim(ctx context.Context, queue string, lease time.Duration) (*Job, error) {
	req := map[string]any{"lease_ms": lease.Milliseconds()}
	var job Job
	err := c.doRaw(ctx, "POST", fmt.Sprintf("/v1/queues/%s/claim", queue), req, func(status int, body []byte) error {
		if status == http.StatusNoContent {
			return ErrNoJob
		}
		if status != http.StatusOK {
			return httpError(status, body)
		}
		return json.Unmarshal(body, &job)
	})
	if err != nil {
		return nil, err
	}
	return &job, nil
}

func (c *Client) Heartbeat(ctx context.Context, jobID string, token int64, extend time.Duration) error {
	req := map[string]any{"token": token, "extend_ms": extend.Milliseconds()}
	return c.leaseCall(ctx, fmt.Sprintf("/v1/jobs/%s/heartbeat", jobID), req)
}

func (c *Client) Ack(ctx context.Context, jobID string, token int64) error {
	return c.leaseCall(ctx, fmt.Sprintf("/v1/jobs/%s/ack", jobID), map[string]any{"token": token})
}

func (c *Client) Nack(ctx context.Context, jobID string, token int64, errMsg string, delay time.Duration) error {
	req := map[string]any{"token": token, "error": errMsg, "delay_ms": delay.Milliseconds()}
	return c.leaseCall(ctx, fmt.Sprintf("/v1/jobs/%s/nack", jobID), req)
}

func (c *Client) leaseCall(ctx context.Context, path string, req any) error {
	return c.doRaw(ctx, "POST", path, req, func(status int, body []byte) error {
		switch status {
		case http.StatusOK:
			return nil
		case http.StatusConflict:
			return ErrFenced
		default:
			return httpError(status, body)
		}
	})
}

func (c *Client) do(ctx context.Context, method, path string, req any, wantStatus int, out any) error {
	return c.doRaw(ctx, method, path, req, func(status int, body []byte) error {
		if status != wantStatus {
			return httpError(status, body)
		}
		if out != nil {
			return json.Unmarshal(body, out)
		}
		return nil
	})
}

func (c *Client) doRaw(ctx context.Context, method, path string, req any, handle func(status int, body []byte) error) error {
	var buf bytes.Buffer
	if req != nil {
		if err := json.NewEncoder(&buf).Encode(req); err != nil {
			return err
		}
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, &buf)
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return handle(resp.StatusCode, body)
}

func httpError(status int, body []byte) error {
	return fmt.Errorf("client: unexpected status %d: %s", status, string(body))
}

// Handler processes one job's payload. Returning an error nacks the job
// (triggering retry-with-backoff or, past max_attempts, dead-lettering);
// returning nil acks it.
type Handler func(ctx context.Context, job *Job) error

// WorkerConfig controls polling cadence, lease length, and retry backoff.
type WorkerConfig struct {
	Queue          string
	Lease          time.Duration // how long a claim is held before it must be heartbeated
	HeartbeatEvery time.Duration // must be comfortably shorter than Lease
	PollInterval   time.Duration // how often to ask for work when the queue is empty
	RetryDelay     func(attempt int) time.Duration
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = c.Lease / 3
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.RetryDelay == nil {
		c.RetryDelay = func(attempt int) time.Duration { return time.Duration(attempt) * time.Second }
	}
	return c
}

// RunWorker polls for jobs and processes them with handler until ctx is
// cancelled. Each job runs synchronously in this goroutine with a background
// heartbeat keeping its lease alive; run multiple RunWorker calls (in
// goroutines, processes, or separate machines) for concurrency.
func RunWorker(ctx context.Context, c *Client, cfg WorkerConfig, handler Handler) {
	cfg = cfg.withDefaults()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := c.Claim(ctx, cfg.Queue, cfg.Lease)
		if errors.Is(err, ErrNoJob) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.PollInterval):
			}
			continue
		}
		if err != nil {
			log.Printf("worker: claim error: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.PollInterval):
			}
			continue
		}

		processJob(ctx, c, cfg, job, handler)
	}
}

func processJob(ctx context.Context, c *Client, cfg WorkerConfig, job *Job, handler Handler) {
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go func() {
		ticker := time.NewTicker(cfg.HeartbeatEvery)
		defer ticker.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if err := c.Heartbeat(hbCtx, job.ID, job.LeaseToken, cfg.Lease); err != nil {
					log.Printf("worker: heartbeat failed for job %s: %v", job.ID, err)
					return
				}
			}
		}
	}()

	err := handler(ctx, job)
	stopHeartbeat()

	if err == nil {
		if ackErr := c.Ack(ctx, job.ID, job.LeaseToken); ackErr != nil && !errors.Is(ackErr, ErrFenced) {
			log.Printf("worker: ack failed for job %s: %v", job.ID, ackErr)
		}
		return
	}

	delay := cfg.RetryDelay(job.Attempts)
	if nackErr := c.Nack(ctx, job.ID, job.LeaseToken, err.Error(), delay); nackErr != nil && !errors.Is(nackErr, ErrFenced) {
		log.Printf("worker: nack failed for job %s: %v", job.ID, nackErr)
	}
}
