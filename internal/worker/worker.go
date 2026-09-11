// Package worker implements a client that claims tasks from the scheduler,
// renews the lease while executing, and completes them.
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"durablesched/internal/sched"
)

// Client talks to a scheduler over HTTP.
type Client struct {
	BaseURL string
	ID      string
	HTTP    *http.Client
}

// NewClient creates a worker client.
func NewClient(baseURL, id string) *Client {
	return &Client{BaseURL: baseURL, ID: id, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) post(path string, body any) (*http.Response, error) {
	b, _ := json.Marshal(body)
	return c.HTTP.Post(c.BaseURL+path, "application/json", bytes.NewReader(b))
}

// Claim tries to lease one task; returns nil, nil if none available.
func (c *Client) Claim(lease time.Duration) (*sched.Task, error) {
	resp, err := c.post("/tasks/claim", map[string]any{
		"worker_id": c.ID, "lease_ms": lease.Milliseconds(),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("claim: %s: %s", resp.Status, b)
	}
	var t sched.Task
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// Renew extends the lease for a claimed task.
func (c *Client) Renew(taskID string, token uint64, lease time.Duration) error {
	resp, err := c.post("/tasks/"+taskID+"/renew", map[string]any{
		"worker_id": c.ID, "token": token, "lease_ms": lease.Milliseconds(),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("renew: %s: %s", resp.Status, b)
	}
	return nil
}

// Complete submits the result for a claimed task.
func (c *Client) Complete(taskID string, token uint64, result string) error {
	resp, err := c.post("/tasks/"+taskID+"/complete", map[string]any{
		"worker_id": c.ID, "token": token, "result": result,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("complete: %s: %s", resp.Status, b)
	}
	return nil
}

// GetTask fetches a task's status.
func (c *Client) GetTask(id string) (*sched.Task, error) {
	resp, err := c.HTTP.Get(c.BaseURL + "/tasks/" + id)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get: %s: %s", resp.Status, b)
	}
	var t sched.Task
	if err := json.NewDecoder(resp.Body).Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}

// Execute runs fn on the task payload while renewing the lease in the
// background, then completes the task. If the lease is lost (renew or
// complete rejected), it returns an error and the result is discarded.
func (c *Client) Execute(ctx context.Context, t *sched.Task, lease time.Duration, fn func(payload string) string) error {
	renewCtx, stop := context.WithCancel(ctx)
	defer stop()
	lost := make(chan error, 1)
	go func() {
		tick := time.NewTicker(lease / 3)
		defer tick.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-tick.C:
				if err := c.Renew(t.ID, t.Token, lease); err != nil {
					lost <- err
					return
				}
			}
		}
	}()
	resultCh := make(chan string, 1)
	go func() { resultCh <- fn(t.Payload) }()
	select {
	case err := <-lost:
		return fmt.Errorf("lease lost: %w", err)
	case result := <-resultCh:
		stop()
		return c.Complete(t.ID, t.Token, result)
	}
}
