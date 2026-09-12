// HTTP API for the scheduler. All bodies are JSON.
//
//	POST   /tasks            {idempotency_key, payload}      -> 201 task | 200 task (dup)
//	POST   /tasks/claim      {worker_id, lease_ms}           -> 200 task | 204 none
//	POST   /tasks/{id}/renew {worker_id, token, lease_ms}    -> 200 task | 404 | 409
//	POST   /tasks/{id}/complete {worker_id, token, result}   -> 200 task | 404 | 409
//	GET    /tasks/{id}                                       -> 200 task | 404
//	GET    /tasks                                            -> 200 [task]
package sched

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// Handler returns an http.Handler serving the scheduler API.
func (c *Core) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/tasks", c.handleTasks)
	mux.HandleFunc("/tasks/", c.handleTaskByID)
	return mux
}

type errBody struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, errBody{err.Error()})
	case errors.Is(err, ErrFencing), errors.Is(err, ErrNotLeaseHolder),
		errors.Is(err, ErrAlreadyDone), errors.Is(err, ErrLeaseExpired):
		writeJSON(w, http.StatusConflict, errBody{err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, errBody{err.Error()})
	}
}

func (c *Core) handleTasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			IdempotencyKey string `json:"idempotency_key"`
			Payload        string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody{"bad json: " + err.Error()})
			return
		}
		t, created, err := c.Create(req.IdempotencyKey, req.Payload)
		if err != nil {
			writeErr(w, err)
			return
		}
		if created {
			writeJSON(w, http.StatusCreated, t)
		} else {
			writeJSON(w, http.StatusOK, t)
		}
	case http.MethodGet:
		writeJSON(w, http.StatusOK, c.List())
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (c *Core) handleTaskByID(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/tasks/")
	if rest == "claim" && r.Method == http.MethodPost {
		c.handleClaim(w, r)
		return
	}
	parts := strings.Split(rest, "/")
	id := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		t, err := c.Get(id)
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, t)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "renew":
			c.handleRenew(w, r, id)
			return
		case "complete":
			c.handleComplete(w, r, id)
			return
		}
	}
	w.WriteHeader(http.StatusNotFound)
}

func (c *Core) handleClaim(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
		LeaseMs  int64  `json:"lease_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{"bad json: " + err.Error()})
		return
	}
	if req.WorkerID == "" || req.LeaseMs <= 0 {
		writeJSON(w, http.StatusBadRequest, errBody{"worker_id and positive lease_ms required"})
		return
	}
	t, err := c.Claim(req.WorkerID, time.Duration(req.LeaseMs)*time.Millisecond)
	if errors.Is(err, ErrNoTask) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (c *Core) handleRenew(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		WorkerID string `json:"worker_id"`
		Token    uint64 `json:"token"`
		LeaseMs  int64  `json:"lease_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{"bad json: " + err.Error()})
		return
	}
	t, err := c.Renew(id, req.WorkerID, req.Token, time.Duration(req.LeaseMs)*time.Millisecond)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (c *Core) handleComplete(w http.ResponseWriter, r *http.Request, id string) {
	var req struct {
		WorkerID string `json:"worker_id"`
		Token    uint64 `json:"token"`
		Result   string `json:"result"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody{"bad json: " + err.Error()})
		return
	}
	t, err := c.Complete(id, req.WorkerID, req.Token, req.Result)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}
