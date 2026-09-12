// Package sched implements the scheduler core: task state machine,
// lease management with fencing tokens, idempotency, and durable
// persistence on top of the append-only store.
package sched

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"durablesched/internal/store"
)

// Task states.
const (
	StatePending   = "PENDING"
	StateLeased    = "LEASED"
	StateCompleted = "COMPLETED"
)

// Error codes returned by the scheduler and surfaced over HTTP.
var (
	ErrNotFound       = errors.New("task not found")
	ErrNoTask         = errors.New("no task available")
	ErrFencing        = errors.New("stale fencing token: lease held by a newer execution")
	ErrNotLeaseHolder = errors.New("caller does not hold the current lease")
	ErrLeaseExpired   = errors.New("lease has expired: execution eligibility is lost")
	ErrAlreadyDone    = errors.New("task already completed with a different result")
)

// Task is the scheduler's view of one unit of work.
type Task struct {
	ID             string `json:"id"`
	IdempotencyKey string `json:"idempotency_key"`
	Payload        string `json:"payload"`
	State          string `json:"state"`
	// Token is the fencing token of the current lease; it increases on
	// every successful claim. Zero means no lease has ever been issued.
	Token       uint64 `json:"token"`
	LeaseOwner  string `json:"lease_owner,omitempty"`
	LeaseExpiry int64  `json:"lease_expiry,omitempty"` // unix nano
	Result      string `json:"result,omitempty"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

// Log record types.
const (
	recCreate   = "create"
	recClaim    = "claim"
	recRenew    = "renew"
	recComplete = "complete"
)

type recEnvelope struct {
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

type createRec struct {
	Task Task `json:"task"`
}

type claimRec struct {
	ID        string `json:"id"`
	Token     uint64 `json:"token"`
	Owner     string `json:"owner"`
	Expiry    int64  `json:"expiry"`
	AppliedAt int64  `json:"applied_at"`
}

type renewRec struct {
	ID        string `json:"id"`
	Token     uint64 `json:"token"`
	Expiry    int64  `json:"expiry"`
	AppliedAt int64  `json:"applied_at"`
}

type completeRec struct {
	ID        string `json:"id"`
	Token     uint64 `json:"token"`
	Owner     string `json:"owner"`
	Result    string `json:"result"`
	AppliedAt int64  `json:"applied_at"`
}

// snapshotData is the serialized in-memory state.
type snapshotData struct {
	Tasks     map[string]*Task `json:"tasks"`
	Idem      map[string]string `json:"idem"`
	NextToken uint64            `json:"next_token"`
}

// Core is the scheduler state machine. All mutations are appended to the
// store and fsynced before being acknowledged.
type Core struct {
	mu        sync.Mutex
	st        *store.Store
	tasks     map[string]*Task
	idem      map[string]string // idempotency key -> task id
	nextToken uint64
	now       func() time.Time
	// CompactThreshold triggers compaction when the log exceeds this size.
	CompactThreshold int64
}

// Open loads (or initializes) a scheduler core rooted at dir.
func Open(dir string) (*Core, error) {
	st, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	c := &Core{
		st:               st,
		tasks:            map[string]*Task{},
		idem:             map[string]string{},
		nextToken:        1,
		now:              time.Now,
		CompactThreshold: 1 << 20,
	}
	if err := c.recover(); err != nil {
		st.Close()
		return nil, err
	}
	return c, nil
}

func (c *Core) recover() error {
	snap, recs, err := c.st.Replay()
	if err != nil {
		return err
	}
	if snap != nil {
		var sd snapshotData
		if err := json.Unmarshal(snap.Data, &sd); err != nil {
			return fmt.Errorf("%w: snapshot decode: %v", store.ErrCorrupt, err)
		}
		c.tasks = sd.Tasks
		c.idem = sd.Idem
		c.nextToken = sd.NextToken
		if c.tasks == nil {
			c.tasks = map[string]*Task{}
		}
		if c.idem == nil {
			c.idem = map[string]string{}
		}
	}
	for _, r := range recs {
		if err := c.apply(r.Payload); err != nil {
			return err
		}
	}
	return nil
}

// apply replays one committed record into memory.
func (c *Core) apply(payload json.RawMessage) error {
	var env recEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return fmt.Errorf("%w: envelope: %v", store.ErrCorrupt, err)
	}
	switch env.Type {
	case recCreate:
		var r createRec
		if err := json.Unmarshal(env.Body, &r); err != nil {
			return fmt.Errorf("%w: create: %v", store.ErrCorrupt, err)
		}
		t := r.Task
		c.tasks[t.ID] = &t
		if t.IdempotencyKey != "" {
			c.idem[t.IdempotencyKey] = t.ID
		}
	case recClaim:
		var r claimRec
		if err := json.Unmarshal(env.Body, &r); err != nil {
			return fmt.Errorf("%w: claim: %v", store.ErrCorrupt, err)
		}
		if t, ok := c.tasks[r.ID]; ok {
			t.State = StateLeased
			t.Token = r.Token
			t.LeaseOwner = r.Owner
			t.LeaseExpiry = r.Expiry
			t.UpdatedAt = r.AppliedAt
		}
		if r.Token >= c.nextToken {
			c.nextToken = r.Token + 1
		}
	case recRenew:
		var r renewRec
		if err := json.Unmarshal(env.Body, &r); err != nil {
			return fmt.Errorf("%w: renew: %v", store.ErrCorrupt, err)
		}
		if t, ok := c.tasks[r.ID]; ok && t.Token == r.Token {
			t.LeaseExpiry = r.Expiry
			t.UpdatedAt = r.AppliedAt
		}
	case recComplete:
		var r completeRec
		if err := json.Unmarshal(env.Body, &r); err != nil {
			return fmt.Errorf("%w: complete: %v", store.ErrCorrupt, err)
		}
		if t, ok := c.tasks[r.ID]; ok {
			t.State = StateCompleted
			t.Result = r.Result
			t.LeaseOwner = ""
			t.LeaseExpiry = 0
			t.UpdatedAt = r.AppliedAt
		}
	default:
		return fmt.Errorf("%w: unknown record type %q", store.ErrCorrupt, env.Type)
	}
	return nil
}

// commit appends a record durably, then applies it in memory.
// Caller must hold c.mu.
func (c *Core) commit(recType string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(recEnvelope{Type: recType, Body: b})
	if err != nil {
		return err
	}
	if _, err := c.st.Append(payload); err != nil {
		return err
	}
	if err := c.apply(payload); err != nil {
		return err
	}
	return c.maybeCompactLocked()
}

func (c *Core) maybeCompactLocked() error {
	if c.st.Size() < c.CompactThreshold {
		return nil
	}
	sd := snapshotData{Tasks: c.tasks, Idem: c.idem, NextToken: c.nextToken}
	data, err := json.Marshal(sd)
	if err != nil {
		return err
	}
	return c.st.Compact(c.st.Seq(), data)
}

// Create creates a task. If idemKey was seen before, the original task is
// returned unchanged (idempotent create).
func (c *Core) Create(idemKey, payload string) (*Task, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idemKey != "" {
		if id, ok := c.idem[idemKey]; ok {
			if t, ok := c.tasks[id]; ok {
				cp := *t
				return &cp, false, nil
			}
		}
	}
	now := c.now().UnixNano()
	t := Task{
		ID:             fmt.Sprintf("t-%d", c.st.Seq()+1),
		IdempotencyKey: idemKey,
		Payload:        payload,
		State:          StatePending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := c.commit(recCreate, createRec{Task: t}); err != nil {
		return nil, false, err
	}
	cp := *c.tasks[t.ID]
	return &cp, true, nil
}

// leaseValidLocked reports whether t's lease is still held.
func (c *Core) leaseValidLocked(t *Task) bool {
	return t.State == StateLeased && t.LeaseExpiry > c.now().UnixNano()
}

// Claim atomically leases the oldest available task to workerID.
// A task is available when PENDING, or LEASED with an expired lease.
func (c *Core) Claim(workerID string, leaseDur time.Duration) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best *Task
	for _, t := range c.tasks {
		avail := t.State == StatePending || (t.State == StateLeased && !c.leaseValidLocked(t))
		if !avail {
			continue
		}
		if best == nil || t.CreatedAt < best.CreatedAt || (t.CreatedAt == best.CreatedAt && t.ID < best.ID) {
			best = t
		}
	}
	if best == nil {
		return nil, ErrNoTask
	}
	token := c.nextToken
	c.nextToken++
	expiry := c.now().Add(leaseDur).UnixNano()
	rec := claimRec{ID: best.ID, Token: token, Owner: workerID, Expiry: expiry, AppliedAt: c.now().UnixNano()}
	if err := c.commit(recClaim, rec); err != nil {
		return nil, err
	}
	cp := *c.tasks[best.ID]
	return &cp, nil
}

// checkLeaseLocked validates the fencing token for a mutation. It does
// not check lease expiry; callers that mutate must also verify the lease
// is still live via checkLeaseLiveLocked.
func (c *Core) checkLeaseLocked(id, workerID string, token uint64) (*Task, error) {
	t, ok := c.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	if token < t.Token {
		// An older execution instance: it has lost commit eligibility.
		return nil, ErrFencing
	}
	if token > t.Token || t.LeaseOwner != workerID {
		return nil, ErrNotLeaseHolder
	}
	return t, nil
}

// checkLeaseLiveLocked verifies the caller holds the current token AND the
// lease has not expired. An expired lease is dead: the old execution must
// not regain eligibility by renewing or completing, even if no other
// worker has taken over yet.
func (c *Core) checkLeaseLiveLocked(id, workerID string, token uint64) (*Task, error) {
	t, err := c.checkLeaseLocked(id, workerID, token)
	if err != nil {
		return nil, err
	}
	if t.State != StateLeased {
		return nil, ErrFencing
	}
	if t.LeaseExpiry <= c.now().UnixNano() {
		return nil, ErrLeaseExpired
	}
	return t, nil
}

// Renew extends a lease. Late renewals from stale tokens are rejected, and
// renewals after the lease has expired are rejected even when the token is
// still current: an expired lease cannot be revived.
func (c *Core) Renew(id, workerID string, token uint64, leaseDur time.Duration) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, err := c.checkLeaseLiveLocked(id, workerID, token)
	if err != nil {
		return nil, err
	}
	expiry := c.now().Add(leaseDur).UnixNano()
	rec := renewRec{ID: id, Token: token, Expiry: expiry, AppliedAt: c.now().UnixNano()}
	if err := c.commit(recRenew, rec); err != nil {
		return nil, err
	}
	cp := *c.tasks[id]
	return &cp, nil
}

// Complete marks a task completed. Idempotency: repeating the same
// completion (same token, same result) succeeds and returns the task;
// a stale token or a conflicting result is rejected and never overwrites
// the committed result.
func (c *Core) Complete(id, workerID string, token uint64, result string) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	if t.State == StateCompleted {
		if t.Token == token && t.Result == result {
			cp := *t
			return &cp, nil // idempotent replay of the same completion
		}
		return nil, ErrAlreadyDone
	}
	if _, err := c.checkLeaseLiveLocked(id, workerID, token); err != nil {
		return nil, err
	}
	rec := completeRec{ID: id, Token: token, Owner: workerID, Result: result, AppliedAt: c.now().UnixNano()}
	if err := c.commit(recComplete, rec); err != nil {
		return nil, err
	}
	cp := *c.tasks[id]
	return &cp, nil
}

// Get returns a task by ID.
func (c *Core) Get(id string) (*Task, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *t
	return &cp, nil
}

// List returns all tasks.
func (c *Core) List() []*Task {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Task, 0, len(c.tasks))
	for _, t := range c.tasks {
		cp := *t
		out = append(out, &cp)
	}
	return out
}

// Close closes the underlying store.
func (c *Core) Close() error { return c.st.Close() }
