package worker

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"durablesched/internal/sched"
)

func newTestServer(t *testing.T) (*httptest.Server, *sched.Core) {
	t.Helper()
	dir := t.TempDir()
	core, err := sched.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	core.CompactThreshold = 1 << 60
	srv := httptest.NewServer(core.Handler())
	t.Cleanup(func() { srv.Close(); core.Close() })
	return srv, core
}

func TestExecuteRenewsAndCompletes(t *testing.T) {
	srv, core := newTestServer(t)
	if _, _, err := core.Create("", "job"); err != nil {
		t.Fatal(err)
	}
	c := NewClient(srv.URL, "w1")
	task, err := c.Claim(120 * time.Millisecond)
	if err != nil || task == nil {
		t.Fatalf("claim: %v %v", task, err)
	}
	// Work outlasts the raw lease; renewal must keep it alive.
	err = c.Execute(context.Background(), task, 120*time.Millisecond, func(p string) string {
		time.Sleep(400 * time.Millisecond)
		return "ok:" + p
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := core.Get(task.ID)
	if got.State != sched.StateCompleted || got.Result != "ok:job" {
		t.Fatalf("task: %+v", got)
	}
	if got.Token != 1 {
		t.Fatalf("lease was lost during execution, token=%d", got.Token)
	}
}

func TestExecuteAbortsWhenLeaseLost(t *testing.T) {
	srv, core := newTestServer(t)
	core.Create("", "job")
	c := NewClient(srv.URL, "w1")
	task, _ := c.Claim(60 * time.Millisecond)
	// Steal the lease after expiry, simulating a stalled worker.
	time.Sleep(80 * time.Millisecond)
	if _, err := core.Claim("w2", time.Minute); err != nil {
		t.Fatal(err)
	}
	var ran int64
	err := c.Execute(context.Background(), task, 60*time.Millisecond, func(p string) string {
		atomic.AddInt64(&ran, 1)
		return "late"
	})
	if err == nil {
		t.Fatal("expected lease-lost error")
	}
	got, _ := core.Get(task.ID)
	if got.State == sched.StateCompleted {
		t.Fatalf("stale worker committed result: %+v", got)
	}
	_ = ran
}

func TestCompleteConflictSurfaces(t *testing.T) {
	srv, core := newTestServer(t)
	core.Create("", "job")
	c1 := NewClient(srv.URL, "w1")
	task, _ := c1.Claim(time.Minute)
	if err := c1.Complete(task.ID, task.Token, "r1"); err != nil {
		t.Fatal(err)
	}
	// A second complete with a different result must fail loudly.
	if err := c1.Complete(task.ID, task.Token, "different"); err == nil ||
		!strings.Contains(err.Error(), "409") {
		t.Fatalf("conflicting complete: %v", err)
	}
	// Idempotent replay succeeds.
	if err := c1.Complete(task.ID, task.Token, "r1"); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
}
