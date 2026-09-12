package sched

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func openCore(t *testing.T) (*Core, string) {
	t.Helper()
	dir := t.TempDir()
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.CompactThreshold = 1 << 60 // disable auto-compaction unless testing it
	t.Cleanup(func() { c.Close() })
	return c, dir
}

// waitFor polls cond until true or the deadline passes. No fixed sleeps.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestConcurrentClaimSingleWinner(t *testing.T) {
	c, _ := openCore(t)
	if _, _, err := c.Create("", "job"); err != nil {
		t.Fatal(err)
	}
	const n = 32
	var wg sync.WaitGroup
	wins := make(chan *Task, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task, err := c.Claim(fmt.Sprintf("w%d", i), time.Minute)
			if err == nil {
				wins <- task
			} else if !errors.Is(err, ErrNoTask) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	close(wins)
	var got []*Task
	for task := range wins {
		got = append(got, task)
	}
	if len(got) != 1 {
		t.Fatalf("exactly one worker must win the claim, got %d", len(got))
	}
	if got[0].Token != 1 {
		t.Fatalf("first claim token = %d, want 1", got[0].Token)
	}
}

func TestLeaseExpiryTakeover(t *testing.T) {
	c, _ := openCore(t)
	c.Create("", "job")
	t1, err := c.Claim("w1", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// While the lease is valid, no one else can claim.
	if _, err := c.Claim("w2", time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("claim during valid lease: %v", err)
	}
	// After expiry, w2 takes over with a strictly newer token.
	var t2 *Task
	waitFor(t, "lease expiry takeover", func() bool {
		var err error
		t2, err = c.Claim("w2", time.Minute)
		return err == nil
	})
	if t2.Token <= t1.Token {
		t.Fatalf("takeover token %d must exceed old token %d", t2.Token, t1.Token)
	}
	if t2.LeaseOwner != "w2" {
		t.Fatalf("owner = %q", t2.LeaseOwner)
	}
}

func TestStaleTokenCannotCommit(t *testing.T) {
	c, _ := openCore(t)
	c.Create("", "job")
	t1, _ := c.Claim("w1", 20*time.Millisecond)
	var t2 *Task
	waitFor(t, "takeover", func() bool {
		var err error
		t2, err = c.Claim("w2", time.Minute)
		return err == nil
	})
	// Old worker's late renew is rejected.
	if _, err := c.Renew(t1.ID, "w1", t1.Token, time.Minute); !errors.Is(err, ErrFencing) {
		t.Fatalf("stale renew: %v", err)
	}
	// New holder completes.
	if _, err := c.Complete(t2.ID, "w2", t2.Token, "new-result"); err != nil {
		t.Fatal(err)
	}
	// Old worker's late complete must not overwrite.
	if _, err := c.Complete(t1.ID, "w1", t1.Token, "stale-result"); !errors.Is(err, ErrFencing) && !errors.Is(err, ErrAlreadyDone) {
		t.Fatalf("stale complete: %v", err)
	}
	got, _ := c.Get(t1.ID)
	if got.State != StateCompleted || got.Result != "new-result" {
		t.Fatalf("state=%s result=%q", got.State, got.Result)
	}
}

func TestIdempotentCreateAndComplete(t *testing.T) {
	c, _ := openCore(t)
	t1, created1, _ := c.Create("key-1", "job")
	t2, created2, _ := c.Create("key-1", "job")
	if !created1 || created2 || t1.ID != t2.ID {
		t.Fatalf("idempotent create broken: %v %v %v %v", created1, created2, t1.ID, t2.ID)
	}
	if len(c.List()) != 1 {
		t.Fatalf("duplicate create produced extra tasks")
	}
	cl, _ := c.Claim("w1", time.Minute)
	if _, err := c.Complete(cl.ID, "w1", cl.Token, "r"); err != nil {
		t.Fatal(err)
	}
	// Exact replay of the same completion is a no-op success.
	if _, err := c.Complete(cl.ID, "w1", cl.Token, "r"); err != nil {
		t.Fatalf("idempotent complete replay: %v", err)
	}
	// Conflicting result is rejected.
	if _, err := c.Complete(cl.ID, "w1", cl.Token, "different"); !errors.Is(err, ErrAlreadyDone) {
		t.Fatalf("conflicting complete: %v", err)
	}
	// Completed task never becomes claimable again.
	if _, err := c.Claim("w2", time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("completed task re-claimed: %v", err)
	}
}

func TestRecoveryAfterReopen(t *testing.T) {
	c, dir := openCore(t)
	c.Create("", "a")
	c.Create("", "b")
	cl, _ := c.Claim("w1", time.Hour) // valid lease persisted
	done, _ := c.Claim("w1", time.Hour)
	c.Complete(done.ID, "w1", done.Token, "ok")
	c.Close()

	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// Completed task stays completed.
	got, _ := c2.Get(done.ID)
	if got.State != StateCompleted || got.Result != "ok" {
		t.Fatalf("recovered completed task: %+v", got)
	}
	// Live lease survives restart: other workers cannot steal it.
	if _, err := c2.Claim("w2", time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("live lease not preserved across restart: %v", err)
	}
	// The leased task is still owned by w1 with the same token.
	got2, _ := c2.Get(cl.ID)
	if got2.State != StateLeased || got2.Token != cl.Token || got2.LeaseOwner != "w1" {
		t.Fatalf("recovered lease: %+v", got2)
	}
}

func TestWorkerDeathMidTask(t *testing.T) {
	c, _ := openCore(t)
	c.Create("", "job")
	// Worker claims with a short lease, then "dies" (never renews/completes).
	dead, _ := c.Claim("doomed-worker", 20*time.Millisecond)
	// Another worker eventually picks the task up and finishes it.
	var t2 *Task
	waitFor(t, "reclaim after worker death", func() bool {
		var err error
		t2, err = c.Claim("survivor", time.Minute)
		return err == nil
	})
	if t2.ID != dead.ID || t2.Token <= dead.Token {
		t.Fatalf("reclaim: %+v vs %+v", t2, dead)
	}
	if _, err := c.Complete(t2.ID, "survivor", t2.Token, "done"); err != nil {
		t.Fatal(err)
	}
	// The dead worker's late completion is rejected.
	if _, err := c.Complete(dead.ID, "doomed-worker", dead.Token, "zombie"); err == nil {
		t.Fatal("zombie completion accepted")
	}
	got, _ := c.Get(dead.ID)
	if got.Result != "done" {
		t.Fatalf("result overwritten: %q", got.Result)
	}
}

func TestCompactionRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.CompactThreshold = 512 // force compaction quickly
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		c.Create(key, "payload")
		cl, err := c.Claim("w", time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Complete(cl.ID, "w", cl.Token, "r"); err != nil {
			t.Fatal(err)
		}
	}
	c.Close()
	// Reopen from snapshot + log tail.
	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if n := len(c2.List()); n != 20 {
		t.Fatalf("recovered %d tasks, want 20", n)
	}
	for _, task := range c2.List() {
		if task.State != StateCompleted {
			t.Fatalf("task %s state %s after compaction recovery", task.ID, task.State)
		}
	}
	// Idempotency keys survive compaction.
	if _, created, _ := c2.Create("k3", "payload"); created {
		t.Fatal("idempotency key lost across compaction")
	}
}

func TestExpiredLeaseCannotRevive(t *testing.T) {
	c, _ := openCore(t)
	c.Create("", "job")
	t1, _ := c.Claim("w1", 20*time.Millisecond)
	// Lease expires with no takeover. The old holder must not revive its
	// execution eligibility by renewing or completing.
	waitFor(t, "lease expiry", func() bool {
		return c.now().UnixNano() > t1.LeaseExpiry
	})
	if _, err := c.Renew(t1.ID, "w1", t1.Token, time.Minute); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after expiry: %v", err)
	}
	if _, err := c.Complete(t1.ID, "w1", t1.Token, "zombie"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("complete after expiry: %v", err)
	}
	// Task is still pending-ish and claimable by a new worker.
	t2, err := c.Claim("w2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if t2.Token <= t1.Token {
		t.Fatalf("token did not advance: %d -> %d", t1.Token, t2.Token)
	}
	// After takeover the old instance is fully isolated.
	if _, err := c.Renew(t1.ID, "w1", t1.Token, time.Minute); !errors.Is(err, ErrFencing) {
		t.Fatalf("stale renew after takeover: %v", err)
	}
	if _, err := c.Complete(t1.ID, "w1", t1.Token, "zombie"); !errors.Is(err, ErrFencing) {
		t.Fatalf("stale complete after takeover: %v", err)
	}
	// New instance works normally.
	if _, err := c.Renew(t2.ID, "w2", t2.Token, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Complete(t2.ID, "w2", t2.Token, "real"); err != nil {
		t.Fatal(err)
	}
	got, _ := c.Get(t1.ID)
	if got.State != StateCompleted || got.Result != "real" {
		t.Fatalf("final state: %+v", got)
	}
}

func TestExpiredLeaseSemanticsAcrossRestart(t *testing.T) {
	c, dir := openCore(t)
	c.Create("", "job")
	t1, _ := c.Claim("w1", 30*time.Millisecond)
	c.Close() // restart while the lease is still valid on disk

	// Reopen after the persisted lease expiry has passed.
	waitFor(t, "persisted lease expiry", func() bool {
		return time.Now().UnixNano() > t1.LeaseExpiry
	})
	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// The expired lease stays expired after recovery: no revive.
	if _, err := c2.Renew(t1.ID, "w1", t1.Token, time.Minute); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after restart+expiry: %v", err)
	}
	if _, err := c2.Complete(t1.ID, "w1", t1.Token, "zombie"); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("complete after restart+expiry: %v", err)
	}
	// A new worker takes over with a strictly larger fencing token.
	t2, err := c2.Claim("w2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if t2.Token <= t1.Token {
		t.Fatalf("fencing token regressed across restart: %d -> %d", t1.Token, t2.Token)
	}
	// Old instance is fenced off; new instance completes.
	if _, err := c2.Complete(t1.ID, "w1", t1.Token, "zombie"); !errors.Is(err, ErrFencing) {
		t.Fatalf("stale complete after restart+takeover: %v", err)
	}
	if _, err := c2.Complete(t2.ID, "w2", t2.Token, "real"); err != nil {
		t.Fatal(err)
	}
	// Restart once more: the terminal state and fencing survive.
	c2.Close()
	c3, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	got, _ := c3.Get(t1.ID)
	if got.State != StateCompleted || got.Result != "real" {
		t.Fatalf("terminal state after second restart: %+v", got)
	}
	if _, err := c3.Complete(t1.ID, "w1", t1.Token, "zombie"); !errors.Is(err, ErrAlreadyDone) && !errors.Is(err, ErrFencing) {
		t.Fatalf("zombie complete after second restart: %v", err)
	}
	if _, err := c3.Claim("w3", time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("completed task re-claimed after restart: %v", err)
	}
}

func TestLiveLeaseRenewalPersistsAcrossRestart(t *testing.T) {
	c, dir := openCore(t)
	c.Create("", "job")
	t1, _ := c.Claim("w1", time.Hour)
	// Renew extends the persisted expiry.
	if _, err := c.Renew(t1.ID, "w1", t1.Token, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	c.Close()

	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	// Renewed lease is still live: holder can complete, others cannot claim.
	if _, err := c2.Claim("w2", time.Minute); !errors.Is(err, ErrNoTask) {
		t.Fatalf("renewed lease not honored after restart: %v", err)
	}
	if _, err := c2.Complete(t1.ID, "w1", t1.Token, "done"); err != nil {
		t.Fatalf("complete with renewed lease after restart: %v", err)
	}
}
