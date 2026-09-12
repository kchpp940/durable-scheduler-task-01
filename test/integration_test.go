// Package integration runs end-to-end tests against real scheduler and
// worker processes, including SIGKILL crashes.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

var (
	schedBin string
	workerBin string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "schedbin")
	if err != nil {
		panic(err)
	}
	schedBin = filepath.Join(dir, "scheduler")
	workerBin = filepath.Join(dir, "worker")
	build := exec.Command("go", "build", "-o", schedBin, "durablesched/cmd/scheduler")
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build scheduler: %v\n%s", err, out))
	}
	build = exec.Command("go", "build", "-o", workerBin, "durablesched/cmd/worker")
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build worker: %v\n%s", err, out))
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type server struct {
	cmd  *exec.Cmd
	addr string
	dir  string
}

func startScheduler(t *testing.T, addr, dir string) *server {
	t.Helper()
	cmd := exec.Command(schedBin, "-addr", addr, "-data-dir", dir, "-compact-bytes", "4096")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	s := &server{cmd: cmd, addr: addr, dir: dir}
	waitFor(t, "scheduler up", func() bool {
		_, err := http.Get("http://" + addr + "/tasks")
		return err == nil
	})
	return s
}

func startWorker(t *testing.T, addr, id string, args ...string) *exec.Cmd {
	t.Helper()
	full := append([]string{"-scheduler", "http://" + addr, "-id", id}, args...)
	cmd := exec.Command(workerBin, full...)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	return cmd
}

func (s *server) kill(t *testing.T) {
	t.Helper()
	s.cmd.Process.Signal(syscall.SIGKILL)
	s.cmd.Wait()
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func post(t *testing.T, addr, path string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func getTask(t *testing.T, addr, id string) map[string]any {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/tasks/" + id)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func claim(t *testing.T, addr, worker string, leaseMs int64) (int, map[string]any) {
	t.Helper()
	return post(t, addr, "/tasks/claim", map[string]any{"worker_id": worker, "lease_ms": leaseMs})
}

func TestSchedulerKill9AndRecovery(t *testing.T) {
	addr := freePort(t)
	dir := t.TempDir()
	s := startScheduler(t, addr, dir)

	// Create two tasks; claim and complete one, lease the other.
	code, task1 := post(t, addr, "/tasks", map[string]any{"idempotency_key": "k1", "payload": "p1"})
	if code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}
	post(t, addr, "/tasks", map[string]any{"idempotency_key": "k2", "payload": "p2"})
	id1 := task1["id"].(string)
	_, claimed := claim(t, addr, "w1", 60000)
	token := uint64(claimed["token"].(float64))
	code, _ = post(t, addr, "/tasks/"+id1+"/complete", map[string]any{
		"worker_id": "w1", "token": token, "result": "r1",
	})
	if code != http.StatusOK {
		t.Fatalf("complete: %d", code)
	}
	// Claim task2 with a long lease, then SIGKILL the scheduler.
	_, claimed2 := claim(t, addr, "w1", 60000)
	id2 := claimed2["id"].(string)
	s.kill(t)

	// Restart on the same data dir.
	s2 := startScheduler(t, addr, dir)
	defer s2.cmd.Process.Kill()

	// Completed task survived the kill.
	got1 := getTask(t, addr, id1)
	if got1["state"] != "COMPLETED" || got1["result"] != "r1" {
		t.Fatalf("after kill -9: %+v", got1)
	}
	// Live lease survived: no other worker can steal task2.
	code, _ = claim(t, addr, "w2", 1000)
	if code != http.StatusNoContent {
		t.Fatalf("live lease lost across kill: %d", code)
	}
	// Original holder can still complete with its token.
	code, _ = post(t, addr, "/tasks/"+id2+"/complete", map[string]any{
		"worker_id": "w1", "token": uint64(claimed2["token"].(float64)), "result": "r2",
	})
	if code != http.StatusOK {
		t.Fatalf("complete after restart: %d", code)
	}
}

func TestWorkerDeathAndLateSubmission(t *testing.T) {
	addr := freePort(t)
	dir := t.TempDir()
	s := startScheduler(t, addr, dir)
	defer s.cmd.Process.Kill()

	_, task := post(t, addr, "/tasks", map[string]any{"idempotency_key": "k", "payload": "p"})
	id := task["id"].(string)
	// Worker A claims with a short lease and "dies".
	_, a := claim(t, addr, "worker-A", 50)
	tokenA := uint64(a["token"].(float64))
	expiryA := int64(a["lease_expiry"].(float64))
	// After expiry but BEFORE any takeover, A's renew/complete must already
	// be rejected: an expired lease cannot be revived.
	waitFor(t, "A's lease expired", func() bool {
		return time.Now().UnixNano() > expiryA
	})
	code, _ := post(t, addr, "/tasks/"+id+"/renew", map[string]any{
		"worker_id": "worker-A", "token": tokenA, "lease_ms": 60000,
	})
	if code != http.StatusConflict {
		t.Fatalf("renew with expired lease: %d", code)
	}
	code, _ = post(t, addr, "/tasks/"+id+"/complete", map[string]any{
		"worker_id": "worker-A", "token": tokenA, "result": "A-early",
	})
	if code != http.StatusConflict {
		t.Fatalf("complete with expired lease: %d", code)
	}
	// Worker B takes over after expiry and completes.
	var b map[string]any
	waitFor(t, "B takes over", func() bool {
		code, got := claim(t, addr, "worker-B", 60000)
		if code == http.StatusOK {
			b = got
			return true
		}
		return false
	})
	tokenB := uint64(b["token"].(float64))
	code, _ = post(t, addr, "/tasks/"+id+"/complete", map[string]any{
		"worker_id": "worker-B", "token": tokenB, "result": "B-result",
	})
	if code != http.StatusOK {
		t.Fatalf("B complete: %d", code)
	}
	// A's late renew and late complete are rejected, result unchanged.
	code, _ = post(t, addr, "/tasks/"+id+"/renew", map[string]any{
		"worker_id": "worker-A", "token": tokenA, "lease_ms": 60000,
	})
	if code != http.StatusConflict {
		t.Fatalf("late renew: %d", code)
	}
	code, _ = post(t, addr, "/tasks/"+id+"/complete", map[string]any{
		"worker_id": "worker-A", "token": tokenA, "result": "A-result",
	})
	if code != http.StatusConflict {
		t.Fatalf("late complete: %d", code)
	}
	got := getTask(t, addr, id)
	if got["result"] != "B-result" {
		t.Fatalf("result overwritten by zombie: %+v", got)
	}
	// Duplicate create with same key returns the same task, not a new one.
	code, dup := post(t, addr, "/tasks", map[string]any{"idempotency_key": "k", "payload": "p"})
	if code != http.StatusOK || dup["id"] != id {
		t.Fatalf("idempotent create: %d %+v", code, dup)
	}
}

// waitTaskState polls until the task reaches the wanted state.
func waitTaskState(t *testing.T, addr, id, state string) map[string]any {
	t.Helper()
	var got map[string]any
	waitFor(t, fmt.Sprintf("task %s -> %s", id, state), func() bool {
		resp, err := http.Get("http://" + addr + "/tasks/" + id)
		if err != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		defer resp.Body.Close()
		got = map[string]any{}
		json.NewDecoder(resp.Body).Decode(&got)
		return got["state"] == state
	})
	return got
}

// TestRealWorkerEndToEnd proves the full loop with real processes:
// task created over HTTP is claimed, executed and completed by a real
// worker process, with lease renewal keeping the claim alive.
func TestRealWorkerEndToEnd(t *testing.T) {
	addr := freePort(t)
	dir := t.TempDir()
	s := startScheduler(t, addr, dir)
	defer s.cmd.Process.Kill()

	// Work takes 500ms while the lease is 150ms: the task can only be
	// completed if the worker's background renewal actually works.
	startWorker(t, addr, "w1", "-lease", "150ms", "-work", "500ms", "-poll", "20ms")

	_, task := post(t, addr, "/tasks", map[string]any{"idempotency_key": "e2e", "payload": "hello"})
	id := task["id"].(string)
	got := waitTaskState(t, addr, id, "COMPLETED")
	if got["result"] != "done:hello" {
		t.Fatalf("unexpected result: %+v", got)
	}
	// Exactly one execution: token must be 1 (never re-claimed).
	if uint64(got["token"].(float64)) != 1 {
		t.Fatalf("task was re-claimed, token=%v", got["token"])
	}
}

// TestRealWorkerKilledMidTask proves that when a worker process dies while
// holding a lease, another worker process takes over and completes the task.
func TestRealWorkerKilledMidTask(t *testing.T) {
	addr := freePort(t)
	dir := t.TempDir()
	s := startScheduler(t, addr, dir)
	defer s.cmd.Process.Kill()

	// Worker A: long work duration so it is killed mid-task.
	a := exec.Command(workerBin, "-scheduler", "http://"+addr, "-id", "worker-A",
		"-lease", "200ms", "-work", "30s", "-poll", "20ms")
	a.Stdout, a.Stderr = os.Stderr, os.Stderr
	if err := a.Start(); err != nil {
		t.Fatal(err)
	}

	_, task := post(t, addr, "/tasks", map[string]any{"idempotency_key": "kill", "payload": "x"})
	id := task["id"].(string)
	// Wait until A actually holds the lease, then SIGKILL it.
	waitTaskState(t, addr, id, "LEASED")
	a.Process.Kill()
	a.Wait()

	// Worker B picks the task up after A's lease expires and finishes it.
	startWorker(t, addr, "worker-B", "-lease", "200ms", "-work", "50ms", "-poll", "20ms")
	got := waitTaskState(t, addr, id, "COMPLETED")
	if got["result"] != "done:x" {
		t.Fatalf("unexpected result: %+v", got)
	}
	if uint64(got["token"].(float64)) < 2 {
		t.Fatalf("expected takeover with token >= 2, got %v", got["token"])
	}
}

// TestCompactionUnderRealProcess forces log compaction in a real scheduler
// process, SIGKILLs it, and proves recovery from snapshot + log tail.
func TestCompactionUnderRealProcess(t *testing.T) {
	addr := freePort(t)
	dir := t.TempDir()
	s := startScheduler(t, addr, dir) // -compact-bytes 4096

	// Create and complete enough tasks to exceed the compaction threshold.
	const n = 40
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("c%d", i)
		post(t, addr, "/tasks", map[string]any{"idempotency_key": key, "payload": "p"})
		code, cl := claim(t, addr, "w", 60000)
		if code != http.StatusOK {
			t.Fatalf("claim %d: %d", i, code)
		}
		post(t, addr, "/tasks/"+cl["id"].(string)+"/complete", map[string]any{
			"worker_id": "w", "token": uint64(cl["token"].(float64)), "result": "r",
		})
	}
	// One live leased task survives the kill.
	post(t, addr, "/tasks", map[string]any{"idempotency_key": "live", "payload": "p"})
	code, live := claim(t, addr, "w", 60000)
	if code != http.StatusOK {
		t.Fatalf("claim live task: %d", code)
	}
	liveID := live["id"].(string)
	liveToken := uint64(live["token"].(float64))

	// Compaction must have happened: snapshot file exists.
	waitFor(t, "snapshot file", func() bool {
		_, err := os.Stat(filepath.Join(dir, "snapshot.json"))
		return err == nil
	})
	s.kill(t)

	// Restart: all completed tasks and the live lease must be intact.
	s2 := startScheduler(t, addr, dir)
	defer s2.cmd.Process.Kill()
	resp, err := http.Get("http://" + addr + "/tasks")
	if err != nil {
		t.Fatal(err)
	}
	var tasks []map[string]any
	json.NewDecoder(resp.Body).Decode(&tasks)
	resp.Body.Close()
	if len(tasks) != n+1 {
		t.Fatalf("recovered %d tasks, want %d", len(tasks), n+1)
	}
	for _, task := range tasks {
		if task["id"] == liveID {
			if task["state"] != "LEASED" {
				t.Fatalf("live lease lost after kill+compaction: %+v", task)
			}
			continue
		}
		if task["state"] != "COMPLETED" {
			t.Fatalf("task not completed after recovery: %+v", task)
		}
	}
	// The pre-kill lease holder can still complete with its fencing token.
	code, _ = post(t, addr, "/tasks/"+liveID+"/complete", map[string]any{
		"worker_id": "w", "token": liveToken, "result": "final",
	})
	if code != http.StatusOK {
		t.Fatalf("complete live task after recovery: %d", code)
	}
	// Idempotency keys survived compaction: duplicate create returns same task.
	code, dup := post(t, addr, "/tasks", map[string]any{"idempotency_key": "c0", "payload": "p"})
	if code != http.StatusOK {
		t.Fatalf("idempotent create after compaction: %d", code)
	}
	if dup["state"] != "COMPLETED" {
		t.Fatalf("duplicate create returned wrong task: %+v", dup)
	}
}
