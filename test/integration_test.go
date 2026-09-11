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

var schedBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "schedbin")
	if err != nil {
		panic(err)
	}
	schedBin = filepath.Join(dir, "scheduler")
	build := exec.Command("go", "build", "-o", schedBin, "durablesched/cmd/scheduler")
	build.Dir = ".."
	if out, err := build.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("build scheduler: %v\n%s", err, out))
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
	code, _ := post(t, addr, "/tasks/"+id+"/complete", map[string]any{
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
