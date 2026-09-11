// Command worker polls the scheduler for tasks and executes them.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"durablesched/internal/worker"
)

func main() {
	sched := flag.String("scheduler", "http://127.0.0.1:8080", "scheduler base URL")
	id := flag.String("id", "", "worker id (default: hostname-pid)")
	lease := flag.Duration("lease", 2*time.Second, "lease duration")
	poll := flag.Duration("poll", 200*time.Millisecond, "poll interval when idle")
	work := flag.Duration("work", 300*time.Millisecond, "simulated work duration per task")
	flag.Parse()

	if *id == "" {
		*id = "worker"
	}
	c := worker.NewClient(*sched, *id)
	log.Printf("worker %s polling %s", *id, *sched)
	for {
		t, err := c.Claim(*lease)
		if err != nil {
			log.Printf("claim error: %v", err)
			time.Sleep(*poll)
			continue
		}
		if t == nil {
			time.Sleep(*poll)
			continue
		}
		log.Printf("claimed %s token=%d payload=%q", t.ID, t.Token, t.Payload)
		err = c.Execute(context.Background(), t, *lease, func(payload string) string {
			time.Sleep(*work) // simulated work
			return "done:" + payload
		})
		if err != nil {
			log.Printf("task %s failed: %v", t.ID, err)
		} else {
			log.Printf("completed %s", t.ID)
		}
	}
}
