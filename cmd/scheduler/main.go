// Command scheduler runs the durable task scheduler HTTP server.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"durablesched/internal/sched"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	dir := flag.String("data-dir", "data", "data directory for the durable log and snapshot")
	compactBytes := flag.Int64("compact-bytes", 1<<20, "log size threshold that triggers compaction")
	flag.Parse()

	core, err := sched.Open(*dir)
	if err != nil {
		log.Fatalf("open scheduler state: %v", err)
	}
	core.CompactThreshold = *compactBytes

	srv := &http.Server{Addr: *addr, Handler: core.Handler()}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		srv.Close()
	}()
	log.Printf("scheduler listening on %s, data dir %s", *addr, *dir)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
	core.Close()
}
