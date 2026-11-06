// Command idemq-server runs the queue's HTTP API plus its background lease
// reaper against a single SQLite file.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/internal/api"
	"github.com/Kuwar-Vishnudatt/idemq/internal/backoff"
	"github.com/Kuwar-Vishnudatt/idemq/internal/reaper"
	"github.com/Kuwar-Vishnudatt/idemq/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dbPath := flag.String("db", "idemq.db", "path to the SQLite database file")
	reapInterval := flag.Duration("reap-interval", 2*time.Second, "how often to scan for expired leases")
	flag.Parse()

	db, err := store.Open(*dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	r := reaper.New(db, *reapInterval, backoff.Default())
	go r.Run(ctx)

	srv := &http.Server{Addr: *addr, Handler: api.New(db)}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("idemq-server listening on %s (db=%s)", *addr, *dbPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
	log.Println("idemq-server stopped")
}
