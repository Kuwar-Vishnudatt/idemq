// Package integration exercises idemq end to end: a real HTTP server backed
// by a real SQLite file, driven by real HTTP clients (the same client
// package a production worker would use), including process-crash and
// concurrent-worker scenarios.
package integration

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Kuwar-Vishnudatt/idemq/internal/api"
	"github.com/Kuwar-Vishnudatt/idemq/internal/backoff"
	"github.com/Kuwar-Vishnudatt/idemq/internal/reaper"
	"github.com/Kuwar-Vishnudatt/idemq/internal/store"
	"github.com/Kuwar-Vishnudatt/idemq/pkg/client"
)

// testServer bundles a real httptest server, its backing store, and its
// reaper goroutine, all torn down together at test cleanup.
type testServer struct {
	URL string
	DB  *store.DB
}

func startServer(t *testing.T, dbPath string, reapInterval time.Duration) *testServer {
	t.Helper()
	db, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := reaper.New(db, reapInterval, backoff.Policy{Base: 10 * time.Millisecond, Cap: time.Second})
	go r.Run(ctx)

	srv := httptest.NewServer(api.New(db))
	t.Cleanup(func() {
		cancel()
		srv.Close()
		db.Close()
	})

	return &testServer{URL: srv.URL, DB: db}
}

func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "idemq.db")
}

func newClient(ts *testServer) *client.Client {
	return client.New(ts.URL)
}
