// Package store implements the durable, transactional job table backing the
// queue. SQLite (pure-Go, via modernc.org/sqlite) was chosen deliberately: a
// single writer connection gives us the atomicity we need for claim/ack/nack
// without implementing our own WAL or locking protocol, and WAL-mode SQLite
// is a genuinely production-grade choice for a single-node broker (this is
// the same pattern used by tools like river and que-go, minus their use of
// Postgres's SKIP LOCKED for multi-writer horizontal scaling — see
// docs/ARCHITECTURE.md for why that tradeoff was made and what it costs).
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
	id               TEXT PRIMARY KEY,
	queue            TEXT NOT NULL,
	idempotency_key  TEXT,
	payload          BLOB NOT NULL,
	status           TEXT NOT NULL, -- pending | in_flight | completed | dead
	priority         INTEGER NOT NULL DEFAULT 0,
	attempts         INTEGER NOT NULL DEFAULT 0,
	max_attempts     INTEGER NOT NULL DEFAULT 5,
	available_at     INTEGER NOT NULL,
	lease_token      INTEGER NOT NULL DEFAULT 0,
	lease_expires_at INTEGER NOT NULL DEFAULT 0,
	last_error       TEXT,
	created_at       INTEGER NOT NULL,
	updated_at       INTEGER NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency
	ON jobs(queue, idempotency_key) WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_claimable
	ON jobs(queue, status, available_at, priority);

CREATE INDEX IF NOT EXISTS idx_jobs_inflight
	ON jobs(status, lease_expires_at) WHERE status = 'in_flight';

CREATE TABLE IF NOT EXISTS lease_seq (
	name  TEXT PRIMARY KEY,
	value INTEGER NOT NULL
);
INSERT OR IGNORE INTO lease_seq(name, value) VALUES ('lease', 0);
`

// DB wraps the SQLite connection pool. Writes are serialized to a single
// connection (SetMaxOpenConns(1)) because SQLite allows only one writer at a
// time regardless of how many *sql.DB connections you open; letting the Go
// pool believe it has more just produces SQLITE_BUSY errors under
// concurrency instead of a clean queue. Reads use a separate unlimited pool
// since SQLite (in WAL mode) allows concurrent readers alongside the writer.
type DB struct {
	write *sql.DB
	read  *sql.DB
}

func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)", path)

	write, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open write conn: %w", err)
	}
	write.SetMaxOpenConns(1)

	read, err := sql.Open("sqlite", dsn)
	if err != nil {
		write.Close()
		return nil, fmt.Errorf("open read conn: %w", err)
	}

	if _, err := write.Exec(schema); err != nil {
		write.Close()
		read.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}

	return &DB{write: write, read: read}, nil
}

func (d *DB) Close() error {
	werr := d.write.Close()
	rerr := d.read.Close()
	if werr != nil {
		return werr
	}
	return rerr
}
