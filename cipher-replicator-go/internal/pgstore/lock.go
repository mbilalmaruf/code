package pgstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StreamLock is a session-level advisory lock that guarantees a single
// writer per (process, couch db) across all replicator instances. It pins
// one pool connection for the life of the stream.
type StreamLock struct {
	conn *pgxpool.Conn
	key  string
}

func AcquireStreamLock(ctx context.Context, pool *pgxpool.Pool, process, db string) (*StreamLock, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	key := "cipher-replicator:stream:" + process + ":" + db
	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, key).Scan(&ok); err != nil {
		conn.Release()
		return nil, err
	}
	if !ok {
		conn.Release()
		return nil, fmt.Errorf("stream %s/%s is already being replicated by another instance (advisory lock held)", process, db)
	}
	return &StreamLock{conn: conn, key: key}, nil
}

// Ping verifies the lock's session is alive. If the connection was lost the
// lock is gone too, and the stream must stop.
func (l *StreamLock) Ping(ctx context.Context) error { return l.conn.Ping(ctx) }

// Release unlocks and returns the connection to the pool.
func (l *StreamLock) Release() {
	ctx := context.Background()
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, l.key)
	l.conn.Release()
}
