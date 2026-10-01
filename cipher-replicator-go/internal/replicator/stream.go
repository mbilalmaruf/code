package replicator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"cipher-replicator/internal/config"
	"cipher-replicator/internal/couch"
	"cipher-replicator/internal/pgstore"
	"cipher-replicator/internal/replog"
	"cipher-replicator/internal/transform"
)

// stream replicates one CouchDB database of one process.
type stream struct {
	process    *config.ProcessConfig
	db         string
	since      string // committed checkpoint at startup
	routes     map[string]*pgstore.Table
	tables     []*pgstore.Table
	metaSchema string
	batchSize  int
	flushEvery time.Duration
	heartbeat  time.Duration
	skipBad    bool
	retry      config.RetryConfig

	pool  *pgxpool.Pool
	couch *couch.Client
	rlog  replog.Recorder
	log   *slog.Logger
	stat  *StreamStatus
}

// StreamStatus is exposed on /status.
type StreamStatus struct {
	mu           sync.Mutex
	Process      string    `json:"process"`
	CouchDB      string    `json:"couchDb"`
	CommittedSeq string    `json:"committedSeq"`
	DocsApplied  int64     `json:"docsApplied"`
	DocsIgnored  int64     `json:"docsIgnored"`
	DocsSkipped  int64     `json:"docsSkipped"`
	LastBatchAt  time.Time `json:"lastBatchAt"`
	LastError    string    `json:"lastError,omitempty"`
}

func (s *StreamStatus) snapshot() StreamStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StreamStatus{
		Process: s.Process, CouchDB: s.CouchDB, CommittedSeq: s.CommittedSeq,
		DocsApplied: s.DocsApplied, DocsIgnored: s.DocsIgnored, DocsSkipped: s.DocsSkipped,
		LastBatchAt: s.LastBatchAt, LastError: s.LastError,
	}
}

func (s *StreamStatus) setError(err error) {
	s.mu.Lock()
	s.LastError = err.Error()
	s.mu.Unlock()
}

func (s *stream) run(ctx context.Context) error {
	lock, err := pgstore.AcquireStreamLock(ctx, s.pool, s.process.Name, s.db)
	if err != nil {
		return err
	}
	defer lock.Release()

	s.log.Info("stream started", "since", s.since)
	changes := make(chan couch.Change, s.batchSize*2)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(safe(func() error { return s.read(gctx, changes) }))
	g.Go(safe(func() error { return s.write(gctx, changes) }))
	g.Go(safe(func() error { return s.watchLock(gctx, lock) }))
	err = g.Wait()
	if err != nil && ctx.Err() == nil {
		s.stat.setError(err)
		return fmt.Errorf("process %s, couch db %s: %w", s.process.Name, s.db, err)
	}
	return nil
}

// read follows the changes feed, reconnecting from the last received seq.
// Consecutive failures beyond retry.MaxAttempts are fatal.
func (s *stream) read(ctx context.Context, out chan<- couch.Change) error {
	since := s.since
	failures := 0
	for {
		got := false
		last, err := s.couch.Changes(ctx, s.db, since, s.heartbeat, func(c couch.Change) error {
			select {
			case out <- c:
				got = true
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		since = last
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			failures = 0 // server closed the feed cleanly; reconnect
			continue
		}
		var se *couch.StatusError
		if errors.As(err, &se) && !se.Transient() {
			return fmt.Errorf("changes feed: %w", err)
		}
		if got {
			failures = 0
		}
		failures++
		if failures > s.retry.MaxAttempts {
			return fmt.Errorf("changes feed failed %d times in a row: %w", failures, err)
		}
		wait := backoff(s.retry, failures)
		s.log.Warn("changes feed interrupted; reconnecting", "error", err, "attempt", failures, "wait", wait)
		if !sleep(ctx, wait) {
			return nil
		}
	}
}

// write batches changes and commits each batch with its checkpoint.
func (s *stream) write(ctx context.Context, in <-chan couch.Change) error {
	ticker := time.NewTicker(s.flushEvery)
	defer ticker.Stop()
	batch := make([]couch.Change, 0, s.batchSize)
	for {
		select {
		case <-ctx.Done():
			return nil // uncommitted changes are replayed from the checkpoint on restart
		case c := <-in:
			batch = append(batch, c)
			if len(batch) < s.batchSize {
				continue
			}
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}
		}
		if err := s.flush(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
	}
}

func (s *stream) flush(ctx context.Context, batch []couch.Change) error {
	ops, entries, ignored, err := s.prepare(batch)
	if err != nil {
		return err
	}
	cp := pgstore.Checkpoint{
		MetaSchema: s.metaSchema, Process: s.process.Name, CouchDB: s.db,
		Seq: batch[len(batch)-1].Seq, Docs: len(ops),
	}
	var skipped []pgstore.OpError
	for attempt := 1; ; attempt++ {
		skipped, err = pgstore.ApplyBatch(ctx, s.pool, s.db, ops, s.tables, cp, s.skipBad)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil
		}
		if !pgstore.IsTransient(err) || attempt > s.retry.MaxAttempts {
			return fmt.Errorf("write batch ending at seq %s: %w", cp.Seq, err)
		}
		wait := backoff(s.retry, attempt)
		s.log.Warn("postgres write failed; retrying batch", "error", err, "attempt", attempt, "wait", wait)
		if !sleep(ctx, wait) {
			return nil
		}
	}

	// Entries for ops that were skipped become errors.
	for _, sk := range skipped {
		e := &entries[sk.Index]
		e.Action, e.Error = "error", sk.Err.Error()
		s.log.Error("document skipped", "docId", e.DocID, "seq", e.Seq, "table", e.Table, "error", sk.Err)
	}
	s.rlog.Record(entries)

	s.stat.mu.Lock()
	s.stat.CommittedSeq = cp.Seq
	s.stat.DocsApplied += int64(len(ops) - len(skipped))
	s.stat.DocsIgnored += int64(ignored)
	s.stat.DocsSkipped += int64(len(skipped))
	s.stat.LastBatchAt = time.Now()
	s.stat.LastError = ""
	s.stat.mu.Unlock()
	s.log.Debug("batch committed", "seq", cp.Seq, "docs", len(ops), "ignored", ignored, "skipped", len(skipped))
	return nil
}

// prepare converts changes into write ops. Docs without a matching table
// (or design docs) are ignored. Conversion errors are fatal unless skipBad.
// Entries are index-aligned with ops, plus trailing entries for conversion
// errors that produced no op.
func (s *stream) prepare(batch []couch.Change) ([]pgstore.Op, []replog.Entry, int, error) {
	ops := make([]pgstore.Op, 0, len(batch))
	entries := make([]replog.Entry, 0, len(batch))
	var convErrs []replog.Entry
	ignored := 0
	now := time.Now().UTC()
	for _, c := range batch {
		base := replog.Entry{Process: s.process.Name, CouchDB: s.db, Seq: c.Seq, DocID: c.ID, Rev: c.Rev, At: now}
		if strings.HasPrefix(c.ID, "_design/") {
			ignored++
			continue
		}
		if c.Deleted {
			if s.process.Target.DeleteMode == config.DeleteIgnore {
				ignored++
				continue
			}
			ops = append(ops, pgstore.Op{Delete: true, DocID: c.ID, Rev: c.Rev, Seq: c.Seq,
				Context: fmt.Sprintf("delete doc %q (seq %s)", c.ID, c.Seq)})
			base.Action = "delete"
			entries = append(entries, base)
			continue
		}
		doc, err := transform.Decode(c.Doc, s.process.Source.DocumentNameFields)
		if err != nil {
			if !s.skipBad {
				return nil, nil, 0, fmt.Errorf("doc %q (seq %s): %w", c.ID, c.Seq, err)
			}
			base.Action, base.Error = "error", err.Error()
			convErrs = append(convErrs, base.WithDocument(c.Doc))
			continue
		}
		tbl, ok := s.routes[doc.DocumentName]
		if !ok {
			ignored++
			continue
		}
		base.DocumentName, base.Table = doc.DocumentName, tbl.Name
		row, err := transform.Project(doc, tbl.Columns)
		if err != nil {
			if !s.skipBad {
				return nil, nil, 0, fmt.Errorf("doc %q (seq %s) -> table %s: %w", c.ID, c.Seq, tbl.Name, err)
			}
			base.Action, base.Error = "error", err.Error()
			convErrs = append(convErrs, base.WithDocument(c.Doc))
			s.log.Error("document skipped", "docId", c.ID, "seq", c.Seq, "table", tbl.Name, "error", err)
			continue
		}
		ops = append(ops, pgstore.Op{Table: tbl, DocID: c.ID, Rev: c.Rev, Seq: c.Seq, Values: row.Values,
			Context: fmt.Sprintf("doc %q (seq %s) -> table %s", c.ID, c.Seq, tbl.Name)})
		base.Action = "upsert"
		entries = append(entries, base.WithDocument(c.Doc))
	}
	if len(convErrs) > 0 {
		s.stat.mu.Lock()
		s.stat.DocsSkipped += int64(len(convErrs))
		s.stat.mu.Unlock()
	}
	return ops, append(entries, convErrs...), ignored, nil
}

// watchLock stops the stream if the advisory-lock session dies.
func (s *stream) watchLock(ctx context.Context, lock *pgstore.StreamLock) error {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := lock.Ping(pctx)
			cancel()
			if err != nil && ctx.Err() == nil {
				return fmt.Errorf("lost advisory lock connection: %w", err)
			}
		}
	}
}

func backoff(r config.RetryConfig, attempt int) time.Duration {
	d := time.Duration(r.InitialBackoffMs) * time.Millisecond
	for i := 1; i < attempt; i++ {
		d *= 2
		if max := time.Duration(r.MaxBackoffMs) * time.Millisecond; d > max {
			return max
		}
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// safe converts a panic in a goroutine into an error so it reaches the
// crash path (and the crash email) instead of killing the process silently.
func safe(fn func() error) func() error {
	return func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return fn()
	}
}
