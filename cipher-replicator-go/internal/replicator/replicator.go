// Package replicator wires startup and runs the streams.
//
// Startup is all-or-nothing: load and validate every process config, create
// or alter every target table, read every checkpoint, and only then start
// replicating. Any error at runtime stops all streams and is returned to
// main, which sends the crash email and exits non-zero.
package replicator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"cipher-replicator/internal/config"
	"cipher-replicator/internal/couch"
	"cipher-replicator/internal/pgstore"
	"cipher-replicator/internal/replog"
)

// Run blocks until ctx is cancelled (clean shutdown, returns nil) or a
// stream fails (returns the error).
func Run(ctx context.Context, app *config.AppConfig, src config.ProcessSource, log *slog.Logger) error {
	// 1. Process configs first: cheap to validate, no connections needed.
	procs, err := src.Load(ctx)
	if err != nil {
		return fmt.Errorf("load process configs: %w", err)
	}
	if err := config.PrepareAll(procs); err != nil {
		return fmt.Errorf("invalid process config:\n%w", err)
	}
	var enabled []*config.ProcessConfig
	for i := range procs {
		if procs[i].IsEnabled() {
			enabled = append(enabled, &procs[i])
		} else {
			log.Info("process disabled; skipping", "process", procs[i].Name)
		}
	}
	if len(enabled) == 0 {
		return errors.New("no enabled process configs")
	}

	// 2. Connections.
	cc, err := couch.New(app.CouchDB.URL.String(), app.CouchDB.Username.String(), app.CouchDB.Password.String(),
		time.Duration(app.CouchDB.RequestTimeoutMs)*time.Millisecond, app.CouchDB.InsecureTLS)
	if err != nil {
		return err
	}
	if err := cc.Ping(ctx); err != nil {
		return fmt.Errorf("couchdb %s: %w", cc.Redacted(), err)
	}
	log.Info("couchdb connected", "url", cc.Redacted())

	// Resolve databases before sizing the pool (one pinned lock conn per stream).
	type plan struct {
		proc *config.ProcessConfig
		dbs  []string
	}
	var plans []plan
	streamsCount := 0
	for _, p := range enabled {
		dbs, err := resolveDatabases(ctx, cc, p)
		if err != nil {
			return fmt.Errorf("process %s: %w", p.Name, err)
		}
		plans = append(plans, plan{p, dbs})
		streamsCount += len(dbs)
	}

	pcfg, err := pgxpool.ParseConfig(app.Postgres.ConnString.String())
	if err != nil {
		return errors.New("postgres.connString is invalid") // don't echo: contains credentials
	}
	pcfg.MaxConns = max(app.Postgres.MaxConns, int32(2*streamsCount+2))
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	log.Info("postgres connected", "host", pcfg.ConnConfig.Host, "database", pcfg.ConnConfig.Database, "maxConns", pcfg.MaxConns)

	var rlog replog.Recorder = replog.Nop{}
	if app.ReplicationLog.Enabled {
		l := app.ReplicationLog
		m, err := replog.Open(ctx, replog.Options{
			URI: l.MongoURI.String(), Database: l.Database, Collection: l.Collection,
			MaxEntries: l.MaxEntries, CappedSizeBytes: l.CappedSizeBytes, IncludeDocument: l.IncludeDocument,
		}, log)
		if err != nil {
			return fmt.Errorf("replication log (mongo): %w", err)
		}
		rlog = m
		log.Info("replication log enabled", "database", l.Database, "collection", l.Collection, "maxEntries", l.MaxEntries)
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rlog.Close(cctx)
	}()

	// 3. Meta tables, then every target table, then checkpoints.
	if err := pgstore.EnsureMeta(ctx, pool, app.Postgres.MetaSchema); err != nil {
		return fmt.Errorf("meta tables: %w", err)
	}
	var streams []*stream
	for _, pl := range plans {
		p := pl.proc
		routes := map[string]*pgstore.Table{}
		var tables []*pgstore.Table
		for i := range p.SchemaConfig {
			t := &p.SchemaConfig[i]
			if err := pgstore.EnsureTable(ctx, pool, app.Postgres.MetaSchema, p.Name, p.Target.Schema, t); err != nil {
				return fmt.Errorf("process %s: %w", p.Name, err)
			}
			actual, err := pgstore.ColumnTypes(ctx, pool, p.Target.Schema, t.Name)
			if err != nil {
				return fmt.Errorf("process %s: %w", p.Name, err)
			}
			tbl, drift, err := pgstore.NewTable(p.Target.Schema, t, actual, p.Target.DeleteMode)
			if err != nil {
				return fmt.Errorf("process %s: %w", p.Name, err)
			}
			for _, d := range drift {
				log.Warn("column type differs from config; values are cast to the existing type",
					"process", p.Name, "table", t.Name, "column", d.Column, "configured", d.Configured, "actual", d.Actual)
			}
			tables = append(tables, tbl)
			for _, dn := range t.DocumentNames {
				routes[dn] = tbl
			}
			log.Info("table ready", "process", p.Name, "table", p.Target.Schema+"."+t.Name, "columns", len(t.Columns))
		}
		for _, db := range pl.dbs {
			since, ok, err := pgstore.LoadCheckpoint(ctx, pool, app.Postgres.MetaSchema, p.Name, db)
			if err != nil {
				return fmt.Errorf("process %s: read checkpoint for %s: %w", p.Name, db, err)
			}
			if !ok {
				since = "0"
				if p.Source.StartFrom == "now" {
					if since, _, err = cc.UpdateSeq(ctx, db); err != nil {
						return fmt.Errorf("process %s: %s update_seq: %w", p.Name, db, err)
					}
				}
				log.Info("no checkpoint; starting fresh", "process", p.Name, "couchDb", db, "startFrom", p.Source.StartFrom)
			}
			streams = append(streams, &stream{
				process: p, db: db, since: since, routes: routes, tables: tables,
				metaSchema: app.Postgres.MetaSchema,
				batchSize:  p.BatchSize(app.Replication),
				flushEvery: time.Duration(p.FlushIntervalMs(app.Replication)) * time.Millisecond,
				heartbeat:  time.Duration(app.CouchDB.HeartbeatMs) * time.Millisecond,
				skipBad:    app.Replication.OnRowError == config.OnRowErrorSkip,
				retry:      app.Replication.Retry,
				pool:       pool, couch: cc, rlog: rlog,
				log:  log.With("process", p.Name, "couchDb", db),
				stat: &StreamStatus{Process: p.Name, CouchDB: db, CommittedSeq: since},
			})
		}
	}
	log.Info("startup complete; starting replication", "streams", len(streams))

	// 4. Replicate.
	g, gctx := errgroup.WithContext(ctx)
	if app.HTTP.Listen != "" {
		srv := statusServer(app.HTTP.Listen, streams)
		g.Go(func() error {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("http server: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(sctx)
		})
	}
	for _, s := range streams {
		g.Go(safe(func() error { return s.run(gctx) }))
	}
	err = g.Wait()
	if ctx.Err() != nil {
		log.Info("shutdown complete")
		return nil
	}
	return err
}

// resolveDatabases returns explicit databases (which must exist) plus those
// in _all_dbs matching DatabasePattern, deduplicated and sorted.
func resolveDatabases(ctx context.Context, cc *couch.Client, p *config.ProcessConfig) ([]string, error) {
	set := map[string]bool{}
	for _, db := range p.Source.Databases {
		_, ok, err := cc.UpdateSeq(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("couch db %q: %w", db, err)
		}
		if !ok {
			return nil, fmt.Errorf("couch db %q does not exist", db)
		}
		set[db] = true
	}
	if p.Source.DatabasePattern != "" {
		re := regexp.MustCompile(p.Source.DatabasePattern) // validated earlier
		all, err := cc.AllDBs(ctx)
		if err != nil {
			return nil, fmt.Errorf("list couch dbs: %w", err)
		}
		for _, db := range all {
			if re.MatchString(db) {
				set[db] = true
			}
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("no couch databases matched (databases=%v, pattern=%q)", p.Source.Databases, p.Source.DatabasePattern)
	}
	out := make([]string, 0, len(set))
	for db := range set {
		out = append(out, db)
	}
	sort.Strings(out)
	return out, nil
}

func statusServer(addr string, streams []*stream) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		out := make([]StreamStatus, 0, len(streams))
		for _, s := range streams {
			out = append(out, s.stat.snapshot())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
