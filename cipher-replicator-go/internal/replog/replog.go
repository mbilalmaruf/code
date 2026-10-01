// Package replog keeps the most recent replications (default 1000) in a
// MongoDB capped collection. It is best effort: failures are logged and never
// stop replication, and entries are written only after the Postgres commit.
package replog

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Entry is one replicated (or skipped) document.
type Entry struct {
	Process      string    `bson:"process"`
	CouchDB      string    `bson:"couchDb"`
	Seq          string    `bson:"seq"`
	DocID        string    `bson:"docId"`
	Rev          string    `bson:"rev"`
	DocumentName string    `bson:"documentName,omitempty"`
	Table        string    `bson:"table,omitempty"`
	Action       string    `bson:"action"` // upsert | delete | error
	Error        string    `bson:"error,omitempty"`
	Document     bson.Raw  `bson:"document,omitempty"`
	At           time.Time `bson:"at"`

	rawDoc json.RawMessage
}

// WithDocument attaches the source document (stored only if includeDocument).
func (e Entry) WithDocument(raw json.RawMessage) Entry { e.rawDoc = raw; return e }

// Recorder accepts entries after each committed batch.
type Recorder interface {
	Record(entries []Entry)
	Close(ctx context.Context)
}

// Nop is used when the replication log is disabled.
type Nop struct{}

func (Nop) Record([]Entry)          {}
func (Nop) Close(context.Context) {}

type Options struct {
	URI             string
	Database        string
	Collection      string
	MaxEntries      int64
	CappedSizeBytes int64
	IncludeDocument bool
}

type Mongo struct {
	client  *mongo.Client
	coll    *mongo.Collection
	opts    Options
	ch      chan []Entry
	done    chan struct{}
	dropped atomic.Int64
	log     *slog.Logger
}

// Open connects and ensures the capped collection exists.
func Open(ctx context.Context, o Options, log *slog.Logger) (*Mongo, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(o.URI).SetServerSelectionTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	db := client.Database(o.Database)
	if err := ensureCapped(ctx, db, o, log); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	m := &Mongo{
		client: client,
		coll:   db.Collection(o.Collection),
		opts:   o,
		ch:     make(chan []Entry, 256),
		done:   make(chan struct{}),
		log:    log,
	}
	go m.loop()
	return m, nil
}

func ensureCapped(ctx context.Context, db *mongo.Database, o Options, log *slog.Logger) error {
	specs, err := db.ListCollectionSpecifications(ctx, bson.D{{Key: "name", Value: o.Collection}})
	if err != nil {
		return err
	}
	if len(specs) == 0 {
		return db.CreateCollection(ctx, o.Collection,
			options.CreateCollection().SetCapped(true).SetSizeInBytes(o.CappedSizeBytes).SetMaxDocuments(o.MaxEntries))
	}
	var info struct {
		Capped bool  `bson:"capped"`
		Max    int64 `bson:"max"`
	}
	if specs[0].Options != nil {
		_ = bson.Unmarshal(specs[0].Options, &info)
	}
	if !info.Capped {
		log.Warn("replication log collection exists but is not capped; it will grow without bound",
			"collection", o.Collection)
	} else if info.Max != o.MaxEntries {
		log.Warn("replication log collection has a different max; drop it to apply the new limit",
			"collection", o.Collection, "existingMax", info.Max, "configuredMax", o.MaxEntries)
	}
	return nil
}

// Record queues entries without blocking replication; if the queue is full
// the entries are dropped and counted.
func (m *Mongo) Record(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	select {
	case m.ch <- entries:
	default:
		if m.dropped.Add(int64(len(entries))) == int64(len(entries)) {
			m.log.Warn("replication log queue full; dropping entries")
		}
	}
}

func (m *Mongo) loop() {
	defer close(m.done)
	for batch := range m.ch {
		docs := make([]any, 0, len(batch))
		for _, e := range batch {
			if m.opts.IncludeDocument && len(e.rawDoc) > 0 {
				var raw bson.Raw
				if err := bson.UnmarshalExtJSON(e.rawDoc, false, &raw); err == nil {
					e.Document = raw
				}
			}
			docs = append(docs, e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_, err := m.coll.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
		cancel()
		if err != nil {
			m.log.Warn("replication log write failed", "error", err, "entries", len(docs))
		}
	}
}

// Close flushes queued entries (bounded by ctx) and disconnects.
func (m *Mongo) Close(ctx context.Context) {
	close(m.ch)
	select {
	case <-m.done:
	case <-ctx.Done():
	}
	if n := m.dropped.Load(); n > 0 {
		m.log.Warn("replication log dropped entries", "count", n)
	}
	_ = m.client.Disconnect(ctx)
}
