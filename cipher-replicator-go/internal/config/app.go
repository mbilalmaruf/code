// Package config defines the application config (connections, email,
// replication log, tuning) and the per-process replication configs
// (source CouchDB databases + target table schema).
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cipher-replicator/internal/secret"
)

// EnvEncryptionKey overrides AppConfig.EncryptionKey when set.
const EnvEncryptionKey = "REPLICATOR_ENCRYPTION_KEY"

type AppConfig struct {
	InstanceName   string               `json:"instanceName"`
	EncryptionKey  string               `json:"encryptionKey"`
	Postgres       PostgresConfig       `json:"postgres"`
	CouchDB        CouchDBConfig        `json:"couchdb"`
	ProcessConfigs ProcessConfigSource  `json:"processConfigs"`
	Replication    ReplicationDefaults  `json:"replication"`
	ReplicationLog ReplicationLogConfig `json:"replicationLog"`
	Email          EmailConfig          `json:"email"`
	HTTP           HTTPConfig           `json:"http"`
	Log            LogConfig            `json:"log"`

	// baseDir is the directory of the app config file; relative paths resolve against it.
	baseDir string
}

type PostgresConfig struct {
	ConnString secret.Value `json:"connString"`
	MaxConns   int32        `json:"maxConns"`
	// MetaSchema holds the replicator's own tables (checkpoint, table ownership).
	MetaSchema string `json:"metaSchema"`
}

type CouchDBConfig struct {
	// URL may embed credentials (https://user:pass@host:5984); they are moved
	// to basic auth and never logged.
	URL              secret.Value `json:"url"`
	Username         secret.Value `json:"username"`
	Password         secret.Value `json:"password"`
	RequestTimeoutMs int          `json:"requestTimeoutMs"`
	HeartbeatMs      int          `json:"heartbeatMs"`
	InsecureTLS      bool         `json:"insecureSkipVerify"`
}

// ProcessConfigSource says where process configs come from. Today: JSON
// files. Later: a MongoDB collection (see ProcessSource interface).
type ProcessConfigSource struct {
	Dir   string   `json:"dir"`
	Files []string `json:"files"`
}

type ReplicationDefaults struct {
	BatchSize       int `json:"batchSize"`
	FlushIntervalMs int `json:"flushIntervalMs"`
	// OnRowError: "fail" (default) crashes the service on a document that
	// cannot be converted/written; "skip" records it in the replication log
	// and moves on.
	OnRowError string      `json:"onRowError"`
	Retry      RetryConfig `json:"retry"`
}

type RetryConfig struct {
	// MaxAttempts is the number of consecutive transient failures (network,
	// connection loss) tolerated before the service crashes.
	MaxAttempts      int `json:"maxAttempts"`
	InitialBackoffMs int `json:"initialBackoffMs"`
	MaxBackoffMs     int `json:"maxBackoffMs"`
}

type ReplicationLogConfig struct {
	Enabled         bool         `json:"enabled"`
	MongoURI        secret.Value `json:"mongoUri"`
	Database        string       `json:"database"`
	Collection      string       `json:"collection"`
	MaxEntries      int64        `json:"maxEntries"`
	CappedSizeBytes int64        `json:"cappedSizeBytes"`
	IncludeDocument bool         `json:"includeDocument"`
}

type EmailConfig struct {
	Enabled       bool         `json:"enabled"`
	Host          string       `json:"host"`
	Port          int          `json:"port"`
	Username      secret.Value `json:"username"`
	Password      secret.Value `json:"password"`
	From          string       `json:"from"`
	To            []string     `json:"to"`
	TLSMode       string       `json:"tlsMode"` // starttls (default) | tls | none
	InsecureTLS   bool         `json:"insecureSkipVerify"`
	SubjectPrefix string       `json:"subjectPrefix"`
	TimeoutMs     int          `json:"timeoutMs"`
}

type HTTPConfig struct {
	// Listen address for /healthz and /status; empty disables the server.
	Listen string `json:"listen"`
}

type LogConfig struct {
	Level  string `json:"level"`  // debug | info | warn | error
	Format string `json:"format"` // json | text
}

// LoadApp reads, defaults, resolves secrets and validates the app config.
// On a validation/secret error it still returns the parsed config (with
// whatever could be resolved) so the caller can try to send a crash email.
func LoadApp(path string) (*AppConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read app config: %w", err)
	}
	var c AppConfig
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse app config %s: %w", path, err)
	}
	abs, _ := filepath.Abs(path)
	c.baseDir = filepath.Dir(abs)
	if k := os.Getenv(EnvEncryptionKey); k != "" {
		c.EncryptionKey = k
	}
	c.applyDefaults()
	if err := c.resolveSecrets(); err != nil {
		return &c, err
	}
	return &c, c.validate()
}

func (c *AppConfig) applyDefaults() {
	if c.InstanceName == "" {
		h, _ := os.Hostname()
		c.InstanceName = "cipher-replicator@" + h
	}
	if c.Postgres.MaxConns == 0 {
		c.Postgres.MaxConns = 10
	}
	if c.Postgres.MetaSchema == "" {
		c.Postgres.MetaSchema = "replicator"
	}
	if c.CouchDB.RequestTimeoutMs == 0 {
		c.CouchDB.RequestTimeoutMs = 30000
	}
	if c.CouchDB.HeartbeatMs == 0 {
		c.CouchDB.HeartbeatMs = 30000
	}
	r := &c.Replication
	if r.BatchSize == 0 {
		r.BatchSize = 500
	}
	if r.FlushIntervalMs == 0 {
		r.FlushIntervalMs = 1000
	}
	if r.OnRowError == "" {
		r.OnRowError = OnRowErrorFail
	}
	if r.Retry.MaxAttempts == 0 {
		r.Retry.MaxAttempts = 5
	}
	if r.Retry.InitialBackoffMs == 0 {
		r.Retry.InitialBackoffMs = 1000
	}
	if r.Retry.MaxBackoffMs == 0 {
		r.Retry.MaxBackoffMs = 30000
	}
	l := &c.ReplicationLog
	if l.Database == "" {
		l.Database = "cipher_replicator"
	}
	if l.Collection == "" {
		l.Collection = "replication_log"
	}
	if l.MaxEntries == 0 {
		l.MaxEntries = 1000
	}
	if l.CappedSizeBytes == 0 {
		l.CappedSizeBytes = 64 << 20
	}
	e := &c.Email
	if e.TLSMode == "" {
		e.TLSMode = "starttls"
	}
	if e.Port == 0 {
		e.Port = 587
	}
	if e.SubjectPrefix == "" {
		e.SubjectPrefix = "[cipher-replicator]"
	}
	if e.TimeoutMs == 0 {
		e.TimeoutMs = 30000
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.Format == "" {
		c.Log.Format = "json"
	}
	if c.ProcessConfigs.Dir == "" && len(c.ProcessConfigs.Files) == 0 {
		c.ProcessConfigs.Dir = "processes"
	}
}

func (c *AppConfig) resolveSecrets() error {
	// Email first, so a crash email can still be sent if a later secret fails.
	fields := []struct {
		name string
		v    *secret.Value
	}{
		{"email.username", &c.Email.Username},
		{"email.password", &c.Email.Password},
		{"postgres.connString", &c.Postgres.ConnString},
		{"couchdb.url", &c.CouchDB.URL},
		{"couchdb.username", &c.CouchDB.Username},
		{"couchdb.password", &c.CouchDB.Password},
		{"replicationLog.mongoUri", &c.ReplicationLog.MongoURI},
	}
	var errs []error
	for _, f := range fields {
		if err := f.v.Resolve(c.EncryptionKey); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
		}
	}
	return errors.Join(errs...)
}

func (c *AppConfig) validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	if !c.Postgres.ConnString.IsSet() {
		add("postgres.connString is required")
	}
	if !c.CouchDB.URL.IsSet() {
		add("couchdb.url is required")
	}
	if !validIdent(c.Postgres.MetaSchema) {
		add("postgres.metaSchema %q is not a valid identifier", c.Postgres.MetaSchema)
	}
	r := c.Replication
	if r.BatchSize < 1 {
		add("replication.batchSize must be >= 1")
	}
	if r.FlushIntervalMs < 1 {
		add("replication.flushIntervalMs must be >= 1")
	}
	if r.OnRowError != OnRowErrorFail && r.OnRowError != OnRowErrorSkip {
		add("replication.onRowError must be %q or %q", OnRowErrorFail, OnRowErrorSkip)
	}
	if c.ReplicationLog.Enabled {
		if !c.ReplicationLog.MongoURI.IsSet() {
			add("replicationLog.mongoUri is required when replicationLog.enabled")
		}
		if c.ReplicationLog.MaxEntries < 1 {
			add("replicationLog.maxEntries must be >= 1")
		}
	}
	if c.Email.Enabled {
		if c.Email.Host == "" || c.Email.From == "" || len(c.Email.To) == 0 {
			add("email.host, email.from and email.to are required when email.enabled")
		}
		switch c.Email.TLSMode {
		case "starttls", "tls", "none":
		default:
			add("email.tlsMode must be starttls, tls or none")
		}
	}
	switch strings.ToLower(c.Log.Format) {
	case "json", "text":
	default:
		add("log.format must be json or text")
	}
	return errors.Join(errs...)
}

// ResolvePath makes p absolute relative to the app config directory.
func (c *AppConfig) ResolvePath(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(c.baseDir, p)
}

func (r ReplicationDefaults) FlushInterval() time.Duration {
	return time.Duration(r.FlushIntervalMs) * time.Millisecond
}

const (
	OnRowErrorFail = "fail"
	OnRowErrorSkip = "skip"
)
