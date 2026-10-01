package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"cipher-replicator/internal/schema"
)

// ModeStreaming is the only supported mode: follow CouchDB _changes continuously.
const ModeStreaming = "streaming"

// Delete handling for CouchDB tombstones.
const (
	DeleteSoft   = "soft"   // set _deleted = true
	DeleteHard   = "hard"   // DELETE the row
	DeleteIgnore = "ignore" // leave the row untouched
)

// SystemColumns are added to every replicated table and are reserved.
var SystemColumns = []string{"id", "_couch_db", "_couch_id", "_couch_rev", "_couch_seq", "_deleted", "_replicated_at"}

// ProcessConfig is one replication pipeline: a set of CouchDB databases whose
// documents are routed by document name into the tables of SchemaConfig.
// The table part keeps the legacy SchemaProfile shape so profiles can be
// copied across (minus Mongo shell syntax such as ObjectId(...)).
type ProcessConfig struct {
	Name         string              `json:"name"`
	Enabled      *bool               `json:"enabled"`
	Mode         string              `json:"mode"`
	Source       SourceConfig        `json:"source"`
	Target       TargetConfig        `json:"target"`
	Replication  *ReplicationOverride `json:"replication"`
	SchemaConfig []TableConfig       `json:"schemaConfig"`

	// Origin is where the config was loaded from (file path), for messages.
	Origin string `json:"-"`
}

type SourceConfig struct {
	// Databases lists CouchDB database names explicitly (e.g. Fabric
	// "mychannel_mycc$$pcollection"). Each must exist at startup.
	Databases []string `json:"databases"`
	// DatabasePattern is a regexp matched against _all_dbs at startup.
	DatabasePattern string `json:"databasePattern"`
	// DocumentNameFields are tried in order to find a doc's document name.
	DocumentNameFields []string `json:"documentNameFields"`
	// StartFrom applies when no checkpoint exists: "0" (default, full
	// history) or "now" (only new changes).
	StartFrom string `json:"startFrom"`
}

type TargetConfig struct {
	Schema     string `json:"schema"`
	DeleteMode string `json:"deleteMode"`
}

type ReplicationOverride struct {
	BatchSize       int `json:"batchSize"`
	FlushIntervalMs int `json:"flushIntervalMs"`
}

type TableConfig struct {
	Name string `json:"name"`
	// DocumentNames that route to this table; defaults to [Name].
	DocumentNames []string      `json:"documentNames"`
	Columns       []ColumnConfig `json:"columns"`
	OtherOptions  OtherOptions   `json:"otherOptions"`
}

type ColumnConfig struct {
	Name string `json:"name"`
	// Path into the document (dotted, a[0] or a.0 for arrays). Defaults to Name.
	Path     string   `json:"path"`
	TypeData TypeData `json:"typeData"`

	Type schema.ColumnType `json:"-"`
}

type TypeData struct {
	Type        string `json:"type"`
	Length      int    `json:"length"`
	TypeOfArray string `json:"typeOfArray"`
}

type OtherOptions struct {
	Indexes []IndexConfig `json:"indexes"`
}

type IndexConfig struct {
	Name   string       `json:"name"`
	Unique bool         `json:"unique"`
	Fields []IndexField `json:"fields"`
}

// IndexField accepts "col" or Sequelize's {"name": "col"} / {"attribute": "col"}.
type IndexField string

func (f *IndexField) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = IndexField(s)
		return nil
	}
	var o struct {
		Name      string `json:"name"`
		Attribute string `json:"attribute"`
	}
	if err := json.Unmarshal(b, &o); err != nil {
		return err
	}
	if o.Name == "" {
		o.Name = o.Attribute
	}
	if o.Name == "" {
		return errors.New("index field object needs name or attribute")
	}
	*f = IndexField(o.Name)
	return nil
}

func (p *ProcessConfig) IsEnabled() bool { return p.Enabled == nil || *p.Enabled }

// BatchSize returns the effective batch size for this process.
func (p *ProcessConfig) BatchSize(d ReplicationDefaults) int {
	if p.Replication != nil && p.Replication.BatchSize > 0 {
		return p.Replication.BatchSize
	}
	return d.BatchSize
}

// FlushIntervalMs returns the effective flush interval for this process.
func (p *ProcessConfig) FlushIntervalMs(d ReplicationDefaults) int {
	if p.Replication != nil && p.Replication.FlushIntervalMs > 0 {
		return p.Replication.FlushIntervalMs
	}
	return d.FlushIntervalMs
}

// ProcessSource loads process configs. FileSource is the current
// implementation; a Mongo-backed source will implement the same interface.
type ProcessSource interface {
	Load(ctx context.Context) ([]ProcessConfig, error)
}

// FileSource loads every *.json in Dir (sorted) plus each of Files.
type FileSource struct {
	Dir   string
	Files []string
}

func NewFileSource(app *AppConfig) *FileSource {
	fs := &FileSource{Dir: app.ResolvePath(app.ProcessConfigs.Dir)}
	for _, f := range app.ProcessConfigs.Files {
		fs.Files = append(fs.Files, app.ResolvePath(f))
	}
	return fs
}

func (s *FileSource) Load(_ context.Context) ([]ProcessConfig, error) {
	var paths []string
	if s.Dir != "" {
		m, err := filepath.Glob(filepath.Join(s.Dir, "*.json"))
		if err != nil {
			return nil, err
		}
		sort.Strings(m)
		paths = append(paths, m...)
	}
	paths = append(paths, s.Files...)
	if len(paths) == 0 {
		return nil, fmt.Errorf("no process config files found (dir=%q)", s.Dir)
	}
	var out []ProcessConfig
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			return nil, fmt.Errorf("read process config: %w", err)
		}
		var pc ProcessConfig
		if err := json.Unmarshal(raw, &pc); err != nil {
			return nil, fmt.Errorf("parse process config %s: %w", p, err)
		}
		pc.Origin = p
		out = append(out, pc)
	}
	return out, nil
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

func validIdent(s string) bool { return len(s) > 0 && len(s) <= 63 && identRe.MatchString(s) }

// validColumnName allows the legacy dotted names (e.g. "accidentDetail.accidentNo");
// all identifiers are quoted in SQL so only length and control chars matter.
func validColumnName(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == '"' {
			return false
		}
	}
	return true
}

// ApplyDefaults fills defaults and resolves column types.
func (p *ProcessConfig) ApplyDefaults() {
	if p.Mode == "" {
		p.Mode = ModeStreaming
	}
	if len(p.Source.DocumentNameFields) == 0 {
		p.Source.DocumentNameFields = []string{"documentName", "DocumentName"}
	}
	if p.Source.StartFrom == "" {
		p.Source.StartFrom = "0"
	}
	if p.Target.Schema == "" {
		p.Target.Schema = "public"
	}
	if p.Target.DeleteMode == "" {
		p.Target.DeleteMode = DeleteSoft
	}
	for i := range p.SchemaConfig {
		t := &p.SchemaConfig[i]
		if len(t.DocumentNames) == 0 {
			t.DocumentNames = []string{t.Name}
		}
		for j := range t.Columns {
			c := &t.Columns[j]
			if c.Path == "" {
				c.Path = c.Name
			}
		}
	}
}

// Validate checks one process config. Call ApplyDefaults first.
func (p *ProcessConfig) Validate() error {
	var errs []error
	add := func(format string, a ...any) {
		errs = append(errs, fmt.Errorf(format, a...))
	}
	if !validIdent(p.Name) {
		add("name %q must match %s", p.Name, identRe)
	}
	if p.Mode != ModeStreaming {
		add("mode %q is not supported (only %q)", p.Mode, ModeStreaming)
	}
	if len(p.Source.Databases) == 0 && p.Source.DatabasePattern == "" {
		add("source.databases or source.databasePattern is required")
	}
	if p.Source.DatabasePattern != "" {
		if _, err := regexp.Compile(p.Source.DatabasePattern); err != nil {
			add("source.databasePattern: %v", err)
		}
	}
	if p.Source.StartFrom != "0" && p.Source.StartFrom != "now" {
		add(`source.startFrom must be "0" or "now"`)
	}
	if !validIdent(p.Target.Schema) {
		add("target.schema %q is not a valid identifier", p.Target.Schema)
	}
	switch p.Target.DeleteMode {
	case DeleteSoft, DeleteHard, DeleteIgnore:
	default:
		add("target.deleteMode must be soft, hard or ignore")
	}
	if len(p.SchemaConfig) == 0 {
		add("schemaConfig must define at least one table")
	}
	reserved := map[string]bool{}
	for _, s := range SystemColumns {
		reserved[s] = true
	}
	tables := map[string]bool{}
	docNames := map[string]string{}
	for i := range p.SchemaConfig {
		t := &p.SchemaConfig[i]
		if !validIdent(t.Name) {
			add("schemaConfig[%d].name %q is not a valid identifier", i, t.Name)
		}
		if tables[t.Name] {
			add("table %q defined twice", t.Name)
		}
		tables[t.Name] = true
		for _, dn := range t.DocumentNames {
			if other, ok := docNames[dn]; ok {
				add("document name %q routes to both %q and %q", dn, other, t.Name)
			}
			docNames[dn] = t.Name
		}
		if len(t.Columns) == 0 {
			add("table %q has no columns", t.Name)
		}
		cols := map[string]bool{}
		for j := range t.Columns {
			c := &t.Columns[j]
			if !validColumnName(c.Name) {
				add("table %q column %q: invalid name", t.Name, c.Name)
			}
			if reserved[c.Name] {
				add("table %q column %q: name is reserved for system columns", t.Name, c.Name)
			}
			if cols[c.Name] {
				add("table %q column %q defined twice", t.Name, c.Name)
			}
			cols[c.Name] = true
			ct, err := schema.Resolve(c.TypeData.Type, c.TypeData.Length, c.TypeData.TypeOfArray)
			if err != nil {
				add("table %q column %q: %v", t.Name, c.Name, err)
			}
			c.Type = ct
		}
		for k, ix := range t.OtherOptions.Indexes {
			if len(ix.Fields) == 0 {
				add("table %q index %d has no fields", t.Name, k)
			}
			for _, f := range ix.Fields {
				if !cols[string(f)] && !reserved[string(f)] {
					add("table %q index %d references unknown column %q", t.Name, k, f)
				}
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("process %q (%s): %w", p.Name, p.Origin, errors.Join(errs...))
	}
	return nil
}

// PrepareAll applies defaults, validates each config and checks cross-process
// conflicts: unique process names and a single owning process per table.
func PrepareAll(pcs []ProcessConfig) error {
	var errs []error
	names := map[string]string{}
	owners := map[string]string{}
	for i := range pcs {
		p := &pcs[i]
		p.ApplyDefaults()
		if err := p.Validate(); err != nil {
			errs = append(errs, err)
			continue
		}
		if prev, ok := names[p.Name]; ok {
			errs = append(errs, fmt.Errorf("process name %q used by %s and %s", p.Name, prev, p.Origin))
		}
		names[p.Name] = p.Origin
		if !p.IsEnabled() {
			continue
		}
		for _, t := range p.SchemaConfig {
			key := p.Target.Schema + "." + t.Name // quoted identifiers are case-sensitive
			if prev, ok := owners[key]; ok && prev != p.Name {
				errs = append(errs, fmt.Errorf("table %s is targeted by processes %q and %q", key, prev, p.Name))
			}
			owners[key] = p.Name
		}
	}
	return errors.Join(errs...)
}
