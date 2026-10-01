package config

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExampleConfigsLoad(t *testing.T) {
	app, err := LoadApp(filepath.Join("..", "..", "configs", "app.example.json"))
	if err != nil {
		t.Fatal(err)
	}
	app.ProcessConfigs = ProcessConfigSource{Dir: "examples"}
	procs, err := NewFileSource(app).Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareAll(procs); err != nil {
		t.Fatal(err)
	}
	if procs[0].SchemaConfig[0].Columns[6].Path != "accidentDetail" {
		t.Error("column path should default to its name")
	}
}

func TestValidationErrors(t *testing.T) {
	procs := []ProcessConfig{
		{Name: "a", Mode: "batch", Source: SourceConfig{Databases: []string{"db"}},
			SchemaConfig: []TableConfig{{Name: "t", Columns: []ColumnConfig{
				{Name: "_couch_id", TypeData: TypeData{Type: "STRING"}},
				{Name: "x", TypeData: TypeData{Type: "WAT"}},
			}}}},
		{Name: "b", Source: SourceConfig{Databases: []string{"db"}},
			SchemaConfig: []TableConfig{{Name: "t", Columns: []ColumnConfig{{Name: "y", TypeData: TypeData{Type: "STRING"}}}}}},
	}
	err := PrepareAll(procs)
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"mode", "reserved", "unsupported column type"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q:\n%v", want, err)
		}
	}
}

func TestTableOwnedByTwoProcesses(t *testing.T) {
	mk := func(name string) ProcessConfig {
		return ProcessConfig{Name: name, Source: SourceConfig{Databases: []string{"db"}},
			SchemaConfig: []TableConfig{{Name: "t", Columns: []ColumnConfig{{Name: "y", TypeData: TypeData{Type: "STRING"}}}}}}
	}
	err := PrepareAll([]ProcessConfig{mk("a"), mk("b")})
	if err == nil || !strings.Contains(err.Error(), "targeted by processes") {
		t.Fatalf("want ownership conflict, got %v", err)
	}
}

func TestEncryptedSecretNeedsKey(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "app.json")
	body := `{"postgres":{"connString":{"encryptedData":"00","iv":"00","authTag":"00000000000000000000000000000000"}},"couchdb":{"url":"http://x"}}`
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvEncryptionKey, "")
	if _, err := LoadApp(p); err == nil || !strings.Contains(err.Error(), "no encryption key") {
		t.Fatalf("want missing-key error, got %v", err)
	}
}
