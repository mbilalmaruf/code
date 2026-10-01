package pgstore

import (
	"strings"
	"testing"

	"cipher-replicator/internal/config"
)

func testTable(t *testing.T) *config.TableConfig {
	t.Helper()
	p := config.ProcessConfig{
		Name:   "p",
		Source: config.SourceConfig{Databases: []string{"db"}},
		SchemaConfig: []config.TableConfig{{
			Name: "claim",
			Columns: []config.ColumnConfig{
				{Name: "key", TypeData: config.TypeData{Type: "STRING"}},
				{Name: "accidentDetail.accidentNo", TypeData: config.TypeData{Type: "STRING", Length: 50}},
				{Name: "tags", TypeData: config.TypeData{Type: "ARRAY", TypeOfArray: "INTEGER"}},
			},
			OtherOptions: config.OtherOptions{Indexes: []config.IndexConfig{{Unique: true, Fields: []config.IndexField{"key"}}}},
		}},
	}
	p.ApplyDefaults()
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return &p.SchemaConfig[0]
}

func TestTableDDL(t *testing.T) {
	ddl := strings.Join(TableDDL("public", testTable(t)), ";\n")
	for _, want := range []string{
		`CREATE TABLE IF NOT EXISTS "public"."claim"`,
		`ADD COLUMN IF NOT EXISTS "accidentDetail.accidentNo" varchar(50)`,
		`ADD COLUMN IF NOT EXISTS "tags" integer[]`,
		`CREATE UNIQUE INDEX IF NOT EXISTS "claim_couch_doc_uk" ON "public"."claim" ("_couch_db", "_couch_id")`,
		`CREATE UNIQUE INDEX IF NOT EXISTS "claim_key_uk" ON "public"."claim" ("key")`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q\n%s", want, ddl)
		}
	}
}

func TestNewTableUsesActualTypes(t *testing.T) {
	actual := map[string]string{
		"key":                       "text", // legacy table with a different type
		"accidentDetail.accidentNo": "character varying(50)",
		"tags":                      "integer[]",
	}
	tb, drift, err := NewTable("public", testTable(t), actual, config.DeleteSoft)
	if err != nil {
		t.Fatal(err)
	}
	if len(drift) != 1 || drift[0].Column != "key" {
		t.Fatalf("drift = %+v", drift)
	}
	sql := tb.UpsertSQL()
	for _, want := range []string{
		`$5::text::text`,
		`$6::text::character varying(50)`,
		`ARRAY(SELECT jsonb_array_elements_text($7::text::jsonb))::integer[]`,
		`ON CONFLICT ("_couch_db", "_couch_id") DO UPDATE`,
		`split_part(t."_couch_rev", '-', 1)`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("upsert missing %q\n%s", want, sql)
		}
	}
	if !strings.HasPrefix(tb.DeleteSQL(), "UPDATE") {
		t.Errorf("soft delete should UPDATE: %s", tb.DeleteSQL())
	}
}

func TestIndexNameLimit(t *testing.T) {
	n := indexName(strings.Repeat("t", 60), strings.Repeat("c", 60), true)
	if len(n) > 63 || !strings.HasSuffix(n, "_uk") {
		t.Fatalf("bad index name %q (%d)", n, len(n))
	}
}
