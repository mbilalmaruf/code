package transform

import (
	"testing"

	"cipher-replicator/internal/schema"
)

func mustType(t *testing.T, typ string, length int, elem string) schema.ColumnType {
	t.Helper()
	ct, err := schema.Resolve(typ, length, elem)
	if err != nil {
		t.Fatal(err)
	}
	return ct
}

func TestDecodeEnvelopeAndLookup(t *testing.T) {
	raw := []byte(`{"_id":"K1","_rev":"3-a","~version":"x","documentName":"claim","key":"K1",
		"accidentDetail":{"accidentNo":"A9","parts":[{"n":1},{"n":2}]},"amount":12.50,
		"accidentDetail.literal":"dotted-key"}`)
	d, err := Decode(raw, []string{"documentName", "DocumentName"})
	if err != nil {
		t.Fatal(err)
	}
	if d.DocumentName != "claim" {
		t.Fatalf("documentName = %q", d.DocumentName)
	}
	cases := map[string]any{
		"key":                       "K1",
		"txnid":                     "K1",
		"status":                    "VALID",
		"accidentDetail.accidentNo": "A9",
		"accidentDetail.parts[1].n": "2",
		"accidentDetail.parts.0.n":  "1",
		"accidentDetail.literal":    "dotted-key",
	}
	for path, want := range cases {
		v, ok := d.Lookup(path)
		if !ok {
			t.Errorf("%s: not found", path)
			continue
		}
		if got := toString(v); got != want {
			t.Errorf("%s = %v, want %v", path, got, want)
		}
	}
	tx, _ := d.Lookup("tranxData")
	m := tx.(map[string]any)
	if _, ok := m["_rev"]; ok {
		t.Error("tranxData must not contain _rev")
	}
	if _, ok := m["~version"]; ok {
		t.Error("tranxData must not contain ~version")
	}
	if _, ok := d.Lookup("missing.path"); ok {
		t.Error("missing path should not resolve")
	}
}

func toString(v any) string {
	s, err := Convert(v, schema.ColumnType{Kind: schema.KindText})
	if err != nil || s == nil {
		return "<nil>"
	}
	return *s
}

func TestConvert(t *testing.T) {
	d, _ := Decode([]byte(`{"i":"42","f":3.0,"bad":"x","n":12.5,"b":"yes","e":"",
		"ms":1700000000000,"obj":{"a":1},"js":"{\"a\":2}","arr":[1,2,3],"sarr":"[4,5]"}`), nil)
	get := func(p string) any { v, _ := d.Lookup(p); return v }

	type tc struct {
		path    string
		ct      schema.ColumnType
		want    string // "<nil>" for NULL
		wantErr bool
	}
	cases := []tc{
		{"i", mustType(t, "INTEGER", 0, ""), "42", false},
		{"f", mustType(t, "BIGINT", 0, ""), "3", false},
		{"n", mustType(t, "INTEGER", 0, ""), "", true},
		{"bad", mustType(t, "DECIMAL", 0, ""), "", true},
		{"n", mustType(t, "DECIMAL", 0, ""), "12.5", false},
		{"b", mustType(t, "BOOLEAN", 0, ""), "true", false},
		{"e", mustType(t, "DATE", 0, ""), "<nil>", false},
		{"ms", mustType(t, "DATE", 0, ""), "2023-11-14T22:13:20Z", false},
		{"obj", mustType(t, "JSON", 0, ""), `{"a":1}`, false},
		{"js", mustType(t, "JSON", 0, ""), `{"a":2}`, false},
		{"obj", mustType(t, "STRING", 0, ""), `{"a":1}`, false},
		{"arr", mustType(t, "ARRAY", 0, "BIGINT"), `["1","2","3"]`, false},
		{"sarr", mustType(t, "ARRAY", 0, "INTEGER"), `["4","5"]`, false},
		{"bad", mustType(t, "ARRAY", 0, "INTEGER"), "", true},
	}
	for _, c := range cases {
		got, err := Convert(get(c.path), c.ct)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s as %s: expected error, got %v", c.path, c.ct.SQL, deref(got))
			}
			continue
		}
		if err != nil {
			t.Errorf("%s as %s: %v", c.path, c.ct.SQL, err)
			continue
		}
		if deref(got) != c.want {
			t.Errorf("%s as %s = %s, want %s", c.path, c.ct.SQL, deref(got), c.want)
		}
	}
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func TestProjectColumnError(t *testing.T) {
	d, _ := Decode([]byte(`{"amount":"abc"}`), nil)
	_, err := Project(d, []Column{{Name: "amount", Path: "amount", Type: mustType(t, "DECIMAL", 0, "")}})
	var ce *ColumnError
	if err == nil || !asColumnError(err, &ce) || ce.Column != "amount" {
		t.Fatalf("want ColumnError for amount, got %v", err)
	}
}

func asColumnError(err error, target **ColumnError) bool {
	ce, ok := err.(*ColumnError)
	if ok {
		*target = ce
	}
	return ok
}
