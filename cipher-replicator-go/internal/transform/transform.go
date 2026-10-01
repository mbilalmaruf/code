// Package transform turns a CouchDB document into column values for a table.
//
// Path resolution follows the legacy replicator's envelope so existing
// schema profiles keep working: document fields are at the top level, plus
//
//	tranxData    the whole document (without _rev and ~version)
//	key          documentKey | key | Key | _id
//	DocumentKey  same as key
//	txnid        same as key
//	DocumentName the routed document name
//	status       "VALID"
//
// Every value is converted to a string (or NULL) and cast by Postgres to the
// column's actual type, so Postgres stays the single authority on parsing.
package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"cipher-replicator/internal/schema"
)

// Doc is a decoded CouchDB document plus the legacy envelope.
type Doc struct {
	Fields       map[string]any
	DocumentName string
	overrides    map[string]any
}

// Decode parses raw JSON (numbers kept exact) and builds the envelope.
// nameFields are tried in order to find the document name.
func Decode(raw []byte, nameFields []string) (*Doc, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode document: %w", err)
	}
	d := &Doc{Fields: m}
	for _, f := range nameFields {
		if s, ok := m[f].(string); ok && s != "" {
			d.DocumentName = s
			break
		}
	}
	key := ""
	for _, f := range []string{"documentKey", "key", "Key", "_id"} {
		if v, ok := m[f]; ok && v != nil {
			key = fmt.Sprint(v)
			if key != "" {
				break
			}
		}
	}
	tranx := make(map[string]any, len(m))
	for k, v := range m {
		if k == "_rev" || k == "~version" {
			continue
		}
		tranx[k] = v
	}
	d.overrides = map[string]any{
		"tranxData":    tranx,
		"key":          key,
		"DocumentKey":  key,
		"txnid":        key,
		"DocumentName": d.DocumentName,
		"status":       "VALID",
	}
	return d, nil
}

// Lookup resolves a dotted path ("a.b", "a[0].b", "a.0.b"). A top-level key
// that literally contains dots wins over traversal.
func (d *Doc) Lookup(path string) (any, bool) {
	if v, ok := d.overrides[path]; ok {
		return v, true
	}
	if v, ok := d.Fields[path]; ok {
		return v, true
	}
	segs := splitPath(path)
	if len(segs) == 0 {
		return nil, false
	}
	var cur any
	if v, ok := d.overrides[segs[0]]; ok {
		cur = v
	} else if v, ok := d.Fields[segs[0]]; ok {
		cur = v
	} else {
		return nil, false
	}
	for _, s := range segs[1:] {
		switch c := cur.(type) {
		case map[string]any:
			v, ok := c[s]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(s)
			if err != nil || i < 0 || i >= len(c) {
				return nil, false
			}
			cur = c[i]
		default:
			return nil, false
		}
	}
	return cur, true
}

func splitPath(p string) []string {
	p = strings.ReplaceAll(p, "[", ".")
	p = strings.ReplaceAll(p, "]", "")
	var out []string
	for _, s := range strings.Split(p, ".") {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Convert renders v as the text Postgres will cast to the column type.
// It returns nil for SQL NULL.
func Convert(v any, ct schema.ColumnType) (*string, error) {
	if v == nil {
		return nil, nil
	}
	str := func(s string) (*string, error) { return &s, nil }

	switch ct.Kind {
	case schema.KindText:
		switch x := v.(type) {
		case string:
			return str(x)
		case json.Number:
			return str(x.String())
		case bool:
			return str(strconv.FormatBool(x))
		default:
			b, err := marshal(x)
			if err != nil {
				return nil, err
			}
			return str(string(b))
		}

	case schema.KindJSON:
		if s, ok := v.(string); ok {
			// Fabric payloads often carry stringified JSON; store it parsed.
			t := strings.TrimSpace(s)
			if (strings.HasPrefix(t, "{") || strings.HasPrefix(t, "[")) && json.Valid([]byte(t)) {
				return str(t)
			}
		}
		b, err := marshal(v)
		if err != nil {
			return nil, err
		}
		return str(string(b))

	case schema.KindArray:
		arr, ok := v.([]any)
		if !ok {
			if s, isStr := v.(string); isStr {
				if strings.TrimSpace(s) == "" {
					return nil, nil
				}
				var parsed []any
				dec := json.NewDecoder(strings.NewReader(s))
				dec.UseNumber()
				if err := dec.Decode(&parsed); err == nil {
					arr = parsed
					ok = true
				}
			}
		}
		if !ok {
			return nil, fmt.Errorf("expected array, got %T", v)
		}
		// Elements are converted individually so bad elements fail here
		// with a clear message instead of as a cast error in Postgres.
		elems := make([]any, len(arr))
		for i, e := range arr {
			s, err := Convert(e, schema.ColumnType{Kind: ct.Elem})
			if err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			if s != nil {
				elems[i] = *s
			}
		}
		b, err := marshal(elems)
		if err != nil {
			return nil, err
		}
		return str(string(b))
	}

	// Scalars below: empty strings mean NULL (legacy updateEmptyDates).
	if s, ok := v.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, nil
		}
		v = s
	}

	switch ct.Kind {
	case schema.KindInt:
		switch x := v.(type) {
		case json.Number:
			return intText(x.String())
		case string:
			return intText(x)
		}
	case schema.KindNumeric, schema.KindFloat:
		var s string
		switch x := v.(type) {
		case json.Number:
			s = x.String()
		case string:
			s = x
		default:
			return nil, fmt.Errorf("expected number, got %T", v)
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil && !isRangeErr(err) {
			return nil, fmt.Errorf("invalid number %q", s)
		}
		return str(s)
	case schema.KindBool:
		switch x := v.(type) {
		case bool:
			return str(strconv.FormatBool(x))
		case json.Number:
			switch x.String() {
			case "0":
				return str("false")
			case "1":
				return str("true")
			}
		case string:
			switch strings.ToLower(x) {
			case "true", "t", "yes", "y", "1":
				return str("true")
			case "false", "f", "no", "n", "0":
				return str("false")
			}
		}
		return nil, fmt.Errorf("invalid boolean %v", v)
	case schema.KindDate, schema.KindTimestamp:
		switch x := v.(type) {
		case string:
			return str(x)
		case json.Number:
			return epochText(x)
		}
	case schema.KindTime:
		if s, ok := v.(string); ok {
			return str(s)
		}
	}
	return nil, fmt.Errorf("cannot convert %T to %s", v, ct.Kind)
}

func intText(s string) (*string, error) {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &s, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f != math.Trunc(f) || math.IsInf(f, 0) {
		return nil, fmt.Errorf("invalid integer %q", s)
	}
	out := strconv.FormatFloat(f, 'f', 0, 64)
	return &out, nil
}

// epochText treats numbers as Unix time: milliseconds when > 1e11, else seconds.
func epochText(n json.Number) (*string, error) {
	f, err := n.Float64()
	if err != nil {
		return nil, fmt.Errorf("invalid epoch %q", n)
	}
	var t time.Time
	if math.Abs(f) > 1e11 {
		t = time.UnixMilli(int64(f))
	} else {
		t = time.Unix(int64(f), int64((f-math.Trunc(f))*1e9))
	}
	s := t.UTC().Format(time.RFC3339Nano)
	return &s, nil
}

func isRangeErr(err error) bool {
	ne, ok := err.(*strconv.NumError)
	return ok && ne.Err == strconv.ErrRange
}

func marshal(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Row is a document projected onto a table's columns, in column order.
type Row struct {
	Values []*string
}

// ColumnError identifies the column a conversion failed on.
type ColumnError struct {
	Column string
	Err    error
}

func (e *ColumnError) Error() string { return fmt.Sprintf("column %q: %v", e.Column, e.Err) }
func (e *ColumnError) Unwrap() error { return e.Err }

// Column is the minimal column description Project needs.
type Column struct {
	Name string
	Path string
	Type schema.ColumnType
}

// Project builds the row for doc. Missing paths become NULL.
func Project(doc *Doc, cols []Column) (Row, error) {
	r := Row{Values: make([]*string, len(cols))}
	for i, c := range cols {
		v, ok := doc.Lookup(c.Path)
		if !ok {
			continue
		}
		s, err := Convert(v, c.Type)
		if err != nil {
			return Row{}, &ColumnError{Column: c.Name, Err: err}
		}
		r.Values[i] = s
	}
	return r, nil
}
