// Package schema maps legacy Sequelize-style column types (as used in the
// SchemaProfile "typeData.type") onto Postgres types.
package schema

import (
	"fmt"
	"strings"
)

// Kind drives value conversion from JSON to the text sent to Postgres.
type Kind int

const (
	KindText Kind = iota
	KindInt
	KindNumeric
	KindFloat
	KindBool
	KindDate      // date only
	KindTimestamp // timestamptz
	KindTime
	KindJSON
	KindArray
)

func (k Kind) String() string {
	return [...]string{"text", "int", "numeric", "float", "bool", "date", "timestamp", "time", "json", "array"}[k]
}

// ColumnType is a resolved column type.
type ColumnType struct {
	Kind Kind
	// SQL is the DDL type, e.g. "varchar(255)".
	SQL string
	// Canonical is what Postgres' format_type() reports for SQL, used to
	// detect drift between config and an existing table.
	Canonical string
	// Elem is the element kind for KindArray.
	Elem Kind
}

type base struct {
	kind      Kind
	sql       string
	canonical string
}

var scalar = map[string]base{
	"TEXT":      {KindText, "text", "text"},
	"UUID":      {KindText, "uuid", "uuid"},
	"SMALLINT":  {KindInt, "smallint", "smallint"},
	"INTEGER":   {KindInt, "integer", "integer"},
	"INT":       {KindInt, "integer", "integer"},
	"BIGINT":    {KindInt, "bigint", "bigint"},
	"DECIMAL":   {KindNumeric, "numeric", "numeric"},
	"NUMERIC":   {KindNumeric, "numeric", "numeric"},
	"FLOAT":     {KindFloat, "double precision", "double precision"},
	"DOUBLE":    {KindFloat, "double precision", "double precision"},
	"REAL":      {KindFloat, "real", "real"},
	"BOOLEAN":   {KindBool, "boolean", "boolean"},
	"BOOL":      {KindBool, "boolean", "boolean"},
	"DATEONLY":  {KindDate, "date", "date"},
	"DATE":      {KindTimestamp, "timestamptz", "timestamp with time zone"}, // Sequelize DATE = timestamptz
	"DATETIME":  {KindTimestamp, "timestamptz", "timestamp with time zone"},
	"TIMESTAMP": {KindTimestamp, "timestamptz", "timestamp with time zone"},
	"TIME":      {KindTime, "time", "time without time zone"},
	"JSON":      {KindJSON, "jsonb", "jsonb"},
	"JSONB":     {KindJSON, "jsonb", "jsonb"},
}

// Resolve maps a config type (case-insensitive) to a ColumnType.
// length applies to STRING/VARCHAR/CHAR; typeOfArray applies to ARRAY.
func Resolve(typ string, length int, typeOfArray string) (ColumnType, error) {
	t := strings.ToUpper(strings.TrimSpace(typ))
	switch t {
	case "STRING", "VARCHAR", "NVARCHAR":
		n := length
		if n <= 0 {
			n = 255
		}
		return ColumnType{Kind: KindText, SQL: fmt.Sprintf("varchar(%d)", n), Canonical: fmt.Sprintf("character varying(%d)", n)}, nil
	case "CHAR", "NCHAR":
		n := length
		if n <= 0 {
			n = 255
		}
		return ColumnType{Kind: KindText, SQL: fmt.Sprintf("char(%d)", n), Canonical: fmt.Sprintf("character(%d)", n)}, nil
	case "ARRAY":
		if strings.TrimSpace(typeOfArray) == "" {
			// Legacy stored untyped arrays as JSON text; jsonb is the native fit.
			b := scalar["JSONB"]
			return ColumnType{Kind: KindJSON, SQL: b.sql, Canonical: b.canonical}, nil
		}
		elem, err := Resolve(typeOfArray, 0, "")
		if err != nil {
			return ColumnType{}, fmt.Errorf("typeOfArray: %w", err)
		}
		if elem.Kind == KindArray || elem.Kind == KindJSON {
			return ColumnType{}, fmt.Errorf("unsupported typeOfArray %q", typeOfArray)
		}
		if elem.Kind == KindText {
			// varchar(n)[] adds nothing over text[] for replicated data.
			elem.SQL, elem.Canonical = "text", "text"
		}
		return ColumnType{Kind: KindArray, SQL: elem.SQL + "[]", Canonical: elem.Canonical + "[]", Elem: elem.Kind}, nil
	}
	b, ok := scalar[t]
	if !ok {
		return ColumnType{}, fmt.Errorf("unsupported column type %q", typ)
	}
	return ColumnType{Kind: b.kind, SQL: b.sql, Canonical: b.canonical}, nil
}
