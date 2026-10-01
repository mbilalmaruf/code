package pgstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"cipher-replicator/internal/config"
	"cipher-replicator/internal/transform"
)

// Table is a target table ready for writing: its columns (config order) and
// the prepared SQL text.
type Table struct {
	Schema    string
	Name      string
	Columns   []transform.Column
	upsertSQL string
	deleteSQL string
}

// Drift describes a configured column whose existing Postgres type differs
// from the configured one. Values are still cast to the existing type.
type Drift struct {
	Column, Configured, Actual string
}

// NewTable builds the SQL for t using the column types actually present in
// Postgres (actual, from ColumnTypes), so tables created by the legacy
// replicator with different types keep working.
func NewTable(schema string, t *config.TableConfig, actual map[string]string, deleteMode string) (*Table, []Drift, error) {
	tb := &Table{Schema: schema, Name: t.Name}
	var drift []Drift
	cols := []string{`"_couch_db"`, `"_couch_id"`, `"_couch_rev"`, `"_couch_seq"`, `"_deleted"`, `"_replicated_at"`}
	vals := []string{"$1", "$2", "$3", "$4", "false", "now()"}
	sets := []string{
		`"_couch_rev" = EXCLUDED."_couch_rev"`,
		`"_couch_seq" = EXCLUDED."_couch_seq"`,
		`"_deleted" = false`,
		`"_replicated_at" = now()`,
	}
	for i, c := range t.Columns {
		typ, ok := actual[c.Name]
		if !ok {
			return nil, nil, fmt.Errorf("column %q missing from %s.%s", c.Name, schema, t.Name)
		}
		if typ != c.Type.Canonical {
			drift = append(drift, Drift{Column: c.Name, Configured: c.Type.Canonical, Actual: typ})
		}
		tb.Columns = append(tb.Columns, transform.Column{Name: c.Name, Path: c.Path, Type: c.Type})
		p := fmt.Sprintf("$%d", i+5)
		q := ident(c.Name)
		cols = append(cols, q)
		vals = append(vals, castExpr(p, typ))
		sets = append(sets, q+" = EXCLUDED."+q)
	}
	tbl := qualified(schema, t.Name)
	// Only apply a change whose revision generation is not older than the
	// stored one, so replays after a crash are harmless.
	tb.upsertSQL = `INSERT INTO ` + tbl + ` AS t (` + strings.Join(cols, ", ") + `)
VALUES (` + strings.Join(vals, ", ") + `)
ON CONFLICT ("_couch_db", "_couch_id") DO UPDATE SET ` + strings.Join(sets, ", ") + `
WHERE ` + revGen(`t."_couch_rev"`) + ` <= ` + revGen(`EXCLUDED."_couch_rev"`)

	switch deleteMode {
	case config.DeleteSoft:
		tb.deleteSQL = `UPDATE ` + tbl + ` SET "_deleted" = true, "_couch_rev" = $3, "_couch_seq" = $4, "_replicated_at" = now()
WHERE "_couch_db" = $1 AND "_couch_id" = $2`
	case config.DeleteHard:
		tb.deleteSQL = `DELETE FROM ` + tbl + ` WHERE "_couch_db" = $1 AND "_couch_id" = $2`
	}
	return tb, drift, nil
}

func revGen(col string) string {
	return `COALESCE(NULLIF(split_part(` + col + `, '-', 1), '')::bigint, 0)`
}

// castExpr casts a text parameter to the column type. Arrays arrive as a JSON
// array of strings.
func castExpr(param, typ string) string {
	if strings.HasSuffix(typ, "[]") {
		return `ARRAY(SELECT jsonb_array_elements_text(` + param + `::text::jsonb))::` + typ
	}
	return param + `::text::` + typ
}

// UpsertSQL / DeleteSQL are exposed for tests.
func (t *Table) UpsertSQL() string { return t.upsertSQL }
func (t *Table) DeleteSQL() string { return t.deleteSQL }

// Op is one write in a batch.
type Op struct {
	Table   *Table // nil with Delete=true means "all tables of the process"
	Delete  bool
	DocID   string
	Rev     string
	Seq     string
	Values  []*string
	Context string // for error messages, e.g. doc id + table
}

func (o *Op) args(db string) []any {
	a := make([]any, 0, 4+len(o.Values))
	a = append(a, db, o.DocID, o.Rev, o.Seq)
	for _, v := range o.Values {
		if v == nil {
			a = append(a, nil)
		} else {
			a = append(a, *v)
		}
	}
	return a
}

// OpError is a write failure attributable to one document.
type OpError struct {
	Index int
	Op    *Op
	Err   error
}

func (e *OpError) Error() string { return fmt.Sprintf("%s: %v", e.Op.Context, e.Err) }
func (e *OpError) Unwrap() error { return e.Err }

// Checkpoint is the stream position committed with each batch.
type Checkpoint struct {
	MetaSchema string
	Process    string
	CouchDB    string
	Seq        string
	Docs       int
}

// ApplyBatch writes ops and advances the checkpoint in one transaction.
// deleteTables lists the tables a tombstone applies to.
//
// With skipBadRows=false any failure aborts the batch. With skipBadRows=true
// each op runs under a savepoint; data errors are returned in skipped and the
// batch still commits. Connection-level errors always abort.
func ApplyBatch(ctx context.Context, pool *pgxpool.Pool, db string, ops []Op, deleteTables []*Table, cp Checkpoint, skipBadRows bool) (skipped []OpError, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		skipped = skipped[:0]
		if !skipBadRows {
			b := &pgx.Batch{}
			idx := []int{}
			for i := range ops {
				for _, s := range statementsFor(&ops[i], db, deleteTables) {
					b.Queue(s.sql, s.args...)
					idx = append(idx, i)
				}
			}
			br := tx.SendBatch(ctx, b)
			for _, i := range idx {
				if _, err := br.Exec(); err != nil {
					br.Close()
					return &OpError{Index: i, Op: &ops[i], Err: err}
				}
			}
			if err := br.Close(); err != nil {
				return err
			}
		} else {
			for i := range ops {
				if err := execWithSavepoint(ctx, tx, &ops[i], db, deleteTables); err != nil {
					if !IsDataError(err) {
						return &OpError{Index: i, Op: &ops[i], Err: err}
					}
					skipped = append(skipped, OpError{Index: i, Op: &ops[i], Err: err})
				}
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+ident(cp.MetaSchema)+`."checkpoint" AS c
			(process_name, couch_db, last_seq, docs_processed, updated_at) VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (process_name, couch_db) DO UPDATE
			SET last_seq = EXCLUDED.last_seq, docs_processed = c.docs_processed + EXCLUDED.docs_processed, updated_at = now()`,
			cp.Process, cp.CouchDB, cp.Seq, cp.Docs)
		return err
	})
	return skipped, err
}

type stmt struct {
	sql  string
	args []any
}

func statementsFor(op *Op, db string, deleteTables []*Table) []stmt {
	if !op.Delete {
		return []stmt{{op.Table.upsertSQL, op.args(db)}}
	}
	var out []stmt
	for _, t := range deleteTables {
		switch {
		case t.deleteSQL == "":
		case strings.HasPrefix(t.deleteSQL, "DELETE"):
			out = append(out, stmt{t.deleteSQL, []any{db, op.DocID}})
		default:
			out = append(out, stmt{t.deleteSQL, []any{db, op.DocID, op.Rev, op.Seq}})
		}
	}
	return out
}

func execWithSavepoint(ctx context.Context, tx pgx.Tx, op *Op, db string, deleteTables []*Table) error {
	return pgx.BeginFunc(ctx, tx, func(sp pgx.Tx) error {
		for _, s := range statementsFor(op, db, deleteTables) {
			if _, err := sp.Exec(ctx, s.sql, s.args...); err != nil {
				return err
			}
		}
		return nil
	})
}

func isNetErr(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

// IsDataError reports whether err is caused by the data itself (bad cast,
// constraint, value too long) rather than connectivity or server state.
// SQLSTATE classes: 22 data exception, 23 integrity constraint violation.
func IsDataError(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return strings.HasPrefix(pe.Code, "22") || strings.HasPrefix(pe.Code, "23")
	}
	return false
}

// IsTransient reports whether retrying the batch could succeed: connection
// failures, serialization failures/deadlocks, server shutdown.
func IsTransient(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return strings.HasPrefix(pe.Code, "08") || pe.Code == "40001" || pe.Code == "40P01" ||
			strings.HasPrefix(pe.Code, "57P") || pe.Code == "53300"
	}
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	var ce *pgconn.ConnectError
	return errors.As(err, &ce) || errors.Is(err, context.DeadlineExceeded) || isNetErr(err)
}

// LoadCheckpoint returns the committed seq for (process, db); ok=false if none.
func LoadCheckpoint(ctx context.Context, pool *pgxpool.Pool, metaSchema, process, db string) (seq string, ok bool, err error) {
	err = pool.QueryRow(ctx, `SELECT last_seq FROM `+ident(metaSchema)+`."checkpoint"
		WHERE process_name = $1 AND couch_db = $2`, process, db).Scan(&seq)
	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	return seq, err == nil, err
}
