// Package pgstore owns everything the replicator does in Postgres: its meta
// tables (checkpoints, table ownership), dynamic DDL for target tables and
// the per-batch upsert/delete/checkpoint transaction.
package pgstore

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"cipher-replicator/internal/config"
)

// ident quotes a single identifier.
func ident(s string) string { return pgx.Identifier{s}.Sanitize() }

// qualified quotes schema.name.
func qualified(schema, name string) string { return pgx.Identifier{schema, name}.Sanitize() }

// MetaDDL returns the statements that create the replicator's meta tables.
func MetaDDL(metaSchema string) []string {
	s := ident(metaSchema)
	return []string{
		`CREATE SCHEMA IF NOT EXISTS ` + s,
		`CREATE TABLE IF NOT EXISTS ` + s + `."checkpoint" (
			process_name   text        NOT NULL,
			couch_db       text        NOT NULL,
			last_seq       text        NOT NULL,
			docs_processed bigint      NOT NULL DEFAULT 0,
			updated_at     timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (process_name, couch_db)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s + `."table_owner" (
			table_schema  text        NOT NULL,
			table_name    text        NOT NULL,
			process_name  text        NOT NULL,
			config_hash   text        NOT NULL,
			updated_at    timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (table_schema, table_name)
		)`,
	}
}

// EnsureMeta creates the meta schema and tables if missing.
func EnsureMeta(ctx context.Context, pool *pgxpool.Pool, metaSchema string) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('cipher-replicator:ddl'))`); err != nil {
			return err
		}
		for _, q := range MetaDDL(metaSchema) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("meta ddl: %w", err)
			}
		}
		return nil
	})
}

// TableDDL returns the idempotent statements that create table t or bring an
// existing table up to date (additive only: new columns and indexes).
func TableDDL(schema string, t *config.TableConfig) []string {
	q := qualified(schema, t.Name)
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + ident(schema),
		`CREATE TABLE IF NOT EXISTS ` + q + ` (
			"id"             bigserial PRIMARY KEY,
			"_couch_db"      text,
			"_couch_id"      text,
			"_couch_rev"     text,
			"_couch_seq"     text,
			"_deleted"       boolean     NOT NULL DEFAULT false,
			"_replicated_at" timestamptz NOT NULL DEFAULT now()
		)`,
	}
	// Tables created by the legacy replicator lack the system columns.
	adds := []string{
		`ADD COLUMN IF NOT EXISTS "_couch_db" text`,
		`ADD COLUMN IF NOT EXISTS "_couch_id" text`,
		`ADD COLUMN IF NOT EXISTS "_couch_rev" text`,
		`ADD COLUMN IF NOT EXISTS "_couch_seq" text`,
		`ADD COLUMN IF NOT EXISTS "_deleted" boolean NOT NULL DEFAULT false`,
		`ADD COLUMN IF NOT EXISTS "_replicated_at" timestamptz NOT NULL DEFAULT now()`,
	}
	for _, c := range t.Columns {
		adds = append(adds, `ADD COLUMN IF NOT EXISTS `+ident(c.Name)+` `+c.Type.SQL)
	}
	stmts = append(stmts, `ALTER TABLE `+q+"\n  "+strings.Join(adds, ",\n  "))
	stmts = append(stmts, `CREATE UNIQUE INDEX IF NOT EXISTS `+ident(indexName(t.Name, "couch_doc", true))+
		` ON `+q+` ("_couch_db", "_couch_id")`)
	for _, ix := range t.OtherOptions.Indexes {
		fields := make([]string, len(ix.Fields))
		raw := make([]string, len(ix.Fields))
		for i, f := range ix.Fields {
			fields[i] = ident(string(f))
			raw[i] = string(f)
		}
		name := ix.Name
		if name == "" {
			name = indexName(t.Name, strings.Join(raw, "_"), ix.Unique)
		}
		kind := "INDEX"
		if ix.Unique {
			kind = "UNIQUE INDEX"
		}
		stmts = append(stmts, `CREATE `+kind+` IF NOT EXISTS `+ident(name)+` ON `+q+` (`+strings.Join(fields, ", ")+`)`)
	}
	return stmts
}

// indexName builds a deterministic name within Postgres' 63-byte limit.
func indexName(table, cols string, unique bool) string {
	suffix := "_idx"
	if unique {
		suffix = "_uk"
	}
	n := table + "_" + cols + suffix
	if len(n) <= 63 {
		return n
	}
	h := sha1.Sum([]byte(n))
	return n[:63-len(suffix)-9] + "_" + hex.EncodeToString(h[:4]) + suffix
}

// ConfigHash fingerprints a table config, recorded in table_owner.
func ConfigHash(t *config.TableConfig) string {
	b, _ := json.Marshal(t)
	h := sha1.Sum(b)
	return hex.EncodeToString(h[:])
}

// EnsureTable applies TableDDL and claims ownership of the table for
// process. It fails if another process already owns the table.
func EnsureTable(ctx context.Context, pool *pgxpool.Pool, metaSchema, process, schema string, t *config.TableConfig) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('cipher-replicator:ddl'))`); err != nil {
			return err
		}
		var owner string
		err := tx.QueryRow(ctx, `SELECT process_name FROM `+ident(metaSchema)+`."table_owner"
			WHERE table_schema = $1 AND table_name = $2`, schema, t.Name).Scan(&owner)
		if err != nil && err != pgx.ErrNoRows {
			return err
		}
		if err == nil && owner != process {
			return fmt.Errorf("table %s.%s is owned by process %q (see %s.table_owner)", schema, t.Name, owner, metaSchema)
		}
		for _, q := range TableDDL(schema, t) {
			if _, err := tx.Exec(ctx, q); err != nil {
				return fmt.Errorf("ddl for %s.%s: %w\n%s", schema, t.Name, err, q)
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO `+ident(metaSchema)+`."table_owner"
			(table_schema, table_name, process_name, config_hash, updated_at) VALUES ($1, $2, $3, $4, now())
			ON CONFLICT (table_schema, table_name) DO UPDATE
			SET config_hash = EXCLUDED.config_hash, updated_at = now()`,
			schema, t.Name, process, ConfigHash(t))
		return err
	})
}

// ColumnTypes returns format_type() of every column of schema.table.
func ColumnTypes(ctx context.Context, pool *pgxpool.Pool, schema, table string) (map[string]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a
		WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped`,
		qualified(schema, table))
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("table %s.%s not found after DDL", schema, table)
	}
	return out, nil
}
