# CLAUDE.md — workspace root

This folder holds several separate projects. Most current work is the **CouchDB → Postgres replicator rewrite in Go** (`cipher-replicator-go/`).

| Folder | What it is | Ours? |
|---|---|---|
| `cipher-replicator-go/` | **New** replicator (Go). Streams CouchDB `_changes` into denormalised Postgres tables. | yes — active |
| `cipher_replicator/` | **Legacy** replicator (Node.js). Reference for behaviour only; do not extend. See `cipher_replicator/CLAUDE.md`. | yes — legacy |
| `DocumentVerify/` | Hyperledger Fabric network on OKE plus API/web UI (separate product). | yes — unrelated |
| `fabric-gateway/` | Upstream Hyperledger Fabric Gateway SDK (git clone). | no — reference |
| `samples-go/` | Upstream Temporal Go samples (git clone). | no — reference |

Decision: **no Temporal.** It was considered and judged overkill. The replicator is a plain long-running Go service.

---

## cipher-replicator-go

### What it does
1. Loads the **app config** (`configs/app.json`). It holds the Postgres connection string, CouchDB URL, encryption key, email settings, replication-log settings and tuning.
2. Loads every **process config** (`configs/processes/*.json`). Each one lists its CouchDB source databases and its own table schema, in the legacy `SchemaProfile` shape (`schemaConfig[].columns[].{name,path,typeData}`). The only supported mode is `streaming`.
3. Runs a **startup phase, all or nothing**:
   - validate every config
   - connect to CouchDB, Postgres and (optionally) Mongo
   - create the meta tables
   - create or alter every target table (additive DDL)
   - introspect the actual column types
   - read every checkpoint

   If any step fails, nothing replicates.
4. Starts **one stream per (process, CouchDB database)**. Each stream:
   - follows the continuous `_changes` feed with `include_docs`
   - batches changes by count or time
   - writes rows **and its checkpoint in one Postgres transaction**
5. On any unrecoverable error, all streams stop, a **crash email** is sent, and the process exits with code 1. The orchestrator (k8s, systemd, …) restarts it, and it resumes from the committed checkpoint.
6. Optionally writes the **last N replications (default 1000)** to a MongoDB **capped collection**. Set `replicationLog.enabled` to turn this on.

### Commands (run from `cipher-replicator-go/`, using PowerShell or cmd)
Go is at `C:\Program Files\Go\bin\go.exe` (1.27.1). It is not on Git Bash's PATH, so use PowerShell.
```powershell
go test ./...                                              # unit tests (no DB needed)
go vet ./...
go build -buildvcs=false -o replicator.exe ./cmd/replicator   # -buildvcs=false: git + svn both detected on this machine
.\replicator.exe validate        [-config configs/app.json]  # load + validate all configs, no connections
.\replicator.exe                 [-config configs/app.json]  # run (env REPLICATOR_CONFIG also works)
$env:REPLICATOR_ENCRYPTION_KEY="..."; "plaintext" | .\replicator.exe encrypt   # make an encrypted config value
```
The config path is relative to the **current directory**. Running from elsewhere fails with `open configs/app.json: The system cannot find the path specified`. Paths inside the app config, such as `processConfigs.dir`, are relative to the app config file.

### Layout
```
cmd/replicator/main.go        run | validate | encrypt; crash handler (log + email + exit 1, also on panic)
internal/config/              app.go (AppConfig, defaults, secret resolution, validation)
                              process.go (ProcessConfig, ProcessSource interface, FileSource, PrepareAll)
internal/secret/              legacy-compatible AES-256-GCM secrets (key = sha256(encryptionKey))
internal/schema/types.go      config type → Postgres type (STRING→varchar(255), DATE→timestamptz, JSON→jsonb, ARRAY…)
internal/transform/           doc decode, legacy path envelope, JSON → text conversion per column kind
internal/couch/               minimal CouchDB client: ping, _all_dbs, update_seq, continuous _changes + stall watchdog
internal/pgstore/             ddl.go (meta + table DDL, ownership, introspection)
                              writer.go (upsert/delete SQL, ApplyBatch = rows + checkpoint in one tx, error classes)
                              lock.go (per-stream advisory lock)
internal/replicator/          replicator.go (startup orchestration, /healthz, /status), stream.go (reader/writer loop)
internal/replog/              Mongo capped-collection replication log (async, best effort)
internal/notify/              SMTP crash email (starttls | tls | none)
configs/app.example.json      annotated defaults; app.json and processes/*.json are gitignored (credentials)
configs/examples/             example process config (trimmed legacy "claim" profile)
```

### Postgres model
- **Meta schema** (`postgres.metaSchema`, default `replicator`):
  - `checkpoint(process_name, couch_db, last_seq, docs_processed, updated_at)`
  - `table_owner(table_schema, table_name, process_name, config_hash, updated_at)`. A table belongs to exactly one process, and startup fails if a different process claims it.
- **Every target table** has these system columns: `id bigserial`, `_couch_db`, `_couch_id`, `_couch_rev`, `_couch_seq`, `_deleted`, `_replicated_at`. There is a unique index on `(_couch_db, _couch_id)`, which is the upsert key. These names are reserved and cannot be used as config columns.
- **DDL is additive only**: `CREATE TABLE/ADD COLUMN/CREATE INDEX IF NOT EXISTS`, run under a global advisory xact lock. Type changes, drops and renames are not automatic. If an existing column's type differs from the config, startup logs a **drift warning** and values are cast to the *existing* type. This keeps tables created by the legacy replicator working.
- **Write path**: every value is sent as text and cast in SQL (`$n::text::<actual type>`; arrays via `jsonb_array_elements_text`), so Postgres does the parsing.
  - Upserts only apply when the incoming `_rev` generation is greater than or equal to the stored one, so replaying after a crash is harmless.
  - Deletes (tombstones) follow `target.deleteMode`: `soft` sets `_deleted=true` across all of the process's tables, `hard` runs `DELETE`, and `ignore` skips them.
- **Single writer**: each stream holds a session advisory lock on `(process, db)`, pinned on one pool connection and pinged every 30s. A second instance fails at startup. The pool size is at least `2*streams+2`.

### Document mapping (legacy-compatible)
- A doc is routed to the table whose `documentNames` (default `[name]`) contains the doc's name. The name is taken from the first non-empty field in `source.documentNameFields` (default `documentName`, `DocumentName`).
- Unrouted docs and `_design/*` docs are ignored, but the checkpoint still advances.
- `columns[].path` defaults to `name`. Paths support `a.b`, `a[0].b` and `a.0.b`, and a literal top-level key containing dots wins over traversal.
- The legacy envelope keys are available as paths:
  - `tranxData`: the whole doc minus `_rev` and `~version`
  - `key`, `DocumentKey`, `txnid`: `documentKey | key | Key | _id`
  - `DocumentName`
  - `status` = `"VALID"`
- Conversions:
  - Empty strings become NULL for non-text types.
  - Epoch numbers for DATE columns are read as ms if greater than 1e11, otherwise as seconds.
  - Stringified JSON in JSON columns is stored parsed.
  - Booleans accept `true/false/yes/no/1/0`.
- Legacy fields that are accepted but **ignored**: `encryption`, `freezeTableName`, `allowNull`, `defaultValue`, `unique` on columns. Use `otherOptions.indexes` for uniqueness.

### Error policy
- **Transient errors** (network, CouchDB 5xx/429, PG connection, serialization or deadlock) are retried with exponential backoff up to `replication.retry.maxAttempts` consecutive failures, then the service crashes.
- **Non-transient errors** (bad config, CouchDB 4xx, DDL failure, lost advisory lock) crash immediately.
- **Bad documents** follow `replication.onRowError`:
  - `fail` (default): crash, with the doc id and seq in the email.
  - `skip`: each row runs under a savepoint. SQLSTATE class 22/23 errors and conversion errors are skipped, logged, and recorded in the replication log with `action:"error"`.
- A clean shutdown (Ctrl+C or SIGTERM) sends no email. The uncommitted batch is dropped and replayed on the next start.
- The replication log (Mongo) is non-fatal at runtime: entries are queued, dropped if the queue is full, and written only after the PG commit. It *is* fatal at startup if enabled but unreachable.

### Secrets
Any secret field (`postgres.connString`, `couchdb.url|username|password`, `email.username|password`, `replicationLog.mongoUri`) can be:
- a plain string, or
- the legacy blob `{"encryptedData","iv","authTag"}` (hex), also as a stringified blob or wrapped as `{"url":{…}}`.

The key comes from `encryptionKey` or env `REPLICATOR_ENCRYPTION_KEY`; the env var wins. It is compatible with the legacy `cryptoTemp`, verified by `internal/secret/testdata/node_fixture.json` (generated by `gen_fixture.js` with Node). Never log resolved secrets. The CouchDB client strips credentials from the URL (use `Redacted()` in logs).

### Status (2026-10-01)
- Done:
  - all packages compile
  - `go vet` clean
  - unit tests pass (secret, config, transform, couch feed parsing and stall detection, DDL and upsert SQL generation)
  - `validate` works against the example configs
- **Not yet verified against real services.** No Postgres, CouchDB, Mongo or SMTP was available locally, so the DDL execution, `ApplyBatch`, advisory locking, the capped collection and email sending have only been checked for compilation and generated SQL. The next step is an end-to-end run against dev instances.
- Next / planned:
  - Mongo-backed `ProcessSource`, so process configs come from Mongo instead of files. The `config.ProcessSource` interface is already in place; implement `Load(ctx)` and wire it in `cmd/replicator/main.go`.
  - An integration test (gated by an env DSN) for `pgstore` and a full stream.
  - Dockerfile.
  - Maybe: a backfill or resync command, and pickup of new CouchDB databases at runtime (`databasePattern` is only resolved at startup).

### Conventions
- Postgres is the only target. Always use parameterised SQL. Quote identifiers with `pgx.Identifier` (`ident`/`qualified` in pgstore), and never concatenate raw config values.
- Keep the startup order: validate everything, then DDL, then checkpoints, then streams. Never start a stream before all tables are ready.
- Any new failure mode must either be classified as transient (retry) or return an error up to `main` (crash + email). Never swallow it silently.
