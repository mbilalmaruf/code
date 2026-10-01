# CLAUDE.md — cipher_replicator

Guidance for working in this repo. Part 1 describes the **legacy** replicator as it is today. Part 2 describes the **target** system we are building to replace it. Don't extend the legacy code unless asked to. Read it to understand how it behaves, then build the new design.

---

## Part 1 — Legacy system (as-is)

### What it does
This Node.js service replicates Hyperledger Fabric world-state into a relational DB (Postgres or MSSQL). It turns nested JSON documents into flat columns. It can optionally publish each change to RabbitMQ. It also has a **reverse** mode that reads rows queued in the SQL DB and submits them back to the chain through an HTTP endpoint.

### Running
- The real entry point is `process.js` (`npm run process`). `npm start` points to `app.js`, which **does not exist**.
- No tests or lint are configured.
- Health endpoint: `GET :9581/health` (port is hard-coded).
- Docker: `Dockerfile` (old alpine node 8 base), plus `DockerFiles/*.rhel|alpine`.
- `config/config.json` and `lib/config/config.json` contain only Key Vault bootstrap credentials. They are gitignored and dockerignored, but the files exist locally. Never print or commit their contents. The same applies to `eventConnect/certs/*` (an AMQP client key) and `lib/db/root.crt.pem`.

### Process model
```
process.js (parent, express :9581)
 ├─ lib/db/config.js: POST keyVault.url {env, header} → full runtime config JSON (cached as global.config)
 ├─ replicatorMode = "forward" (default)
 │    POST fetchNetworkURL → Fabric network profile (global.netConfig)
 │    fork child/index.js once per replicatorConfig[i]   (auto-restart 3s after exit)
 └─ replicatorMode = "reverse"
      schedulers/dataTableScheduler.js → fork controllers/DataTable.controller.js per reverseReplicatorConfig[i]
```
- Every `terminateInterval` ms (default 60–80s), the parent **sends SIGINT to all forward children**, and the close handler respawns them. The design relies on these restarts to recover from stalls.
- Config lives only in globals (`global.config`, `global.netConfig`). `lib/config/index.js` captures `global.config` **at require time**, so require order matters.
- All secrets in the config (`connectionString`, `mssqlConfig`, `mongodb`, `amqp.url`, …) are AES-256-GCM blobs `{encryptedData, iv, authTag}` and are decrypted by `lib/helpers/crypto.js#decrypt`. The encrypt side still uses `crypto.createCipher`, which Node 22 removed.

### Forward child (`child/index.js`)
1. Loads the schema profile from **MongoDB** collection `SchemaProfile` by `schemaProfileName` (`eventConnect/schemaProfile.js`). The profile contains:
   - `schemaConfig[]`: target tables. Each has `{name, columns[{name, path, typeData{type,length,typeOfArray}}], otherOptions{indexes, freezeTableName}, encryption?}`. `path` is a lodash/flat dotted path into the source doc. The column `name` often contains dots, for example `accidentDetail.accidentNo`. See `newschema.json` for a real example.
   - `schema[]`: an older duplicate used by `setUpDBTables` to define Sequelize models.
2. `lib/start.js#server()`:
   - Sequelize-defines `meta`, `errors`, and the retry queue table (`replicatorQueueName`, columns from `lib/schemaConfig.json`).
   - Runs `sequelize.sync()`.
   - Then `updateSchemaFromConfig()` issues raw `CREATE TABLE … (id SERIAL)` and `ALTER TABLE … ADD COLUMN IF NOT EXISTS` statements per column. This handles additive changes only: no type changes, drops or renames.
3. `eventConnect/eventEmitter.js#listenPeerEvents` picks a source based on `replicatorSource`:
   | value | behaviour |
   |---|---|
   | `EVENTHUB_GETDATABYKEY` / `EVENTHUB_FETCHFROMCOUCH` | Fabric block listener (`fabric-network` gateway). For each chaincode event with `events[]`, push to `queueEvents` (fastq). The worker calls `GetDataByKey` on the chaincode, or reads CouchDB directly (`query-transaction.js#queryCouch`). |
   | `COUCH_FOLLOW` | Lists CouchDB DBs matching `<channel>_<chaincode>$$p<collection>` (private data collections), then runs `follow` on each DB's `_changes` feed with `since = meta.seek`. If `isEncrypted`, it re-reads the doc via chaincode. `entity`/`user` docs are also upserted into Mongo. |
   Custom events (`typeDataSync`, `stateSetTransitionSync`, `errorCodeSync`, `entitySync`) sync reference data into **Mongo**, not SQL.
4. `ProcessData` wraps each doc as `{_metaData, eventData, extra:{tranxData}}`. Depending on `replicatorTarget` (`EVENT_QUEUE_AND_DB` | `EVENT_QUEUE` | `DB_ONLY`), it publishes to the AMQP fanout exchange `logs` (only for collections listed in `eventToReplicate`) and/or calls `start.eventHandle`.
5. `start.eventHandle` flattens the doc, maps `columns[].path` → column, and is meant to upsert the row into the table named by `DocumentName`.

### Checkpointing (`meta` table)
- Unique key: `(name, pvtCollection)`. The checkpoint is stored in `seek`.
- Block mode: `name = pvtCollection = <channel>_<chaincode>` and `seek = "<blockNum>_<eventIndex>"`.
- Couch mode: `name = <smartContract>`, `pvtCollection = <channel>_<Collection>` and `seek = CouchDB update_seq`.
- `meta.upsert` runs **after** the target write, in a separate statement with no shared transaction. On failure, a row goes to `meta_proxy` (whose create step is commented out) and the child calls `process.exit(1)`.
- Retry queue (`replicatorQueueName` table): `QueryQueue` polls rows where `isprocessed=false AND retry<=retryCnt`, then re-queries the chain. This only re-fetches the data and does not re-write it.

### Reverse replication (`controllers/`, `services/DataTable.service.js`)
- Each child runs a cron (`cronInterval`, default every 5s). It reads `PENDING` rows from `reversereplicatorqueues` for its `documentName`, de-duplicates them by `uniqueIdentifierValue`, and groups them by `smartContractFunction:collection`.
- It then builds a `SELECT` with optional joins (`params.fields`, `params.join`) and POSTs the rows to `reverseReplicationSubmitEndpoint`. If `errorCode==200`, it sets the status to `POSTED`.
- When the forward replicator later sees the chain event carrying `replicatorID`, it sets the status to `CONFIRMED`. Statuses are defined in `utils/constants.js`.

### Known defects (verified in code — don't carry these patterns forward)
- **`start.eventHandle` never writes the replicated row.** The `flag.upsert(data)` call is commented out, so forward replication only updates the reverse-queue status.
- `dbType` (`postgres|mssql`) and `db` (`pg|mssql`) are two different config keys with different vocabularies:
  - `buildPSQLDynamically` checks `dbType === "pg"`, so on Postgres it emits MSSQL `IF NOT EXISTS … ALTER TABLE [x]` SQL.
  - `dbHelper.executeQuery` needs `'pg'`, but `getParamPlaceholder` only accepts `'postgres'`. Every parameterised reverse query on Postgres therefore throws.
- With `fastq` concurrency 5, checkpoints can commit out of order. A later seek can be saved before an earlier one fails, which loses data on restart. In block mode, `evt.events.forEach(async …)` is not awaited.
- `workerEvent` calls `toString(error)` where `error` is undefined. The custom-event branch calls `_.set(element, …)` where `element` is undefined. `checkKeyAndCollectionExist` always returns `false` because its `return` is inside a `forEach`. `eh.connect(true)` refers to a removed event hub. The `oracle` branch uses an undefined `SequelizeOracle`.
- Decrypted connection strings are written to stdout (`lib/db/postgres.js`, `rawPostgres.js`, `eventEmitter.js`).
- Dead or stale files: `lib/start__.js`, `*/dist/*.dev.js`, `utils/index.js` (requires `../../app.js`), `otput*.txt` (old Trivy scan output), `Dockerfile_OLD.rhelv1`.

---

## Part 2 — Target system (what we are building)

### Goals
1. **Simple config in MongoDB.** Each concern is a small, validated document instead of one Key Vault blob plus a separate schema profile.
2. **CouchDB → Postgres streaming, denormalised.** Read the CouchDB `_changes` feed for each Fabric state DB / private-data collection. Project each doc into flat typed columns, with an optional raw `jsonb` copy, and upsert by key.
3. **Checkpoints live in Postgres, in the same transaction as the data.** Rows and checkpoint commit together, which gives effectively-once delivery and no drift between them.
4. Postgres is the only target. Drop the MSSQL and Oracle code paths.

### Proposed Mongo config model (sketch — refine before coding)
- `sources`: `{_id, kind:"couchdb", url(secretRef), auth(secretRef), dbPattern | dbs[], batchSize, includeDocs:true}`
- `targets`: `{_id, kind:"postgres", dsn(secretRef), schema}`
- `pipelines`: `{_id, sourceId, targetId, couchDb, enabled, docFilter:{field:"documentName", in:[…]}, mappings:[mappingId]}`
- `mappings`: `{_id, documentName, table, primaryKey:["key"], keepRaw:true, columns:[{name, path, type, nullable}], indexes:[…], version}`

Secrets should be references (env or vault keys), not inline ciphertext. Validate config with a schema such as zod or JSON Schema when it loads and when it changes.

### Postgres meta schema (owned by the replicator)
```sql
create schema if not exists replicator;
create table replicator.checkpoint (
  pipeline_id text primary key,
  couch_db    text not null,
  last_seq    text not null,          -- CouchDB seq is opaque; store as text
  updated_at  timestamptz not null default now()
);
create table replicator.applied_mapping (   -- which mapping version produced each table's DDL
  table_name text primary key, mapping_id text, version int, ddl_hash text, applied_at timestamptz);
create table replicator.dead_letter (
  id bigserial primary key, pipeline_id text, doc_id text, seq text, doc jsonb, error text, created_at timestamptz default now());
```
**Batch loop per pipeline:**
1. Read `last_seq`.
2. Call `_changes?since=…&include_docs=true&limit=N` with a longpoll, or a continuous feed buffered into batches.
3. Transform each doc. Docs that fail go to `dead_letter`.
4. In **one transaction**: `INSERT … ON CONFLICT (pk) DO UPDATE` for each target table, mark deletes (`_deleted`) or delete them, then `UPDATE checkpoint SET last_seq = <batch last_seq>`.

Each pipeline needs a single writer. Use `pg_try_advisory_lock(hashtext(pipeline_id))`, or rely on Temporal's workflow-ID uniqueness.

Every upsert must be idempotent. Because CouchDB `_changes` delivers at-least-once, replaying a batch must be harmless. Add `_rev` and `_seq` columns, and skip an update when the incoming `_rev` generation is not newer.

### Temporal — recommendation
Use Temporal to orchestrate the pipelines, but don't make every change a workflow step:
- **Don't** model each CouchDB change as an activity. History size and per-step latency are too heavy for a hot feed.
- **Do** run one long-lived workflow per pipeline (`workflowId = pipeline:<id>`). Its single **long-running activity** runs the batch loop above:
  - It sends a heartbeat with `last_seq` after each committed batch, and recovers from the PG checkpoint, which is the source of truth, not the heartbeat.
  - The workflow uses `continue-as-new` periodically and listens for **signals** (`pause`, `resume`, `reloadConfig`).
  - This replaces the legacy "SIGINT every 60s" restart hack, the fork-per-config model and the hand-rolled restart timers.
- Temporal is a good fit for the control-plane jobs:
  - **backfill / resync** of a table (`since=0` into a shadow table, then swap)
  - **schema migration** when a mapping version changes
  - **dead-letter replay**
  - **reverse replication** (Postgres → chain submit), which needs retries plus a wait-for-confirmation step (a signal or a polled activity) with a timeout.
- Without Temporal, the same design works as a plain worker process using the advisory lock for leadership, plus node-schedule or k8s CronJobs for backfills. Start with whichever ops can support. The checkpoint and transaction design does not change.
- References in this workspace:
  - `../samples-go/` (Temporal Go samples): see `polling/`, `batch-sliding-window/`, `child-workflow-continue-as-new/`, `schedule/`, `retryactivity/`.
  - `../fabric-gateway/`: the current Fabric Gateway SDK, which replaces the deprecated `fabric-network`/`fabric-client` used here, if chain access is still needed for encrypted docs or reverse submit.

### Rules when writing the new service
- Postgres only. Use parameterised SQL everywhere. Quote identifiers with a helper, never with string concat of config values.
- Generate DDL from the mapping and record it in `replicator.applied_mapping`. Additive changes are automatic. Type changes or drops need an explicit migration job.
- Column names come from config. Normalise dotted names, for example `accidentDetail.accidentNo` becomes `accident_detail__accident_no`, or keep them quoted, but do so consistently.
- Never log decrypted secrets or full documents at info level.
- Encrypted or private docs (`isEncrypted`, org filtering by `orgCode`) need an explicit design decision. The legacy behaviour is to re-query the chaincode via `GetDataByKey` with transient args `[key, orgCode, documentName]`.
