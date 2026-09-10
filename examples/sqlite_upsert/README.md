# sqlite_upsert — SQLite outlet via `SqliteConsumer` (contrib: `sql_upsert`)

The SQL upsert implementation (engines / upsert / backfill / dialects /
classifier) is a first-class contrib module: `streamgate.contrib.sql_upsert`,
with `streamgate.contrib.sqlite_upsert` and `streamgate.contrib.mssql_upsert`
as per-backend entry packages shipping pre-assembled `Consumer` factories.
The framework core is storage-agnostic: `Consumer(handler=...)` accepts any
async callable — SQL upsert is just the bundled shortcut.

```bash
pip install "streamgate[sql]"
```

## Layout

| Module | Contents |
|--------|----------|
| `streamgate.contrib.sqlite_upsert` | SQLite entry (`aiosqlite`): `SqliteConsumer` factory, `ON CONFLICT` upsert, DELETE+INSERT fallback, auto create-table with WAL PRAGMAs |
| `streamgate.contrib.mssql_upsert` | MSSQL entry (`aioodbc`): `MssqlConsumer` factory, multi-row `MERGE` + HOLDLOCK, param-budget chunking, SQL Server error-number classifier (schema managed by your migration tool) |
| `streamgate.contrib.sql_upsert` | shared base: `Upsert` declaration, `upsert_outlet` primitive (batch handler + single-record probe), engine factory, `SqlBackfill`, `SQLAlchemyErrorClassifier`, `DbConfig` |
| `models.py`, `produce.py`, `consume.py` | runnable demo (stays in the repo, not in the wheel) |

## Run

```bash
# infrastructure
docker compose up -d                # from examples/docker-compose.yml

pip install "streamgate[sql]"

# consume (terminal 1)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py

# produce (terminal 2)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

# verify
sqlite3 data/streamgate.db "select * from orders;"
```

SQL Server: set `DB_CONN='mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server'`
(the `[sql]` extra already includes `aioodbc`).

## What the factory pre-wires

`SqliteConsumer(...)` returns a core `Consumer` with three things already
connected (every one overridable via `options=...`):

- `handler` — idempotent batch upsert (single transaction per batch);
- `probe` — single-record upsert with identical semantics, so a POISON
  batch is located record-by-record and quarantined precisely to the DLQ;
- `classifier` — `SQLAlchemyErrorClassifier` (transient vs. data errors).

## Why inject a classifier when self-assembling?

The framework's `DefaultErrorClassifier` only recognizes generic exceptions
and maps unknown errors to `POISON` (→ probe-located DLQ quarantine). A
connection-pool timeout from your DB is transient and should be `RETRY`
(paused self-healing) — only a DB-aware classifier can tell. When you build
the outlet yourself with `upsert_outlet`, inject
`streamgate.contrib.sql_upsert.SQLAlchemyErrorClassifier` via
`ConsumerOptions(classifier=...)`.
