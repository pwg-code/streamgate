# sqlite_sink — RecordWriter injection with the original SQL upsert sugar

The former built-in `db/` package (engines / upsert / backfill / dialects) and
the `Upsert` declaration now live here as copy-paste local modules. The
framework core is storage-agnostic: `ConsumeSpec(sink=...)` accepts **any**
`RecordWriter`, and SQL upsert is just one reference implementation.

## Layout

| Module | Contents |
|--------|----------|
| `upsert.py` | `Upsert` declaration + `UpsertWriter` (RecordWriter impl, single-transaction multi-target idempotent upsert) |
| `engines.py` | async engine factory (read/write pools, pyodbc statement-timeout hook) |
| `dialects/` | SQLite (`ON CONFLICT`, DELETE+INSERT fallback) and MSSQL (multi-row `MERGE` + HOLDLOCK) statement builders |
| `backfill.py` | `SqlBackfill` (BackfillSource impl for cold-entity load, pairs with `examples/redis_admission/`) |
| `classifier.py` | `SQLAlchemyErrorClassifier` — the **required** DB error classifier (RETRY/POISON mapping incl. SQL Server error numbers) |
| `config.py` | example-local DB connection config |

## Run

```bash
# infrastructure
docker compose up -d                # from examples/docker-compose.yml

# streamgate + this example's extra dependencies
pip install streamgate sqlalchemy sqlmodel aiosqlite

# consume (terminal 1)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py

# produce (terminal 2)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py

# verify
sqlite3 data/streamgate.db "select * from orders;"
```

SQL Server: set `DB_CONN='mssql+aioodbc://user:pass@host/db?driver=ODBC+Driver+18+for+SQL+Server'`
and install `aioodbc` instead of `aiosqlite`.

## Why inject a classifier?

The framework's `DefaultErrorClassifier` only recognizes generic exceptions
and maps unknown errors to `POISON` (→ DLQ bisection). A connection-pool
timeout from your DB is transient and should be `RETRY` (paused self-healing)
— only a DB-aware classifier can tell. Inject it via
`ConsumerWorker(..., error_classifier=...)`; `classifier.py` here is a
production-hardened starting point you can copy.
