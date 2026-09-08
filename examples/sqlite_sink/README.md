# sqlite_sink — RecordWriter injection with SQL upsert (contrib: `sql_sink`)

The SQL upsert strategy (engines / upsert / backfill / dialects / classifier)
is a first-class contrib module: `streamgate.contrib.sql_sink`, with
`streamgate.contrib.sqlite_sink` and `streamgate.contrib.mssql_sink` as
per-backend entry packages. It ships in the wheel and installs its
dependencies via the `[sql]` extra. The framework core is storage-agnostic:
`ConsumeSpec(sink=...)` accepts **any** `RecordWriter`, and SQL upsert is just
one of them.

```bash
pip install "streamgate[sql]"
```

## Layout

| Module | Contents |
|--------|----------|
| `streamgate.contrib.sqlite_sink` | SQLite entry (`aiosqlite`): `ON CONFLICT` upsert, DELETE+INSERT fallback, auto create-table with WAL PRAGMAs |
| `streamgate.contrib.mssql_sink` | MSSQL entry (`aioodbc`): multi-row `MERGE` + HOLDLOCK, param-budget chunking, SQL Server error-number classifier |
| `streamgate.contrib.sql_sink` | shared base: `Upsert` declaration, `UpsertWriter` (RecordWriter impl), engine factory, `SqlBackfill`, `SQLAlchemyErrorClassifier`, `DbConfig` |
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

## Why inject a classifier?

The framework's `DefaultErrorClassifier` only recognizes generic exceptions
and maps unknown errors to `POISON` (→ DLQ bisection). A connection-pool
timeout from your DB is transient and should be `RETRY` (paused self-healing)
— only a DB-aware classifier can tell. Inject it via
`ConsumerWorker(..., error_classifier=...)`;
`streamgate.contrib.sql_sink.SQLAlchemyErrorClassifier` is a
production-hardened starting point you can subclass.
