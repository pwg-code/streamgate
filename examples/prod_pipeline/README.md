# prod_pipeline — production topology: Redis admission + HTTP-probe backpressure + MSSQL sink

The full production wiring in one runnable demo. Every strategy comes from
`streamgate.contrib` — install once, import everywhere:

- **Redis uniqueness admission** (`contrib.redis_admission`, extra `[redis]`) —
  shared-storage dedup, multi-instance safe (atomic Lua reservation, TTL
  idle-GC, empty-entity sentinels, fail-closed).
- **HTTP probe backpressure** (`contrib.http_probe`, extra `[http]`) — ingest
  rejects when the consumer reports backlog or the health endpoint is
  unreachable (fail-closed), with trip/recover hysteresis.
- **MSSQL idempotent sink** (`contrib.mssql_sink`, extra `[sql]`) —
  `MERGE` + HOLDLOCK upsert with the SQL Server error-number classifier.
  The dialect auto-selects from the connection string, so the same wiring
  runs on SQLite for local smoke.

## Layout

| Module | Contents |
|--------|----------|
| `produce.py` | ingest side: `RedisExistenceAdmission` + `HttpProbeSignal` injection |
| `consume.py` | consumer side: `UpsertWriter` + `SQLAlchemyErrorClassifier` + health endpoint |
| `health_server.py` | demo-grade stdlib `GET /health` serving `worker.health_snapshot()` — swap in your web framework for production |
| `models.py` | ingress schema + landing table (`orders`) |

## Run

```bash
# infrastructure (kafka + redis)
docker compose up -d                # from examples/docker-compose.yml

pip install "streamgate[redis,sql,http]"

# consume — production MSSQL (terminal 1)
DB_CONN='mssql+aioodbc://user:pass@host:1433/streamgate?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes' \
  KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python consume.py

# ...or local smoke on SQLite (default DB_CONN, same wiring)
KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python consume.py

# produce (terminal 2)
KAFKA__BOOTSTRAP_SERVERS=localhost:9092 python produce.py
# o-1: accepted
# o-2: accepted

# run produce.py again → o-1: conflict
#   (Redis reservation survives across processes — multi-instance safe)

# stop consume.py, run produce.py again → fail-closed backpressure:
# o-1: backpressure
```

## Notes

- **Cold-entity backfill (optional):** pass `backfill=SqlBackfill(...)`
  (from `contrib.sql_sink`) to `RedisExistenceAdmission` so cache misses are
  verified against the database instead of rejected (fail-closed default).
- **Consumer-side authoritative refresh:** inject the same admission policy
  as `ConsumeSpec.persist_policy` so Redis summaries are refreshed after each
  successful write — see
  [`examples/redis_admission/README.md`](../redis_admission/README.md).
- **Schema management:** the demo creates tables at startup (checkfirst);
  production uses your migration tool.
- **Table naming:** the demo table is `orders` — the MSSQL `MERGE` template
  does not bracket-quote table names, so reserved words (e.g. `order`) would
  break.
