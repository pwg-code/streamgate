# redis_admission — shared-storage uniqueness admission via `AdmissionPolicy` injection

The former built-in Redis admission (existence cache with cluster-safe Lua
reservation, TTL idle-GC, empty-entity sentinels, fail-closed semantics) now
lives here as copy-paste modules. The framework owns the admission
orchestration; **the storage carrier is yours**.

## Layout

| Module | Contents |
|--------|----------|
| `existence.py` | `RedisExistenceCache` — entity→slot→summary HASH, atomic reservation via Lua, TTL heartbeats |
| `admission.py` | `RedisExistenceAdmission` — `AdmissionPolicy` impl: check + reserve + overwrite precheck + fail-closed; cold-entity backfill hook (`BackfillSource`) |
| `settings.py` | example-local Redis connection config |

## Run

```bash
# infrastructure (kafka + redis)
docker compose up -d                # from examples/docker-compose.yml

pip install streamgate redis

# produce — twice
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: accepted
# o-2: accepted
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: conflict   <- Redis reservation survives across processes/runs
# o-2: conflict
```

## Cold-entity backfill (optional)

`RedisExistenceAdmission(backfill=...)` accepts any `BackfillSource` — for a
SQL source copy `backfill.py` from [`examples/sqlite_sink/`](../sqlite_sink/)
(`SqlBackfill`). With a backfill wired, cold entities are loaded from the
database, fully cached into Redis, and 409s stay correct even after Redis
flushes (TTL idle-GC).

## Wire the consumer side

Pair this with any consumer example: `examples/pure_pipeline/consume.py`
(zero-dep) or `examples/sqlite_sink/consume.py` (SQL upsert). To keep the
uniqueness contract authoritative, inject the same admission policy as
`ConsumeSpec.persist_policy` — the consumer then refreshes Redis summaries
after each successful write (`on_persisted`).
