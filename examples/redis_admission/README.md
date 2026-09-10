# redis_admission — shared-storage uniqueness admission via `AdmissionPolicy` injection

The Redis admission strategy (existence cache with cluster-safe Lua
reservation, TTL idle-GC, empty-entity sentinels, fail-closed semantics) is a
first-class contrib module: `streamgate.contrib.redis_admission`. It ships in
the wheel and installs its dependency via the `[redis]` extra. The framework
owns the admission orchestration; **the storage carrier is yours**.

```bash
pip install "streamgate[redis]"
```

## Layout

| Module | Contents |
|--------|----------|
| `streamgate.contrib.redis_admission` | `RedisExistenceCache` (entity→slot→summary HASH, atomic Lua reservation, TTL heartbeats), `RedisExistenceAdmission` (`AdmissionPolicy` impl: check + reserve + overwrite precheck + fail-closed), `RedisConfig` |
| `models.py`, `produce.py` | runnable demo (stays in the repo, not in the wheel) |

## Run

```bash
# infrastructure (kafka + redis)
docker compose up -d                # from examples/docker-compose.yml

pip install "streamgate[redis]"

# produce — twice
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: accepted
# o-2: accepted
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: conflict   <- Redis reservation survives across processes/runs
# o-2: conflict
```

## Cold-entity backfill (optional)

`RedisExistenceAdmission(backfill=...)` accepts any `BackfillSource` — use
`SqlBackfill` from `streamgate.contrib.sql_upsert` (extra `[sql]`). With a
backfill wired, cold entities are loaded from the database, fully cached into
Redis, and 409s stay correct even after Redis flushes (TTL idle-GC).

## Wire the consumer side

Pair this with any consumer example: `examples/pure_pipeline/consume.py`
(zero-dep) or `examples/sqlite_upsert/consume.py` (SQL upsert). To keep the
uniqueness contract authoritative, inject the same admission policy as
`ConsumerOptions(persist_hook=...)` — the consumer then refreshes Redis
summaries after each successful batch (`on_persisted`).
