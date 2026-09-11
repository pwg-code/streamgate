# redis_admission — distributed dedup via `DedupCarrier` injection

The Redis dedup carrier (per-identity-key storage with cluster-safe Lua
reservation, TTL idle-GC, fail-closed semantics) is a first-class contrib
module: `streamgate.contrib.redis_dedup`. It ships in the wheel and installs
its dependency via the `[redis]` extra. The framework owns the dedup
orchestration; **the storage carrier is yours**
(`guarantee="distributed"` is self-reported by the carrier).

```bash
pip install "streamgate[redis]"
```

## Layout

| Module | Contents |
|--------|----------|
| `streamgate.contrib.redis_dedup` | `RedisDedupCache` (identity→summary keys, atomic Lua reservation, TTL heartbeats), `RedisDedupCarrier` (`DedupCarrier` impl: check + reserve + force precheck + fail-closed), `RedisDedupCarrierConfig`, `RedisConfig` |
| `models.py`, `produce.py` | runnable demo (stays in the repo, not in the wheel) |

## Run

```bash
# infrastructure (kafka + redis)
docker compose up -d                # from examples/docker-compose.yml

pip install "streamgate[redis]"

# produce — twice
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: accepted (guarantee=distributed)
# o-2: accepted (guarantee=distributed)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: duplicate <- Redis reservation survives across processes/runs
# o-2: duplicate
```

## Cold-identity backfill (optional)

`RedisDedupCarrier(backfill=...)` accepts any `BackfillSource` — use
`SqlBackfill` from `streamgate.contrib.sql_upsert` (extra `[sql]`). With a
backfill wired, cold identities are loaded from the database, cached into
Redis, and duplicates stay correct even after Redis flushes (TTL idle-GC).

## Wire the consumer side

Pair this with any consumer example: `examples/pure_pipeline/consume.py`
(zero-dep) or `examples/sqlite_upsert/consume.py` (SQL upsert). To keep the
dedup contract authoritative, inject the same carrier as
`ConsumerOptions(persist_hook=...)` — the consumer then refreshes Redis
summaries after each successful batch (`on_persisted`).
