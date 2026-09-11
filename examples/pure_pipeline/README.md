# pure_pipeline — bare-install end-to-end demo (the main example)

The living proof of streamgate's pure-core promise: this pipeline runs with
**only `pip install streamgate`** — no DB driver, no redis, no httpx. The
outlet handler upserts SQLite via the Python standard library.

## Topology

```
produce.py ──► Producer (dedup=DedupOptions(key=...)) ──► Kafka topic "orders"
                                                              │
consume.py ◄──────────────────────────────────────────────────┘
     └─ Consumer(handler=...) ──► stdlib sqlite3 (data/pipeline.db)
```

## Run

```bash
# 1. infrastructure (Kafka only; redis is not needed here)
docker compose up -d kafka          # from examples/docker-compose.yml

# 2. install streamgate — nothing else
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install streamgate

# 3. consume (terminal 1)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py

# 4. produce (terminal 2)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: accepted (guarantee=process-local)
# o-1: duplicate <- built-in in-process dedup carrier (same identity in one run)
# o-2: accepted (guarantee=process-local)

# 5. verify the outlet
sqlite3 data/pipeline.db "select * from orders;"
# o-1|9.9
# o-2|9.9
```

## What this demonstrates

| Mechanism | Where |
|-----------|-------|
| Flat producer entry (`Producer(bootstrap_servers, topic, options=...)`) | `produce.py` |
| One-line in-process dedup (`DedupOptions(key=...)`) | `produce.py` |
| Default backpressure signal (`ManualBackpressureSignal`, off) | `produce.py` (implicit) |
| Custom outlet (`Consumer(handler=...)`) | `consume.py` |
| Graceful shutdown (Ctrl+C → flush → leave group) | `consume.py` |

## Limits (by design)

- The built-in dedup carrier is **single-process only**
  (`guarantee="process-local"`) — memory is not shared across replicas. For
  multi-instance deployments inject a shared-storage carrier; use
  [`streamgate.contrib.redis_dedup`](https://github.com/pwg-code/streamgate#contrib-official-strategy-implementations)
  (see also [`examples/redis_admission/`](../redis_admission/) for a runnable demo).
- Your `handler` owns the outlet: retries and pausing stay with the framework,
  the storage action is yours. For a pre-assembled SQL outlet with DLQ
  probe-location, use
  [`streamgate.contrib.sqlite_upsert`](https://github.com/pwg-code/streamgate#contrib-official-strategy-implementations)
  (see also [`examples/sqlite_upsert/`](../sqlite_upsert/)).
