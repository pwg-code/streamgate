# pure_pipeline — bare-install end-to-end demo (the main example)

The living proof of streamgate's pure-core promise: this pipeline runs with
**only `pip install streamgate`** — no DB driver, no redis, no httpx. The
sink writes SQLite via the Python standard library.

## Topology

```
produce.py ──► IngestGateway (admission="in-memory") ──► Kafka topic "orders"
                                                             │
consume.py ◄─────────────────────────────────────────────────┘
     └─ ConsumeSpec(on_record=...) ──► stdlib sqlite3 (data/pipeline.db)
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
# o-1: accepted
# o-1: conflict   <- InMemoryAdmission uniqueness (duplicate in the same run)
# o-2: accepted

# 5. verify the sink
sqlite3 data/pipeline.db "select * from orders;"
# o-1|9.9
# o-2|9.9
```

## What this demonstrates

| Mechanism | Where |
|-----------|-------|
| Declarative ingest (`IngestBinding`) | `produce.py` |
| Built-in zero-I/O admission (`admission="in-memory"`) | `produce.py` |
| Default backpressure signal (`ManualBackpressureSignal`) | `produce.py` (implicit) |
| Escape-hatch sink (`ConsumeSpec.on_record`) | `consume.py` |
| Graceful shutdown (Ctrl+C → flush → leave group) | `consume.py` |

## Limits (by design)

- `InMemoryAdmission` is **single-process only** — memory is not shared across
  replicas. For multi-instance deployments inject a shared-storage admission
  policy; use [`streamgate.contrib.redis_admission`](https://github.com/pwg-code/streamgate#contrib-official-strategy-implementations)
  (see also [`examples/redis_admission/`](../redis_admission/) for a runnable demo).
- `on_record` is the no-sink escape hatch: your callable owns the storage
  write, retries and pausing stay with the framework. For a typed
  `RecordWriter` sink with DLQ bisection, use
  [`streamgate.contrib.sqlite_sink`](https://github.com/pwg-code/streamgate#contrib-official-strategy-implementations)
  (see also [`examples/sqlite_sink/`](../sqlite_sink/)).
