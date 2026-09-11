# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

English | [中文](README.zh-CN.md)

**streamgate** is a pure-core Kafka data pipeline framework: a dedup-aware door on the way in, reliable delivery through Kafka, and one obvious outlet on the other side — `Producer(bootstrap_servers, topic, ...)` / `Consumer(bootstrap_servers, topic, group_id, handler)`.

The wheel installs exactly three dependencies (`aiokafka`, `loguru`, `pydantic`) and the core contains **zero database, Redis or HTTP-client code**. Official I/O strategy implementations live in [`streamgate.contrib`](#contrib-official-strategy-implementations) — opt-in via extras — and runnable demos live in [`examples/`](examples/).

- 中文提示：本仓库对外文档以英文为主；内部代码注释保留中文。

## Why streamgate

aiokafka gives you **transport**; streamgate gives you the **operating semantics of the door**. Idempotence only prevents in-session broker duplicates — not HTTP retries, double clicks, or cross-session re-pushes. The outlet side (offsets, backoff retries, error classification, precise poison isolation, backlog alerting) is where the gap versus raw aiokafka is widest. The uniqueness contract below is the reason this project is a *gate* and not just a *client*. Honest boundary: for pure fire-and-forget forwarding, a dozen lines of raw aiokafka are enough — streamgate does not try to replace that; it standardizes the operating semantics every service otherwise hand-writes and gets wrong.

- **Mechanisms in the framework, policies in your hands.** The framework owns dedup *orchestration* (existence check → atomic reservation → send → success/failure hooks → authoritative refresh), backpressure, health probing, reconnection, offset handling, batch buffering and graceful shutdown. You own identity semantics, schemas, and — critically — **what happens to the data**: your database, your analysis, your forwarding, your alerting — all of it is just the few lines inside your `handler`.
- **A dedup contract that is opt-in and store-agnostic.** Dedup is **off by default**: without `dedup` a `Producer` is a pure push door with zero dedup concepts. One line turns on the in-process carrier (`DedupOptions(key=...)`); a shared-store carrier turns on multi-instance dedup. The strength of the guarantee equals the strength of the carrier you inject — it is self-reported by the carrier and surfaces as `PushResult.guarantee` (`"process-local"` / `"distributed"`); the framework does no deployment-shape validation. A production-grade Redis carrier ships as [`streamgate.contrib.redis_dedup`](#contrib-official-strategy-implementations).
- **Routing key and identity key are two different keys.** `key=` is the *routing* key: same key ⇒ same partition ⇒ ordered (omit it ⇒ round-robin, unordered — a documented promise, not a runtime warning). `DedupOptions(key=...)` is the *identity* key: it decides when two records are "the same record" and only matters when dedup is on. streamgate never welds the two together.
- **One outlet contract, any destination.** `Consumer(..., handler=handle_batch)` is the whole story: normal return = the batch is done (framework commits the offset); an exception = classified and handled (retry back-off, poison quarantine, fatal stop). Batch or single-record is just `batch_size=N` vs `batch_size=1`. SQL upsert outlets ship pre-assembled as [`streamgate.contrib.sqlite_upsert` / `mssql_upsert`](#contrib-official-strategy-implementations).
- **Error classification as a hook.** Transient vs. poison vs. fatal is destination-specific knowledge. Inject an `ErrorClassifier`; the default only knows generic exceptions (and maps unknowns to poison — probe-protected location, never silent drops). A production-hardened SQLAlchemy classifier ships as `streamgate.contrib.sql_upsert.SQLAlchemyErrorClassifier`.
- **Backpressure with hysteresis.** The producer probes consumer health on a side channel and rejects pushes based on backlog age with separate trip/recover thresholds — rejecting means **zero writes**: no dedup reservation, no Kafka send. The core ships zero-I/O signals (`ManualBackpressureSignal`); the HTTP-probe implementation ships as [`streamgate.contrib.http_probe`](#contrib-official-strategy-implementations).
- **Precise poison isolation instead of head-of-line blocking.** A poison message never wedges a partition: the failing batch is located record-by-record with a single-record probe, bad records are quarantined to a dead-letter topic, good records count as handled and the offset commits.
- **Ops visibility built into the snapshot.** Health snapshots carry rate/latency metrics (push rate, dedup hits, produce success/failure, handle rate, retries, handle latency avg/max) computed over a configurable sliding window — your existing `/health` endpoint doubles as a monitoring feed, no Prometheus required.

**Responsibility boundaries.** Routing: pass `key` and same-key records stay ordered; omit it and ordering is not guaranteed (documentation-only, by design). Dedup: the strength is exactly the carrier's strength, self-reported and surfaced on every `PushResult`. Presentation: `push()` returns a transport-neutral `PushResult` (`accepted` / `duplicate` / `rejected` / `backpressure` / `unavailable`) — HTTP status codes, error-code strings and user-facing copy are **your** adapter's job; the core ships none.

## Quick Start

A complete produce → Kafka → consume → SQLite round trip that runs on a **bare install** (stdlib outlet, zero extra packages) — [`examples/pure_pipeline/`](examples/pure_pipeline/):

```python
import asyncio
from pydantic import BaseModel

from streamgate import (
    Consumer, DedupOptions, Producer, ProducerOptions,
)

KAFKA_BOOTSTRAP = "localhost:29092"

class OrderIn(BaseModel):          # ingress schema (policy lives with you)
    order_id: str
    amount: float

async def produce_one(record: OrderIn) -> None:
    producer = Producer(
        KAFKA_BOOTSTRAP,           # which cluster the data goes to
        topic="orders",            # which topic to push to
        key=lambda r: r.order_id,  # routing key: same key ⇒ same partition ⇒ ordered
        options=ProducerOptions(
            dedup=DedupOptions(    # one line = in-process dedup (off if omitted)
                key=lambda r: r.order_id,
                summary=lambda r: {"amount": r.amount},
            ),
        ),
    )
    await producer.start()
    result = await producer.push(record)   # accepted / duplicate / backpressure / ...
    await producer.close()
    print(result.kind, result.guarantee)

async def handle(batch, context) -> None:       # your storage, any library
    ...

async def consume() -> None:
    consumer = Consumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,      # which cluster the data comes from
        topic="orders",                         # which topic to read
        group_id="order-analytics",             # consumer group (offset ownership)
        handler=handle,                         # where the data goes — the only outlet
        batch_size=500,                         # =1 for single-record real-time
        flush_timeout=5.0,                      # flush a partial batch after 5s
    )
    await consumer.run()                        # blocks until SIGINT/SIGTERM
```

## Core concepts

| Tier | What you use | When |
|------|--------------|------|
| **0 — Flat entry** | `Producer(bootstrap_servers, topic, key, options)` + flat `Consumer(...)` | The common path: two required strings; everything else defaults or folds into `options` |
| **1 — Component swap** | Protocols in `streamgate.protocols` + built-in zero-I/O implementations | Replace dedup carrier, backpressure signal, codec, error classifier with your own |
| **2 — Options** | `ProducerOptions` / `DedupOptions` / `ConsumerOptions` / `DlqOptions` / `RuntimeTuning` | Dedup, backpressure, expected type, probe, classifier, persist hook, DLQ, tuning — all optional, all defaulted |

Key protocols (all in `streamgate.protocols`, the frozen contract):

- `DedupCarrier` — the storage side of the dedup contract: identity check + atomic reservation per record before it reaches Kafka, plus notification hooks (`on_send_success` / `on_send_failed` / `on_force_accepted` / `on_persisted`) and health probes. Existence storage is *your* implementation against *your* store; the built-in in-process carrier is zero-I/O.
- `BatchHandler` — `handler(batch, context)`, the only outlet contract: normal return = batch done, exception = classified handling.
- `BackpressureSignal` — tells the producer whether to accept; built-ins are zero-I/O, dynamic probes are injected.
- `ErrorClassifier` — RETRY / POISON / FATAL mapping for handler failures.
- `BackfillSource` — cold-identity load for carriers that need one.

**Stability:** 1.0.0. The protocols module is the frozen contract; everything else may still evolve within the documented deprecation policy.

## Architecture

```
your app (presentation is yours: HTTP/auth/routing/OpenAPI)
        │
        ▼
    Producer ────► BackpressureSignal (injected: manual / HTTP probe)
        │         DedupCarrier  (injected: built-in in-process /
        │          your Redis|DB-backed implementation) — off by default
        ▼
      Kafka ◄──────────────────────────────────────────────┐
        │                                                  │ DLQ ◄─ poisoned records
        ▼                                                  │      (probe location)
     Consumer ──► handler (your outlet: SQL / analysis /    │
        │             forwarding / alerting ...) ──────────┘
        │             └─ ErrorClassifier (injected via options)
        │             └─ persist_hook (carrier's on_persisted: authoritative refresh)
        └─ health snapshot (state + rates/latencies; expose it with your own web framework)
```

`Producer` and `Consumer` are loops you embed in any host — a script, a FastAPI service, a standalone worker. Process boundaries are yours; the send/receive machinery, dedup orchestration, retries, self-healing and graceful shutdown are the framework's.

The package contains **zero web-framework code** (no fastapi/uvicorn), and the core layers never import `streamgate.contrib` — enforced by the import-linter layered contract in CI.

## Contrib: official strategy implementations

`streamgate.contrib` is the official home for I/O strategy implementations on top of the core protocols. Everything below ships in the wheel; third-party dependencies are opt-in via extras, and importing without the extra raises an error that tells you which extra to install.

| Extra | Install | Subpackage(s) | Provides |
|-------|---------|---------------|----------|
| `[redis]` | `pip install "streamgate[redis]"` | `contrib.redis_dedup` | `RedisDedupCarrier` (`DedupCarrier` impl, `guarantee="distributed"`), `RedisDedupCache`, `RedisDedupCarrierConfig`, `RedisConfig` |
| `[sql]` | `pip install "streamgate[sql]"` | `contrib.sql_upsert` / `contrib.sqlite_upsert` / `contrib.mssql_upsert` | `Upsert` + `upsert_outlet` (batch handler + single-record probe, SQLite `ON CONFLICT` + MSSQL `MERGE`+HOLDLOCK), `SqliteConsumer` / `MssqlConsumer` pre-assembled factories, engine factory, `SqlBackfill` (`BackfillSource` impl), `SQLAlchemyErrorClassifier`, `DbConfig` |
| `[http]` | `pip install "streamgate[http]"` | `contrib.http_probe` | `HttpProbeSignal` / `HysteresisController` (`BackpressureSignal` impl with trip/recover hysteresis, fail-closed probing) |

Distributed dedup in three lines — inject the carrier, everything else stays the same:

```python
from streamgate import DedupOptions, Producer, ProducerOptions
from streamgate.contrib.redis_dedup import RedisConfig, RedisDedupCarrier, RedisDedupCache

redis_config = RedisConfig(url="redis://localhost:6379/0")
producer = Producer(
    "localhost:29092",
    topic="orders",
    options=ProducerOptions(
        dedup=DedupOptions(
            key=lambda r: r.order_id,
            carrier=RedisDedupCarrier(
                cache=RedisDedupCache(redis_config),
                key=lambda r: r.order_id,
                summary=lambda r: {"amount": r.amount},
                redis_config=redis_config,
            ),
        ),
    ),
)
result = await producer.push(order)
if result.kind == "duplicate":
    ...   # result.summary = the existing record's summary
```

The SQL outlet as an out-of-the-box consumer — the factory pre-wires the
batch handler, the single-record probe (precise DLQ location on poison
batches) and the dialect-aware classifier; everything else stays exactly the
core `Consumer` API:

```python
from models import Order                        # your SQLModel table
from streamgate.contrib.sqlite_upsert import SqliteConsumer, Upsert

consumer = SqliteConsumer(
    db="sqlite+aiosqlite:///./data/app.db",
    upserts=[Upsert(model=Order, keys=["order_id"])],
    bootstrap_servers="localhost:29092",
    topic="orders",
    group_id="order-sink",
    # batch_size / flush_timeout / options: identical to the core Consumer
)
await consumer.run()
```

**Stability:** contrib packages are provisional (beta-grade) — usable in production (these implementations ran in production before being merged), but signatures may still adjust within minor releases. The core API contract is unaffected: the core never imports contrib.

## Installation

```bash
pip install streamgate             # aiokafka + loguru + pydantic. That's it.
pip install "streamgate[redis]"    # + Redis distributed dedup carrier
pip install "streamgate[sql]"      # + SQL upsert outlet (SQLite + MSSQL)
pip install "streamgate[http]"     # + HTTP probe backpressure
```

For anything else (PostgreSQL, Elasticsearch, your own destination...), write the `handler` yourself — the same contract every contrib factory wires for you.

## Configuration

Both sides take required settings as flat constructor arguments with environment-variable fallbacks (`KAFKA__BOOTSTRAP_SERVERS`, `KAFKA__TOPIC`, `CONSUMER__GROUP_ID`); advanced knobs fold into `ProducerOptions` / `ConsumerOptions` (dedup, backpressure, DLQ, tuning, metrics window) with the same env fallbacks. Required settings fail fast at startup with fix instructions. Database/Redis connection settings belong to your application (see the examples' local configs). See [CONFIGURATION.md](CONFIGURATION.md).

## Examples

See [`examples/`](examples/) for five runnable pipelines plus a `docker-compose.yml` (Kafka + Redis). Start with [`examples/pure_pipeline/`](examples/pure_pipeline/) — it runs with nothing but `pip install streamgate` and shows the custom-outlet path. The strategy demos (`redis_admission/`, `sqlite_upsert/`, `http_probe/`) import from `streamgate.contrib`, and [`examples/prod_pipeline/`](examples/prod_pipeline/) wires the full production topology (Redis dedup + HTTP-probe backpressure + MSSQL outlet) in one demo.

## Migrating from 0.x

1.0.0 redesigned **both** sides (breaking): the consumer side replaces `ConsumerWorker` + `ConsumeSpec` + `RecordWriter` with the flat `Consumer` constructor; the producer side replaces `IngestBinding` + `IngestGateway` with the flat `Producer` constructor (`process()` → `push()`, `IngestOutcome` → `PushResult`, `overwrite` → `force`, `admission` → `DedupOptions`, `entity_key`/`slot_key` split into routing `key` + identity `DedupOptions.key`). See the [CHANGELOG](CHANGELOG.md) for the complete old→new mapping (API, result kinds, metrics names, log events, env keys, import paths).

## Roadmap

- Test suite (first release ships without tests; APIs are exercised in production but the project considers this its top debt)
- Documentation site
- More dedup carrier/outlet implementations in `streamgate.contrib`

## License

[MIT](LICENSE)
