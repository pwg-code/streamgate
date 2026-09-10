# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

English | [中文](README.zh-CN.md)

**streamgate** is a pure-core Kafka data pipeline framework: conditional admission at the door, reliable delivery through Kafka, and one obvious outlet on the other side — `Consumer(bootstrap_servers, topic, group_id, handler)`.

The wheel installs exactly three dependencies (`aiokafka`, `loguru`, `pydantic`) and the core contains **zero database, Redis or HTTP-client code**. Official I/O strategy implementations live in [`streamgate.contrib`](#contrib-official-strategy-implementations) — opt-in via extras — and runnable demos live in [`examples/`](examples/).

- 中文提示：本仓库对外文档以英文为主；内部代码注释保留中文。

## Why streamgate

- **Mechanisms in the framework, policies in your hands.** The framework owns admission *orchestration*, backpressure, health probing, reconnection, offset handling, batch buffering and graceful shutdown. You own entity/slot semantics, schemas, and — critically — **what happens to the data**: your database, your analysis, your forwarding, your alerting — all of it is just the few lines inside your `handler`.
- **A uniqueness contract without a mandated store.** The admission flow (existence check → atomic slot reservation → cold-entity backfill → consumer-side authoritative refresh) is framework machinery. Whether the existence store is Redis, an in-process dict, or something else is your policy: implement `AdmissionPolicy`, or use the built-in zero-I/O shortcuts (`"none"`, `"in-memory"`). A production-grade Redis implementation ships as [`streamgate.contrib.redis_admission`](#contrib-official-strategy-implementations).
- **One outlet contract, any destination.** `Consumer(..., handler=handle_batch)` is the whole story: normal return = the batch is done (framework commits the offset); an exception = classified and handled (retry back-off, poison quarantine, fatal stop). Batch or single-record is just `batch_size=N` vs `batch_size=1`. SQL upsert outlets ship pre-assembled as [`streamgate.contrib.sqlite_upsert` / `mssql_upsert`](#contrib-official-strategy-implementations).
- **Error classification as a hook.** Transient vs. poison vs. fatal is destination-specific knowledge. Inject an `ErrorClassifier`; the default only knows generic exceptions (and maps unknowns to poison — probe-protected location, never silent drops). A production-hardened SQLAlchemy classifier ships as `streamgate.contrib.sql_upsert.SQLAlchemyErrorClassifier`.
- **Backpressure with hysteresis.** Ingest probes consumer health on a side channel and rejects based on backlog age with separate trip/recover thresholds. The core ships zero-I/O signals (`ManualBackpressureSignal`); the HTTP-probe implementation ships as [`streamgate.contrib.http_probe`](#contrib-official-strategy-implementations).
- **Precise poison isolation instead of head-of-line blocking.** A poison message never wedges a partition: the failing batch is located record-by-record with a single-record probe, bad records are quarantined to a dead-letter topic, good records count as handled and the offset commits.
- **Ops visibility built into the snapshot.** Health snapshots carry rate/latency metrics (receive rate, admission conflicts, produce success/failure, handle rate, retries, handle latency avg/max) computed over a configurable sliding window — your existing `/health` endpoint doubles as a monitoring feed, no Prometheus required.

## Quick Start

A complete ingest → Kafka → consume → SQLite round trip that runs on a **bare install** (stdlib outlet, zero extra packages) — [`examples/pure_pipeline/`](examples/pure_pipeline/):

```python
import asyncio
from pydantic import BaseModel

from streamgate import (
    BackpressureConfig, Consumer, IngestBinding, IngestGateway, KafkaConfig,
)

KAFKA_BOOTSTRAP = "localhost:29092"

class OrderIn(BaseModel):          # ingress schema (policy lives with you)
    order_id: str
    amount: float

binding = IngestBinding(
    message_type="order",
    entity_key=lambda r: r.order_id,
    slot_key=lambda r: "order",
    summary=lambda r: {"amount": r.amount},
    admission="in-memory",         # built-in zero-I/O uniqueness (single process)
)

async def ingest_one(record: OrderIn) -> None:
    gateway = IngestGateway(binding=binding,
                            kafka_config=KafkaConfig(bootstrap_servers=KAFKA_BOOTSTRAP, topic="orders"),
                            backpressure_config=BackpressureConfig(enabled=False))
    await gateway.start()
    outcome = await gateway.process(record)     # ACCEPTED / CONFLICT / BACKPRESSURE / ...
    await gateway.close()
    print(outcome.kind)

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
| **0 — Declarative** | `IngestBinding` (ingest side) + flat `Consumer(...)` (outlet side) | The common path: declare message type and uniqueness keys; pass the four constructor arguments |
| **1 — Component swap** | Protocols in `streamgate.protocols` + built-in zero-I/O implementations | Replace admission, backpressure signal, codec, error classifier with your own |
| **2 — Options** | `ConsumerOptions` / `DlqOptions` / `RuntimeTuning` | Expected type, probe, classifier, persist hook, DLQ, tuning — all optional, all defaulted |

Key protocols (all in `streamgate.protocols`, the frozen contract):

- `AdmissionPolicy` — decide admit / 409-conflict / reject per record before it reaches Kafka (existence check + atomic reservation + cold backfill are *your* implementation against *your* store).
- `BatchHandler` — `handler(batch, context)`, the only outlet contract: normal return = batch done, exception = classified handling.
- `BackpressureSignal` — tells the gateway whether to accept; built-ins are zero-I/O, dynamic probes are injected.
- `ErrorClassifier` — RETRY / POISON / FATAL mapping for handler failures.
- `BackfillSource` — cold-entity load for admission policies that need one.

**Stability:** 1.0.0. The protocols module is the frozen contract; everything else may still evolve within the documented deprecation policy.

## Architecture

```
your HTTP app (presentation is yours: auth/routing/OpenAPI)
        │
        ▼
  IngestGateway ──► BackpressureSignal (injected: manual / HTTP probe)
        │          AdmissionPolicy (injected: none / in-memory /
        │           your Redis|DB-backed implementation)
        ▼
      Kafka ◄──────────────────────────────────────────────┐
        │                                                  │ DLQ ◄─ poisoned records
        ▼                                                  │      (probe location)
     Consumer ──► handler (your outlet: SQL / analysis /    │
        │             forwarding / alerting ...) ──────────┘
        │             └─ ErrorClassifier (injected via options)
        └─ health snapshot (state + rates/latencies; expose it with your own web framework)
```

`Consumer` is a consume loop you embed in any host — a script, a FastAPI service, a standalone worker. Process boundaries are yours; the loop, offsets, retries, self-healing and graceful shutdown are the framework's.

The package contains **zero web-framework code** (no fastapi/uvicorn), and the core layers never import `streamgate.contrib` — enforced by the import-linter layered contract in CI.

## Contrib: official strategy implementations

`streamgate.contrib` is the official home for I/O strategy implementations on top of the core protocols. Everything below ships in the wheel; third-party dependencies are opt-in via extras, and importing without the extra raises an error that tells you which extra to install.

| Extra | Install | Subpackage(s) | Provides |
|-------|---------|---------------|----------|
| `[redis]` | `pip install "streamgate[redis]"` | `contrib.redis_admission` | `RedisExistenceAdmission` (`AdmissionPolicy` impl), `RedisExistenceCache`, `RedisConfig` |
| `[sql]` | `pip install "streamgate[sql]"` | `contrib.sql_upsert` / `contrib.sqlite_upsert` / `contrib.mssql_upsert` | `Upsert` + `upsert_outlet` (batch handler + single-record probe, SQLite `ON CONFLICT` + MSSQL `MERGE`+HOLDLOCK), `SqliteConsumer` / `MssqlConsumer` pre-assembled factories, engine factory, `SqlBackfill` (`BackfillSource` impl), `SQLAlchemyErrorClassifier`, `DbConfig` |
| `[http]` | `pip install "streamgate[http]"` | `contrib.http_probe` | `HttpProbeSignal` / `HysteresisController` (`BackpressureSignal` impl with trip/recover hysteresis, fail-closed probing) |

```python
from streamgate import IngestBinding
from streamgate.contrib.redis_admission import RedisConfig, RedisExistenceAdmission, RedisExistenceCache

binding = IngestBinding(
    message_type="order",
    entity_key=lambda r: r.order_id,
    slot_key=lambda r: "order",
    summary=lambda r: {"amount": r.amount},
    admission=RedisExistenceAdmission(
        cache=RedisExistenceCache(RedisConfig(url="redis://localhost:6379/0")),
        entity_key=lambda r: r.order_id,
        slot_key=lambda r: "order",
        summary=lambda r: {"amount": r.amount},
    ),
)
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
pip install "streamgate[redis]"    # + Redis admission
pip install "streamgate[sql]"      # + SQL upsert outlet (SQLite + MSSQL)
pip install "streamgate[http]"     # + HTTP probe backpressure
```

For anything else (PostgreSQL, Elasticsearch, your own destination...), write the `handler` yourself — the same contract every contrib factory wires for you.

## Configuration

Consumer required settings can be passed directly or fall back to environment variables (`KAFKA__BOOTSTRAP_SERVERS`, `KAFKA__TOPIC`, `CONSUMER__GROUP_ID`); advanced knobs live in `ConsumerOptions` (DLQ, tuning, metrics window) with the same env fallbacks. Ingest-side components configure via typed objects that map 1:1 to environment variables (`KAFKA__*`, `BACKPRESSURE__*`). Required settings fail fast at startup with fix instructions. Database/Redis connection settings belong to your application (see the examples' local configs). See [CONFIGURATION.md](CONFIGURATION.md).

## Examples

See [`examples/`](examples/) for five runnable pipelines plus a `docker-compose.yml` (Kafka + Redis). Start with [`examples/pure_pipeline/`](examples/pure_pipeline/) — it runs with nothing but `pip install streamgate` and shows the custom-outlet path. The strategy demos (`redis_admission/`, `sqlite_upsert/`, `http_probe/`) import from `streamgate.contrib`, and [`examples/prod_pipeline/`](examples/prod_pipeline/) wires the full production topology (Redis admission + HTTP-probe backpressure + MSSQL outlet) in one demo.

## Migrating from 0.x

1.0.0 redesigned the consumer side (breaking): `ConsumerWorker` + `ConsumeSpec` + `RecordWriter` are replaced by the flat `Consumer` constructor. See the [CHANGELOG](CHANGELOG.md) for the complete old→new mapping (API, metrics names, log events, env keys, import paths).

## Roadmap

- Test suite (first release ships without tests; APIs are exercised in production but the project considers this its top debt)
- Documentation site
- More admission/outlet implementations in `streamgate.contrib`

## License

[MIT](LICENSE)
