# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

English | [中文](README.zh-CN.md)

**streamgate** is a pure-core Kafka data pipeline framework: conditional admission at the door, reliable delivery through Kafka, and a single typed hook — `RecordWriter` — wherever you want the data to land.

The wheel installs exactly three dependencies (`aiokafka`, `loguru`, `pydantic`) and contains **zero database, Redis or HTTP-client code**. All I/O strategies ship as copy-paste examples in [`examples/`](examples/).

- 中文提示：本仓库对外文档以英文为主；内部代码注释保留中文。

## Why streamgate

- **Mechanisms in the framework, policies in your hands.** The framework owns admission *orchestration*, backpressure, health probing, reconnection, offset handling, batch buffering and graceful shutdown. You own entity/slot semantics, schemas, and — critically — **the storage carriers**: your database, your Redis, your HTTP probe are injected via typed protocols, never imported by the core.
- **A uniqueness contract without a mandated store.** The admission flow (existence check → atomic slot reservation → cold-entity backfill → consumer-side authoritative refresh) is framework machinery. Whether the existence store is Redis, an in-process dict, or something else is your policy: implement `AdmissionPolicy`, or use the built-in zero-I/O shortcuts (`"none"`, `"in-memory"`). A production-grade Redis reference lives in [`examples/redis_admission/`](examples/redis_admission/).
- **One sink hook, any storage.** `ConsumeSpec(sink=...)` takes a `RecordWriter` — write to SQL, Elasticsearch, another service, a file; the framework handles batching, retries, error classification and DLQ bisection around it. No database is built in, on purpose.
- **Error classification as a hook.** Transient vs. poison vs. fatal is storage-specific knowledge. Inject an `ErrorClassifier`; the default only knows generic exceptions (and maps unknowns to poison — probe-protected bisect, never silent drops). A production-hardened SQLAlchemy classifier is in [`examples/sqlite_sink/`](examples/sqlite_sink/).
- **Backpressure with hysteresis.** Ingest probes consumer health on a side channel and rejects based on backlog age with separate trip/recover thresholds. The core ships zero-I/O signals (`ManualBackpressureSignal`); the HTTP-probe reference is in [`examples/http_probe/`](examples/http_probe/).
- **DLQ bisection instead of head-of-line blocking.** A poison message never wedges a partition: the failing batch is bisected with probe comparison, the bad record is quarantined to a dead-letter topic, the rest commits.

## Quick Start

A complete ingest → Kafka → consume → SQLite round trip that runs on a **bare install** (stdlib sink, zero extra packages) — [`examples/pure_pipeline/`](examples/pure_pipeline/):

```python
import asyncio
from pydantic import BaseModel

from streamgate import (
    BackpressureConfig, ConsumeSpec, ConsumerConfig, ConsumerWorker,
    IngestBinding, IngestGateway, KafkaConfig,
)

KAFKA = KafkaConfig(bootstrap_servers="localhost:29092", topic="orders")

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
    gateway = IngestGateway(binding=binding, kafka_config=KAFKA,
                            backpressure_config=BackpressureConfig(enabled=False))
    await gateway.start()
    outcome = await gateway.process(record)     # ACCEPTED / CONFLICT / BACKPRESSURE / ...
    await gateway.close()
    print(outcome.kind)

async def handle(records, context) -> None:     # your storage, any library
    ...

async def consume() -> None:
    spec = ConsumeSpec(on_record=handle, expected_message_type="order")
    worker = ConsumerWorker(spec, kafka_config=KAFKA,
                            consumer_config=ConsumerConfig(group_id="order-sink"))
    await worker.run()             # blocks until SIGINT/SIGTERM
```

## Core concepts

| Tier | What you use | When |
|------|--------------|------|
| **0 — Declarative** | `IngestBinding` (ingest side) + `ConsumeSpec` with a `sink` or `on_record` (sink side) | The common path: declare message type and uniqueness keys; inject your writer or handler |
| **1 — Component swap** | Protocols in `streamgate.protocols` + built-in zero-I/O implementations | Replace admission, backpressure signal, codec, error classifier with your own |
| **2 — Escape hatch** | `ConsumeSpec.on_record` / `ConsumeContext` | Handle records yourself; no sink machinery at all |

Key protocols (all in `streamgate.protocols`, the frozen contract):

- `AdmissionPolicy` — decide admit / 409-conflict / reject per record before it reaches Kafka (existence check + atomic reservation + cold backfill are *your* implementation against *your* store).
- `RecordWriter` — where consumed batches land (database, search index, another service...). The only persistence path.
- `BackpressureSignal` — tells the gateway whether to accept; built-ins are zero-I/O, dynamic probes are injected.
- `ErrorClassifier` — RETRY / POISON / FATAL mapping for write failures.
- `BackfillSource` — cold-entity load for admission policies that need one.

**Stability:** v0.x, experimental. The protocols module is the frozen contract; everything else may still shift.

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
        ▼                                                  │      (bisect + probe compare)
  ConsumerWorker ──► RecordWriter (your storage) ──────────┘
        │             └─ ErrorClassifier (injected)
        └─ health snapshot (expose it with your own web framework)
```

The package contains **zero web-framework code** (no fastapi/uvicorn) and **zero DB/Redis code** — enforced by an import-linter forbidden contract in CI.

## Installation

```bash
pip install streamgate    # aiokafka + loguru + pydantic. That's it.
```

No extras. For storage/backpressure carriers, copy from `examples/` and add the
third-party library your implementation needs (e.g. `redis`, `sqlalchemy`,
`httpx`) to *your* project:

| Example | Demonstrates | Extra deps for the example |
|---------|--------------|----------------------------|
| [`pure_pipeline/`](examples/pure_pipeline/) (main) | bare-install full round trip, `in-memory` admission, `on_record` stdlib SQLite sink | none |
| [`sqlite_sink/`](examples/sqlite_sink/) | `RecordWriter` injection with the SQL upsert sugar + DB error classifier | `sqlalchemy`, `sqlmodel`, `aiosqlite` |
| [`redis_admission/`](examples/redis_admission/) | `AdmissionPolicy` injection with Redis existence cache | `redis` |
| [`http_probe/`](examples/http_probe/) | `BackpressureSignal` injection with hysteresis | `httpx` |

Other databases (PostgreSQL, MySQL, ...): implement `RecordWriter` (Tier 1) — the same hook every built path uses.

## Configuration

All components configure via typed objects that map 1:1 to environment variables (`KAFKA__*`, `CONSUMER__*`, `BACKPRESSURE__*`). Required settings (e.g. the Kafka topic, consumer group id) fail fast at startup with fix instructions. Database/Redis connection settings belong to your application (see the examples' local configs). See [CONFIGURATION.md](CONFIGURATION.md).

## Examples

See [`examples/`](examples/) for four runnable pipelines plus a `docker-compose.yml` (Kafka + Redis). Start with [`examples/pure_pipeline/`](examples/pure_pipeline/) — it runs with nothing but `pip install streamgate`.

## Roadmap

- Test suite (first release ships without tests; APIs are exercised in production but the project considers this its top debt)
- Documentation site
- More admission/sink reference implementations in `examples/`

## License

[MIT](LICENSE)
