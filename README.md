# streamgate

[![PyPI version](https://img.shields.io/pypi/v/streamgate.svg)](https://pypi.org/project/streamgate/)
[![Python versions](https://img.shields.io/pypi/pyversions/streamgate.svg)](https://pypi.org/project/streamgate/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/pwg-code/streamgate/blob/main/LICENSE)
[![CI](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml/badge.svg)](https://github.com/pwg-code/streamgate/actions/workflows/ci.yml)

**streamgate** is a Kafka-backed data pipeline framework: conditional admission at the door, reliable delivery through Kafka, and idempotent sinking wherever you want the data to land.

- 中文提示：本仓库对外文档以英文为主；内部代码注释保留中文。

## Why streamgate

- **Mechanisms in the framework, policies in your hands.** The framework owns admission plumbing, backpressure, health probing, reconnection, offset handling and graceful shutdown. You own entity/slot semantics, schemas and sink targets.
- **A uniqueness contract, not just "send to Kafka".** Existence checks + atomic slot reservation (Lua, cluster-safe) + cold-path backfill from your database turn at-least-once ingestion into at-most-once admission (409 conflicts instead of duplicates).
- **Backpressure with hysteresis.** Ingest probes consumer health on a side channel and starts/stop rejecting based on backlog age, with separate trip/recover thresholds so flapping states settle.
- **DLQ bisection instead of head-of-line blocking.** A poison message never wedges a partition: the failing batch is bisected, the bad record is quarantined to a dead-letter topic, the rest commits.
- **Tier 0 declarative API covers ~90% of use cases.** Declare what you ingest and where it lands; drop to protocols and escape hatches only when you must.

## Quick Start

```python
import asyncio
from pydantic import BaseModel
from sqlmodel import SQLModel, Field

from streamgate import (
    BackpressureConfig, ConsumeSpec, ConsumerConfig, ConsumerWorker,
    DbConfig, IngestBinding, IngestGateway, KafkaConfig, Upsert,
)

KAFKA = KafkaConfig(bootstrap_servers="localhost:9092", topic="orders")

class OrderIn(BaseModel):          # ingress schema (policy lives with you)
    order_id: str
    amount: float

class Order(SQLModel, table=True): # persistence model
    order_id: str = Field(primary_key=True)
    amount: float

binding = IngestBinding(
    message_type="order",
    entity_key=lambda r: r.order_id,
    slot_key=lambda r: "order",
    summary=lambda r: {"amount": r.amount},
    admission="none",              # or "redis-existence" (extras: streamgate[redis])
)

async def ingest_one(record: OrderIn) -> None:
    gateway = IngestGateway(binding=binding, kafka_config=KAFKA,
                            backpressure_config=BackpressureConfig(enabled=False))
    await gateway.start()
    outcome = await gateway.process(record)
    await gateway.close()
    print(outcome.kind)

async def consume() -> None:
    spec = ConsumeSpec(upserts=[Upsert(model=Order, keys=["order_id"])])
    worker = ConsumerWorker(
        spec, kafka_config=KAFKA,
        consumer_config=ConsumerConfig(group_id="order-sink"),
        db_config=DbConfig(connection_string="sqlite+aiosqlite:///./data/streamgate.db"),
    )
    await worker.run()             # blocks until SIGINT/SIGTERM
```

A runnable version of this (producer + consumer scripts, docker-compose included) lives in [`examples/`](examples/).

## Core concepts

| Tier | What you use | When |
|------|--------------|------|
| **0 — Declarative** | `IngestBinding` + `ConsumeSpec`/`Upsert` | ~90% of cases: declare message type, uniqueness keys, sink target |
| **1 — Component swap** | Protocols in `streamgate.protocols` + built-ins | Replace admission, sinks, backpressure, codec, backfill with your own |
| **2 — Escape hatch** | `ConsumeSpec.on_record` / `ConsumeContext` | Handle records yourself; no sink machinery at all |

Key protocols (all in `streamgate.protocols`):

- `AdmissionPolicy` — decide admit / 409-conflict / reject per record before it reaches Kafka.
- `RecordWriter` — where consumed batches land (database, search index, another service...).
- `BackpressureSignal` — tells the gateway whether to accept; the built-in HTTP probe is just one implementation.

**Stability:** v0.x, experimental. The protocols module is the frozen contract; everything else may still shift.

## Architecture

```
your HTTP app (presentation is yours: auth/routing/OpenAPI)
        │
        ▼
  IngestGateway ──► backpressure signal (side channel) ─┐
        │          admission: existence check +         │
        │          atomic slot reservation (+DB         │
        │          backfill on cold entities)           │
        ▼                                               │
     Kafka ─────────────────────────────────────────────┘  DLQ ◄─ poisoned records
        │                                                      (bisection)
        ▼
  ConsumerWorker ──► sink / upserts / on_record ──► your storage
        │
        └─ health snapshot (expose it with your own web framework)
```

The package contains **zero web-framework code** (no fastapi/uvicorn): HTTP presentation belongs to your adapter layer.

## Installation

```bash
pip install streamgate              # pure Kafka pipeline: no DB, no redis
```

Mechanisms are core; policy carriers are extras:

| Extra | Installs | Enables |
|-------|----------|---------|
| `[sqlite]` | aiosqlite | built-in upserts / backfill against SQLite |
| `[mssql]` | aioodbc | built-in upserts / backfill against SQL Server |
| `[redis]` | redis | `redis-existence` admission strategy |
| `[http-probe]` | httpx | built-in HTTP backpressure probe signal |
| `[all]` | all of the above | everything built-in |

```bash
pip install "streamgate[sqlite]"    # built-in persistence (Quick Start above)
pip install "streamgate[all]"       # full built-in capability
```

Other databases (PostgreSQL, MySQL, ...): inject your own `RecordWriter` (Tier 1). The built-in upsert sugar targets SQLite and SQL Server semantics.

## Configuration

All components configure via typed objects that map 1:1 to environment variables (`KAFKA__*`, `CONSUMER__*`, `DB__*`, `REDIS__*`, `BACKPRESSURE__*`). Required settings (e.g. the Kafka topic, consumer group id, and the DB connection string when upserts are declared) fail fast at startup with fix instructions. See [CONFIGURATION.md](CONFIGURATION.md).

## Examples

See [`examples/`](examples/) for a complete ingest → consume → SQLite round trip with a `docker-compose.yml` for Kafka + Redis.

## Roadmap

- Test suite (first release ships without tests; APIs are exercised in production but the project considers this its top debt)
- Documentation site
- More admission policies and sink writers

## License

[MIT](LICENSE)
