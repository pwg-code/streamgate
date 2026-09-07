# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-09-07

**Pure-core release.** All database, Redis and HTTP-client code is removed from
the wheel; every I/O strategy is now a typed protocol hook with a copy-paste
reference implementation in `examples/`. Core dependencies collapse to
`aiokafka` + `loguru` + `pydantic`. This is a breaking release (0.1.0 was
published days earlier; the migration cost is intentionally accepted).

### Changed

- **Core dependencies**: `aiokafka` / `loguru` / `pydantic` only. The
  `sqlalchemy` and `sqlmodel` dependencies — and the entire extras system
  (`[sqlite]`, `[mssql]`, `[redis]`, `[http-probe]`, `[all]`) — are gone.
- **`ConsumeSpec`**: landing targets reduced to two, mutually exclusive:
  `sink` (a `RecordWriter` — the only persistence path) or `on_record`
  (escape hatch). Providing neither — or both — fails fast at declaration
  with fix guidance.
- **`IngestBinding.admission`**: string shortcuts are now `"none"` and
  `"in-memory"` (new built-in, zero-I/O, single-process). `"redis-existence"`
  is removed — inject a `RedisExistenceAdmission` instance instead
  (copy from `examples/redis_admission/`).
- **`IngestGateway`**: default backpressure signal is now the built-in
  `ManualBackpressureSignal` (static switch, defaults to not rejecting);
  `db_config` / `redis_config` constructor parameters removed.
- **`ConsumerWorker`**: constructor parameters `db_config`, `redis_config`,
  `writer`, `engine`, `session_factory` removed — the sink comes from
  `ConsumeSpec.sink`. New `error_classifier` parameter (see below).

### Added

- **`ErrorClassifier` protocol + `ErrorKind` enum** (`RETRY` / `POISON` /
  `FATAL`): write-failure classification is now an injected policy.
  `DefaultErrorClassifier` only recognizes generic exceptions
  (timeouts / KafkaError → retry; ValueError → poison; unknown → poison,
  probe-protected bisect). Consumers writing to a database **must** inject a
  DB-aware classifier — `examples/sqlite_sink/classifier.py` ships a
  production-hardened `SQLAlchemyErrorClassifier` (incl. SQL Server error
  numbers).
- **`ManualBackpressureSignal`**: built-in pure in-memory backpressure switch.
- **`InMemoryAdmission`**: built-in zero-I/O uniqueness admission
  (single-process only; multi-instance topologies should inject a
  shared-storage policy).
- **`examples/`** (not part of the wheel, CI-gated for lint/typecheck):
  - `pure_pipeline/` — bare-install full round trip (main example; the
    living proof of `pip install streamgate` purity)
  - `sqlite_sink/` — the former `db/` package (engines / upsert sugar /
    backfill / SQLite+MSSQL dialects) as local modules + `RecordWriter`
    injection + `SQLAlchemyErrorClassifier`
  - `redis_admission/` — the former `cache/existence.py` +
    `ingest/admission/redis_existence.py` + `AdmissionPolicy` injection
  - `http_probe/` — the former `HttpProbeSignal` (hysteresis state machine) +
    `BackpressureSignal` injection

### Removed

- `streamgate.db` package (`UpsertWriter`, `SqlBackfill`, engine factories,
  SQLite/MSSQL dialects), `Upsert` declaration, `DbConfig` / `RedisConfig`
  (and their `DB__*` / `REDIS__*` env mappings), `RedisExistenceCache`,
  `RedisExistenceAdmission` (+ related symbols), `HttpProbeSignal`,
  `_optional` lazy-loading gate.
- `FailureCategory` / `add_failure_rule` / `classify_write_failure`
  (superseded by `ErrorClassifier`).

### Migration Guide (0.1.0 → 0.2.0)

| 0.1.0 symbol | Where it went |
|--------------|---------------|
| `Upsert` | `examples/sqlite_sink/upsert.py` (`Upsert`, local class) |
| `UpsertWriter` | `examples/sqlite_sink/upsert.py` — inject via `ConsumeSpec(sink=UpsertWriter(...))` |
| `DbConfig`, `create_read_engine`, `create_write_engine`, `async_session_factory` | `examples/sqlite_sink/config.py` + `engines.py` |
| `SqlBackfill` | `examples/sqlite_sink/backfill.py` |
| SQLite / MSSQL dialect internals | `examples/sqlite_sink/dialects/` |
| `RedisExistenceCache`, `EMPTY_FIELD` | `examples/redis_admission/existence.py` |
| `RedisExistenceAdmission`, `RedisExistenceAdmissionConfig`, `EntitySlots`, `SlotSource`, `ExistenceUnavailableError`, `UndeterminedReason` | `examples/redis_admission/admission.py` — inject via `IngestBinding(admission=RedisExistenceAdmission(...))` |
| `HttpProbeSignal` | `examples/http_probe/probe_signal.py` — inject via `IngestGateway(signal=HttpProbeSignal(...))` |
| `FailureCategory`, `classify_write_failure`, `add_failure_rule` | `ErrorKind` + `ErrorClassifier`; DB mapping in `examples/sqlite_sink/classifier.py` |
| `ConsumeSpec(upserts=[...])` | `ConsumeSpec(sink=UpsertWriter(..., [Upsert(...)]))` (writer built by you) |
| `ConsumerWorker(db_config=..., spec=ConsumeSpec(upserts=...))` | `ConsumerWorker(spec, ...)` with `ConsumeSpec(sink=...)`; pass `error_classifier=` |
| `admission="redis-existence"` | `admission=RedisExistenceAdmission(...)` (instance injection) |
| `pip install "streamgate[redis]"` etc. | `pip install streamgate <third-party-lib>` + copy the example module |

## [0.1.0] - 2026-09-07

First public release. Data pipeline framework: conditional admission → reliable delivery → pluggable sinks.

### Added

- **Ingest (admission)**
  - `IngestGateway`: transport-agnostic ingest kernel — backpressure check → admission → Kafka send → on_accepted orchestration.
  - `IngestBinding`: declarative ingest binding (Tier 0) with `admission="none" | "redis-existence"` shortcuts or a custom `AdmissionPolicy`.
  - `RedisExistenceAdmission` / `RedisExistenceCache` (extras: `[redis]`): uniqueness contract via existence check + atomic slot reservation (cluster-safe Lua) + TTL idle-GC + empty-entity sentinels + fail-closed semantics; optional cold-entity DB backfill via `BackfillSource` (`SqlBackfill`).
- **Transport**
  - `KafkaProducerService` / `KafkaConsumerService` over aiokafka: lazy connect, health probing, self-healing reconnects with exponential backoff and rebuild thresholds.
  - `JsonEnvelopeCodec` envelope format; custom `MessageCodec` supported.
- **Consumer (sink)**
  - `ConsumerWorker` + `consume_loop`: batch buffering, at-least-once processing, graceful shutdown.
  - `ConsumeSpec` (Tier 0) with three mutually exclusive landing targets: `upserts` (built-in idempotent SQL upserts via `Upsert`), `sink` (`RecordWriter` injection point), `on_record` (no-sink escape hatch).
  - Built-in SQLite / SQL Server upsert dialects (extras: `[sqlite]` / `[mssql]`); other databases via Tier 1 `RecordWriter`.
  - DLQ bisection: poison records are located by batch bisection and quarantined to a dead-letter topic instead of wedging the partition.
- **Resilience & observability**
  - Consumption backpressure with hysteresis (trip/recover on backlog age), fail-closed probing, `HttpProbeSignal` (extras: `[http-probe]`); custom `BackpressureSignal` supported.
  - Health snapshots for both processes (`collect_ingest_health` / `collect_consumer_health`); exposure is left to the caller (the package contains no web-framework code).
  - Structured logging via loguru (`configure_logger`, `logger`) and a pluggable `MetricsSink`.
- **Packaging**
  - Mechanisms as core dependencies; all policy carriers (DB drivers, redis, httpx) as optional extras with lazy loading and install-guidance errors.
  - Typed configuration objects mapping 1:1 to `KAFKA__*` / `CONSUMER__*` / `DB__*` / `REDIS__*` / `BACKPRESSURE__*` environment variables; required settings fail fast at startup.

[0.2.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.2.0
[0.1.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.1.0
