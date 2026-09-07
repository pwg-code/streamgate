# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.1.0
