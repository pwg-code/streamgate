# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2026-09-11

### BREAKING — unified backfill contract (feat!)

`BackfillSource.load` / `SqlBackfill.load` now speak one mapped contract:

```text
await load(scope: str) -> dict[str, JsonObject] | None
```

where `scope` is an identity key (single-key carriers) or a group id (group
carriers), and the return is a `{identity: summary}` mapping — `None` and `{}`
are equivalent (no records).

- old single-summary `None` (confirmed absent) → `{}` / key-not-present
- old single-summary hit → `{identity: summary}`
- duplicate / allow judgment paths are preserved verbatim

**Runtime breakage:** external custom `BackfillSource` implementations must
switch `load`'s return shape to the mapped form. Mechanical migration, no
other surface changed.

### Added

- **`RedisGroupDedupCarrier`** — a group-scoped `DedupCarrier`
  (`guarantee="distributed"`) in `streamgate.contrib.redis_dedup`. A group
  (batch / task / import session) is one Redis HASH
  (`{prefix}dedup_group:{group}`, field = group-internal identity);
  `group_key(record)` extracts the group id, `key(record)` the identity.
  Three-stage admit: in-group HASH hit → group-exists fast path (**zero DB**) →
  new-group cold load of the **whole group** (group-level single-flight +
  concurrency gate). The group key's lifecycle is immutable — created only by a
  successful whole-group backfill or the first placeholder after an
  empty-group confirm — so a present group key makes in-group judgment
  authoritative with no safety valve.
- **`RedisGroupDedupCache`** — HASH storage for the group carrier: atomic
  per-field Lua placeholder, idempotent HSET+EXPIRE writes, chunked-pipeline
  whole-group backfill, `group_exists` / `get_group_fields` / `get_ttl` /
  `delete_group`.
- **`SqlBackfill(group_column=...)`** — optional group-mode backfill
  (`WHERE group_column = :scope`, returns the whole group); unset keeps the
  1.0.0 single-key behavior unchanged.
- `RedisConfig.group_key_prefix` / `RedisConfig.group_ttl_seconds` for the
  group HASH keys.

### Notes

- The 1.0.0 single-key `RedisDedupCarrier` / `RedisDedupCache` keep their public
  API; only their internal consumption of `load` moved to the mapped contract.
  Both carriers coexist in one Redis instance (`dedup:` STRING vs
  `dedup_group:` HASH, no type clash).
- Large active groups: whole-group backfill is chunked internally (implementation
  detail — no config, no cap). Mind the `group_ttl_seconds` vs write-rate
  relationship (idle-GC renewal grows the memory watermark of active groups);
  a low-rate group that crosses its TTL mid-batch simply re-sources once, which
  is normal self-healing.

## [1.0.0] - 2026-09-10

### BREAKING — both sides redesigned as flat entry points

**Consumer side — a "data outlet".** The consumer API stopped modeling
"persistence" and now models what it always was: a consume loop. You hand it a
handler; offsets, retries, self-healing, poison isolation and graceful
shutdown are the framework's job. Single-record vs. batch is just
`batch_size=1` vs `batch_size=N`.

**Producer side — a "door".** The producer API stopped modeling the
"entity + slot" business scenario and the two-hop `IngestBinding →
IngestGateway` assembly. `Producer(bootstrap_servers, topic, key, options)`
is the whole entry: dedup is opt-in (`DedupOptions`, one line for the
in-process carrier), backpressure folds into `options`, and the routing key
(`key=`) is separated from the identity key (`DedupOptions.key`) — they were
welded together in 0.x and that welding was the root cause of every awkward
semantic.

**Migration guide (old → new):**

Constructing the consumer:

```text
old: ConsumeSpec(on_record=h) + ConsumerWorker(spec, kafka_config=..., consumer_config=...)
new: Consumer(bootstrap_servers=..., topic=..., group_id=..., handler=h)

old: ConsumeSpec(sink=UpsertWriter(...)) + ConsumerWorker(..., error_classifier=...)
new (out-of-the-box): SqliteConsumer(db=..., upserts=[...], topic=..., group_id=...)
new (self-assembled): Consumer(..., handler=h, probe=p,
                              options=ConsumerOptions(classifier=...))

old: batch_size=1 (on ConsumerConfig)
new: Consumer(..., batch_size=1)
```

Constructing the producer:

```text
old: IngestBinding(...) + IngestGateway(binding=..., kafka_config=..., backpressure_config=...)
new: Producer(bootstrap_servers=..., topic=..., options=ProducerOptions(...))

old: gateway.process(record, overwrite=True)
new: producer.push(record, force=True)

old: IngestBinding(admission="in-memory", entity_key=..., slot_key=..., summary=...)
new: ProducerOptions(dedup=DedupOptions(key=..., summary=...))   # in-process carrier

old: IngestBinding(admission="none")
new: omit dedup entirely (pure push, zero dedup concepts)

old: outcome = await gateway.process(record); outcome.kind == "conflict"
new: result = await producer.push(record); result.kind == "duplicate"
```

API renames / removals (consumer side):

| 0.x | 1.0.0 |
|-----|-------|
| `ConsumerWorker(spec, kafka_config=..., consumer_config=..., ...)` | `Consumer(bootstrap_servers, topic, group_id, handler, batch_size, flush_timeout, options)` |
| `ConsumeSpec` (deleted) | flat constructor arguments + `ConsumerOptions` |
| `RecordWriter` protocol + `WriteResult` (retired) | `BatchHandler = (batch, context) -> None` — the only outlet contract |
| `RecordHandler` alias | `BatchHandler` |
| — | `Probe = (record) -> None` (single-record probe for precise DLQ location) |
| `ConsumerConfig` (deleted) | flat arguments + `ConsumerOptions.tuning` / `options.dlq` |
| `ConsumeSpec.expected_message_type` | `ConsumerOptions.expected_type` |
| `ConsumeSpec.persist_policy` | `ConsumerOptions.persist_hook` |
| `ConsumeSpec.collapse_key` / `log_context` | `ConsumerOptions.collapse_key` / `log_context` |
| `ConsumeSpec.dlq` / `dlq_topic` / `dlq_message_type` | `ConsumerOptions.dlq.enabled` / `.topic` / `.message_type` |
| `ConsumerWorker.error_classifier` | `ConsumerOptions.classifier` |
| `ConsumerWorker.codec` | `ConsumerOptions.codec` |
| `ConsumerWorker.cache` | `ConsumerOptions.health_probe` |
| `ConsumerWorker.existence_ttl_seconds` | `ConsumerOptions.backlog_ttl_seconds` |
| `ConsumerWorker.metrics_config` | `ConsumerOptions.metrics_window_seconds` |
| `ConsumerConfig.batch_timeout_seconds` | `flush_timeout` argument / `CONSUMER__FLUSH_TIMEOUT_SECONDS` |
| `ConsumerConfig.reconnect_base_backoff_seconds` / `reconnect_max_backoff_seconds` | `RuntimeTuning.reconnect_base` / `.reconnect_max` |
| `ConsumerConfig.backlog_check_interval_seconds` | `RuntimeTuning.backlog_check_interval` |
| `KafkaConfig.dlq_topic` | `DlqOptions.topic` / env `KAFKA__DLQ_TOPIC` |
| `BisectOutcome.written` | `BisectOutcome.handled` |
| `locate_and_write(writer, ...)` | `locate_and_quarantine(probe, ...)` |
| `DlqProducer(kafka_config, ...)` | `DlqProducer(bootstrap_servers, topic=..., send_retries=..., ...)` |
| `KafkaConsumerService(kafka_config, consumer_config, topic)` | `KafkaConsumerService(bootstrap_servers, topic, group_id, auto_offset_reset=..., ...)` |

API renames / removals (producer side):

| 0.x | 1.0.0 |
|-----|-------|
| `IngestBinding` (deleted) | flat `Producer` constructor arguments + `ProducerOptions` |
| `IngestGateway(binding=...)` | `Producer(bootstrap_servers, topic, key, options)` |
| `IngestGateway.process()` | `Producer.push()` |
| `IngestOutcome` / `OutcomeKind` | `PushResult` / `PushKind` |
| `OutcomeKind.CONFLICT` | `PushKind.DUPLICATE` |
| `OutcomeKind.KAFKA_UNAVAILABLE` | `PushKind.UNAVAILABLE` |
| `IngestGateway.process(overwrite=True)` / `IngestBinding.is_overwrite` | `Producer.push(force=True)` (per-call, not per-record) |
| `IngestBinding.entity_key` + `slot_key` | split: `Producer(key=...)` = routing key (ordering); `DedupOptions(key=...)` = identity key (dedup) |
| `IngestBinding.partition_key` | `Producer(key=...)` |
| `IngestBinding.message_type` | `ProducerOptions.message_type` (default `streamgate_record`) |
| `IngestBinding.summary` | `DedupOptions.summary` |
| `IngestBinding.admission="in-memory"` | `ProducerOptions(dedup=DedupOptions(key=...))` (built-in in-process carrier) |
| `IngestBinding.admission=<AdmissionPolicy>` | `DedupOptions(carrier=<DedupCarrier>)` |
| `IngestBinding.log_context` | `ProducerOptions.log_context` |
| `IngestBinding.request_log_context` | removed (HTTP presentation contract — map it in your adapter) |
| `IngestBinding.backpressure_error_codes` / `backpressure_default_code` / `backpressure_detail` | removed (core only returns `kind`; HTTP mapping is yours) |
| `IngestBinding.kafka_unavailable_code` / `kafka_unavailable_detail` | removed (`PushResult.reason` only) |
| `IngestOutcome.status_code` / `error_code` / `detail` | removed — `PushResult` carries `kind` + `reason` + `retry_after` only |
| `IngestGateway.health()` → `IngestHealthResponse` | `Producer.health()` → `ProducerHealthResponse` |
| `AdmissionPolicy` protocol | `DedupCarrier` protocol (same hooks; `RejectInfo` carries `reason` + `retry_after`, no HTTP codes; `on_overwrite_accepted` renamed `on_force_accepted`; new `guarantee` property, self-reported) |
| `RejectInfo.status_code` / `error_code` / `detail` | removed — `RejectInfo(reason, retry_after, ...)` |
| `DecisionKind.CONFLICT` | `DecisionKind.DUPLICATE` |
| `InMemoryAdmission` / `NoAdmission` | built-in `InMemoryDedupCarrier` (auto-selected); `dedup=None` replaces `NoAdmission` |
| `KafkaProducerService` | internalized into `Producer` (self-healing preserved, class no longer exported) |
| `streamgate.contrib.redis_admission` | `streamgate.contrib.redis_dedup` |
| `RedisExistenceAdmission` | `RedisDedupCarrier` (`guarantee="distributed"`; `entity_key`/`slot_key` → single `key`; query endpoint `entity_slots` removed) |
| `RedisExistenceAdmissionConfig` | `RedisDedupCarrierConfig` (error-code fields removed) |
| `RedisExistenceCache` | `RedisDedupCache` (identity→summary STRING keys, single-key Lua reserve) |
| `RedisConfig.existence_ttl_seconds` | `RedisConfig.identity_ttl_seconds` (empty-entity sentinel removed — no entity grouping) |
| `BackfillSource.load(entity) -> dict[slot, summary]` | `BackfillSource.load(identity) -> summary \| None` |
| `SqlBackfill(entity_column=..., slot_column=...)` | `SqlBackfill(key_column=...)` (single identity key) |
| `IngestMetrics` | `ProducerMetrics` (`admission_conflict` → `duplicate`) |
| `collect_ingest_health` | `collect_producer_health` |

Environment variables:

| 0.x | 1.0.0 |
|-----|-------|
| `CONSUMER__BATCH_TIMEOUT_SECONDS` | renamed to `CONSUMER__FLUSH_TIMEOUT_SECONDS` |
| all other `KAFKA__*` / `CONSUMER__*` / `METRICS__*` keys | unchanged (now read directly by `Producer` / `Consumer` as fallbacks: `KAFKA__BOOTSTRAP_SERVERS`, `KAFKA__TOPIC`, `CONSUMER__GROUP_ID`, `CONSUMER__BATCH_SIZE`, DLQ, tuning and metrics-window keys) |

Health-snapshot metrics:

| 0.x | 1.0.0 |
|-----|-------|
| `ConsumerHealthResponse.database` | `output` (outlet connectivity; outlet without an observable carrier reads `connected`) |
| `sink_write_rate` | `handle_rate` |
| `sink_write_failure_rate` | `handle_failure_rate` |
| `sink_write_latency_ms_avg` / `_max` | `handle_latency_ms_avg` / `_max` |
| `consume_rate` / `retry_rate` | unchanged |
| `IngestHealthResponse.receive_rate` | `ProducerHealthResponse.push_rate` |
| `IngestHealthResponse.admission_conflict_rate` | `ProducerHealthResponse.duplicate_rate` |

Log events (both sides keep stable names; producer-side field renames only):

| 0.x | 1.0.0 |
|-----|-------|
| `batch_write_start` | `batch_handle_start` |
| `batch_write_success` | `batch_handle_success` |
| `batch_write_failed` | `batch_handle_failed` |
| `batch_write_exhausted_retries` | `batch_handle_exhausted_retries` |
| `batch_write_success_with_quarantine` | `batch_handle_success_with_quarantine` |
| `consumer_paused_due_to_write_failures` | `consumer_paused_due_to_handle_failures` |
| `offset_commit_failed_after_write` | `offset_commit_failed_after_handle` |
| `write_failure_classified` | `handle_failure_classified` |
| `batch_bisect_triggered` / `bisect_aborted_probe_failed` / `consumer_record_quarantined` | unchanged |
| `existence_ttl_seconds` log field | `backlog_ttl_seconds` |
| `ingest_request` field `overwrite` | field `force` (event name unchanged) |
| `admission_send_success_hook_failed` / `admission_send_failed_hook_failed` | `dedup_send_success_hook_failed` / `dedup_send_failed_hook_failed` |
| `overwrite_rejected_redis_unavailable` / `overwrite_summary_write_failed` | `force_rejected_redis_unavailable` / `force_summary_write_failed` |
| `load_entity_failed` / `load_entity_failed_degraded` / `cold_path_db_load` | `load_identity_failed` / `load_identity_failed_degraded` / `cold_path_load` |
| `ingest_request` / `ingest_conflict` / `ingest_rejected_backpressure` / `kafka_*` | unchanged (the `error_code` field was dropped from push-path rejections) |

Import paths:

| 0.x | 1.0.0 |
|-----|-------|
| `streamgate.contrib.sql_sink` | `streamgate.contrib.sql_upsert` (base: `Upsert`, `upsert_outlet`, engine factory, `SqlBackfill`, `SQLAlchemyErrorClassifier`, `DbConfig`) |
| `streamgate.contrib.sqlite_sink` | `streamgate.contrib.sqlite_upsert` (exports `SqliteConsumer` factory) |
| `streamgate.contrib.mssql_sink` | `streamgate.contrib.mssql_upsert` (exports `MssqlConsumer` factory) |
| `UpsertWriter` (retired) | `upsert_outlet(db, upserts) -> tuple[BatchHandler, Probe]` |
| `streamgate.ingest.gateway` / `streamgate.specs` / `streamgate.ingest.admission` | deleted — use `streamgate.Producer` / `ProducerOptions` / `DedupOptions` |
| `streamgate.contrib.redis_admission` | `streamgate.contrib.redis_dedup` |

No compatibility shims are provided (consistent with a major boundary);
passing a retired keyword to `Consumer` or `Producer` raises a `TypeError`
that points to this section.

### Added

- **`Consumer`** — the flat, embeddable consume loop: four required arguments
  answer "which cluster / which topic / which identity / where data goes";
  `batch_size` (default 500) and `flush_timeout` (default 5.0s) are
  first-class; everything else lives in `ConsumerOptions` (with nested
  `DlqOptions` and `RuntimeTuning`) and defaults to zero conceptual load.
  Required settings fall back to environment variables (12-factor):
  `KAFKA__BOOTSTRAP_SERVERS` / `KAFKA__TOPIC` / `CONSUMER__GROUP_ID` (the
  handler is code-only by design).
- **`Producer`** — the flat, embeddable push entry: two required arguments
  (`bootstrap_servers`, `topic`, both with `KAFKA__*` env fallbacks) plus an
  optional routing `key` (same key ⇒ same partition ⇒ ordered; omitted ⇒
  round-robin, unordered — documented promise). `push(record, force=False,
  source=...)` returns a transport-neutral `PushResult` (`accepted` /
  `duplicate` / `rejected` / `backpressure` / `unavailable`) and never raises
  runtime exceptions for degraded dependencies. Dedup is opt-in via
  `ProducerOptions(dedup=DedupOptions(key=...))` — one line enables the
  built-in in-process carrier (`guarantee="process-local"`); injecting a
  `DedupCarrier` (e.g. `contrib.redis_dedup.RedisDedupCarrier`,
  `guarantee="distributed"`) scales the guarantee with your topology. The
  carrier self-reports its guarantee strength and it surfaces as
  `PushResult.guarantee` on every result. Rejection windows mean zero writes:
  no reservation, no shared-store write, no Kafka send. The Kafka
  connection's self-healing monitor (probing, single-flight rebuild,
  exponential backoff) is preserved internally — no separate class to manage.
- **Single-record probe (`ConsumerOptions.probe`)** — neutral primitive for
  precise poison isolation: when a handler raises a POISON-classified error
  and a probe is provided, the framework re-runs the probe per record
  (with reference comparison to distinguish data poison from outlet
  failure); probe-successful records count as handled and commit, probe-
  failing records are quarantined to the DLQ individually. Without a probe
  the whole batch is quarantined (offsets still commit, no poison retries).
  With the DLQ disabled the legacy paused self-healing applies.
- **`SqliteConsumer` / `MssqlConsumer` factories** — pre-assembled `Consumer`s
  for the SQL outlet: batch upsert handler + single-record probe + dialect
  error classifier wired, user `options` merged field-by-field over factory
  defaults. They return the core `Consumer` (no subclass hierarchy). SQLite
  auto-creates tables at start (unchanged); MSSQL schema stays with your
  migration tool.
- Old parameter names passed to `Consumer` or `Producer` fail with a
  `TypeError` carrying a migration hint.

### Changed

- Poison handling is probe-based instead of write-batch bisection: location
  calls the user probe record-by-record with reference verification; the
  safety invariants are unchanged (probe-failed entries are never
  quarantined; a suspected outlet failure aborts location and pauses —
  handled entries are idempotently re-processed on the next pass).
- `ConsumerHealthResponse` / `ConsumeMetrics` vocabulary neutralized to
  handle-rate/handle-latency; the producer side follows:
  `IngestHealthResponse` → `ProducerHealthResponse`,
  `receive_rate` → `push_rate`, `admission_conflict_rate` → `duplicate_rate`
  (mapping tables above).
- Handler objects (callables implementing `__call__`) may opt into framework-
  managed lifecycle via duck-typed `start()` / `close()` and into the health
  snapshot via `check_health()` (used by the SQL outlet to report
  `output` connectivity).
- Dedup carriers and backpressure signals implementing `start()`/`close()`
  get framework-managed lifecycles (same duck-typed contract as consumer
  hooks).

### Removed

- `ConsumeSpec`, `RecordWriter`, `WriteResult`, `RecordHandler`,
  `ConsumerWorker`, `ConsumerConfig`, `KafkaConfig.dlq_topic`,
  `contrib.sql_sink` / `contrib.sqlite_sink` / `contrib.mssql_sink`
  (renamed — see import mapping above).
- Producer side: `IngestBinding`, `IngestGateway`, `IngestOutcome`,
  `OutcomeKind`, `KafkaProducerService`, `InMemoryAdmission`, `NoAdmission`,
  the `streamgate.ingest.admission` package, `streamgate.specs`,
  `IngestRecordT`, and the HTTP-presentation contracts
  (`status_code` / `error_code` / `detail` fields on results and rejects,
  `backpressure_error_codes` / `backpressure_default_code` /
  `backpressure_detail` / `kafka_unavailable_code` /
  `kafka_unavailable_detail` / `request_log_context`).
- `contrib.redis_admission` (renamed to `contrib.redis_dedup` — see import
  mapping above; the per-entity query endpoint `entity_slots` and the
  empty-entity sentinel were not carried over: the single-identity-key model
  has no entity grouping).

## [0.5.0] - 2026-09-10

### Added

- **`AdmissionPolicy` gains `on_send_success` / `on_send_failed` hooks.**
  `on_send_success(record)` fires after every broker-acked Kafka send
  (overwrite path included); `on_send_failed(record)` fires when a send
  fails after the admission reservation was placed. The framework default
  (no-op) preserves current behavior — the placeholder stays reserved and
  self-heals via TTL/backfill — but implementations can now release the
  reservation to make the key immediately re-submittable, accepting the
  duplicate risk of ambiguous (timeout) failures.

### Changed

- **`AdmissionPolicy.on_accepted` renamed to `on_overwrite_accepted`.**
  The old hook only fired on the overwrite path (not on every accepted
  send, despite the name); the new name states that. Custom policies that
  implement `on_accepted` keep working: the gateway falls back to it when
  `on_overwrite_accepted` is absent. Built-in policies provide both
  (the old name delegates to the new one).

## [0.4.0] - 2026-09-08

### Added

- **Health snapshots now carry ops rate/latency metrics.** Both
  `IngestHealthResponse` and `ConsumerHealthResponse` gain sliding-window
  fields (all defaulting to `0.0`, computed on read over a 1-second bucket
  sliding window — no background tasks, no extra endpoint):
  - ingest: `receive_rate`, `admission_conflict_rate`,
    `backpressure_reject_rate`, `produce_success_rate`,
    `produce_failure_rate`, `produce_latency_ms_avg`,
    `produce_latency_ms_max`;
  - consume: `consume_rate`, `sink_write_rate`, `sink_write_failure_rate`,
    `retry_rate`, `sink_write_latency_ms_avg`, `sink_write_latency_ms_max`.
  Rates are records/second, latencies milliseconds; the window length is
  configurable via the new `MetricsConfig` (`METRICS__WINDOW_SECONDS`,
  default `60`, range 1–600). `IngestGateway(metrics_config=...)` and
  `ConsumerWorker(metrics_config=...)` accept it; when omitted everything
  works with the default window. Purely additive — old consumers that
  deserialize the snapshot ignore the new fields.
- **`MetricsConfig`** exported from the package root.

### Changed

- `ConsumeRuntime.__init__`: the structured-metrics sink parameter
  `metrics` was renamed to `metrics_sink` to free the `metrics` name for
  the new rate-metrics container (`ConsumeMetrics`). No in-repo caller
  passed it explicitly; external callers using `metrics=<MetricsSink>`
  must rename the keyword.

## [0.3.1] - 2026-09-08

### Fixed

- **`ConsumerWorker` now manages the `ConsumeSpec.persist_policy` lifecycle**
  (the `AdmissionPolicy` protocol promises "the framework guarantees call
  ordering", but the consumer entry point never honored it — unlike
  `IngestGateway`, which calls `start()`/`close()`). `prepare()` now calls
  `await policy.start()` before starting the sink, and `shutdown()` calls
  `await policy.close()` before closing the writer. Previously the consumer
  side never started the policy, so `on_persisted` authoritative refresh /
  TTL heartbeats failed on every batch (e.g. `RedisExistenceAdmission`
  logging `cache_refresh_failed: RedisExistenceCache not started`) and the
  failure surfaced only as per-record WARN logs. Lifecycle calls are
  duck-typed (defensive `getattr`) so legacy policy implementations without
  `start()`/`close()` keep working.
- **`ConsumerWorker(cache=...)` lifecycle is now symmetric.** The bypass
  `cache` parameter (health-probe component) is started in `prepare()` when
  it implements `start()`, instead of being closed at shutdown without ever
  having been started. `RedisExistenceCache.start()`/`close()` are
  idempotent, so sharing one cache instance between the policy and the
  bypass parameter remains safe (double start/close is a no-op).
  The pre-fix workaround (`await cache.start()` before `worker.run()`)
  is no longer required.

## [0.3.0] - 2026-09-08

**Contrib release.** The production-grade I/O strategies that 0.2.0 demoted to
copy-paste examples are back in the wheel — as `streamgate.contrib.*`,
importable, opt-in subpackages. The core stays pure: it gains no dependencies,
and it still never imports contrib (the import-linter forbidden contract is
replaced by a layered contract with the same guarantee). Purely additive; no
breaking changes.

### Added

- **`streamgate.contrib`** — official strategy layer (provisional / beta
  grade; ships in the wheel, third-party deps via extras, friendly
  "install streamgate[...]" errors when an extra is missing):
  - `contrib.redis_admission` (extra `[redis]`): `RedisExistenceAdmission`,
    `RedisExistenceCache`, `RedisConfig` — shared-storage uniqueness
    admission with atomic Lua reservation, TTL idle-GC, empty-entity
    sentinels, fail-closed semantics.
  - `contrib.sql_sink` / `contrib.sqlite_sink` / `contrib.mssql_sink`
    (extra `[sql]`): `Upsert`/`UpsertWriter` (`RecordWriter` impl),
    engine factory, `SqlBackfill` (`BackfillSource` impl),
    `SQLAlchemyErrorClassifier`, `DbConfig` — SQLite `ON CONFLICT` and
    MSSQL multi-row `MERGE` + HOLDLOCK dialects.
  - `contrib.http_probe` (extra `[http]`): `HttpProbeSignal` /
    `HysteresisController` — backlog-age trip/recover hysteresis,
    fail-closed probing, kafka-down fast reject.
- **Extras** `[redis]` / `[sql]` / `[http]` (new names; the pre-0.2.0 extras
  `[sqlite]` / `[mssql]` / `[http-probe]` / `[all]` are not restored).
- **`examples/prod_pipeline/`** — production-topology demo wiring the three
  contrib components together (Redis uniqueness admission + HTTP-probe
  backpressure + MSSQL idempotent sink; runs on SQLite locally via
  connection-string dialect selection).

### Changed

- **import-linter**: the "pure core" forbidden-modules contract is replaced
  by a layered contract — `streamgate.contrib` sits on top and may use the
  core; no core layer may import contrib. Bare-install purity is still
  CI-gated (bare install pulls exactly `aiokafka`/`loguru`/`pydantic`).
- **`examples/`**: the three strategy examples now import from
  `streamgate.contrib`; demo scripts (produce/consume/health_server/models)
  stay in the repo only. `pure_pipeline/` remains the zero-dependency main
  example and is unchanged.
- **Dev group** now depends on `streamgate[redis,sql,http]` instead of
  duplicating the third-party libs, keeping a single source of truth for
  contrib dependency versions.

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

[0.5.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.5.0
[0.4.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.4.0
[0.2.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.2.0
[0.1.0]: https://github.com/pwg-code/streamgate/releases/tag/v0.1.0
