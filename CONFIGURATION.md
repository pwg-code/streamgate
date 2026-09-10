# Configuration reference

streamgate components are configured with typed parameters and Pydantic objects.
The consumer side takes its required settings as flat constructor arguments with
environment-variable fallback; the ingest side uses config objects whose fields
map 1:1 to environment variables using a `PREFIX__field` convention (double
underscore):

| Prefix | Config object / consumer |
|--------|--------------------------|
| `KAFKA__` | `KafkaConfig` (ingest) + `Consumer` fallbacks |
| `CONSUMER__` | `Consumer` fallbacks + `ConsumerOptions` |
| `BACKPRESSURE__` | `BackpressureConfig` |
| `METRICS__` | metrics window |

**Fallback chain (consumer side):** explicit argument > environment variable >
built-in default. **Fail-fast semantics:** required settings without a built-in
default (topic, group id) fail at construction with an error that names the
setting and how to fix it.

Database and Redis connection settings are **not part of the core framework**:
storage carriers are injected and their configuration belongs to your
application. The official strategy implementations under `streamgate.contrib`
ship typed config objects (below) that you construct and pass to the factories.

---

## Consumer (flat arguments + env fallbacks)

| Argument | Env fallback | Default | Description |
|----------|--------------|---------|-------------|
| `bootstrap_servers` | `KAFKA__BOOTSTRAP_SERVERS` | `kafka:9092` | Comma-separated broker list (`http://`/`kafka://` scheme prefixes are stripped). |
| `topic` | `KAFKA__TOPIC` | **required** | Topic to consume from. Missing ⇒ construction error. |
| `group_id` | `CONSUMER__GROUP_ID` | **required** | Kafka consumer group id. Missing ⇒ construction error. |
| `handler` | — (code only) | **required** | Async callable `handler(batch, context)` — the only outlet. |
| `batch_size` | `CONSUMER__BATCH_SIZE` | `500` | Records buffered before a flush. `1` = single-record real-time. |
| `flush_timeout` | `CONSUMER__FLUSH_TIMEOUT_SECONDS` | `5.0` | Max wait before a partially filled batch is flushed. |
| `options` | — | `None` | `ConsumerOptions` — everything below. |

## ConsumerOptions

| Field | Default | Description |
|-------|---------|-------------|
| `expected_type` | `None` | Envelope `type` validation (`None` = accept any; mismatched records are quarantined to the DLQ). |
| `probe` | `None` | Single-record callable with handler-equivalent, idempotent semantics. Provided ⇒ POISON batches are located record-by-record and bad records quarantined precisely; omitted ⇒ the whole batch is quarantined. |
| `classifier` | `DefaultErrorClassifier` | RETRY/POISON/FATAL mapping for handler exceptions. Connecting a DB? Inject a DB-aware classifier (`contrib.sql_upsert.SQLAlchemyErrorClassifier`). |
| `persist_hook` | `None` | `AdmissionPolicy` hooked into the consume loop: `on_persisted` runs after each successful batch (ingest linkage). Lifecycle is framework-managed. |
| `collapse_key` | `None` | In-batch dedup key (keeps the last record) applied to hook notifications. |
| `log_context` | `None` | Per-record extra fields for quarantine logs. |
| `codec` | `JsonEnvelopeCodec` | Message envelope codec. |
| `health_probe` | `None` | Side-channel health component; implements `check_health()` ⇒ observed in snapshots; `start()`/`close()` ⇒ lifecycle managed. |
| `backlog_ttl_seconds` | `18000` | Backlog alert budget: WARN > TTL/2, ERROR > TTL×0.8. |
| `metrics_window_seconds` | `METRICS__WINDOW_SECONDS` → `60` | Sliding-window length (1–600) for rate/latency snapshot fields. |
| `dlq` | `DlqOptions()` | DLQ options (below). |
| `tuning` | `RuntimeTuning()` | Runtime tuning (below). |

## DlqOptions

| Field | Env fallback | Default | Description |
|-------|--------------|---------|-------------|
| `enabled` | `CONSUMER__DLQ_ENABLED` | `true` | Master switch; `false` falls back to the paused behavior (emergency escape hatch). |
| `topic` | `KAFKA__DLQ_TOPIC` | `None` | Dead-letter topic; required when DLQ is enabled. |
| `message_type` | — | `streamgate_dlq` | Envelope `type` of DLQ messages. |
| `send_retries` | `CONSUMER__DLQ_SEND_RETRIES` | `3` | Total attempts per DLQ send (exhausting them pauses the batch). |

## RuntimeTuning

Each field falls back to its `CONSUMER__*` environment variable, then to the
built-in default:

| Field | Env fallback | Default | Description |
|-------|--------------|---------|-------------|
| `max_retries` | `CONSUMER__MAX_RETRIES` | `3` | Handler retries per batch. |
| `retry_backoff_base` | `CONSUMER__RETRY_BACKOFF_BASE` | `1.0` | Retry backoff base (seconds, exponential). |
| `reconnect_base` | `CONSUMER__RECONNECT_BASE_BACKOFF_SECONDS` | `1.0` | Reconnect/paused-recovery backoff base (seconds). |
| `reconnect_max` | `CONSUMER__RECONNECT_MAX_BACKOFF_SECONDS` | `30.0` | Reconnect backoff cap (seconds). |
| `max_poll_records` | `CONSUMER__MAX_POLL_RECORDS` | `500` | Max records per poll. |
| `session_timeout_ms` | `CONSUMER__SESSION_TIMEOUT_MS` | `30000` | Kafka session timeout. |
| `max_poll_interval_ms` | `CONSUMER__MAX_POLL_INTERVAL_MS` | `300000` | Max time between polls before rebalance. |
| `auto_offset_reset` | `CONSUMER__AUTO_OFFSET_RESET` | `earliest` | Offset reset policy. |
| `backlog_check_interval` | `CONSUMER__BACKLOG_CHECK_INTERVAL_SECONDS` | `30.0` | Backlog age check period (seconds). |

---

## Contrib strategy configs

These are plain constructor arguments (not env-mapped). All belong to
`streamgate.contrib` packages — install the matching extra first
(`[redis]` / `[sql]` / `[http]`).

### `contrib.redis_admission.RedisConfig`

| Field | Default | Description |
|-------|---------|-------------|
| `url` | `redis://localhost:6379/0` | Redis connection URL. |
| `key_prefix` | `streamgate:` | Key prefix for existence hashes. |
| `existence_ttl_seconds` | `18000` | Existence hash TTL (idle-GC heartbeat: every write renews it). |
| `empty_existence_ttl_seconds` | `3600` | Empty-entity sentinel TTL (shorter than existence TTL). |
| `socket_timeout_ms` | `1000` | Read/query path timeout. |
| `recv_timeout_ms` | `500` | Ingest path (reserve/summary write) timeout. |
| `fail_closed_on_unavailable` | `true` | Reject instead of silently degrading when Redis is unavailable (`false` = legacy degraded escape hatch). |

Strategy-level knobs live on `RedisExistenceAdmissionConfig`
(`fail_closed_on_unavailable`, `cold_path_max_concurrency`, gate-full /
unavailable `retry_after` seconds, stable error codes, `log_context` mapping).

### `contrib.sql_upsert.DbConfig`

| Field | Default | Description |
|-------|---------|-------------|
| `connection_string` | **required** | SQLAlchemy async URL (`sqlite+aiosqlite:///...` or `mssql+aioodbc://...`). Missing ⇒ assembly error with fix instructions. |
| `echo` | `false` | SQLAlchemy echo logging. |
| `write_timeout_seconds` | `20` | Driver statement timeout (MSSQL/pyodbc hook; must be < `write_wait_seconds`). |
| `write_wait_seconds` | `25` | Batch upsert call-level `wait_for` ceiling. |
| `pool_timeout_seconds` | `3` | Pool checkout timeout (fail fast when exhausted). |
| `write_pool_size` | `10` | Fixed write-pool connections. |
| `write_pool_max_overflow` | `20` | Write-pool overflow connections. |

The dialect (SQLite vs MSSQL) is selected by the connection string;
`contrib.sqlite_upsert` and `contrib.mssql_upsert` are the pre-assembled
factory entries (`SqliteConsumer` / `MssqlConsumer`).

### Backpressure probing (contrib.http_probe)

`HttpProbeSignal` consumes the same `BACKPRESSURE__*` environment mapping via
`BackpressureConfig` (see below) — no extra configuration surface.

---

## KafkaConfig (`KAFKA__*`, ingest side)

| Env var | Default | Description |
|---------|---------|-------------|
| `KAFKA__BOOTSTRAP_SERVERS` | `kafka:9092` | Comma-separated broker list. `http://` / `https://` / `kafka://` scheme prefixes are stripped automatically. Also the consumer-side `bootstrap_servers` fallback. |
| `KAFKA__TOPIC` | **required** | Default topic for the producer (and the consumer-side `topic` fallback). Missing ⇒ startup failure. |
| `KAFKA__ACKS` | `all` | Producer acks level. |
| `KAFKA__REQUEST_TIMEOUT_MS` | `10000` | Producer request timeout (ms). |
| `KAFKA__ENABLE_IDEMPOTENCE` | `true` | Idempotent producer (safe retries). |
| `KAFKA__HEALTH_CHECK_INTERVAL_SECONDS` | `30.0` | Periodic health probe interval; unhealthy instances are rebuilt. |
| `KAFKA__RECONNECT_BASE_BACKOFF_SECONDS` | `1.0` | Reconnect exponential backoff base. |
| `KAFKA__RECONNECT_MAX_BACKOFF_SECONDS` | `30.0` | Reconnect backoff cap. |
| `KAFKA__RECONNECT_FAILURE_THRESHOLD` | `1` | Consecutive send failures before a rebuild is triggered (raise to tolerate broker jitter). |
| `KAFKA__SEND_FAILURE_WINDOW_SECONDS` | `30.0` | A send failure within this window marks the instance unhealthy. |
| `KAFKA__UNHEALTHY_CHECK_INTERVAL_SECONDS` | `5.0` | High-frequency probe interval while unhealthy/rebuilding. |

The dead-letter topic is configured on the consumer side via
`DlqOptions.topic` / `KAFKA__DLQ_TOPIC` (see above) — no longer on
`KafkaConfig`.

## BackpressureConfig (`BACKPRESSURE__*`)

The gateway defaults to the built-in `ManualBackpressureSignal` (static
switch). For dynamic probing inject your own `BackpressureSignal` —
`contrib.http_probe.HttpProbeSignal` reads this config object.

| Env var | Default | Description |
|---------|---------|-------------|
| `BACKPRESSURE__ENABLED` | `true` | Master switch; `false` = fully open (emergency rollback). |
| `BACKPRESSURE__CONSUMER_HEALTH_URL` | `http://localhost:9109/health` | Consumer health endpoint polled by the probe. |
| `BACKPRESSURE__CHECK_INTERVAL_SECONDS` | `30.0` | Poll period while OPEN. |
| `BACKPRESSURE__TIMEOUT_SECONDS` | `2.0` | Per-probe timeout. |
| `BACKPRESSURE__PROBE_RETRIES` | `3` | Retries per cycle (excluding the first attempt); `0` disables. |
| `BACKPRESSURE__PROBE_RETRY_INTERVAL_SECONDS` | `15.0` | Interval between retries. |
| `BACKPRESSURE__TRIP_SECONDS` | `9000.0` | Reject when backlog age exceeds this. |
| `BACKPRESSURE__RECOVER_SECONDS` | `7200.0` | Resume when backlog age falls below this (hysteresis anti-flapping). |
| `BACKPRESSURE__RETRY_AFTER_SECONDS` | `60` | `Retry-After` hint (seconds) while rejecting. |
| `BACKPRESSURE__FAIL_CLOSED_ON_UNREACHABLE` | `true` | Treat unreachable probe (retries exhausted) as backlog-exceeded. |
| `BACKPRESSURE__REJECT_ON_ANY_DEGRADED` | `false` | Legacy escape hatch: reject when any component is degraded. |
| `BACKPRESSURE__UNHEALTHY_CHECK_INTERVAL_SECONDS` | `5.0` | Poll period while REJECTING. |

## Metrics window (`METRICS__*`)

Health-snapshot rate metrics: both `IngestGateway` and `Consumer` maintain
in-memory sliding windows and expose computed rates / latencies through their
health snapshots (`receive_rate`, `handle_rate`, `produce_latency_ms_avg`,
...). Windowed aggregates are computed on read — no background tasks, no extra
endpoint; rates decay to `0.0` once traffic stops for longer than the window.

| Env var | Default | Description |
|---------|---------|-------------|
| `METRICS__WINDOW_SECONDS` | `60` | Sliding-window length (seconds) for all rate/latency fields. Valid range 1–600; out-of-range values fail at startup. Short windows react faster but are noisier. |
