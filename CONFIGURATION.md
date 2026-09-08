# Configuration reference

streamgate components are configured with typed Pydantic objects whose fields map
1:1 to environment variables using a `PREFIX__field` convention (double
underscore):

| Prefix | Config object |
|--------|---------------|
| `KAFKA__` | `KafkaConfig` |
| `CONSUMER__` | `ConsumerConfig` |
| `BACKPRESSURE__` | `BackpressureConfig` |
| `METRICS__` | `MetricsConfig` |

**Fail-fast semantics:** required settings are not defaulted. If a required value
is missing, assembly fails at startup with an error that names the setting and
how to fix it — the process never runs with project-specific guessed defaults.

Database and Redis connection settings are **not part of the core framework**:
storage carriers are injected via protocols and their configuration belongs to
your application. The official strategy implementations under
`streamgate.contrib` ship typed config objects (below) that you construct and
pass to the strategy classes.

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

### `contrib.sql_sink.DbConfig`

| Field | Default | Description |
|-------|---------|-------------|
| `connection_string` | **required** | SQLAlchemy async URL (`sqlite+aiosqlite:///...` or `mssql+aioodbc://...`). Missing ⇒ assembly error with fix instructions. |
| `echo` | `false` | SQLAlchemy echo logging. |
| `write_timeout_seconds` | `20` | Driver statement timeout (MSSQL/pyodbc hook; must be < `write_wait_seconds`). |
| `write_wait_seconds` | `25` | `UpsertWriter.write` call-level `wait_for` ceiling. |
| `pool_timeout_seconds` | `3` | Pool checkout timeout (fail fast when exhausted). |
| `write_pool_size` | `10` | Fixed write-pool connections. |
| `write_pool_max_overflow` | `20` | Write-pool overflow connections. |

The dialect (SQLite vs MSSQL) is selected by the connection string;
`contrib.sqlite_sink` and `contrib.mssql_sink` are thin entry packages
documenting each backend.

### Backpressure probing (contrib.http_probe)

`HttpProbeSignal` consumes the same `BACKPRESSURE__*` environment mapping via
`BackpressureConfig` (see below) — no extra configuration surface.

---

## KafkaConfig (`KAFKA__*`)

| Env var | Default | Description |
|---------|---------|-------------|
| `KAFKA__BOOTSTRAP_SERVERS` | `kafka:9092` | Comma-separated broker list. `http://` / `https://` / `kafka://` scheme prefixes are stripped automatically. |
| `KAFKA__TOPIC` | **required** | Default topic for producer and consumer. Missing ⇒ startup failure (producer/consumer assembly validates). |
| `KAFKA__DLQ_TOPIC` | `None` (required when DLQ enabled) | Dead-letter topic for quarantined poison records. Required when `dlq_enabled` is on and no per-spec override is set. |
| `KAFKA__ACKS` | `all` | Producer acks level. |
| `KAFKA__REQUEST_TIMEOUT_MS` | `10000` | Producer request timeout (ms). |
| `KAFKA__ENABLE_IDEMPOTENCE` | `true` | Idempotent producer (safe retries). |
| `KAFKA__HEALTH_CHECK_INTERVAL_SECONDS` | `30.0` | Periodic health probe interval; unhealthy instances are rebuilt. |
| `KAFKA__RECONNECT_BASE_BACKOFF_SECONDS` | `1.0` | Reconnect exponential backoff base. |
| `KAFKA__RECONNECT_MAX_BACKOFF_SECONDS` | `30.0` | Reconnect backoff cap. |
| `KAFKA__RECONNECT_FAILURE_THRESHOLD` | `1` | Consecutive send failures before a rebuild is triggered (raise to tolerate broker jitter). |
| `KAFKA__SEND_FAILURE_WINDOW_SECONDS` | `30.0` | A send failure within this window marks the instance unhealthy. |
| `KAFKA__UNHEALTHY_CHECK_INTERVAL_SECONDS` | `5.0` | High-frequency probe interval while unhealthy/rebuilding. |

## ConsumerConfig (`CONSUMER__*`)

| Env var | Default | Description |
|---------|---------|-------------|
| `CONSUMER__GROUP_ID` | **required** | Kafka consumer group id. Missing ⇒ startup failure. |
| `CONSUMER__BATCH_SIZE` | `100` | Records buffered before a write attempt. |
| `CONSUMER__BATCH_TIMEOUT_SECONDS` | `5.0` | Max wait before a partially filled batch is flushed. |
| `CONSUMER__MAX_RETRIES` | `3` | Write retries per batch. |
| `CONSUMER__RETRY_BACKOFF_BASE` | `1.0` | Write retry backoff base (seconds). |
| `CONSUMER__RECONNECT_BASE_BACKOFF_SECONDS` | `1.0` | Consumer reconnect backoff base. |
| `CONSUMER__RECONNECT_MAX_BACKOFF_SECONDS` | `30.0` | Consumer reconnect backoff cap. |
| `CONSUMER__MAX_POLL_RECORDS` | `500` | Max records per poll. |
| `CONSUMER__SESSION_TIMEOUT_MS` | `30000` | Kafka session timeout. |
| `CONSUMER__MAX_POLL_INTERVAL_MS` | `300000` | Max time between polls before rebalance. |
| `CONSUMER__AUTO_OFFSET_RESET` | `earliest` | Offset reset policy. |
| `CONSUMER__BACKLOG_CHECK_INTERVAL_SECONDS` | `30.0` | Backlog age check period (feeds backpressure metrics). |
| `CONSUMER__DLQ_ENABLED` | `true` | DLQ master switch; `false` falls back to the legacy paused behavior (emergency escape hatch). |
| `CONSUMER__DLQ_SEND_RETRIES` | `3` | Total attempts per DLQ send (exhausting them pauses the batch). |

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

## MetricsConfig (`METRICS__*`)

Health-snapshot rate metrics: both `IngestGateway` and `ConsumerWorker`
maintain in-memory sliding windows and expose computed rates / latencies
through their health snapshots (`receive_rate`, `sink_write_rate`,
`produce_latency_ms_avg`, ...). Windowed aggregates are computed on read —
no background tasks, no extra endpoint; rates decay to `0.0` once traffic
stops for longer than the window.

| Env var | Default | Description |
|---------|---------|-------------|
| `METRICS__WINDOW_SECONDS` | `60` | Sliding-window length (seconds) for all rate/latency fields. Valid range 1–600; out-of-range values fail at startup. Short windows react faster but are noisier. |
