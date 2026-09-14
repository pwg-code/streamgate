# Configuration reference

streamgate components are configured with typed parameters and Pydantic
objects. There are exactly **two sources of truth**:

1. **Required settings are true required constructor arguments** —
   `Producer(bootstrap_servers, topic, ...)` and
   `Consumer(bootstrap_servers, topic, group_id, handler)`. Missing one fails
   with a native Python `TypeError` (missing argument) at construction. The
   library never reads environment variables and never silently defaults a
   required setting.
2. **Optional settings directly hold their built-in defaults** — advanced
   knobs fold into `ProducerOptions` / `ConsumerOptions` (dedup, backpressure,
   DLQ, tuning, metrics window); omit them and the documented defaults apply.

If your host wants env-driven configuration, resolve the environment yourself
and pass the values explicitly (the examples do exactly that with
`os.environ.get(..., default)`).

Database and Redis connection settings are **not part of the core framework**:
storage carriers are injected and their configuration belongs to your
application. The official strategy implementations under `streamgate.contrib`
ship typed config objects (below) that you construct and pass to the factories.

## Logging (not a configuration surface)

streamgate emits logs through the standard-library `logging` module and ships
**no logging configuration** — the library only emits records under the
`streamgate.*` logger tree; level, handlers and formats are configured by the
host application (e.g. `logging.getLogger("streamgate")`).

---

## Consumer (flat arguments)

| Argument | Default | Description |
|----------|---------|-------------|
| `bootstrap_servers` | **required** | Comma-separated broker list. Missing ⇒ native `TypeError`. |
| `topic` | **required** | Topic to consume from. Missing ⇒ native `TypeError`. |
| `group_id` | **required** | Kafka consumer group id. Missing ⇒ native `TypeError`. |
| `handler` | **required** | Async callable `handler(batch, context)` — the only outlet. |
| `batch_size` | `500` | Records buffered before a flush. `1` = single-record real-time. |
| `flush_timeout` | `5.0` | Max wait before a partially filled batch is flushed. |
| `options` | `None` | `ConsumerOptions` — everything below. |

## ConsumerOptions

| Field | Default | Description |
|-------|---------|-------------|
| `expected_type` | `None` | Envelope `type` validation (`None` = accept any; mismatched records are quarantined to the DLQ). |
| `probe` | `None` | Single-record callable with handler-equivalent, idempotent semantics. Provided ⇒ POISON batches are located record-by-record and bad records quarantined precisely; omitted ⇒ the whole batch is quarantined. |
| `classifier` | `DefaultErrorClassifier` | RETRY/POISON/FATAL mapping for handler exceptions. Connecting a DB? Inject a DB-aware classifier (`contrib.sql_upsert.SQLAlchemyErrorClassifier`). |
| `persist_hook` | `None` | `DedupCarrier` hooked into the consume loop: `on_persisted` runs after each successful batch (dedup linkage with the producer side). Lifecycle is framework-managed. |
| `collapse_key` | `None` | In-batch dedup key (keeps the last record) applied to hook notifications. |
| `log_context` | `None` | Per-record extra fields for quarantine logs. |
| `codec` | `JsonEnvelopeCodec` | Message envelope codec. |
| `health_probe` | `None` | Side-channel health component; implements `check_health()` ⇒ observed in snapshots; `start()`/`close()` ⇒ lifecycle managed. |
| `backlog_ttl_seconds` | `18000` | Backlog alert budget: WARN > TTL/2, ERROR > TTL×0.8. |
| `metrics_window_seconds` | `60` | Sliding-window length (1–600) for rate/latency snapshot fields; out-of-range values fail at construction. |
| `dlq` | `DlqOptions()` | DLQ options (below). |
| `tuning` | `RuntimeTuning()` | Runtime tuning (below). |

## DlqOptions

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Master switch; `false` falls back to the paused behavior (emergency escape hatch). |
| `topic` | `None` | Dead-letter topic; required when DLQ is enabled (validated at DLQ-producer construction). |
| `message_type` | `streamgate_dlq` | Envelope `type` of DLQ messages. |
| `send_retries` | `3` | Total attempts per DLQ send (exhausting them pauses the batch). |

## RuntimeTuning

Every field directly holds its built-in default:

| Field | Default | Description |
|-------|---------|-------------|
| `max_retries` | `3` | Handler retries per batch. |
| `retry_backoff_base` | `1.0` | Retry backoff base (seconds, exponential). |
| `reconnect_base` | `1.0` | Reconnect/paused-recovery backoff base (seconds). |
| `reconnect_max` | `30.0` | Reconnect backoff cap (seconds). |
| `max_poll_records` | `500` | Max records per poll. |
| `session_timeout_ms` | `30000` | Kafka session timeout. |
| `max_poll_interval_ms` | `300000` | Max time between polls before rebalance. |
| `auto_offset_reset` | `earliest` | Offset reset policy. |
| `backlog_check_interval` | `30.0` | Backlog age check period (seconds). |

---

## Contrib strategy configs

These are plain constructor arguments. All belong to `streamgate.contrib`
packages — install the matching extra first (`[redis]` / `[sql]` / `[http]`).

### `contrib.redis_dedup.RedisConfig`

| Field | Default | Description |
|-------|---------|-------------|
| `url` | `redis://localhost:6379/0` | Redis connection URL. |
| `key_prefix` | `streamgate:` | Key prefix for dedup keys (`<prefix>dedup:<identity>`). |
| `identity_ttl_seconds` | `18000` | Single-key placeholder/summary TTL (idle-GC heartbeat: every write renews it). |
| `group_key_prefix` | `dedup_group:` | Group-carrier key prefix (`<prefix>dedup_group:<group>` HASH; type-isolated from the single-key STRING). |
| `group_ttl_seconds` | `18000` | Group HASH TTL (idle-GC heartbeat: any group write renews it). |
| `socket_timeout_ms` | `1000` | Read/query path timeout. |
| `recv_timeout_ms` | `500` | Push path (reserve/summary write) timeout. |

Strategy-level knobs live on `RedisDedupCarrierConfig`
(`fail_closed_on_unavailable`, `cold_path_max_concurrency`, gate-full /
unavailable `retry_after` seconds, `log_context` mapping) and on
`RedisGroupDedupCarrierConfig` (`group_cold_path_max_concurrency`, same
family of knobs).

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

`HttpProbeSignal` is constructed with a `BackpressureConfig` instance (see
below) — build the config object yourself and pass it to the producer via
`ProducerOptions(signal=HttpProbeSignal(config), backpressure=config)`.

---

## KafkaConfig (`ProducerOptions.kafka`)

The producer takes `bootstrap_servers`/`topic` as **required** flat
constructor arguments; the remaining connection/self-healing knobs live on
`KafkaConfig`, injectable via `ProducerOptions.kafka` (defaults below). The
constructor values always override the `bootstrap_servers` / `topic` fields.

| Field | Default | Description |
|-------|---------|-------------|
| `bootstrap_servers` | `kafka:9092` | Comma-separated broker list. `http://` / `https://` / `kafka://` scheme prefixes are stripped automatically. Always overridden by the `Producer` constructor value. |
| `topic` | `None` | Topic to push to (constructor value wins; validated when a raw `KafkaConfig` is used directly). |
| `acks` | `all` | Producer acks level. |
| `request_timeout_ms` | `10000` | Producer request timeout (ms). |
| `enable_idempotence` | `true` | Idempotent producer (safe retries). |
| `health_check_interval_seconds` | `30.0` | Periodic health probe interval; unhealthy instances are rebuilt. |
| `reconnect_base_backoff_seconds` | `1.0` | Reconnect exponential backoff base. |
| `reconnect_max_backoff_seconds` | `30.0` | Reconnect backoff cap. |
| `reconnect_failure_threshold` | `1` | Consecutive send failures before a rebuild is triggered (raise to tolerate broker jitter). |
| `send_failure_window_seconds` | `30.0` | A send failure within this window marks the instance unhealthy. |
| `unhealthy_check_interval_seconds` | `5.0` | High-frequency probe interval while unhealthy/rebuilding. |

The dead-letter topic is configured on the consumer side via
`DlqOptions.topic` (see above) — no longer on `KafkaConfig`.

## BackpressureConfig (`ProducerOptions.backpressure`)

The producer defaults to the built-in `ManualBackpressureSignal` (static
switch, backpressure off). For dynamic probing pass
`options=ProducerOptions(backpressure=..., signal=...)` —
`contrib.http_probe.HttpProbeSignal` reads this config object.

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `true` | Master switch; `false` = fully open (emergency rollback). |
| `consumer_health_url` | `http://localhost:9109/health` | Consumer health endpoint polled by the probe. |
| `check_interval_seconds` | `30.0` | Poll period while OPEN. |
| `timeout_seconds` | `2.0` | Per-probe timeout. |
| `probe_retries` | `3` | Retries per cycle (excluding the first attempt); `0` disables. |
| `probe_retry_interval_seconds` | `15.0` | Interval between retries. |
| `trip_seconds` | `9000.0` | Reject when backlog age exceeds this. |
| `recover_seconds` | `7200.0` | Resume when backlog age falls below this (hysteresis anti-flapping). |
| `retry_after_seconds` | `60` | `Retry-After` hint (seconds) while rejecting. |
| `fail_closed_on_unreachable` | `true` | Treat unreachable probe (retries exhausted) as backlog-exceeded. |
| `reject_on_any_degraded` | `false` | Legacy escape hatch: reject when any component is degraded. |
| `unhealthy_check_interval_seconds` | `5.0` | Poll period while REJECTING. |

## Metrics window

Health-snapshot rate metrics: both `Producer` and `Consumer` maintain
in-memory sliding windows and expose computed rates / latencies through their
health snapshots (`push_rate`, `handle_rate`, `produce_latency_ms_avg`,
...). Windowed aggregates are computed on read — no background tasks, no extra
endpoint; rates decay to `0.0` once traffic stops for longer than the window.

Configure via `metrics_window_seconds` on `ProducerOptions` /
`ConsumerOptions`:

| Value | Description |
|-------|-------------|
| `60` (default) | Sliding-window length (seconds) for all rate/latency fields. Valid range 1–600; out-of-range values fail at construction. Short windows react faster but are noisier. |
