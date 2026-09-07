# Configuration reference

streamgate components are configured with typed Pydantic objects whose fields map
1:1 to environment variables using a `PREFIX__field` convention (double
underscore):

| Prefix | Config object |
|--------|---------------|
| `KAFKA__` | `KafkaConfig` |
| `CONSUMER__` | `ConsumerConfig` |
| `DB__` | `DbConfig` |
| `REDIS__` | `RedisConfig` |
| `BACKPRESSURE__` | `BackpressureConfig` |

**Fail-fast semantics:** required settings are not defaulted. If a required value
is missing, assembly fails at startup with an error that names the setting and
how to fix it — the process never runs with project-specific guessed defaults.

Extras annotations: fields that require a database or Redis **connection** only
take effect when the matching extra is installed — `[sqlite]`, `[mssql]` or
`[redis]`. A pure Kafka pipeline (`pip install streamgate`) needs none of them.

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

## DbConfig (`DB__*`)

Needs extras: `[sqlite]` for `sqlite+aiosqlite://...` connection strings,
`[mssql]` for SQL Server (`mssql+aioodbc://...`).

| Env var | Default | Description |
|---------|---------|-------------|
| `DB__CONNECTION_STRING` | **required when upserts / backfill are declared** | SQLAlchemy async connection string. Missing ⇒ assembly failure at engine creation with fix instructions (a pure Kafka pipeline never touches it). |
| `DB__ECHO` | `false` | SQLAlchemy SQL echo. |
| `DB__QUERY_TIMEOUT_SECONDS` | `5` | Driver statement timeout for existence backfill queries (must be ≤ `DB__QUERY_WAIT_SECONDS`). |
| `DB__QUERY_WAIT_SECONDS` | `8` | Call-level `wait_for` around backfill queries. |
| `DB__WRITE_TIMEOUT_SECONDS` | `20` | Driver statement timeout for batch writes (must be ≤ `DB__WRITE_WAIT_SECONDS`). |
| `DB__WRITE_WAIT_SECONDS` | `25` | Call-level `wait_for` around batch writes. |
| `DB__POOL_TIMEOUT_SECONDS` | `3` | Connection pool checkout timeout (fails fast when exhausted). |
| `DB__COLD_PATH_MAX_CONCURRENCY` | `10` | Concurrency gate for cold-entity backfill (`<=0` disables). |
| `DB__COLD_PATH_GATE_RETRY_AFTER_SECONDS` | `1` | Suggested retry interval when the gate is full. |
| `DB__READ_POOL_SIZE` | `10` | Read pool size (ingest-side backfill). |
| `DB__READ_POOL_MAX_OVERFLOW` | `10` | Read pool overflow. |
| `DB__WRITE_POOL_SIZE` | `10` | Write pool size (consumer-side batch writes). |
| `DB__WRITE_POOL_MAX_OVERFLOW` | `20` | Write pool overflow. |

## RedisConfig (`REDIS__*`)

Needs extra: `[redis]`. Only relevant when the `redis-existence` admission
strategy (or its query endpoints) is used.

| Env var | Default | Description |
|---------|---------|-------------|
| `REDIS__URL` | `redis://localhost:6379/0` | Redis connection URL. |
| `REDIS__KEY_PREFIX` | `streamgate:` | Prefix for existence keys. |
| `REDIS__EXISTENCE_TTL_SECONDS` | `18000` | Existence entry TTL (5h; idle-GC via heartbeat renewal). |
| `REDIS__EMPTY_EXISTENCE_TTL_SECONDS` | `3600` | Empty-entity sentinel TTL (shorter than existence TTL). |
| `REDIS__SOCKET_TIMEOUT_MS` | `1000` | Query/validation path socket timeout. |
| `REDIS__RECV_TIMEOUT_MS` | `500` | Reservation/summary write timeout on the ingest path. |
| `REDIS__FAIL_CLOSED_ON_UNAVAILABLE` | `true` | Fail closed when Redis is unavailable (reject instead of silently degrading); `false` restores the legacy degraded behavior. |

## BackpressureConfig (`BACKPRESSURE__*`)

The built-in HTTP probe signal needs extra: `[http-probe]`. Alternatively inject
your own `BackpressureSignal` (no extra required).

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
