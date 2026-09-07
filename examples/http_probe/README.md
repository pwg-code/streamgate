# http_probe — dynamic backpressure via `BackpressureSignal` injection

The former built-in `HttpProbeSignal` (health-probe polling with a trip/recover
hysteresis state machine, fail-closed probing, kafka-down fast reject) now
lives here as a copy-paste module. The core ships only zero-I/O static signals
(`ManualBackpressureSignal`, `AllowAllSignal`); dynamic topologies inject their
own signal — this is the reference.

## Layout

| Module | Contents |
|--------|----------|
| `probe_signal.py` | `HysteresisController` (OPEN↔REJECTING state machine, backlog-age trip/recover, non-age reject recovery) + `HttpProbeSignal` (httpx polling with in-cycle retries) |
| `health_server.py` | demo-grade stdlib health endpoint serving `worker.health_snapshot()` — swap in your web framework for production |

## Run

```bash
# infrastructure
docker compose up -d kafka         # from examples/docker-compose.yml

pip install streamgate httpx

# consume — hosts GET /health on :9109 (terminal 1)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python consume.py

# produce — probes the consumer health endpoint (terminal 2)
KAFKA__BOOTSTRAP_SERVERS=localhost:29092 python produce.py
# o-1: accepted ... o-5: accepted

# stop consume.py, run produce.py again → fail-closed rejection:
# o-1: backpressure (reason=unreachable)
```

## Semantics

- **Trip**: `backlog_age_seconds > trip_seconds` → start rejecting (503-style).
- **Recover**: `backlog_age_seconds < recover_seconds` → resume (hysteresis
  anti-flapping; the band prevents OPEN/REJECT flapping).
- **Fail-closed**: probe unreachable after retries → reject
  (`fail_closed_on_unreachable=false` restores fail-open).
- **kafka down**: consumer reports kafka disconnected → reject immediately
  (metrics are meaningless once the consumer stops draining).

All knobs live on `BackpressureConfig` (see [CONFIGURATION.md](../../CONFIGURATION.md)).
