"""Consumer 高级项收口：ConsumerOptions / DlqOptions / RuntimeTuning。

设计原则：必填项平铺在 Consumer 构造器上；高级项收进本模块的 Options
对象，不传 = 全默认零概念负担。所有可选项在未显式传值时回退对应环境
变量（12-factor），再回退内置默认值——三层回退链：
显式传值 > 环境变量 > 内置默认。
"""

import os
from collections.abc import Callable, Hashable
from dataclasses import dataclass, field

from streamgate.protocols import (
    DedupCarrier,
    ErrorClassifier,
    JsonObject,
    MessageCodec,
    Probe,
)

# 积压告警阈值基准（无注入载体 TTL 时的默认预算，行为与历史一致）
DEFAULT_BACKLOG_TTL_SECONDS = 18000
DEFAULT_METRICS_WINDOW_SECONDS = 60
METRICS_WINDOW_RANGE = (1, 600)


def env_str(key: str) -> str | None:
    """读取环境变量（去空白；空串视为未设置）。"""
    value = os.environ.get(key)
    if value is None or not value.strip():
        return None
    return value.strip()


def env_bool(key: str, default: bool) -> bool:
    """布尔环境变量回退：true/1/yes/on，false/0/no/off（大小写不敏感）。"""
    raw = env_str(key)
    if raw is None:
        return default
    lowered = raw.lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise ValueError(
        f"env {key}={raw!r} is not a valid boolean; "
        "use true/false (e.g. CONSUMER__DLQ_ENABLED=false)"
    )


def env_int(key: str, default: int) -> int:
    """整数环境变量回退（非法值报错并指明变量名）。"""
    raw = env_str(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"env {key}={raw!r} is not a valid integer") from None


def env_float(key: str, default: float) -> float:
    """浮点环境变量回退（非法值报错并指明变量名）。"""
    raw = env_str(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"env {key}={raw!r} is not a valid number") from None


@dataclass
class DlqOptions:
    """DLQ 隔离选项（消费端坏数据兜底）。

    enabled=False = 紧急逃生门：毒批不隔离，回退 paused 自愈旧行为。
    """

    enabled: bool | None = None       # None → CONSUMER__DLQ_ENABLED → True
    topic: str | None = None          # None → KAFKA__DLQ_TOPIC（启用时必填）
    message_type: str = "streamgate_dlq"
    send_retries: int | None = None   # None → CONSUMER__DLQ_SEND_RETRIES → 3

    def resolved_enabled(self) -> bool:
        return (
            self.enabled
            if self.enabled is not None
            else env_bool("CONSUMER__DLQ_ENABLED", True)
        )

    def resolved_topic(self) -> str | None:
        return self.topic if self.topic is not None else env_str("KAFKA__DLQ_TOPIC")

    def resolved_send_retries(self) -> int:
        return (
            self.send_retries
            if self.send_retries is not None
            else env_int("CONSUMER__DLQ_SEND_RETRIES", 3)
        )


@dataclass(frozen=True)
class RuntimeTuning:
    """运行时调优（每项未传时回退 CONSUMER__* 同名环境变量，再回退默认）。

    重连/暂停恢复走指数退避：reconnect_base 起步、每轮翻倍、reconnect_max 封顶。
    """

    max_retries: int | None = None               # CONSUMER__MAX_RETRIES → 3
    retry_backoff_base: float | None = None      # CONSUMER__RETRY_BACKOFF_BASE → 1.0
    reconnect_base: float | None = None          # CONSUMER__RECONNECT_BASE_BACKOFF_SECONDS → 1.0
    reconnect_max: float | None = None           # CONSUMER__RECONNECT_MAX_BACKOFF_SECONDS → 30.0
    max_poll_records: int | None = None          # CONSUMER__MAX_POLL_RECORDS → 500
    session_timeout_ms: int | None = None        # CONSUMER__SESSION_TIMEOUT_MS → 30000
    max_poll_interval_ms: int | None = None      # CONSUMER__MAX_POLL_INTERVAL_MS → 300000
    auto_offset_reset: str | None = None         # CONSUMER__AUTO_OFFSET_RESET → "earliest"
    backlog_check_interval: float | None = None  # CONSUMER__BACKLOG_CHECK_INTERVAL_SECONDS → 30.0


@dataclass(frozen=True)
class ResolvedRuntimeTuning:
    """解析完成后的运行时调优（环境变量/默认值已落定；框架内部使用）。"""

    max_retries: int
    retry_backoff_base: float
    reconnect_base: float
    reconnect_max: float
    max_poll_records: int
    session_timeout_ms: int
    max_poll_interval_ms: int
    auto_offset_reset: str
    backlog_check_interval: float

    def backoff_seconds(self, attempt: int) -> float:
        """第 attempt 轮退避时长：base * 2^attempt，封顶 reconnect_max。"""
        return min(
            self.reconnect_max,
            self.reconnect_base * (2 ** min(attempt, 5)),
        )


def resolve_tuning(tuning: RuntimeTuning) -> ResolvedRuntimeTuning:
    """按"显式传值 > 环境变量 > 内置默认"逐项解析。"""
    return ResolvedRuntimeTuning(
        max_retries=(
            tuning.max_retries
            if tuning.max_retries is not None
            else env_int("CONSUMER__MAX_RETRIES", 3)
        ),
        retry_backoff_base=(
            tuning.retry_backoff_base
            if tuning.retry_backoff_base is not None
            else env_float("CONSUMER__RETRY_BACKOFF_BASE", 1.0)
        ),
        reconnect_base=(
            tuning.reconnect_base
            if tuning.reconnect_base is not None
            else env_float("CONSUMER__RECONNECT_BASE_BACKOFF_SECONDS", 1.0)
        ),
        reconnect_max=(
            tuning.reconnect_max
            if tuning.reconnect_max is not None
            else env_float("CONSUMER__RECONNECT_MAX_BACKOFF_SECONDS", 30.0)
        ),
        max_poll_records=(
            tuning.max_poll_records
            if tuning.max_poll_records is not None
            else env_int("CONSUMER__MAX_POLL_RECORDS", 500)
        ),
        session_timeout_ms=(
            tuning.session_timeout_ms
            if tuning.session_timeout_ms is not None
            else env_int("CONSUMER__SESSION_TIMEOUT_MS", 30000)
        ),
        max_poll_interval_ms=(
            tuning.max_poll_interval_ms
            if tuning.max_poll_interval_ms is not None
            else env_int("CONSUMER__MAX_POLL_INTERVAL_MS", 300000)
        ),
        auto_offset_reset=(
            tuning.auto_offset_reset
            if tuning.auto_offset_reset is not None
            else env_str("CONSUMER__AUTO_OFFSET_RESET") or "earliest"
        ),
        backlog_check_interval=(
            tuning.backlog_check_interval
            if tuning.backlog_check_interval is not None
            else env_float("CONSUMER__BACKLOG_CHECK_INTERVAL_SECONDS", 30.0)
        ),
    )


@dataclass
class ConsumerOptions:
    """Consumer 高级项（不传 = 全默认零负担）。

    每一项的一行语义：
    - expected_type：消息 type 校验（None=不校验，兼容历史无 type 消息）
    - probe：单条探针（与 handler 幂等同构）；提供后 POISON 批可精确定位隔离，
      否则整批隔离
    - classifier：异常分类器（RETRY/POISON/FATAL 处置路径）；接 DB 务必注入
      对应分类器（见 streamgate.contrib.sql_upsert）
    - persist_hook：与生产侧判重联动的钩子（DedupCarrier，处理成功后
      on_persisted 权威刷新）；生命周期由框架托管（start/close）
    - collapse_key：批内去重键（保留最后一条；作用于钩子通知，不改变 handler 入参）
    - log_context：隔离日志的每条扩展上下文
    - codec：消息信封编解码（默认 JsonEnvelopeCodec）
    - health_probe：健康快照旁路观测组件（鸭子类型：实现 check_health() 即被
      观测；实现 start()/close() 时生命周期由框架托管）
    - backlog_ttl_seconds：积压告警预算（秒）：WARN > TTL/2、ERROR > TTL*0.8
    - metrics_window_seconds：健康快照速率指标滑动窗口（1–600 秒）
    - dlq：DLQ 隔离选项（DlqOptions）
    - tuning：运行时调优（RuntimeTuning）
    """

    expected_type: str | None = None
    probe: Probe | None = None
    classifier: ErrorClassifier | None = None
    persist_hook: DedupCarrier[JsonObject] | None = None
    collapse_key: Callable[[JsonObject], Hashable] | None = None
    log_context: Callable[[JsonObject], JsonObject] | None = None
    codec: MessageCodec | None = None
    health_probe: object | None = None
    backlog_ttl_seconds: int | None = None  # None → DEFAULT_BACKLOG_TTL_SECONDS
    metrics_window_seconds: int | None = None  # None → METRICS__WINDOW_SECONDS → 60
    dlq: DlqOptions = field(default_factory=DlqOptions)
    tuning: RuntimeTuning = field(default_factory=RuntimeTuning)

    def resolved_metrics_window_seconds(self) -> int:
        """解析指标窗口（显式传值 > METRICS__WINDOW_SECONDS > 60；越界报错）。"""
        window = (
            self.metrics_window_seconds
            if self.metrics_window_seconds is not None
            else env_int("METRICS__WINDOW_SECONDS", DEFAULT_METRICS_WINDOW_SECONDS)
        )
        low, high = METRICS_WINDOW_RANGE
        if not low <= window <= high:
            raise ValueError(
                f"metrics_window_seconds={window} out of range; "
                f"set a value between {low} and {high} "
                "(or env METRICS__WINDOW_SECONDS)"
            )
        return window

    def resolved_backlog_ttl_seconds(self) -> int:
        return (
            self.backlog_ttl_seconds
            if self.backlog_ttl_seconds is not None
            else DEFAULT_BACKLOG_TTL_SECONDS
        )


__all__ = [
    "ConsumerOptions",
    "DEFAULT_BACKLOG_TTL_SECONDS",
    "DlqOptions",
    "ResolvedRuntimeTuning",
    "RuntimeTuning",
    "resolve_tuning",
]
