"""Consumer 高级项收口：ConsumerOptions / DlqOptions / RuntimeTuning。

设计原则：必填项平铺在 Consumer 构造器上（真必填参数）；高级项收进
本模块的 Options 对象，不传 = 全默认零概念负担。配置只有两个真相
来源：必填项显式传入，可选项直接持有内置默认值。
"""

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


@dataclass
class DlqOptions:
    """DLQ 隔离选项（消费端坏数据兜底）。

    enabled=False = 紧急逃生门：毒批不隔离，回退 paused 自愈旧行为。
    topic 在 enabled=True 时必填（DlqProducer 构造期运行时校验）。
    """

    enabled: bool = True
    topic: str | None = None          # 启用（enabled=True）时必填
    message_type: str = "streamgate_dlq"
    send_retries: int = 3


@dataclass(frozen=True)
class RuntimeTuning:
    """运行时调优（每项直接持有内置默认值，不传即默认行为）。

    重连/暂停恢复走指数退避：reconnect_base 起步、每轮翻倍、reconnect_max 封顶。
    """

    max_retries: int = 3
    retry_backoff_base: float = 1.0
    reconnect_base: float = 1.0
    reconnect_max: float = 30.0
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    max_poll_interval_ms: int = 300000
    auto_offset_reset: str = "earliest"
    backlog_check_interval: float = 30.0

    def backoff_seconds(self, attempt: int) -> float:
        """第 attempt 轮退避时长：base * 2^attempt，封顶 reconnect_max。"""
        return min(
            self.reconnect_max,
            self.reconnect_base * (2 ** min(attempt, 5)),
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
    - metrics_window_seconds：健康快照速率指标滑动窗口（默认 60，范围 1–600 秒）
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
    metrics_window_seconds: int = DEFAULT_METRICS_WINDOW_SECONDS
    dlq: DlqOptions = field(default_factory=DlqOptions)
    tuning: RuntimeTuning = field(default_factory=RuntimeTuning)

    def resolved_metrics_window_seconds(self) -> int:
        """指标窗口越界校验（1–600 秒）。"""
        low, high = METRICS_WINDOW_RANGE
        if not low <= self.metrics_window_seconds <= high:
            raise ValueError(
                f"metrics_window_seconds={self.metrics_window_seconds} "
                f"out of range; set a value between {low} and {high}"
            )
        return self.metrics_window_seconds

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
    "RuntimeTuning",
]
