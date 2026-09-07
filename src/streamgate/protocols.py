"""Tier 1 公共协议（约 7 个，小而稳定，冻结承诺）。

机制归框架，策略归使用方：替换任一协议实现即可改变对应策略，
不要求继承任何框架基类（Protocol + 组合注入）。
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol, TypeVar, runtime_checkable

# 记录类型（逆变）：准入策略只消费记录（方法参数位），不生产记录，
# 因此 AdmissionPolicy[子类型] 可安全赋给 AdmissionPolicy[基类型]。
RecordT = TypeVar("RecordT", contravariant=True)

# JSON 载荷统一容器：值域为可 JSON 序列化的对象，具体结构由使用方 Schema 约束。
JsonObject = dict[str, object]


# ---- 准入判定结果 ----

class DecisionKind(str, Enum):
    ALLOW = "allow"
    CONFLICT = "conflict"
    REJECT = "reject"


@dataclass(frozen=True)
class RejectInfo:
    """拒绝详情：HTTP 状态/错误码/建议重试间隔 + 观测事件（策略自带语义）。"""

    status_code: int
    error_code: str
    detail: str
    retry_after: int | None = None
    log_event: str | None = None          # 拒绝时由路由发射的稳定事件名
    log_fields: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    """admit 的判定结果。CONFLICT 时 summary 为既有数据摘要（dict，409 载荷来源）。"""

    kind: DecisionKind
    summary: JsonObject | None = None
    reject: RejectInfo | None = None

    @classmethod
    def allow(cls) -> "Decision":
        return cls(DecisionKind.ALLOW)

    @classmethod
    def conflict(cls, summary: JsonObject | None) -> "Decision":
        return cls(DecisionKind.CONFLICT, summary=summary)

    @classmethod
    def rejected(cls, info: RejectInfo) -> "Decision":
        return cls(DecisionKind.REJECT, reject=info)


# ---- 信封 ----

@dataclass(frozen=True)
class Envelope:
    """Kafka 消息信封解析结果（信封带版本号 v，未知 type 走 DLQ 不 crash）。"""

    data: JsonObject
    type: str | None = None      # 缺失视为可接受（向后兼容无 type 的历史消息）
    version: str | None = None
    raw: str = ""


class MessageCodec(Protocol):
    def encode(
        self,
        message_type: str,
        data: JsonObject,
        received_at: str,
        source: str,
    ) -> JsonObject: ...

    def decode(self, raw: str | None) -> Envelope | None:
        """解析 Kafka 消息 JSON，提取 data 载荷。None = 毒丸。

        raw 为 None（tombstone/空值消息）同样按毒丸处理。
        """
        ...


# ---- 准入策略：唯一性契约的两端 + 回源，三者解耦----

@runtime_checkable
class AdmissionPolicy(Protocol[RecordT]):
    async def admit(self, record: RecordT, *, overwrite: bool = False) -> Decision:
        """发送前判定：ALLOW | CONFLICT(既有摘要) | REJECT(status/error_code/retry_after)。
        overwrite=True 时策略应跳过唯一性判定（409 确认后的完整重发）。"""
        ...

    async def on_accepted(self, record: RecordT) -> bool:
        """Kafka 发送成功后：占位/摘要写（失败语义由实现自定）。
        返回 False 表示"缓存未反映本次记录"（映射到 cache_updated=false）。"""
        ...

    async def on_persisted(self, record: RecordT) -> None:
        """consumer 落库成功后：权威刷新/TTL 心跳——准入与消费侧的唯一耦合点。
        实现应自行兜底异常（best-effort），不得影响消费主链路。"""
        ...

    async def start(self) -> None:
        """资源生命周期（框架保证调用时序）。"""
        ...

    async def close(self) -> None:
        """资源释放（框架保证调用时序）。"""
        ...

    async def check_cache_health(self) -> bool:
        """缓存侧健康（/health 用；无缓存的策略恒 False 或恒 True 由语义自定）。"""
        ...

    async def check_backfill_health(self) -> bool:
        """回源库健康（/health 用；无回源的策略返回 False）。"""
        ...

    async def check_backfill_health_detail(self) -> tuple[bool, str | None]:
        """回源库连接探测：返回 (是否可用, 错误信息)。"""
        ...

    @property
    def existence_ttl_seconds(self) -> int | None:
        """缓存 TTL 预算（背压 trip 校验用；无则 None）。"""
        ...


@runtime_checkable
class BackfillSource(Protocol):
    async def load(self, entity: str) -> dict[str, JsonObject]:
        """冷实体回源：返回 {slot: summary_dict}；空 dict=确认不存在；
        失败抛异常（由准入策略转 UNDETERMINED/DEPENDENCY）。"""
        ...


class NoBackfill:
    """无回源（纯缓存判定）。"""

    async def load(self, entity: str) -> dict[str, JsonObject]:
        raise RuntimeError("NoBackfill has no source")


# ---- 写入 ----

@dataclass(frozen=True)
class WriteResult:
    """write 全成时的计数（按 Upsert.name 汇总）；失败以异常表达。"""

    counts: dict[str, int] = field(default_factory=dict)


@runtime_checkable
class RecordWriter(Protocol):
    async def start(self) -> None:
        """资源生命周期启动（建表/连接探测等；装配时由框架调用一次）。"""
        ...

    async def write(self, batch: list[JsonObject]) -> WriteResult:
        """全成 / 部分成(含坏条定位,由框架编排) / 整批失败(触发 paused)。
        唯一写原语是幂等 upsert：失败重试与重投均安全。"""
        ...

    async def close(self) -> None:
        """释放写侧资源（幂等；停机时由消费运行器调用）。"""
        ...


# ---- 背压信号（拓扑无关）----

@dataclass(frozen=True)
class BackpressureSnapshot:
    """背压只读快照：ingest 路由热路径唯一的判定输入。"""

    rejecting: bool
    reason: str | None = None
    backlog_age_seconds: float | None = None
    pending_count: int = 0
    lag: int = 0
    status: str = "healthy"
    kafka: str = "connected"


class ProbeResult:
    """一次探活/观测的判定输入（Signal 实现产出）。"""

    def __init__(
        self,
        status: str,
        backlog_age_seconds: float | None,
        pending_count: int = 0,
        lag: int = 0,
        kafka: str = "connected",
    ) -> None:
        self.status = status
        self.backlog_age_seconds = backlog_age_seconds
        self.pending_count = pending_count
        self.lag = lag
        self.kafka = kafka


@runtime_checkable
class BackpressureSignal(Protocol):
    async def snapshot(self) -> BackpressureSnapshot:
        """rejecting? backlog_age? reason? —— InProcess 直读 / HttpProbe 探远端。"""
        ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    @property
    def rejecting(self) -> bool: ...

    @property
    def reject_reason(self) -> str | None: ...


class AllowAllSignal:
    """未接线时的放行哑实现（测试/单组件场景，避免连锁 503）。"""

    rejecting: bool = False
    reject_reason: str | None = None
    state: str = "OPEN"

    async def snapshot(self) -> BackpressureSnapshot:
        return BackpressureSnapshot(rejecting=False)

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None


# ---- 接收判定结果（传输无关；HTTP 映射归使用方适配层）----

class OutcomeKind(str, Enum):
    ACCEPTED = "accepted"
    CONFLICT = "conflict"
    BACKPRESSURE = "backpressure"
    REJECTED = "rejected"                    # 准入策略显式拒绝（带建议状态码）
    KAFKA_UNAVAILABLE = "kafka_unavailable"  # 发送失败（建议 502）


@dataclass(frozen=True)
class IngestOutcome:
    """process() 的判定结果。字段按 kind 取用：
    ACCEPTED → received_at/cache_updated；CONFLICT → summary；
    BACKPRESSURE → reason/error_code/detail/retry_after；
    REJECTED → status_code/error_code/detail/retry_after；
    KAFKA_UNAVAILABLE → error_code/detail。"""

    kind: OutcomeKind
    received_at: datetime | None = None
    cache_updated: bool = True
    summary: JsonObject | None = None
    reason: str | None = None
    status_code: int | None = None
    error_code: str | None = None
    detail: str | None = None
    retry_after: int | None = None

    @classmethod
    def accepted(
        cls, received_at: datetime, *, cache_updated: bool = True
    ) -> "IngestOutcome":
        return cls(
            OutcomeKind.ACCEPTED,
            received_at=received_at,
            cache_updated=cache_updated,
        )

    @classmethod
    def conflict(cls, summary: JsonObject) -> "IngestOutcome":
        return cls(OutcomeKind.CONFLICT, summary=summary)

    @classmethod
    def backpressure(
        cls,
        reason: str | None,
        *,
        error_code: str,
        detail: str,
        retry_after: int,
    ) -> "IngestOutcome":
        return cls(
            OutcomeKind.BACKPRESSURE,
            reason=reason,
            error_code=error_code,
            detail=detail,
            retry_after=retry_after,
        )

    @classmethod
    def rejected(cls, info: RejectInfo) -> "IngestOutcome":
        return cls(
            OutcomeKind.REJECTED,
            status_code=info.status_code,
            error_code=info.error_code,
            detail=info.detail,
            retry_after=info.retry_after,
        )

    @classmethod
    def kafka_unavailable(cls, *, error_code: str, detail: str) -> "IngestOutcome":
        return cls(
            OutcomeKind.KAFKA_UNAVAILABLE, error_code=error_code, detail=detail
        )


# ---- Tier 2 逃生口：只读消费上下文（位点/commit 不开放）----

@dataclass(frozen=True)
class ConsumeContext:
    """on_record 钩子的只读快照。框架管位点：钩子正常返回即视为整批可提交
    （抛异常 = 本批不提交，走既有重试 → paused 自愈）。"""

    lag: int = 0
    pending_count: int = 0
    paused: bool = False
    quarantined_count: int = 0
    backlog_age_seconds: float | None = None


RecordHandler = Callable[[list[JsonObject], ConsumeContext], Awaitable[None]]

__all__ = [
    "AdmissionPolicy",
    "AllowAllSignal",
    "BackfillSource",
    "BackpressureSignal",
    "BackpressureSnapshot",
    "ConsumeContext",
    "Decision",
    "DecisionKind",
    "Envelope",
    "IngestOutcome",
    "JsonObject",
    "MessageCodec",
    "NoBackfill",
    "OutcomeKind",
    "ProbeResult",
    "RecordHandler",
    "RecordWriter",
    "RejectInfo",
    "WriteResult",
]
