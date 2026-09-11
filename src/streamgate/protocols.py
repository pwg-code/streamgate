"""Tier 1 公共协议（小而稳定，冻结承诺）。

机制归框架，策略归使用方：替换任一协议实现即可改变对应策略，
不要求继承任何框架基类（Protocol + 组合注入）。
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol, TypeVar, runtime_checkable

# 记录类型（逆变）：判重载体只消费记录（方法参数位），不生产记录，
# 因此 DedupCarrier[子类型] 可安全赋给 DedupCarrier[基类型]。
RecordT = TypeVar("RecordT", contravariant=True)

# JSON 载荷统一容器：值域为可 JSON 序列化的对象，具体结构由使用方 Schema 约束。
JsonObject = dict[str, object]


# ---- 判重判定结果 ----

class DecisionKind(str, Enum):
    ALLOW = "allow"
    DUPLICATE = "duplicate"
    REJECT = "reject"


@dataclass(frozen=True)
class RejectInfo:
    """拒绝详情：拒绝原因 + 建议重试间隔 + 观测事件（载体自带语义）。

    呈现契约（HTTP 状态码/error_code/文案映射）归使用方适配层，此处不承载。
    """

    reason: str
    retry_after: int | None = None
    log_event: str | None = None          # 拒绝时由框架发射的稳定事件名
    log_fields: JsonObject = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    """admit 的判定结果。DUPLICATE 时 summary 为既有记录摘要。"""

    kind: DecisionKind
    summary: JsonObject | None = None
    reject: RejectInfo | None = None

    @classmethod
    def allow(cls) -> "Decision":
        return cls(DecisionKind.ALLOW)

    @classmethod
    def duplicate(cls, summary: JsonObject | None) -> "Decision":
        return cls(DecisionKind.DUPLICATE, summary=summary)

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


# ---- 判重载体：唯一性契约的存储侧（opt-in，机制归核心 / 载体归使用方）----

@runtime_checkable
class DedupCarrier(Protocol[RecordT]):
    """身份判定 + 占位 + 通知钩子 + 健康探测。

    自报保证强度（guarantee）：强度 = 所选载体的强度。进程内载体只有
    单进程保证，分布式载体才有多实例保证；框架不做部署形态启动期校验。
    """

    @property
    def guarantee(self) -> str:
        """判重保证强度自报（"process-local" | "distributed"），随 PushResult 透出。"""
        ...

    async def admit(self, record: RecordT, *, force: bool = False) -> Decision:
        """发送前判定：ALLOW（占位已生效）| DUPLICATE(既有摘要) | REJECT(reason/retry_after)。
        force=True 时跳过唯一性判定（确认覆盖后的完整重推），实现可只做
        依赖预检（fail-closed 载体用，防"已发 Kafka 但摘要写失败"半完成态）。"""
        ...

    async def on_send_success(self, record: RecordT) -> None:
        """每次 Kafka 发送成功（broker ack）后调用（含 force 路径；通知型）。
        非 force 路径占位已在 admit 写入，默认无需动作；实现可做缓存续期等。"""
        ...

    async def on_send_failed(self, record: RecordT) -> None:
        """Kafka 发送失败后调用（此时占位可能已写入）。
        框架默认语义是保留占位（防模糊失败重复，靠 TTL/回源自愈）；
        实现可在此释放占位换取"立即可重推"，自担重复风险。"""
        ...

    async def on_force_accepted(self, record: RecordT) -> bool:
        """仅 force 路径、Kafka 成功后：幂等摘要写（失败语义由实现自定）。
        返回 False 表示"存储未反映本次记录"。"""
        ...

    async def on_persisted(self, record: RecordT) -> None:
        """consumer 处理成功后：权威刷新/TTL 心跳——判重与消费侧的唯一耦合点。
        实现应自行兜底异常（best-effort），不得影响消费主链路。"""
        ...

    async def start(self) -> None:
        """资源生命周期（框架保证调用时序）。"""
        ...

    async def close(self) -> None:
        """资源释放（框架保证调用时序）。"""
        ...

    async def check_cache_health(self) -> bool:
        """存储侧健康（/health 用；无缓存的载体恒 False 或恒 True 由语义自定）。"""
        ...

    async def check_backfill_health(self) -> bool:
        """回源库健康（/health 用；无回源的载体返回 False）。"""
        ...

    async def check_backfill_health_detail(self) -> tuple[bool, str | None]:
        """回源库连接探测：返回 (是否可用, 错误信息)。"""
        ...

    @property
    def existence_ttl_seconds(self) -> int | None:
        """占位 TTL 预算（背压 trip 校验用；无则 None）。"""
        ...


@runtime_checkable
class BackfillSource(Protocol):
    async def load(self, scope: str) -> dict[str, JsonObject] | None:
        """统一冷回源：scope 为回源作用域，返回 {identity: summary} 映射。

        - scope = 身份键（单键载体传入）：返回该身份 {identity: summary} 单条目映射，
          无该身份返回 {}（None 与 {} 等价，判重结果一致）
        - scope = 组号（组载体传入）：返回该组全量 {identity: summary}；
          组无记录返回 {}。契约强约束：必须返回整组全量身份集合——
          "组 key 存在 → field 缺失即无此身份"快路径以此为正确性依据
        - 失败抛异常（由判重载体转 REJECT/DEPENDENCY）
        """
        ...


class NoBackfill:
    """无回源（纯存储判定）。"""

    async def load(self, scope: str) -> dict[str, JsonObject] | None:
        raise RuntimeError("NoBackfill has no source")


# ---- 消费端异常分类（异常处置的决策输入）----

class ErrorKind(str, Enum):
    """消费端异常的处置路径分类：

    - RETRY：瞬态错误（超时/连接抖动/限流）→ 退避重试当前批次
    - POISON：毒消息（内容本身处理不了）→ DLQ 定位隔离
    - FATAL：致命错误（框架级不可恢复）→ 停机告警
    """

    RETRY = "retry"
    POISON = "poison"
    FATAL = "fatal"


@runtime_checkable
class ErrorClassifier(Protocol):
    """消费端异常分类器（ConsumerOptions.classifier 注入点）。

    实现必须是纯函数式判定（不产生 I/O、不抛异常）：
    返回 ErrorKind 决定框架对该批失败的处置路径。
    attempt 为当前批次已重试次数（从 0 起），供实现做次数升级策略
    （如"同一异常重试 N 次后升级为 FATAL"），默认实现不使用该参数。

    未注入时框架使用 DefaultErrorClassifier（只认通用异常，
    不认识任何 DB/中间件专有类型；接 DB 的使用方应注入对应分类器，
    参考实现见 streamgate.contrib.sql_upsert）。
    """

    def classify(self, exc: Exception, attempt: int) -> ErrorKind: ...


# ---- 背压信号（拓扑无关）----

@dataclass(frozen=True)
class BackpressureSnapshot:
    """背压只读快照：push 热路径唯一的判定输入。"""

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
    """未接线时的放行哑实现（测试/单组件场景）。"""

    rejecting: bool = False
    reject_reason: str | None = None
    state: str = "OPEN"

    async def snapshot(self) -> BackpressureSnapshot:
        return BackpressureSnapshot(rejecting=False)

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None


# ---- 推入判定结果（传输无关；呈现层映射归使用方适配层）----

class PushKind(str, Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"                  # 已有同身份记录（判重命中）
    BACKPRESSURE = "backpressure"
    REJECTED = "rejected"                    # 判重载体/策略显式拒绝
    UNAVAILABLE = "unavailable"              # Kafka 发送失败


@dataclass(frozen=True)
class PushResult:
    """push() 的判定结果。字段按 kind 取用：
    ACCEPTED → received_at/cache_updated；DUPLICATE → summary；
    BACKPRESSURE → reason/retry_after；REJECTED → reason/retry_after；
    UNAVAILABLE → reason。
    guarantee：判重保证强度（载体自报）；判重未开启时 None。
    呈现层（HTTP 状态码/error_code/文案）映射归使用方适配层。"""

    kind: PushKind
    guarantee: str | None = None
    received_at: datetime | None = None
    cache_updated: bool = True
    summary: JsonObject | None = None
    reason: str | None = None
    retry_after: int | None = None

    @classmethod
    def accepted(
        cls, received_at: datetime, *, guarantee: str | None = None
    ) -> "PushResult":
        return cls(
            PushKind.ACCEPTED, guarantee=guarantee, received_at=received_at
        )

    @classmethod
    def duplicate(
        cls, summary: JsonObject, *, guarantee: str | None = None
    ) -> "PushResult":
        return cls(PushKind.DUPLICATE, guarantee=guarantee, summary=summary)

    @classmethod
    def backpressure(
        cls, reason: str | None, *, retry_after: int, guarantee: str | None = None
    ) -> "PushResult":
        return cls(
            PushKind.BACKPRESSURE,
            guarantee=guarantee,
            reason=reason,
            retry_after=retry_after,
        )

    @classmethod
    def rejected(cls, info: RejectInfo, *, guarantee: str | None = None) -> "PushResult":
        return cls(
            PushKind.REJECTED,
            guarantee=guarantee,
            reason=info.reason,
            retry_after=info.retry_after,
        )

    @classmethod
    def unavailable(cls, reason: str, *, guarantee: str | None = None) -> "PushResult":
        return cls(PushKind.UNAVAILABLE, guarantee=guarantee, reason=reason)


# ---- 出口契约：只读消费上下文（位点/commit 不开放）----

@dataclass(frozen=True)
class ConsumeContext:
    """handler 的只读快照。框架管位点：handler 正常返回即视为整批处理完成
    （抛异常 = 本批不提交，走既有重试 → paused 自愈）。"""

    lag: int = 0
    pending_count: int = 0
    paused: bool = False
    quarantined_count: int = 0
    backlog_age_seconds: float | None = None


BatchHandler = Callable[[list[JsonObject], ConsumeContext], Awaitable[None]]
"""批处理出口契约：Consumer 唯一的数据出口。

正常返回 = 整批处理成功（框架提交位点）；抛异常 = 按 ErrorClassifier
分类处置（RETRY 退避重试 / POISON 定位隔离 / FATAL 停机）。实现须幂等。
"""

Probe = Callable[[JsonObject], Awaitable[None]]
"""单条探针：对单条记录执行与 handler 等价的处理动作（ConsumerOptions.probe）。

用于 POISON 批的精确定位：probe 成功 = 该条已处理；抛异常 = 该条无法
处理（精确隔离进 DLQ）。要求与 handler 幂等同构（框架本就要求幂等）。
"""

__all__ = [
    "AllowAllSignal",
    "BackfillSource",
    "BackpressureSignal",
    "BackpressureSnapshot",
    "BatchHandler",
    "ConsumeContext",
    "Decision",
    "DecisionKind",
    "DedupCarrier",
    "Envelope",
    "ErrorClassifier",
    "ErrorKind",
    "JsonObject",
    "MessageCodec",
    "NoBackfill",
    "Probe",
    "ProbeResult",
    "PushKind",
    "PushResult",
    "RejectInfo",
]
