"""Tier 0 声明式 API：IngestBinding / ConsumeSpec。

机制归框架，策略归使用方：
- IngestBinding 声明"接收什么消息、怎么判定唯一性"（HTTP 呈现归使用方适配层；
  唯一性载体是注入的 AdmissionPolicy 或零 I/O 内置捷径）；
- ConsumeSpec 声明"消费后做什么"：sink（RecordWriter 注入，唯一落库路径）/
  on_record（无 sink 逃生口）二选一。
"""

from collections.abc import Callable, Hashable
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel

from streamgate.protocols import (
    AdmissionPolicy,
    JsonObject,
    RecordHandler,
    RecordWriter,
)

# ingest 侧记录类型：schema 锚定推断（lambda 策略钩子获得精确字段补全）。
IngestRecordT = TypeVar("IngestRecordT", bound=BaseModel)


@dataclass
class IngestBinding(Generic[IngestRecordT]):
    """接收侧机制绑定：使用方完成 Schema 校验后，网关执行
    背压 → 准入 → Kafka 发送 → 发送结果钩子（on_send_success/on_send_failed/
    on_overwrite_accepted）编排。

    路由/URL/鉴权/OpenAPI/响应模型等 HTTP 呈现职责归使用方适配层，
    本声明只承载机制所需的策略钩子与错误码契约。
    """

    message_type: str
    entity_key: Callable[[IngestRecordT], str]
    slot_key: Callable[[IngestRecordT], str]
    summary: Callable[[IngestRecordT], JsonObject]
    topic: str | None = None                                  # None → KAFKA__TOPIC
    partition_key: Callable[[IngestRecordT], str] | None = None  # None → f"{entity}_{slot}"
    admission: AdmissionPolicy[IngestRecordT] | Literal["none", "in-memory"] = "none"
    is_overwrite: Callable[[IngestRecordT], bool] | None = None  # 409 确认后的完整重发标志
    log_context: Callable[[IngestRecordT], JsonObject] | None = None
    # 仅用于 ingest_request 成功日志的扩展上下文（调用方自定义附加字段）
    request_log_context: Callable[[IngestRecordT], JsonObject] | None = None
    # 错误契约（error_code 是与调用方约定的稳定枚举；HTTP 状态映射归适配层）
    backpressure_error_codes: dict[str, str] = field(default_factory=dict)
    backpressure_default_code: str = "BACKPRESSURE_ACTIVE"
    backpressure_detail: str = "backpressure active, retry later"
    kafka_unavailable_code: str = "KAFKA_UNAVAILABLE"
    kafka_unavailable_detail: str = "Message broker unavailable"


@dataclass
class ConsumeSpec:
    """消费侧声明：批量缓冲 → 落地 → on_persisted / DLQ。

    落地目标二选一（声明期校验，互斥）：
    - sink：RecordWriter 注入点（唯一落库路径：写库/写 ES/转发/告警…）
    - on_record：无 sink 逃生口（处理成功即整批可提交，失败走既有重试自愈）
    """

    topic: str | None = None                      # None → KAFKA__TOPIC
    group_id: str | None = None                   # None → CONSUMER__GROUP_ID
    sink: RecordWriter | None = None              # 唯一写侧注入点（与 on_record 互斥）
    on_record: RecordHandler | None = None        # Tier 2：无 sink 时的处理逃生口
    persist_policy: AdmissionPolicy[JsonObject] | None = None  # 仅消费侧 on_persisted（权威刷新）
    collapse_key: Callable[[JsonObject], Hashable] | None = None
    log_context: Callable[[JsonObject], JsonObject] | None = None
    expected_message_type: str | None = None       # None=不校验（兼容历史无 type 消息）
    dlq: bool | None = None                        # None → 跟随 CONSUMER__DLQ_ENABLED
    dlq_message_type: str = "streamgate_dlq"
    dlq_topic: str | None = None                   # None → KAFKA__DLQ_TOPIC

    def __post_init__(self) -> None:
        if self.sink is not None and self.on_record is not None:
            raise ValueError(
                "ConsumeSpec: sink and on_record are mutually exclusive; keep only one"
            )
        if self.sink is None and self.on_record is None:
            raise ValueError(
                "ConsumeSpec requires a landing target: set sink (a RecordWriter "
                "injection for your storage) or on_record (handle records yourself). "
                "See examples/pure_pipeline (on_record) and "
                "examples/sqlite_sink (sink) in the repository."
            )


__all__ = ["ConsumeSpec", "IngestBinding", "IngestRecordT"]
