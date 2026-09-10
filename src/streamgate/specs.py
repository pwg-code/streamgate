"""Tier 0 声明式 API：IngestBinding（接收侧）。

机制归框架，策略归使用方：
IngestBinding 声明"接收什么消息、怎么判定唯一性"（HTTP 呈现归使用方适配层；
唯一性载体是注入的 AdmissionPolicy 或零 I/O 内置捷径）。

消费侧声明式 API（ConsumeSpec）已在 1.0.0 移除：数据出口即
Consumer(bootstrap_servers, topic, group_id, handler)——
见 CHANGELOG.md 1.0.0 迁移指南。
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel

from streamgate.protocols import AdmissionPolicy, JsonObject

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


__all__ = ["IngestBinding", "IngestRecordT"]
