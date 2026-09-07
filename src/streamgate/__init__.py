"""streamgate 公共 API 出口：管道机制内核（admission + transport + resilience + sink）。

定位：条件接收 → 可靠投递 → 可插拔落地的数据管道框架。
- 机制归包：准入（唯一性契约：存在性判定+原子占位+冷回源）、背压/测活/自愈
  （重连、paused、DLQ 二分、位点）、Kafka 收发、批量缓冲、优雅停机
- 策略归使用方：entity/slot/summary、Schema 校验、落地目标（sink/upserts/on_record）
- 呈现归使用方：HTTP 路由/鉴权/OpenAPI、健康端点暴露（包内零 fastapi/uvicorn）

Tier 0 声明式（90% 使用方）：IngestBinding + ConsumeSpec/Upsert
    → IngestGateway（接收内核）/ ConsumerWorker（消费内核）
Tier 1 组件替换：protocols.py 中的协议 + 下方内置实现
Tier 2 逃生口：ConsumeSpec.on_record / ConsumeContext（只读快照）

使用方只允许 import 本模块，不得深入内部子模块
（import-linter 门禁强制）。
"""

from streamgate.cache.existence import EMPTY_FIELD, RedisExistenceCache
from streamgate.config import (
    BackpressureConfig,
    ConsumerConfig,
    DbConfig,
    KafkaConfig,
    RedisConfig,
)
from streamgate.consumer.classifier import (
    FailureCategory,
    add_failure_rule,
    classify_write_failure,
)
from streamgate.consumer.dlq import (
    BisectOutcome,
    BufferedMessage,
    DlqProducer,
    DlqSendError,
    QuarantineRequest,
    locate_and_write,
)
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.consumer.runner import ConsumerWorker
from streamgate.db.backfill import SqlBackfill
from streamgate.db.engines import (
    async_session_factory,
    create_read_engine,
    create_write_engine,
)
from streamgate.db.upsert import UpsertWriter
from streamgate.ingest.admission.no_admission import NoAdmission
from streamgate.ingest.admission.redis_existence import (
    EntitySlots,
    SlotSource,
    ExistenceUnavailableError,
    RedisExistenceAdmission,
    RedisExistenceAdmissionConfig,
    UndeterminedReason,
)
from streamgate.ingest.gateway import IngestGateway
from streamgate.ingest.producer import KafkaProducerService
from streamgate.obs.logging import configure_logger, logger
from streamgate.obs.metrics import LoggingMetricsSink, MetricsSink
from streamgate.protocols import (
    AdmissionPolicy,
    AllowAllSignal,
    BackfillSource,
    BackpressureSignal,
    BackpressureSnapshot,
    ConsumeContext,
    JsonObject,
    NoBackfill,
    Decision,
    DecisionKind,
    Envelope,
    IngestOutcome,
    MessageCodec,
    OutcomeKind,
    ProbeResult,
    RecordHandler,
    RecordWriter,
    RejectInfo,
    WriteResult,
)
from streamgate.resilience.backpressure import HttpProbeSignal
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    IngestHealthResponse,
    collect_consumer_health,
    collect_ingest_health,
)
from streamgate.specs import ConsumeSpec, IngestBinding, IngestRecordT, Upsert
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService

__version__ = "0.3.0"

__all__ = [
    "AdmissionPolicy",
    "AllowAllSignal",
    "BackfillSource",
    "BackpressureConfig",
    "BackpressureSignal",
    "BackpressureSnapshot",
    "BisectOutcome",
    "EntitySlots",
    "SlotSource",
    "BufferedMessage",
    "ConsumerConfig",
    "ConsumerHealthResponse",
    "ConsumerWorker",
    "ConsumeContext",
    "ConsumeRuntime",
    "ConsumeSpec",
    "DbConfig",
    "Decision",
    "DecisionKind",
    "DlqProducer",
    "DlqSendError",
    "EMPTY_FIELD",
    "Envelope",
    "ExistenceUnavailableError",
    "FailureCategory",
    "HttpProbeSignal",
    "IngestBinding",
    "IngestGateway",
    "IngestHealthResponse",
    "IngestOutcome",
    "IngestRecordT",
    "JsonObject",
    "JsonEnvelopeCodec",
    "KafkaConfig",
    "KafkaConsumerService",
    "KafkaProducerService",
    "LoggingMetricsSink",
    "MessageCodec",
    "MetricsSink",
    "NoAdmission",
    "NoBackfill",
    "OutcomeKind",
    "ProbeResult",
    "QuarantineRequest",
    "RecordHandler",
    "RecordWriter",
    "RedisExistenceAdmission",
    "RedisExistenceAdmissionConfig",
    "RedisExistenceCache",
    "RedisConfig",
    "RejectInfo",
    "SqlBackfill",
    "UndeterminedReason",
    "Upsert",
    "UpsertWriter",
    "WriteResult",
    "add_failure_rule",
    "async_session_factory",
    "classify_write_failure",
    "collect_consumer_health",
    "collect_ingest_health",
    "configure_logger",
    "consume_loop",
    "create_read_engine",
    "create_write_engine",
    "locate_and_write",
    "logger",
]
