"""streamgate 公共 API 出口：数据管道机制内核（admission + transport + outlet）。

定位：条件接收 → 可靠投递 → 自由出口的数据管道框架。
- 机制归包：准入编排（唯一性契约流程：存在性判定 + 原子占位 + 冷回源，
  存储载体由你注入）、背压/测活/自愈（重连、paused、DLQ 定位隔离、位点）、
  Kafka 收发、批量缓冲、优雅停机
- 策略归使用方：entity/slot/summary、Schema 校验、数据出口
  （Consumer 的 handler / probe / ConsumerOptions 注入）；DB/Redis/HTTP 等
  I/O 策略实现收敛于 streamgate.contrib（随 wheel 发布、按 extras 携带
  依赖，可运行演示见仓库 examples/）
- 呈现归使用方：HTTP 路由/鉴权/OpenAPI、健康端点暴露（包内零 fastapi/uvicorn）

核心依赖仅 aiokafka / loguru / pydantic 三项；核心层不得 import
streamgate.contrib（import-linter 分层契约强制）。

接收侧 Tier 0 声明式：IngestBinding → IngestGateway（接收内核）
出口侧扁平构造：Consumer(bootstrap_servers, topic, group_id, handler)
    —— 数据出口即消费循环，嵌入宿主即可，进程边界归使用方
组件替换：protocols.py 中的协议 + 下方内置零 I/O 实现
高级项收口：ConsumerOptions / DlqOptions / RuntimeTuning（不传即全默认）

使用方只允许 import 本模块与 streamgate.contrib.* 子包出口，
不得深入其余内部子模块（import-linter 门禁强制）。
"""

from streamgate.config import (
    BackpressureConfig,
    KafkaConfig,
    MetricsConfig,
)
from streamgate.consumer.classifier import DefaultErrorClassifier
from streamgate.consumer.dlq import (
    BisectOutcome,
    BufferedMessage,
    DlqProducer,
    DlqSendError,
    QuarantineRequest,
    locate_and_quarantine,
)
from streamgate.consumer.loop import ConsumeRuntime, consume_loop
from streamgate.consumer.options import (
    ConsumerOptions,
    DlqOptions,
    RuntimeTuning,
)
from streamgate.consumer.runner import Consumer
from streamgate.ingest.admission.in_memory import InMemoryAdmission
from streamgate.ingest.admission.no_admission import NoAdmission
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
    BatchHandler,
    ConsumeContext,
    Decision,
    DecisionKind,
    Envelope,
    ErrorClassifier,
    ErrorKind,
    IngestOutcome,
    JsonObject,
    MessageCodec,
    NoBackfill,
    OutcomeKind,
    Probe,
    ProbeResult,
    RejectInfo,
)
from streamgate.resilience.backpressure import ManualBackpressureSignal
from streamgate.resilience.health import (
    ConsumerHealthResponse,
    IngestHealthResponse,
    collect_consumer_health,
    collect_ingest_health,
)
from streamgate.specs import IngestBinding, IngestRecordT
from streamgate.transport.codec import JsonEnvelopeCodec
from streamgate.transport.kafka import KafkaConsumerService

__version__ = "1.0.0"

__all__ = [
    "AdmissionPolicy",
    "AllowAllSignal",
    "BackfillSource",
    "BackpressureConfig",
    "BackpressureSignal",
    "BackpressureSnapshot",
    "BatchHandler",
    "BisectOutcome",
    "BufferedMessage",
    "ConsumeContext",
    "ConsumeRuntime",
    "Consumer",
    "ConsumerHealthResponse",
    "ConsumerOptions",
    "Decision",
    "DecisionKind",
    "DefaultErrorClassifier",
    "DlqOptions",
    "DlqProducer",
    "DlqSendError",
    "Envelope",
    "ErrorClassifier",
    "ErrorKind",
    "InMemoryAdmission",
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
    "ManualBackpressureSignal",
    "MessageCodec",
    "MetricsConfig",
    "MetricsSink",
    "NoAdmission",
    "NoBackfill",
    "OutcomeKind",
    "Probe",
    "ProbeResult",
    "QuarantineRequest",
    "RejectInfo",
    "RuntimeTuning",
    "collect_consumer_health",
    "collect_ingest_health",
    "configure_logger",
    "consume_loop",
    "locate_and_quarantine",
    "logger",
]
